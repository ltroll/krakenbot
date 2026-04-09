import os
import json
import time
import hmac
import base64
import hashlib
import requests

from dotenv import load_dotenv
from datetime import datetime, timezone
from statistics import pstdev
from urllib.parse import urlencode

########################################
# ENV
########################################

load_dotenv()

API_KEY = os.getenv("KRAKEN_API_KEY")
API_SECRET = os.getenv("KRAKEN_API_SECRET")

SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
TICKER_URL = os.getenv("KRAKEN_TICKER_URL")

STATE_FILE = os.getenv("BOT_STATE_FILE")
CONFIG_FILE = os.getenv("BOT_CONFIG_FILE")

TRADE_LOG = "trade_log.jsonl"

PAIR = "XXBTZUSD"

PRICE_LOG_PATH = SIGNAL_URL.replace(
    "llm_signal.json",
    "btc_price_log.jsonl"
)

########################################
# UTILITIES
########################################

def now():
    return datetime.now(timezone.utc).isoformat()


def log_event(event, **kwargs):

    record = {
        "ts": now(),
        "event": event,
        **kwargs
    }

    with open(TRADE_LOG, "a") as f:
        f.write(json.dumps(record) + "\n")


########################################
# LOAD CONFIG
########################################

def load_config():

    if not CONFIG_FILE:
        raise RuntimeError("BOT_CONFIG_FILE missing")

    with open(CONFIG_FILE) as f:
        return json.load(f)


########################################
# STATE MGMT
########################################

def load_state():

    if not os.path.exists(STATE_FILE):
        return {}

    with open(STATE_FILE) as f:
        return json.load(f)


def save_state(state):

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


########################################
# SIGNAL
########################################

def load_signal():

    r = requests.get(SIGNAL_URL)
    r.raise_for_status()

    return r.json()


########################################
# VOLATILITY
########################################

def compute_volatility():

    if not os.path.exists(PRICE_LOG_PATH):
        return 0

    prices = []

    with open(PRICE_LOG_PATH) as f:

        for line in f:
            prices.append(
                json.loads(line)["btc_price_usd"]
            )

    if len(prices) < 10:
        return 0

    returns = [

        (prices[i] - prices[i - 1]) /
        prices[i - 1]

        for i in range(1, len(prices))

    ]

    return pstdev(returns)


########################################
# KRAKEN AUTH
########################################

def kraken_signature(urlpath, data):

    postdata = urlencode(data)

    encoded = (
        str(data["nonce"]) + postdata
    ).encode()

    message = (
        urlpath.encode() +
        hashlib.sha256(encoded).digest()
    )

    signature = hmac.new(

        base64.b64decode(API_SECRET),

        message,

        hashlib.sha512

    )

    return base64.b64encode(
        signature.digest()
    ).decode()


def kraken_private(endpoint, data):

    url = "https://api.kraken.com" + endpoint

    data["nonce"] = str(
        int(time.time() * 1000)
    )

    headers = {

        "API-Key": API_KEY,

        "API-Sign": kraken_signature(
            endpoint,
            data
        )

    }

    return requests.post(
        url,
        headers=headers,
        data=data
    ).json()


########################################
# BALANCES
########################################

def get_balances():

    result = kraken_private(
        "/0/private/Balance",
        {}
    )

    btc = float(
        result["result"].get("XXBT", 0)
    )

    usd = float(
        result["result"].get("ZUSD", 0)
    )

    return btc, usd


########################################
# PRICE
########################################

def get_price():

    r = requests.get(TICKER_URL).json()

    return float(
        list(
            r["result"].values()
        )[0]["c"][0]
    )


########################################
# TARGET ALLOCATION
########################################

def compute_target(signal, config, volatility):

    base = config["base_btc_allocation"]

    execution_signal = signal[
        "execution_signal"
    ]

    confidence = signal["confidence"]

    multiplier = signal[
        "smoothed_risk_multiplier"
    ]

    target = base + execution_signal * multiplier

    if config["confidence_weighting"]:
        target = base + (
            execution_signal *
            multiplier *
            confidence
        )

    if (
        config["volatility_dampening"]
        and volatility >
        config["volatility_cutoff"]
    ):

        target = (
            base +
            (target - base) * 0.5
        )

    target = max(
        config["min_total_allocation"],
        min(
            config["max_total_allocation"],
            target
        )
    )

    return target


########################################
# ORDER EXECUTION
########################################

def place_order(side, volume, dry_run):

    if dry_run:

        return {
            "status": "DRY_RUN",
            "side": side,
            "volume": volume
        }

    return kraken_private(

        "/0/private/AddOrder",

        {

            "pair": PAIR,

            "type": side,

            "ordertype": "market",

            "volume": volume

        }

    )


########################################
# MAIN LOOP
########################################

def execute():

    config = load_config()

    state = load_state()

    log_event(
        "BOT_START",
        message="Sentiment Executor V2"
    )

    signal = load_signal()

    log_event(

        "SIGNAL_UPDATE",

        execution_signal=signal[
            "execution_signal"
        ],

        confidence=signal["confidence"]

    )

    if (
        signal["confidence"]
        < config["confidence_threshold"]
    ):

        log_event(
            "SKIP_LOW_CONFIDENCE"
        )

        return

    volatility = compute_volatility()

    price = get_price()

    btc, usd = get_balances()

    portfolio_value = btc * price + usd

    current_allocation = (

        btc * price
    ) / portfolio_value

    target = compute_target(
        signal,
        config,
        volatility
    )

    delta = target - current_allocation

    delta = max(

        -config[
            "max_adjustment_per_cycle"
        ],

        min(
            config[
                "max_adjustment_per_cycle"
            ],
            delta
        )

    )

    trade_value = delta * portfolio_value

    if abs(trade_value) < config[
        "min_trade_usd"
    ]:

        return

    side = "buy" if trade_value > 0 else "sell"

    volume = abs(trade_value) / price

    log_event(

        "TRADE_DECISION",

        side=side,

        volume=volume,

        price=price

    )

    result = place_order(
        side,
        volume,
        config["dry_run"]
    )

    log_event(

        "TRADE_EXECUTED",

        side=side,

        volume=volume,

        price=price,

        kraken_result=result

    )

    state[
        "last_target_allocation"
    ] = target

    state[
        "last_execution_signal"
    ] = signal[
        "execution_signal"
    ]

    state[
        "last_trade_ts"
    ] = now()

    save_state(state)


########################################
# ENTRYPOINT
########################################

if __name__ == "__main__":
    execute()
