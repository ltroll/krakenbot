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
# ENVIRONMENT
########################################

load_dotenv()

API_KEY = os.getenv("KRAKEN_API_KEY")
API_SECRET = os.getenv("KRAKEN_API_SECRET")

KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")
TICKER_URL = os.getenv("KRAKEN_TICKER_URL")

PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")

SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
STATE_FILE = os.getenv("BOT_STATE_FILE")
CONFIG_FILE = os.getenv("BOT_CONFIG_FILE")

TRADE_LOG = "trade_log.jsonl"

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
# CONFIG LOADING
########################################

def load_config():

    if not CONFIG_FILE:
        raise RuntimeError("BOT_CONFIG_FILE missing")

    if not os.path.exists(CONFIG_FILE):
        raise RuntimeError(
            f"Config file not found: {CONFIG_FILE}"
        )

    with open(CONFIG_FILE) as f:
        return json.load(f)


########################################
# STATE MANAGEMENT
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
# SIGNAL INGESTION
########################################

def load_signal():

    r = requests.get(SIGNAL_URL)
    r.raise_for_status()

    return r.json()


########################################
# VOLATILITY CALCULATION
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

    if not KRAKEN_API_URL:
        raise RuntimeError(
            "KRAKEN_API_URL missing from .env"
        )

    url = KRAKEN_API_URL.rstrip("/") + endpoint

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

    response = requests.post(
        url,
        headers=headers,
        data=data
    )

    result = response.json()

    if result.get("error"):
        raise RuntimeError(
            f"Kraken API error: {result['error']}"
        )

    return result


########################################
# ACCOUNT BALANCES
########################################

def get_balances():

    result = kraken_private(
        "/0/private/Balance",
        {}
    )

    if "result" not in result:

        raise RuntimeError(
            f"Unexpected Kraken response: {result}"
        )

    btc = float(
        result["result"].get("XXBT", 0)
    )

    usd = float(
        result["result"].get("ZUSD", 0)
    )

    return btc, usd


########################################
# CURRENT PRICE
########################################

def get_price():

    if not TICKER_URL:
        raise RuntimeError(
            "KRAKEN_TICKER_URL missing from .env"
        )

    r = requests.get(TICKER_URL)

    data = r.json()

    if data.get("error"):
        raise RuntimeError(
            f"Ticker error: {data['error']}"
        )

    return float(
        list(data["result"].values())[0]["c"][0]
    )


########################################
# TARGET ALLOCATION ENGINE
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
# MAIN EXECUTION ENGINE
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

    if portfolio_value == 0:

        log_event(
            "ERROR",
            message="Portfolio value is zero"
        )

        return

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

        log_event(
            "SKIP_SMALL_TRADE",
            trade_value=trade_value
        )

        return

    side = "buy" if trade_value > 0 else "sell"

    volume = abs(trade_value) / price

    log_event(

        "TRADE_DECISION",

        side=side,

        volume=volume,

        price=price,

        allocation_current=current_allocation,

        allocation_target=target

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
