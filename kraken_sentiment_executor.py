import os
import json
import time
import hmac
import base64
import hashlib
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from statistics import pstdev
from urllib.parse import urlencode

########################################
# ENVIRONMENT
########################################

load_dotenv()

API_KEY = os.getenv("KRAKEN_API_KEY")
API_SECRET = os.getenv("KRAKEN_API_SECRET")

SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
STATE_FILE = os.getenv("BOT_STATE_FILE")
CONFIG_FILE = os.getenv("BOT_CONFIG_FILE")

PRICE_LOG_PATH = SIGNAL_URL.replace("llm_signal.json", "btc_price_log.jsonl")

TRADE_LOG = "trade_log.jsonl"

PAIR = "XXBTZUSD"

########################################
# EXECUTION PARAMETERS (your choices)
########################################

BASE_BTC_ALLOCATION = 0.50
MAX_ADJUSTMENT = 0.10
MIN_TRADE_USD = 30

########################################
# HELPERS
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
# SIGNAL READER
########################################

def load_signal():
    r = requests.get(SIGNAL_URL)
    r.raise_for_status()
    return r.json()


########################################
# PRICE HISTORY VOLATILITY
########################################

def load_volatility():

    if not os.path.exists(PRICE_LOG_PATH):
        return 1.0

    prices = []

    with open(PRICE_LOG_PATH) as f:
        for line in f:
            prices.append(json.loads(line)["btc_price_usd"])

    if len(prices) < 10:
        return 1.0

    returns = [
        (prices[i] - prices[i-1]) / prices[i-1]
        for i in range(1, len(prices))
    ]

    return pstdev(returns)


########################################
# KRAKEN AUTH
########################################

def kraken_signature(urlpath, data, secret):

    postdata = urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()

    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    signature = hmac.new(
        base64.b64decode(secret),
        message,
        hashlib.sha512
    )

    return base64.b64encode(signature.digest()).decode()


def kraken_private(endpoint, data):

    url = "https://api.kraken.com" + endpoint

    data["nonce"] = str(int(time.time() * 1000))

    headers = {
        "API-Key": API_KEY,
        "API-Sign": kraken_signature(endpoint, data, API_SECRET)
    }

    return requests.post(url, headers=headers, data=data).json()


########################################
# ACCOUNT BALANCES
########################################

def get_balances():

    result = kraken_private("/0/private/Balance", {})

    btc = float(result["result"].get("XXBT", 0))
    usd = float(result["result"].get("ZUSD", 0))

    return btc, usd


########################################
# CURRENT PRICE
########################################

def get_price():

    r = requests.get(TICKER_URL).json()

    return float(
        list(r["result"].values())[0]["c"][0]
    )


########################################
# EXECUTION LOGIC
########################################

def compute_target(signal):

    execution_signal = signal["execution_signal"]

    risk_multiplier = signal["smoothed_risk_multiplier"]

    allocation_target = (
        BASE_BTC_ALLOCATION +
        execution_signal * risk_multiplier
    )

    allocation_target = max(0, min(1, allocation_target))

    return allocation_target


########################################
# ORDER EXECUTION
########################################

def place_market_order(side, volume):

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
# MAIN EXECUTION LOOP
########################################

def execute():

    log_event("BOT_START", message="Kraken Sentiment Trader Starting")

    state = load_state()

    signal = load_signal()

    log_event(
        "SIGNAL_UPDATE",
        execution_signal=signal["execution_signal"],
        confidence=signal["confidence"]
    )

    volatility = load_volatility()

    price = get_price()

    btc, usd = get_balances()

    portfolio_value = btc * price + usd

    current_allocation = (btc * price) / portfolio_value

    target_allocation = compute_target(signal)

    adjustment = target_allocation - current_allocation

    adjustment = max(
        -MAX_ADJUSTMENT,
        min(MAX_ADJUSTMENT, adjustment)
    )

    trade_value = adjustment * portfolio_value

    if abs(trade_value) < MIN_TRADE_USD:
        return

    side = "buy" if trade_value > 0 else "sell"

    volume = abs(trade_value) / price

    log_event(
        "TRADE_DECISION",
        side=side,
        volume=volume,
        price=price
    )

    result = place_market_order(side, volume)

    log_event(
        "TRADE_EXECUTED",
        side=side,
        volume=volume,
        price=price,
        kraken_result=result
    )

    state["last_execution_signal"] = signal["execution_signal"]
    state["last_trade_ts"] = now()
    state["last_target_allocation"] = target_allocation

    save_state(state)


########################################
# ENTRYPOINT
########################################

if __name__ == "__main__":

    execute()
