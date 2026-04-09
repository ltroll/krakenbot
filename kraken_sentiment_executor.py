# Kraken Sentiment Executor V3
# Production-ready allocation controller with:
# - confidence weighting
# - signal smoothing memory
# - minimum allocation delta threshold
# - .env-driven endpoints
# - sandbox compatibility
# - configurable behavior via JSON config

import os
import time
import json
import base64
import hashlib
import hmac
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv


#########################################
# LOAD ENVIRONMENT
#########################################

load_dotenv()

API_KEY = os.getenv("KRAKEN_API_KEY")
API_SECRET = os.getenv("KRAKEN_API_SECRET")
API_URL = os.getenv("KRAKEN_API_URL")
TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")

SIGNAL_FILE = os.getenv("SIGNAL_FILE")
CONFIG_FILE = os.getenv("BOT_CONFIG_FILE")
TRADE_LOG = os.getenv("TRADE_LOG_FILE", "sentiment_trade_log.txt")


#########################################
# LOAD CONFIG
#########################################

with open(CONFIG_FILE) as f:
    CONFIG = json.load(f)

BASE_ALLOCATION = CONFIG["base_btc_allocation"]
MAX_STEP = CONFIG["max_adjustment_per_cycle"]
MIN_TRADE_USD = CONFIG["min_trade_usd"]
CONF_THRESHOLD = CONFIG["confidence_threshold"]
DRY_RUN = CONFIG.get("dry_run", False)

MIN_ALLOC = CONFIG.get("min_total_allocation", 0.05)
MAX_ALLOC = CONFIG.get("max_total_allocation", 0.95)

MIN_ALLOC_DELTA = CONFIG.get("min_allocation_change", 0.02)


#########################################
# LOGGING
#########################################


def log(event, message=""):

    timestamp = datetime.now(timezone.utc).isoformat()

    line = f"{timestamp} | {event:<18} | {message}"

    print(line)

    with open(TRADE_LOG, "a") as f:
        f.write(line + "\n")


#########################################
# SIGNAL MEMORY (SMOOTHING)
#########################################

SIGNAL_HISTORY_FILE = "signal_memory.json"


def load_signal_memory():

    if not os.path.exists(SIGNAL_HISTORY_FILE):
        return [0, 0]

    with open(SIGNAL_HISTORY_FILE) as f:
        return json.load(f)



def save_signal_memory(memory):

    with open(SIGNAL_HISTORY_FILE, "w") as f:
        json.dump(memory, f)


#########################################
# SIGNATURE
#########################################


def kraken_signature(endpoint, data):

    postdata = "&".join([f"{k}={v}" for k, v in data.items()])

    encoded = (str(data["nonce"]) + postdata).encode()

    message = endpoint.encode() + hashlib.sha256(encoded).digest()

    mac = hmac.new(
        base64.b64decode(API_SECRET),
        message,
        hashlib.sha512
    )

    return base64.b64encode(mac.digest()).decode()


#########################################
# PRIVATE API
#########################################


def kraken_private(endpoint, data):

    url = API_URL.rstrip("/") + endpoint

    data["nonce"] = str(int(time.time() * 1000))

    headers = {
        "API-Key": API_KEY,
        "API-Sign": kraken_signature(endpoint, data)
    }

    r = requests.post(url, headers=headers, data=data)

    result = r.json()

    if result.get("error"):
        raise RuntimeError(result["error"])

    return result


#########################################
# PUBLIC PRICE
#########################################


def get_price():

    r = requests.get(TICKER_URL)

    data = r.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    return float(list(data["result"].values())[0]["c"][0])


#########################################
# BALANCES
#########################################


def get_balances():

    result = kraken_private("/0/private/Balance", {})

    btc = float(result["result"].get("XXBT", 0))

    usd = float(result["result"].get("ZUSD", 0))

    return btc, usd


#########################################
# SIGNAL
#########################################

def load_signal():

    signal_url = os.getenv("LLM_SIGNAL_URL")

    if signal_url:

        r = requests.get(signal_url, timeout=5)

        if r.status_code != 200:

            raise RuntimeError(
                f"Signal fetch failed: {r.status_code}"
            )

        signal = r.json()

    else:

        if not SIGNAL_FILE:

            raise RuntimeError(
                "Missing SIGNAL_FILE or LLM_SIGNAL_URL in .env"
            )

        with open(SIGNAL_FILE) as f:

            signal = json.load(f)

    return signal["execution_signal"], signal["confidence"]



#########################################
# SMOOTH SIGNAL
#########################################


def smooth_signal(current_signal):

    prev1, prev2 = load_signal_memory()

    smoothed = (
        0.6 * current_signal
        + 0.3 * prev1
        + 0.1 * prev2
    )

    save_signal_memory([current_signal, prev1])

    return smoothed


#########################################
# ORDER EXECUTION
#########################################


def place_market(side, volume):

    if DRY_RUN:
        log("DRY_RUN", f"{side} {volume}")
        return

    result = kraken_private(
        "/0/private/AddOrder",
        {
            "pair": PAIR,
            "type": side,
            "ordertype": "market",
            "volume": volume
        }
    )

    log("TRADE_EXECUTED", str(result))


#########################################
# EXECUTOR
#########################################


def execute():

    log("BOT_START", "Sentiment Executor V3")

    raw_signal, confidence = load_signal()

    if confidence < CONF_THRESHOLD:

        log("SKIP_LOW_CONF", confidence)

        return

    smoothed_signal = smooth_signal(raw_signal)

    weighted_signal = smoothed_signal * confidence

    log(
        "SIGNAL_UPDATE",
        f"raw={raw_signal} smooth={smoothed_signal} conf={confidence}"
    )

    btc, usd = get_balances()

    price = get_price()

    portfolio_value = btc * price + usd

    allocation_current = (btc * price) / portfolio_value

    allocation_target = BASE_ALLOCATION + weighted_signal

    allocation_target = max(MIN_ALLOC, allocation_target)

    allocation_target = min(MAX_ALLOC, allocation_target)

    delta = allocation_target - allocation_current

    if abs(delta) < MIN_ALLOC_DELTA:

        log("SKIP_SMALL_DELTA", delta)

        return

    delta = max(-MAX_STEP, min(MAX_STEP, delta))

    trade_value = abs(delta) * portfolio_value

    if trade_value < MIN_TRADE_USD:

        log("SKIP_SMALL_TRADE", trade_value)

        return

    side = "buy" if delta > 0 else "sell"

    volume = trade_value / price

    log(
        "TRADE_DECISION",
        f"side={side} volume={volume} current={allocation_current} target={allocation_target}"
    )

    place_market(side, volume)


#########################################
# MAIN
#########################################


if __name__ == "__main__":

    execute()
