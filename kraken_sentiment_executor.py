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
import math
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

REQUEST_TIMEOUT = 10
PAIR_INFO_CACHE = None
EXECUTION_BUFFER_PCT = 0.0025

API_KEY = None
API_SECRET = None
API_URL = None
PAIR = "XXBTZUSD"
SIGNAL_FILE = None
CONFIG_FILE = None
TRADE_LOG = "sentiment_trade_log.txt"

BASE_ALLOCATION = None
MAX_STEP = None
MIN_TRADE_USD = None
CONF_THRESHOLD = None
DRY_RUN = False
MIN_ALLOC = 0.05
MAX_ALLOC = 0.95
MIN_ALLOC_DELTA = 0.02

#########################################
# LOAD ENVIRONMENT
#########################################

def initialize_runtime():

    global API_KEY, API_SECRET, API_URL, PAIR
    global SIGNAL_FILE, CONFIG_FILE, TRADE_LOG, PAIR_INFO_CACHE
    global BASE_ALLOCATION, MAX_STEP, MIN_TRADE_USD
    global CONF_THRESHOLD, DRY_RUN, MIN_ALLOC
    global MAX_ALLOC, MIN_ALLOC_DELTA, EXECUTION_BUFFER_PCT

    load_dotenv()

    API_KEY = os.getenv("KRAKEN_API_KEY")
    API_SECRET = os.getenv("KRAKEN_API_SECRET")
    API_URL = os.getenv("KRAKEN_API_URL")
    PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
    SIGNAL_FILE = os.getenv("SIGNAL_FILE")
    CONFIG_FILE = os.getenv("BOT_CONFIG_FILE")
    TRADE_LOG = os.getenv("TRADE_LOG_FILE", "sentiment_trade_log.txt")
    PAIR_INFO_CACHE = None

    if not CONFIG_FILE:
        raise RuntimeError("Missing BOT_CONFIG_FILE in .env")

    with open(CONFIG_FILE) as f:
        config = json.load(f)

    BASE_ALLOCATION = config["base_btc_allocation"]
    MAX_STEP = config["max_adjustment_per_cycle"]
    MIN_TRADE_USD = config["min_trade_usd"]
    CONF_THRESHOLD = config["confidence_threshold"]
    DRY_RUN = config.get("dry_run", False)
    MIN_ALLOC = config.get("min_total_allocation", 0.05)
    MAX_ALLOC = config.get("max_total_allocation", 0.95)
    MIN_ALLOC_DELTA = config.get("min_allocation_change", 0.02)
    EXECUTION_BUFFER_PCT = config.get("execution_buffer_pct", 0.0025)


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

    r = requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    result = r.json()

    if result.get("error"):
        raise RuntimeError(result["error"])

    return result


#########################################
# PUBLIC PRICE
#########################################


def get_price():

    url = API_URL.rstrip("/") + "/0/public/Ticker"
    r = requests.get(url, params={"pair": PAIR}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    data = r.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    ticker = next(iter(data["result"].values()), None)

    if not ticker:
        raise RuntimeError(f"Ticker data not found for {PAIR}")

    return float(ticker["c"][0])


def get_pair_info():

    global PAIR_INFO_CACHE

    if PAIR_INFO_CACHE:
        return PAIR_INFO_CACHE

    url = API_URL.rstrip("/") + "/0/public/AssetPairs"
    r = requests.get(url, params={"pair": PAIR}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()

    data = r.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    pair_info = next(iter(data["result"].values()), None)

    if not pair_info:
        raise RuntimeError(f"Pair metadata not found for {PAIR}")

    PAIR_INFO_CACHE = pair_info
    return PAIR_INFO_CACHE


#########################################
# BALANCES
#########################################


def get_balances():

    result = kraken_private("/0/private/Balance", {})
    pair_info = get_pair_info()
    base_asset = pair_info["base"]
    quote_asset = pair_info["quote"]

    base_balance = float(result["result"].get(base_asset, 0))
    quote_balance = float(result["result"].get(quote_asset, 0))

    return base_balance, quote_balance


def round_volume(volume):

    pair_info = get_pair_info()
    lot_decimals = int(pair_info.get("lot_decimals", 8))
    factor = 10 ** lot_decimals
    return math.floor(volume * factor) / factor


def get_min_order_volume():

    pair_info = get_pair_info()
    return float(pair_info.get("ordermin") or 0)


#########################################
# SIGNAL
#########################################

def load_signal():

    signal_url = os.getenv("LLM_SIGNAL_URL")

    if signal_url:

        r = requests.get(signal_url, timeout=5)
        r.raise_for_status()

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

    initialize_runtime()
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

    base_balance, quote_balance = get_balances()

    price = get_price()

    portfolio_value = base_balance * price + quote_balance

    if portfolio_value <= 0:
        log("SKIP_EMPTY_PORTFOLIO", portfolio_value)
        return

    allocation_current = (base_balance * price) / portfolio_value

    allocation_target = BASE_ALLOCATION + weighted_signal

    allocation_target = max(MIN_ALLOC, allocation_target)

    allocation_target = min(MAX_ALLOC, allocation_target)

    raw_delta = allocation_target - allocation_current
    delta = max(-MAX_STEP, min(MAX_STEP, raw_delta))

    if abs(delta) < MIN_ALLOC_DELTA:

        log("SKIP_SMALL_DELTA", delta)

        return

    trade_value = abs(delta) * portfolio_value
    trade_value *= max(0, 1 - EXECUTION_BUFFER_PCT)

    if trade_value < MIN_TRADE_USD:

        log("SKIP_SMALL_TRADE", trade_value)

        return

    side = "buy" if delta > 0 else "sell"

    volume = round_volume(trade_value / price)
    min_volume = get_min_order_volume()

    if volume <= 0:
        log("SKIP_ZERO_VOLUME", volume)
        return

    if volume < min_volume:
        log("SKIP_MIN_VOLUME", f"{volume} < {min_volume}")
        return

    log(
        "TRADE_DECISION",
        f"side={side} volume={volume} current={allocation_current} target={allocation_target} raw_delta={raw_delta} delta={delta}"
    )

    place_market(side, volume)


#########################################
# MAIN
#########################################


if __name__ == "__main__":

    execute()
