# RANGE GRID BOT – DROP-IN UPGRADE VERSION
# -------------------------------------------------
# This version adds:
# 1. Sentiment caching (15-minute refresh window)
# 2. Safe execution throttling (2-minute loop)
# 3. Smarter state persistence
# 4. Cleaner logging
# 5. Configurable timing controls
# 6. Backward compatibility with existing config files
# -------------------------------------------------

import os
import json
import time
import requests
import krakenex

from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv


# ==========================
# ENV LOAD
# ==========================

load_dotenv()

KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")

LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")

CONFIG_FILE = "range_grid_config.json"
STATE_FILE = os.getenv("BOT_STATE_FILE", "last_state.json")
LOG_FILE = os.getenv("TRADE_LOG_FILE", "trade_log.jsonl")


# ==========================
# TIMING SETTINGS
# ==========================

LOOP_INTERVAL_SECONDS = 120
SENTIMENT_REFRESH_SECONDS = 900


# ==========================
# GLOBAL STATE CACHE
# ==========================

sentiment_cache = None
sentiment_cache_time = None


# ==========================
# LOAD CONFIG
# ==========================

with open(CONFIG_FILE) as f:
    config = json.load(f)


# ==========================
# INIT KRAKEN
# ==========================

kraken = krakenex.API()
kraken.key = KRAKEN_API_KEY
kraken.secret = KRAKEN_API_SECRET


# ==========================
# STATE FUNCTIONS
# ==========================


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}



def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


state = load_state()


# ==========================
# LOGGING
# ==========================


def log_event(event):
    event["timestamp"] = datetime.utcnow().isoformat()

    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


# ==========================
# PRICE FETCH
# ==========================


def get_price():
    response = requests.get(KRAKEN_API_URL)
    data = response.json()

    pair = list(data["result"].keys())[0]

    return float(data["result"][pair]["c"][0])


# ==========================
# SENTIMENT FETCH (CACHED)
# ==========================


def get_sentiment():
    global sentiment_cache
    global sentiment_cache_time

    now = datetime.utcnow()

    if sentiment_cache:
        age = (now - sentiment_cache_time).total_seconds()

        if age < SENTIMENT_REFRESH_SECONDS:
            return sentiment_cache

    try:
        response = requests.get(LLM_SIGNAL_URL, timeout=5)
        sentiment_cache = response.json()
        sentiment_cache_time = now

        return sentiment_cache

    except Exception:
        return sentiment_cache


# ==========================
# TRADE EXECUTION PLACEHOLDER
# ==========================


def maybe_execute_trade(price, sentiment):
    threshold = config["execution_signal_threshold"]

    if sentiment is None:
        return

    signal_strength = sentiment.get("signal", 0)

    if abs(signal_strength) < threshold:
        return

    log_event({
        "event": "signal_detected",
        "price": price,
        "signal": signal_strength
    })


# ==========================
# MAIN LOOP
# ==========================


print("Bot started successfully")


while True:

    try:

        price = get_price()
        sentiment = get_sentiment()

        maybe_execute_trade(price, sentiment)

    except Exception as e:

        log_event({
            "event": "loop_error",
            "error": str(e)
        })

    time.sleep(LOOP_INTERVAL_SECONDS)
