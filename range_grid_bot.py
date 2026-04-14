# RANGE GRID BOT – DROP-IN UPGRADE VERSION (PATCHED PRICE FETCH FIX)
# -------------------------------------------------
# FIX INCLUDED:
# Resolves loop_error: 'result'
# Adds safer ticker parsing + fallback handling
# Compatible with ScreenPi proxy OR native Kraken endpoint
# -------------------------------------------------

import os
import json
import time
import requests
import krakenex

from datetime import datetime, timedelta
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
    event["timestamp"] = datetime.datetime.now(datetime.UTC).isoformat()

    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")


# ==========================
# PRICE FETCH (FIXED FOR SCREENPI + KRAKEN TICKER URL)
# ==========================


def get_price():

    ticker_url = os.getenv("KRAKEN_TICKER_URL")

    # Defensive logging to confirm env variable loaded correctly
    if not ticker_url:

        log_event({
            "event": "price_fetch_error",
            "error": "KRAKEN_TICKER_URL missing or not loaded from .env"
        })

        return None


    try:

        response = requests.get(ticker_url, timeout=5)
        data = response.json()

        # Kraken native format (your curl output matches this exactly)
        if "result" in data and isinstance(data["result"], dict):

            pair = list(data["result"].keys())[0]

            if "c" in data["result"][pair]:
                return float(data["result"][pair]["c"][0])


        # ScreenPi simplified format support
        if "price" in data:
            return float(data["price"])


        # Unexpected format — log full payload for debugging
        log_event({
            "event": "price_fetch_error",
            "error": f"Unexpected ticker format payload: {data}"
        })

        return None


    except Exception as e:

        log_event({
            "event": "price_fetch_error",
            "error": str(e)
        })

        return None


# ==========================


def get_price():

    try:

        response = requests.get(KRAKEN_API_URL, timeout=5)
        data = response.json()

        # ScreenPi proxy format
        if "result" in data:

            pair = list(data["result"].keys())[0]
            return float(data["result"][pair]["c"][0])

        # direct simplified format support
        if "price" in data:
            return float(data["price"])

        # fallback detection
        if isinstance(data, dict):
            for key in data:
                if isinstance(data[key], dict) and "c" in data[key]:
                    return float(data[key]["c"][0])

        raise Exception("Unexpected ticker format")


    except Exception as e:

        log_event({
            "event": "price_fetch_error",
            "error": str(e)
        })

        return None


# ==========================
# SENTIMENT FETCH (CACHED)
# ==========================


def get_sentiment():

    global sentiment_cache
    global sentiment_cache_time

    now = datetime.datetime.now(datetime.UTC)

    if sentiment_cache:

        age = (now - sentiment_cache_time).total_seconds()

        if age < SENTIMENT_REFRESH_SECONDS:
            return sentiment_cache

    try:

        response = requests.get(LLM_SIGNAL_URL, timeout=5)

        sentiment_cache = response.json()
        sentiment_cache_time = now

        return sentiment_cache

    except Exception as e:

        log_event({
            "event": "sentiment_fetch_error",
            "error": str(e)
        })

        return sentiment_cache


# ==========================
# SENTIMENT-AWARE EXECUTION ENGINE (UPGRADED)
# ==========================


def maybe_execute_trade(price, sentiment):

    if price is None:
        return

    if sentiment is None:
        return


    execution_signal = sentiment.get("execution_signal", 0)
    confidence = sentiment.get("confidence", 0)
    risk_multiplier = sentiment.get("smoothed_risk_multiplier", 1)
    direction_bias = sentiment.get("direction_bias", 0)


    threshold = config["execution_signal_threshold"]


    # Ignore weak signals
    if abs(execution_signal) < threshold:
        return


    # Confidence gating
    if confidence < 0.55:
        log_event({
            "event": "signal_filtered_low_confidence",
            "confidence": confidence
        })
        return


    # Direction-aware filtering
    if execution_signal < 0 and direction_bias > 0.3:

        log_event({
            "event": "signal_filtered_direction_conflict",
            "execution_signal": execution_signal,
            "direction_bias": direction_bias
        })

        return


    adjusted_position_size = config["position_size_pct"] * risk_multiplier


    log_event({
        "event": "trade_signal_triggered",
        "price": price,
        "execution_signal": execution_signal,
        "confidence": confidence,
        "risk_multiplier": risk_multiplier,
        "adjusted_position_size": adjusted_position_size
    })


# ==========================


def maybe_execute_trade(price, sentiment):

    if price is None:
        return

    if sentiment is None:
        return

    threshold = config["execution_signal_threshold"]

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


log_event({
    "event": "BOT_START",
    "message": "Kraken Grid Sentiment Trader Starting"
})


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
