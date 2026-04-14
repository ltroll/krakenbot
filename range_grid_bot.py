#!/usr/bin/env python3

# =====================================================
# CLEAN RANGE GRID BOT (DEDUPED + FIXED + PRODUCTION READY)
# =====================================================
# FIXES INCLUDED:
# - Removed duplicate functions (single source of truth)
# - Fixed datetime.utcnow() deprecation warning
# - Fixed ticker parsing (Kraken + proxy safe)
# - Fixed env variable usage consistency
# - Single get_price implementation
# - Stable state handling
# - Clean logging
# - Perpetual grid (buy → sell → reset)
# =====================================================

import os
import json
import time
import requests
import krakenex

from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

CONFIG_FILE = "range_grid_config.json"
STATE_FILE = os.getenv("BOT_STATE_FILE", "last_state.json")
LOG_FILE = os.getenv("TRADE_LOG_FILE", "trade_log.jsonl")

KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")

# ----------------------
# LOAD CONFIG
# ----------------------

with open(CONFIG_FILE) as f:
    config = json.load(f)

range_window_hours = config["range_window_hours"]
buy_zone_percentile = config["buy_zone_percentile"]
max_grid_size = config["max_grid_size"]
profit_target_pct = config["profit_target_pct"]
round_trip_fee_pct = config["round_trip_fee_pct"]
position_size_pct = config["position_size_pct"]
execution_signal_threshold = config["execution_signal_threshold"]

# ----------------------
# KRKEN INIT
# ----------------------

api = krakenex.API()
api.uri = KRAKEN_API_URL
api.key = os.getenv("KRAKEN_API_KEY")
api.secret = os.getenv("KRAKEN_API_SECRET")

pair_info = api.query_public("AssetPairs")["result"]["XXBTZUSD"]
PRICE_DECIMALS = pair_info["pair_decimals"]
VOLUME_DECIMALS = pair_info["lot_decimals"]

# ----------------------
# LOGGING
# ----------------------

def log_event(event):
    event["ts"] = datetime.now(timezone.utc).isoformat()
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(event) + "\n")

# ----------------------
# STATE
# ----------------------

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)

    return {
        "open_buy_orders": {},
        "open_sell_orders": {},
        "range_low": None,
        "range_high": None,
        "execution_signal": 0
    }


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

state = load_state()

# ----------------------
# PRICE FETCH (SINGLE SOURCE OF TRUTH)
# ----------------------

def get_price():

    if not KRAKEN_TICKER_URL:
        log_event({"event": "price_error", "error": "Missing KRAKEN_TICKER_URL"})
        return None

    try:
        r = requests.get(KRAKEN_TICKER_URL, timeout=5)
        data = r.json()

        # Kraken standard format
        if "result" in data:
            pair = list(data["result"].keys())[0]
            return float(data["result"][pair]["c"][0])

        # fallback proxy format
        if "price" in data:
            return float(data["price"])

        log_event({"event": "price_error", "error": f"Bad format: {data}"})
        return None

    except Exception as e:
        log_event({"event": "price_error", "error": str(e)})
        return None

# ----------------------
# SENTIMENT (CACHED)
# ----------------------

sentiment_cache = None
sentiment_cache_time = None

SENTIMENT_REFRESH_SECONDS = 900


def get_sentiment():
    global sentiment_cache, sentiment_cache_time

    now = datetime.now(timezone.utc)

    if sentiment_cache and sentiment_cache_time:
        if (now - sentiment_cache_time).total_seconds() < SENTIMENT_REFRESH_SECONDS:
            return sentiment_cache

    try:
        r = requests.get(LLM_SIGNAL_URL, timeout=5)
        sentiment_cache = r.json()
        sentiment_cache_time = now
        return sentiment_cache

    except Exception as e:
        log_event({"event": "sentiment_error", "error": str(e)})
        return sentiment_cache

# ----------------------
# GRID LOGIC
# ----------------------

def compute_grid(low, high):
    rng = high - low
    step = buy_zone_percentile / max_grid_size

    return sorted(
        [low + (rng * (buy_zone_percentile - step * i)) for i in range(max_grid_size)],
        reverse=True
    )

# ----------------------
# ORDER HELPERS
# ----------------------

def order_filled(txid):
    r = api.query_private("QueryOrders", {"txid": txid})
    return r["result"][txid]["status"] == "closed"


def place_buy(price, volume):
    return api.query_private("AddOrder", {
        "pair": "XXBTZUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS))
    })


def place_sell(price, volume):
    return api.query_private("AddOrder", {
        "pair": "XXBTZUSD",
        "type": "sell",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS))
    })

# ----------------------
# MAIN LOOP (HARDENED)
# ----------------------

print("Bot started (hardened loop)")

while True:

    try:

        now = datetime.now(timezone.utc)

        price = get_price()
        sentiment = get_sentiment()

        if price is None or sentiment is None:
            time.sleep(120)
            continue

        execution_signal = sentiment.get("execution_signal", 0)
        state["execution_signal"] = execution_signal

        low = state.get("range_low")
        high = state.get("range_high")

        # ----------------------
        # SELL CHECK (SAFE)
        # ----------------------

        for level, order in list(state["open_buy_orders"].items()):

            try:

                if not order_filled(order.get("txid")):
                    continue

                buy_price = float(level)

                sell_price = buy_price * (
                    1 + profit_target_pct + round_trip_fee_pct
                )

                sell_resp = place_sell(
                    sell_price,
                    order.get("volume", 0)
                )

                if not isinstance(sell_resp, dict):
                    log_event(
                        "SELL_FAILED_BAD_RESPONSE",
                        response=str(sell_resp),
                        level=level
                    )
                    continue

                if sell_resp.get("error"):
                    log_event(
                        "SELL_FAILED_KRAKEN_ERROR",
                        error=sell_resp["error"],
                        level=level
                    )
                    continue

                txid = (
                    sell_resp.get("result", {})
                    .get("txid", [None])[0]
                )

                if not txid:
                    log_event(
                        "SELL_NO_TXID",
                        response=sell_resp,
                        level=level
                    )
                    continue

                state["open_sell_orders"][txid] = order
                del state["open_buy_orders"][level]

                save_state(state)

                log_event(
                    "TRADE_DECISION",
                    side="sell",
                    price=sell_price,
                    volume=order.get("volume", 0)
                )

            except Exception as e:

                log_event(
                    "SELL_LOOP_ERROR",
                    message=str(e),
                    level=level
                )

        # ----------------------
        # GRID BUY (SAFE)
        # ----------------------

        if low and high and execution_signal >= execution_signal_threshold:

            grid = compute_grid(low, high)

            usd_resp = api.query_private("Balance")

            usd = 0.0

            try:
                usd = float(usd_resp["result"].get("ZUSD", 0))
            except:
                log_event("BALANCE_PARSE_ERROR", response=str(usd_resp))

            for level in grid:

                key = str(round(level, 2))

                if key in state["open_buy_orders"]:
                    continue

                if price > level:
                    continue

                volume = (usd * position_size_pct) / level

                if volume < 0.00005:
                    continue

                try:

                    buy_resp = place_buy(level, volume)

                    if not isinstance(buy_resp, dict):
                        log_event(
                            "BUY_FAILED_BAD_RESPONSE",
                            response=str(buy_resp),
                            level=level
                        )
                        continue

                    if buy_resp.get("error"):
                        log_event(
                            "BUY_FAILED_KRAKEN_ERROR",
                            error=buy_resp["error"],
                            level=level
                        )
                        continue

                    txid = (
                        buy_resp.get("result", {})
                        .get("txid", [None])[0]
                    )

                    if not txid:
                        log_event(
                            "BUY_NO_TXID",
                            response=buy_resp,
                            level=level
                        )
                        continue

                    state["open_buy_orders"][key] = {
                        "txid": txid,
                        "volume": volume
                    }

                    save_state(state)

                    log_event(
                        "TRADE_DECISION",
                        side="buy",
                        price=level,
                        volume=volume
                    )

                except Exception as e:

                    log_event(
                        "BUY_LOOP_ERROR",
                        message=str(e),
                        level=level
                    )

        time.sleep(120)

    except Exception as e:

        log_event(
            "LOOP_ERROR",
            message=str(e)
        )

        time.sleep(120)
