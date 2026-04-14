#!/usr/bin/env python3

# =====================================================
# CLEAN + STABLE RANGE GRID BOT (BASELINE VERSION)
# =====================================================
# - Safe Kraken execution wrapper
# - Verbose structured logging
# - No missing function dependencies
# - Stable state handling
# - Defensive JSON parsing
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
# KRAKEN INIT
# ----------------------

api = krakenex.API()
api.uri = KRAKEN_API_URL
api.key = os.getenv("KRAKEN_API_KEY")
api.secret = os.getenv("KRAKEN_API_SECRET")

pair_info = api.query_public("AssetPairs")["result"]["XXBTZUSD"]

PRICE_DECIMALS = pair_info["pair_decimals"]
VOLUME_DECIMALS = pair_info["lot_decimals"]

# ----------------------
# LOGGING (SAFE)
# ----------------------

def log_event(event, **kwargs):

    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "message": kwargs.pop("message", "")
    }

    record.update(kwargs)

    try:
        with open(LOG_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")
    except:
        pass


def console(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")

# ----------------------
# SAFE KRAKEN WRAPPER
# ----------------------

def kraken_call(label, fn, *args, **kwargs):

    try:
        resp = fn(*args, **kwargs)

    except Exception as e:
        log_event("KRAKEN_EXCEPTION", operation=label, message=str(e))
        return None

    if not isinstance(resp, dict):
        log_event("KRAKEN_BAD_RESPONSE", operation=label, response=str(resp))
        return None

    if resp.get("error"):
        log_event("KRAKEN_API_ERROR", operation=label, error=resp["error"])
        return resp

    log_event("KRAKEN_OK", operation=label)
    return resp

# ----------------------
# STATE
# ----------------------

def load_state():

    default = {
        "open_buy_orders": {},
        "open_sell_orders": {},
        "range_low": None,
        "range_high": None,
        "execution_signal": 0,
        "last_range_refresh": None
    }

    if not os.path.exists(STATE_FILE):
        return default

    with open(STATE_FILE) as f:
        state = json.load(f)

    for k in default:
        if k not in state:
            state[k] = default[k]

    return state


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

state = load_state()

# ----------------------
# BOOTSTRAP RANGE IF EMPTY
# ----------------------

if (
    state.get("range_low") is None
    or state.get("range_high") is None
):
    log_event("BOOTSTRAP_START", message="Initializing range from market data")

    try:
        r = requests.get(PRICE_LOG_URL, timeout=10)

        records = []
        for line in r.text.splitlines():
            if not line.strip():
                continue
            try:
                records.append(json.loads(line))
            except:
                continue

        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

        prices = []
        for x in records:
            try:
                ts = datetime.fromisoformat(x["timestamp"])
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                if ts >= cutoff:
                    prices.append(float(x["btc_price_usd"]))
            except:
                continue

        if prices:
            state["range_low"] = min(prices)
            state["range_high"] = max(prices)
            state.setdefault("open_buy_orders", {})
            state.setdefault("open_sell_orders", {})

            save_state(state)

            print(
                f"[BOOTSTRAP] Range initialized: "
                f"{state['range_low']:.0f} → {state['range_high']:.0f}"
            )

            log_event(
                "BOOTSTRAP_COMPLETE",
                range_low=state["range_low"],
                range_high=state["range_high"]
            )

        else:
            log_event(
                "BOOTSTRAP_FAILED",
                message="No price history available"
            )

    except Exception as e:
        log_event(
            "BOOTSTRAP_ERROR",
            message=str(e)
        )

# ----------------------
# PRICE
# ----------------------

def get_price():

    try:

        r = requests.get(KRAKEN_TICKER_URL, timeout=5)
        data = r.json()

        if "result" in data:
            pair = list(data["result"].keys())[0]
            return float(data["result"][pair]["c"][0])

        return None

    except Exception as e:
        log_event("PRICE_ERROR", message=str(e))
        return None

# ----------------------
# SENTIMENT
# ----------------------

def get_sentiment():

    try:
        r = requests.get(LLM_SIGNAL_URL, timeout=5)
        return r.json()
    except Exception as e:
        log_event("SENTIMENT_ERROR", message=str(e))
        return None

# ----------------------
# GRID
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


def order_filled(txid):

    try:
        r = api.query_private("QueryOrders", {"txid": txid})

        if not isinstance(r, dict):
            return False

        if r.get("error"):
            log_event("ORDER_CHECK_ERROR", error=r["error"])
            return False

        result = r.get("result", {})
        if txid not in result:
            return False

        return result[txid].get("status") == "closed"

    except Exception as e:
        log_event("ORDER_CHECK_EXCEPTION", message=str(e))
        return False



# ----------------------
# MAIN LOOP
# ----------------------

console("Bot started (stable baseline)")

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
        # SELL CHECK
        # ----------------------

        for level, order in list(state["open_buy_orders"].items()):

            txid = order.get("txid")
            if not txid:
                continue

            if not order_filled(txid):
                continue

            buy_price = float(level)

            sell_price = buy_price * (1 + profit_target_pct + round_trip_fee_pct)

            sell_resp = kraken_call(
                "SELL",
                place_sell,
                sell_price,
                order.get("volume", 0)
            )

            if not sell_resp or sell_resp.get("error"):
                continue

            txid = sell_resp.get("result", {}).get("txid", [None])[0]
            if not txid:
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

        # ----------------------
        # GRID BUY
        # ----------------------

        if low and high and execution_signal >= execution_signal_threshold:

            grid = compute_grid(low, high)

            usd = float(api.query_private("Balance")["result"].get("ZUSD", 0))

            for level in grid:

                key = str(round(level, 2))

                if key in state["open_buy_orders"]:
                    continue

                if price > level:
                    continue

                volume = (usd * position_size_pct) / level

                if volume < 0.00005:
                    continue

                buy_resp = kraken_call(
                    "BUY",
                    place_buy,
                    level,
                    volume
                )

                if not buy_resp or buy_resp.get("error"):
                    continue

                txid = buy_resp.get("result", {}).get("txid", [None])[0]
                if not txid:
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

        time.sleep(120)

    except Exception as e:

        log_event("LOOP_ERROR", message=str(e))
        time.sleep(120)
