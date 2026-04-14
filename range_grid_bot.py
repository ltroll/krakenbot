#!/usr/bin/env python3

# =====================================================
# RANGE GRID SENTIMENT BOT (RESTORED STABLE VERSION)
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
PRICE_LOG_URL = os.getenv("PRICE_LOG_URL")

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
# LOGGING
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
        log_event("KRAKEN_BAD_RESPONSE", operation=label)
        return None

    if resp.get("error"):
        log_event("KRAKEN_API_ERROR", operation=label, error=resp["error"])
        return resp

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
# PRICE
# ----------------------

def get_price():

    try:

        if KRAKEN_TICKER_URL:

            r = requests.get(KRAKEN_TICKER_URL, timeout=5)
            data = r.json()

            if "result" in data:
                pair = list(data["result"].keys())[0]
                return float(data["result"][pair]["c"][0])

        r = api.query_public("Ticker", {"pair": "XXBTZUSD"})
        return float(r["result"]["XXBTZUSD"]["c"][0])

    except Exception as e:
        log_event("PRICE_ERROR", message=str(e))
        return None

# ----------------------
# SENTIMENT
# ----------------------

def get_sentiment():

    try:

        r = requests.get(LLM_SIGNAL_URL, timeout=5)
        data = r.json()

        if isinstance(data, dict):
            return data.get("execution_signal", 0)

        return float(data)

    except Exception as e:
        log_event("SENTIMENT_ERROR", message=str(e))
        return None

# ----------------------
# RANGE REFRESH
# ----------------------

def refresh_range():

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

        cutoff = datetime.now(timezone.utc) - timedelta(hours=range_window_hours)

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
            state["last_range_refresh"] = datetime.now(timezone.utc).isoformat()

            save_state(state)

            console(f"Range refreshed: {state['range_low']} → {state['range_high']}")

    except Exception as e:
        log_event("RANGE_REFRESH_ERROR", message=str(e))

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

        if r.get("error"):
            return False

        return r["result"][txid]["status"] == "closed"

    except:

        return False

# ----------------------
# MAIN LOOP
# ----------------------

def main():

    console("Bot started (restored operational build)")

    while True:

        try:

            now = datetime.now(timezone.utc)

            price = get_price()
            sentiment = get_sentiment()

            if price is None or sentiment is None:
                time.sleep(120)
                continue

            execution_signal = sentiment

            console(f"Price: {price} | Signal: {execution_signal}")

            if (
                state["last_range_refresh"] is None
                or (
                    now - datetime.fromisoformat(state["last_range_refresh"])
                ).seconds > 3600
            ):
                refresh_range()

            low = state["range_low"]
            high = state["range_high"]

            # SELL CHECK

            for level, order in list(state["open_buy_orders"].items()):

                txid = order["txid"]

                if not order_filled(txid):
                    continue

                buy_price = float(level)

                sell_price = buy_price * (
                    1 + profit_target_pct + round_trip_fee_pct
                )

                sell_resp = kraken_call(
                    "SELL",
                    place_sell,
                    sell_price,
                    order["volume"]
                )

                if not sell_resp or sell_resp.get("error"):
                    continue

                txid = sell_resp["result"]["txid"][0]

                state["open_sell_orders"][txid] = order

                del state["open_buy_orders"][level]

                save_state(state)

                console(f"SELL placed @ {sell_price}")

            # GRID BUY

            if low and high and execution_signal >= execution_signal_threshold:

                grid = compute_grid(low, high)

                bal = kraken_call("BALANCE", api.query_private, "Balance")

                if not bal:
                    time.sleep(120)
                    continue

                usd = float(bal["result"].get("ZUSD", 0))

                for level in grid:

                    key = str(level)

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

                    txid = buy_resp["result"]["txid"][0]

                    state["open_buy_orders"][key] = {
                        "txid": txid,
                        "volume": volume
                    }

                    save_state(state)

                    console(f"BUY placed @ {level}")

            time.sleep(120)

        except Exception as e:

            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")

            time.sleep(120)


if __name__ == "__main__":
    main()
