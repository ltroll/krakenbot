#!/usr/bin/env python3

import os
import json
import time
import requests
import krakenex

from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from pnl_tracker import PnLTracker


load_dotenv()


CONFIG_FILE = "range_grid_config.json"
STATE_FILE = os.getenv("BOT_STATE_FILE", "last_state.json")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
PRICE_LOG_URL = "http://screenpi.local/bot/btc_price_log.jsonl"


api = krakenex.API()
api.key = os.getenv("KRAKEN_API_KEY")
api.secret = os.getenv("KRAKEN_API_SECRET")

tracker = PnLTracker()


# ------------------------
# Utility Functions
# ------------------------

def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)

    return {
        "grid_fills": [],
        "last_range_refresh": None
    }


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def fetch_sentiment():

    r = requests.get(LLM_SIGNAL_URL)
    data = r.json()

    sentiment = data["execution_signal"]

    print("Execution signal:", sentiment)

    return sentiment


def fetch_price_log():

    r = requests.get(PRICE_LOG_URL)
    lines = r.text.splitlines()

    data = []

    for line in lines:
        try:
            data.append(json.loads(line))
        except Exception:
            continue

    return data


def compute_24h_range(price_log):

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    prices = []

    for entry in price_log:

        try:
            ts = datetime.fromisoformat(entry["timestamp"])

            # normalize timezone
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            if ts >= cutoff:
                prices.append(entry["btc_price_usd"])

        except Exception:
            continue

    if not prices:
        return None, None

    return min(prices), max(prices)


def get_current_price():

    r = api.query_public("Ticker", {"pair": "XXBTZUSD"})

    if "result" not in r:
        print("Ticker fetch error:", r)
        return None

    return float(r["result"]["XXBTZUSD"]["c"][0])


def get_balances():

    r = api.query_private("Balance")

    if "result" not in r:
        print("Balance fetch error:", r)
        return 0, 0

    usd = float(r["result"].get("ZUSD", 0))
    btc = float(r["result"].get("XXBT", 0))

    return usd, btc


def place_limit_buy(price, volume):

    return api.query_private("AddOrder", {
        "pair": "XXBTZUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": price,
        "volume": volume
    })


def place_limit_sell(price, volume):

    return api.query_private("AddOrder", {
        "pair": "XXBTZUSD",
        "type": "sell",
        "ordertype": "limit",
        "price": price,
        "volume": volume
    })


# ------------------------
# Grid Logic
# ------------------------

def compute_grid_levels(low, high, percentile, grid_size):

    if low is None or high is None:
        return []

    range_size = high - low
    step = percentile / grid_size

    levels = []

    for i in range(grid_size):
        level = low + (range_size * (percentile - step * i))
        levels.append(level)

    return sorted(levels, reverse=True)


# ------------------------
# Main Loop
# ------------------------

def main():

    config = load_config()
    state = load_state()

    low = None
    high = None

    while True:

        try:

            sentiment = fetch_sentiment()

            if sentiment < config["execution_signal_threshold"]:
                print("Sentiment too low:", sentiment)
                time.sleep(config["price_check_interval_seconds"])
                #continue
                print("Running anyways, need to disable later")


            now = datetime.now(timezone.utc)


            # Refresh range if needed
            if (
                not state["last_range_refresh"]
                or datetime.fromisoformat(state["last_range_refresh"])
                < now - timedelta(minutes=config["range_refresh_interval_minutes"])
            ):

                price_log = fetch_price_log()

                low, high = compute_24h_range(price_log)

                if low is None or high is None:
                    print("No valid 24h range available yet")
                    time.sleep(60)
                    continue

                state["last_range_refresh"] = now.isoformat()
                save_state(state)

                print("Range refreshed:", low, high)


            current_price = get_current_price()

            if current_price is None:
                time.sleep(60)
                continue


            grid_levels = compute_grid_levels(
                low,
                high,
                config["buy_zone_percentile"],
                config["max_grid_size"]
            )

            if not grid_levels:
                print("Grid levels unavailable yet")
                time.sleep(60)
                continue


            usd_balance, btc_balance = get_balances()

            filled_levels = state["grid_fills"]


            for level in grid_levels:

                if len(filled_levels) >= config["max_grid_size"]:
                    break

                if level in filled_levels:
                    continue

                if current_price <= level:

                    position_size = usd_balance * config["position_size_pct"]

                    volume = position_size / current_price


                    if volume < 0.00005:
                        print("Volume below Kraken minimum:", volume)
                        continue


                    print("Placing BUY order @", current_price)

                    order = place_limit_buy(current_price, volume)


                    if "error" in order and order["error"]:
                        print("Kraken BUY error:", order["error"])
                        continue


                    sell_price = current_price * (
                        1
                        + config["profit_target_pct"]
                        + config["round_trip_fee_pct"]
                    )


                    sell_order = place_limit_sell(sell_price, volume)


                    if "error" in sell_order and sell_order["error"]:
                        print("Kraken SELL error:", sell_order["error"])
                        continue


                    print("Placed SELL order @", sell_price)


                    filled_levels.append(level)


                    tracker.record_trade(
                        "buy",
                        current_price,
                        volume,
                        0,
                        btc_balance,
                        usd_balance,
                        current_price,
                        current_price
                    )


                    save_state(state)


            time.sleep(config["price_check_interval_seconds"])


        except Exception as e:

            print("Runtime error:", e)
            time.sleep(60)


if __name__ == "__main__":
    main()
