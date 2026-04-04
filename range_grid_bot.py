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

KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")


api = krakenex.API()
api.uri = KRAKEN_API_URL
api.key = os.getenv("KRAKEN_API_KEY")
api.secret = os.getenv("KRAKEN_API_SECRET")

print("Using Kraken endpoint:", api.uri)


tracker = PnLTracker()


###########################################################
# STATE MANAGEMENT (AUTO-UPGRADE SAFE)
###########################################################

def load_state():

    default_state = {

        "grid_fills": [],
        "open_buy_orders": {},
        "last_range_refresh": None,
        "range_low": None,
        "range_high": None

    }

    if not os.path.exists(STATE_FILE):
        return default_state


    with open(STATE_FILE) as f:
        state = json.load(f)


    # auto-upgrade missing keys
    for key in default_state:
        if key not in state:
            state[key] = default_state[key]


    return state


def save_state(state):

    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


###########################################################
# SENTIMENT
###########################################################

def fetch_sentiment():

    r = requests.get(LLM_SIGNAL_URL)
    data = r.json()

    sentiment = data["execution_signal"]

    print("Execution signal:", sentiment)

    return sentiment


###########################################################
# PRICE RANGE
###########################################################

def fetch_price_log():

    r = requests.get(PRICE_LOG_URL)

    return [json.loads(line) for line in r.text.splitlines()]


def compute_24h_range(price_log):

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    prices = []

    for entry in price_log:

        try:

            ts = datetime.fromisoformat(entry["timestamp"])

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)

            if ts >= cutoff:
                prices.append(entry["btc_price_usd"])

        except Exception:
            continue

    if not prices:
        return None, None

    return min(prices), max(prices)


###########################################################
# MARKET DATA
###########################################################

def get_current_price():

    r = api.query_public("Ticker", {"pair": "XXBTZUSD"})

    return float(r["result"]["XXBTZUSD"]["c"][0])


def get_balances():

    r = api.query_private("Balance")

    return float(r["result"].get("ZUSD", 0)), float(r["result"].get("XXBT", 0))


###########################################################
# ORDER HELPERS
###########################################################

def place_limit_buy(price, volume):

    price = round(price, 2)
    volume = round(volume, 6)

    order = api.query_private("AddOrder", {

        "pair": "XXBTZUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": str(price),
        "volume": str(volume)

    })

    print("BUY response:", order)

    return order


def place_limit_sell(price, volume):

    price = round(price, 2)
    volume = round(volume, 6)

    order = api.query_private("AddOrder", {

        "pair": "XXBTZUSD",
        "type": "sell",
        "ordertype": "limit",
        "price": str(price),
        "volume": str(volume)

    })

    print("SELL response:", order)

    return order


def order_is_filled(txid):

    result = api.query_private("QueryOrders", {

        "txid": txid

    })

    if result["error"]:
        return False

    order_data = list(result["result"].values())[0]

    return order_data["status"] == "closed"


###########################################################
# GRID LEVELS
###########################################################

def compute_grid_levels(low, high, percentile, grid_size):

    range_size = high - low

    step = percentile / grid_size

    levels = [

        low + (range_size * (percentile - step * i))

        for i in range(grid_size)

    ]

    return sorted(levels, reverse=True)


###########################################################
# MAIN LOOP
###########################################################

def main():

    config = json.load(open(CONFIG_FILE))

    state = load_state()


    while True:

        try:

            sentiment = fetch_sentiment()

            if (

                sentiment < config["execution_signal_threshold"]

                and not config.get("ignore_sentiment_gate", False)

            ):

                print("Sentiment too low")
                time.sleep(config["price_check_interval_seconds"])
                continue


            now = datetime.now(timezone.utc)


            ###################################################
            # RANGE REFRESH
            ###################################################

            refresh_needed = False

            if not state["last_range_refresh"]:
                refresh_needed = True

            else:

                last_refresh = datetime.fromisoformat(
                    state["last_range_refresh"]
                )

                if last_refresh.tzinfo is None:
                    last_refresh = last_refresh.replace(
                        tzinfo=timezone.utc
                    )

                if last_refresh < now - timedelta(
                    minutes=config["range_refresh_interval_minutes"]
                ):
                    refresh_needed = True


            if refresh_needed:

                price_log = fetch_price_log()

                low, high = compute_24h_range(price_log)

                if not low:

                    print("Range unavailable")
                    time.sleep(60)
                    continue


                state["range_low"] = low
                state["range_high"] = high
                state["last_range_refresh"] = now.isoformat()

                save_state(state)

                print("Range refreshed:", low, high)

            else:

                low = state["range_low"]
                high = state["range_high"]


            ###################################################
            # CHECK OPEN BUYS FOR FILLS
            ###################################################

            for level_str, order_data in list(
                state["open_buy_orders"].items()
            ):

                level = float(level_str)

                txid = order_data["txid"]

                volume = order_data["volume"]

                if order_is_filled(txid):

                    print("BUY filled:", level)

                    sell_price = level * (

                        1
                        + config["profit_target_pct"]
                        + config["round_trip_fee_pct"]

                    )

                    place_limit_sell(
                        sell_price,
                        volume
                    )

                    state["grid_fills"].append(level)

                    del state["open_buy_orders"][level_str]

                    save_state(state)


            ###################################################
            # NEW GRID BUYS
            ###################################################

            current_price = get_current_price()

            usd_balance, btc_balance = get_balances()


            grid_levels = compute_grid_levels(

                low,
                high,
                config["buy_zone_percentile"],
                config["max_grid_size"]

            )


            print("Current:", current_price)
            print("Lowest grid:", grid_levels[-1])


            for level in grid_levels:

                if level in state["grid_fills"]:
                    continue

                if str(level) in state["open_buy_orders"]:
                    continue

                if current_price <= level:

                    position_size = (

                        usd_balance
                        * config["position_size_pct"]

                    )

                    volume = position_size / current_price

                    if volume < 0.00005:
                        continue


                    order = place_limit_buy(
                        current_price,
                        volume
                    )


                    if order["error"]:
                        continue


                    txid = order["result"]["txid"][0]


                    state["open_buy_orders"][str(level)] = {

                        "txid": txid,
                        "volume": volume

                    }

                    save_state(state)


            time.sleep(
                config["price_check_interval_seconds"]
            )


        except Exception as e:

            print("Runtime error:", e)

            time.sleep(60)


if __name__ == "__main__":

    main()
