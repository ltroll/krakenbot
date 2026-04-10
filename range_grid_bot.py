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
LOG_FILE = os.getenv("TRADE_LOG_FILE", "trade_log.jsonl")

LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
PRICE_LOG_URL = "http://screenpi.local/bot/btc_price_log.jsonl"
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")


###########################################################
# LOGGING
###########################################################

def log_event(event, **kwargs):

    record = {

        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "message": kwargs.pop("message", "")

    }

    record.update(kwargs)

    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(record) + "\n")


def console(msg):

    print(f"[{datetime.utcnow().isoformat()}] {msg}")


###########################################################
# KRAKEN INIT
###########################################################

api = krakenex.API()
api.uri = KRAKEN_API_URL
api.key = os.getenv("KRAKEN_API_KEY")
api.secret = os.getenv("KRAKEN_API_SECRET")

console(f"Using Kraken endpoint: {api.uri}")
log_event("BOT_START", message="Kraken Grid Sentiment Trader Starting")


pair_info = api.query_public("AssetPairs")["result"]["XXBTZUSD"]

PRICE_DECIMALS = pair_info["pair_decimals"]
VOLUME_DECIMALS = pair_info["lot_decimals"]


def round_price(price):

    return round(price, PRICE_DECIMALS)


def round_volume(volume):

    return round(volume, VOLUME_DECIMALS)


###########################################################
# STATE
###########################################################

def load_state():

    default = {

        "open_buy_orders": {},
        "open_sell_orders": {},
        "last_range_refresh": None,
        "range_low": None,
        "range_high": None

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


###########################################################
# SENTIMENT
###########################################################

def fetch_sentiment():

    r = requests.get(LLM_SIGNAL_URL)

    data = r.json()

    log_event("SIGNAL_UPDATE", **data)

    return data["execution_signal"]


###########################################################
# RANGE
###########################################################

def compute_24h_range(records):

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    prices = []

    for entry in records:

        try:

            ts = datetime.fromisoformat(entry["timestamp"])

            if ts >= cutoff:

                prices.append(float(entry["btc_price_usd"]))

        except:

            continue


    return min(prices), max(prices)


###########################################################
# HELPERS
###########################################################

def get_current_price():

    r = api.query_public("Ticker", {"pair": "XXBTZUSD"})

    return float(r["result"]["XXBTZUSD"]["c"][0])


def get_balances():

    r = api.query_private("Balance")

    return float(r["result"].get("ZUSD", 0)), float(
        r["result"].get("XXBT", 0)
    )


def place_limit_buy(price, volume):

    price = round_price(price)
    volume = round_volume(volume)

    order = api.query_private("AddOrder", {

        "pair": "XXBTZUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": str(price),
        "volume": str(volume)

    })

    log_event(

        "ORDER_RESPONSE",

        side="buy",

        price=price,

        volume=volume,

        response=order

    )

    return order


def place_limit_sell(price, volume):

    price = round_price(price)
    volume = round_volume(volume)

    order = api.query_private("AddOrder", {

        "pair": "XXBTZUSD",
        "type": "sell",
        "ordertype": "limit",
        "price": str(price),
        "volume": str(volume)

    })

    log_event(

        "ORDER_RESPONSE",

        side="sell",

        price=price,

        volume=volume,

        response=order

    )

    return order


def order_is_filled(txid):

    result = api.query_private("QueryOrders", {"txid": txid})

    if result["error"]:

        return False


    status = list(result["result"].values())[0]["status"]

    return status == "closed"


###########################################################
# GRID
###########################################################

def compute_grid_levels(low, high, percentile, size):

    rng = high - low

    step = percentile / size

    return sorted(

        [

            low + (rng * (percentile - step * i))

            for i in range(size)

        ],

        reverse=True

    )


###########################################################
# MAIN LOOP
###########################################################

def main():

    config = json.load(open(CONFIG_FILE))

    state = load_state()


    while True:

        try:

            sentiment = fetch_sentiment()


            if sentiment < config["execution_signal_threshold"]:

                console("Sentiment gate active")

                log_event(

                    "NO_TRADE",

                    reason="sentiment_gate",

                    execution_signal=sentiment

                )


            now = datetime.now(timezone.utc)


            refresh_needed = (

                not state["last_range_refresh"]

                or datetime.fromisoformat(

                    state["last_range_refresh"]

                )

                < now

                - timedelta(

                    minutes=config[

                        "range_refresh_interval_minutes"

                    ]

                )

            )


            if refresh_needed:

                records = requests.get(

                    PRICE_LOG_URL

                ).json()

                low, high = compute_24h_range(records)

                state["range_low"] = low
                state["range_high"] = high
                state["last_range_refresh"] = now.isoformat()

                save_state(state)

                console(f"Range refreshed {low} → {high}")

                log_event(

                    "RANGE_REFRESH",

                    low=low,

                    high=high

                )


            low = state["range_low"]
            high = state["range_high"]


            current_price = get_current_price()

            usd_balance, _ = get_balances()


            grid = compute_grid_levels(

                low,

                high,

                config["buy_zone_percentile"],

                config["max_grid_size"]

            )


            console(

                f"Price {current_price} | lowest grid {grid[-1]}"

            )


            for level in grid:

                if str(level) in state["open_buy_orders"]:

                    continue


                if current_price <= level:

                    size = (

                        usd_balance

                        * config["position_size_pct"]

                    )

                    volume = size / level

                    if volume < 0.00005:

                        continue


                    order = place_limit_buy(

                        level,

                        volume

                    )


                    if order["error"]:

                        continue


                    txid = order["result"]["txid"][0]


                    state["open_buy_orders"][

                        str(level)

                    ] = {

                        "txid": txid,

                        "volume": volume

                    }


                    save_state(state)


                    log_event(

                        "TRADE_DECISION",

                        side="buy",

                        volume=volume,

                        price=level

                    )


            time.sleep(

                config["price_check_interval_seconds"]

            )


        except Exception as e:

            console(f"Runtime error {e}")

            log_event(

                "ERROR",

                message=str(e)

            )

            time.sleep(60)


if __name__ == "__main__":

    main()
