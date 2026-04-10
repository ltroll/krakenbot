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
# CONFIG
###########################################################

if not os.path.exists(CONFIG_FILE):

    raise FileNotFoundError(
        f"Config file missing: {CONFIG_FILE}"
    )


with open(CONFIG_FILE) as f:

    config = json.load(f)


range_window_hours = config["range_window_hours"]
buy_zone_percentile = config["buy_zone_percentile"]
max_grid_size = config["max_grid_size"]
profit_target_pct = config["profit_target_pct"]
round_trip_fee_pct = config["round_trip_fee_pct"]
position_size_pct = config["position_size_pct"]
execution_signal_threshold = config["execution_signal_threshold"]

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

    console("Starting Kraken Sentiment Bot")

    state = load_state()

    last_range_refresh = state["last_range_refresh"]
    low_price = state["range_low"]
    high_price = state["range_high"]
    
    if isinstance(last_range_refresh, str):
    
        last_range_refresh = datetime.fromisoformat(
            last_range_refresh
        )
    
    
    while True:

        try:

            now = datetime.now(timezone.utc)
           
            # Refresh price range once per hour
            if (
            
                last_range_refresh is None
            
                or
            
                (now - last_range_refresh).seconds > 3600
            
            ):
            
                records = []
            
                response = requests.get(
                    PRICE_LOG_URL
                )
            
                for line in response.text.splitlines():
            
                    if not line.strip():
            
                        continue
            
                    try:
            
                        records.append(
                            json.loads(line)
                        )
            
                    except Exception as e:
            
                        console(
                            f"Malformed JSONL entry: {e}"
                        )
            
                        log_event(
                            "ERROR",
                            message=f"Malformed JSONL entry: {e}"
                        )
            
            
                cutoff = now - timedelta(
                    hours=range_window_hours
                )
            
            
                recent_prices = [
            
                    r["btc_price_usd"]
            
                    for r in records
            
                    if datetime.fromisoformat(
                        r["timestamp"]
                    ) >= cutoff
            
                ]
            
            
                if recent_prices:
            
                    low_price = min(recent_prices)
                    high_price = max(recent_prices)
            
                    console(
            
                        f"Range refreshed "
            
                        f"{low_price:.0f} → {high_price:.0f}"
            
                    )
            
                    log_event(
            
                        "INFO",
            
                        message=f"Range refreshed "
            
                        f"{low_price:.2f} → {high_price:.2f}"
            
                    )
            
            
                    state["range_low"] = low_price
                    state["range_high"] = high_price
                    state["last_range_refresh"] = now.isoformat()
            
                    save_state(state)
            
            
                else:
            
                    console(
                        "No recent price data found "
                        "for range calculation"
                    )
            
                    log_event(
                        "ERROR",
                        message="No recent price data found"
                    )
            
            
                last_range_refresh = now
          
                records = []

                response = requests.get(
                    PRICE_LOG_URL
                )

                for line in response.text.splitlines():

                    if not line.strip():

                        continue

                    try:

                        records.append(
                            json.loads(line)
                        )

                    except Exception as e:

                        console(
                            f"Malformed JSONL entry: {e}"
                        )

                        log_event(
                            "ERROR",
                            message=f"Malformed JSONL entry: {e}"
                        )


                cutoff = now - timedelta(
                    hours=range_window_hours
                )


                recent_prices = [

                    r["btc_price_usd"]

                    for r in records

                    if datetime.fromisoformat(
                        r["timestamp"]
                    ) >= cutoff

                ]


                if recent_prices:

                    low_price = min(recent_prices)
                    high_price = max(recent_prices)

                    console(

                        f"Range refreshed "

                        f"{low_price:.0f} → {high_price:.0f}"

                    )

                    log_event(

                        "INFO",

                        message=f"Range refreshed "

                        f"{low_price:.2f} → {high_price:.2f}"

                    )


                    state["range_low"] = low_price
                    state["range_high"] = high_price
                    state["last_range_refresh"] = now.isoformat()

                    save_state(state)


                else:

                    console(

                        "No recent price data found "

                        "for range calculation"

                    )

                    log_event(

                        "ERROR",

                        message="No recent price data found"

                    )


                last_range_refresh = now


            # Run trading logic
            run_trading_cycle(

                low_price,

                high_price

            )


            time.sleep(60)


        except Exception as e:

            console(f"ERROR: {e}")

            log_event(

                "ERROR",

                message=str(e)

            )

            time.sleep(60)


if __name__ == "__main__":

    main()

if __name__ == "__main__":

    main()
