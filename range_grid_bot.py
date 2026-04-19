#!/usr/bin/env python3

# =====================================================
# RANGE GRID SENTIMENT BOT (RESTORED STABLE VERSION)
# =====================================================

import json
import os
import time
from datetime import datetime, timedelta, timezone

import krakenex
import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

CONFIG_FILE = os.getenv("BOT_CONFIG_FILE", "range_grid_config.json")
STATE_FILE = os.getenv("BOT_STATE_FILE", "last_state.json")
LOG_FILE = os.getenv("TRADE_LOG_FILE", "trade_log.jsonl")

KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")
PRICE_LOG_URL = os.getenv("PRICE_LOG_URL")
GRID_ANCHOR = os.getenv("GRID_ANCHOR")

with open(CONFIG_FILE, encoding="utf-8") as f:
    config = json.load(f)

range_window_hours = config["range_window_hours"]
buy_zone_percentile = config["buy_zone_percentile"]
max_grid_size = config["max_grid_size"]
profit_target_pct = config["profit_target_pct"]
round_trip_fee_pct = config["round_trip_fee_pct"]
position_size_pct = config["position_size_pct"]
execution_signal_threshold = config["execution_signal_threshold"]
grid_anchor = (GRID_ANCHOR or config.get("grid_anchor", "low")).strip().lower()

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
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
    except Exception as e:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] "
            f"LOG_WRITE_ERROR: {e}"
        )


def console(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def log_and_console(event, message="", **kwargs):
    log_event(event, message=message, **kwargs)

    if message:
        console(f"{event}: {message}")
    else:
        console(event)


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
        "range_mean": None,
        "last_range_refresh": None
    }

    if not os.path.exists(STATE_FILE):
        return default

    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)

    for key in default:
        if key not in state:
            state[key] = default[key]

    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def normalize_state(state):
    normalized_buy_orders = {}
    normalized_sell_orders = {}

    for level, order in state["open_buy_orders"].items():
        if isinstance(order, dict):
            normalized_buy_orders[level] = {
                "txid": order.get("txid"),
                "volume": order.get("volume"),
                "price": order.get("price", float(level)),
                "placed_at": order.get("placed_at")
            }
        else:
            normalized_buy_orders[level] = {
                "txid": None,
                "volume": None,
                "price": float(level),
                "placed_at": None
            }

    for txid, order in state["open_sell_orders"].items():
        if isinstance(order, dict) and "level" in order:
            normalized_sell_orders[txid] = {
                "level": order.get("level"),
                "volume": order.get("volume"),
                "buy_price": order.get("buy_price"),
                "sell_price": order.get("sell_price"),
                "placed_at": order.get("placed_at")
            }
        else:
            normalized_sell_orders[txid] = {
                "level": None,
                "volume": order.get("volume"),
                "buy_price": None,
                "sell_price": None,
                "placed_at": None
            }

    state["open_buy_orders"] = normalized_buy_orders
    state["open_sell_orders"] = normalized_sell_orders
    return state


state = normalize_state(load_state())


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
            except Exception:
                continue

        cutoff = datetime.now(timezone.utc) - timedelta(hours=range_window_hours)
        prices = []

        for record in records:
            try:
                ts = datetime.fromisoformat(record["timestamp"])

                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                if ts >= cutoff:
                    prices.append(float(record["btc_price_usd"]))
            except Exception:
                continue

        if prices:
            state["range_low"] = min(prices)
            state["range_high"] = max(prices)
            state["range_mean"] = sum(prices) / len(prices)
            state["last_range_refresh"] = datetime.now(timezone.utc).isoformat()

            save_state(state)

            log_and_console(
                "RANGE_REFRESH",
                message=(
                    f"low={state['range_low']} "
                    f"high={state['range_high']} "
                    f"mean={round(state['range_mean'], 2)}"
                ),
                range_low=state["range_low"],
                range_high=state["range_high"],
                range_mean=state["range_mean"],
                samples=len(prices)
            )
        else:
            log_event(
                "RANGE_REFRESH_SKIPPED",
                message="No prices available in configured range window"
            )
    except Exception as e:
        log_event("RANGE_REFRESH_ERROR", message=str(e))


# ----------------------
# GRID
# ----------------------

def compute_grid(low, high, mean):
    rng = high - low
    step = buy_zone_percentile / max_grid_size

    return sorted(
        [
            mean - (rng * (step * (i + 1)))
            for i in range(max_grid_size)
        ],
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
    except Exception:
        return False


def get_order_status(txid):
    try:
        r = api.query_private("QueryOrders", {"txid": txid})

        if r.get("error"):
            log_event(
                "ORDER_STATUS_ERROR",
                txid=txid,
                error=r["error"]
            )
            return None

        return r["result"][txid]["status"]
    except Exception as e:
        log_event("ORDER_STATUS_ERROR", txid=txid, message=str(e))
        return None


# ----------------------
# MAIN LOOP
# ----------------------

def main():
    log_and_console(
        "BOT_START",
        message="Range Grid Average bot starting",
        config_file=CONFIG_FILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE
    )

    while True:
        try:
            now = datetime.now(timezone.utc)
            cycle_id = now.isoformat()
            actions = []

            price = get_price()
            sentiment = get_sentiment()

            if price is None or sentiment is None:
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    execution_signal=sentiment,
                    reason="missing_price_or_signal",
                    cycle_id=cycle_id
                )
                time.sleep(120)
                continue

            execution_signal = sentiment

            log_event(
                "SIGNAL_UPDATE",
                execution_signal=execution_signal,
                price=price
            )
            console(f"Price: {price} | Signal: {execution_signal}")

            if (
                state["last_range_refresh"] is None
                or (
                    now - datetime.fromisoformat(state["last_range_refresh"])
                ).total_seconds() > 3600
            ):
                refresh_range()

            low = state["range_low"]
            high = state["range_high"]
            mean = state["range_mean"]

            # SELL EXIT CHECK
            for txid, order in list(state["open_sell_orders"].items()):
                status = get_order_status(txid)

                if status is None:
                    continue

                if status == "closed":
                    buy_price = order.get("buy_price")
                    sell_price = order.get("sell_price")
                    volume = order.get("volume", 0)
                    gross_pnl = None
                    estimated_net_pnl = None

                    if (
                        buy_price is not None
                        and sell_price is not None
                        and volume is not None
                    ):
                        gross_pnl = volume * (sell_price - buy_price)
                        estimated_net_pnl = gross_pnl - (
                            volume * buy_price * round_trip_fee_pct
                        )

                    sell_level = order.get("level")
                    del state["open_sell_orders"][txid]
                    save_state(state)
                    actions.append("sell_filled")

                    log_and_console(
                        "SELL_ORDER_FILLED",
                        message=f"SELL filled for level {sell_level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=sell_level,
                        volume=volume,
                        buy_price=buy_price,
                        sell_price=sell_price,
                        gross_pnl=gross_pnl,
                        estimated_net_pnl=estimated_net_pnl
                    )
                    continue

                if status in ("canceled", "expired"):
                    sell_level = order.get("level")
                    del state["open_sell_orders"][txid]
                    save_state(state)
                    actions.append(f"sell_{status}")

                    log_and_console(
                        "ORDER_" + status.upper(),
                        message=f"SELL order {status} for level {sell_level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        side="sell",
                        level=sell_level,
                        volume=order.get("volume"),
                        buy_price=order.get("buy_price"),
                        sell_price=order.get("sell_price")
                    )

            # SELL CHECK
            for level, order in list(state["open_buy_orders"].items()):
                txid = order["txid"]
                status = get_order_status(txid)

                if status is None:
                    continue

                if status in ("canceled", "expired"):
                    del state["open_buy_orders"][level]
                    save_state(state)
                    actions.append(f"buy_{status}")

                    log_and_console(
                        "ORDER_" + status.upper(),
                        message=f"BUY order {status} for level {level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        side="buy",
                        level=level,
                        volume=order.get("volume"),
                        price=order.get("price", float(level))
                    )
                    continue

                if status != "closed":
                    continue

                buy_price = float(level)
                sell_price = buy_price * (
                    1 + profit_target_pct + round_trip_fee_pct
                )
                actions.append("buy_filled")

                log_and_console(
                    "BUY_ORDER_FILLED",
                    message=f"BUY filled @ {round(buy_price, PRICE_DECIMALS)}",
                    cycle_id=cycle_id,
                    txid=txid,
                    level=level,
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(buy_price, PRICE_DECIMALS)
                )

                log_event(
                    "TRADE_DECISION",
                    cycle_id=cycle_id,
                    side="sell",
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(sell_price, PRICE_DECIMALS),
                    buy_price=round(buy_price, PRICE_DECIMALS)
                )

                sell_resp = kraken_call(
                    "SELL",
                    place_sell,
                    sell_price,
                    order["volume"]
                )

                if not sell_resp or sell_resp.get("error"):
                    actions.append("sell_rejected")
                    log_event(
                        "ORDER_REJECTED",
                        cycle_id=cycle_id,
                        side="sell",
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        buy_price=round(buy_price, PRICE_DECIMALS),
                        sell_price=round(sell_price, PRICE_DECIMALS),
                        error=(
                            None if not sell_resp
                            else sell_resp.get("error")
                        )
                    )
                    continue

                txid = sell_resp["result"]["txid"][0]
                state["open_sell_orders"][txid] = {
                    "level": level,
                    "volume": order["volume"],
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "placed_at": cycle_id
                }
                del state["open_buy_orders"][level]
                save_state(state)
                actions.append("sell_placed")

                log_and_console(
                    "SELL_ORDER_PLACED",
                    message=f"SELL placed @ {round(sell_price, PRICE_DECIMALS)}",
                    cycle_id=cycle_id,
                    txid=txid,
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(sell_price, PRICE_DECIMALS),
                    buy_price=round(buy_price, PRICE_DECIMALS)
                )

            # GRID BUY
            if low and high and execution_signal >= execution_signal_threshold:
                if grid_anchor == "mean":
                    grid = compute_grid(low, high, mean)
                else:
                    grid = compute_grid(low, high, low)

                bal = kraken_call("BALANCE", api.query_private, "Balance")

                if not bal:
                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="hold",
                        price=price,
                        execution_signal=execution_signal,
                        reason="balance_fetch_failed"
                    )
                    actions.append("hold_balance_fetch_failed")
                    time.sleep(120)
                    continue

                usd = float(bal["result"].get("ZUSD", 0))
                reserved_sell_levels = {
                    sell_order.get("level")
                    for sell_order in state["open_sell_orders"].values()
                }

                for level in grid:
                    key = str(level)
                    skip_reason = None

                    if key in state["open_buy_orders"]:
                        skip_reason = "open_buy_order"
                    elif key in reserved_sell_levels:
                        skip_reason = "open_sell_order"
                    elif price > level:
                        skip_reason = "price_above_level"

                    volume = (usd * position_size_pct) / level

                    if skip_reason is None and volume < 0.00005:
                        skip_reason = "below_min_volume"

                    if skip_reason is not None:
                        log_event(
                            "GRID_LEVEL_EVAL",
                            cycle_id=cycle_id,
                            level=round(level, PRICE_DECIMALS),
                            market_price=price,
                            execution_signal=execution_signal,
                            usd_balance=usd,
                            reason=skip_reason
                        )
                        continue

                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="buy",
                        volume=round(volume, VOLUME_DECIMALS),
                        price=round(level, PRICE_DECIMALS),
                        execution_signal=execution_signal,
                        range_low=low,
                        range_high=high,
                        range_mean=mean
                    )

                    buy_resp = kraken_call(
                        "BUY",
                        place_buy,
                        level,
                        volume
                    )

                    if not buy_resp or buy_resp.get("error"):
                        actions.append("buy_rejected")
                        log_event(
                            "ORDER_REJECTED",
                            cycle_id=cycle_id,
                            side="buy",
                            level=round(level, PRICE_DECIMALS),
                            volume=round(volume, VOLUME_DECIMALS),
                            execution_signal=execution_signal,
                            error=(
                                None if not buy_resp
                                else buy_resp.get("error")
                            )
                        )
                        continue

                    txid = buy_resp["result"]["txid"][0]
                    state["open_buy_orders"][key] = {
                        "txid": txid,
                        "volume": volume,
                        "price": level,
                        "placed_at": cycle_id
                    }
                    save_state(state)
                    actions.append("buy_placed")

                    log_and_console(
                        "BUY_ORDER_PLACED",
                        message=f"BUY placed @ {round(level, PRICE_DECIMALS)}",
                        cycle_id=cycle_id,
                        txid=txid,
                        volume=round(volume, VOLUME_DECIMALS),
                        price=round(level, PRICE_DECIMALS)
                    )
            else:
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    execution_signal=execution_signal,
                    threshold=execution_signal_threshold,
                    range_low=low,
                    range_high=high,
                    reason="signal_below_threshold_or_range_unavailable",
                    cycle_id=cycle_id
                )
                actions.append("hold")

            log_event(
                "CYCLE_SUMMARY",
                cycle_id=cycle_id,
                price=price,
                execution_signal=execution_signal,
                threshold=execution_signal_threshold,
                range_low=low,
                range_high=high,
                range_mean=mean,
                grid_anchor=grid_anchor,
                grid_levels=(
                    [round(level, PRICE_DECIMALS) for level in grid]
                    if low and high and execution_signal >= execution_signal_threshold
                    else []
                ),
                open_buy_count=len(state["open_buy_orders"]),
                open_sell_count=len(state["open_sell_orders"]),
                actions=actions or ["no_action"]
            )

            time.sleep(120)
        except Exception as e:
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            time.sleep(120)


if __name__ == "__main__":
    main()
