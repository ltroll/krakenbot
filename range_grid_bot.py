#!/usr/bin/env python3

# =====================================================
# RANGE GRID SENTIMENT BOT (RESTORED STABLE VERSION)
# =====================================================

import json
import os
import statistics
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


def env_int(name, default):
    value = os.getenv(name)
    return default if value is None else int(value)


def env_float(name, default):
    value = os.getenv(name)
    return default if value is None else float(value)

with open(CONFIG_FILE, encoding="utf-8") as f:
    config = json.load(f)

range_window_hours = env_int("RANGE_WINDOW_HOURS", config["range_window_hours"])
max_grid_size = env_int("MAX_GRID_SIZE", config["max_grid_size"])
profit_target_pct = env_float("PROFIT_TARGET_PCT", config["profit_target_pct"])
entry_step_pct = env_float(
    "ENTRY_STEP_PCT",
    config.get("entry_step_pct", profit_target_pct / 2)
)
round_trip_fee_pct = env_float("ROUND_TRIP_FEE_PCT", config["round_trip_fee_pct"])
position_size_pct = env_float("POSITION_SIZE_PCT", config["position_size_pct"])
execution_signal_threshold = env_float(
    "EXECUTION_SIGNAL_THRESHOLD",
    config["execution_signal_threshold"]
)
price_check_interval_seconds = env_int(
    "PRICE_CHECK_INTERVAL_SECONDS",
    config.get("price_check_interval_seconds", 120)
)
range_refresh_interval_minutes = env_int(
    "RANGE_REFRESH_INTERVAL_MINUTES",
    config.get("range_refresh_interval_minutes", 60)
)
max_open_sell_orders = env_int(
    "MAX_OPEN_SELL_ORDERS",
    config.get("max_open_sell_orders", 999999)
)
max_inventory_usd = env_float(
    "MAX_INVENTORY_USD",
    config.get("max_inventory_usd", 1e18)
)
aging_start_minutes = env_int(
    "AGING_START_MINUTES",
    config.get("aging_start_minutes", 999999)
)
aging_step_minutes = env_int(
    "AGING_STEP_MINUTES",
    config.get("aging_step_minutes", 60)
)
aging_profit_reduction_pct = env_float(
    "AGING_PROFIT_REDUCTION_PCT",
    config.get("aging_profit_reduction_pct", 0.0)
)
min_profit_target_pct = env_float(
    "MIN_PROFIT_TARGET_PCT",
    config.get("min_profit_target_pct", profit_target_pct)
)
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
        "range_median": None,
        "last_range_refresh": None,
        "stats": {
            "buy_orders_placed": 0,
            "buy_orders_filled": 0,
            "sell_orders_placed": 0,
            "sell_orders_filled": 0,
            "realized_gross_pnl": 0.0,
            "realized_estimated_net_pnl": 0.0
        }
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
    state.setdefault("stats", {})

    for key, default_value in {
        "buy_orders_placed": 0,
        "buy_orders_filled": 0,
        "sell_orders_placed": 0,
        "sell_orders_filled": 0,
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0
    }.items():
        state["stats"].setdefault(key, default_value)

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


def parse_iso8601(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


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
            state["range_median"] = statistics.median(prices)
            state["last_range_refresh"] = datetime.now(timezone.utc).isoformat()

            save_state(state)

            log_and_console(
                "RANGE_REFRESH",
                message=(
                    f"low={state['range_low']} "
                    f"high={state['range_high']} "
                    f"mean={round(state['range_mean'], 2)} "
                    f"median={round(state['range_median'], 2)}"
                ),
                range_low=state["range_low"],
                range_high=state["range_high"],
                range_mean=state["range_mean"],
                range_median=state["range_median"],
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
    return sorted(
        [
            mean * (1 - (entry_step_pct * (i + 1)))
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


def cancel_order(txid):
    return api.query_private("CancelOrder", {"txid": txid})


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


def compute_sell_target_price(buy_price, profit_target_override=None):
    profit_target = (
        profit_target_pct
        if profit_target_override is None
        else profit_target_override
    )
    return buy_price * (1 + profit_target + round_trip_fee_pct)


def compute_adjusted_profit_target(age_minutes):
    if (
        age_minutes is None
        or aging_profit_reduction_pct <= 0
        or age_minutes < aging_start_minutes
    ):
        return profit_target_pct

    reduction_steps = (
        int((age_minutes - aging_start_minutes) // max(1, aging_step_minutes))
        + 1
    )
    adjusted_profit = (
        profit_target_pct - reduction_steps * aging_profit_reduction_pct
    )
    return max(min_profit_target_pct, adjusted_profit)


def current_inventory_usd(current_price):
    open_buy_notional = sum(
        (order.get("price") or current_price) * (order.get("volume") or 0)
        for order in state["open_buy_orders"].values()
    )
    open_sell_notional = sum(
        (order.get("buy_price") or current_price) * (order.get("volume") or 0)
        for order in state["open_sell_orders"].values()
    )
    return open_buy_notional + open_sell_notional


# ----------------------
# MAIN LOOP
# ----------------------

def main():
    log_and_console(
        "BOT_START",
        message="Range Grid Average bot starting",
        config_file=CONFIG_FILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        grid_anchor=grid_anchor,
        range_window_hours=range_window_hours,
        max_grid_size=max_grid_size,
        profit_target_pct=profit_target_pct,
        entry_step_pct=entry_step_pct,
        round_trip_fee_pct=round_trip_fee_pct,
        position_size_pct=position_size_pct,
        execution_signal_threshold=execution_signal_threshold,
        price_check_interval_seconds=price_check_interval_seconds,
        range_refresh_interval_minutes=range_refresh_interval_minutes,
        max_open_sell_orders=max_open_sell_orders,
        max_inventory_usd=max_inventory_usd,
        aging_start_minutes=aging_start_minutes,
        aging_step_minutes=aging_step_minutes,
        aging_profit_reduction_pct=aging_profit_reduction_pct,
        min_profit_target_pct=min_profit_target_pct
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
                time.sleep(price_check_interval_seconds)
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
                ).total_seconds() > (range_refresh_interval_minutes * 60)
            ):
                refresh_range()

            low = state["range_low"]
            high = state["range_high"]
            mean = state["range_mean"]
            median = state["range_median"]

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
                    hold_minutes = None

                    if (
                        buy_price is not None
                        and sell_price is not None
                        and volume is not None
                    ):
                        gross_pnl = volume * (sell_price - buy_price)
                        estimated_net_pnl = gross_pnl - (
                            volume * buy_price * round_trip_fee_pct
                        )

                    placed_at = parse_iso8601(order.get("placed_at"))
                    if placed_at is not None:
                        hold_minutes = (
                            now - placed_at
                        ).total_seconds() / 60

                    sell_level = order.get("level")
                    del state["open_sell_orders"][txid]
                    state["stats"]["sell_orders_filled"] += 1
                    if gross_pnl is not None:
                        state["stats"]["realized_gross_pnl"] += gross_pnl
                    if estimated_net_pnl is not None:
                        state["stats"]["realized_estimated_net_pnl"] += (
                            estimated_net_pnl
                        )
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
                        estimated_net_pnl=estimated_net_pnl,
                        hold_minutes=hold_minutes
                    )
                    continue

                if status == "open":
                    buy_price = order.get("buy_price")
                    current_sell_price = order.get("sell_price")
                    placed_at = parse_iso8601(order.get("placed_at"))
                    age_minutes = None
                    if placed_at is not None:
                        age_minutes = (
                            now - placed_at
                        ).total_seconds() / 60

                    adjusted_profit_target = compute_adjusted_profit_target(
                        age_minutes
                    )

                    if buy_price is None or current_sell_price is None:
                        continue

                    adjusted_sell_price = compute_sell_target_price(
                        buy_price,
                        adjusted_profit_target
                    )

                    if round(adjusted_sell_price, PRICE_DECIMALS) >= round(
                        current_sell_price,
                        PRICE_DECIMALS
                    ):
                        continue

                    cancel_resp = kraken_call(
                        "CANCEL_SELL",
                        cancel_order,
                        txid
                    )

                    if not cancel_resp or cancel_resp.get("error"):
                        actions.append("sell_reprice_cancel_failed")
                        log_event(
                            "SELL_REPRICE_SKIPPED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_price=buy_price,
                            current_sell_price=current_sell_price,
                            adjusted_sell_price=adjusted_sell_price,
                            age_minutes=age_minutes,
                            adjusted_profit_target_pct=adjusted_profit_target,
                            reason="cancel_failed"
                        )
                        continue

                    replace_resp = kraken_call(
                        "REPRICE_SELL",
                        place_sell,
                        adjusted_sell_price,
                        order["volume"]
                    )

                    if not replace_resp or replace_resp.get("error"):
                        actions.append("sell_reprice_replace_failed")
                        log_event(
                            "SELL_REPRICE_SKIPPED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_price=buy_price,
                            current_sell_price=current_sell_price,
                            adjusted_sell_price=adjusted_sell_price,
                            age_minutes=age_minutes,
                            adjusted_profit_target_pct=adjusted_profit_target,
                            reason="replace_failed"
                        )
                        continue

                    new_txid = replace_resp["result"]["txid"][0]
                    state["open_sell_orders"][new_txid] = {
                        "level": order.get("level"),
                        "volume": order["volume"],
                        "buy_price": buy_price,
                        "sell_price": adjusted_sell_price,
                        "placed_at": order.get("placed_at")
                    }
                    del state["open_sell_orders"][txid]
                    save_state(state)
                    actions.append("sell_repriced")

                    log_and_console(
                        "SELL_ORDER_REPRICED",
                        message=(
                            f"SELL repriced from "
                            f"{round(current_sell_price, PRICE_DECIMALS)} to "
                            f"{round(adjusted_sell_price, PRICE_DECIMALS)}"
                        ),
                        cycle_id=cycle_id,
                        old_txid=txid,
                        txid=new_txid,
                        level=order.get("level"),
                        volume=order.get("volume"),
                        buy_price=buy_price,
                        old_sell_price=current_sell_price,
                        sell_price=adjusted_sell_price,
                        age_minutes=age_minutes,
                        adjusted_profit_target_pct=adjusted_profit_target
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
                sell_price = compute_sell_target_price(buy_price)
                placed_at = parse_iso8601(order.get("placed_at"))
                hold_minutes = None
                if placed_at is not None:
                    hold_minutes = (
                        now - placed_at
                    ).total_seconds() / 60

                state["stats"]["buy_orders_filled"] += 1
                actions.append("buy_filled")

                log_and_console(
                    "BUY_ORDER_FILLED",
                    message=f"BUY filled @ {round(buy_price, PRICE_DECIMALS)}",
                    cycle_id=cycle_id,
                    txid=txid,
                    level=level,
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(buy_price, PRICE_DECIMALS),
                    hold_minutes=hold_minutes
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
                state["stats"]["sell_orders_placed"] += 1
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
                elif grid_anchor == "median" and median is not None:
                    grid = compute_grid(low, high, median)
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
                    time.sleep(price_check_interval_seconds)
                    continue

                usd = float(bal["result"].get("ZUSD", 0))
                reserved_sell_levels = {
                    sell_order.get("level")
                    for sell_order in state["open_sell_orders"].values()
                }
                deployed_inventory_usd = current_inventory_usd(price)

                for level in grid:
                    key = str(level)
                    skip_reason = None

                    if key in state["open_buy_orders"]:
                        skip_reason = "open_buy_order"
                    elif key in reserved_sell_levels:
                        skip_reason = "open_sell_order"
                    elif price > level:
                        skip_reason = "price_above_level"
                    elif len(state["open_sell_orders"]) >= max_open_sell_orders:
                        skip_reason = "max_open_sell_orders"
                    elif deployed_inventory_usd >= max_inventory_usd:
                        skip_reason = "max_inventory_usd"

                    volume = (usd * position_size_pct) / level
                    projected_inventory_usd = deployed_inventory_usd + (
                        level * volume
                    )

                    if (
                        skip_reason is None
                        and projected_inventory_usd > max_inventory_usd
                    ):
                        skip_reason = "max_inventory_usd"

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
                            deployed_inventory_usd=deployed_inventory_usd,
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
                        range_mean=mean,
                        range_median=median
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
                    state["stats"]["buy_orders_placed"] += 1
                    save_state(state)
                    actions.append("buy_placed")
                    deployed_inventory_usd = projected_inventory_usd

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
                    range_mean=mean,
                    range_median=median,
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
                range_median=median,
                grid_anchor=grid_anchor,
                grid_levels=(
                    [round(level, PRICE_DECIMALS) for level in grid]
                    if low and high and execution_signal >= execution_signal_threshold
                    else []
                ),
                deployed_inventory_usd=round(
                    current_inventory_usd(price), 8
                ),
                open_buy_count=len(state["open_buy_orders"]),
                open_sell_count=len(state["open_sell_orders"]),
                open_buy_volume=sum(
                    order.get("volume", 0) or 0
                    for order in state["open_buy_orders"].values()
                ),
                open_sell_volume=sum(
                    order.get("volume", 0) or 0
                    for order in state["open_sell_orders"].values()
                ),
                buy_orders_placed=state["stats"]["buy_orders_placed"],
                buy_orders_filled=state["stats"]["buy_orders_filled"],
                sell_orders_placed=state["stats"]["sell_orders_placed"],
                sell_orders_filled=state["stats"]["sell_orders_filled"],
                realized_gross_pnl=round(
                    state["stats"]["realized_gross_pnl"], 8
                ),
                realized_estimated_net_pnl=round(
                    state["stats"]["realized_estimated_net_pnl"], 8
                ),
                actions=actions or ["no_action"]
            )

            time.sleep(price_check_interval_seconds)
        except Exception as e:
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            time.sleep(price_check_interval_seconds)


if __name__ == "__main__":
    main()
