#!/usr/bin/env python3

# =====================================================
# KRAKEN SENTIMENT EXECUTOR
# =====================================================

import base64
import csv
import hashlib
import hmac
import json
import math
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

CONFIG_FILE = os.getenv("BOT_CONFIG_FILE", "sentiment_bot_config.json")
STATE_FILE = os.getenv("BOT_STATE_FILE", "sentiment_state.json")
LOG_FILE = os.getenv("TRADE_LOG_FILE", "sentiment_trade_log.jsonl")
DECISION_CSV_FILE = os.getenv(
    "SENTIMENT_DECISION_CSV_FILE",
    "sentiment_decisions.csv"
)

KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
SIGNAL_FILE = os.getenv("SIGNAL_FILE")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}

    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


config = load_config()


def env_bool(name, default):
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in ("1", "true", "yes", "on")


def env_float(name, default):
    value = os.getenv(name)
    return default if value is None else float(value)


def env_int(name, default):
    value = os.getenv(name)
    return default if value is None else int(value)


REQUEST_TIMEOUT = env_int(
    "REQUEST_TIMEOUT_SECONDS",
    config.get("request_timeout_seconds", 10)
)
KRAKEN_NONCE_RETRIES = env_int(
    "KRAKEN_NONCE_RETRIES",
    config.get("kraken_nonce_retries", 2)
)
PRICE_CHECK_INTERVAL_SECONDS = env_int(
    "PRICE_CHECK_INTERVAL_SECONDS",
    config.get("price_check_interval_seconds", 60)
)
MIN_TRADE_USD = env_float(
    "MIN_TRADE_USD",
    config.get("min_trade_usd", 30)
)
CONF_THRESHOLD = env_float(
    "CONFIDENCE_THRESHOLD",
    config.get("confidence_threshold", 0.45)
)
CONFIDENCE_WEIGHTING = env_bool(
    "CONFIDENCE_WEIGHTING",
    config.get("confidence_weighting", True)
)
DRY_RUN = env_bool("DRY_RUN", config.get("dry_run", False))
EXECUTION_BUFFER_PCT = env_float(
    "EXECUTION_BUFFER_PCT",
    config.get("execution_buffer_pct", 0.0025)
)
REBALANCE_COOLDOWN_MINUTES = env_float(
    "REBALANCE_COOLDOWN_MINUTES",
    config.get("rebalance_cooldown_minutes", 15)
)
COOLDOWN_OVERRIDE_SIGNAL_ABS = env_float(
    "COOLDOWN_OVERRIDE_SIGNAL_ABS",
    config.get("cooldown_override_signal_abs", 0.20)
)
SENTIMENT_BUY_THRESHOLD = env_float(
    "SENTIMENT_BUY_THRESHOLD",
    config.get("sentiment_buy_threshold", 0.03)
)
POSITION_SIZE_PCT = env_float(
    "POSITION_SIZE_PCT",
    config.get("position_size_pct", 0.10)
)
MAX_TRADE_USD = env_float(
    "MAX_TRADE_USD",
    config.get("max_trade_usd", 0)
)
TARGET_PROFIT_PCT = env_float(
    "TARGET_PROFIT_PCT",
    config.get("target_profit_pct", 0.006)
)
ROUND_TRIP_FEE_PCT = env_float(
    "ROUND_TRIP_FEE_PCT",
    config.get("round_trip_fee_pct", 0.0032)
)
MAX_OPEN_SELL_ORDERS = env_int(
    "MAX_OPEN_SELL_ORDERS",
    config.get("max_open_sell_orders", 1)
)
MAX_INVENTORY_USD = env_float(
    "MAX_INVENTORY_USD",
    config.get("max_inventory_usd", 250)
)
PREVENT_BUY_ABOVE_LAST_SELL = env_bool(
    "PREVENT_BUY_ABOVE_LAST_SELL",
    config.get("prevent_buy_above_last_sell", True)
)
BUY_AFTER_SELL_DISCOUNT_PCT = env_float(
    "BUY_AFTER_SELL_DISCOUNT_PCT",
    config.get("buy_after_sell_discount_pct", 0.0)
)
HIGH_PRICE_BUY_BLOCK_PCT = env_float(
    "HIGH_PRICE_BUY_BLOCK_PCT",
    config.get("high_price_buy_block_pct", 0.0005)
)

PAIR_INFO_CACHE = None

DECISION_CSV_FIELDS = [
    "ts",
    "cycle_id",
    "event",
    "side",
    "reason",
    "price",
    "execution_signal",
    "smoothed_signal",
    "weighted_signal",
    "confidence",
    "volume",
    "trade_value",
    "base_balance",
    "quote_balance",
    "portfolio_value",
    "dry_run",
    "order_txid",
    "order_status",
    "fill_volume",
    "fill_cost",
    "fill_fee",
    "fill_price",
    "result",
    "elapsed_minutes",
    "cooldown_minutes",
    "cooldown_override_signal_abs",
    "sentiment_buy_threshold",
    "target_profit_pct",
    "round_trip_fee_pct",
    "last_sell_price",
    "max_rebuy_price",
    "open_sell_count",
    "deployed_inventory_usd",
    "max_inventory_usd",
    "price_high",
    "high_price_buy_block_pct",
    "max_high_buy_price"
]

# ----------------------
# LOGGING
# ----------------------


def console(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def log_event(event, message="", **kwargs):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "message": message
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
        console(f"LOG_WRITE_ERROR: {e}")


def log_and_console(event, message="", **kwargs):
    log_event(event, message=message, **kwargs)

    if message:
        console(f"{event}: {message}")
    else:
        console(event)


# ----------------------
# STATE
# ----------------------


def load_state():
    default = {
        "signal_memory": [0, 0],
        "last_cycle": None,
        "stats": {
            "cycles": 0,
            "trades_executed": 0,
            "dry_run_trades": 0,
            "skips": 0,
            "errors": 0,
            "buy_orders_placed": 0,
            "buy_orders_filled": 0,
            "sell_orders_placed": 0,
            "sell_orders_filled": 0
        },
        "last_nonce": 0,
        "last_trade_at": None,
        "trade_history": [],
        "open_buy_orders": {},
        "open_sell_orders": {},
        "last_sell_price": None,
        "last_sell_at": None
    }

    if not os.path.exists(STATE_FILE):
        return default

    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)

    for key, default_value in default.items():
        state.setdefault(key, default_value)

    state.setdefault("stats", {})
    for key, default_value in default["stats"].items():
        state["stats"].setdefault(key, default_value)

    if not isinstance(state.get("signal_memory"), list):
        state["signal_memory"] = [0, 0]
    state["signal_memory"] = (state["signal_memory"] + [0, 0])[:2]
    if not isinstance(state.get("trade_history"), list):
        state["trade_history"] = []
    if not isinstance(state.get("open_buy_orders"), dict):
        state["open_buy_orders"] = {}
    if not isinstance(state.get("open_sell_orders"), dict):
        state["open_sell_orders"] = {}

    return state


def save_state(state):
    state_dir = os.path.dirname(STATE_FILE)
    if state_dir:
        os.makedirs(state_dir, exist_ok=True)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


state = load_state()


def parse_iso8601(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def append_decision_csv(event, **kwargs):
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event
    }
    row.update(kwargs)

    csv_dir = os.path.dirname(DECISION_CSV_FILE)
    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)

    write_header = (
        not os.path.exists(DECISION_CSV_FILE)
        or os.path.getsize(DECISION_CSV_FILE) == 0
    )

    csv_row = {
        field: row.get(field, "")
        for field in DECISION_CSV_FIELDS
    }

    if isinstance(csv_row.get("result"), (dict, list)):
        csv_row["result"] = json.dumps(csv_row["result"])

    try:
        with open(DECISION_CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DECISION_CSV_FIELDS)

            if write_header:
                writer.writeheader()

            writer.writerow(csv_row)
    except Exception as e:
        log_event("CSV_WRITE_ERROR", message=str(e), file=DECISION_CSV_FILE)


def record_trade_history(entry):
    state.setdefault("trade_history", [])
    state["trade_history"].append(entry)
    state["trade_history"] = state["trade_history"][-250:]
    save_state(state)

# ----------------------
# SAFE REQUEST HELPERS
# ----------------------


def require_runtime_config():
    required = {
        "KRAKEN_API_KEY": KRAKEN_API_KEY,
        "KRAKEN_API_SECRET": KRAKEN_API_SECRET,
        "KRAKEN_API_URL": KRAKEN_API_URL
    }
    missing = [name for name, value in required.items() if not value]

    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")

    if not LLM_SIGNAL_URL and not SIGNAL_FILE:
        raise RuntimeError("Missing LLM_SIGNAL_URL or SIGNAL_FILE in .env")


def next_nonce():
    wall_nonce = int(time.time() * 1000)
    last_nonce = int(state.get("last_nonce", 0))
    nonce = max(wall_nonce, last_nonce + 1)
    state["last_nonce"] = nonce
    save_state(state)
    return str(nonce)


def kraken_signature(endpoint, data):
    postdata = "&".join([f"{k}={v}" for k, v in data.items()])
    encoded = (str(data["nonce"]) + postdata).encode()
    message = endpoint.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(
        base64.b64decode(KRAKEN_API_SECRET),
        message,
        hashlib.sha512
    )
    return base64.b64encode(mac.digest()).decode()


def kraken_private(endpoint, data):
    url = KRAKEN_API_URL.rstrip("/") + endpoint
    payload = dict(data)
    payload["nonce"] = next_nonce()

    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": kraken_signature(endpoint, payload)
    }

    r = requests.post(
        url,
        headers=headers,
        data=payload,
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    result = r.json()
    if result.get("error"):
        raise RuntimeError(result["error"])

    return result


def safe_kraken_private(label, endpoint, data=None):
    attempts = max(1, KRAKEN_NONCE_RETRIES + 1)

    for attempt in range(1, attempts + 1):
        try:
            return kraken_private(endpoint, data or {})
        except Exception as e:
            message = str(e)
            log_event(
                "KRAKEN_EXCEPTION",
                operation=label,
                message=message,
                attempt=attempt
            )

            if "Invalid nonce" not in message or attempt >= attempts:
                return None

            state["last_nonce"] = max(
                int(state.get("last_nonce", 0)),
                int(time.time() * 1000)
            ) + 1000
            save_state(state)
            time.sleep(1)

    return None


# ----------------------
# MARKET DATA
# ----------------------


def get_price():
    try:
        if KRAKEN_TICKER_URL:
            r = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()

            if data.get("error"):
                raise RuntimeError(data["error"])

            ticker = next(iter(data["result"].values()), None)
            if ticker:
                return float(ticker["c"][0])

        url = KRAKEN_API_URL.rstrip("/") + "/0/public/Ticker"
        r = requests.get(
            url,
            params={"pair": KRAKEN_PAIR},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        data = r.json()

        if data.get("error"):
            raise RuntimeError(data["error"])

        ticker = next(iter(data["result"].values()), None)
        if not ticker:
            raise RuntimeError(f"Ticker data not found for {KRAKEN_PAIR}")

        return float(ticker["c"][0])
    except Exception as e:
        log_event("PRICE_ERROR", message=str(e))
        return None


def get_pair_info():
    global PAIR_INFO_CACHE

    if PAIR_INFO_CACHE:
        return PAIR_INFO_CACHE

    url = KRAKEN_API_URL.rstrip("/") + "/0/public/AssetPairs"
    r = requests.get(
        url,
        params={"pair": KRAKEN_PAIR},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    pair_info = next(iter(data["result"].values()), None)
    if not pair_info:
        raise RuntimeError(f"Pair metadata not found for {KRAKEN_PAIR}")

    PAIR_INFO_CACHE = pair_info
    return PAIR_INFO_CACHE


# ----------------------
# BALANCES
# ----------------------


def get_balances():
    result = safe_kraken_private("BALANCE", "/0/private/Balance")
    if not result:
        return None

    pair_info = get_pair_info()
    base_asset = pair_info["base"]
    quote_asset = pair_info["quote"]

    base_balance = float(result["result"].get(base_asset, 0))
    quote_balance = float(result["result"].get(quote_asset, 0))

    return base_balance, quote_balance


def round_volume(volume):
    pair_info = get_pair_info()
    lot_decimals = int(pair_info.get("lot_decimals", 8))
    factor = 10 ** lot_decimals
    return math.floor(volume * factor) / factor


def round_price(price):
    pair_info = get_pair_info()
    pair_decimals = int(pair_info.get("pair_decimals", 1))
    return round(price, pair_decimals)


def get_min_order_volume():
    pair_info = get_pair_info()
    return float(pair_info.get("ordermin") or 0)


def query_order(txid):
    result = safe_kraken_private(
        "QUERY_ORDER",
        "/0/private/QueryOrders",
        {"txid": txid}
    )

    if not result:
        return None

    return result.get("result", {}).get(txid)


def order_fill_summary(order):
    if not order:
        return {}

    return {
        "order_status": order.get("status"),
        "fill_volume": order.get("vol_exec"),
        "fill_cost": order.get("cost"),
        "fill_fee": order.get("fee"),
        "fill_price": order.get("price")
    }


# ----------------------
# SIGNAL
# ----------------------


def load_signal():
    try:
        if LLM_SIGNAL_URL:
            r = requests.get(LLM_SIGNAL_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            signal = r.json()
        else:
            with open(SIGNAL_FILE, encoding="utf-8") as f:
                signal = json.load(f)

        return signal
    except Exception as e:
        log_event("SENTIMENT_ERROR", message=str(e))
        return None


def normalize_signal(signal):
    if not isinstance(signal, dict):
        return {
            "execution_signal": float(signal),
            "confidence": 1.0,
            "price_regime": {}
        }

    price_regime = signal.get("price_regime")
    if not isinstance(price_regime, dict):
        price_regime = {}

    return {
        "execution_signal": float(signal.get("execution_signal", 0)),
        "confidence": float(signal.get("confidence", 0)),
        "btc_sentiment": signal.get("btc_sentiment"),
        "regulatory_risk": signal.get("regulatory_risk"),
        "macro_tightening_bias": signal.get("macro_tightening_bias"),
        "direction_bias": signal.get("direction_bias"),
        "raw_btc_sentiment": signal.get("raw_btc_sentiment"),
        "raw_confidence": signal.get("raw_confidence"),
        "raw_direction_bias": signal.get("raw_direction_bias"),
        "fear_greed_index": signal.get("fear_greed_index"),
        "processed_at": signal.get("processed_at"),
        "price_regime": price_regime
    }


def smooth_signal(current_signal):
    prev1, prev2 = state["signal_memory"]
    smoothed = (
        0.6 * current_signal
        + 0.3 * prev1
        + 0.1 * prev2
    )

    state["signal_memory"] = [current_signal, prev1]
    save_state(state)

    return smoothed


# ----------------------
# ORDER EXECUTION
# ----------------------


def fill_values(order, fallback_volume=None, fallback_price=None):
    if not order:
        return {
            "volume": fallback_volume,
            "price": fallback_price,
            "cost": None,
            "fee": None
        }

    fill_volume = float(order.get("vol_exec") or order.get("vol") or 0)
    fill_cost = float(order.get("cost") or 0)
    fill_fee = float(order.get("fee") or 0)
    fill_price = float(order.get("price") or 0)

    if fill_price <= 0 and fill_cost > 0 and fill_volume > 0:
        fill_price = fill_cost / fill_volume

    return {
        "volume": fill_volume or fallback_volume,
        "price": fill_price or fallback_price,
        "cost": fill_cost or None,
        "fee": fill_fee or None
    }


def sell_target_price(buy_price):
    return buy_price * (1 + TARGET_PROFIT_PCT + ROUND_TRIP_FEE_PCT)


def place_market_buy(volume, cycle_id):
    if DRY_RUN:
        state["stats"]["dry_run_trades"] += 1
        state["stats"]["buy_orders_placed"] += 1
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": True,
                "side": "buy",
                "volume": volume
            }
        )
        log_and_console(
            "DRY_RUN_BUY",
            message=f"buy {volume}",
            cycle_id=cycle_id,
            side="buy",
            volume=volume
        )
        return {"dry_run": True}

    result = safe_kraken_private(
        "BUY",
        "/0/private/AddOrder",
        {
            "pair": KRAKEN_PAIR,
            "type": "buy",
            "ordertype": "market",
            "volume": volume
        }
    )

    if result:
        txids = result.get("result", {}).get("txid", [])
        txid = txids[0] if txids else None
        fill = order_fill_summary(query_order(txid)) if txid else {}
        state["stats"]["trades_executed"] += 1
        state["stats"]["buy_orders_placed"] += 1
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": False,
                "side": "buy",
                "volume": volume,
                "txid": txid,
                **fill
            }
        )
        log_and_console(
            "BUY_ORDER_PLACED",
            message=f"buy {volume}",
            cycle_id=cycle_id,
            side="buy",
            volume=volume,
            txid=txid,
            **fill,
            result=result.get("result")
        )
        result["fill"] = fill

    return result


def place_limit_sell(price, volume, cycle_id, buy_txid=None, buy_price=None):
    sell_price = round_price(price)
    sell_volume = round_volume(volume)

    if DRY_RUN:
        log_and_console(
            "DRY_RUN_SELL",
            message=f"sell {sell_volume} @ {sell_price}",
            cycle_id=cycle_id,
            side="sell",
            volume=sell_volume,
            price=sell_price,
            buy_txid=buy_txid,
            buy_price=buy_price
        )
        return {"dry_run": True}

    result = safe_kraken_private(
        "SELL",
        "/0/private/AddOrder",
        {
            "pair": KRAKEN_PAIR,
            "type": "sell",
            "ordertype": "limit",
            "price": str(sell_price),
            "volume": str(sell_volume)
        }
    )

    if result:
        txids = result.get("result", {}).get("txid", [])
        txid = txids[0] if txids else None
        if not txid:
            log_event(
                "ORDER_REJECTED",
                cycle_id=cycle_id,
                side="sell",
                reason="missing_txid",
                result=result.get("result")
            )
            return result

        state["stats"]["sell_orders_placed"] += 1
        state["open_sell_orders"][txid] = {
            "txid": txid,
            "volume": sell_volume,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "buy_txid": buy_txid,
            "placed_at": cycle_id
        }
        save_state(state)
        log_and_console(
            "SELL_ORDER_PLACED",
            message=f"sell {sell_volume} @ {sell_price}",
            cycle_id=cycle_id,
            txid=txid,
            volume=sell_volume,
            price=sell_price,
            buy_price=buy_price,
            buy_txid=buy_txid
        )

    return result


# ----------------------
# EXECUTOR
# ----------------------


def skip_cycle(reason, cycle_id, **kwargs):
    state["stats"]["skips"] += 1
    save_state(state)
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="hold",
        reason=reason,
        **kwargs
    )
    append_decision_csv(
        "hold",
        cycle_id=cycle_id,
        side="hold",
        reason=reason,
        dry_run=DRY_RUN,
        **kwargs
    )


def cooldown_active(now, weighted_signal):
    last_trade_at = parse_iso8601(state.get("last_trade_at"))
    if REBALANCE_COOLDOWN_MINUTES <= 0 or last_trade_at is None:
        return None

    elapsed_minutes = (now - last_trade_at).total_seconds() / 60
    if elapsed_minutes >= REBALANCE_COOLDOWN_MINUTES:
        return None

    if abs(weighted_signal) >= COOLDOWN_OVERRIDE_SIGNAL_ABS:
        return None

    return {
        "elapsed_minutes": elapsed_minutes,
        "cooldown_minutes": REBALANCE_COOLDOWN_MINUTES,
        "cooldown_override_signal_abs": COOLDOWN_OVERRIDE_SIGNAL_ABS
    }


def numeric_or_none(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


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


def place_profit_sell_for_buy(cycle_id, buy_txid, buy_price, volume):
    target_price = sell_target_price(buy_price)
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="sell",
        reason="profit_target_after_buy_fill",
        buy_txid=buy_txid,
        buy_price=buy_price,
        price=round_price(target_price),
        volume=round_volume(volume),
        target_profit_pct=TARGET_PROFIT_PCT,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT
    )
    append_decision_csv(
        "trade_decision",
        cycle_id=cycle_id,
        side="sell",
        reason="profit_target_after_buy_fill",
        price=round_price(target_price),
        volume=round_volume(volume),
        dry_run=DRY_RUN,
        order_txid=buy_txid
    )
    return place_limit_sell(
        target_price,
        volume,
        cycle_id,
        buy_txid=buy_txid,
        buy_price=buy_price
    )


def process_open_buy_orders(cycle_id):
    for txid, order in list(state["open_buy_orders"].items()):
        status = query_order(txid)
        if status is None:
            continue

        order_status = status.get("status")
        if order_status == "closed":
            fill = fill_values(
                status,
                fallback_volume=order.get("volume"),
                fallback_price=order.get("price")
            )
            state["stats"]["buy_orders_filled"] += 1
            del state["open_buy_orders"][txid]
            save_state(state)
            log_and_console(
                "BUY_ORDER_FILLED",
                message=f"buy filled @ {round_price(fill['price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=fill["volume"],
                price=fill["price"],
                cost=fill["cost"],
                fee=fill["fee"]
            )
            place_profit_sell_for_buy(
                cycle_id,
                txid,
                fill["price"],
                fill["volume"]
            )
        elif order_status in ("canceled", "expired"):
            del state["open_buy_orders"][txid]
            save_state(state)
            log_and_console(
                "ORDER_" + order_status.upper(),
                message=f"buy order {order_status}",
                cycle_id=cycle_id,
                txid=txid,
                side="buy"
            )


def process_open_sell_orders(cycle_id):
    for txid, order in list(state["open_sell_orders"].items()):
        status = query_order(txid)
        if status is None:
            continue

        order_status = status.get("status")
        if order_status == "closed":
            fill = fill_values(
                status,
                fallback_volume=order.get("volume"),
                fallback_price=order.get("sell_price")
            )
            state["stats"]["sell_orders_filled"] += 1
            state["last_sell_price"] = fill["price"] or order.get("sell_price")
            state["last_sell_at"] = cycle_id
            del state["open_sell_orders"][txid]
            save_state(state)
            log_and_console(
                "SELL_ORDER_FILLED",
                message=f"sell filled @ {round_price(state['last_sell_price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=fill["volume"],
                price=state["last_sell_price"],
                cost=fill["cost"],
                fee=fill["fee"],
                buy_price=order.get("buy_price")
            )
        elif order_status in ("canceled", "expired"):
            del state["open_sell_orders"][txid]
            save_state(state)
            log_and_console(
                "ORDER_" + order_status.upper(),
                message=f"sell order {order_status}",
                cycle_id=cycle_id,
                txid=txid,
                side="sell"
            )


def maybe_handle_submitted_buy(result, cycle_id, volume, price):
    if DRY_RUN:
        state["stats"]["buy_orders_filled"] += 1
        save_state(state)
        place_profit_sell_for_buy(
            cycle_id,
            "dry_run_buy",
            price,
            volume
        )
        return

    if not result:
        return

    result_payload = result.get("result", {})
    txids = result_payload.get("txid", [])
    txid = txids[0] if txids else None
    fill = result.get("fill", {})

    fill_volume = float(fill.get("fill_volume") or 0)
    fill_price = float(fill.get("fill_price") or 0)
    order_status = fill.get("order_status")

    if txid and order_status == "closed" and fill_volume > 0 and fill_price > 0:
        state["stats"]["buy_orders_filled"] += 1
        save_state(state)
        place_profit_sell_for_buy(cycle_id, txid, fill_price, fill_volume)
        return

    if txid:
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": volume,
            "price": price,
            "placed_at": cycle_id
        }
        save_state(state)


def run_cycle():
    now = datetime.now(timezone.utc)
    cycle_id = now.isoformat()
    state["last_cycle"] = cycle_id
    state["stats"]["cycles"] += 1
    save_state(state)

    price = get_price()
    if price is None:
        skip_cycle("missing_price", cycle_id)
        return

    process_open_buy_orders(cycle_id)
    process_open_sell_orders(cycle_id)

    signal = load_signal()
    if signal is None:
        skip_cycle("missing_signal", cycle_id, price=price)
        return

    sentiment = normalize_signal(signal)
    raw_signal = sentiment["execution_signal"]
    confidence = sentiment["confidence"]
    smoothed_signal = smooth_signal(raw_signal)
    weighted_signal = (
        smoothed_signal * confidence
        if CONFIDENCE_WEIGHTING
        else smoothed_signal
    )

    log_event(
        "SIGNAL_UPDATE",
        cycle_id=cycle_id,
        price=price,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        btc_sentiment=sentiment.get("btc_sentiment"),
        raw_btc_sentiment=sentiment.get("raw_btc_sentiment"),
        raw_confidence=sentiment.get("raw_confidence"),
        direction_bias=sentiment.get("direction_bias"),
        raw_direction_bias=sentiment.get("raw_direction_bias"),
        fear_greed_index=sentiment.get("fear_greed_index"),
        processed_at=sentiment.get("processed_at")
    )
    console(f"Price: {price} | Signal: {raw_signal} | Confidence: {confidence}")

    if confidence < CONF_THRESHOLD:
        skip_cycle(
            "low_confidence",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            confidence_threshold=CONF_THRESHOLD
        )
        return

    if weighted_signal < SENTIMENT_BUY_THRESHOLD:
        skip_cycle(
            "sentiment_below_buy_threshold",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD
        )
        return

    price_regime = sentiment.get("price_regime", {})
    price_high = numeric_or_none(price_regime.get("price_high_24h"))
    if price_high is not None and HIGH_PRICE_BUY_BLOCK_PCT >= 0:
        max_high_buy_price = price_high * (1 - HIGH_PRICE_BUY_BLOCK_PCT)
        if price >= max_high_buy_price:
            skip_cycle(
                "price_near_regime_high",
                cycle_id,
                price=price,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                price_high=price_high,
                high_price_buy_block_pct=HIGH_PRICE_BUY_BLOCK_PCT,
                max_high_buy_price=max_high_buy_price
            )
            return

    cooldown = cooldown_active(now, weighted_signal)
    if cooldown is not None:
        skip_cycle(
            "cooldown",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            **cooldown
        )
        return

    last_sell_price = state.get("last_sell_price")
    if PREVENT_BUY_ABOVE_LAST_SELL and last_sell_price is not None:
        max_rebuy_price = float(last_sell_price) * (
            1 - BUY_AFTER_SELL_DISCOUNT_PCT
        )
        if price >= max_rebuy_price:
            skip_cycle(
                "price_above_last_sell",
                cycle_id,
                price=price,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                last_sell_price=last_sell_price,
                max_rebuy_price=max_rebuy_price
            )
            return

    if len(state["open_sell_orders"]) >= MAX_OPEN_SELL_ORDERS:
        skip_cycle(
            "max_open_sell_orders",
            cycle_id,
            price=price,
            open_sell_count=len(state["open_sell_orders"])
        )
        return

    if current_inventory_usd(price) >= MAX_INVENTORY_USD:
        skip_cycle(
            "max_inventory_usd",
            cycle_id,
            price=price,
            deployed_inventory_usd=current_inventory_usd(price),
            max_inventory_usd=MAX_INVENTORY_USD
        )
        return

    balances = get_balances()
    if balances is None:
        skip_cycle(
            "balance_fetch_failed",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            confidence=confidence
        )
        return

    base_balance, quote_balance = balances
    trade_value = quote_balance * POSITION_SIZE_PCT
    if MAX_TRADE_USD > 0:
        trade_value = min(trade_value, MAX_TRADE_USD)
    trade_value *= max(0, 1 - EXECUTION_BUFFER_PCT)

    projected_inventory = current_inventory_usd(price) + trade_value
    if projected_inventory > MAX_INVENTORY_USD:
        trade_value = max(0, MAX_INVENTORY_USD - current_inventory_usd(price))

    if trade_value < MIN_TRADE_USD:
        skip_cycle(
            "small_trade",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            confidence=confidence,
            trade_value=trade_value,
            min_trade_usd=MIN_TRADE_USD
        )
        return

    volume = round_volume(trade_value / price)
    min_volume = get_min_order_volume()
    if volume <= 0 or volume < min_volume:
        skip_cycle(
            "below_min_volume",
            cycle_id,
            price=price,
            trade_value=trade_value,
            volume=volume,
            min_volume=min_volume
        )
        return

    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="buy",
        reason="sentiment_buy",
        volume=volume,
        price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD
    )
    append_decision_csv(
        "trade_decision",
        cycle_id=cycle_id,
        side="buy",
        reason="sentiment_buy",
        volume=volume,
        price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        dry_run=DRY_RUN
    )

    result = place_market_buy(volume, cycle_id)
    result_payload = result.get("result") if isinstance(result, dict) else None
    fill = result.get("fill", {}) if isinstance(result, dict) else {}
    txids = result_payload.get("txid", []) if isinstance(result_payload, dict) else []
    append_decision_csv(
        "trade_executed" if result else "trade_rejected",
        cycle_id=cycle_id,
        side="buy",
        reason="dry_run" if DRY_RUN else ("submitted" if result else "rejected"),
        volume=volume,
        price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        dry_run=DRY_RUN,
        order_txid=txids[0] if txids else "",
        **fill,
        result=result_payload or result
    )
    maybe_handle_submitted_buy(result, cycle_id, volume, price)

    log_event(
        "CYCLE_SUMMARY",
        cycle_id=cycle_id,
        price=price,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        side="buy",
        volume=volume,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        open_buy_count=len(state["open_buy_orders"]),
        open_sell_count=len(state["open_sell_orders"]),
        deployed_inventory_usd=current_inventory_usd(price),
        last_sell_price=state.get("last_sell_price"),
        dry_run=DRY_RUN
    )


# ----------------------
# MAIN LOOP
# ----------------------


def main():
    require_runtime_config()

    log_and_console(
        "BOT_START",
        message="Sentiment executor starting",
        config_file=CONFIG_FILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        pair=KRAKEN_PAIR,
        dry_run=DRY_RUN,
        min_trade_usd=MIN_TRADE_USD,
        confidence_threshold=CONF_THRESHOLD,
        confidence_weighting=CONFIDENCE_WEIGHTING,
        execution_buffer_pct=EXECUTION_BUFFER_PCT,
        rebalance_cooldown_minutes=REBALANCE_COOLDOWN_MINUTES,
        cooldown_override_signal_abs=COOLDOWN_OVERRIDE_SIGNAL_ABS,
        sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD,
        position_size_pct=POSITION_SIZE_PCT,
        max_trade_usd=MAX_TRADE_USD,
        target_profit_pct=TARGET_PROFIT_PCT,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        max_open_sell_orders=MAX_OPEN_SELL_ORDERS,
        max_inventory_usd=MAX_INVENTORY_USD,
        prevent_buy_above_last_sell=PREVENT_BUY_ABOVE_LAST_SELL,
        buy_after_sell_discount_pct=BUY_AFTER_SELL_DISCOUNT_PCT,
        high_price_buy_block_pct=HIGH_PRICE_BUY_BLOCK_PCT,
        decision_csv_file=DECISION_CSV_FILE,
        price_check_interval_seconds=PRICE_CHECK_INTERVAL_SECONDS
    )

    while True:
        try:
            run_cycle()
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            log_and_console("BOT_STOP", message="Sentiment executor stopped")
            break
        except Exception as e:
            state["stats"]["errors"] += 1
            save_state(state)
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
