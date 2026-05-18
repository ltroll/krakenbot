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

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

# ----------------------
# CONFIG
# ----------------------

CONFIG_FILE = (
    os.getenv("SENTIMENT_CONFIG_FILE")
    or os.getenv("BOT_CONFIG_FILE")
    or "sentiment_bot_config.json"
)
STRATEGY_PROFILE = (
    os.getenv("SENTIMENT_STRATEGY_PROFILE")
    or os.getenv("STRATEGY_PROFILE")
    or "sentiment_strategy_default.json"
)
STATE_FILE = (
    os.getenv("SENTIMENT_STATE_FILE")
    or os.getenv("BOT_STATE_FILE")
    or "sentiment_state.json"
)
LOG_FILE = (
    os.getenv("SENTIMENT_TRADE_LOG_FILE")
    or os.getenv("TRADE_LOG_FILE")
    or "sentiment_trade_log.jsonl"
)
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


def load_json_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def strategy_profile_is_file():
    if not STRATEGY_PROFILE:
        return False

    expanded_path = os.path.expanduser(STRATEGY_PROFILE)
    return (
        os.path.exists(expanded_path)
        or STRATEGY_PROFILE.endswith(".json")
        or os.sep in STRATEGY_PROFILE
    )


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}

    return load_json_file(CONFIG_FILE)


def select_strategy_profile(config_data):
    profiles = config_data.get("strategy_profiles")
    if profiles is None:
        return config_data

    if not isinstance(profiles, dict):
        raise RuntimeError(f"{CONFIG_FILE} strategy_profiles must be an object")

    profile = profiles.get(STRATEGY_PROFILE)
    if profile is None:
        available = ", ".join(sorted(profiles)) or "<none>"
        raise RuntimeError(
            f"Strategy profile '{STRATEGY_PROFILE}' not found in "
            f"{CONFIG_FILE}. Available profiles: {available}"
        )

    return profile


def load_strategy_config():
    if strategy_profile_is_file():
        path = os.path.expanduser(STRATEGY_PROFILE)
        if not os.path.exists(path):
            raise RuntimeError(f"Strategy profile file not found: {path}")

        return load_json_file(path)

    return select_strategy_profile(load_config())


strategy_config = load_strategy_config()


def profile_bool(name, default):
    value = strategy_config.get(name, default)
    if value is None:
        return default

    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in ("1", "true", "yes", "on")


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


def profile_str(name, default=""):
    value = strategy_config.get(name, default)
    return default if value is None else str(value)


REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
KRAKEN_NONCE_RETRIES = int(os.getenv("KRAKEN_NONCE_RETRIES", "2"))
ORDER_TRACKER_URL = (
    os.getenv("ORDER_TRACKER_URL")
    or profile_str("order_tracker_url")
    or profile_str("external_order_tracker_url")
)
ORDER_TRACKER_USER_AGENT = (
    os.getenv("ORDER_TRACKER_USER_AGENT")
    or profile_str("order_tracker_user_agent", "mean-reversion-bot/1.0")
)
ORDER_TRACKER_SYMBOL = (
    os.getenv("ORDER_TRACKER_SYMBOL")
    or profile_str("order_tracker_symbol", KRAKEN_PAIR)
)
ORDER_TRACKER_TIMEOUT = profile_float("order_tracker_timeout_seconds", 5)
PRICE_CHECK_INTERVAL_SECONDS = profile_int("price_check_interval_seconds", 60)
MIN_TRADE_USD = profile_float("min_trade_usd", 30)
CONF_THRESHOLD = profile_float("confidence_threshold", 0.45)
CONFIDENCE_WEIGHTING = profile_bool("confidence_weighting", True)
DRY_RUN = profile_bool("dry_run", False)
EXECUTION_BUFFER_PCT = profile_float("execution_buffer_pct", 0.0025)
REBALANCE_COOLDOWN_MINUTES = profile_float("rebalance_cooldown_minutes", 15)
COOLDOWN_OVERRIDE_SIGNAL_ABS = profile_float("cooldown_override_signal_abs", 0.20)
SENTIMENT_BUY_THRESHOLD = profile_float("sentiment_buy_threshold", 0.03)
POSITION_SIZE_PCT = profile_float("position_size_pct", 0.10)
MAX_TRADE_USD = profile_float("max_trade_usd", 0)
TARGET_PROFIT_PCT = profile_float("target_profit_pct", 0.006)
ROUND_TRIP_FEE_PCT = profile_float("round_trip_fee_pct", 0.0032)
DYNAMIC_PROFIT_TARGETS = profile_bool("dynamic_profit_targets", False)
MIN_TARGET_PROFIT_PCT = profile_float("min_target_profit_pct", TARGET_PROFIT_PCT)
BASE_TARGET_PROFIT_PCT = profile_float("base_target_profit_pct", TARGET_PROFIT_PCT)
MAX_TARGET_PROFIT_PCT = profile_float("max_target_profit_pct", TARGET_PROFIT_PCT)
DYNAMIC_PROFIT_LOW_VOLATILITY_PCT = profile_float(
    "dynamic_profit_low_volatility_pct",
    0.025
)
DYNAMIC_PROFIT_HIGH_VOLATILITY_PCT = profile_float(
    "dynamic_profit_high_volatility_pct",
    0.06
)
MAX_OPEN_SELL_ORDERS = profile_int("max_open_sell_orders", 1)
MAX_INVENTORY_USD = profile_float("max_inventory_usd", 250)
PREVENT_BUY_ABOVE_LAST_SELL = profile_bool("prevent_buy_above_last_sell", True)
BUY_AFTER_SELL_DISCOUNT_PCT = profile_float("buy_after_sell_discount_pct", 0.0)
HIGH_PRICE_BUY_BLOCK_PCT = profile_float("high_price_buy_block_pct", 0.0005)
USE_SIGNAL_STATUS_GATES = profile_bool("use_signal_status_gates", True)
REQUIRE_BOT_ACTION_ALLOWED = profile_bool("require_bot_action_allowed", True)
MAX_SIGNAL_AGE_MINUTES = profile_float("max_signal_age_minutes", 30)
CRITICAL_SOURCE_STATUSES = [
    source.strip()
    for source in profile_str(
        "critical_source_statuses",
        "market_data,price_regime,kraken_flow"
    ).split(",")
    if source.strip()
]
USE_RISK_MULTIPLIER = profile_bool("use_risk_multiplier", True)
MIN_RISK_MULTIPLIER = profile_float("min_risk_multiplier", 0.25)
MAX_RISK_MULTIPLIER = profile_float("max_risk_multiplier", 1.25)
ENABLE_TARGET_LIMIT_BUYS = profile_bool("enable_target_limit_buys", True)
MAX_OPEN_BUY_ORDERS = profile_int("max_open_buy_orders", 2)
MAX_TARGET_LIMIT_ORDERS_PER_CYCLE = profile_int(
    "max_target_limit_orders_per_cycle",
    2
)
TARGET_LIMIT_MAX_PREMIUM_PCT = profile_float(
    "target_limit_max_premium_pct",
    0.0005
)
MEAN_REVERSION_BUY_THRESHOLD = profile_float(
    "mean_reversion_buy_threshold",
    0.35
)
MEAN_REVERSION_RANGE_POSITION_MAX = profile_float(
    "mean_reversion_range_position_max",
    0.15
)
MEAN_REVERSION_FLOW_PRESSURE_MIN = profile_float(
    "mean_reversion_flow_pressure_min",
    0.0
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
    "gross_target_pct",
    "last_sell_price",
    "max_rebuy_price",
    "open_sell_count",
    "deployed_inventory_usd",
    "max_inventory_usd",
    "risk_multiplier",
    "effective_risk_multiplier",
    "signal_status",
    "bot_action_allowed",
    "signal_age_minutes",
    "source_status_result",
    "mean_reversion_opportunity",
    "range_position_24h",
    "flow_pressure",
    "target_buy_price",
    "target_allocation_pct",
    "open_buy_count",
    "max_open_buy_orders",
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


def notify_order_tracker(
    trade_id,
    side,
    price,
    quantity,
    order_id=None,
    fee=None,
    timestamp=None,
    notes=None
):
    if not ORDER_TRACKER_URL:
        return

    if not trade_id or side not in ("buy", "sell") or price is None or quantity is None:
        log_event(
            "ORDER_TRACKER_SKIPPED",
            reason="missing_required_fields",
            trade_id=trade_id,
            side=side,
            price=price,
            quantity=quantity,
            order_id=order_id
        )
        return

    payload = {
        "trade_id": str(trade_id),
        "side": side,
        "price": str(price),
        "quantity": str(quantity)
    }
    optional_fields = {
        "symbol": ORDER_TRACKER_SYMBOL,
        "order_id": order_id,
        "fee": fee,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "notes": notes
    }
    payload.update(
        {
            key: str(value)
            for key, value in optional_fields.items()
            if value is not None and value != ""
        }
    )

    try:
        response = requests.post(
            ORDER_TRACKER_URL,
            data=payload,
            headers={"User-Agent": ORDER_TRACKER_USER_AGENT},
            timeout=ORDER_TRACKER_TIMEOUT
        )
        response.raise_for_status()
        log_event(
            "ORDER_TRACKER_UPDATED",
            trade_id=trade_id,
            side=side,
            order_id=order_id,
            status_code=response.status_code
        )
    except Exception as e:
        log_event(
            "ORDER_TRACKER_ERROR",
            message=str(e),
            trade_id=trade_id,
            side=side,
            order_id=order_id
        )


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
        if isinstance(value, str) and value.endswith("Z"):
            value = value[:-1] + "+00:00"
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

    kraken_flow = signal.get("kraken_flow")
    if not isinstance(kraken_flow, dict):
        kraken_flow = {}

    source_status = signal.get("source_status")
    if not isinstance(source_status, dict):
        source_status = {}

    target_prices = signal.get("target_prices")
    if not isinstance(target_prices, list):
        target_prices = []

    return {
        "execution_signal": float(signal.get("execution_signal", 0)),
        "confidence": float(signal.get("confidence", 0)),
        "btc_sentiment": signal.get("btc_sentiment"),
        "regulatory_risk": signal.get("regulatory_risk"),
        "macro_tightening_bias": signal.get("macro_tightening_bias"),
        "direction_bias": signal.get("direction_bias"),
        "risk_multiplier": signal.get("risk_multiplier"),
        "smoothed_risk_multiplier": signal.get("smoothed_risk_multiplier"),
        "mean_reversion_opportunity": signal.get("mean_reversion_opportunity"),
        "flow_pressure": signal.get("flow_pressure"),
        "raw_btc_sentiment": signal.get("raw_btc_sentiment"),
        "raw_confidence": signal.get("raw_confidence"),
        "raw_direction_bias": signal.get("raw_direction_bias"),
        "fear_greed_index": signal.get("fear_greed_index"),
        "signal_status": signal.get("signal_status"),
        "bot_action_allowed": signal.get("bot_action_allowed"),
        "reason": signal.get("reason"),
        "processed_at": signal.get("processed_at"),
        "price_regime": price_regime,
        "kraken_flow": kraken_flow,
        "source_status": source_status,
        "target_prices": target_prices
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


def sell_target_price(buy_price, target_profit_pct=None):
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )
    return buy_price * (1 + profit_pct + ROUND_TRIP_FEE_PCT)


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


def place_limit_buy(price, volume, cycle_id, target_profit_pct=None):
    buy_price = round_price(price)
    buy_volume = round_volume(volume)
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )

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
                "ordertype": "limit",
                "volume": buy_volume,
                "price": buy_price,
                "target_profit_pct": profit_pct,
                "round_trip_fee_pct": ROUND_TRIP_FEE_PCT
            }
        )
        log_and_console(
            "DRY_RUN_LIMIT_BUY",
            message=f"buy {buy_volume} @ {buy_price}",
            cycle_id=cycle_id,
            side="buy",
            volume=buy_volume,
            price=buy_price,
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT
        )
        return {"dry_run": True}

    result = safe_kraken_private(
        "LIMIT_BUY",
        "/0/private/AddOrder",
        {
            "pair": KRAKEN_PAIR,
            "type": "buy",
            "ordertype": "limit",
            "price": str(buy_price),
            "volume": str(buy_volume)
        }
    )

    if result:
        txids = result.get("result", {}).get("txid", [])
        txid = txids[0] if txids else None
        if not txid:
            log_event(
                "ORDER_REJECTED",
                cycle_id=cycle_id,
                side="buy",
                reason="missing_txid",
                result=result.get("result")
            )
            return result

        state["stats"]["trades_executed"] += 1
        state["stats"]["buy_orders_placed"] += 1
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": buy_volume,
            "price": buy_price,
            "target_profit_pct": profit_pct,
            "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
            "placed_at": cycle_id,
            "trade_id": txid
        }
        save_state(state)
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": False,
                "side": "buy",
                "ordertype": "limit",
                "volume": buy_volume,
                "price": buy_price,
                "target_profit_pct": profit_pct,
                "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
                "txid": txid
            }
        )
        log_and_console(
            "BUY_LIMIT_ORDER_PLACED",
            message=f"buy {buy_volume} @ {buy_price}",
            cycle_id=cycle_id,
            side="buy",
            volume=buy_volume,
            price=buy_price,
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT,
            txid=txid,
            result=result.get("result")
        )
        notify_order_tracker(
            trade_id=txid,
            side="buy",
            price=buy_price,
            quantity=buy_volume,
            order_id=txid,
            timestamp=cycle_id,
            notes="limit_buy_submitted"
        )

    return result


def place_limit_sell(
    price,
    volume,
    cycle_id,
    buy_txid=None,
    buy_price=None,
    target_profit_pct=None
):
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
            buy_price=buy_price,
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=(
                target_profit_pct + ROUND_TRIP_FEE_PCT
                if target_profit_pct is not None
                else None
            )
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
            "target_profit_pct": target_profit_pct,
            "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
            "placed_at": cycle_id,
            "trade_id": buy_txid or txid
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
            buy_txid=buy_txid,
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=(
                target_profit_pct + ROUND_TRIP_FEE_PCT
                if target_profit_pct is not None
                else None
            )
        )
        notify_order_tracker(
            trade_id=buy_txid or txid,
            side="sell",
            price=sell_price,
            quantity=sell_volume,
            order_id=txid,
            timestamp=cycle_id
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


def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def signal_age_minutes(sentiment, now):
    processed_at = parse_iso8601(sentiment.get("processed_at"))
    if processed_at is None:
        return None

    return (now - processed_at).total_seconds() / 60


def signal_gate_failure(sentiment, now):
    if not USE_SIGNAL_STATUS_GATES:
        return None

    signal_status = sentiment.get("signal_status")
    if signal_status and signal_status != "fresh":
        return {
            "reason": "signal_not_fresh",
            "signal_status": signal_status,
            "bot_action_allowed": sentiment.get("bot_action_allowed"),
            "source_status_result": signal_status
        }

    if (
        REQUIRE_BOT_ACTION_ALLOWED
        and sentiment.get("bot_action_allowed") is False
    ):
        return {
            "reason": "bot_action_not_allowed",
            "signal_status": signal_status,
            "bot_action_allowed": sentiment.get("bot_action_allowed"),
            "source_status_result": sentiment.get("reason")
        }

    age_minutes = signal_age_minutes(sentiment, now)
    if (
        MAX_SIGNAL_AGE_MINUTES > 0
        and age_minutes is not None
        and age_minutes > MAX_SIGNAL_AGE_MINUTES
    ):
        return {
            "reason": "signal_too_old",
            "signal_status": signal_status,
            "bot_action_allowed": sentiment.get("bot_action_allowed"),
            "signal_age_minutes": age_minutes
        }

    source_status = sentiment.get("source_status", {})
    for source in CRITICAL_SOURCE_STATUSES:
        status = source_status.get(source, {})
        if not isinstance(status, dict):
            continue
        if status.get("status") not in (None, "fresh", "not_configured"):
            return {
                "reason": "critical_source_not_fresh",
                "signal_status": signal_status,
                "bot_action_allowed": sentiment.get("bot_action_allowed"),
                "source_status_result": f"{source}:{status.get('status')}"
            }

    return None


def effective_risk_multiplier(sentiment):
    if not USE_RISK_MULTIPLIER:
        return 1.0

    multiplier = numeric_or_none(sentiment.get("smoothed_risk_multiplier"))
    if multiplier is None:
        multiplier = numeric_or_none(sentiment.get("risk_multiplier"))
    if multiplier is None:
        return 1.0

    return clamp(multiplier, MIN_RISK_MULTIPLIER, MAX_RISK_MULTIPLIER)


def mean_reversion_setup_allowed(sentiment):
    price_regime = sentiment.get("price_regime", {})
    kraken_flow = sentiment.get("kraken_flow", {})

    opportunity = numeric_or_none(sentiment.get("mean_reversion_opportunity"))
    if opportunity is None:
        opportunity = numeric_or_none(
            price_regime.get("mean_reversion_opportunity")
        )

    range_position = numeric_or_none(price_regime.get("range_position_24h"))

    flow_pressure = numeric_or_none(sentiment.get("flow_pressure"))
    if flow_pressure is None:
        flow_pressure = numeric_or_none(kraken_flow.get("aggression_score"))

    if opportunity is None or range_position is None or flow_pressure is None:
        return False

    return (
        opportunity >= MEAN_REVERSION_BUY_THRESHOLD
        and range_position <= MEAN_REVERSION_RANGE_POSITION_MAX
        and flow_pressure >= MEAN_REVERSION_FLOW_PRESSURE_MIN
    )


def dynamic_target_profit_pct(sentiment, weighted_signal):
    if not DYNAMIC_PROFIT_TARGETS:
        return TARGET_PROFIT_PCT

    price_regime = sentiment.get("price_regime", {})
    kraken_flow = sentiment.get("kraken_flow", {})
    target = BASE_TARGET_PROFIT_PCT

    range_position = numeric_or_none(price_regime.get("range_position_24h"))
    if range_position is not None:
        if range_position <= 0.10:
            target += 0.002
        elif range_position <= 0.20:
            target += 0.001
        elif range_position >= 0.70:
            target -= 0.002

    opportunity = numeric_or_none(sentiment.get("mean_reversion_opportunity"))
    if opportunity is not None:
        if opportunity >= 0.55:
            target += 0.002
        elif opportunity >= MEAN_REVERSION_BUY_THRESHOLD:
            target += 0.001

    flow_pressure = numeric_or_none(sentiment.get("flow_pressure"))
    if flow_pressure is None:
        flow_pressure = numeric_or_none(kraken_flow.get("aggression_score"))
    if flow_pressure is not None:
        if flow_pressure >= 0.40:
            target += 0.0015
        elif flow_pressure <= 0:
            target -= 0.0015

    if weighted_signal >= SENTIMENT_BUY_THRESHOLD + 0.04:
        target += 0.002
    elif weighted_signal < 0:
        target -= 0.001

    volatility = numeric_or_none(
        price_regime.get("realized_volatility_24h_pct")
    )
    if volatility is not None:
        if volatility >= DYNAMIC_PROFIT_HIGH_VOLATILITY_PCT:
            target += 0.0015
        elif volatility <= DYNAMIC_PROFIT_LOW_VOLATILITY_PCT:
            target -= 0.001

    return clamp(target, MIN_TARGET_PROFIT_PCT, MAX_TARGET_PROFIT_PCT)


def target_limit_orders(
    sentiment,
    current_price,
    total_trade_value,
    max_buy_price=None
):
    if not ENABLE_TARGET_LIMIT_BUYS:
        return []

    targets = []
    for target in sentiment.get("target_prices", []):
        if not isinstance(target, dict):
            continue

        buy_price = numeric_or_none(target.get("buy_price"))
        if buy_price is None or buy_price <= 0:
            continue

        max_price = current_price * (1 + TARGET_LIMIT_MAX_PREMIUM_PCT)
        if buy_price > max_price:
            continue
        if max_buy_price is not None and buy_price >= max_buy_price:
            continue

        allocation = numeric_or_none(target.get("sell_pct"))
        if allocation is None:
            allocation = numeric_or_none(target.get("allocation_pct"))
        if allocation is None or allocation <= 0:
            allocation = 1.0

        targets.append(
            {
                "buy_price": buy_price,
                "allocation": allocation
            }
        )

    if not targets:
        return []

    targets = targets[:MAX_TARGET_LIMIT_ORDERS_PER_CYCLE]
    allocation_sum = sum(target["allocation"] for target in targets)
    if allocation_sum <= 0:
        return []

    return [
        {
            "buy_price": target["buy_price"],
            "trade_value": total_trade_value * target["allocation"] / allocation_sum,
            "allocation_pct": target["allocation"] / allocation_sum
        }
        for target in targets
    ]


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


def place_profit_sell_for_buy(
    cycle_id,
    buy_txid,
    buy_price,
    volume,
    target_profit_pct=None
):
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )
    target_price = sell_target_price(buy_price, profit_pct)
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="sell",
        reason="profit_target_after_buy_fill",
        buy_txid=buy_txid,
        buy_price=buy_price,
        price=round_price(target_price),
        volume=round_volume(volume),
        target_profit_pct=profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT
    )
    append_decision_csv(
        "trade_decision",
        cycle_id=cycle_id,
        side="sell",
        reason="profit_target_after_buy_fill",
        price=round_price(target_price),
        volume=round_volume(volume),
        dry_run=DRY_RUN,
        order_txid=buy_txid,
        target_profit_pct=profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT
    )
    return place_limit_sell(
        target_price,
        volume,
        cycle_id,
        buy_txid=buy_txid,
        buy_price=buy_price,
        target_profit_pct=profit_pct
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
                fill["volume"],
                order.get("target_profit_pct")
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


def maybe_handle_submitted_buy(
    result,
    cycle_id,
    volume,
    price,
    target_profit_pct=None
):
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )

    if DRY_RUN:
        state["stats"]["buy_orders_filled"] += 1
        save_state(state)
        place_profit_sell_for_buy(
            cycle_id,
            "dry_run_buy",
            price,
            volume,
            profit_pct
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

    if txid:
        notify_order_tracker(
            trade_id=txid,
            side="buy",
            price=fill_price or price,
            quantity=fill_volume or volume,
            order_id=txid,
            fee=fill.get("fill_fee"),
            timestamp=cycle_id,
            notes=(
                None
                if order_status == "closed"
                else f"order_status={order_status or 'submitted'}"
            )
        )

    if txid and order_status == "closed" and fill_volume > 0 and fill_price > 0:
        state["stats"]["buy_orders_filled"] += 1
        save_state(state)
        place_profit_sell_for_buy(
            cycle_id,
            txid,
            fill_price,
            fill_volume,
            profit_pct
        )
        return

    if txid:
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": volume,
            "price": price,
            "target_profit_pct": profit_pct,
            "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
            "placed_at": cycle_id,
            "trade_id": txid
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
    age_minutes = signal_age_minutes(sentiment, now)
    risk_multiplier = numeric_or_none(sentiment.get("risk_multiplier"))
    effective_multiplier = effective_risk_multiplier(sentiment)
    price_regime = sentiment.get("price_regime", {})
    range_position = numeric_or_none(price_regime.get("range_position_24h"))
    mean_reversion_opportunity = numeric_or_none(
        sentiment.get("mean_reversion_opportunity")
    )
    flow_pressure = numeric_or_none(sentiment.get("flow_pressure"))
    target_profit_pct = dynamic_target_profit_pct(sentiment, weighted_signal)

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
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        signal_status=sentiment.get("signal_status"),
        bot_action_allowed=sentiment.get("bot_action_allowed"),
        signal_age_minutes=age_minutes,
        mean_reversion_opportunity=mean_reversion_opportunity,
        range_position_24h=range_position,
        flow_pressure=flow_pressure,
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        processed_at=sentiment.get("processed_at")
    )
    console(f"Price: {price} | Signal: {raw_signal} | Confidence: {confidence}")

    gate_failure = signal_gate_failure(sentiment, now)
    if gate_failure is not None:
        reason = gate_failure.pop("reason")
        skip_cycle(
            reason,
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            **gate_failure
        )
        return

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

    allow_market_buy = weighted_signal >= SENTIMENT_BUY_THRESHOLD
    allow_target_limit_buy = (
        weighted_signal < SENTIMENT_BUY_THRESHOLD
        and mean_reversion_setup_allowed(sentiment)
        and bool(target_limit_orders(sentiment, price, 1))
    )

    if not allow_market_buy and not allow_target_limit_buy:
        skip_cycle(
            "sentiment_below_buy_threshold",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD,
            mean_reversion_opportunity=mean_reversion_opportunity,
            range_position_24h=range_position,
            flow_pressure=flow_pressure,
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT
        )
        return

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
    max_rebuy_price = None
    if PREVENT_BUY_ABOVE_LAST_SELL and last_sell_price is not None:
        max_rebuy_price = float(last_sell_price) * (
            1 - BUY_AFTER_SELL_DISCOUNT_PCT
        )
        if price >= max_rebuy_price:
            if not (
                allow_target_limit_buy
                and target_limit_orders(sentiment, price, 1, max_rebuy_price)
            ):
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

    if len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS:
        skip_cycle(
            "max_open_buy_orders",
            cycle_id,
            price=price,
            open_buy_count=len(state["open_buy_orders"]),
            max_open_buy_orders=MAX_OPEN_BUY_ORDERS
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
    trade_value *= effective_multiplier
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
            min_trade_usd=MIN_TRADE_USD,
            risk_multiplier=risk_multiplier,
            effective_risk_multiplier=effective_multiplier
        )
        return

    min_volume = get_min_order_volume()
    if allow_target_limit_buy:
        orders = target_limit_orders(
            sentiment,
            price,
            trade_value,
            max_rebuy_price
        )
        placed_orders = 0

        for order in orders:
            if len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS:
                break

            target_price = order["buy_price"]
            target_trade_value = order["trade_value"]
            volume = round_volume(target_trade_value / target_price)
            if volume <= 0 or volume < min_volume:
                log_event(
                    "TRADE_DECISION",
                    cycle_id=cycle_id,
                    side="hold",
                    reason="target_below_min_volume",
                    price=price,
                    target_buy_price=target_price,
                    trade_value=target_trade_value,
                    volume=volume,
                    min_volume=min_volume
                )
                continue

            log_event(
                "TRADE_DECISION",
                cycle_id=cycle_id,
                side="buy",
                reason="mean_reversion_target_limit_buy",
                volume=volume,
                price=target_price,
                target_buy_price=target_price,
                target_allocation_pct=order["allocation_pct"],
                trade_value=target_trade_value,
                base_balance=base_balance,
                quote_balance=quote_balance,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD,
                target_profit_pct=target_profit_pct,
                round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
                gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
                risk_multiplier=risk_multiplier,
                effective_risk_multiplier=effective_multiplier,
                mean_reversion_opportunity=mean_reversion_opportunity,
                range_position_24h=range_position,
                flow_pressure=flow_pressure
            )
            append_decision_csv(
                "trade_decision",
                cycle_id=cycle_id,
                side="buy",
                reason="mean_reversion_target_limit_buy",
                volume=volume,
                price=target_price,
                target_buy_price=target_price,
                target_allocation_pct=order["allocation_pct"],
                trade_value=target_trade_value,
                base_balance=base_balance,
                quote_balance=quote_balance,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                dry_run=DRY_RUN,
                target_profit_pct=target_profit_pct,
                round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
                gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
                risk_multiplier=risk_multiplier,
                effective_risk_multiplier=effective_multiplier,
                mean_reversion_opportunity=mean_reversion_opportunity,
                range_position_24h=range_position,
                flow_pressure=flow_pressure
            )

            result = place_limit_buy(
                target_price,
                volume,
                cycle_id,
                target_profit_pct
            )
            result_payload = (
                result.get("result") if isinstance(result, dict) else None
            )
            txids = (
                result_payload.get("txid", [])
                if isinstance(result_payload, dict)
                else []
            )
            append_decision_csv(
                "trade_executed" if result else "trade_rejected",
                cycle_id=cycle_id,
                side="buy",
                reason=(
                    "dry_run"
                    if DRY_RUN
                    else ("limit_submitted" if result else "rejected")
                ),
                volume=volume,
                price=target_price,
                target_buy_price=target_price,
                target_allocation_pct=order["allocation_pct"],
                trade_value=target_trade_value,
                base_balance=base_balance,
                quote_balance=quote_balance,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                dry_run=DRY_RUN,
                order_txid=txids[0] if txids else "",
                target_profit_pct=target_profit_pct,
                round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
                gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
                result=result_payload or result
            )

            if result:
                placed_orders += 1

        if placed_orders <= 0:
            skip_cycle(
                "no_target_limit_orders_placed",
                cycle_id,
                price=price,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence
            )
            return

        log_event(
            "CYCLE_SUMMARY",
            cycle_id=cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            side="buy",
            reason="mean_reversion_target_limit_buy",
            trade_value=trade_value,
            base_balance=base_balance,
            quote_balance=quote_balance,
            open_buy_count=len(state["open_buy_orders"]),
            open_sell_count=len(state["open_sell_orders"]),
            deployed_inventory_usd=current_inventory_usd(price),
            last_sell_price=state.get("last_sell_price"),
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
            dry_run=DRY_RUN
        )
        return

    volume = round_volume(trade_value / price)
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
        sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD,
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier
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
        dry_run=DRY_RUN,
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier
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
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        **fill,
        result=result_payload or result
    )
    maybe_handle_submitted_buy(
        result,
        cycle_id,
        volume,
        price,
        target_profit_pct
    )

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
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
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
        strategy_profile=STRATEGY_PROFILE,
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
        dynamic_profit_targets=DYNAMIC_PROFIT_TARGETS,
        min_target_profit_pct=MIN_TARGET_PROFIT_PCT,
        base_target_profit_pct=BASE_TARGET_PROFIT_PCT,
        max_target_profit_pct=MAX_TARGET_PROFIT_PCT,
        dynamic_profit_low_volatility_pct=DYNAMIC_PROFIT_LOW_VOLATILITY_PCT,
        dynamic_profit_high_volatility_pct=DYNAMIC_PROFIT_HIGH_VOLATILITY_PCT,
        max_open_sell_orders=MAX_OPEN_SELL_ORDERS,
        max_open_buy_orders=MAX_OPEN_BUY_ORDERS,
        max_inventory_usd=MAX_INVENTORY_USD,
        prevent_buy_above_last_sell=PREVENT_BUY_ABOVE_LAST_SELL,
        buy_after_sell_discount_pct=BUY_AFTER_SELL_DISCOUNT_PCT,
        high_price_buy_block_pct=HIGH_PRICE_BUY_BLOCK_PCT,
        use_signal_status_gates=USE_SIGNAL_STATUS_GATES,
        require_bot_action_allowed=REQUIRE_BOT_ACTION_ALLOWED,
        max_signal_age_minutes=MAX_SIGNAL_AGE_MINUTES,
        use_risk_multiplier=USE_RISK_MULTIPLIER,
        min_risk_multiplier=MIN_RISK_MULTIPLIER,
        max_risk_multiplier=MAX_RISK_MULTIPLIER,
        enable_target_limit_buys=ENABLE_TARGET_LIMIT_BUYS,
        max_target_limit_orders_per_cycle=MAX_TARGET_LIMIT_ORDERS_PER_CYCLE,
        mean_reversion_buy_threshold=MEAN_REVERSION_BUY_THRESHOLD,
        mean_reversion_range_position_max=MEAN_REVERSION_RANGE_POSITION_MAX,
        mean_reversion_flow_pressure_min=MEAN_REVERSION_FLOW_PRESSURE_MIN,
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
