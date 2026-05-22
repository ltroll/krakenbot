#!/usr/bin/env python3

# =====================================================
# KRAKEN STATS/TREND BOT
# =====================================================

import base64
import csv
import hashlib
import hmac
import json
import math
import os
import statistics
import time
from datetime import datetime, timedelta, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

STRATEGY_PROFILE = (
    os.getenv("STATS_TREND_STRATEGY_PROFILE")
    or os.getenv("STRATEGY_PROFILE")
    or "stats_trend_strategy_default.json"
)
STATE_FILE = os.getenv("STATS_TREND_STATE_FILE", "stats_trend_state.json")
LOG_FILE = os.getenv("STATS_TREND_TRADE_LOG_FILE", "stats_trend_trade_log.jsonl")
DECISION_CSV_FILE = os.getenv(
    "STATS_TREND_DECISION_CSV_FILE",
    "stats_trend_decisions.csv"
)

KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
KRAKEN_ORDERBOOK_URL = os.getenv("KRAKEN_ORDERBOOK_URL")
KRAKEN_OHLC_URL = os.getenv("KRAKEN_OHLC_URL")
PRICE_LOG_URL = os.getenv("PRICE_LOG_URL")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
PRICE_INTELLIGENCE_URL = (
    os.getenv("STATS_TREND_PRICE_INTELLIGENCE_URL")
    or os.getenv("PRICE_INTELLIGENCE_URL")
)


def load_json_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_strategy_config():
    path = os.path.expanduser(STRATEGY_PROFILE)
    if not os.path.exists(path):
        raise RuntimeError(f"Strategy profile file not found: {path}")

    return load_json_file(path)


strategy_config = load_strategy_config()


def profile_bool(name, default):
    value = strategy_config.get(name, default)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def env_bool(name, default):
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def env_int(name, default):
    value = os.getenv(name)
    return default if value is None or value == "" else int(value)


def env_float(name, default):
    value = os.getenv(name)
    return default if value is None or value == "" else float(value)


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


def profile_str(name, default=""):
    value = strategy_config.get(name, default)
    return default if value is None else str(value)


def profile_float_list(name, default):
    value = strategy_config.get(name, default)
    if value is None:
        return default
    if isinstance(value, list):
        return [float(item) for item in value]
    return [
        float(item.strip())
        for item in str(value).split(",")
        if item.strip()
    ]


def build_float_grid(start, stop, step):
    if step <= 0:
        return []

    values = []
    current = start
    epsilon = step / 10
    while current <= stop + epsilon:
        values.append(round(current, 10))
        current += step

    return values


if not PRICE_INTELLIGENCE_URL:
    PRICE_INTELLIGENCE_URL = profile_str("price_intelligence_url") or None


REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
KRAKEN_NONCE_RETRIES = int(os.getenv("KRAKEN_NONCE_RETRIES", "2"))
KRAKEN_LOCKOUT_COOLDOWN_SECONDS = env_int(
    "KRAKEN_LOCKOUT_COOLDOWN_SECONDS",
    300
)
BACKTEST_MODE = env_bool(
    "STATS_TREND_BACKTEST_MODE",
    profile_bool("backtest_mode", False)
)
BACKTEST_STEP_PATH = (
    os.getenv("STATS_TREND_BACKTEST_STEP_PATH")
    or profile_str("backtest_step_path", "/admin/api/backtest/step")
)
BACKTEST_STEPS_PER_CYCLE = env_int(
    "STATS_TREND_BACKTEST_STEPS_PER_CYCLE",
    profile_int("backtest_steps_per_cycle", 1)
)
BACKTEST_STEP_AFTER_CYCLE = env_bool(
    "STATS_TREND_BACKTEST_STEP_AFTER_CYCLE",
    profile_bool("backtest_step_after_cycle", True)
)
BACKTEST_SLEEP_SECONDS = env_float(
    "STATS_TREND_BACKTEST_SLEEP_SECONDS",
    profile_float("backtest_sleep_seconds", 0)
)
BACKTEST_USE_API_MARKET_DATA = env_bool(
    "STATS_TREND_BACKTEST_USE_API_MARKET_DATA",
    profile_bool("backtest_use_api_market_data", True)
)
MARKET_HISTORY_SOURCE = (
    os.getenv("STATS_TREND_MARKET_HISTORY_SOURCE")
    or profile_str(
        "market_history_source",
        "ohlc" if BACKTEST_MODE else "price_log"
    )
).strip().lower()
KRAKEN_OHLC_INTERVAL_MINUTES = env_int(
    "KRAKEN_OHLC_INTERVAL_MINUTES",
    profile_int("kraken_ohlc_interval_minutes", 60)
)
ORDER_TRACKER_URL = (
    os.getenv("ORDER_TRACKER_URL")
    or os.getenv("EXTERNAL_ORDER_TRACKER_URL")
)
ORDER_TRACKER_USER_AGENT = os.getenv("ORDER_TRACKER_USER_AGENT")
ORDER_TRACKER_SYMBOL = (
    os.getenv("ORDER_TRACKER_SYMBOL")
    or profile_str("order_tracker_symbol", KRAKEN_PAIR)
)
ORDER_TRACKER_TIMEOUT = profile_float("order_tracker_timeout_seconds", 5)
ORDER_TRACKER_CHECKIN_TIMEOUT = profile_float(
    "order_tracker_checkin_timeout_seconds",
    min(ORDER_TRACKER_TIMEOUT, 5)
)
DRY_RUN = env_bool("STATS_TREND_DRY_RUN", profile_bool("dry_run", True))
PRICE_INTELLIGENCE_ENABLED = env_bool(
    "STATS_TREND_PRICE_INTELLIGENCE_ENABLED",
    profile_bool("price_intelligence_enabled", True)
)
PRICE_INTELLIGENCE_MAX_AGE_MINUTES = profile_float(
    "price_intelligence_max_age_minutes",
    20
)
PRICE_INTELLIGENCE_MAX_PROBABILITY_NUDGE = profile_float(
    "price_intelligence_max_probability_nudge",
    0.12
)
PRICE_INTELLIGENCE_MAX_EV_NUDGE_PCT = profile_float(
    "price_intelligence_max_ev_nudge_pct",
    0.001
)
PRICE_CHECK_INTERVAL_SECONDS = profile_int("price_check_interval_seconds", 60)
HISTORY_WINDOW_HOURS = profile_int("history_window_hours", 48)
MIN_SAMPLES = profile_int("min_samples", 24)
FAST_MA_SAMPLES = profile_int("fast_ma_samples", 6)
SLOW_MA_SAMPLES = profile_int("slow_ma_samples", 24)
MOMENTUM_LOOKBACK_SAMPLES = profile_int("momentum_lookback_samples", 12)
BREAKOUT_LOOKBACK_SAMPLES = profile_int("breakout_lookback_samples", 24)
TREND_EXIT_THRESHOLD = profile_float("trend_exit_threshold", -0.25)
MIN_MOMENTUM_PCT = profile_float("min_momentum_pct", 0.0015)
MIN_MA_SPREAD_PCT = profile_float("min_ma_spread_pct", 0.0008)
BREAKOUT_BUFFER_PCT = profile_float("breakout_buffer_pct", 0.0005)
MAX_VOLATILITY_PCT = profile_float("max_volatility_pct", 0.045)
MIN_TRADE_USD = profile_float("min_trade_usd", 20)
POSITION_SIZE_PCT = profile_float("position_size_pct", 0.1)
MAX_TRADE_USD = profile_float("max_trade_usd", 90)
DRY_RUN_BASE_BALANCE = profile_float("dry_run_base_balance", 0)
DRY_RUN_QUOTE_BALANCE = profile_float("dry_run_quote_balance", 1000)
ORDERBOOK_DEPTH_COUNT = profile_int("orderbook_depth_count", 50)
ORDERBOOK_CANDIDATE_COUNT = profile_int("orderbook_candidate_count", 5)
ORDERBOOK_ENTRY_STEP_PCT = profile_float("orderbook_entry_step_pct", 0.0015)
ORDERBOOK_EXIT_TARGET_MIN_PCT = profile_float(
    "orderbook_exit_target_min_pct",
    0.005
)
ORDERBOOK_EXIT_TARGET_MAX_PCT = profile_float(
    "orderbook_exit_target_max_pct",
    0.015
)
ORDERBOOK_EXIT_TARGET_STEP_PCT = profile_float(
    "orderbook_exit_target_step_pct",
    0.001
)
ORDERBOOK_EXIT_TARGET_PCTS = profile_float_list(
    "orderbook_exit_target_pcts",
    build_float_grid(
        ORDERBOOK_EXIT_TARGET_MIN_PCT,
        ORDERBOOK_EXIT_TARGET_MAX_PCT,
        ORDERBOOK_EXIT_TARGET_STEP_PCT
    )
)
ORDERBOOK_EXIT_HORIZON_HOURS = profile_float(
    "orderbook_exit_horizon_hours",
    3
)
ORDERBOOK_MIN_ENTRY_PROBABILITY = profile_float(
    "orderbook_min_entry_probability",
    0.28
)
ORDERBOOK_MIN_EXIT_PROBABILITY = profile_float(
    "orderbook_min_exit_probability",
    0.42
)
ORDERBOOK_MIN_EXPECTED_VALUE_PCT = profile_float(
    "orderbook_min_expected_value_pct",
    0.0015
)
ORDERBOOK_MAX_ENTRY_DROP_PCT = profile_float("orderbook_max_entry_drop_pct", 0.012)
ORDERBOOK_PRESSURE_WINDOW_PCT = profile_float(
    "orderbook_pressure_window_pct",
    0.004
)
MAX_OPEN_BUY_ORDERS = profile_int("max_open_buy_orders", 2)
MAX_OPEN_SELL_ORDERS = profile_int("max_open_sell_orders", 2)
MAX_OPEN_ORDERS = profile_int("max_open_orders", 4)
MAX_OPEN_BUY_ORDERS_PER_DAY = profile_int("max_open_buy_orders_per_day", 2)
MAX_OPEN_BUY_AGE_MINUTES = profile_float("max_open_buy_age_minutes", 180)
REVALIDATE_OPEN_BUYS = profile_bool("revalidate_open_buys", True)
OPEN_BUY_REVALIDATION_GRACE_MINUTES = profile_float(
    "open_buy_revalidation_grace_minutes",
    5
)
OPEN_BUY_REVALIDATION_HOLD_NEAR_ENTRY_PCT = profile_float(
    "open_buy_revalidation_hold_near_entry_pct",
    ORDERBOOK_ENTRY_STEP_PCT
)
OPEN_BUY_REVALIDATION_MIN_EXPECTED_VALUE_PCT = profile_float(
    "open_buy_revalidation_min_expected_value_pct",
    ORDERBOOK_MIN_EXPECTED_VALUE_PCT
)
OPEN_BUY_REVALIDATION_MIN_EXIT_PROBABILITY = profile_float(
    "open_buy_revalidation_min_exit_probability",
    ORDERBOOK_MIN_EXIT_PROBABILITY
)
MAX_INVENTORY_USD = profile_float("max_inventory_usd", 400)
REBALANCE_COOLDOWN_MINUTES = profile_float("rebalance_cooldown_minutes", 15)
COOLDOWN_OVERRIDE_SCORE = profile_float("cooldown_override_score", 0.85)
TARGET_PROFIT_PCT = profile_float("target_profit_pct", 0.008)
ROUND_TRIP_FEE_PCT = profile_float("round_trip_fee_pct", 0.006)
EXECUTION_BUFFER_PCT = profile_float("execution_buffer_pct", 0.0025)
PREVENT_BUY_ABOVE_LAST_SELL = profile_bool("prevent_buy_above_last_sell", True)
BUY_AFTER_SELL_DISCOUNT_PCT = profile_float("buy_after_sell_discount_pct", 0.001)
HIGH_RANGE_BUY_BLOCK_PCT = profile_float("high_range_buy_block_pct", 0.001)

PAIR_INFO_CACHE = None

DECISION_CSV_FIELDS = [
    "ts",
    "cycle_id",
    "event",
    "side",
    "reason",
    "price",
    "trend_score",
    "momentum_pct",
    "ma_spread_pct",
    "breakout_pct",
    "volatility_pct",
    "range_position",
    "price_intelligence_status",
    "price_intelligence_age_minutes",
    "price_intelligence_regime_score",
    "price_intelligence_entry_nudge",
    "price_intelligence_exit_nudge",
    "price_intelligence_ev_nudge_pct",
    "price_intelligence_rsi_7d",
    "price_intelligence_rsi_14d",
    "price_intelligence_above_200d_sma",
    "price_intelligence_distance_from_sma_200d_pct",
    "price_intelligence_volatility_regime",
    "candidate_entry_price",
    "candidate_exit_price",
    "candidate_entry_probability",
    "candidate_exit_probability",
    "candidate_joint_probability",
    "candidate_expected_value_pct",
    "candidate_entry_drop_pct",
    "candidate_exit_target_pct",
    "candidate_exit_horizon_hours",
    "open_buy_orders_today",
    "open_buy_order_date",
    "max_open_buy_orders_per_day",
    "predicted_expected_value_pct",
    "predicted_entry_probability",
    "predicted_exit_probability",
    "predicted_joint_probability",
    "volume",
    "trade_value",
    "base_balance",
    "quote_balance",
    "dry_run",
    "order_txid",
    "order_status",
    "fill_volume",
    "fill_cost",
    "fill_fee",
    "fill_price",
    "result"
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
    console(f"{event}: {message}" if message else event)


def positive_float(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None

    return parsed if parsed > 0 else None


def tracker_value(value, fallback=None):
    return positive_float(value) or positive_float(fallback)


def notify_order_tracker(
    trade_id,
    side,
    price,
    quantity,
    order_id=None,
    fee=None,
    timestamp=None,
    notes=None,
    status=None
):
    if not ORDER_TRACKER_URL or not ORDER_TRACKER_USER_AGENT:
        return

    price = positive_float(price)
    quantity = positive_float(quantity)

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
        "fee": positive_float(fee),
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "notes": notes,
        "status": status
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


def short_error_summary(error):
    return str(error).replace("\n", " ")[:200]


def send_checkin(status="ok", loop_count=None, message="loop_complete"):
    if not ORDER_TRACKER_URL or not ORDER_TRACKER_USER_AGENT:
        return

    payload = {
        "action": "checkin",
        "status": status,
        "message": message
    }
    if loop_count is not None:
        payload["loop_count"] = str(loop_count)

    try:
        response = requests.post(
            ORDER_TRACKER_URL,
            data=payload,
            headers={"User-Agent": ORDER_TRACKER_USER_AGENT},
            timeout=ORDER_TRACKER_CHECKIN_TIMEOUT
        )
        response.raise_for_status()
    except Exception as e:
        log_event("ORDER_TRACKER_CHECKIN_ERROR", message=str(e), status=status)


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
    csv_row = {field: row.get(field, "") for field in DECISION_CSV_FIELDS}

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


# ----------------------
# STATE
# ----------------------


def load_state():
    default = {
        "last_cycle": None,
        "last_nonce": 0,
        "last_trade_at": None,
        "last_sell_price": None,
        "last_sell_at": None,
        "open_buy_orders": {},
        "open_sell_orders": {},
        "trade_history": [],
        "backtest_price_history": [],
        "kraken_private_paused_until": None,
        "last_signal": None,
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
        }
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

    if not isinstance(state.get("open_buy_orders"), dict):
        state["open_buy_orders"] = {}
    if not isinstance(state.get("open_sell_orders"), dict):
        state["open_sell_orders"] = {}
    if not isinstance(state.get("trade_history"), list):
        state["trade_history"] = []
    if not isinstance(state.get("backtest_price_history"), list):
        state["backtest_price_history"] = []

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


def trading_day_key(now=None):
    now = now or datetime.now(timezone.utc)
    return now.astimezone(timezone.utc).date().isoformat()


def open_buy_orders_for_day(now=None):
    day_key = trading_day_key(now)
    count = 0

    for order in state.get("open_buy_orders", {}).values():
        placed_at = parse_iso8601(order.get("placed_at"))
        if placed_at is None:
            continue

        if trading_day_key(placed_at) == day_key:
            count += 1

    return count


def record_trade_history(entry):
    state.setdefault("trade_history", [])
    state["trade_history"].append(entry)
    state["trade_history"] = state["trade_history"][-250:]
    save_state(state)


def record_backtest_price(price):
    if not BACKTEST_MODE:
        return []

    history = state.setdefault("backtest_price_history", [])
    history.append(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "price": price
        }
    )
    max_points = max(
        MIN_SAMPLES,
        int(
            math.ceil(
                (HISTORY_WINDOW_HOURS * 60)
                / max(KRAKEN_OHLC_INTERVAL_MINUTES, 1)
            )
        )
    )
    state["backtest_price_history"] = history[-max_points:]
    save_state(state)

    records = []
    for item in state["backtest_price_history"]:
        try:
            ts = datetime.fromisoformat(item["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            records.append({"timestamp": ts, "price": float(item["price"])})
        except Exception:
            continue

    return records


# ----------------------
# KRAKEN HELPERS
# ----------------------


def require_runtime_config():
    required = {"KRAKEN_API_URL": KRAKEN_API_URL}
    if MARKET_HISTORY_SOURCE == "price_log":
        required["PRICE_LOG_URL"] = PRICE_LOG_URL
    if not DRY_RUN:
        required["KRAKEN_API_KEY"] = KRAKEN_API_KEY
        required["KRAKEN_API_SECRET"] = KRAKEN_API_SECRET

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")


def next_nonce():
    wall_nonce = int(time.time() * 1000)
    last_nonce = int(state.get("last_nonce", 0))
    nonce = max(wall_nonce, last_nonce + 1)
    state["last_nonce"] = nonce
    save_state(state)
    return str(nonce)


def kraken_private_pause_seconds():
    paused_until = parse_iso8601(state.get("kraken_private_paused_until"))
    if paused_until is None:
        return 0

    if paused_until.tzinfo is None:
        paused_until = paused_until.replace(tzinfo=timezone.utc)

    remaining = (paused_until - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        state["kraken_private_paused_until"] = None
        save_state(state)
        return 0

    return remaining


def pause_kraken_private_api(reason):
    paused_until = datetime.fromtimestamp(
        time.time() + KRAKEN_LOCKOUT_COOLDOWN_SECONDS,
        timezone.utc
    )
    state["kraken_private_paused_until"] = paused_until.isoformat()
    save_state(state)
    log_event(
        "KRAKEN_PRIVATE_PAUSED",
        message=reason,
        paused_until=state["kraken_private_paused_until"],
        cooldown_seconds=KRAKEN_LOCKOUT_COOLDOWN_SECONDS
    )


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
    pause_seconds = kraken_private_pause_seconds()
    if pause_seconds > 0:
        log_event(
            "KRAKEN_PRIVATE_SKIPPED",
            operation=label,
            reason="private_api_paused",
            pause_seconds=round(pause_seconds, 2)
        )
        return None

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

            if "Temporary lockout" in message:
                pause_kraken_private_api(message)
                return None

            if "Invalid nonce" not in message or attempt >= attempts:
                return None

            state["last_nonce"] = max(
                int(state.get("last_nonce", 0)),
                int(time.time() * 1000)
            ) + 1000
            save_state(state)
            time.sleep(1)

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


def get_price():
    try:
        if KRAKEN_TICKER_URL and not (
            BACKTEST_MODE and BACKTEST_USE_API_MARKET_DATA
        ):
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


def get_orderbook():
    try:
        if KRAKEN_ORDERBOOK_URL and not (
            BACKTEST_MODE and BACKTEST_USE_API_MARKET_DATA
        ):
            r = requests.get(KRAKEN_ORDERBOOK_URL, timeout=REQUEST_TIMEOUT)
        else:
            url = KRAKEN_API_URL.rstrip("/") + "/0/public/Depth"
            r = requests.get(
                url,
                params={"pair": KRAKEN_PAIR, "count": ORDERBOOK_DEPTH_COUNT},
                timeout=REQUEST_TIMEOUT
            )
        r.raise_for_status()
        data = r.json()

        if data.get("error"):
            raise RuntimeError(data["error"])

        book = next(iter(data["result"].values()), None)
        if not book:
            raise RuntimeError(f"Order book not found for {KRAKEN_PAIR}")

        bids = [
            {"price": float(row[0]), "volume": float(row[1])}
            for row in book.get("bids", [])
        ]
        asks = [
            {"price": float(row[0]), "volume": float(row[1])}
            for row in book.get("asks", [])
        ]
        bids.sort(key=lambda item: item["price"], reverse=True)
        asks.sort(key=lambda item: item["price"])

        if not bids or not asks:
            raise RuntimeError("Order book missing bids or asks")

        return {"bids": bids, "asks": asks}
    except Exception as e:
        log_event("ORDERBOOK_ERROR", message=str(e))
        return None


def get_balances():
    if DRY_RUN and (not KRAKEN_API_KEY or not KRAKEN_API_SECRET):
        return DRY_RUN_BASE_BALANCE, DRY_RUN_QUOTE_BALANCE

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
# MARKET STATS
# ----------------------


def load_price_history_from_jsonl():
    r = requests.get(PRICE_LOG_URL, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=HISTORY_WINDOW_HOURS)
    records = []

    for line in r.text.splitlines():
        if not line.strip():
            continue

        try:
            record = json.loads(line)
            ts = datetime.fromisoformat(record["timestamp"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if ts < cutoff:
                continue
            records.append(
                {
                    "timestamp": ts,
                    "price": float(record["btc_price_usd"])
                }
            )
        except Exception:
            continue

    records.sort(key=lambda item: item["timestamp"])
    return records


def load_price_history_from_ohlc():
    if KRAKEN_OHLC_URL and not (
        BACKTEST_MODE and BACKTEST_USE_API_MARKET_DATA
    ):
        r = requests.get(KRAKEN_OHLC_URL, timeout=REQUEST_TIMEOUT)
    else:
        url = KRAKEN_API_URL.rstrip("/") + "/0/public/OHLC"
        r = requests.get(
            url,
            params={
                "pair": KRAKEN_PAIR,
                "interval": KRAKEN_OHLC_INTERVAL_MINUTES
            },
            timeout=REQUEST_TIMEOUT
        )
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    rows = None
    for key, value in data.get("result", {}).items():
        if key != "last":
            rows = value
            break

    if not rows:
        return []

    cutoff = None
    if not BACKTEST_MODE:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=HISTORY_WINDOW_HOURS)

    records = []
    for row in rows:
        try:
            ts = datetime.fromtimestamp(float(row[0]), tz=timezone.utc)
            if cutoff is not None and ts < cutoff:
                continue
            records.append(
                {
                    "timestamp": ts,
                    "price": float(row[4])
                }
            )
        except Exception:
            continue

    records.sort(key=lambda item: item["timestamp"])
    if BACKTEST_MODE:
        max_points = int(
            math.ceil(
                (HISTORY_WINDOW_HOURS * 60)
                / max(KRAKEN_OHLC_INTERVAL_MINUTES, 1)
            )
        )
        records = records[-max(max_points, MIN_SAMPLES):]

    return records


def load_price_history():
    if MARKET_HISTORY_SOURCE == "ohlc":
        return load_price_history_from_ohlc()

    return load_price_history_from_jsonl()


def load_price_intelligence():
    if not PRICE_INTELLIGENCE_ENABLED or not PRICE_INTELLIGENCE_URL:
        return {
            "usable": False,
            "status": "disabled" if not PRICE_INTELLIGENCE_ENABLED else "missing_url"
        }

    try:
        if PRICE_INTELLIGENCE_URL.startswith(("http://", "https://")):
            r = requests.get(PRICE_INTELLIGENCE_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
        else:
            data = load_json_file(os.path.expanduser(PRICE_INTELLIGENCE_URL))

        ts = parse_iso8601(data.get("timestamp"))
        age_minutes = None
        if ts is not None:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            age_minutes = (
                datetime.now(timezone.utc) - ts.astimezone(timezone.utc)
            ).total_seconds() / 60

        if age_minutes is not None and age_minutes > PRICE_INTELLIGENCE_MAX_AGE_MINUTES:
            return {
                "usable": False,
                "status": "stale",
                "age_minutes": age_minutes
            }

        return {
            "usable": True,
            "status": "fresh",
            "age_minutes": age_minutes,
            "data": data
        }
    except Exception as e:
        log_event("PRICE_INTELLIGENCE_ERROR", message=str(e))
        return {
            "usable": False,
            "status": "error",
            "message": str(e)
        }


def score_price_intelligence(intelligence):
    if not intelligence.get("usable"):
        return {
            "status": intelligence.get("status"),
            "age_minutes": intelligence.get("age_minutes"),
            "regime_score": 0,
            "entry_probability_nudge": 0,
            "exit_probability_nudge": 0,
            "expected_value_nudge_pct": 0
        }

    data = intelligence.get("data", {})
    trend = data.get("trend", {})
    ranges = data.get("ranges", {})
    volatility = data.get("volatility", {})
    mean_reversion = data.get("mean_reversion", {})
    momentum = data.get("momentum", {})
    volume = data.get("volume", {})

    score = 0
    rsi_7d = mean_reversion.get("rsi_7d")
    rsi_14d = mean_reversion.get("rsi_14d")
    range_position_7d = ranges.get("range_position_7d")
    range_position_30d = ranges.get("range_position_30d")
    distance_from_sma_200d_pct = trend.get("distance_from_sma_200d_pct")
    volatility_regime = volatility.get("volatility_regime")
    consecutive_down_days = momentum.get("consecutive_down_days") or 0
    volume_ratio_30d = volume.get("volume_ratio_30d")

    if rsi_7d is not None:
        if rsi_7d <= 25:
            score += 0.18
        elif rsi_7d <= 35:
            score += 0.10
        elif rsi_7d >= 75:
            score -= 0.16
        elif rsi_7d >= 65:
            score -= 0.08

    if rsi_14d is not None:
        if rsi_14d <= 40:
            score += 0.06
        elif rsi_14d >= 65:
            score -= 0.06

    if range_position_7d is not None:
        if range_position_7d <= 0.15:
            score += 0.10
        elif range_position_7d >= 0.85:
            score -= 0.10

    if range_position_30d is not None:
        if range_position_30d <= 0.30:
            score += 0.05
        elif range_position_30d >= 0.80:
            score -= 0.07

    if trend.get("above_200d_sma") is True:
        score += 0.05
    elif trend.get("above_200d_sma") is False:
        score -= 0.04

    if distance_from_sma_200d_pct is not None:
        if distance_from_sma_200d_pct < -10:
            score -= 0.08
        elif -8 <= distance_from_sma_200d_pct <= -2:
            score += 0.04
        elif distance_from_sma_200d_pct > 8:
            score -= 0.06

    if trend.get("sma_20d_slope_7d_pct", 0) > 0:
        score += 0.04
    if trend.get("sma_50d_slope_7d_pct", 0) > 0:
        score += 0.04
    if trend.get("sma_200d_slope_7d_pct", 0) < -2:
        score -= 0.05

    if consecutive_down_days >= 3:
        score += 0.05
    if momentum.get("return_7d_pct", 0) < -8:
        score -= 0.05

    if volatility_regime == "low":
        score += 0.03
    elif volatility_regime == "high":
        score -= 0.10

    if volume_ratio_30d is not None and volume_ratio_30d < 0.10:
        score -= 0.04

    score = clamp(score, -1, 1)
    max_nudge = PRICE_INTELLIGENCE_MAX_PROBABILITY_NUDGE
    entry_nudge = clamp(score * 0.35 * max_nudge, -max_nudge, max_nudge)
    exit_nudge = clamp(score * max_nudge, -max_nudge, max_nudge)
    ev_nudge = clamp(
        score * PRICE_INTELLIGENCE_MAX_EV_NUDGE_PCT,
        -PRICE_INTELLIGENCE_MAX_EV_NUDGE_PCT,
        PRICE_INTELLIGENCE_MAX_EV_NUDGE_PCT
    )

    return {
        "status": intelligence.get("status"),
        "age_minutes": intelligence.get("age_minutes"),
        "regime_score": score,
        "entry_probability_nudge": entry_nudge,
        "exit_probability_nudge": exit_nudge,
        "expected_value_nudge_pct": ev_nudge,
        "rsi_7d": rsi_7d,
        "rsi_14d": rsi_14d,
        "above_200d_sma": trend.get("above_200d_sma"),
        "distance_from_sma_200d_pct": distance_from_sma_200d_pct,
        "volatility_regime": volatility_regime
    }


def pct_change(current, previous):
    if previous == 0:
        return 0
    return (current - previous) / previous


def mean(values):
    return sum(values) / len(values)


def realized_volatility_pct(prices):
    returns = [
        pct_change(prices[i], prices[i - 1])
        for i in range(1, len(prices))
        if prices[i - 1] > 0
    ]

    if len(returns) < 2:
        return 0

    return statistics.stdev(returns) * math.sqrt(len(returns))


def clamp(value, low, high):
    return max(low, min(high, value))


def compute_trend_signal(price):
    history = load_price_history()
    if BACKTEST_MODE and len(history) < MIN_SAMPLES:
        fallback_history = record_backtest_price(price)
        if len(fallback_history) > len(history):
            history = fallback_history

    prices = [item["price"] for item in history]

    if len(prices) < MIN_SAMPLES:
        return {
            "usable": False,
            "reason": "insufficient_price_history",
            "sample_count": len(prices)
        }

    fast_count = min(FAST_MA_SAMPLES, len(prices))
    slow_count = min(SLOW_MA_SAMPLES, len(prices))
    momentum_idx = max(0, len(prices) - MOMENTUM_LOOKBACK_SAMPLES)
    breakout_count = min(BREAKOUT_LOOKBACK_SAMPLES, len(prices))
    recent_range = prices[-breakout_count:]
    fast_ma = mean(prices[-fast_count:])
    slow_ma = mean(prices[-slow_count:])
    momentum_pct = pct_change(price, prices[momentum_idx])
    ma_spread_pct = pct_change(fast_ma, slow_ma)
    range_low = min(recent_range)
    range_high = max(recent_range)
    range_width = max(range_high - range_low, 1e-9)
    range_position = (price - range_low) / range_width
    breakout_pct = pct_change(price, range_high)
    volatility_pct = realized_volatility_pct(prices[-slow_count:])

    momentum_component = clamp(momentum_pct / max(MIN_MOMENTUM_PCT, 1e-9), -1, 1)
    ma_component = clamp(ma_spread_pct / max(MIN_MA_SPREAD_PCT, 1e-9), -1, 1)
    breakout_component = clamp(
        breakout_pct / max(BREAKOUT_BUFFER_PCT, 1e-9),
        -1,
        1
    )
    range_component = clamp((range_position - 0.5) * 2, -1, 1)
    volatility_penalty = clamp(volatility_pct / max(MAX_VOLATILITY_PCT, 1e-9), 0, 1)
    price_intelligence = score_price_intelligence(load_price_intelligence())
    raw_score = (
        0.35 * momentum_component
        + 0.35 * ma_component
        + 0.20 * breakout_component
        + 0.10 * range_component
    )
    trend_score = clamp(
        raw_score * (1 - 0.35 * volatility_penalty)
        + (0.20 * price_intelligence["regime_score"]),
        -1,
        1
    )

    return {
        "usable": True,
        "sample_count": len(prices),
        "fast_ma": fast_ma,
        "slow_ma": slow_ma,
        "momentum_pct": momentum_pct,
        "ma_spread_pct": ma_spread_pct,
        "range_low": range_low,
        "range_high": range_high,
        "range_position": range_position,
        "breakout_pct": breakout_pct,
        "volatility_pct": volatility_pct,
        "trend_score": trend_score,
        "price_intelligence": price_intelligence
    }


def depth_notional(levels, predicate):
    return sum(
        level["price"] * level["volume"]
        for level in levels
        if predicate(level["price"])
    )


def estimate_exit_probability(entry_price, exit_price, book, trend_signal):
    asks_between = depth_notional(
        book["asks"],
        lambda ask_price: entry_price <= ask_price <= exit_price
    )
    bid_support = depth_notional(
        book["bids"],
        lambda bid_price: bid_price >= entry_price * (1 - ORDERBOOK_PRESSURE_WINDOW_PCT)
    )
    depth_total = max(asks_between + bid_support, 1e-9)
    resistance_ratio = asks_between / depth_total
    trend_bias = clamp((trend_signal["trend_score"] + 1) / 2, 0, 1)
    momentum_bias = clamp(
        trend_signal["momentum_pct"] / max(MIN_MOMENTUM_PCT * 4, 1e-9),
        -1,
        1
    )
    distance_pct = pct_change(exit_price, entry_price)
    horizon_volatility_pct = trend_signal["volatility_pct"] * math.sqrt(
        max(ORDERBOOK_EXIT_HORIZON_HOURS, 0.01)
        / max(HISTORY_WINDOW_HOURS, 1)
    )
    horizon_reachability = clamp(
        horizon_volatility_pct / max(distance_pct, 1e-9),
        0,
        1
    )
    distance_penalty = clamp(
        distance_pct / max(max(ORDERBOOK_EXIT_TARGET_PCTS), 1e-9),
        0,
        1
    )
    probability = (
        0.25
        + 0.35 * trend_bias
        + 0.20 * max(0, momentum_bias)
        + 0.15 * (1 - resistance_ratio)
        + 0.10 * horizon_reachability
        - 0.15 * distance_penalty
        + trend_signal.get("price_intelligence", {}).get(
            "exit_probability_nudge",
            0
        )
    )
    return clamp(probability, 0.05, 0.95)


def estimate_entry_probability(price, entry_price, book, trend_signal):
    drop_pct = max(0, pct_change(price, entry_price))
    asks_near = depth_notional(
        book["asks"],
        lambda ask_price: price <= ask_price <= price * (1 + ORDERBOOK_PRESSURE_WINDOW_PCT)
    )
    bids_to_entry = depth_notional(
        book["bids"],
        lambda bid_price: entry_price <= bid_price <= price
    )
    pressure_total = max(asks_near + bids_to_entry, 1e-9)
    sell_pressure = asks_near / pressure_total
    trend_down_bias = clamp((-trend_signal["trend_score"] + 1) / 2, 0, 1)
    distance_penalty = clamp(
        drop_pct / max(ORDERBOOK_MAX_ENTRY_DROP_PCT, 1e-9),
        0,
        1
    )
    probability = (
        0.15
        + 0.45 * sell_pressure
        + 0.25 * trend_down_bias
        + 0.15 * (1 - distance_penalty)
        + trend_signal.get("price_intelligence", {}).get(
            "entry_probability_nudge",
            0
        )
    )
    return clamp(probability, 0.03, 0.9)


def candidate_csv_kwargs(candidate):
    if not candidate:
        return {
            "candidate_entry_price": None,
            "candidate_exit_price": None,
            "candidate_entry_probability": None,
            "candidate_exit_probability": None,
            "candidate_joint_probability": None,
            "candidate_expected_value_pct": None,
            "candidate_entry_drop_pct": None,
            "candidate_exit_target_pct": None,
            "candidate_exit_horizon_hours": None,
            "predicted_expected_value_pct": None,
            "predicted_entry_probability": None,
            "predicted_exit_probability": None,
            "predicted_joint_probability": None
        }

    return {
        "candidate_entry_price": candidate.get("entry_price"),
        "candidate_exit_price": candidate.get("exit_price"),
        "candidate_entry_probability": candidate.get("entry_probability"),
        "candidate_exit_probability": candidate.get("exit_probability"),
        "candidate_joint_probability": candidate.get("joint_probability"),
        "candidate_expected_value_pct": candidate.get("expected_value_pct"),
        "candidate_entry_drop_pct": candidate.get("entry_drop_pct"),
        "candidate_exit_target_pct": candidate.get("exit_target_pct"),
        "candidate_exit_horizon_hours": candidate.get("exit_horizon_hours"),
        "predicted_expected_value_pct": candidate.get("expected_value_pct"),
        "predicted_entry_probability": candidate.get("entry_probability"),
        "predicted_exit_probability": candidate.get("exit_probability"),
        "predicted_joint_probability": candidate.get("joint_probability")
    }


def prediction_metadata(candidate):
    if not candidate:
        return {}

    return {
        "candidate_entry_price": candidate.get("entry_price"),
        "candidate_exit_price": candidate.get("exit_price"),
        "candidate_entry_probability": candidate.get("entry_probability"),
        "candidate_exit_probability": candidate.get("exit_probability"),
        "candidate_joint_probability": candidate.get("joint_probability"),
        "candidate_expected_value_pct": candidate.get("expected_value_pct"),
        "candidate_entry_drop_pct": candidate.get("entry_drop_pct"),
        "candidate_exit_target_pct": candidate.get("exit_target_pct"),
        "candidate_exit_horizon_hours": candidate.get("exit_horizon_hours")
    }


def compute_orderbook_candidates(price, trend_signal):
    book = get_orderbook()
    if book is None:
        return {
            "usable": False,
            "reason": "missing_orderbook",
            "candidates": []
        }

    candidates = []
    entry_summaries = []
    for index in range(ORDERBOOK_CANDIDATE_COUNT):
        entry_drop_pct = ORDERBOOK_ENTRY_STEP_PCT * (index + 1)
        if entry_drop_pct > ORDERBOOK_MAX_ENTRY_DROP_PCT:
            continue

        entry_price = price * (1 - entry_drop_pct)
        entry_probability = estimate_entry_probability(
            price,
            entry_price,
            book,
            trend_signal
        )
        exit_candidates = []

        for exit_target_pct in ORDERBOOK_EXIT_TARGET_PCTS:
            exit_price = entry_price * (1 + exit_target_pct + ROUND_TRIP_FEE_PCT)
            exit_probability = estimate_exit_probability(
                entry_price,
                exit_price,
                book,
                trend_signal
            )
            joint_probability = entry_probability * exit_probability
            gross_reward_pct = exit_target_pct
            expected_value_pct = (
                entry_probability
                * (
                    exit_probability * gross_reward_pct
                    - (1 - exit_probability) * entry_drop_pct
                )
                + trend_signal.get("price_intelligence", {}).get(
                    "expected_value_nudge_pct",
                    0
                )
            )
            candidates.append(
                {
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "entry_drop_pct": entry_drop_pct,
                    "exit_target_pct": exit_target_pct,
                    "exit_horizon_hours": ORDERBOOK_EXIT_HORIZON_HOURS,
                    "entry_probability": entry_probability,
                    "exit_probability": exit_probability,
                    "joint_probability": joint_probability,
                    "expected_value_pct": expected_value_pct
                }
            )
            exit_candidates.append(candidates[-1])

        if exit_candidates:
            entry_summaries.append(
                max(exit_candidates, key=lambda item: item["expected_value_pct"])
            )

    if not candidates:
        return {
            "usable": False,
            "reason": "no_orderbook_candidates",
            "candidates": []
        }

    best = max(entry_summaries, key=lambda item: item["expected_value_pct"])
    return {
        "usable": True,
        "best": best,
        "candidates": candidates,
        "entry_summaries": entry_summaries
    }


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


def sell_target_price_for_order(order, buy_price):
    exit_price = order.get("candidate_exit_price")
    if exit_price:
        return float(exit_price)
    return sell_target_price(buy_price)


def place_limit_buy(
    price,
    volume,
    cycle_id,
    candidate_exit_price=None,
    candidate=None
):
    buy_price = round_price(price)
    buy_volume = round_volume(volume)
    candidate_meta = prediction_metadata(candidate)

    if DRY_RUN:
        state["stats"]["dry_run_trades"] += 1
        state["stats"]["buy_orders_placed"] += 1
        txid = f"dry_run_buy_{int(time.time() * 1000)}"
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": buy_volume,
            "price": buy_price,
            "placed_at": cycle_id,
            "candidate_exit_price": candidate_exit_price,
            **candidate_meta,
            "trade_id": txid,
            "dry_run": True
        }
        save_state(state)
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": True,
                "side": "buy",
                "volume": buy_volume,
                "price": buy_price,
                "txid": txid,
                **candidate_meta
            }
        )
        log_and_console(
            "DRY_RUN_BUY",
            message=f"limit buy {buy_volume} @ {buy_price}",
            cycle_id=cycle_id,
            side="buy",
            volume=buy_volume,
            price=buy_price,
            txid=txid,
            **candidate_meta
        )
        return {"dry_run": True, "result": {"txid": [txid]}}

    result = safe_kraken_private(
        "BUY",
        "/0/private/AddOrder",
        {
            "pair": KRAKEN_PAIR,
            "type": "buy",
            "ordertype": "limit",
            "price": str(buy_price),
            "volume": buy_volume
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
                "volume": buy_volume,
                "price": buy_price,
                "txid": txid,
                **fill,
                **candidate_meta
            }
        )
        log_and_console(
            "BUY_ORDER_PLACED",
            message=f"limit buy {buy_volume} @ {buy_price}",
            cycle_id=cycle_id,
            side="buy",
            volume=buy_volume,
            price=buy_price,
            txid=txid,
            **fill,
            **candidate_meta,
            result=result.get("result")
        )
        notify_order_tracker(
            trade_id=txid,
            side="buy",
            price=tracker_value(fill.get("fill_price"), buy_price),
            quantity=tracker_value(fill.get("fill_volume"), buy_volume),
            order_id=txid,
            fee=fill.get("fill_fee"),
            timestamp=cycle_id,
            notes=(
                None
                if fill.get("order_status") == "closed"
                else f"order_status={fill.get('order_status') or 'submitted'}"
            )
        )
        result["fill"] = fill

    return result


def place_limit_sell(price, volume, cycle_id, buy_txid=None, buy_price=None):
    sell_price = round_price(price)
    sell_volume = round_volume(volume)

    if DRY_RUN:
        txid = f"dry_run_sell_{int(time.time() * 1000)}"
        state["stats"]["sell_orders_placed"] += 1
        state["open_sell_orders"][txid] = {
            "txid": txid,
            "volume": sell_volume,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "buy_txid": buy_txid,
            "placed_at": cycle_id,
            "trade_id": buy_txid or txid,
            "dry_run": True
        }
        save_state(state)
        log_and_console(
            "DRY_RUN_SELL",
            message=f"sell {sell_volume} @ {sell_price}",
            cycle_id=cycle_id,
            side="sell",
            volume=sell_volume,
            price=sell_price,
            buy_txid=buy_txid,
            buy_price=buy_price,
            txid=txid
        )
        return {"dry_run": True, "result": {"txid": [txid]}}

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
            buy_txid=buy_txid
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


def cancel_order(txid):
    if DRY_RUN and str(txid).startswith("dry_run_"):
        return {"dry_run": True, "result": {"count": 1}}

    return safe_kraken_private(
        "CANCEL_ORDER",
        "/0/private/CancelOrder",
        {"txid": txid}
    )


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


def place_profit_sell_for_buy(cycle_id, buy_txid, buy_price, volume, order=None):
    order = order or {}
    target_price = sell_target_price_for_order(order, buy_price)
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="sell",
        reason="profit_target_after_buy_fill",
        buy_txid=buy_txid,
        buy_price=buy_price,
        price=round_price(target_price),
        volume=round_volume(volume),
        target_profit_pct=(
            pct_change(target_price, buy_price) - ROUND_TRIP_FEE_PCT
        ),
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT
    )
    return place_limit_sell(
        target_price,
        volume,
        cycle_id,
        buy_txid=buy_txid,
        buy_price=buy_price
    )


def order_age_minutes(order, now):
    placed_at = parse_iso8601(order.get("placed_at"))
    if placed_at is None:
        return None

    return (now - placed_at).total_seconds() / 60


def revalidation_candidate_for_order(order, current_price, trend_signal):
    try:
        result = compute_orderbook_candidates(current_price, trend_signal)
    except Exception as e:
        log_event(
            "OPEN_BUY_REVALIDATION_ERROR",
            message=str(e),
            txid=order.get("txid")
        )
        return None

    if not result.get("usable"):
        return None

    order_price = float(order.get("price") or 0)
    best_match = None
    best_distance = None
    for candidate in result.get("candidates", []):
        distance = abs(float(candidate["entry_price"]) - order_price)
        if best_distance is None or distance < best_distance:
            best_match = candidate
            best_distance = distance

    return best_match


def cancel_open_buy_order(txid, order, cycle_id, reason, **kwargs):
    cancel_result = cancel_order(txid)
    if not cancel_result:
        log_event(
            "OPEN_BUY_CANCEL_FAILED",
            cycle_id=cycle_id,
            txid=txid,
            side="buy",
            reason=reason,
            **kwargs
        )
        return False

    if txid in state["open_buy_orders"]:
        del state["open_buy_orders"][txid]
        save_state(state)

    log_and_console(
        "OPEN_BUY_CANCELLED",
        message=f"buy order cancelled: {reason}",
        cycle_id=cycle_id,
        txid=txid,
        side="buy",
        price=order.get("price"),
        volume=order.get("volume"),
        reason=reason,
        result=cancel_result.get("result") if isinstance(cancel_result, dict) else cancel_result,
        **kwargs
    )
    notify_order_tracker(
        trade_id=order.get("trade_id") or txid,
        side="buy",
        price=order.get("price"),
        quantity=order.get("volume"),
        order_id=txid,
        timestamp=cycle_id,
        notes=f"order_status=canceled; reason={reason}",
        status="canceled"
    )
    return True


def maybe_cancel_stale_open_buy(txid, order, cycle_id, current_price, trend_signal):
    now = parse_iso8601(cycle_id) or datetime.now(timezone.utc)
    age_minutes = order_age_minutes(order, now)

    if (
        MAX_OPEN_BUY_AGE_MINUTES > 0
        and age_minutes is not None
        and age_minutes >= MAX_OPEN_BUY_AGE_MINUTES
    ):
        return cancel_open_buy_order(
            txid,
            order,
            cycle_id,
            "open_buy_age_limit",
            age_minutes=age_minutes,
            max_open_buy_age_minutes=MAX_OPEN_BUY_AGE_MINUTES
        )

    if not REVALIDATE_OPEN_BUYS or current_price is None or trend_signal is None:
        return False

    if (
        OPEN_BUY_REVALIDATION_GRACE_MINUTES > 0
        and age_minutes is not None
        and age_minutes < OPEN_BUY_REVALIDATION_GRACE_MINUTES
    ):
        return False

    order_price = float(order.get("price") or 0)
    if order_price > 0:
        distance_above_entry_pct = pct_change(current_price, order_price)
        if (
            distance_above_entry_pct >= 0
            and distance_above_entry_pct <= OPEN_BUY_REVALIDATION_HOLD_NEAR_ENTRY_PCT
        ):
            return False

    candidate = revalidation_candidate_for_order(order, current_price, trend_signal)
    if candidate is None:
        return False

    expected_value_pct = candidate.get("expected_value_pct")
    exit_probability = candidate.get("exit_probability")
    if (
        expected_value_pct is not None
        and expected_value_pct < OPEN_BUY_REVALIDATION_MIN_EXPECTED_VALUE_PCT
    ):
        return cancel_open_buy_order(
            txid,
            order,
            cycle_id,
            "open_buy_ev_below_min",
            age_minutes=age_minutes,
            revalidated_expected_value_pct=expected_value_pct,
            min_expected_value_pct=OPEN_BUY_REVALIDATION_MIN_EXPECTED_VALUE_PCT
        )

    if (
        exit_probability is not None
        and exit_probability < OPEN_BUY_REVALIDATION_MIN_EXIT_PROBABILITY
    ):
        return cancel_open_buy_order(
            txid,
            order,
            cycle_id,
            "open_buy_exit_probability_below_min",
            age_minutes=age_minutes,
            revalidated_exit_probability=exit_probability,
            min_exit_probability=OPEN_BUY_REVALIDATION_MIN_EXIT_PROBABILITY
        )

    return False


def process_open_buy_orders(cycle_id, current_price=None, trend_signal=None):
    for txid, order in list(state["open_buy_orders"].items()):
        if DRY_RUN and order.get("dry_run"):
            if current_price is None or current_price > order.get("price", 0):
                if maybe_cancel_stale_open_buy(
                    txid,
                    order,
                    cycle_id,
                    current_price,
                    trend_signal
                ):
                    continue
                continue

            fill = {
                "volume": order.get("volume"),
                "price": order.get("price"),
                "cost": order.get("volume", 0) * order.get("price", 0),
                "fee": None
            }
            state["stats"]["buy_orders_filled"] += 1
            del state["open_buy_orders"][txid]
            save_state(state)
            log_and_console(
                "DRY_RUN_BUY_FILLED",
                message=f"buy filled @ {round_price(fill['price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=fill["volume"],
                price=fill["price"],
                cost=fill["cost"]
            )
            place_profit_sell_for_buy(
                cycle_id,
                txid,
                fill["price"],
                fill["volume"],
                order=order
            )
            continue

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
                order=order
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
            notify_order_tracker(
                trade_id=order.get("trade_id") or txid,
                side="buy",
                price=order.get("price"),
                quantity=order.get("volume"),
                order_id=txid,
                timestamp=cycle_id,
                notes=f"order_status={order_status}",
                status=order_status
            )
        elif order_status == "open":
            maybe_cancel_stale_open_buy(
                txid,
                order,
                cycle_id,
                current_price,
                trend_signal
            )


def process_open_sell_orders(cycle_id, current_price=None):
    for txid, order in list(state["open_sell_orders"].items()):
        if DRY_RUN and order.get("dry_run"):
            if current_price is None or current_price < order.get("sell_price", 0):
                continue

            state["stats"]["sell_orders_filled"] += 1
            state["last_sell_price"] = order.get("sell_price")
            state["last_sell_at"] = cycle_id
            del state["open_sell_orders"][txid]
            save_state(state)
            log_and_console(
                "DRY_RUN_SELL_FILLED",
                message=f"sell filled @ {round_price(state['last_sell_price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=order.get("volume"),
                price=state["last_sell_price"],
                buy_price=order.get("buy_price")
            )
            continue

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
            notify_order_tracker(
                trade_id=order.get("trade_id") or order.get("buy_txid") or txid,
                side="sell",
                price=order.get("sell_price"),
                quantity=order.get("volume"),
                order_id=txid,
                timestamp=cycle_id,
                notes=f"order_status={order_status}",
                status=order_status
            )


def maybe_handle_submitted_buy(
    result,
    cycle_id,
    volume,
    price,
    candidate_exit_price=None,
    candidate=None
):
    if DRY_RUN:
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
        place_profit_sell_for_buy(
            cycle_id,
            txid,
            fill_price,
            fill_volume,
            order={
                "candidate_exit_price": candidate_exit_price,
                **prediction_metadata(candidate)
            }
        )
        return

    if txid:
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": volume,
            "price": price,
            "placed_at": cycle_id,
            "candidate_exit_price": candidate_exit_price,
            "trade_id": txid,
            **prediction_metadata(candidate)
        }
        save_state(state)


# ----------------------
# EXECUTOR
# ----------------------


def signal_csv_kwargs(signal):
    price_intelligence = signal.get("price_intelligence") or {}
    return {
        "trend_score": signal.get("trend_score"),
        "momentum_pct": signal.get("momentum_pct"),
        "ma_spread_pct": signal.get("ma_spread_pct"),
        "breakout_pct": signal.get("breakout_pct"),
        "volatility_pct": signal.get("volatility_pct"),
        "range_position": signal.get("range_position"),
        "price_intelligence_status": price_intelligence.get("status"),
        "price_intelligence_age_minutes": price_intelligence.get("age_minutes"),
        "price_intelligence_regime_score": price_intelligence.get("regime_score"),
        "price_intelligence_entry_nudge": (
            price_intelligence.get("entry_probability_nudge")
        ),
        "price_intelligence_exit_nudge": (
            price_intelligence.get("exit_probability_nudge")
        ),
        "price_intelligence_ev_nudge_pct": (
            price_intelligence.get("expected_value_nudge_pct")
        ),
        "price_intelligence_rsi_7d": price_intelligence.get("rsi_7d"),
        "price_intelligence_rsi_14d": price_intelligence.get("rsi_14d"),
        "price_intelligence_above_200d_sma": (
            price_intelligence.get("above_200d_sma")
        ),
        "price_intelligence_distance_from_sma_200d_pct": (
            price_intelligence.get("distance_from_sma_200d_pct")
        ),
        "price_intelligence_volatility_regime": (
            price_intelligence.get("volatility_regime")
        )
    }


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


def cooldown_active(now, trend_score):
    last_trade_at = parse_iso8601(state.get("last_trade_at"))
    if REBALANCE_COOLDOWN_MINUTES <= 0 or last_trade_at is None:
        return None

    elapsed_minutes = (now - last_trade_at).total_seconds() / 60
    if elapsed_minutes >= REBALANCE_COOLDOWN_MINUTES:
        return None

    if trend_score >= COOLDOWN_OVERRIDE_SCORE:
        return None

    return {
        "elapsed_minutes": elapsed_minutes,
        "cooldown_minutes": REBALANCE_COOLDOWN_MINUTES,
        "cooldown_override_score": COOLDOWN_OVERRIDE_SCORE
    }


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

    process_open_buy_orders(cycle_id, current_price=price)
    process_open_sell_orders(cycle_id, current_price=price)

    try:
        trend_signal = compute_trend_signal(price)
    except Exception as e:
        log_event("TREND_SIGNAL_ERROR", cycle_id=cycle_id, message=str(e))
        skip_cycle("trend_signal_error", cycle_id, price=price)
        return

    if not trend_signal.get("usable"):
        skip_cycle(
            trend_signal.get("reason", "unusable_trend_signal"),
            cycle_id,
            price=price,
            sample_count=trend_signal.get("sample_count")
        )
        return

    state["last_signal"] = trend_signal
    save_state(state)

    process_open_buy_orders(
        cycle_id,
        current_price=price,
        trend_signal=trend_signal
    )

    trend_score = trend_signal["trend_score"]
    orderbook_result = compute_orderbook_candidates(price, trend_signal)
    best_candidate = orderbook_result.get("best")
    signal_fields = {
        **signal_csv_kwargs(trend_signal),
        **candidate_csv_kwargs(best_candidate),
        "open_buy_orders_today": open_buy_orders_for_day(now),
        "open_buy_order_date": trading_day_key(now),
        "max_open_buy_orders_per_day": MAX_OPEN_BUY_ORDERS_PER_DAY
    }
    log_event(
        "SIGNAL_UPDATE",
        cycle_id=cycle_id,
        price=price,
        sample_count=trend_signal["sample_count"],
        fast_ma=trend_signal["fast_ma"],
        slow_ma=trend_signal["slow_ma"],
        range_low=trend_signal["range_low"],
        range_high=trend_signal["range_high"],
        orderbook_candidate_count=len(orderbook_result.get("candidates", [])),
        **signal_fields
    )

    console(
        "Price: "
        f"{price} | Trend score: {round(trend_score, 4)} | "
        f"Momentum: {round(trend_signal['momentum_pct'], 5)} | "
        f"Best EV: "
        f"{None if not best_candidate else round(best_candidate['expected_value_pct'], 5)}"
    )

    if not orderbook_result.get("usable"):
        skip_cycle(
            orderbook_result.get("reason", "unusable_orderbook_model"),
            cycle_id,
            price=price,
            **signal_fields
        )
        return

    if trend_signal["volatility_pct"] > MAX_VOLATILITY_PCT:
        skip_cycle(
            "volatility_above_limit",
            cycle_id,
            price=price,
            max_volatility_pct=MAX_VOLATILITY_PCT,
            **signal_fields
        )
        return

    if trend_score < TREND_EXIT_THRESHOLD:
        skip_cycle(
            "trend_score_below_exit_threshold",
            cycle_id,
            price=price,
            trend_exit_threshold=TREND_EXIT_THRESHOLD,
            **signal_fields
        )
        return

    if best_candidate["entry_probability"] < ORDERBOOK_MIN_ENTRY_PROBABILITY:
        skip_cycle(
            "entry_probability_below_min",
            cycle_id,
            price=price,
            orderbook_min_entry_probability=ORDERBOOK_MIN_ENTRY_PROBABILITY,
            **signal_fields
        )
        return

    if best_candidate["exit_probability"] < ORDERBOOK_MIN_EXIT_PROBABILITY:
        skip_cycle(
            "exit_probability_below_min",
            cycle_id,
            price=price,
            orderbook_min_exit_probability=ORDERBOOK_MIN_EXIT_PROBABILITY,
            **signal_fields
        )
        return

    if best_candidate["expected_value_pct"] < ORDERBOOK_MIN_EXPECTED_VALUE_PCT:
        skip_cycle(
            "expected_value_below_min",
            cycle_id,
            price=price,
            orderbook_min_expected_value_pct=ORDERBOOK_MIN_EXPECTED_VALUE_PCT,
            **signal_fields
        )
        return

    cooldown = cooldown_active(now, trend_score)
    if cooldown is not None:
        skip_cycle(
            "cooldown",
            cycle_id,
            price=price,
            **signal_fields,
            **cooldown
        )
        return

    last_sell_price = state.get("last_sell_price")
    if PREVENT_BUY_ABOVE_LAST_SELL and last_sell_price is not None:
        max_rebuy_price = float(last_sell_price) * (1 - BUY_AFTER_SELL_DISCOUNT_PCT)
        if price >= max_rebuy_price:
            skip_cycle(
                "price_above_last_sell",
                cycle_id,
                price=price,
                last_sell_price=last_sell_price,
                max_rebuy_price=max_rebuy_price,
                **signal_fields
            )
            return

    if len(state["open_sell_orders"]) >= MAX_OPEN_SELL_ORDERS:
        skip_cycle(
            "max_open_sell_orders",
            cycle_id,
            price=price,
            open_sell_count=len(state["open_sell_orders"]),
            **signal_fields
        )
        return

    open_buy_orders_today = open_buy_orders_for_day(now)
    if (
        MAX_OPEN_BUY_ORDERS_PER_DAY > 0
        and open_buy_orders_today >= MAX_OPEN_BUY_ORDERS_PER_DAY
    ):
        skip_cycle(
            "max_open_buy_orders_per_day",
            cycle_id,
            price=price,
            open_buy_orders_today=open_buy_orders_today,
            open_buy_order_date=trading_day_key(now),
            max_open_buy_orders_per_day=MAX_OPEN_BUY_ORDERS_PER_DAY,
            **signal_fields
        )
        return

    if (
        MAX_OPEN_BUY_ORDERS > 0
        and len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS
    ):
        skip_cycle(
            "max_open_buy_orders",
            cycle_id,
            price=price,
            open_buy_count=len(state["open_buy_orders"]),
            max_open_buy_orders=MAX_OPEN_BUY_ORDERS,
            **signal_fields
        )
        return

    open_order_count = (
        len(state["open_buy_orders"])
        + len(state["open_sell_orders"])
    )
    if MAX_OPEN_ORDERS > 0 and open_order_count >= MAX_OPEN_ORDERS:
        skip_cycle(
            "max_open_orders",
            cycle_id,
            price=price,
            open_order_count=open_order_count,
            open_buy_count=len(state["open_buy_orders"]),
            open_sell_count=len(state["open_sell_orders"]),
            max_open_orders=MAX_OPEN_ORDERS,
            **signal_fields
        )
        return

    deployed_inventory_usd = current_inventory_usd(price)
    if deployed_inventory_usd >= MAX_INVENTORY_USD:
        skip_cycle(
            "max_inventory_usd",
            cycle_id,
            price=price,
            deployed_inventory_usd=deployed_inventory_usd,
            max_inventory_usd=MAX_INVENTORY_USD,
            **signal_fields
        )
        return

    private_pause_seconds = kraken_private_pause_seconds()
    if private_pause_seconds > 0:
        skip_cycle(
            "kraken_private_paused",
            cycle_id,
            price=price,
            kraken_private_pause_seconds=round(private_pause_seconds, 2),
            kraken_private_paused_until=state.get("kraken_private_paused_until"),
            **signal_fields
        )
        return

    balances = get_balances()
    if balances is None:
        skip_cycle("balance_fetch_failed", cycle_id, price=price, **signal_fields)
        return

    base_balance, quote_balance = balances
    trade_value = quote_balance * POSITION_SIZE_PCT
    if MAX_TRADE_USD > 0:
        trade_value = min(trade_value, MAX_TRADE_USD)
    trade_value *= max(0, 1 - EXECUTION_BUFFER_PCT)

    projected_inventory = deployed_inventory_usd + trade_value
    if projected_inventory > MAX_INVENTORY_USD:
        trade_value = max(0, MAX_INVENTORY_USD - deployed_inventory_usd)

    if trade_value < MIN_TRADE_USD:
        skip_cycle(
            "small_trade",
            cycle_id,
            price=price,
            trade_value=trade_value,
            min_trade_usd=MIN_TRADE_USD,
            **signal_fields
        )
        return

    entry_price = best_candidate["entry_price"]
    exit_price = best_candidate["exit_price"]
    volume = round_volume(trade_value / entry_price)
    min_volume = get_min_order_volume()
    if volume <= 0 or volume < min_volume:
        skip_cycle(
            "below_min_volume",
            cycle_id,
            price=price,
            trade_value=trade_value,
            volume=volume,
            min_volume=min_volume,
            **signal_fields
        )
        return

    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="buy",
        reason="stats_trend_buy",
        volume=volume,
        price=entry_price,
        current_price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        **signal_fields
    )
    append_decision_csv(
        "trade_decision",
        cycle_id=cycle_id,
        side="buy",
        reason="stats_trend_buy",
        volume=volume,
        price=entry_price,
        current_price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        dry_run=DRY_RUN,
        **signal_fields
    )

    result = place_limit_buy(
        entry_price,
        volume,
        cycle_id,
        candidate_exit_price=exit_price,
        candidate=best_candidate
    )
    result_payload = result.get("result") if isinstance(result, dict) else None
    fill = result.get("fill", {}) if isinstance(result, dict) else {}
    txids = result_payload.get("txid", []) if isinstance(result_payload, dict) else []
    append_decision_csv(
        "trade_executed" if result else "trade_rejected",
        cycle_id=cycle_id,
        side="buy",
        reason="dry_run" if DRY_RUN else ("submitted" if result else "rejected"),
        volume=volume,
        price=entry_price,
        current_price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        dry_run=DRY_RUN,
        order_txid=txids[0] if txids else "",
        **signal_fields,
        **fill,
        result=result_payload or result
    )
    maybe_handle_submitted_buy(
        result,
        cycle_id,
        volume,
        entry_price,
        candidate_exit_price=exit_price,
        candidate=best_candidate
    )

    log_event(
        "CYCLE_SUMMARY",
        cycle_id=cycle_id,
        price=price,
        side="buy",
        volume=volume,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        open_buy_count=len(state["open_buy_orders"]),
        open_sell_count=len(state["open_sell_orders"]),
        deployed_inventory_usd=current_inventory_usd(price),
        last_sell_price=state.get("last_sell_price"),
        dry_run=DRY_RUN,
        **signal_fields
    )


def backtest_loop_sleep_seconds(on_error=False):
    if BACKTEST_MODE:
        if on_error:
            return max(BACKTEST_SLEEP_SECONDS, 1)
        return BACKTEST_SLEEP_SECONDS
    return PRICE_CHECK_INTERVAL_SECONDS


def step_backtest():
    if not BACKTEST_MODE or not BACKTEST_STEP_AFTER_CYCLE:
        return

    if BACKTEST_STEPS_PER_CYCLE <= 0:
        return

    url = KRAKEN_API_URL.rstrip("/") + BACKTEST_STEP_PATH
    try:
        r = requests.post(
            url,
            params={"steps": BACKTEST_STEPS_PER_CYCLE},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        try:
            payload = r.json()
        except Exception:
            payload = r.text

        log_event(
            "BACKTEST_STEP",
            steps=BACKTEST_STEPS_PER_CYCLE,
            url=url,
            result=payload
        )
    except Exception as e:
        log_event(
            "BACKTEST_STEP_ERROR",
            steps=BACKTEST_STEPS_PER_CYCLE,
            url=url,
            message=str(e)
        )
        raise


# ----------------------
# MAIN LOOP
# ----------------------


def main():
    require_runtime_config()

    log_and_console(
        "BOT_START",
        message="Stats/trend bot starting",
        strategy_profile=STRATEGY_PROFILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        decision_csv_file=DECISION_CSV_FILE,
        pair=KRAKEN_PAIR,
        dry_run=DRY_RUN,
        kraken_lockout_cooldown_seconds=KRAKEN_LOCKOUT_COOLDOWN_SECONDS,
        backtest_mode=BACKTEST_MODE,
        backtest_step_path=BACKTEST_STEP_PATH,
        backtest_steps_per_cycle=BACKTEST_STEPS_PER_CYCLE,
        backtest_step_after_cycle=BACKTEST_STEP_AFTER_CYCLE,
        backtest_sleep_seconds=BACKTEST_SLEEP_SECONDS,
        backtest_use_api_market_data=BACKTEST_USE_API_MARKET_DATA,
        market_history_source=MARKET_HISTORY_SOURCE,
        kraken_ohlc_interval_minutes=KRAKEN_OHLC_INTERVAL_MINUTES,
        price_intelligence_url=PRICE_INTELLIGENCE_URL,
        price_intelligence_enabled=PRICE_INTELLIGENCE_ENABLED,
        price_intelligence_max_age_minutes=PRICE_INTELLIGENCE_MAX_AGE_MINUTES,
        price_intelligence_max_probability_nudge=(
            PRICE_INTELLIGENCE_MAX_PROBABILITY_NUDGE
        ),
        price_intelligence_max_ev_nudge_pct=PRICE_INTELLIGENCE_MAX_EV_NUDGE_PCT,
        history_window_hours=HISTORY_WINDOW_HOURS,
        min_samples=MIN_SAMPLES,
        fast_ma_samples=FAST_MA_SAMPLES,
        slow_ma_samples=SLOW_MA_SAMPLES,
        momentum_lookback_samples=MOMENTUM_LOOKBACK_SAMPLES,
        breakout_lookback_samples=BREAKOUT_LOOKBACK_SAMPLES,
        trend_exit_threshold=TREND_EXIT_THRESHOLD,
        min_momentum_pct=MIN_MOMENTUM_PCT,
        min_ma_spread_pct=MIN_MA_SPREAD_PCT,
        breakout_buffer_pct=BREAKOUT_BUFFER_PCT,
        max_volatility_pct=MAX_VOLATILITY_PCT,
        orderbook_depth_count=ORDERBOOK_DEPTH_COUNT,
        orderbook_candidate_count=ORDERBOOK_CANDIDATE_COUNT,
        orderbook_entry_step_pct=ORDERBOOK_ENTRY_STEP_PCT,
        orderbook_exit_target_min_pct=ORDERBOOK_EXIT_TARGET_MIN_PCT,
        orderbook_exit_target_max_pct=ORDERBOOK_EXIT_TARGET_MAX_PCT,
        orderbook_exit_target_step_pct=ORDERBOOK_EXIT_TARGET_STEP_PCT,
        orderbook_exit_target_pcts=ORDERBOOK_EXIT_TARGET_PCTS,
        orderbook_exit_horizon_hours=ORDERBOOK_EXIT_HORIZON_HOURS,
        orderbook_min_entry_probability=ORDERBOOK_MIN_ENTRY_PROBABILITY,
        orderbook_min_exit_probability=ORDERBOOK_MIN_EXIT_PROBABILITY,
        orderbook_min_expected_value_pct=ORDERBOOK_MIN_EXPECTED_VALUE_PCT,
        orderbook_max_entry_drop_pct=ORDERBOOK_MAX_ENTRY_DROP_PCT,
        orderbook_pressure_window_pct=ORDERBOOK_PRESSURE_WINDOW_PCT,
        min_trade_usd=MIN_TRADE_USD,
        position_size_pct=POSITION_SIZE_PCT,
        max_trade_usd=MAX_TRADE_USD,
        max_open_buy_orders_per_day=MAX_OPEN_BUY_ORDERS_PER_DAY,
        max_open_buy_orders=MAX_OPEN_BUY_ORDERS,
        max_open_sell_orders=MAX_OPEN_SELL_ORDERS,
        max_open_orders=MAX_OPEN_ORDERS,
        max_open_buy_age_minutes=MAX_OPEN_BUY_AGE_MINUTES,
        revalidate_open_buys=REVALIDATE_OPEN_BUYS,
        open_buy_revalidation_grace_minutes=OPEN_BUY_REVALIDATION_GRACE_MINUTES,
        open_buy_revalidation_hold_near_entry_pct=(
            OPEN_BUY_REVALIDATION_HOLD_NEAR_ENTRY_PCT
        ),
        open_buy_revalidation_min_expected_value_pct=(
            OPEN_BUY_REVALIDATION_MIN_EXPECTED_VALUE_PCT
        ),
        open_buy_revalidation_min_exit_probability=(
            OPEN_BUY_REVALIDATION_MIN_EXIT_PROBABILITY
        ),
        max_inventory_usd=MAX_INVENTORY_USD,
        target_profit_pct=TARGET_PROFIT_PCT,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        price_check_interval_seconds=PRICE_CHECK_INTERVAL_SECONDS
    )

    while True:
        try:
            run_cycle()
            step_backtest()
            send_checkin(
                loop_count=state.get("stats", {}).get("cycles"),
                message="loop_complete"
            )
            time.sleep(backtest_loop_sleep_seconds())
        except KeyboardInterrupt:
            log_and_console("BOT_STOP", message="Stats/trend bot stopped")
            break
        except Exception as e:
            state["stats"]["errors"] += 1
            save_state(state)
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            send_checkin(
                status="error",
                loop_count=state.get("stats", {}).get("cycles"),
                message=short_error_summary(e)
            )
            time.sleep(backtest_loop_sleep_seconds(on_error=True))


if __name__ == "__main__":
    main()
