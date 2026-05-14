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
PRICE_LOG_URL = os.getenv("PRICE_LOG_URL")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")


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


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


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


REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
KRAKEN_NONCE_RETRIES = int(os.getenv("KRAKEN_NONCE_RETRIES", "2"))
DRY_RUN = profile_bool("dry_run", True)
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
ORDERBOOK_EXIT_TARGET_PCTS = profile_float_list(
    "orderbook_exit_target_pcts",
    [0.004, 0.007, 0.01]
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
    "candidate_entry_price",
    "candidate_exit_price",
    "candidate_entry_probability",
    "candidate_exit_probability",
    "candidate_joint_probability",
    "candidate_expected_value_pct",
    "candidate_entry_drop_pct",
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


def record_trade_history(entry):
    state.setdefault("trade_history", [])
    state["trade_history"].append(entry)
    state["trade_history"] = state["trade_history"][-250:]
    save_state(state)


# ----------------------
# KRAKEN HELPERS
# ----------------------


def require_runtime_config():
    required = {
        "KRAKEN_API_URL": KRAKEN_API_URL,
        "PRICE_LOG_URL": PRICE_LOG_URL
    }
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


def get_orderbook():
    try:
        if KRAKEN_ORDERBOOK_URL:
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


def load_price_history():
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
    raw_score = (
        0.35 * momentum_component
        + 0.35 * ma_component
        + 0.20 * breakout_component
        + 0.10 * range_component
    )
    trend_score = raw_score * (1 - 0.35 * volatility_penalty)

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
        "trend_score": trend_score
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
    distance_penalty = clamp(
        distance_pct / max(max(ORDERBOOK_EXIT_TARGET_PCTS), 1e-9),
        0,
        1
    )
    probability = (
        0.25
        + 0.35 * trend_bias
        + 0.20 * max(0, momentum_bias)
        + 0.20 * (1 - resistance_ratio)
        - 0.15 * distance_penalty
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
            "candidate_entry_drop_pct": None
        }

    return {
        "candidate_entry_price": candidate.get("entry_price"),
        "candidate_exit_price": candidate.get("exit_price"),
        "candidate_entry_probability": candidate.get("entry_probability"),
        "candidate_exit_probability": candidate.get("exit_probability"),
        "candidate_joint_probability": candidate.get("joint_probability"),
        "candidate_expected_value_pct": candidate.get("expected_value_pct"),
        "candidate_entry_drop_pct": candidate.get("entry_drop_pct")
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
            )
            candidates.append(
                {
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "entry_drop_pct": entry_drop_pct,
                    "exit_target_pct": exit_target_pct,
                    "entry_probability": entry_probability,
                    "exit_probability": exit_probability,
                    "joint_probability": joint_probability,
                    "expected_value_pct": expected_value_pct
                }
            )

    if not candidates:
        return {
            "usable": False,
            "reason": "no_orderbook_candidates",
            "candidates": []
        }

    best = max(candidates, key=lambda item: item["expected_value_pct"])
    return {
        "usable": True,
        "best": best,
        "candidates": candidates
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


def place_limit_buy(price, volume, cycle_id, candidate_exit_price=None):
    buy_price = round_price(price)
    buy_volume = round_volume(volume)

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
                "txid": txid
            }
        )
        log_and_console(
            "DRY_RUN_BUY",
            message=f"limit buy {buy_volume} @ {buy_price}",
            cycle_id=cycle_id,
            side="buy",
            volume=buy_volume,
            price=buy_price,
            txid=txid
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
                **fill
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
            result=result.get("result")
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


def process_open_buy_orders(cycle_id, current_price=None):
    for txid, order in list(state["open_buy_orders"].items()):
        if DRY_RUN and order.get("dry_run"):
            if current_price is None or current_price > order.get("price", 0):
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


def maybe_handle_submitted_buy(
    result,
    cycle_id,
    volume,
    price,
    candidate_exit_price=None
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
            order={"candidate_exit_price": candidate_exit_price}
        )
        return

    if txid:
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": volume,
            "price": price,
            "placed_at": cycle_id,
            "candidate_exit_price": candidate_exit_price
        }
        save_state(state)


# ----------------------
# EXECUTOR
# ----------------------


def signal_csv_kwargs(signal):
    return {
        "trend_score": signal.get("trend_score"),
        "momentum_pct": signal.get("momentum_pct"),
        "ma_spread_pct": signal.get("ma_spread_pct"),
        "breakout_pct": signal.get("breakout_pct"),
        "volatility_pct": signal.get("volatility_pct"),
        "range_position": signal.get("range_position")
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

    trend_score = trend_signal["trend_score"]
    orderbook_result = compute_orderbook_candidates(price, trend_signal)
    best_candidate = orderbook_result.get("best")
    signal_fields = {
        **signal_csv_kwargs(trend_signal),
        **candidate_csv_kwargs(best_candidate)
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

    if len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS:
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
    if open_order_count >= MAX_OPEN_ORDERS:
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
        candidate_exit_price=exit_price
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
        candidate_exit_price=exit_price
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
        orderbook_exit_target_pcts=ORDERBOOK_EXIT_TARGET_PCTS,
        orderbook_min_entry_probability=ORDERBOOK_MIN_ENTRY_PROBABILITY,
        orderbook_min_exit_probability=ORDERBOOK_MIN_EXIT_PROBABILITY,
        orderbook_min_expected_value_pct=ORDERBOOK_MIN_EXPECTED_VALUE_PCT,
        orderbook_max_entry_drop_pct=ORDERBOOK_MAX_ENTRY_DROP_PCT,
        orderbook_pressure_window_pct=ORDERBOOK_PRESSURE_WINDOW_PCT,
        min_trade_usd=MIN_TRADE_USD,
        position_size_pct=POSITION_SIZE_PCT,
        max_trade_usd=MAX_TRADE_USD,
        max_open_buy_orders=MAX_OPEN_BUY_ORDERS,
        max_open_sell_orders=MAX_OPEN_SELL_ORDERS,
        max_open_orders=MAX_OPEN_ORDERS,
        max_inventory_usd=MAX_INVENTORY_USD,
        target_profit_pct=TARGET_PROFIT_PCT,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        price_check_interval_seconds=PRICE_CHECK_INTERVAL_SECONDS
    )

    while True:
        try:
            run_cycle()
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            log_and_console("BOT_STOP", message="Stats/trend bot stopped")
            break
        except Exception as e:
            state["stats"]["errors"] += 1
            save_state(state)
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
