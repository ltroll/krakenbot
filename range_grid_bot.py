#!/usr/bin/env python3

# =====================================================
# RANGE GRID SENTIMENT BOT (RESTORED STABLE VERSION)
# =====================================================

import base64
import hashlib
import hmac
import json
import os
import statistics
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import krakenex
import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

CONFIG_FILE = (
    os.getenv("RANGE_GRID_CONFIG_FILE")
    or os.getenv("BOT_CONFIG_FILE")
    or "range_grid_config.json"
)
STRATEGY_PROFILE = (
    os.getenv("RANGE_GRID_STRATEGY_PROFILE")
    or os.getenv("STRATEGY_PROFILE")
    or "range_grid_strategy_default.json"
)
STATE_FILE = (
    os.getenv("RANGE_GRID_STATE_FILE")
    or os.getenv("BOT_STATE_FILE")
    or "last_state.json"
)
LOG_FILE = (
    os.getenv("RANGE_GRID_TRADE_LOG_FILE")
    or os.getenv("TRADE_LOG_FILE")
    or "trade_log.jsonl"
)

KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")
PRICE_LOG_URL = os.getenv("PRICE_LOG_URL")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
KRAKEN_NONCE_RETRIES = int(os.getenv("KRAKEN_NONCE_RETRIES", "2"))
KRAKEN_LOCKOUT_COOLDOWN_SECONDS = int(
    os.getenv("KRAKEN_LOCKOUT_COOLDOWN_SECONDS", "300")
)
SELL_INSUFFICIENT_FUNDS_COOLDOWN_SECONDS = int(
    os.getenv(
        "SELL_INSUFFICIENT_FUNDS_COOLDOWN_SECONDS",
        str(KRAKEN_LOCKOUT_COOLDOWN_SECONDS)
    )
)


def parse_strategy_modes(raw_value):
    if not raw_value:
        return ["low"]

    modes = [
        mode.strip().lower()
        for mode in raw_value.split(",")
        if mode.strip()
    ]
    return modes or ["low"]

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


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


def profile_str(name, default=""):
    value = strategy_config.get(name, default)
    return default if value is None else str(value)


strategy_config = load_strategy_config()

order_tracker_url = (
    os.getenv("ORDER_TRACKER_URL")
    or os.getenv("EXTERNAL_ORDER_TRACKER_URL")
)
order_tracker_user_agent = os.getenv("ORDER_TRACKER_USER_AGENT")
order_tracker_symbol = (
    os.getenv("ORDER_TRACKER_SYMBOL")
    or profile_str("order_tracker_symbol", KRAKEN_PAIR)
)
order_tracker_timeout_seconds = profile_float("order_tracker_timeout_seconds", 5)
order_tracker_checkin_timeout_seconds = profile_float(
    "order_tracker_checkin_timeout_seconds",
    min(order_tracker_timeout_seconds, 5)
)

range_window_hours = profile_int("range_window_hours", 24)
max_grid_size = profile_int("max_grid_size", 4)
profit_target_pct = profile_float("profit_target_pct", 0.01)
entry_step_pct = profile_float("entry_step_pct", profit_target_pct / 2)
round_trip_fee_pct = profile_float("round_trip_fee_pct", 0.0032)
position_size_pct = profile_float("position_size_pct", 0.10)
min_buy_notional_usd = profile_float("min_buy_notional_usd", 8.0)
min_buy_volume_btc = profile_float("min_buy_volume_btc", 0.00010)
execution_signal_threshold = profile_float("execution_signal_threshold", 0.0)
llm_target_proximity_pct = profile_float(
    "llm_target_proximity_pct",
    entry_step_pct
)
llm_target_min_signal = profile_float(
    "llm_target_min_signal",
    min(-0.05, execution_signal_threshold)
)
low_min_signal = profile_float(
    "low_min_signal",
    min(-0.05, execution_signal_threshold)
)
mean_min_signal = profile_float(
    "mean_min_signal",
    execution_signal_threshold
)
median_min_signal = profile_float(
    "median_min_signal",
    execution_signal_threshold
)
high_min_signal = profile_float(
    "high_min_signal",
    max(0.05, execution_signal_threshold)
)
min_signal_status = strategy_config.get("min_signal_status", "fresh")
risk_multiplier_floor = profile_float("risk_multiplier_floor", 0.75)
risk_multiplier_ceiling = profile_float("risk_multiplier_ceiling", 1.15)
flow_defensive_threshold = profile_float("flow_defensive_threshold", -0.20)
flow_block_threshold = profile_float("flow_block_threshold", -0.40)
flow_defensive_size_multiplier = profile_float(
    "flow_defensive_size_multiplier",
    0.75
)
flow_block_high_only = bool(
    strategy_config.get("flow_block_high_only", True)
)
flow_block_llm_only_below = profile_float("flow_block_llm_only_below", -0.50)
mean_reversion_min_opportunity = profile_float(
    "mean_reversion_min_opportunity",
    0.0
)
require_fresh_signal = bool(strategy_config.get("require_fresh_signal", True))
price_check_interval_seconds = profile_int("price_check_interval_seconds", 120)
range_refresh_interval_minutes = profile_int("range_refresh_interval_minutes", 60)
max_open_sell_orders = profile_int("max_open_sell_orders", 999999)
max_inventory_usd = profile_float("max_inventory_usd", 1e18)
aging_start_minutes = profile_int("aging_start_minutes", 999999)
aging_step_minutes = profile_int("aging_step_minutes", 60)
aging_profit_reduction_pct = profile_float("aging_profit_reduction_pct", 0.0)
min_profit_target_pct = profile_float(
    "min_profit_target_pct",
    profit_target_pct
)
high_anchor_buy_cooldown_minutes = profile_int(
    "high_anchor_buy_cooldown_minutes",
    15
)
max_open_high_anchor_orders = profile_int("max_open_high_anchor_orders", 3)
high_anchor_profit_target_pct = profile_float(
    "high_anchor_profit_target_pct",
    profit_target_pct
)
prevent_buy_above_last_sell = bool(
    strategy_config.get("prevent_buy_above_last_sell", True)
)
buy_after_sell_discount_pct = profile_float(
    "buy_after_sell_discount_pct",
    0.001
)
llm_buy_cooldown_minutes_after_sell = profile_int(
    "llm_buy_cooldown_minutes_after_sell",
    30
)
sentiment_defensive_threshold = profile_float(
    "sentiment_defensive_threshold",
    max(0.03, execution_signal_threshold)
)
sentiment_risk_on_threshold = profile_float("sentiment_risk_on_threshold", 0.12)
sentiment_defensive_size_multiplier = profile_float(
    "sentiment_defensive_size_multiplier",
    0.65
)
sentiment_risk_on_size_multiplier = profile_float(
    "sentiment_risk_on_size_multiplier",
    1.2
)
sentiment_defensive_inventory_multiplier = profile_float(
    "sentiment_defensive_inventory_multiplier",
    0.7
)
sentiment_risk_on_inventory_multiplier = profile_float(
    "sentiment_risk_on_inventory_multiplier",
    1.2
)
sentiment_defensive_open_sell_multiplier = profile_float(
    "sentiment_defensive_open_sell_multiplier",
    0.75
)
sentiment_risk_on_open_sell_multiplier = profile_float(
    "sentiment_risk_on_open_sell_multiplier",
    1.25
)
sentiment_disable_high_anchor_below = profile_float(
    "sentiment_disable_high_anchor_below",
    0.05
)
sentiment_defensive_extra_aging_reduction_pct = profile_float(
    "sentiment_defensive_extra_aging_reduction_pct",
    0.001
)
grid_anchor = strategy_config.get("grid_anchor", "low").strip().lower()
strategy_modes = parse_strategy_modes(grid_anchor)

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
    if not order_tracker_url or not order_tracker_user_agent:
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
        "symbol": order_tracker_symbol,
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
            order_tracker_url,
            data=payload,
            headers={"User-Agent": order_tracker_user_agent},
            timeout=order_tracker_timeout_seconds
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
    if not order_tracker_url or not order_tracker_user_agent:
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
            order_tracker_url,
            data=payload,
            headers={"User-Agent": order_tracker_user_agent},
            timeout=order_tracker_checkin_timeout_seconds
        )
        response.raise_for_status()
    except Exception as e:
        log_event("ORDER_TRACKER_CHECKIN_ERROR", message=str(e), status=status)


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


def next_nonce():
    wall_nonce = int(time.time() * 1000)
    last_nonce = int(state.get("last_nonce", 0))
    nonce = max(wall_nonce, last_nonce + 1)
    state["last_nonce"] = nonce
    save_state(state)
    return str(nonce)


def kraken_signature(endpoint, data):
    postdata = urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()
    message = endpoint.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(
        base64.b64decode(KRAKEN_API_SECRET),
        message,
        hashlib.sha512
    )
    return base64.b64encode(mac.digest()).decode()


def kraken_private(endpoint, data=None):
    payload = dict(data or {})
    payload["nonce"] = next_nonce()
    url = KRAKEN_API_URL.rstrip("/") + endpoint
    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": kraken_signature(endpoint, payload)
    }

    response = requests.post(
        url,
        headers=headers,
        data=payload,
        timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    result = response.json()

    if result.get("error"):
        raise RuntimeError(result["error"])

    return result


def safe_kraken_private(label, endpoint, data=None):
    backoff_until = float(state.get("private_api_backoff_until", 0) or 0)
    now_ts = time.time()
    if backoff_until > now_ts:
        log_event(
            "KRAKEN_BACKOFF_ACTIVE",
            operation=label,
            wait_seconds=round(backoff_until - now_ts, 2)
        )
        return None

    attempts = max(1, KRAKEN_NONCE_RETRIES + 1)

    for attempt in range(1, attempts + 1):
        try:
            return kraken_private(endpoint, data)
        except Exception as e:
            message = str(e)
            log_event(
                "KRAKEN_EXCEPTION",
                operation=label,
                message=message,
                attempt=attempt
            )

            if "Temporary lockout" in message:
                state["private_api_backoff_until"] = (
                    time.time() + KRAKEN_LOCKOUT_COOLDOWN_SECONDS
                )
                save_state(state)
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


# ----------------------
# STATE
# ----------------------

def load_state():
    default = {
        "open_buy_orders": {},
        "open_sell_orders": {},
        "last_nonce": 0,
        "private_api_backoff_until": 0,
        "sell_insufficient_funds_backoff_until": 0,
        "range_low": None,
        "range_high": None,
        "range_mean": None,
        "range_median": None,
        "last_range_refresh": None,
        "last_high_anchor_buy_at": None,
        "last_sell_price": None,
        "last_sell_at": None,
        "last_llm_sell_at": None,
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
                "placed_at": order.get("placed_at"),
                "sell_pct_override": order.get("sell_pct_override"),
                "buy_source": order.get("buy_source"),
                "trade_id": order.get("trade_id") or order.get("txid"),
                "sell_retry_after": order.get("sell_retry_after"),
                "sell_failure_reason": order.get("sell_failure_reason")
            }
        else:
            normalized_buy_orders[level] = {
                "txid": None,
                "volume": None,
                "price": float(level),
                "placed_at": None,
                "sell_pct_override": None,
                "buy_source": None,
                "trade_id": None,
                "sell_retry_after": None,
                "sell_failure_reason": None
            }

    for txid, order in state["open_sell_orders"].items():
        if isinstance(order, dict) and "level" in order:
            normalized_sell_orders[txid] = {
                "level": order.get("level"),
                "volume": order.get("volume"),
                "buy_price": order.get("buy_price"),
                "sell_price": order.get("sell_price"),
                "placed_at": order.get("placed_at"),
                "sell_pct_override": order.get("sell_pct_override"),
                "buy_source": order.get("buy_source"),
                "trade_id": order.get("trade_id") or order.get("buy_txid") or txid
            }
        else:
            normalized_sell_orders[txid] = {
                "level": None,
                "volume": order.get("volume"),
                "buy_price": None,
                "sell_price": None,
                "placed_at": None,
                "sell_pct_override": None,
                "buy_source": None,
                "trade_id": txid
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
            target_prices = data.get("target_prices", [])
            if not isinstance(target_prices, list):
                target_prices = []

            return {
                "execution_signal": data.get("execution_signal", 0),
                "target_prices": target_prices,
                "risk_multiplier": data.get("risk_multiplier"),
                "smoothed_risk_multiplier": data.get("smoothed_risk_multiplier"),
                "btc_sentiment": data.get("btc_sentiment"),
                "regulatory_risk": data.get("regulatory_risk"),
                "macro_tightening_bias": data.get("macro_tightening_bias"),
                "confidence": data.get("confidence"),
                "direction_bias": data.get("direction_bias"),
                "raw_btc_sentiment": data.get("raw_btc_sentiment"),
                "raw_regulatory_risk": data.get("raw_regulatory_risk"),
                "raw_macro_tightening_bias": data.get("raw_macro_tightening_bias"),
                "raw_confidence": data.get("raw_confidence"),
                "raw_direction_bias": data.get("raw_direction_bias"),
                "btc_price": data.get("btc_price"),
                "fear_greed_index": data.get("fear_greed_index"),
                "flow_pressure": data.get("flow_pressure"),
                "mean_reversion_opportunity": data.get(
                    "mean_reversion_opportunity"
                ),
                "signal_status": data.get("signal_status"),
                "source_status": (
                    data.get("source_status")
                    if isinstance(data.get("source_status"), dict)
                    else {}
                ),
                "bot_action_allowed": data.get("bot_action_allowed"),
                "action_reason": data.get("reason"),
                "processed_at": data.get("processed_at"),
                "price_regime": (
                    data.get("price_regime")
                    if isinstance(data.get("price_regime"), dict)
                    else {}
                ),
                "trend_snapshot": (
                    data.get("trend_snapshot")
                    if isinstance(data.get("trend_snapshot"), dict)
                    else {}
                ),
                "kraken_flow": (
                    data.get("kraken_flow")
                    if isinstance(data.get("kraken_flow"), dict)
                    else {}
                )
            }

        return {
            "execution_signal": float(data),
            "target_prices": [],
            "price_regime": {},
            "trend_snapshot": {},
            "kraken_flow": {},
            "source_status": {}
        }
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


def compute_high_anchor_grid(high, price):
    lower_bound = high * (1 - entry_step_pct)

    if lower_bound <= price <= high:
        return [price]

    return []


def high_anchor_open_order_count():
    open_buy_count = sum(
        1
        for order in state["open_buy_orders"].values()
        if order.get("buy_source") == "range_high_band"
    )
    open_sell_count = sum(
        1
        for order in state["open_sell_orders"].values()
        if order.get("buy_source") == "range_high_band"
    )
    return open_buy_count + open_sell_count


def high_anchor_cooldown_remaining_minutes(now):
    last_buy_at = parse_iso8601(state.get("last_high_anchor_buy_at"))

    if last_buy_at is None:
        return 0

    elapsed_minutes = (now - last_buy_at).total_seconds() / 60
    return max(0, high_anchor_buy_cooldown_minutes - elapsed_minutes)


def llm_sell_cooldown_remaining_minutes(now):
    last_sell_at = parse_iso8601(state.get("last_llm_sell_at"))

    if last_sell_at is None:
        return 0

    elapsed_minutes = (now - last_sell_at).total_seconds() / 60
    return max(0, llm_buy_cooldown_minutes_after_sell - elapsed_minutes)


# ----------------------
# ORDER HELPERS
# ----------------------

def place_buy(price, volume):
    return safe_kraken_private("BUY", "/0/private/AddOrder", {
        "pair": "XXBTZUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS))
    })


def place_sell(price, volume):
    return safe_kraken_private("SELL", "/0/private/AddOrder", {
        "pair": "XXBTZUSD",
        "type": "sell",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS))
    })


def cancel_order(txid):
    return safe_kraken_private("CANCEL_ORDER", "/0/private/CancelOrder", {"txid": txid})


def order_filled(txid):
    try:
        r = safe_kraken_private("ORDER_FILLED", "/0/private/QueryOrders", {"txid": txid})
        if not r:
            return False

        return r["result"][txid]["status"] == "closed"
    except Exception:
        return False


def get_order_status(txid):
    try:
        r = safe_kraken_private(
            "ORDER_STATUS",
            "/0/private/QueryOrders",
            {"txid": txid}
        )

        if not r or "result" not in r or txid not in r["result"]:
            log_event("ORDER_STATUS_ERROR", txid=txid, message="missing_result")
            return None

        return r["result"][txid]["status"]
    except Exception as e:
        log_event("ORDER_STATUS_ERROR", txid=txid, message=str(e))
        return None


def get_order_statuses(txids):
    unique_txids = [txid for txid in dict.fromkeys(txids) if txid]
    if not unique_txids:
        return {}

    response = safe_kraken_private(
        "ORDER_STATUS_BATCH",
        "/0/private/QueryOrders",
        {"txid": ",".join(unique_txids)}
    )
    if not response or "result" not in response:
        return {}

    status_map = {}
    for txid in unique_txids:
        order = response["result"].get(txid)
        if not isinstance(order, dict):
            log_event("ORDER_STATUS_ERROR", txid=txid, message="missing_result")
            continue
        status_map[txid] = order.get("status")

    return status_map


def compute_sell_target_price(buy_price, profit_target_override=None):
    profit_target = (
        profit_target_pct
        if profit_target_override is None
        else profit_target_override
    )
    return buy_price * (1 + profit_target + round_trip_fee_pct)


def sell_backoff_remaining_seconds():
    backoff_until = float(
        state.get("sell_insufficient_funds_backoff_until", 0) or 0
    )
    return max(0.0, backoff_until - time.time())


def find_existing_sell_for_buy(level, order):
    buy_trade_id = order.get("trade_id") or order.get("txid")
    buy_source = order.get("buy_source")
    buy_volume = positive_float(order.get("volume"))
    buy_price = positive_float(order.get("price", level))
    level_str = str(level)

    for sell_txid, sell_order in state["open_sell_orders"].items():
        sell_trade_id = sell_order.get("trade_id")
        if buy_trade_id and sell_trade_id == buy_trade_id:
            return sell_txid, sell_order, "trade_id"

        sell_level = str(sell_order.get("level"))
        sell_source = sell_order.get("buy_source")
        sell_volume = positive_float(sell_order.get("volume"))
        sell_buy_price = positive_float(sell_order.get("buy_price"))

        if (
            sell_level == level_str
            and sell_source == buy_source
            and buy_volume is not None
            and sell_volume is not None
            and round(sell_volume, VOLUME_DECIMALS)
            == round(buy_volume, VOLUME_DECIMALS)
            and buy_price is not None
            and sell_buy_price is not None
            and round(sell_buy_price, PRICE_DECIMALS)
            == round(buy_price, PRICE_DECIMALS)
        ):
            return sell_txid, sell_order, "level_volume_price"

    return None, None, None


def normalize_llm_sell_pct(raw_sell_pct):
    if raw_sell_pct is None:
        return None

    try:
        return float(raw_sell_pct) / 100.0
    except Exception:
        return None


def select_llm_target(target_prices, current_price):
    valid_targets = []

    for target in target_prices:
        if not isinstance(target, dict):
            continue

        buy_price = target.get("buy_price")
        sell_pct = normalize_llm_sell_pct(target.get("sell_pct"))

        if buy_price is None or sell_pct is None:
            continue

        try:
            buy_price = float(buy_price)
        except Exception:
            continue

        distance_pct = abs(current_price - buy_price) / buy_price
        if distance_pct > llm_target_proximity_pct:
            continue

        valid_targets.append(
            {
                "buy_price": buy_price,
                "sell_pct": sell_pct,
                "distance_pct": distance_pct
            }
        )

    if not valid_targets:
        return None

    return min(valid_targets, key=lambda target: target["distance_pct"])


def numeric_or_default(value, default):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def compute_adjusted_profit_target(age_minutes, base_profit_target=None):
    starting_profit_target = (
        profit_target_pct
        if base_profit_target is None
        else base_profit_target
    )

    if (
        age_minutes is None
        or aging_profit_reduction_pct <= 0
        or age_minutes < aging_start_minutes
    ):
        return starting_profit_target

    reduction_steps = (
        int((age_minutes - aging_start_minutes) // max(1, aging_step_minutes))
        + 1
    )
    adjusted_profit = (
        starting_profit_target - reduction_steps * aging_profit_reduction_pct
    )
    return max(min_profit_target_pct, adjusted_profit)


def sentiment_regime(execution_signal):
    if execution_signal < execution_signal_threshold:
        return {
            "name": "paused",
            "position_size_multiplier": 0.0,
            "inventory_multiplier": 0.0,
            "open_sell_multiplier": 0.0,
            "allow_high_anchor": False,
            "extra_aging_reduction_pct": sentiment_defensive_extra_aging_reduction_pct
        }

    if execution_signal < sentiment_defensive_threshold:
        return {
            "name": "defensive",
            "position_size_multiplier": sentiment_defensive_size_multiplier,
            "inventory_multiplier": sentiment_defensive_inventory_multiplier,
            "open_sell_multiplier": sentiment_defensive_open_sell_multiplier,
            "allow_high_anchor": (
                execution_signal >= sentiment_disable_high_anchor_below
            ),
            "extra_aging_reduction_pct": sentiment_defensive_extra_aging_reduction_pct
        }

    if execution_signal >= sentiment_risk_on_threshold:
        return {
            "name": "risk_on",
            "position_size_multiplier": sentiment_risk_on_size_multiplier,
            "inventory_multiplier": sentiment_risk_on_inventory_multiplier,
            "open_sell_multiplier": sentiment_risk_on_open_sell_multiplier,
            "allow_high_anchor": True,
            "extra_aging_reduction_pct": 0.0
        }

    return {
        "name": "neutral",
        "position_size_multiplier": 1.0,
        "inventory_multiplier": 1.0,
        "open_sell_multiplier": 1.0,
        "allow_high_anchor": True,
        "extra_aging_reduction_pct": 0.0
    }


def min_signal_for_buy_source(buy_source):
    if buy_source == "llm_target":
        return llm_target_min_signal
    if buy_source == "range_mean":
        return mean_min_signal
    if buy_source == "range_median":
        return median_min_signal
    if buy_source == "range_high_band":
        return high_min_signal
    return low_min_signal


def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def source_status_allows_trading(signal_status, source_status):
    if not require_fresh_signal:
        return True, None

    if signal_status and signal_status != min_signal_status:
        return False, f"signal_status_{signal_status}"

    if not isinstance(source_status, dict):
        return True, None

    for source_name, status_info in source_status.items():
        if not isinstance(status_info, dict):
            continue
        status_value = status_info.get("status")
        if status_value in ("fresh", "not_configured", None):
            continue
        return False, f"source_status_{source_name}_{status_value}"

    return True, None


def flow_adjustment(flow_pressure, buy_source):
    if flow_pressure is None:
        return {
            "size_multiplier": 1.0,
            "block_buy": False,
            "reason": None
        }

    if flow_pressure <= flow_block_llm_only_below and buy_source == "llm_target":
        return {
            "size_multiplier": flow_defensive_size_multiplier,
            "block_buy": False,
            "reason": "flow_llm_only"
        }

    if flow_pressure <= flow_block_threshold:
        if flow_block_high_only:
            return {
                "size_multiplier": flow_defensive_size_multiplier,
                "block_buy": buy_source == "range_high_band",
                "reason": "flow_block_high"
            }
        return {
            "size_multiplier": flow_defensive_size_multiplier,
            "block_buy": True,
            "reason": "flow_block_all"
        }

    if flow_pressure <= flow_defensive_threshold:
        return {
            "size_multiplier": flow_defensive_size_multiplier,
            "block_buy": buy_source == "range_high_band",
            "reason": "flow_defensive"
        }

    return {
        "size_multiplier": 1.0,
        "block_buy": False,
        "reason": None
    }


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
        strategy_profile=STRATEGY_PROFILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        grid_anchor=grid_anchor,
        strategy_modes=strategy_modes,
        range_window_hours=range_window_hours,
        max_grid_size=max_grid_size,
        profit_target_pct=profit_target_pct,
        entry_step_pct=entry_step_pct,
        round_trip_fee_pct=round_trip_fee_pct,
        position_size_pct=position_size_pct,
        min_buy_notional_usd=min_buy_notional_usd,
        min_buy_volume_btc=min_buy_volume_btc,
        execution_signal_threshold=execution_signal_threshold,
        llm_target_proximity_pct=llm_target_proximity_pct,
        llm_target_min_signal=llm_target_min_signal,
        low_min_signal=low_min_signal,
        mean_min_signal=mean_min_signal,
        median_min_signal=median_min_signal,
        high_min_signal=high_min_signal,
        request_timeout=REQUEST_TIMEOUT,
        kraken_nonce_retries=KRAKEN_NONCE_RETRIES,
        kraken_lockout_cooldown_seconds=KRAKEN_LOCKOUT_COOLDOWN_SECONDS,
        min_signal_status=min_signal_status,
        require_fresh_signal=require_fresh_signal,
        risk_multiplier_floor=risk_multiplier_floor,
        risk_multiplier_ceiling=risk_multiplier_ceiling,
        flow_defensive_threshold=flow_defensive_threshold,
        flow_block_threshold=flow_block_threshold,
        flow_defensive_size_multiplier=flow_defensive_size_multiplier,
        flow_block_high_only=flow_block_high_only,
        flow_block_llm_only_below=flow_block_llm_only_below,
        mean_reversion_min_opportunity=mean_reversion_min_opportunity,
        price_check_interval_seconds=price_check_interval_seconds,
        range_refresh_interval_minutes=range_refresh_interval_minutes,
        max_open_sell_orders=max_open_sell_orders,
        max_inventory_usd=max_inventory_usd,
        aging_start_minutes=aging_start_minutes,
        aging_step_minutes=aging_step_minutes,
        aging_profit_reduction_pct=aging_profit_reduction_pct,
        min_profit_target_pct=min_profit_target_pct,
        high_anchor_buy_cooldown_minutes=high_anchor_buy_cooldown_minutes,
        max_open_high_anchor_orders=max_open_high_anchor_orders,
        high_anchor_profit_target_pct=high_anchor_profit_target_pct,
        prevent_buy_above_last_sell=prevent_buy_above_last_sell,
        buy_after_sell_discount_pct=buy_after_sell_discount_pct,
        llm_buy_cooldown_minutes_after_sell=(
            llm_buy_cooldown_minutes_after_sell
        ),
        sentiment_defensive_threshold=sentiment_defensive_threshold,
        sentiment_risk_on_threshold=sentiment_risk_on_threshold,
        sentiment_defensive_size_multiplier=(
            sentiment_defensive_size_multiplier
        ),
        sentiment_risk_on_size_multiplier=sentiment_risk_on_size_multiplier,
        sentiment_defensive_inventory_multiplier=(
            sentiment_defensive_inventory_multiplier
        ),
        sentiment_risk_on_inventory_multiplier=(
            sentiment_risk_on_inventory_multiplier
        ),
        sentiment_defensive_open_sell_multiplier=(
            sentiment_defensive_open_sell_multiplier
        ),
        sentiment_risk_on_open_sell_multiplier=(
            sentiment_risk_on_open_sell_multiplier
        ),
        sentiment_disable_high_anchor_below=(
            sentiment_disable_high_anchor_below
        ),
        sentiment_defensive_extra_aging_reduction_pct=(
            sentiment_defensive_extra_aging_reduction_pct
        )
    )

    loop_count = 0
    while True:
        try:
            loop_count += 1
            now = datetime.now(timezone.utc)
            cycle_id = now.isoformat()
            actions = []
            deduped_candidates = []

            price = get_price()
            sentiment_payload = get_sentiment()

            if price is None or sentiment_payload is None:
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    execution_signal=None,
                    reason="missing_price_or_signal",
                    cycle_id=cycle_id
                )
                send_checkin(loop_count=loop_count, message="loop_complete")
                time.sleep(price_check_interval_seconds)
                continue

            execution_signal = sentiment_payload["execution_signal"]
            target_prices = sentiment_payload.get("target_prices", [])
            llm_target = select_llm_target(target_prices, price)
            price_regime = sentiment_payload.get("price_regime", {})
            trend_snapshot = sentiment_payload.get("trend_snapshot", {})
            kraken_flow = sentiment_payload.get("kraken_flow", {})
            flow_pressure = numeric_or_default(
                sentiment_payload.get("flow_pressure"),
                None
            )
            mean_reversion_opportunity = numeric_or_default(
                sentiment_payload.get("mean_reversion_opportunity"),
                0.0
            )
            smoothed_risk_multiplier = clamp(
                numeric_or_default(
                    sentiment_payload.get("smoothed_risk_multiplier"),
                    1.0
                ),
                risk_multiplier_floor,
                risk_multiplier_ceiling
            )
            signal_status = sentiment_payload.get("signal_status")
            source_status = sentiment_payload.get("source_status", {})
            freshness_allows_trading, freshness_block_reason = (
                source_status_allows_trading(signal_status, source_status)
            )
            signal_allows_trading = sentiment_payload.get("bot_action_allowed")
            if signal_allows_trading is None:
                signal_allows_trading = True
            external_block_reason = sentiment_payload.get("action_reason")
            regime = sentiment_regime(execution_signal)
            effective_position_size_pct = (
                position_size_pct * regime["position_size_multiplier"]
            )
            effective_max_inventory_usd = (
                max_inventory_usd * regime["inventory_multiplier"]
            )
            effective_max_open_sell_orders = max(
                1,
                int(round(
                    max_open_sell_orders * regime["open_sell_multiplier"]
                ))
            )
            effective_position_size_pct *= smoothed_risk_multiplier
            effective_max_inventory_usd *= smoothed_risk_multiplier
            effective_max_open_sell_orders = max(
                1,
                int(round(
                    effective_max_open_sell_orders * smoothed_risk_multiplier
                ))
            )

            log_event(
                "SIGNAL_UPDATE",
                execution_signal=execution_signal,
                price=price,
                btc_sentiment=sentiment_payload.get("btc_sentiment"),
                confidence=sentiment_payload.get("confidence"),
                raw_btc_sentiment=sentiment_payload.get("raw_btc_sentiment"),
                raw_confidence=sentiment_payload.get("raw_confidence"),
                direction_bias=sentiment_payload.get("direction_bias"),
                raw_direction_bias=sentiment_payload.get("raw_direction_bias"),
                fear_greed_index=sentiment_payload.get("fear_greed_index"),
                llm_target_count=len(target_prices),
                active_llm_target=(
                    None if llm_target is None
                    else round(llm_target["buy_price"], PRICE_DECIMALS)
                ),
                signal_status=signal_status,
                signal_allows_trading=signal_allows_trading,
                freshness_allows_trading=freshness_allows_trading,
                freshness_block_reason=freshness_block_reason,
                smoothed_risk_multiplier=smoothed_risk_multiplier,
                flow_pressure=flow_pressure,
                mean_reversion_opportunity=mean_reversion_opportunity,
                price_regime_range_position=price_regime.get("range_position_24h"),
                price_regime_volatility_pct=price_regime.get(
                    "realized_volatility_24h_pct"
                ),
                trend_range_position_change=(
                    trend_snapshot.get("price_regime_trends", {})
                    .get("range_position_24h_change")
                ),
                sentiment_regime=regime["name"],
                effective_position_size_pct=effective_position_size_pct,
                effective_max_inventory_usd=effective_max_inventory_usd,
                effective_max_open_sell_orders=effective_max_open_sell_orders,
                high_anchor_enabled=regime["allow_high_anchor"]
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

            low = numeric_or_default(
                price_regime.get("price_low_24h"),
                low
            )
            high = numeric_or_default(
                price_regime.get("price_high_24h"),
                high
            )
            mean = numeric_or_default(
                price_regime.get("price_mean_24h"),
                mean
            )
            median = numeric_or_default(
                price_regime.get("price_median_24h"),
                median
            )

            order_status_map = get_order_statuses(
                list(state["open_sell_orders"].keys())
                + [
                    order.get("txid")
                    for order in state["open_buy_orders"].values()
                ]
            )

            # SELL EXIT CHECK
            for txid, order in list(state["open_sell_orders"].items()):
                status = order_status_map.get(txid)

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
                    state["last_sell_price"] = sell_price
                    state["last_sell_at"] = cycle_id
                    if order.get("buy_source") == "llm_target":
                        state["last_llm_sell_at"] = cycle_id
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
                        buy_source=order.get("buy_source"),
                        gross_pnl=gross_pnl,
                        estimated_net_pnl=estimated_net_pnl,
                        hold_minutes=hold_minutes
                    )
                    continue

                if status == "open":
                    buy_price = order.get("buy_price")
                    current_sell_price = order.get("sell_price")
                    sell_pct_override = order.get("sell_pct_override")
                    placed_at = parse_iso8601(order.get("placed_at"))
                    age_minutes = None
                    if placed_at is not None:
                        age_minutes = (
                            now - placed_at
                        ).total_seconds() / 60

                    adjusted_profit_target = compute_adjusted_profit_target(
                        age_minutes,
                        sell_pct_override
                    )
                    if (
                        age_minutes is not None
                        and age_minutes >= aging_start_minutes
                    ):
                        adjusted_profit_target = max(
                            min_profit_target_pct,
                            adjusted_profit_target
                            - regime["extra_aging_reduction_pct"]
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
                        "placed_at": order.get("placed_at"),
                        "sell_pct_override": sell_pct_override,
                        "buy_source": order.get("buy_source"),
                        "trade_id": order.get("trade_id") or txid
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
                        adjusted_profit_target_pct=adjusted_profit_target,
                        sell_pct_override=sell_pct_override,
                        buy_source=order.get("buy_source")
                    )
                    notify_order_tracker(
                        trade_id=order.get("trade_id") or txid,
                        side="sell",
                        price=round(adjusted_sell_price, PRICE_DECIMALS),
                        quantity=round(order["volume"], VOLUME_DECIMALS),
                        order_id=new_txid,
                        timestamp=cycle_id,
                        notes="sell_reprice"
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
                    notify_order_tracker(
                        trade_id=order.get("trade_id") or txid,
                        side="sell",
                        price=order.get("sell_price"),
                        quantity=order.get("volume"),
                        order_id=txid,
                        timestamp=cycle_id,
                        notes=f"order_status={status}",
                        status=status
                    )

            # SELL CHECK
            for level, order in list(state["open_buy_orders"].items()):
                txid = order["txid"]
                status = order_status_map.get(txid)

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
                    notify_order_tracker(
                        trade_id=order.get("trade_id") or txid,
                        side="buy",
                        price=order.get("price", float(level)),
                        quantity=order.get("volume"),
                        order_id=txid,
                        timestamp=cycle_id,
                        notes=f"order_status={status}",
                        status=status
                    )
                    continue

                if status != "closed":
                    continue

                sell_backoff_remaining = sell_backoff_remaining_seconds()
                order_sell_retry_after = parse_iso8601(
                    order.get("sell_retry_after")
                )
                if sell_backoff_remaining > 0:
                    actions.append("sell_backoff_active")
                    log_event(
                        "SELL_BACKOFF_ACTIVE",
                        cycle_id=cycle_id,
                        level=level,
                        txid=txid,
                        wait_seconds=round(sell_backoff_remaining, 2),
                        reason=order.get("sell_failure_reason")
                        or "insufficient_funds"
                    )
                    continue
                if (
                    order_sell_retry_after is not None
                    and now < order_sell_retry_after
                ):
                    actions.append("sell_retry_deferred")
                    log_event(
                        "SELL_RETRY_DEFERRED",
                        cycle_id=cycle_id,
                        level=level,
                        txid=txid,
                        retry_after=order.get("sell_retry_after"),
                        reason=order.get("sell_failure_reason")
                    )
                    continue

                buy_price = float(level)
                sell_pct_override = order.get("sell_pct_override")
                buy_source = order.get("buy_source")
                sell_price = compute_sell_target_price(
                    buy_price,
                    sell_pct_override
                )
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
                    hold_minutes=hold_minutes,
                    sell_pct_override=sell_pct_override
                )
                order.pop("sell_retry_after", None)
                order.pop("sell_failure_reason", None)

                log_event(
                    "TRADE_DECISION",
                    cycle_id=cycle_id,
                    side="sell",
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(sell_price, PRICE_DECIMALS),
                    buy_price=round(buy_price, PRICE_DECIMALS),
                    sell_pct_override=sell_pct_override,
                    buy_source=buy_source
                )

                existing_sell_txid, existing_sell_order, match_reason = (
                    find_existing_sell_for_buy(level, order)
                )
                if existing_sell_txid:
                    del state["open_buy_orders"][level]
                    save_state(state)
                    actions.append("buy_reconciled_to_existing_sell")

                    log_and_console(
                        "BUY_RECONCILED_TO_OPEN_SELL",
                        message=(
                            f"BUY level {level} already tracked by open sell "
                            f"{existing_sell_txid}"
                        ),
                        cycle_id=cycle_id,
                        buy_txid=order.get("txid"),
                        sell_txid=existing_sell_txid,
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        buy_price=round(buy_price, PRICE_DECIMALS),
                        sell_price=round(
                            existing_sell_order.get("sell_price", sell_price),
                            PRICE_DECIMALS
                        ),
                        buy_source=buy_source,
                        match_reason=match_reason
                    )
                    continue

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
                        sell_pct_override=sell_pct_override,
                        buy_source=buy_source,
                        error=(
                            None if not sell_resp
                            else sell_resp.get("error")
                        )
                    )
                    cooldown_seconds = (
                        SELL_INSUFFICIENT_FUNDS_COOLDOWN_SECONDS
                    )
                    failure_reason = "sell_rejected"
                    if (
                        not sell_resp
                        and "insufficient funds"
                        in (order.get("sell_failure_reason") or "").lower()
                    ):
                        failure_reason = "insufficient_funds"
                    if not sell_resp:
                        failure_reason = "insufficient_funds"
                    retry_after = (
                        now + timedelta(seconds=cooldown_seconds)
                    ).isoformat()
                    order["sell_retry_after"] = retry_after
                    order["sell_failure_reason"] = failure_reason
                    if failure_reason == "insufficient_funds":
                        state["sell_insufficient_funds_backoff_until"] = max(
                            float(state.get(
                                "sell_insufficient_funds_backoff_until",
                                0
                            ) or 0),
                            time.time() + cooldown_seconds
                        )
                    save_state(state)
                    log_event(
                        "SELL_RETRY_SCHEDULED",
                        cycle_id=cycle_id,
                        level=level,
                        txid=order.get("txid"),
                        retry_after=retry_after,
                        reason=failure_reason
                    )
                    existing_sell_txid, existing_sell_order, match_reason = (
                        find_existing_sell_for_buy(level, order)
                    )
                    if existing_sell_txid:
                        del state["open_buy_orders"][level]
                        save_state(state)
                        actions.append("buy_reconciled_after_sell_reject")
                        log_and_console(
                            "BUY_RECONCILED_AFTER_SELL_REJECT",
                            message=(
                                f"BUY level {level} already tracked by open sell "
                                f"{existing_sell_txid} after rejected sell attempt"
                            ),
                            cycle_id=cycle_id,
                            buy_txid=order.get("txid"),
                            sell_txid=existing_sell_txid,
                            level=level,
                            volume=round(order["volume"], VOLUME_DECIMALS),
                            buy_price=round(buy_price, PRICE_DECIMALS),
                            sell_price=round(
                                existing_sell_order.get("sell_price", sell_price),
                                PRICE_DECIMALS
                            ),
                            buy_source=buy_source,
                            match_reason=match_reason
                        )
                    continue

                txid = sell_resp["result"]["txid"][0]
                state["open_sell_orders"][txid] = {
                    "level": level,
                    "volume": order["volume"],
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "placed_at": cycle_id,
                    "sell_pct_override": sell_pct_override,
                    "buy_source": buy_source,
                    "trade_id": order.get("trade_id") or order.get("txid")
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
                    buy_price=round(buy_price, PRICE_DECIMALS),
                    sell_pct_override=sell_pct_override,
                    buy_source=buy_source
                )
                notify_order_tracker(
                    trade_id=order.get("trade_id") or order.get("txid"),
                    side="sell",
                    price=round(sell_price, PRICE_DECIMALS),
                    quantity=round(order["volume"], VOLUME_DECIMALS),
                    order_id=txid,
                    timestamp=cycle_id,
                    notes=buy_source
                )

            llm_buy_allowed = (
                low and high
                and llm_target is not None
                and execution_signal >= llm_target_min_signal
                and freshness_allows_trading
                and signal_allows_trading
                and mean_reversion_opportunity >= mean_reversion_min_opportunity
            )

            # BUY CANDIDATES
            if (
                freshness_allows_trading
                and signal_allows_trading
                and effective_position_size_pct > 0
                and low and high and (
                llm_buy_allowed or execution_signal >= min(
                    low_min_signal,
                    mean_min_signal,
                    median_min_signal,
                    high_min_signal
                )
                )
            ):
                candidate_levels = []
                if llm_buy_allowed:
                    candidate_levels = [
                        {
                            "level": llm_target["buy_price"],
                            "sell_pct_override": llm_target["sell_pct"],
                            "buy_source": "llm_target"
                        }
                    ]
                else:
                    for strategy_mode in strategy_modes:
                        if strategy_mode == "mean":
                            if execution_signal < mean_min_signal:
                                continue
                            grid = compute_grid(low, high, mean)
                            sell_pct_override = None
                            buy_source = "range_mean"
                        elif strategy_mode == "median" and median is not None:
                            if execution_signal < median_min_signal:
                                continue
                            grid = compute_grid(low, high, median)
                            sell_pct_override = None
                            buy_source = "range_median"
                        elif strategy_mode == "high":
                            if execution_signal < high_min_signal:
                                continue
                            if not regime["allow_high_anchor"]:
                                continue
                            grid = compute_high_anchor_grid(high, price)
                            sell_pct_override = high_anchor_profit_target_pct
                            buy_source = "range_high_band"
                        else:
                            if execution_signal < low_min_signal:
                                continue
                            grid = compute_grid(low, high, low)
                            sell_pct_override = None
                            buy_source = "range_low"

                        for level in grid:
                            candidate_levels.append(
                                {
                                    "level": level,
                                    "sell_pct_override": sell_pct_override,
                                    "buy_source": buy_source
                                }
                            )

                deduped_candidates = []
                seen_levels = set()
                for candidate in candidate_levels:
                    rounded_level = round(candidate["level"], PRICE_DECIMALS)
                    if rounded_level in seen_levels:
                        continue
                    seen_levels.add(rounded_level)
                    deduped_candidates.append(candidate)

                bal = safe_kraken_private("BALANCE", "/0/private/Balance")

                if not bal or "result" not in bal:
                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="hold",
                        price=price,
                        execution_signal=execution_signal,
                        reason="balance_fetch_failed"
                    )
                    actions.append("hold_balance_fetch_failed")
                    send_checkin(loop_count=loop_count, message="loop_complete")
                    time.sleep(price_check_interval_seconds)
                    continue

                usd = float(bal["result"].get("ZUSD", 0))
                reserved_sell_levels = {
                    sell_order.get("level")
                    for sell_order in state["open_sell_orders"].values()
                }
                last_sell_price = positive_float(state.get("last_sell_price"))
                llm_sell_cooldown_remaining = (
                    llm_sell_cooldown_remaining_minutes(now)
                )
                deployed_inventory_usd = current_inventory_usd(price)
                high_anchor_order_count = high_anchor_open_order_count()
                high_anchor_cooldown_remaining = (
                    high_anchor_cooldown_remaining_minutes(now)
                )

                for candidate in deduped_candidates:
                    level = candidate["level"]
                    active_sell_pct_override = candidate["sell_pct_override"]
                    buy_source = candidate["buy_source"]
                    flow_control = flow_adjustment(flow_pressure, buy_source)
                    key = str(level)
                    skip_reason = None

                    if key in state["open_buy_orders"]:
                        skip_reason = "open_buy_order"
                    elif key in reserved_sell_levels:
                        skip_reason = "open_sell_order"
                    elif (
                        prevent_buy_above_last_sell
                        and last_sell_price is not None
                        and level > (
                            last_sell_price * (1 - buy_after_sell_discount_pct)
                        )
                    ):
                        skip_reason = "above_last_sell_discount"
                    elif (
                        mean_reversion_opportunity < mean_reversion_min_opportunity
                    ):
                        skip_reason = "mean_reversion_opportunity_below_min"
                    elif buy_source != "llm_target" and price > level:
                        skip_reason = "price_above_level"
                    elif (
                        len(state["open_sell_orders"])
                        >= effective_max_open_sell_orders
                    ):
                        skip_reason = "max_open_sell_orders"
                    elif deployed_inventory_usd >= effective_max_inventory_usd:
                        skip_reason = "max_inventory_usd"
                    elif (
                        buy_source == "llm_target"
                        and execution_signal < llm_target_min_signal
                    ):
                        skip_reason = "llm_signal_below_min"
                    elif (
                        buy_source != "llm_target"
                        and execution_signal < min_signal_for_buy_source(buy_source)
                    ):
                        skip_reason = "strategy_signal_below_min"
                    elif flow_control["block_buy"]:
                        skip_reason = flow_control["reason"]
                    elif (
                        buy_source == "llm_target"
                        and llm_sell_cooldown_remaining > 0
                    ):
                        skip_reason = "llm_sell_cooldown"
                    elif (
                        buy_source == "range_high_band"
                        and high_anchor_cooldown_remaining > 0
                    ):
                        skip_reason = "high_anchor_cooldown"
                    elif (
                        buy_source == "range_high_band"
                        and high_anchor_order_count >= max_open_high_anchor_orders
                    ):
                        skip_reason = "max_open_high_anchor_orders"

                    volume = (
                        usd
                        * effective_position_size_pct
                        * flow_control["size_multiplier"]
                    ) / level
                    trade_notional_usd = level * volume
                    projected_inventory_usd = deployed_inventory_usd + (
                        level * volume
                    )

                    if (
                        skip_reason is None
                        and trade_notional_usd < min_buy_notional_usd
                    ):
                        skip_reason = "below_min_notional"

                    if (
                        skip_reason is None
                        and projected_inventory_usd > effective_max_inventory_usd
                    ):
                        skip_reason = "max_inventory_usd"

                    if skip_reason is None and volume < min_buy_volume_btc:
                        skip_reason = "below_min_volume"

                    if skip_reason is not None:
                        log_event(
                            "GRID_LEVEL_EVAL",
                            cycle_id=cycle_id,
                            level=round(level, PRICE_DECIMALS),
                            market_price=price,
                            execution_signal=execution_signal,
                            usd_balance=usd,
                            trade_notional_usd=round(trade_notional_usd, 8),
                            deployed_inventory_usd=deployed_inventory_usd,
                            high_anchor_order_count=high_anchor_order_count,
                            high_anchor_cooldown_remaining_minutes=round(
                                high_anchor_cooldown_remaining,
                                2
                            ),
                            llm_sell_cooldown_remaining_minutes=round(
                                llm_sell_cooldown_remaining,
                                2
                            ),
                            last_sell_price=last_sell_price,
                            sentiment_regime=regime["name"],
                            effective_position_size_pct=(
                                effective_position_size_pct
                            ),
                            effective_max_inventory_usd=(
                                effective_max_inventory_usd
                            ),
                            effective_max_open_sell_orders=(
                                effective_max_open_sell_orders
                            ),
                            smoothed_risk_multiplier=smoothed_risk_multiplier,
                            flow_pressure=flow_pressure,
                            flow_control_reason=flow_control["reason"],
                            mean_reversion_opportunity=(
                                mean_reversion_opportunity
                            ),
                            buy_source=buy_source,
                            reason=skip_reason
                        )
                        continue

                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="buy",
                        volume=round(volume, VOLUME_DECIMALS),
                        trade_notional_usd=round(trade_notional_usd, 8),
                        price=round(level, PRICE_DECIMALS),
                        execution_signal=execution_signal,
                        range_low=low,
                        range_high=high,
                        range_mean=mean,
                        range_median=median,
                        sentiment_regime=regime["name"],
                        effective_position_size_pct=effective_position_size_pct,
                        smoothed_risk_multiplier=smoothed_risk_multiplier,
                        flow_pressure=flow_pressure,
                        flow_control_reason=flow_control["reason"],
                        mean_reversion_opportunity=mean_reversion_opportunity,
                        buy_source=buy_source,
                        high_anchor_order_count=high_anchor_order_count,
                        last_sell_price=last_sell_price,
                        sell_pct_override=active_sell_pct_override
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
                            buy_source=buy_source,
                            sell_pct_override=active_sell_pct_override,
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
                        "placed_at": cycle_id,
                        "sell_pct_override": active_sell_pct_override,
                        "buy_source": buy_source,
                        "trade_id": txid
                    }
                    if buy_source == "range_high_band":
                        state["last_high_anchor_buy_at"] = cycle_id
                        high_anchor_order_count += 1
                        high_anchor_cooldown_remaining = (
                            high_anchor_buy_cooldown_minutes
                        )
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
                        price=round(level, PRICE_DECIMALS),
                        buy_source=buy_source,
                        sell_pct_override=active_sell_pct_override
                    )
                    notify_order_tracker(
                        trade_id=txid,
                        side="buy",
                        price=round(level, PRICE_DECIMALS),
                        quantity=round(volume, VOLUME_DECIMALS),
                        order_id=txid,
                        timestamp=cycle_id,
                        notes=buy_source
                    )
            else:
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    execution_signal=execution_signal,
                    threshold=execution_signal_threshold,
                    llm_target_min_signal=llm_target_min_signal,
                    low_min_signal=low_min_signal,
                    mean_min_signal=mean_min_signal,
                    median_min_signal=median_min_signal,
                    high_min_signal=high_min_signal,
                    signal_status=signal_status,
                    freshness_allows_trading=freshness_allows_trading,
                    freshness_block_reason=freshness_block_reason,
                    signal_allows_trading=signal_allows_trading,
                    external_block_reason=external_block_reason,
                    smoothed_risk_multiplier=smoothed_risk_multiplier,
                    flow_pressure=flow_pressure,
                    mean_reversion_opportunity=mean_reversion_opportunity,
                    effective_position_size_pct=effective_position_size_pct,
                    range_low=low,
                    range_high=high,
                    range_mean=mean,
                    range_median=median,
                    sentiment_regime=regime["name"],
                    reason=(
                        freshness_block_reason
                        or external_block_reason
                        or "signal_below_threshold_or_range_unavailable"
                    ),
                    cycle_id=cycle_id,
                    price_regime_range_position=price_regime.get(
                        "range_position_24h"
                    ),
                    price_regime_volatility_pct=price_regime.get(
                        "realized_volatility_24h_pct"
                    )
                )
                actions.append("hold")

            log_event(
                "CYCLE_SUMMARY",
                cycle_id=cycle_id,
                price=price,
                execution_signal=execution_signal,
                threshold=execution_signal_threshold,
                llm_target_min_signal=llm_target_min_signal,
                low_min_signal=low_min_signal,
                mean_min_signal=mean_min_signal,
                median_min_signal=median_min_signal,
                high_min_signal=high_min_signal,
                signal_status=signal_status,
                freshness_allows_trading=freshness_allows_trading,
                freshness_block_reason=freshness_block_reason,
                signal_allows_trading=signal_allows_trading,
                external_block_reason=external_block_reason,
                smoothed_risk_multiplier=smoothed_risk_multiplier,
                flow_pressure=flow_pressure,
                mean_reversion_opportunity=mean_reversion_opportunity,
                range_low=low,
                range_high=high,
                range_mean=mean,
                range_median=median,
                price_regime_timestamp=price_regime.get("timestamp"),
                price_regime_range_position=price_regime.get(
                    "range_position_24h"
                ),
                price_regime_volatility_pct=price_regime.get(
                    "realized_volatility_24h_pct"
                ),
                price_regime_mean_reversion_buy_target=price_regime.get(
                    "mean_reversion_buy_target"
                ),
                price_regime_median_reversion_buy_target=price_regime.get(
                    "median_reversion_buy_target"
                ),
                trend_range_position_change=(
                    trend_snapshot.get("price_regime_trends", {})
                    .get("range_position_24h_change")
                ),
                trend_distance_from_mean_change=(
                    trend_snapshot.get("price_regime_trends", {})
                    .get("distance_from_mean_pct_change")
                ),
                kraken_flow_trade_imbalance_pct=kraken_flow.get(
                    "trade_imbalance_pct"
                ),
                sentiment_regime=regime["name"],
                effective_position_size_pct=effective_position_size_pct,
                effective_max_inventory_usd=effective_max_inventory_usd,
                effective_max_open_sell_orders=effective_max_open_sell_orders,
                high_anchor_enabled=regime["allow_high_anchor"],
                grid_anchor=grid_anchor,
                buy_source=(
                    "llm_target"
                    if llm_buy_allowed
                    else ",".join(strategy_modes)
                ),
                strategy_modes=strategy_modes,
                grid_levels=(
                    [
                        round(candidate["level"], PRICE_DECIMALS)
                        for candidate in deduped_candidates
                    ]
                    if freshness_allows_trading and signal_allows_trading and low and high and (
                        llm_buy_allowed or execution_signal >= min(
                            low_min_signal,
                            mean_min_signal,
                            median_min_signal,
                            high_min_signal
                        )
                    )
                    else []
                ),
                high_anchor_order_count=high_anchor_open_order_count(),
                high_anchor_cooldown_remaining_minutes=round(
                    high_anchor_cooldown_remaining_minutes(now),
                    2
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

            send_checkin(loop_count=loop_count, message="loop_complete")
            time.sleep(price_check_interval_seconds)
        except Exception as e:
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            send_checkin(
                status="error",
                loop_count=loop_count,
                message=short_error_summary(e)
            )
            time.sleep(price_check_interval_seconds)


if __name__ == "__main__":
    main()
