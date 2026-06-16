#!/usr/bin/env python3

import base64
import hashlib
import hmac
import json
import math
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

from target_quality import (
    evaluate_quality_target,
    load_target_quality_snapshot,
    match_quality_target,
    normalize_profit_target_pct,
    parse_iso8601,
    unavailable_quality_decision,
)
from signal_normalizer import normalize_signal_payload, selected_signal_asset_id

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

CONFIG_FILE = (
    os.getenv("LLM_TARGET_CONFIG_FILE")
    or os.getenv("BOT_CONFIG_FILE")
    or "sentiment_bot_config.json"
)
STRATEGY_PROFILE = (
    os.getenv("LLM_TARGET_STRATEGY_PROFILE")
    or os.getenv("STRATEGY_PROFILE")
    or "llm_target_strategy_default.json"
)
STATE_FILE = (
    os.getenv("LLM_TARGET_STATE_FILE")
    or os.getenv("BOT_STATE_FILE")
    or "llm_target_state.json"
)
LOG_FILE = (
    os.getenv("LLM_TARGET_TRADE_LOG_FILE")
    or os.getenv("TRADE_LOG_FILE")
    or "llm_target_trade_log.jsonl"
)

KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
SIGNAL_FILE = os.getenv("SIGNAL_FILE")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
SIGNAL_ASSET_ID = selected_signal_asset_id(pair=KRAKEN_PAIR)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
KRAKEN_NONCE_RETRIES = int(os.getenv("KRAKEN_NONCE_RETRIES", "2"))
PRICE_CHECK_INTERVAL_SECONDS = int(os.getenv("PRICE_CHECK_INTERVAL_SECONDS", "60"))


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


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


def profile_bool(name, default):
    value = strategy_config.get(name, default)
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def env_or_profile(name, profile_name, default=None):
    value = os.getenv(name)
    if value not in (None, ""):
        return value
    return strategy_config.get(profile_name, default)


def key_fingerprint(value):
    if not value:
        return "missing"

    digest = hashlib.sha256(value.encode()).hexdigest()[:12]
    return f"sha256:{digest}"


MIN_TRADE_USD = profile_float("min_trade_usd", 20.0)
POSITION_SIZE_PCT = profile_float("position_size_pct", 0.15)
MAX_TRADE_USD = profile_float("max_trade_usd", 90.0)
TARGET_PROFIT_PCT = profile_float("target_profit_pct", 0.005)
ROUND_TRIP_FEE_PCT = profile_float("round_trip_fee_pct", 0.0032)
MAX_OPEN_SELL_ORDERS = profile_int("max_open_sell_orders", 2)
MAX_OPEN_BUY_ORDERS = profile_int("max_open_buy_orders", 2)
MAX_INVENTORY_USD = profile_float("max_inventory_usd", 400.0)
PREVENT_BUY_ABOVE_LAST_SELL = profile_bool("prevent_buy_above_last_sell", True)
BUY_AFTER_SELL_DISCOUNT_PCT = profile_float("buy_after_sell_discount_pct", 0.001)
USE_SIGNAL_STATUS_GATES = profile_bool("use_signal_status_gates", True)
REQUIRE_BOT_ACTION_ALLOWED = profile_bool("require_bot_action_allowed", True)
MAX_SIGNAL_AGE_MINUTES = profile_float("max_signal_age_minutes", 30.0)
ENABLE_TARGET_LIMIT_BUYS = profile_bool("enable_target_limit_buys", True)
MAX_TARGET_LIMIT_ORDERS_PER_CYCLE = profile_int(
    "max_target_limit_orders_per_cycle",
    2
)
TARGET_LIMIT_MAX_PREMIUM_PCT = profile_float(
    "target_limit_max_premium_pct",
    0.0005
)
DRY_RUN = profile_bool("dry_run", False)
EXECUTION_BUFFER_PCT = profile_float("execution_buffer_pct", 0.0025)

TARGET_QUALITY_FILE = env_or_profile(
    "TARGET_QUALITY_FILE",
    "target_quality_file",
    "http://screenpi.local/bot/target_price_quality.json"
)
TARGET_QUALITY_ENABLED = str(env_or_profile(
    "TARGET_QUALITY_ENABLED",
    "target_quality_enabled",
    True
)).strip().lower() in ("1", "true", "yes", "on")
TARGET_QUALITY_FAIL_CLOSED = str(env_or_profile(
    "TARGET_QUALITY_FAIL_CLOSED",
    "target_quality_fail_closed",
    False
)).strip().lower() in ("1", "true", "yes", "on")
TARGET_QUALITY_MAX_AGE_MINUTES = float(env_or_profile(
    "TARGET_QUALITY_MAX_AGE_MINUTES",
    "target_quality_max_age_minutes",
    30
))
TARGET_QUALITY_MIN_SAMPLES = int(env_or_profile(
    "TARGET_QUALITY_MIN_SAMPLES",
    "target_quality_min_samples",
    20
))
TARGET_QUALITY_MIN_EV_PCT = float(env_or_profile(
    "TARGET_QUALITY_MIN_EV_PCT",
    "target_quality_min_ev_pct",
    0.02
))
TARGET_QUALITY_MIN_4H_FILL_PROBABILITY = float(env_or_profile(
    "TARGET_QUALITY_MIN_4H_FILL_PROBABILITY",
    "target_quality_min_4h_fill_probability",
    0.35
))
TARGET_QUALITY_ALLOWED_RECOMMENDATIONS = {
    value.strip()
    for value in str(env_or_profile(
        "TARGET_QUALITY_ALLOWED_RECOMMENDATIONS",
        "target_quality_allowed_recommendations",
        "buy_allowed,watch"
    )).split(",")
    if value.strip()
}
DERIVE_TARGETS_FROM_QUALITY = str(env_or_profile(
    "LLM_TARGET_DERIVE_TARGETS_FROM_QUALITY",
    "derive_targets_from_quality",
    True
)).strip().lower() in ("1", "true", "yes", "on")
KILL_SWITCH_FILE = env_or_profile(
    "LLM_TARGET_KILL_SWITCH_FILE",
    "kill_switch_file",
    ""
)
MAX_DAILY_REALIZED_LOSS_USD = float(env_or_profile(
    "LLM_TARGET_MAX_DAILY_REALIZED_LOSS_USD",
    "max_daily_realized_loss_usd",
    0
))
MAX_CONSECUTIVE_LOSING_SELLS = int(env_or_profile(
    "LLM_TARGET_MAX_CONSECUTIVE_LOSING_SELLS",
    "max_consecutive_losing_sells",
    0
))
ORDER_TRACKER_URL = (
    os.getenv("ORDER_TRACKER_URL")
    or os.getenv("EXTERNAL_ORDER_TRACKER_URL")
)
ORDER_TRACKER_USER_AGENT = os.getenv("ORDER_TRACKER_USER_AGENT")
ORDER_TRACKER_SYMBOL = env_or_profile(
    "ORDER_TRACKER_SYMBOL",
    "order_tracker_symbol",
    KRAKEN_PAIR
)
ORDER_TRACKER_TIMEOUT = float(env_or_profile(
    "ORDER_TRACKER_TIMEOUT_SECONDS",
    "order_tracker_timeout_seconds",
    5
))
ORDER_TRACKER_CHECKIN_TIMEOUT = float(env_or_profile(
    "ORDER_TRACKER_CHECKIN_TIMEOUT_SECONDS",
    "order_tracker_checkin_timeout_seconds",
    min(ORDER_TRACKER_TIMEOUT, 5)
))


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
    except Exception as exc:
        print(f"[{datetime.now(timezone.utc).isoformat()}] LOG_WRITE_ERROR: {exc}")


def console(message):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {message}")


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
            status=status,
            status_code=response.status_code
        )
    except Exception as exc:
        log_event(
            "ORDER_TRACKER_ERROR",
            message=str(exc),
            trade_id=trade_id,
            side=side,
            order_id=order_id,
            status=status
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
    except Exception as exc:
        log_event("ORDER_TRACKER_CHECKIN_ERROR", message=str(exc), status=status)


def load_state():
    default = {
        "open_buy_orders": {},
        "open_sell_orders": {},
        "last_nonce": 0,
        "last_sell_price": None,
        "last_sell_at": None,
        "last_cycle": None,
        "risk": {
            "day": None,
            "realized_pnl_usd": 0.0,
            "consecutive_losing_sells": 0,
            "last_realized_pnl_usd": None,
        },
        "stats": {
            "cycles": 0,
            "buy_orders_placed": 0,
            "buy_orders_filled": 0,
            "sell_orders_placed": 0,
            "sell_orders_filled": 0,
            "errors": 0
        }
    }
    if not os.path.exists(STATE_FILE):
        return default

    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)

    for key, value in default.items():
        state.setdefault(key, value)
    if not isinstance(state.get("risk"), dict):
        state["risk"] = {}
    for key, value in default["risk"].items():
        state["risk"].setdefault(key, value)
    for key, value in default["stats"].items():
        state["stats"].setdefault(key, value)
    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


state = load_state()


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
        raise RuntimeError("Missing LLM_SIGNAL_URL or SIGNAL_FILE")


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


def kraken_private(endpoint, data):
    payload = dict(data)
    payload["nonce"] = next_nonce()
    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": kraken_signature(endpoint, payload)
    }
    response = requests.post(
        KRAKEN_API_URL.rstrip("/") + endpoint,
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
    attempts = max(1, KRAKEN_NONCE_RETRIES + 1)
    for attempt in range(1, attempts + 1):
        try:
            return kraken_private(endpoint, data or {})
        except Exception as exc:
            message = str(exc)
            log_event(
                "KRAKEN_EXCEPTION",
                operation=label,
                message=message,
                attempt=attempt,
                kraken_key_fingerprint=key_fingerprint(KRAKEN_API_KEY)
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


def get_price():
    try:
        if KRAKEN_TICKER_URL:
            response = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()
        else:
            response = requests.get(
                KRAKEN_API_URL.rstrip("/") + "/0/public/Ticker",
                params={"pair": KRAKEN_PAIR},
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            data = response.json()
        ticker = next(iter(data.get("result", {}).values()), None)
        return float(ticker["c"][0]) if ticker else None
    except Exception as exc:
        log_event("PRICE_ERROR", message=str(exc))
        return None


PAIR_INFO_CACHE = None


def get_pair_info():
    global PAIR_INFO_CACHE
    if PAIR_INFO_CACHE:
        return PAIR_INFO_CACHE

    response = requests.get(
        KRAKEN_API_URL.rstrip("/") + "/0/public/AssetPairs",
        params={"pair": KRAKEN_PAIR},
        timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    data = response.json()
    pair_info = next(iter(data.get("result", {}).values()), None)
    if not pair_info:
        raise RuntimeError(f"Pair metadata not found for {KRAKEN_PAIR}")
    PAIR_INFO_CACHE = pair_info
    return PAIR_INFO_CACHE


def round_volume(volume):
    lot_decimals = int(get_pair_info().get("lot_decimals", 8))
    factor = 10 ** lot_decimals
    return math.floor(volume * factor) / factor


def round_price(price):
    return round(float(price), int(get_pair_info().get("pair_decimals", 1)))


def get_min_order_volume():
    return float(get_pair_info().get("ordermin") or 0)


def get_balances():
    result = safe_kraken_private("BALANCE", "/0/private/Balance")
    if not result:
        return None
    pair_info = get_pair_info()
    base_asset = pair_info["base"]
    quote_asset = pair_info["quote"]
    return (
        float(result["result"].get(base_asset, 0)),
        float(result["result"].get(quote_asset, 0))
    )


def query_orders(txids):
    txids = [txid for txid in txids if txid]
    if not txids:
        return {}

    result = safe_kraken_private(
        "QUERY_ORDERS",
        "/0/private/QueryOrders",
        {"txid": ",".join(txids)}
    )
    if not result:
        return {}
    return result.get("result", {})


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
    fill_price = fallback_price
    if fill_volume > 0 and fill_cost > 0:
        fill_price = fill_cost / fill_volume
    return {
        "volume": fill_volume or fallback_volume,
        "price": fill_price,
        "cost": fill_cost or None,
        "fee": fill_fee or None
    }


def target_price_from_quality_target(target):
    if not isinstance(target, dict):
        return None

    try:
        buy_price = float(target.get("buy_price"))
    except Exception:
        return None
    if buy_price <= 0:
        return None

    sell_pct = (
        target.get("best_profit_target_pct")
        if target.get("best_profit_target_pct") is not None else
        target.get("sell_pct")
    )
    return {
        "buy_price": buy_price,
        "sell_pct": sell_pct,
        "source": "target_quality"
    }


def derived_quality_target_prices(target_quality_payload):
    if not isinstance(target_quality_payload, dict):
        return []

    targets = target_quality_payload.get("targets")
    if not isinstance(targets, list):
        return []

    derived = []
    seen = set()
    for target in targets:
        normalized = target_price_from_quality_target(target)
        if normalized is None:
            continue
        key = round(normalized["buy_price"], 2)
        if key in seen:
            continue
        seen.add(key)
        derived.append(normalized)
    return derived


def add_quality_target_fallback(signal, quality_snapshot):
    targets = signal.get("target_prices")
    if isinstance(targets, list) and targets:
        return False
    if not DERIVE_TARGETS_FROM_QUALITY or not quality_snapshot.get("available"):
        return False

    derived_targets = derived_quality_target_prices(quality_snapshot.get("payload"))
    if not derived_targets:
        return False

    signal["target_prices"] = derived_targets
    signal["target_prices_source"] = "target_quality"
    return True


def place_limit_buy(price, volume):
    if DRY_RUN:
        return {"result": {"txid": [f"dry_buy_{int(time.time())}"]}}
    return safe_kraken_private("BUY", "/0/private/AddOrder", {
        "pair": KRAKEN_PAIR,
        "type": "buy",
        "ordertype": "limit",
        "price": str(round_price(price)),
        "volume": str(round_volume(volume))
    })


def place_limit_sell(price, volume):
    if DRY_RUN:
        return {"result": {"txid": [f"dry_sell_{int(time.time())}"]}}
    return safe_kraken_private("SELL", "/0/private/AddOrder", {
        "pair": KRAKEN_PAIR,
        "type": "sell",
        "ordertype": "limit",
        "price": str(round_price(price)),
        "volume": str(round_volume(volume))
    })


def sell_target_price(buy_price, target_profit_pct):
    return buy_price * (1 + target_profit_pct + ROUND_TRIP_FEE_PCT)


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


def reset_daily_risk_if_needed(now):
    today = now.date().isoformat()
    risk = state.setdefault("risk", {})
    if risk.get("day") == today:
        return

    risk["day"] = today
    risk["realized_pnl_usd"] = 0.0
    risk["consecutive_losing_sells"] = 0
    risk["last_realized_pnl_usd"] = None
    save_state(state)


def record_realized_sell_pnl(order, fill):
    volume = fill.get("volume") or order.get("volume") or 0
    buy_price = order.get("buy_price") or 0
    sell_price = fill.get("price") or order.get("sell_price") or 0
    buy_fee = order.get("buy_fee") or 0
    sell_fee = fill.get("fee") or 0
    pnl = ((sell_price - buy_price) * volume) - buy_fee - sell_fee

    risk = state.setdefault("risk", {})
    risk["realized_pnl_usd"] = float(risk.get("realized_pnl_usd") or 0) + pnl
    risk["last_realized_pnl_usd"] = pnl
    if pnl < 0:
        risk["consecutive_losing_sells"] = int(
            risk.get("consecutive_losing_sells") or 0
        ) + 1
    else:
        risk["consecutive_losing_sells"] = 0
    return pnl


def risk_gate_failure():
    if KILL_SWITCH_FILE and os.path.exists(os.path.expanduser(KILL_SWITCH_FILE)):
        return {
            "reason": "kill_switch_file_present",
            "kill_switch_file": os.path.expanduser(KILL_SWITCH_FILE)
        }

    risk = state.get("risk", {})
    realized_pnl = float(risk.get("realized_pnl_usd") or 0)
    if (
        MAX_DAILY_REALIZED_LOSS_USD > 0
        and realized_pnl <= -abs(MAX_DAILY_REALIZED_LOSS_USD)
    ):
        return {
            "reason": "max_daily_realized_loss",
            "daily_realized_pnl_usd": realized_pnl,
            "max_daily_realized_loss_usd": MAX_DAILY_REALIZED_LOSS_USD
        }

    losing_sells = int(risk.get("consecutive_losing_sells") or 0)
    if (
        MAX_CONSECUTIVE_LOSING_SELLS > 0
        and losing_sells >= MAX_CONSECUTIVE_LOSING_SELLS
    ):
        return {
            "reason": "max_consecutive_losing_sells",
            "consecutive_losing_sells": losing_sells,
            "max_consecutive_losing_sells": MAX_CONSECUTIVE_LOSING_SELLS
        }

    return None


def load_signal():
    try:
        if SIGNAL_FILE:
            with open(SIGNAL_FILE, encoding="utf-8") as f:
                return json.load(f)
        response = requests.get(LLM_SIGNAL_URL, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        log_event("SIGNAL_ERROR", message=str(exc))
        return None


def normalize_signal(signal):
    return normalize_signal_payload(
        signal,
        asset_id=SIGNAL_ASSET_ID,
        pair=KRAKEN_PAIR
    )


def signal_age_minutes(signal, now):
    freshness = signal.get("freshness")
    freshness_processed_at = None
    if isinstance(freshness, dict):
        freshness_processed_at = freshness.get("processed_at")
    processed_at = parse_iso8601(signal.get("processed_at") or freshness_processed_at)
    if processed_at is None:
        return None
    return (now - processed_at).total_seconds() / 60.0


def signal_stale_after_minutes(signal):
    freshness = signal.get("freshness")
    freshness_stale_after = None
    if isinstance(freshness, dict):
        freshness_stale_after = positive_float(freshness.get("stale_after_minutes"))

    if freshness_stale_after is None:
        return MAX_SIGNAL_AGE_MINUTES

    return min(freshness_stale_after, MAX_SIGNAL_AGE_MINUTES)


def source_status_allows_trading(source_status):
    if not USE_SIGNAL_STATUS_GATES or not isinstance(source_status, dict):
        return True, None
    for source_name, status_info in source_status.items():
        if not isinstance(status_info, dict):
            continue
        status_value = status_info.get("status")
        if status_value in ("fresh", "not_configured", None):
            continue
        return False, f"source_status_{source_name}_{status_value}"
    return True, None


def signal_gate_failure(signal, now):
    age_minutes = signal_age_minutes(signal, now)
    stale_after_minutes = signal_stale_after_minutes(signal)
    if age_minutes is not None and age_minutes > stale_after_minutes:
        return {
            "reason": "signal_too_old",
            "signal_age_minutes": age_minutes,
            "signal_stale_after_minutes": stale_after_minutes
        }

    freshness_ok, freshness_reason = source_status_allows_trading(
        signal.get("source_status")
    )
    if not freshness_ok:
        return {"reason": freshness_reason}

    if REQUIRE_BOT_ACTION_ALLOWED and signal.get("bot_action_allowed") is False:
        return {"reason": "bot_action_not_allowed"}

    return None


def target_limit_orders(signal, current_price, total_trade_value, max_buy_price=None):
    if not ENABLE_TARGET_LIMIT_BUYS:
        return []

    targets = []
    for target in signal.get("target_prices", []):
        if not isinstance(target, dict):
            continue
        try:
            buy_price = float(target.get("buy_price"))
        except Exception:
            continue
        if buy_price <= 0:
            continue

        max_price = current_price * (1 + TARGET_LIMIT_MAX_PREMIUM_PCT)
        if buy_price > max_price:
            continue
        if max_buy_price is not None and buy_price >= max_buy_price:
            continue

        allocation = target.get("sell_pct")
        try:
            allocation = float(allocation) if allocation is not None else 1.0
        except Exception:
            allocation = 1.0
        if allocation <= 0:
            allocation = 1.0

        targets.append({
            "buy_price": buy_price,
            "allocation": allocation,
            "signal_sell_pct": target.get("sell_pct"),
        })

    if not targets:
        return []

    targets = targets[:MAX_TARGET_LIMIT_ORDERS_PER_CYCLE]
    allocation_sum = sum(target["allocation"] for target in targets)
    return [
        {
            "buy_price": target["buy_price"],
            "trade_value": total_trade_value * target["allocation"] / allocation_sum,
            "allocation_pct": target["allocation"] / allocation_sum,
            "signal_sell_pct": target["signal_sell_pct"],
        }
        for target in targets
    ]


def evaluate_candidate_target_quality(candidate_buy_price, snapshot):
    if not TARGET_QUALITY_ENABLED:
        return {
            "quality_allowed": True,
            "quality_reason": "target_quality_disabled",
            "quality_matched_buy_price": None,
            "quality_recommendation": None,
            "quality_matched_sample_count": None,
            "quality_fill_probability_4h": None,
            "quality_best_ev_pct": None,
            "quality_best_profit_target_pct": None,
            "quality_profit_target_override": None
        }

    if not snapshot.get("available"):
        decision = unavailable_quality_decision(
            snapshot,
            fail_closed=TARGET_QUALITY_FAIL_CLOSED
        )
        return {
            "quality_allowed": decision["allowed"],
            "quality_reason": decision["reason"],
            "quality_matched_buy_price": None,
            "quality_recommendation": None,
            "quality_matched_sample_count": None,
            "quality_fill_probability_4h": None,
            "quality_best_ev_pct": None,
            "quality_best_profit_target_pct": None,
            "quality_profit_target_override": None
        }

    matched = match_quality_target(candidate_buy_price, snapshot.get("targets", []))
    evaluation = evaluate_quality_target(
        matched,
        min_samples=TARGET_QUALITY_MIN_SAMPLES,
        min_ev_pct=TARGET_QUALITY_MIN_EV_PCT,
        min_4h_fill_probability=TARGET_QUALITY_MIN_4H_FILL_PROBABILITY,
        allowed_recommendations=TARGET_QUALITY_ALLOWED_RECOMMENDATIONS
    )
    return {
        "quality_allowed": evaluation["allowed"],
        "quality_reason": evaluation["reason"],
        "quality_matched_buy_price": evaluation["matched_buy_price"],
        "quality_recommendation": evaluation["recommendation"],
        "quality_matched_sample_count": evaluation["matched_sample_count"],
        "quality_fill_probability_4h": evaluation["fill_probability_4h"],
        "quality_best_ev_pct": evaluation["best_expected_value_pct_per_signal"],
        "quality_best_profit_target_pct": evaluation["best_profit_target_pct"],
        "quality_profit_target_override": normalize_profit_target_pct(
            evaluation["best_profit_target_pct"]
        ) if evaluation["allowed"] else None
    }


def process_open_buy_orders(cycle_id):
    statuses = query_orders(state["open_buy_orders"].keys())
    for txid, order in list(state["open_buy_orders"].items()):
        status = statuses.get(txid)
        if status is None:
            continue
        order_status = status.get("status")
        if order_status == "closed":
            fill = fill_values(status, order.get("volume"), order.get("price"))
            state["stats"]["buy_orders_filled"] += 1
            del state["open_buy_orders"][txid]
            save_state(state)
            log_and_console(
                "BUY_ORDER_FILLED",
                message=f"BUY filled @ {round_price(fill['price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=fill["volume"],
                price=fill["price"]
            )
            notify_order_tracker(
                trade_id=order.get("trade_id") or txid,
                side="buy",
                price=tracker_value(fill.get("price"), order.get("price")),
                quantity=tracker_value(fill.get("volume"), order.get("volume")),
                order_id=txid,
                fee=fill.get("fee"),
                timestamp=cycle_id,
                status="closed"
            )
            profit_pct = order.get("target_profit_pct", TARGET_PROFIT_PCT)
            sell_price = sell_target_price(fill["price"], profit_pct)
            sell_result = place_limit_sell(sell_price, fill["volume"])
            if sell_result and sell_result.get("result", {}).get("txid"):
                sell_txid = sell_result["result"]["txid"][0]
                state["open_sell_orders"][sell_txid] = {
                    "buy_txid": txid,
                    "buy_price": fill["price"],
                    "buy_fee": fill["fee"],
                    "sell_price": sell_price,
                    "volume": fill["volume"],
                    "placed_at": cycle_id,
                    "target_profit_pct": profit_pct,
                    "trade_id": order.get("trade_id") or txid
                }
                state["stats"]["sell_orders_placed"] += 1
                save_state(state)
                log_and_console(
                    "SELL_ORDER_PLACED",
                    message=f"SELL placed @ {round_price(sell_price)}",
                    cycle_id=cycle_id,
                    txid=sell_txid,
                    buy_txid=txid,
                    volume=fill["volume"],
                    price=sell_price,
                    buy_price=fill["price"],
                    target_profit_pct=profit_pct
                )
                notify_order_tracker(
                    trade_id=order.get("trade_id") or txid,
                    side="sell",
                    price=sell_price,
                    quantity=fill["volume"],
                    order_id=sell_txid,
                    timestamp=cycle_id,
                    notes="profit_sell_submitted"
                )
        elif order_status in ("canceled", "expired"):
            del state["open_buy_orders"][txid]
            save_state(state)
            log_and_console(
                "ORDER_" + order_status.upper(),
                message=f"BUY order {order_status}",
                cycle_id=cycle_id,
                txid=txid
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


def process_open_sell_orders(cycle_id):
    statuses = query_orders(state["open_sell_orders"].keys())
    for txid, order in list(state["open_sell_orders"].items()):
        status = statuses.get(txid)
        if status is None:
            continue
        order_status = status.get("status")
        if order_status == "closed":
            fill = fill_values(status, order.get("volume"), order.get("sell_price"))
            state["stats"]["sell_orders_filled"] += 1
            state["last_sell_price"] = fill["price"] or order.get("sell_price")
            state["last_sell_at"] = cycle_id
            realized_pnl_usd = record_realized_sell_pnl(order, fill)
            del state["open_sell_orders"][txid]
            save_state(state)
            log_and_console(
                "SELL_ORDER_FILLED",
                message=f"SELL filled @ {round_price(state['last_sell_price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=fill["volume"],
                price=state["last_sell_price"],
                buy_price=order.get("buy_price"),
                realized_pnl_usd=realized_pnl_usd,
                daily_realized_pnl_usd=state.get("risk", {}).get(
                    "realized_pnl_usd"
                ),
                consecutive_losing_sells=state.get("risk", {}).get(
                    "consecutive_losing_sells"
                )
            )
            notify_order_tracker(
                trade_id=order.get("trade_id") or order.get("buy_txid") or txid,
                side="sell",
                price=tracker_value(fill.get("price"), order.get("sell_price")),
                quantity=tracker_value(fill.get("volume"), order.get("volume")),
                order_id=txid,
                fee=fill.get("fee"),
                timestamp=cycle_id,
                notes=f"realized_pnl_usd={realized_pnl_usd:.8f}",
                status="closed"
            )
        elif order_status in ("canceled", "expired"):
            del state["open_sell_orders"][txid]
            save_state(state)
            log_and_console(
                "ORDER_" + order_status.upper(),
                message=f"SELL order {order_status}",
                cycle_id=cycle_id,
                txid=txid
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


def skip_cycle(reason, cycle_id, **kwargs):
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="hold",
        reason=reason,
        **kwargs
    )
    log_event(
        "CYCLE_SUMMARY",
        cycle_id=cycle_id,
        side="hold",
        reason=reason,
        open_buy_count=len(state["open_buy_orders"]),
        open_sell_count=len(state["open_sell_orders"]),
        **kwargs
    )


def run_cycle():
    now = datetime.now(timezone.utc)
    cycle_id = now.isoformat()
    reset_daily_risk_if_needed(now)
    state["last_cycle"] = cycle_id
    state["stats"]["cycles"] += 1
    save_state(state)

    price = get_price()
    if price is None:
        skip_cycle("missing_price", cycle_id)
        return

    process_open_buy_orders(cycle_id)
    process_open_sell_orders(cycle_id)

    risk_failure = risk_gate_failure()
    if risk_failure is not None:
        skip_cycle(risk_failure.pop("reason"), cycle_id, price=price, **risk_failure)
        return

    signal = load_signal()
    if signal is None:
        skip_cycle("missing_signal", cycle_id, price=price)
        return

    sentiment = normalize_signal(signal)
    action_policy = sentiment.get("action_policy", {})
    action_recommendation = sentiment.get("action_recommendation")
    log_event(
        "SIGNAL_UPDATE",
        cycle_id=cycle_id,
        price=price,
        execution_signal=sentiment.get("execution_signal"),
        confidence=sentiment.get("confidence"),
        action_recommendation=action_recommendation,
        action_policy_reason=action_policy.get("reason") or sentiment.get("reason"),
        contributor_count=sentiment.get("contributor_count"),
        bot_action_allowed=sentiment.get("bot_action_allowed"),
        signal_status=sentiment.get("signal_status"),
        processed_at=sentiment.get("processed_at"),
        freshness=sentiment.get("freshness"),
        llm_target_count=len(sentiment.get("target_prices", []))
    )

    gate_failure = signal_gate_failure(sentiment, now)
    if gate_failure is not None:
        skip_cycle(gate_failure.pop("reason"), cycle_id, price=price, **gate_failure)
        return

    if action_recommendation != "bullish_allowed":
        skip_cycle(
            "action_policy_" + (action_recommendation or "missing"),
            cycle_id,
            price=price,
            action_recommendation=action_recommendation,
            action_policy_reason=action_policy.get("reason") or sentiment.get("reason"),
            contributor_count=sentiment.get("contributor_count"),
        )
        return

    if len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS:
        skip_cycle("max_open_buy_orders", cycle_id, price=price)
        return

    if len(state["open_sell_orders"]) >= MAX_OPEN_SELL_ORDERS:
        skip_cycle("max_open_sell_orders", cycle_id, price=price)
        return

    if current_inventory_usd(price) >= MAX_INVENTORY_USD:
        skip_cycle(
            "max_inventory_usd",
            cycle_id,
            price=price,
            deployed_inventory_usd=current_inventory_usd(price)
        )
        return

    balances = get_balances()
    if balances is None:
        skip_cycle("balance_fetch_failed", cycle_id, price=price)
        return

    _, quote_balance = balances
    trade_value = min(quote_balance * POSITION_SIZE_PCT, MAX_TRADE_USD)
    trade_value *= max(0, 1 - EXECUTION_BUFFER_PCT)
    trade_value = max(0, min(trade_value, MAX_INVENTORY_USD - current_inventory_usd(price)))
    if trade_value < MIN_TRADE_USD:
        skip_cycle(
            "small_trade",
            cycle_id,
            price=price,
            trade_value=trade_value,
            min_trade_usd=MIN_TRADE_USD
        )
        return

    last_sell_price = state.get("last_sell_price")
    max_rebuy_price = None
    if PREVENT_BUY_ABOVE_LAST_SELL and last_sell_price is not None:
        max_rebuy_price = float(last_sell_price) * (1 - BUY_AFTER_SELL_DISCOUNT_PCT)

    quality_snapshot = load_target_quality_snapshot(
        TARGET_QUALITY_FILE,
        TARGET_QUALITY_MAX_AGE_MINUTES,
        now=now,
        timeout=REQUEST_TIMEOUT
    ) if TARGET_QUALITY_ENABLED else {"available": False, "reason": "target_quality_disabled", "targets": []}
    used_quality_target_fallback = add_quality_target_fallback(
        sentiment,
        quality_snapshot
    )

    orders = target_limit_orders(sentiment, price, trade_value, max_rebuy_price)
    if not orders:
        skip_cycle(
            "no_target_limit_orders",
            cycle_id,
            price=price,
            target_prices_source=sentiment.get("target_prices_source"),
            target_quality_available=quality_snapshot.get("available"),
            target_quality_reason=quality_snapshot.get("reason")
        )
        return

    min_volume = get_min_order_volume()
    placed_orders = 0
    for order in orders:
        if len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS:
            break

        target_price = order["buy_price"]
        volume = round_volume(order["trade_value"] / target_price)
        if volume <= 0 or volume < min_volume:
            log_event(
                "TRADE_DECISION",
                cycle_id=cycle_id,
                side="hold",
                reason="target_below_min_volume",
                price=price,
                target_buy_price=target_price,
                trade_value=order["trade_value"],
                volume=volume,
                min_volume=min_volume
            )
            continue

        quality = evaluate_candidate_target_quality(target_price, quality_snapshot)
        log_event(
            "TARGET_QUALITY_EVAL",
            cycle_id=cycle_id,
            candidate_buy_price=round_price(target_price),
            matched_quality_buy_price=quality["quality_matched_buy_price"],
            recommendation=quality["quality_recommendation"],
            matched_sample_count=quality["quality_matched_sample_count"],
            fill_probability_4h=quality["quality_fill_probability_4h"],
            best_expected_value_pct_per_signal=quality["quality_best_ev_pct"],
            best_profit_target_pct=quality["quality_best_profit_target_pct"],
            quality_allowed=quality["quality_allowed"],
            quality_reason=quality["quality_reason"]
        )
        if not quality["quality_allowed"]:
            log_event(
                "TRADE_DECISION",
                cycle_id=cycle_id,
                side="hold",
                reason=quality["quality_reason"],
                price=price,
                target_buy_price=target_price,
                quality_allowed=False
            )
            continue

        target_profit_pct = quality["quality_profit_target_override"]
        if target_profit_pct is None:
            target_profit_pct = normalize_profit_target_pct(order["signal_sell_pct"])
        if target_profit_pct is None:
            target_profit_pct = TARGET_PROFIT_PCT

        result = place_limit_buy(target_price, volume)
        if not result or not result.get("result", {}).get("txid"):
            log_event(
                "ORDER_REJECTED",
                cycle_id=cycle_id,
                side="buy",
                price=round_price(target_price),
                volume=volume,
                target_profit_pct=target_profit_pct,
                quality_reason=quality["quality_reason"]
            )
            continue

        txid = result["result"]["txid"][0]
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": volume,
            "price": target_price,
            "placed_at": cycle_id,
            "target_profit_pct": target_profit_pct,
            "trade_id": txid
        }
        state["stats"]["buy_orders_placed"] += 1
        save_state(state)
        placed_orders += 1
        log_and_console(
            "BUY_ORDER_PLACED",
            message=f"BUY placed @ {round_price(target_price)}",
            cycle_id=cycle_id,
            txid=txid,
            volume=volume,
            price=target_price,
            target_profit_pct=target_profit_pct,
            quality_reason=quality["quality_reason"],
            target_prices_source=sentiment.get("target_prices_source")
        )
        notify_order_tracker(
            trade_id=txid,
            side="buy",
            price=target_price,
            quantity=volume,
            order_id=txid,
            timestamp=cycle_id,
            notes="target_limit_buy_submitted"
        )

    if placed_orders <= 0:
        skip_cycle("no_target_limit_orders_placed", cycle_id, price=price)
        return

    log_event(
        "CYCLE_SUMMARY",
        cycle_id=cycle_id,
        side="buy",
        reason="llm_target_limit_buy",
        price=price,
        trade_value=trade_value,
        open_buy_count=len(state["open_buy_orders"]),
        open_sell_count=len(state["open_sell_orders"]),
        deployed_inventory_usd=current_inventory_usd(price),
        action_recommendation=action_recommendation,
        target_quality_enabled=TARGET_QUALITY_ENABLED,
        target_quality_fail_closed=TARGET_QUALITY_FAIL_CLOSED,
        target_prices_source=sentiment.get("target_prices_source"),
        used_quality_target_fallback=used_quality_target_fallback
    )


def main():
    require_runtime_config()
    log_and_console(
        "BOT_START",
        message="LLM target bot starting",
        config_file=CONFIG_FILE,
        strategy_profile=STRATEGY_PROFILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        pair=KRAKEN_PAIR,
        kraken_api_url=KRAKEN_API_URL,
        kraken_key_fingerprint=key_fingerprint(KRAKEN_API_KEY),
        dry_run=DRY_RUN,
        min_trade_usd=MIN_TRADE_USD,
        position_size_pct=POSITION_SIZE_PCT,
        max_trade_usd=MAX_TRADE_USD,
        target_profit_pct=TARGET_PROFIT_PCT,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        max_open_sell_orders=MAX_OPEN_SELL_ORDERS,
        max_open_buy_orders=MAX_OPEN_BUY_ORDERS,
        max_inventory_usd=MAX_INVENTORY_USD,
        target_quality_file=TARGET_QUALITY_FILE,
        target_quality_enabled=TARGET_QUALITY_ENABLED,
        derive_targets_from_quality=DERIVE_TARGETS_FROM_QUALITY,
        target_quality_fail_closed=TARGET_QUALITY_FAIL_CLOSED,
        target_quality_max_age_minutes=TARGET_QUALITY_MAX_AGE_MINUTES,
        target_quality_min_samples=TARGET_QUALITY_MIN_SAMPLES,
        target_quality_min_ev_pct=TARGET_QUALITY_MIN_EV_PCT,
        target_quality_min_4h_fill_probability=(
            TARGET_QUALITY_MIN_4H_FILL_PROBABILITY
        ),
        target_quality_allowed_recommendations=sorted(
            TARGET_QUALITY_ALLOWED_RECOMMENDATIONS
        ),
        kill_switch_file=KILL_SWITCH_FILE,
        max_daily_realized_loss_usd=MAX_DAILY_REALIZED_LOSS_USD,
        max_consecutive_losing_sells=MAX_CONSECUTIVE_LOSING_SELLS,
        order_tracker_configured=bool(
            ORDER_TRACKER_URL and ORDER_TRACKER_USER_AGENT
        ),
        order_tracker_symbol=ORDER_TRACKER_SYMBOL,
        order_tracker_timeout_seconds=ORDER_TRACKER_TIMEOUT,
        order_tracker_checkin_timeout_seconds=ORDER_TRACKER_CHECKIN_TIMEOUT
    )

    while True:
        try:
            run_cycle()
            send_checkin(
                loop_count=state.get("stats", {}).get("cycles"),
                message="loop_complete"
            )
        except Exception as exc:
            state["stats"]["errors"] = int(state["stats"].get("errors") or 0) + 1
            save_state(state)
            log_event("LOOP_ERROR", message=str(exc))
            console(f"LOOP_ERROR: {exc}")
            send_checkin(
                status="error",
                loop_count=state.get("stats", {}).get("cycles"),
                message=short_error_summary(exc)
            )
        time.sleep(PRICE_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
