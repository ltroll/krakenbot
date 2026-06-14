#!/usr/bin/env python3

import base64
import hashlib
import hmac
import json
import os
import socket
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

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
STATUS_FILE = (
    os.getenv("RANGE_GRID_STATUS_FILE")
    or "range_grid_status.json"
)

KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
KRAKEN_ORDERBOOK_URL = os.getenv("KRAKEN_ORDERBOOK_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
SIGNAL_FILE = os.getenv("SIGNAL_FILE")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))

SNAPSHOT_LOG_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_SNAPSHOT_FILE",
    "range_grid_backtest_snapshot_log.jsonl"
)
SNAPSHOT_ROTATE_DAILY = os.getenv(
    "RANGE_GRID_BACKTEST_ROTATE_DAILY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")
SNAPSHOT_RETENTION_DAYS = int(os.getenv(
    "RANGE_GRID_BACKTEST_SNAPSHOT_RETENTION_DAYS",
    "14"
))


def now_utc():
    return datetime.now(timezone.utc)


def is_url(value):
    if not value:
        return False
    parsed = urlparse(str(value))
    return parsed.scheme in ("http", "https")


def read_json_source(source, timeout):
    if not source:
        return {
            "ok": False,
            "error": "missing_source",
            "payload": None,
        }

    try:
        if is_url(source):
            response = requests.get(source, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
        else:
            with open(source, encoding="utf-8") as f:
                payload = json.load(f)
        return {
            "ok": True,
            "error": None,
            "payload": payload,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "payload": None,
        }


def fetch_public_json(url, params=None, timeout=10):
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return {
            "ok": True,
            "error": None,
            "payload": response.json(),
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "payload": None,
        }


def next_nonce():
    return str(int(time.time() * 1000))


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


def fetch_private_json(endpoint, data=None, timeout=10):
    if not KRAKEN_API_KEY or not KRAKEN_API_SECRET:
        return {
            "ok": False,
            "error": "missing_private_api_credentials",
            "payload": None,
        }

    payload = dict(data or {})
    payload["nonce"] = next_nonce()
    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": kraken_signature(endpoint, payload)
    }
    url = KRAKEN_API_URL.rstrip("/") + endpoint

    try:
        response = requests.post(
            url,
            headers=headers,
            data=payload,
            timeout=timeout
        )
        response.raise_for_status()
        data = response.json()
        if data.get("error"):
            return {
                "ok": False,
                "error": str(data["error"]),
                "payload": data,
            }
        return {
            "ok": True,
            "error": None,
            "payload": data,
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "payload": None,
        }


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

    return select_strategy_profile(load_json_file(CONFIG_FILE))


def strategy_profile_path():
    expanded = os.path.expanduser(STRATEGY_PROFILE)
    if os.path.exists(expanded):
        return os.path.abspath(expanded)

    local_default = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        STRATEGY_PROFILE
    )
    if os.path.exists(local_default):
        return os.path.abspath(local_default)

    return os.path.abspath(expanded)


def file_sha256(path):
    if not path or not os.path.exists(path) or not os.path.isfile(path):
        return None

    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strategy_profile_snapshot():
    path = strategy_profile_path()
    profile = read_json_source(path, timeout=REQUEST_TIMEOUT)
    return {
        "path": path,
        "exists": os.path.exists(path),
        "sha256": file_sha256(path),
        "payload": profile["payload"] if profile["ok"] else None,
        "error": profile["error"],
    }


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def parse_strategy_modes(raw_value):
    if not raw_value:
        return []

    normalized_modes = []
    alias_map = {
        "llm": "llm_target",
        "sentiment": "llm_target",
    }
    valid_modes = {"low", "mean", "median", "high", "llm_target"}
    disabled_values = {"false", "none", "off", "disabled", "no"}

    for mode in str(raw_value).split(","):
        normalized_mode = mode.strip().lower()
        if not normalized_mode:
            continue
        if normalized_mode in disabled_values:
            continue
        normalized_mode = alias_map.get(normalized_mode, normalized_mode)
        if normalized_mode in valid_modes and normalized_mode not in normalized_modes:
            normalized_modes.append(normalized_mode)
    return normalized_modes


def normalize_buy_order(level, order):
    if not isinstance(order, dict):
        return {
            "level": str(level),
            "price": safe_float(level),
            "volume": None,
            "txid": None,
        }

    return {
        "level": str(level),
        "txid": order.get("txid"),
        "trade_id": order.get("trade_id") or order.get("txid"),
        "volume": safe_float(order.get("volume")),
        "price": safe_float(order.get("price")) or safe_float(level),
        "placed_at": order.get("placed_at"),
        "filled_at": order.get("filled_at"),
        "sell_pct_override": safe_float(order.get("sell_pct_override")),
        "buy_source": order.get("buy_source"),
        "sell_retry_after": order.get("sell_retry_after"),
        "sell_failure_reason": order.get("sell_failure_reason"),
    }


def normalize_sell_order(txid, order):
    if not isinstance(order, dict):
        return {
            "txid": txid,
            "price": None,
            "volume": None,
        }

    return {
        "txid": txid,
        "trade_id": order.get("trade_id") or txid,
        "level": str(order.get("level")) if order.get("level") is not None else None,
        "volume": safe_float(order.get("volume")),
        "price": safe_float(order.get("price")),
        "buy_price": safe_float(order.get("buy_price")),
        "placed_at": order.get("placed_at"),
        "sell_pct_override": safe_float(order.get("sell_pct_override")),
        "buy_source": order.get("buy_source"),
    }


def buy_source_bucket(buy_source):
    mapping = {
        "llm_target": "llm_target",
        "range_low": "range_low",
        "range_mean": "range_mean",
        "range_median": "range_median",
        "range_high_band": "range_high_band"
    }
    return mapping.get(buy_source or "", "unknown")


def state_snapshot():
    state_result = read_json_source(STATE_FILE, timeout=REQUEST_TIMEOUT)
    if not state_result["ok"] or not isinstance(state_result["payload"], dict):
        return {
            "ok": state_result["ok"],
            "error": state_result["error"],
            "path": os.path.abspath(STATE_FILE),
            "summary": None,
            "open_buy_orders": [],
            "open_sell_orders": [],
            "stats": None,
            "raw": state_result["payload"] if isinstance(state_result["payload"], dict) else None,
        }

    raw_state = state_result["payload"]
    open_buy_orders = [
        normalize_buy_order(level, order)
        for level, order in (raw_state.get("open_buy_orders") or {}).items()
    ]
    open_sell_orders = [
        normalize_sell_order(txid, order)
        for txid, order in (raw_state.get("open_sell_orders") or {}).items()
    ]

    open_buy_volume = round(sum(
        order["volume"] or 0.0
        for order in open_buy_orders
    ), 8)
    open_sell_volume = round(sum(
        order["volume"] or 0.0
        for order in open_sell_orders
    ), 8)
    deployed_inventory_usd = round(sum(
        (order["buy_price"] or order["price"] or 0.0) * (order["volume"] or 0.0)
        for order in open_sell_orders
    ), 8)
    inventory_buckets_usd = {}
    for order in open_buy_orders:
        bucket = buy_source_bucket(order.get("buy_source"))
        notional = (order.get("price") or 0.0) * (order.get("volume") or 0.0)
        inventory_buckets_usd[bucket] = round(
            inventory_buckets_usd.get(bucket, 0.0) + notional,
            8
        )
    for order in open_sell_orders:
        bucket = buy_source_bucket(order.get("buy_source"))
        notional = (order.get("buy_price") or order.get("price") or 0.0) * (
            order.get("volume") or 0.0
        )
        inventory_buckets_usd[bucket] = round(
            inventory_buckets_usd.get(bucket, 0.0) + notional,
            8
        )

    return {
        "ok": True,
        "error": None,
        "path": os.path.abspath(STATE_FILE),
        "summary": {
            "open_buy_count": len(open_buy_orders),
            "open_sell_count": len(open_sell_orders),
            "open_buy_volume": open_buy_volume,
            "open_sell_volume": open_sell_volume,
            "deployed_inventory_usd": deployed_inventory_usd,
            "inventory_buckets_usd": inventory_buckets_usd,
            "last_nonce": raw_state.get("last_nonce"),
            "private_api_backoff_until": raw_state.get("private_api_backoff_until"),
            "sell_insufficient_funds_backoff_until": raw_state.get(
                "sell_insufficient_funds_backoff_until"
            ),
            "range_low": safe_float(raw_state.get("range_low")),
            "range_high": safe_float(raw_state.get("range_high")),
            "range_mean": safe_float(raw_state.get("range_mean")),
            "range_median": safe_float(raw_state.get("range_median")),
            "last_range_refresh": raw_state.get("last_range_refresh"),
            "last_high_anchor_buy_at": raw_state.get("last_high_anchor_buy_at"),
            "last_sell_price": safe_float(raw_state.get("last_sell_price")),
            "last_sell_at": raw_state.get("last_sell_at"),
            "last_llm_sell_at": raw_state.get("last_llm_sell_at"),
            "processed_buy_fill_count": len((raw_state.get("processed_fills") or {}).get("buy", {})),
            "processed_sell_fill_count": len((raw_state.get("processed_fills") or {}).get("sell", {})),
        },
        "open_buy_orders": open_buy_orders,
        "open_sell_orders": open_sell_orders,
        "stats": raw_state.get("stats"),
        "raw": None,
    }


def status_snapshot():
    status_result = read_json_source(STATUS_FILE, timeout=REQUEST_TIMEOUT)
    if not status_result["ok"] or not isinstance(status_result["payload"], dict):
        return {
            "ok": status_result["ok"],
            "error": status_result["error"],
            "path": os.path.abspath(STATUS_FILE),
            "summary": None,
            "raw": status_result["payload"] if isinstance(status_result["payload"], dict) else None,
        }

    raw_status = status_result["payload"]
    summary = {
        "timestamp": raw_status.get("timestamp"),
        "operating_mode": raw_status.get("operating_mode"),
        "strategy_profile": raw_status.get("strategy_profile"),
        "grid_anchor": raw_status.get("grid_anchor"),
        "configured_strategy_modes": raw_status.get("configured_strategy_modes"),
        "strategy_modes": raw_status.get("strategy_modes"),
        "price": safe_float(raw_status.get("price")),
        "execution_signal": safe_float(raw_status.get("execution_signal")),
        "signal_status": raw_status.get("signal_status"),
        "action_recommendation": raw_status.get("action_recommendation"),
        "runtime_block_reason": raw_status.get("runtime_block_reason"),
        "realized_pnl_today": safe_float(raw_status.get("realized_pnl_today")),
        "sell_backlog_count": int(raw_status.get("sell_backlog_count") or 0),
        "sell_backlog_oldest_minutes": safe_float(raw_status.get("sell_backlog_oldest_minutes")),
        "range_fallback_active": bool(raw_status.get("range_fallback_active", False)),
        "open_buy_count": int(raw_status.get("open_buy_count") or 0),
        "open_sell_count": int(raw_status.get("open_sell_count") or 0),
        "deployed_inventory_usd": safe_float(raw_status.get("deployed_inventory_usd")),
        "inventory_buckets_usd": raw_status.get("inventory_buckets_usd"),
        "actions": raw_status.get("actions"),
    }
    return {
        "ok": True,
        "error": None,
        "path": os.path.abspath(STATUS_FILE),
        "summary": summary,
        "raw": None,
    }


def ticker_snapshot():
    if KRAKEN_TICKER_URL:
        result = fetch_public_json(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT)
    else:
        result = fetch_public_json(
            KRAKEN_API_URL.rstrip("/") + "/0/public/Ticker",
            params={"pair": KRAKEN_PAIR},
            timeout=REQUEST_TIMEOUT
        )

    ticker = None
    last_price = None
    if result["ok"]:
        ticker = next(iter(result["payload"].get("result", {}).values()), None)
        try:
            last_price = float(ticker["c"][0]) if ticker else None
        except Exception:
            last_price = None

    return {
        "source": KRAKEN_TICKER_URL or f"{KRAKEN_API_URL.rstrip('/')}/0/public/Ticker",
        "ok": result["ok"],
        "error": result["error"],
        "last_price": last_price,
        "payload": result["payload"],
    }


def private_balance_snapshot():
    result = fetch_private_json(
        "/0/private/Balance",
        timeout=REQUEST_TIMEOUT
    )
    balance_result = (
        result["payload"].get("result", {})
        if result["ok"] and isinstance(result.get("payload"), dict)
        else {}
    )
    usd_balance = None
    btc_balance = None
    if isinstance(balance_result, dict):
        for key in ("ZUSD", "USD"):
            usd_balance = safe_float(balance_result.get(key))
            if usd_balance is not None:
                break
        for key in ("XXBT", "XBT", "BTC"):
            btc_balance = safe_float(balance_result.get(key))
            if btc_balance is not None:
                break

    return {
        "source": f"{KRAKEN_API_URL.rstrip('/')}/0/private/Balance",
        "ok": result["ok"],
        "error": result["error"],
        "usd_balance": usd_balance,
        "btc_balance": btc_balance,
        "payload": result["payload"],
    }


def private_open_orders_snapshot():
    result = fetch_private_json(
        "/0/private/OpenOrders",
        timeout=REQUEST_TIMEOUT
    )
    open_orders_result = (
        result["payload"].get("result", {})
        if result["ok"] and isinstance(result.get("payload"), dict)
        else {}
    )
    open_orders = open_orders_result.get("open", {}) if isinstance(open_orders_result, dict) else {}
    open_order_count = 0
    reserved_sell_volume_btc = 0.0
    reserved_buy_notional_usd = 0.0

    if isinstance(open_orders, dict):
        open_order_count = len(open_orders)
        for order in open_orders.values():
            if not isinstance(order, dict):
                continue
            descr = order.get("descr", {})
            order_type = descr.get("type")
            volume = safe_float(order.get("vol"))
            price = safe_float(descr.get("price"))
            if order_type == "sell" and volume is not None:
                reserved_sell_volume_btc += volume
            if order_type == "buy" and volume is not None and price is not None:
                reserved_buy_notional_usd += volume * price

    return {
        "source": f"{KRAKEN_API_URL.rstrip('/')}/0/private/OpenOrders",
        "ok": result["ok"],
        "error": result["error"],
        "open_order_count": open_order_count,
        "reserved_sell_volume_btc": round(reserved_sell_volume_btc, 8),
        "reserved_buy_notional_usd": round(reserved_buy_notional_usd, 8),
        "payload": result["payload"],
    }


def orderbook_snapshot():
    if KRAKEN_ORDERBOOK_URL:
        result = fetch_public_json(
            KRAKEN_ORDERBOOK_URL,
            timeout=REQUEST_TIMEOUT
        )
    else:
        result = fetch_public_json(
            KRAKEN_API_URL.rstrip("/") + "/0/public/Depth",
            params={"pair": KRAKEN_PAIR, "count": 10},
            timeout=REQUEST_TIMEOUT
        )

    book = None
    best_bid = None
    best_ask = None
    if result["ok"]:
        book = next(iter(result["payload"].get("result", {}).values()), None)
        try:
            best_bid = float(book.get("bids", [])[0][0]) if book else None
        except Exception:
            best_bid = None
        try:
            best_ask = float(book.get("asks", [])[0][0]) if book else None
        except Exception:
            best_ask = None

    return {
        "source": KRAKEN_ORDERBOOK_URL or f"{KRAKEN_API_URL.rstrip('/')}/0/public/Depth",
        "ok": result["ok"],
        "error": result["error"],
        "best_bid": best_bid,
        "best_ask": best_ask,
        "payload": result["payload"],
    }


def append_jsonl(path, record):
    log_dir = os.path.dirname(path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def rotated_snapshot_path(base_path, dt):
    root, ext = os.path.splitext(base_path)
    suffix = dt.strftime("%Y%m%d")
    return f"{root}_{suffix}{ext or '.jsonl'}"


def prune_rotated_snapshots(base_path, retention_days, now=None):
    if retention_days <= 0:
        return []

    now = now or now_utc()
    cutoff_date = (now - timedelta(days=retention_days)).date()
    directory = os.path.dirname(base_path) or "."
    base_name = os.path.basename(base_path)
    root, ext = os.path.splitext(base_name)
    suffix_ext = ext or ".jsonl"
    prefix = f"{root}_"

    removed = []
    try:
        for name in os.listdir(directory):
            if not name.startswith(prefix) or not name.endswith(suffix_ext):
                continue
            stamp = name[len(prefix):-len(suffix_ext)]
            try:
                file_date = datetime.strptime(stamp, "%Y%m%d").date()
            except ValueError:
                continue
            if file_date < cutoff_date:
                os.remove(os.path.join(directory, name))
                removed.append(name)
    except FileNotFoundError:
        pass

    return removed


def build_snapshot():
    captured_at = now_utc().isoformat()
    signal_source = SIGNAL_FILE or LLM_SIGNAL_URL
    sentiment = read_json_source(signal_source, timeout=REQUEST_TIMEOUT)
    ticker = ticker_snapshot()
    orderbook = orderbook_snapshot()
    private_balance = private_balance_snapshot()
    private_open_orders = private_open_orders_snapshot()
    strategy = strategy_profile_snapshot()
    state = state_snapshot()
    status = status_snapshot()

    strategy_payload = strategy["payload"] if isinstance(strategy["payload"], dict) else {}
    grid_anchor = str(strategy_payload.get("grid_anchor", "low")).strip().lower()
    strategy_modes = parse_strategy_modes(grid_anchor)

    snapshot = {
        "captured_at": captured_at,
        "hostname": socket.gethostname(),
        "pair": KRAKEN_PAIR,
        "snapshot_kind": "range_grid_backtest_input",
        "sources": {
            "signal_source": signal_source,
            "ticker_source": ticker["source"],
            "orderbook_source": orderbook["source"],
            "private_balance_source": private_balance["source"],
            "private_open_orders_source": private_open_orders["source"],
            "strategy_profile_path": strategy["path"],
            "state_file_path": state["path"],
            "status_file_path": status["path"],
        },
        "strategy_context": {
            "grid_anchor": grid_anchor,
            "strategy_modes": strategy_modes,
            "range_window_hours": strategy_payload.get("range_window_hours"),
            "max_grid_size": strategy_payload.get("max_grid_size"),
            "profit_target_pct": strategy_payload.get("profit_target_pct"),
            "entry_step_pct": strategy_payload.get("entry_step_pct"),
            "position_size_pct": strategy_payload.get("position_size_pct"),
            "max_inventory_usd": strategy_payload.get("max_inventory_usd"),
            "max_open_sell_orders": strategy_payload.get("max_open_sell_orders"),
            "prevent_buy_above_last_sell": strategy_payload.get("prevent_buy_above_last_sell"),
            "buy_after_sell_discount_pct": strategy_payload.get("buy_after_sell_discount_pct"),
            "llm_target_min_signal": strategy_payload.get("llm_target_min_signal"),
            "low_min_signal": strategy_payload.get("low_min_signal"),
            "mean_min_signal": strategy_payload.get("mean_min_signal"),
            "median_min_signal": strategy_payload.get("median_min_signal"),
            "high_min_signal": strategy_payload.get("high_min_signal"),
            "flow_defensive_threshold": strategy_payload.get("flow_defensive_threshold"),
            "flow_block_threshold": strategy_payload.get("flow_block_threshold"),
            "mean_reversion_min_opportunity": strategy_payload.get("mean_reversion_min_opportunity"),
        },
        "signal": {
            "ok": sentiment["ok"],
            "error": sentiment["error"],
            "payload": sentiment["payload"],
        },
        "ticker": {
            "ok": ticker["ok"],
            "error": ticker["error"],
            "last_price": ticker["last_price"],
            "payload": ticker["payload"],
        },
        "orderbook": {
            "ok": orderbook["ok"],
            "error": orderbook["error"],
            "best_bid": orderbook["best_bid"],
            "best_ask": orderbook["best_ask"],
            "payload": orderbook["payload"],
        },
        "private_balance": {
            "ok": private_balance["ok"],
            "error": private_balance["error"],
            "usd_balance": private_balance["usd_balance"],
            "btc_balance": private_balance["btc_balance"],
            "payload": private_balance["payload"],
        },
        "private_open_orders": {
            "ok": private_open_orders["ok"],
            "error": private_open_orders["error"],
            "open_order_count": private_open_orders["open_order_count"],
            "reserved_sell_volume_btc": (
                private_open_orders["reserved_sell_volume_btc"]
            ),
            "reserved_buy_notional_usd": (
                private_open_orders["reserved_buy_notional_usd"]
            ),
            "payload": private_open_orders["payload"],
        },
        "strategy_profile": {
            "exists": strategy["exists"],
            "error": strategy["error"],
            "sha256": strategy["sha256"],
            "payload": strategy["payload"],
        },
        "state": {
            "ok": state["ok"],
            "error": state["error"],
            "summary": state["summary"],
            "open_buy_orders": state["open_buy_orders"],
            "open_sell_orders": state["open_sell_orders"],
            "stats": state["stats"],
        },
        "runtime_status": {
            "ok": status["ok"],
            "error": status["error"],
            "summary": status["summary"],
        },
    }

    errors = {}
    for key in (
        "signal",
        "ticker",
        "orderbook",
        "private_balance",
        "private_open_orders"
    ):
        error = snapshot[key]["error"]
        if error:
            errors[key] = error
    if strategy["error"]:
        errors["strategy_profile"] = strategy["error"]
    if state["error"]:
        errors["state"] = state["error"]
    if status["error"]:
        errors["runtime_status"] = status["error"]
    snapshot["errors"] = errors

    return snapshot


def main():
    snapshot = build_snapshot()
    now = now_utc()
    actual_snapshot_file = SNAPSHOT_LOG_FILE
    pruned_files = []

    if SNAPSHOT_ROTATE_DAILY:
        actual_snapshot_file = rotated_snapshot_path(SNAPSHOT_LOG_FILE, now)
        pruned_files = prune_rotated_snapshots(
            SNAPSHOT_LOG_FILE,
            SNAPSHOT_RETENTION_DAYS,
            now=now
        )

    append_jsonl(actual_snapshot_file, snapshot)
    print(json.dumps({
        "captured_at": snapshot["captured_at"],
        "snapshot_file": os.path.abspath(actual_snapshot_file),
        "snapshot_file_base": os.path.abspath(SNAPSHOT_LOG_FILE),
        "rotate_daily": SNAPSHOT_ROTATE_DAILY,
        "retention_days": SNAPSHOT_RETENTION_DAYS,
        "pruned_files": pruned_files,
        "errors": snapshot["errors"],
        "signal_ok": snapshot["signal"]["ok"],
        "ticker_ok": snapshot["ticker"]["ok"],
        "orderbook_ok": snapshot["orderbook"]["ok"],
        "private_balance_ok": snapshot["private_balance"]["ok"],
        "private_open_orders_ok": snapshot["private_open_orders"]["ok"],
        "state_ok": snapshot["state"]["ok"],
        "runtime_status_ok": snapshot["runtime_status"]["ok"],
    }))


if __name__ == "__main__":
    main()
