#!/usr/bin/env python3

import hashlib
import json
import os
import socket
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

from signal_normalizer import normalize_signal_payload, selected_signal_asset_id


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
KRAKEN_ORDERBOOK_URL = os.getenv("KRAKEN_ORDERBOOK_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
SIGNAL_FILE = os.getenv("SIGNAL_FILE")
TARGET_QUALITY_FILE = os.getenv(
    "TARGET_QUALITY_FILE",
    "http://screenpi.local/bot/target_price_quality.json"
)
LLM_TARGET_STRATEGY_PROFILE = (
    os.getenv("LLM_TARGET_STRATEGY_PROFILE")
    or os.getenv("STRATEGY_PROFILE")
    or "llm_target_strategy_default.json"
)


def configured_snapshot_log_file():
    snapshot_dir = os.getenv("LLM_TARGET_BACKTEST_SNAPSHOT_DIR")
    if snapshot_dir:
        basename = os.getenv(
            "LLM_TARGET_BACKTEST_SNAPSHOT_BASENAME",
            "llm_target_backtest_snapshot_log.jsonl"
        )
        return os.path.join(os.path.expanduser(snapshot_dir), basename)

    return os.getenv(
        "LLM_TARGET_BACKTEST_SNAPSHOT_FILE",
        "llm_target_backtest_snapshot_log.jsonl"
    )


SNAPSHOT_LOG_FILE = configured_snapshot_log_file()
SNAPSHOT_ROTATE_DAILY = os.getenv(
    "LLM_TARGET_BACKTEST_ROTATE_DAILY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")
SNAPSHOT_RETENTION_DAYS = int(os.getenv(
    "LLM_TARGET_BACKTEST_SNAPSHOT_RETENTION_DAYS",
    "14"
))
SNAPSHOT_REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))


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
            "payload": None
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
            "payload": payload
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "payload": None
        }


def file_sha256(path):
    if not path or not os.path.exists(path) or not os.path.isfile(path):
        return None

    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def strategy_profile_path():
    expanded = os.path.expanduser(LLM_TARGET_STRATEGY_PROFILE)
    if os.path.exists(expanded):
        return os.path.abspath(expanded)

    local_default = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        LLM_TARGET_STRATEGY_PROFILE
    )
    if os.path.exists(local_default):
        return os.path.abspath(local_default)

    return os.path.abspath(expanded)


def strategy_profile_snapshot():
    path = strategy_profile_path()
    profile = read_json_source(path, timeout=SNAPSHOT_REQUEST_TIMEOUT)
    return {
        "path": path,
        "exists": os.path.exists(path),
        "sha256": file_sha256(path),
        "payload": profile["payload"] if profile["ok"] else None,
        "error": profile["error"]
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


def target_count(payload, key):
    if not isinstance(payload, dict):
        return 0
    targets = payload.get(key)
    return len(targets) if isinstance(targets, list) else 0


def fetch_public_json(url, params=None, timeout=10):
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return {
            "ok": True,
            "error": None,
            "payload": response.json()
        }
    except Exception as exc:
        return {
            "ok": False,
            "error": str(exc),
            "payload": None
        }


def ticker_snapshot():
    if KRAKEN_TICKER_URL:
        result = fetch_public_json(KRAKEN_TICKER_URL, timeout=SNAPSHOT_REQUEST_TIMEOUT)
    else:
        result = fetch_public_json(
            KRAKEN_API_URL.rstrip("/") + "/0/public/Ticker",
            params={"pair": KRAKEN_PAIR},
            timeout=SNAPSHOT_REQUEST_TIMEOUT
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
        "payload": result["payload"]
    }


def orderbook_snapshot():
    if KRAKEN_ORDERBOOK_URL:
        result = fetch_public_json(
            KRAKEN_ORDERBOOK_URL,
            timeout=SNAPSHOT_REQUEST_TIMEOUT
        )
    else:
        result = fetch_public_json(
            KRAKEN_API_URL.rstrip("/") + "/0/public/Depth",
            params={"pair": KRAKEN_PAIR, "count": 5},
            timeout=SNAPSHOT_REQUEST_TIMEOUT
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
        "payload": result["payload"]
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
    sentiment = read_json_source(signal_source, timeout=SNAPSHOT_REQUEST_TIMEOUT)
    signal_asset_id = selected_signal_asset_id(pair=KRAKEN_PAIR)
    signal_payload = (
        normalize_signal_payload(
            sentiment["payload"],
            asset_id=signal_asset_id,
            pair=KRAKEN_PAIR
        )
        if sentiment["ok"] else
        sentiment["payload"]
    )
    target_quality = read_json_source(
        TARGET_QUALITY_FILE,
        timeout=SNAPSHOT_REQUEST_TIMEOUT
    )
    signal_target_count = target_count(signal_payload, "target_prices")
    quality_target_count = target_count(target_quality["payload"], "targets")
    derived_targets = []
    if sentiment["ok"] and signal_target_count == 0:
        derived_targets = derived_quality_target_prices(target_quality["payload"])
        if derived_targets:
            signal_payload = dict(signal_payload)
            signal_payload["target_prices"] = derived_targets
            signal_payload["target_prices_source"] = "target_quality"
            signal_target_count = len(derived_targets)
    ticker = ticker_snapshot()
    orderbook = orderbook_snapshot()
    strategy = strategy_profile_snapshot()

    snapshot = {
        "captured_at": captured_at,
        "hostname": socket.gethostname(),
        "pair": KRAKEN_PAIR,
        "snapshot_kind": "llm_target_backtest_input",
        "sources": {
            "signal_source": signal_source,
            "signal_asset_id": signal_asset_id,
            "signal_target_count": signal_target_count,
            "quality_target_count": quality_target_count,
            "derived_signal_target_count": len(derived_targets),
            "target_quality_source": TARGET_QUALITY_FILE,
            "ticker_source": ticker["source"],
            "orderbook_source": orderbook["source"],
            "strategy_profile_path": strategy["path"]
        },
        "signal": {
            "ok": sentiment["ok"],
            "error": sentiment["error"],
            "payload": signal_payload
        },
        "target_quality": {
            "ok": target_quality["ok"],
            "error": target_quality["error"],
            "payload": target_quality["payload"]
        },
        "ticker": {
            "ok": ticker["ok"],
            "error": ticker["error"],
            "last_price": ticker["last_price"],
            "payload": ticker["payload"]
        },
        "orderbook": {
            "ok": orderbook["ok"],
            "error": orderbook["error"],
            "best_bid": orderbook["best_bid"],
            "best_ask": orderbook["best_ask"],
            "payload": orderbook["payload"]
        },
        "strategy_profile": {
            "exists": strategy["exists"],
            "error": strategy["error"],
            "sha256": strategy["sha256"],
            "payload": strategy["payload"]
        }
    }

    errors = {}
    for key in ("signal", "target_quality", "ticker", "orderbook"):
        error = snapshot[key]["error"]
        if error:
            errors[key] = error
    if strategy["error"]:
        errors["strategy_profile"] = strategy["error"]
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
    print(
        json.dumps(
            {
                "captured_at": snapshot["captured_at"],
                "snapshot_file": os.path.abspath(actual_snapshot_file),
                "snapshot_file_base": os.path.abspath(SNAPSHOT_LOG_FILE),
                "rotate_daily": SNAPSHOT_ROTATE_DAILY,
                "retention_days": SNAPSHOT_RETENTION_DAYS,
                "pruned_files": pruned_files,
                "errors": snapshot["errors"],
                "signal_ok": snapshot["signal"]["ok"],
                "target_quality_ok": snapshot["target_quality"]["ok"],
                "ticker_ok": snapshot["ticker"]["ok"],
                "orderbook_ok": snapshot["orderbook"]["ok"]
            }
        )
    )


if __name__ == "__main__":
    main()
