#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

DEFAULT_DECISION_URL = "http://192.168.50.211/bot/competition_decision.json"
DEFAULT_CONFIG_FILE = "competition_bot_config.json"
DEFAULT_SNAPSHOT_LOG_FILE = "competition_backtest_snapshot_log.jsonl"


def parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="Capture competition decision snapshots for backtesting."
    )
    parser.add_argument(
        "--config-file",
        default=os.getenv("COMPETITION_CONFIG_FILE", DEFAULT_CONFIG_FILE),
        help="Competition config JSON file.",
    )
    parser.add_argument(
        "--decision-url",
        help="Competition decision JSON URL.",
    )
    parser.add_argument(
        "--snapshot-file",
        help="JSONL snapshot log path.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=float,
        help="HTTP request timeout.",
    )
    parser.add_argument(
        "--rotate-daily",
        default=os.getenv("COMPETITION_BACKTEST_ROTATE_DAILY", "true"),
        help="Whether to append to date-suffixed JSONL files.",
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=int(os.getenv("COMPETITION_BACKTEST_RETENTION_DAYS", "14")),
        help="Number of rotated daily files to retain.",
    )
    return parser.parse_args()


def load_config(path):
    if not path or not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def env_config_value(env_name, config, key, default=None):
    value = os.getenv(env_name)
    if value is not None:
        return value
    return config.get(key, default)


def runtime_from_args(args):
    config = load_config(args.config_file)
    return {
        "config_file": args.config_file,
        "decision_url": (
            args.decision_url
            or env_config_value(
                "COMPETITION_DECISION_URL",
                config,
                "decision_url",
                DEFAULT_DECISION_URL,
            )
        ),
        "snapshot_file": (
            args.snapshot_file
            or env_config_value(
                "COMPETITION_BACKTEST_SNAPSHOT_FILE",
                config,
                "backtest_snapshot_file",
                DEFAULT_SNAPSHOT_LOG_FILE,
            )
        ),
        "request_timeout_seconds": float(
            args.request_timeout_seconds
            if args.request_timeout_seconds is not None
            else env_config_value(
                "REQUEST_TIMEOUT_SECONDS",
                config,
                "request_timeout_seconds",
                10,
            )
        ),
        "rotate_daily": parse_bool(args.rotate_daily, default=True),
        "retention_days": args.retention_days,
    }


def now_utc():
    return datetime.now(timezone.utc)


def sha256_text(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def rotated_snapshot_path(base_path, dt):
    root, ext = os.path.splitext(base_path)
    return f"{root}_{dt.strftime('%Y%m%d')}{ext or '.jsonl'}"


def append_jsonl(path, record):
    log_dir = os.path.dirname(path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")
        f.flush()


def prune_rotated_snapshots(base_path, retention_days, now=None):
    if retention_days <= 0:
        return []

    now = now or now_utc()
    cutoff = now.date() - timedelta(days=retention_days)
    root, ext = os.path.splitext(base_path)
    directory = os.path.dirname(root) or "."
    prefix = os.path.basename(root) + "_"
    suffix = ext or ".jsonl"
    pruned = []

    if not os.path.isdir(directory):
        return pruned

    for name in os.listdir(directory):
        if not name.startswith(prefix) or not name.endswith(suffix):
            continue
        date_text = name[len(prefix):-len(suffix)]
        try:
            file_date = datetime.strptime(date_text, "%Y%m%d").date()
        except ValueError:
            continue
        if file_date >= cutoff:
            continue
        path = os.path.join(directory, name)
        try:
            os.remove(path)
            pruned.append(os.path.abspath(path))
        except OSError:
            continue

    return pruned


def fetch_json(url, timeout):
    import requests

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    text = response.text
    return json.loads(text), sha256_text(text)


def compact_decision_summary(payload):
    if not isinstance(payload, dict):
        return {}

    competition = payload.get("competition") or {}
    market = payload.get("market") or {}
    risk = payload.get("risk") or {}

    return {
        "status": payload.get("status"),
        "decision": payload.get("decision"),
        "reason": payload.get("reason"),
        "processed_at": payload.get("processed_at"),
        "source_timestamp": payload.get("source_timestamp"),
        "source_age_minutes": payload.get("source_age_minutes"),
        "source_stale_after_minutes": payload.get("source_stale_after_minutes"),
        "asset_id": competition.get("asset_id"),
        "symbol": competition.get("symbol"),
        "kraken_pair": competition.get("kraken_pair"),
        "competition_mode": competition.get("mode"),
        "last_price": market.get("last_price"),
        "mid_price": market.get("mid_price"),
        "spread_bps": market.get("spread_bps"),
        "top10_bid_depth_usd": market.get("top10_bid_depth_usd"),
        "top10_ask_depth_usd": market.get("top10_ask_depth_usd"),
        "trade_count": market.get("trade_count"),
        "total_notional_usd": market.get("total_notional_usd"),
        "aggression_score": market.get("aggression_score"),
        "shadow_only": risk.get("shadow_only"),
        "live_trading_enabled": risk.get("live_trading_enabled"),
        "max_position_usd": risk.get("max_position_usd"),
        "max_daily_loss_usd": risk.get("max_daily_loss_usd"),
        "max_trades_per_day": risk.get("max_trades_per_day"),
        "max_spread_bps": risk.get("max_spread_bps"),
        "min_top10_usd_depth": risk.get("min_top10_usd_depth"),
        "filter_failures": payload.get("filter_failures"),
    }


def build_snapshot(decision_url, timeout):
    captured_at = now_utc().isoformat()
    decision = {
        "ok": False,
        "error": None,
        "url": decision_url,
        "sha256": None,
        "payload": None,
        "summary": {},
    }

    try:
        payload, digest = fetch_json(decision_url, timeout)
        decision.update({
            "ok": True,
            "sha256": digest,
            "payload": payload,
            "summary": compact_decision_summary(payload),
        })
    except Exception as e:
        decision["error"] = str(e)

    return {
        "captured_at": captured_at,
        "snapshot_kind": "competition_backtest_input",
        "decision": decision,
        "errors": {"decision": decision["error"]} if decision["error"] else {},
    }


def main():
    args = parse_cli_args()
    runtime = runtime_from_args(args)
    snapshot = build_snapshot(
        runtime["decision_url"],
        runtime["request_timeout_seconds"],
    )
    now = now_utc()
    snapshot_file = runtime["snapshot_file"]
    pruned_files = []

    if runtime["rotate_daily"]:
        snapshot_file = rotated_snapshot_path(runtime["snapshot_file"], now)
        pruned_files = prune_rotated_snapshots(
            runtime["snapshot_file"],
            runtime["retention_days"],
            now=now,
        )

    append_jsonl(snapshot_file, snapshot)
    print(json.dumps({
        "captured_at": snapshot["captured_at"],
        "snapshot_file": os.path.abspath(snapshot_file),
        "snapshot_file_base": os.path.abspath(runtime["snapshot_file"]),
        "config_file": os.path.abspath(runtime["config_file"]),
        "rotate_daily": runtime["rotate_daily"],
        "retention_days": runtime["retention_days"],
        "pruned_files": pruned_files,
        "errors": snapshot["errors"],
        "decision_ok": snapshot["decision"]["ok"],
        "status": snapshot["decision"]["summary"].get("status"),
        "decision": snapshot["decision"]["summary"].get("decision"),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
