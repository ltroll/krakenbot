#!/usr/bin/env python3

import argparse
import csv
import json
import os
import time
from datetime import datetime, timezone

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False

ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

DEFAULT_DECISION_URL = "http://192.168.50.211/bot/competition_decision.json"
DEFAULT_CONFIG_FILE = "competition_bot_config.json"
DEFAULT_LOG_FILE = "competition_shadow_trade_log.jsonl"
DEFAULT_DECISION_CSV_FILE = "competition_shadow_decisions.csv"

DECISION_CSV_FIELDS = [
    "ts",
    "event",
    "status",
    "decision",
    "action",
    "reason",
    "source_age_minutes",
    "source_stale_after_minutes",
    "shadow_only",
    "live_trading_enabled",
    "asset_id",
    "kraken_pair",
    "last_price",
    "mid_price",
    "spread_bps",
    "top10_bid_depth_usd",
    "top10_ask_depth_usd",
    "trade_count",
    "total_notional_usd",
    "aggression_score",
    "filter_failures",
]


def parse_bool(value):
    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in ("1", "true", "yes", "on")


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="Shadow-only Kraken competition decision monitor."
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Evaluate the competition decision file once and exit.",
    )
    parser.add_argument(
        "--decision-url",
        help="Competition decision JSON URL. Overrides COMPETITION_DECISION_URL.",
    )
    parser.add_argument(
        "--config-file",
        help="Config JSON file. Overrides COMPETITION_CONFIG_FILE.",
    )
    parser.add_argument(
        "--log-file",
        help="JSONL log file. Overrides COMPETITION_TRADE_LOG_FILE.",
    )
    parser.add_argument(
        "--decision-csv-file",
        help="CSV decision trace file. Overrides COMPETITION_DECISION_CSV_FILE.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        help="Loop sleep interval. Overrides COMPETITION_POLL_INTERVAL_SECONDS.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        help="HTTP timeout. Overrides REQUEST_TIMEOUT_SECONDS.",
    )
    return parser.parse_args()


def load_json_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_config(path):
    if not path or not os.path.exists(path):
        return {}

    return load_json_file(path)


def env_or_config(name, config, key, default=None):
    value = os.getenv(name)
    if value is not None:
        return value

    return config.get(key, default)


def make_runtime(args):
    config_file = (
        args.config_file
        or os.getenv("COMPETITION_CONFIG_FILE")
        or DEFAULT_CONFIG_FILE
    )
    config = load_config(config_file)

    return {
        "config_file": config_file,
        "decision_url": (
            args.decision_url
            or env_or_config(
                "COMPETITION_DECISION_URL",
                config,
                "decision_url",
                DEFAULT_DECISION_URL,
            )
        ),
        "log_file": (
            args.log_file
            or env_or_config(
                "COMPETITION_TRADE_LOG_FILE",
                config,
                "log_file",
                DEFAULT_LOG_FILE,
            )
        ),
        "decision_csv_file": (
            args.decision_csv_file
            or env_or_config(
                "COMPETITION_DECISION_CSV_FILE",
                config,
                "decision_csv_file",
                DEFAULT_DECISION_CSV_FILE,
            )
        ),
        "poll_interval_seconds": int(
            args.interval_seconds
            or env_or_config(
                "COMPETITION_POLL_INTERVAL_SECONDS",
                config,
                "poll_interval_seconds",
                60,
            )
        ),
        "request_timeout_seconds": int(
            args.request_timeout_seconds
            or env_or_config(
                "REQUEST_TIMEOUT_SECONDS",
                config,
                "request_timeout_seconds",
                10,
            )
        ),
        "write_decision_csv": parse_bool(
            env_or_config(
                "COMPETITION_WRITE_DECISION_CSV",
                config,
                "write_decision_csv",
                True,
            )
        ),
    }


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def console(msg):
    print(f"[{utc_now_iso()}] {msg}")


def write_jsonl(path, record):
    log_dir = os.path.dirname(path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, sort_keys=True) + "\n")
        f.flush()


def log_event(runtime, event, message="", **kwargs):
    record = {
        "ts": utc_now_iso(),
        "event": event,
        "message": message,
    }
    record.update(kwargs)

    try:
        write_jsonl(runtime["log_file"], record)
    except Exception as e:
        console(f"LOG_WRITE_ERROR: {e}")


def flatten_filter_failures(payload):
    failures = payload.get("filter_failures")
    if not isinstance(failures, dict):
        return failures

    flattened = []
    for category, values in failures.items():
        if isinstance(values, list):
            for value in values:
                flattened.append(f"{category}:{value}")
        elif values:
            flattened.append(f"{category}:{values}")

    return flattened


def csv_row_from_result(result):
    payload = result.get("payload") or {}
    competition = payload.get("competition") or {}
    market = payload.get("market") or {}
    risk = payload.get("risk") or {}
    row = {
        "ts": utc_now_iso(),
        "event": "competition_decision",
        "status": payload.get("status"),
        "decision": payload.get("decision"),
        "action": result.get("action"),
        "reason": result.get("reason"),
        "source_age_minutes": payload.get("source_age_minutes"),
        "source_stale_after_minutes": payload.get("source_stale_after_minutes"),
        "shadow_only": risk.get("shadow_only"),
        "live_trading_enabled": risk.get("live_trading_enabled"),
        "asset_id": competition.get("asset_id"),
        "kraken_pair": competition.get("kraken_pair"),
        "last_price": market.get("last_price"),
        "mid_price": market.get("mid_price"),
        "spread_bps": market.get("spread_bps"),
        "top10_bid_depth_usd": market.get("top10_bid_depth_usd"),
        "top10_ask_depth_usd": market.get("top10_ask_depth_usd"),
        "trade_count": market.get("trade_count"),
        "total_notional_usd": market.get("total_notional_usd"),
        "aggression_score": market.get("aggression_score"),
        "filter_failures": flatten_filter_failures(payload),
    }

    if isinstance(row["filter_failures"], (dict, list)):
        row["filter_failures"] = json.dumps(row["filter_failures"], sort_keys=True)

    return row


def append_decision_csv(runtime, result):
    if not runtime["write_decision_csv"]:
        return

    path = runtime["decision_csv_file"]
    csv_dir = os.path.dirname(path)
    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)

    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    row = csv_row_from_result(result)
    csv_row = {field: row.get(field, "") for field in DECISION_CSV_FIELDS}

    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DECISION_CSV_FIELDS)
            if write_header:
                writer.writeheader()
            writer.writerow(csv_row)
    except Exception as e:
        log_event(runtime, "CSV_WRITE_ERROR", message=str(e), file=path)


def fetch_decision_payload(url, timeout):
    import requests

    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def number_value(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def decision_summary(payload):
    competition = payload.get("competition") or {}
    market = payload.get("market") or {}
    risk = payload.get("risk") or {}

    return {
        "schema_version": payload.get("schema_version"),
        "processed_at": payload.get("processed_at"),
        "source_timestamp": payload.get("source_timestamp"),
        "source_age_minutes": payload.get("source_age_minutes"),
        "source_stale_after_minutes": payload.get("source_stale_after_minutes"),
        "status": payload.get("status"),
        "decision": payload.get("decision"),
        "reason": payload.get("reason"),
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
        "invalidation": payload.get("invalidation"),
    }


def evaluate_competition_decision(payload):
    if not isinstance(payload, dict):
        return {
            "action": "do_nothing",
            "reason": "decision_payload_not_object",
            "payload": {},
        }

    summary = decision_summary(payload)

    if payload.get("status") != "ok":
        return {
            "action": "do_nothing",
            "reason": "status_not_ok",
            "payload": payload,
            "summary": summary,
        }

    source_age = number_value(payload.get("source_age_minutes"))
    stale_after = number_value(payload.get("source_stale_after_minutes"))
    if source_age is None or stale_after is None:
        return {
            "action": "do_nothing",
            "reason": "missing_source_freshness",
            "payload": payload,
            "summary": summary,
        }

    if source_age > stale_after:
        return {
            "action": "do_nothing",
            "reason": "source_snapshot_stale",
            "payload": payload,
            "summary": summary,
        }

    risk = payload.get("risk") or {}
    if risk.get("shadow_only") is True:
        if payload.get("decision") == "shadow_candidate":
            action = "record_tradeable_shadow_candidate"
            reason = "shadow_only_market_tradeable"
        else:
            action = "record_shadow_blocked"
            reason = payload.get("reason") or "shadow_only_decision_blocked"

        return {
            "action": action,
            "reason": reason,
            "payload": payload,
            "summary": summary,
        }

    return {
        "action": "do_nothing",
        "reason": "live_mode_not_supported_by_shadow_bot",
        "payload": payload,
        "summary": summary,
    }


def run_once(runtime):
    try:
        payload = fetch_decision_payload(
            runtime["decision_url"],
            runtime["request_timeout_seconds"],
        )
    except Exception as e:
        log_event(
            runtime,
            "COMPETITION_DECISION_FETCH_ERROR",
            message=str(e),
            decision_url=runtime["decision_url"],
        )
        return {
            "action": "do_nothing",
            "reason": "fetch_error",
            "error": str(e),
            "payload": {},
        }

    result = evaluate_competition_decision(payload)
    summary = result.get("summary") or decision_summary(payload)

    log_event(
        runtime,
        "COMPETITION_DECISION",
        message=result["reason"],
        action=result["action"],
        **summary,
    )
    append_decision_csv(runtime, result)
    return result


def main():
    args = parse_cli_args()
    runtime = make_runtime(args)

    log_event(
        runtime,
        "COMPETITION_BOT_START",
        message="competition shadow bot starting",
        decision_url=runtime["decision_url"],
        config_file=runtime["config_file"],
        poll_interval_seconds=runtime["poll_interval_seconds"],
        request_timeout_seconds=runtime["request_timeout_seconds"],
    )

    while True:
        result = run_once(runtime)
        console(
            "COMPETITION_DECISION: "
            f"action={result.get('action')} reason={result.get('reason')}"
        )

        if args.once:
            return 0

        time.sleep(runtime["poll_interval_seconds"])


if __name__ == "__main__":
    raise SystemExit(main())
