#!/usr/bin/env python3

"""Validate a range-grid instance environment without contacting Kraken."""

import argparse
import ipaddress
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from range_grid_guardrails import validate_strategy_config
from range_grid_instance import (
    instance_runtime_path,
    instance_web_path,
    normalize_instance_id,
    parse_optional_positive_float,
    validate_instance_configuration,
)


def bool_value(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def load_strategy(directory, filename):
    path = Path(filename).expanduser()
    if not path.is_absolute():
        path = Path(directory).expanduser() / path
    path = path.resolve()
    with path.open(encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError("strategy file must contain a JSON object")
    return path, payload


def host_is_loopback(host):
    normalized = str(host or "").strip().lower()
    if normalized in {"localhost", "ip6-localhost"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def build_preflight(env_file):
    load_dotenv(env_file, override=True)
    instance_id = normalize_instance_id(os.getenv("RANGE_GRID_INSTANCE_ID"))
    asset_id = str(os.getenv("SIGNAL_ASSET_ID") or "").strip().upper()
    kraken_pair = str(os.getenv("KRAKEN_PAIR") or "").strip()
    strategy_directory = os.getenv(
        "RANGE_GRID_STRATEGY_DIRECTORY",
        str(Path(__file__).resolve().parent),
    )
    strategy_file = (
        os.getenv("RANGE_GRID_STRATEGY_PROFILE")
        or os.getenv("STRATEGY_PROFILE")
        or "range_grid_strategy_default.json"
    )
    errors = []
    strategy_path = None
    strategy = {}
    try:
        strategy_path, strategy = load_strategy(
            strategy_directory,
            strategy_file,
        )
    except (OSError, ValueError, TypeError) as exc:
        errors.append(f"strategy load failed: {exc}")

    errors.extend(validate_strategy_config(strategy))
    paper = bool_value(strategy.get("paper_trading_enabled"), False)
    tracker_symbol = (
        os.getenv("RANGE_GRID_ORDER_TRACKER_SYMBOL")
        or os.getenv("ORDER_TRACKER_SYMBOL")
        or strategy.get("order_tracker_symbol")
        or kraken_pair
    )
    errors.extend(validate_instance_configuration(
        instance_id=instance_id,
        asset_id=asset_id,
        kraken_pair=kraken_pair,
        strategy=strategy,
        order_tracker_symbol=tracker_symbol,
        paper_trading_enabled=paper,
        live_enabled=bool_value(os.getenv("RANGE_GRID_LIVE_ENABLED"), False),
        live_confirmation=os.getenv("RANGE_GRID_LIVE_CONFIRMATION", ""),
    ))
    try:
        capital_allocation = parse_optional_positive_float(
            os.getenv("RANGE_GRID_CAPITAL_ALLOCATION_USD"),
            "RANGE_GRID_CAPITAL_ALLOCATION_USD",
        )
    except ValueError as exc:
        errors.append(str(exc))
        capital_allocation = None
    if asset_id != "BTC" and capital_allocation is None:
        errors.append(
            "RANGE_GRID_CAPITAL_ALLOCATION_USD is required for non-BTC instances"
        )
    if not os.getenv("KRAKEN_API_KEY") or not os.getenv("KRAKEN_API_SECRET"):
        errors.append(
            "KRAKEN_API_KEY and KRAKEN_API_SECRET are required for a bot instance"
        )
    try:
        quote_cash_reserve = float(
            os.getenv("RANGE_GRID_QUOTE_CASH_RESERVE_USD", "0")
        )
        if quote_cash_reserve < 0:
            raise ValueError
    except ValueError:
        errors.append("RANGE_GRID_QUOTE_CASH_RESERVE_USD must be >= 0")
        quote_cash_reserve = 0.0

    control_host = os.getenv("RANGE_GRID_CONTROL_HOST", "127.0.0.1")
    control_token = os.getenv("RANGE_GRID_CONTROL_TOKEN", "").strip()
    if not host_is_loopback(control_host) and (
        len(control_token) < 24
        or control_token.lower().startswith("replace-with-")
    ):
        errors.append(
            "a unique RANGE_GRID_CONTROL_TOKEN of at least 24 characters is "
            "required when RANGE_GRID_CONTROL_HOST is not loopback"
        )

    runtime_root = os.getenv("RANGE_GRID_INSTANCE_RUNTIME_ROOT", "instances")
    web_root = os.getenv("RANGE_GRID_INSTANCE_WEB_ROOT", "/var/www/html/bot")
    paths = {
        "state": os.getenv("RANGE_GRID_STATE_FILE")
        or instance_runtime_path(instance_id, "last_state.json", runtime_root),
        "trade_log": os.getenv("RANGE_GRID_TRADE_LOG_FILE")
        or instance_runtime_path(instance_id, "trade_log.jsonl", runtime_root),
        "control": os.getenv("RANGE_GRID_CONTROL_FILE")
        or instance_runtime_path(
            instance_id,
            "range_grid_control_state.json",
            runtime_root,
        ),
        "activity_log": os.getenv("RANGE_GRID_ACTIVITY_LOG_FILE")
        or instance_web_path(
            instance_id,
            "range_grid_activity.jsonl",
            legacy_path="/var/www/html/bot/range_grid_activity.jsonl",
            web_root=web_root,
        ),
    }
    return {
        "ok": not errors,
        "env_file": str(Path(env_file).expanduser().resolve()),
        "instance_id": instance_id or None,
        "asset_id": asset_id or None,
        "kraken_pair": kraken_pair or None,
        "strategy_file": str(strategy_path) if strategy_path else strategy_file,
        "paper_trading_enabled": paper,
        "live_enabled": bool_value(os.getenv("RANGE_GRID_LIVE_ENABLED"), False),
        "capital_allocation_usd": capital_allocation,
        "quote_cash_reserve_usd": quote_cash_reserve,
        "control_port": int(os.getenv("RANGE_GRID_CONTROL_PORT", "8787")),
        "control_host": control_host,
        "order_tracker_symbol": tracker_symbol,
        "paths": paths,
        "errors": errors,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", required=True)
    args = parser.parse_args(argv)
    result = build_preflight(args.env_file)
    print(json.dumps(result, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
