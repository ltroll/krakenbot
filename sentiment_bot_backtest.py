#!/usr/bin/env python3
"""Offline replay for sentiment-bot snapshots."""

import argparse
import json
import os
import statistics
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from risk_context import derive_risk_context

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args, **_kwargs):
        return False


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

DEFAULT_SNAPSHOT_FILE = os.getenv(
    "SENTIMENT_BACKTEST_SNAPSHOT_FILE",
    "sentiment_backtest_snapshot_log.jsonl",
)
DEFAULT_OUTPUT_FILE = os.getenv(
    "SENTIMENT_BOT_BACKTEST_OUTPUT_FILE",
    "/var/www/html/bot/sentiment_bot_local_backtest.json",
)
HTTP_TIMEOUT_SECONDS = float(os.getenv("SENTIMENT_BOT_BACKTEST_HTTP_TIMEOUT_SECONDS", "10"))


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Replay sentiment-bot snapshot logs offline.",
    )
    parser.add_argument(
        "snapshot_files",
        nargs="*",
        default=[DEFAULT_SNAPSHOT_FILE],
        help="Snapshot JSONL files or URLs. Multiple files are merged by timestamp.",
    )
    parser.add_argument("--hours", type=float, default=24.0)
    parser.add_argument("--output", default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--recent-limit", type=int, default=25)
    parser.add_argument("--starting-usd", type=float, default=1000.0)
    parser.add_argument("--entry-wait-hours", type=float, default=4.0)
    parser.add_argument("--max-hold-hours", type=float, default=24.0)
    parser.add_argument("--stop-loss-pct", type=float, default=0.5)
    parser.add_argument(
        "--variant",
        action="append",
        choices=("current", "target_limit_only", "market_disabled", "strict_high_guard"),
        help="Variant to run. May be repeated. Default: all.",
    )
    parser.add_argument(
        "--strict-high-guard-pct",
        type=float,
        default=0.005,
        help="High-price guard used by strict_high_guard variant.",
    )
    parser.add_argument("--stdout", action="store_true")
    return parser.parse_args(argv)


def now_utc():
    return datetime.now(timezone.utc)


def parse_iso8601(value):
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except Exception:
        return None


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default


def parse_bool(value):
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def is_url(value):
    return urlparse(str(value or "")).scheme in ("http", "https")


def load_jsonl(path):
    rows = []
    try:
        if is_url(path):
            with urllib.request.urlopen(path, timeout=HTTP_TIMEOUT_SECONDS) as response:
                lines = response.read().decode("utf-8").splitlines()
        else:
            if not os.path.exists(path):
                return []
            with open(path, encoding="utf-8") as f:
                lines = list(f)
    except (OSError, urllib.error.URLError):
        return []

    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            rows.append(json.loads(text))
        except json.JSONDecodeError:
            continue
    return rows


def snapshot_time(snapshot):
    return parse_iso8601(snapshot.get("captured_at"))


def snapshot_price(snapshot):
    ticker = snapshot.get("ticker") if isinstance(snapshot.get("ticker"), dict) else {}
    price = safe_float(ticker.get("last_price"))
    if price is not None:
        return price
    signal = signal_payload(snapshot)
    return safe_float(signal.get("asset_price")) or safe_float(signal.get("btc_price"))


def signal_payload(snapshot):
    signal = snapshot.get("signal")
    if not isinstance(signal, dict):
        return {}
    payload = signal.get("payload")
    return payload if isinstance(payload, dict) else {}


def strategy_payload(snapshot):
    profile = snapshot.get("strategy_profile")
    if not isinstance(profile, dict):
        return {}
    payload = profile.get("payload")
    return payload if isinstance(payload, dict) else {}


def state_payload(snapshot):
    state = snapshot.get("state")
    return state if isinstance(state, dict) else {}


def state_summary(snapshot):
    state = state_payload(snapshot)
    summary = state.get("summary")
    return summary if isinstance(summary, dict) else {}


def open_order_count(snapshot, side):
    state = state_payload(snapshot)
    key = "open_buy_orders" if side == "buy" else "open_sell_orders"
    orders = state.get(key)
    return len(orders) if isinstance(orders, list) else 0


def current_inventory_usd(snapshot, price):
    state = state_payload(snapshot)
    total = 0.0
    for key in ("open_buy_orders", "open_sell_orders"):
        orders = state.get(key)
        if not isinstance(orders, list):
            continue
        for order in orders:
            if not isinstance(order, dict):
                continue
            order_price = (
                safe_float(order.get("buy_price"))
                or safe_float(order.get("price"))
                or price
            )
            total += order_price * (safe_float(order.get("volume")) or 0.0)
    return total


def config_bool(config, name, default):
    return parse_bool(config.get(name, default))


def config_float(config, name, default):
    value = safe_float(config.get(name))
    return default if value is None else value


def config_int(config, name, default):
    return safe_int(config.get(name), default)


def freshness_contract(signal):
    freshness = signal.get("freshness")
    return freshness if isinstance(freshness, dict) else {}


def freshness_minutes(signal, key):
    value = safe_float(freshness_contract(signal).get(key))
    return value


def signal_age_minutes(signal, at_time):
    freshness = freshness_contract(signal)
    processed_at = parse_iso8601(
        signal.get("processed_at") or freshness.get("processed_at")
    )
    if processed_at is None:
        return None
    return (at_time - processed_at).total_seconds() / 60.0


def signal_freshness_state(signal, at_time):
    age = signal_age_minutes(signal, at_time)
    if age is None:
        return "unknown"
    stale_after = freshness_minutes(signal, "stale_after_minutes")
    warn_after = freshness_minutes(signal, "warn_after_minutes")
    fresh_for = freshness_minutes(signal, "fresh_for_minutes")
    if stale_after is not None and age > stale_after:
        return "stale"
    if warn_after is not None and age > warn_after:
        return "warn"
    if fresh_for is not None and age <= fresh_for:
        return "fresh"
    return "fresh"


def weather_report_bot_decides(signal):
    risk_context = signal.get("risk_context")
    if not isinstance(risk_context, dict):
        return False
    weather = risk_context.get("weather_report")
    if not isinstance(weather, dict):
        return False
    return (
        weather.get("bot_decision_authority") == "bot"
        or weather.get("trade_permission") == "bot_decides"
    )


def derived_risk_view(signal, at_time):
    return derive_risk_context(
        signal.get("risk_context"),
        fallback_processed_at=signal.get("processed_at"),
        stale=signal_freshness_state(signal, at_time) == "stale",
        now=at_time,
    )


def effective_risk_multiplier(signal, config):
    if not config_bool(config, "use_risk_multiplier", True):
        return 1.0
    multiplier = (
        safe_float(signal.get("smoothed_risk_multiplier"))
        or safe_float(signal.get("risk_multiplier"))
    )
    if multiplier is None:
        return 1.0
    return clamp(
        multiplier,
        config_float(config, "min_risk_multiplier", 0.25),
        config_float(config, "max_risk_multiplier", 1.25),
    )


def weighted_signal(signal, config):
    raw_signal = safe_float(signal.get("execution_signal")) or 0.0
    smoothed = safe_float(signal.get("smoothed_signal"))
    if smoothed is None:
        smoothed = raw_signal
    confidence = safe_float(signal.get("confidence")) or 0.0
    if config_bool(config, "confidence_weighting", True):
        return smoothed * confidence, smoothed, confidence, raw_signal
    return smoothed, smoothed, confidence, raw_signal


def dynamic_target_profit_pct(signal, config, weighted):
    target_profit = config_float(config, "target_profit_pct", 0.006)
    if not config_bool(config, "dynamic_profit_targets", False):
        return target_profit
    min_target = config_float(config, "min_target_profit_pct", target_profit)
    base_target = config_float(config, "base_target_profit_pct", target_profit)
    max_target = config_float(config, "max_target_profit_pct", target_profit)
    target = base_target
    price_regime = signal.get("price_regime") if isinstance(signal.get("price_regime"), dict) else {}
    range_position = safe_float(price_regime.get("range_position_24h"))
    if range_position is not None:
        if range_position <= 0.10:
            target += 0.002
        elif range_position <= 0.20:
            target += 0.001
        elif range_position >= 0.70:
            target -= 0.002
    opportunity = safe_float(signal.get("mean_reversion_opportunity"))
    mean_reversion_threshold = config_float(config, "mean_reversion_buy_threshold", 0.35)
    if opportunity is not None:
        if opportunity >= 0.55:
            target += 0.002
        elif opportunity >= mean_reversion_threshold:
            target += 0.001
    flow = safe_float(signal.get("flow_pressure"))
    if flow is not None:
        if flow >= 0.40:
            target += 0.0015
        elif flow <= 0:
            target -= 0.0015
    sentiment_threshold = config_float(config, "sentiment_buy_threshold", 0.03)
    if weighted >= sentiment_threshold + 0.04:
        target += 0.002
    elif weighted < 0:
        target -= 0.001
    volatility = safe_float(price_regime.get("realized_volatility_24h_pct"))
    if volatility is not None:
        if volatility >= config_float(config, "dynamic_profit_high_volatility_pct", 0.06):
            target += 0.0015
        elif volatility <= config_float(config, "dynamic_profit_low_volatility_pct", 0.025):
            target -= 0.001
    return clamp(target, min_target, max_target)


def mean_reversion_setup_allowed(signal, config):
    price_regime = signal.get("price_regime") if isinstance(signal.get("price_regime"), dict) else {}
    kraken_flow = signal.get("kraken_flow") if isinstance(signal.get("kraken_flow"), dict) else {}
    opportunity = (
        safe_float(signal.get("mean_reversion_opportunity"))
        or safe_float(price_regime.get("mean_reversion_opportunity"))
    )
    range_position = safe_float(price_regime.get("range_position_24h"))
    flow_pressure = safe_float(signal.get("flow_pressure"))
    if flow_pressure is None:
        flow_pressure = safe_float(kraken_flow.get("aggression_score"))
    if opportunity is None or range_position is None or flow_pressure is None:
        return False
    return (
        opportunity >= config_float(config, "mean_reversion_buy_threshold", 0.35)
        and range_position <= config_float(config, "mean_reversion_range_position_max", 0.15)
        and flow_pressure >= config_float(config, "mean_reversion_flow_pressure_min", 0.0)
    )


def target_limit_orders(signal, config, current_price, total_trade_value, max_buy_price=None):
    if not config_bool(config, "enable_target_limit_buys", True):
        return []
    targets = []
    for target in signal.get("target_prices", []):
        if not isinstance(target, dict):
            continue
        buy_price = safe_float(target.get("buy_price"))
        if buy_price is None or buy_price <= 0:
            continue
        max_price = current_price * (1 + config_float(config, "target_limit_max_premium_pct", 0.0005))
        if buy_price > max_price:
            continue
        if max_buy_price is not None and buy_price >= max_buy_price:
            continue
        allocation = safe_float(target.get("sell_pct"))
        if allocation is None:
            allocation = safe_float(target.get("allocation_pct"))
        if allocation is None or allocation <= 0:
            allocation = 1.0
        targets.append({"buy_price": buy_price, "allocation": allocation})
    if not targets:
        return []
    targets = targets[:config_int(config, "max_target_limit_orders_per_cycle", 2)]
    allocation_sum = sum(item["allocation"] for item in targets)
    if allocation_sum <= 0:
        return []
    return [
        {
            "buy_price": item["buy_price"],
            "trade_value": total_trade_value * item["allocation"] / allocation_sum,
            "allocation_pct": item["allocation"] / allocation_sum,
        }
        for item in targets
    ]


def signal_gate_failure(signal, config, at_time):
    if not config_bool(config, "use_signal_status_gates", True):
        return None
    signal_status = signal.get("signal_status")
    freshness_state = signal_freshness_state(signal, at_time)
    contract = freshness_contract(signal)
    if (
        signal_status
        and signal_status != "fresh"
        and not contract
        and not weather_report_bot_decides(signal)
    ):
        return "signal_not_fresh"
    if freshness_state == "stale":
        return "signal_too_old"
    if (
        config_bool(config, "require_bot_action_allowed", True)
        and signal.get("bot_action_allowed") is False
        and not weather_report_bot_decides(signal)
    ):
        return "bot_action_not_allowed"
    max_age = config_float(config, "max_signal_age_minutes", 30)
    age = signal_age_minutes(signal, at_time)
    if not contract and max_age > 0 and age is not None and age > max_age:
        return "signal_too_old"
    return None


def variant_config(config, variant, strict_high_guard_pct):
    adjusted = dict(config)
    if variant in ("target_limit_only", "market_disabled"):
        adjusted["disable_market_buys_for_backtest"] = True
    if variant == "strict_high_guard":
        adjusted["high_price_buy_block_pct"] = strict_high_guard_pct
    return adjusted


def effective_market_location_range_position(risk_view, fallback_range_position):
    value = risk_view.get("weather_market_range_position")
    if value is not None:
        return value
    return fallback_range_position


def market_location_near_high(risk_view, config, fallback_range_position):
    if not config_bool(config, "high_entry_quality_enabled", True):
        return False
    range_position = effective_market_location_range_position(
        risk_view,
        fallback_range_position,
    )
    distance_to_high = risk_view.get("weather_market_distance_to_recent_high_pct")
    range_zone = str(risk_view.get("weather_market_range_zone") or "").lower()
    if range_position is not None and range_position >= config_float(config, "high_entry_range_position_min", 0.90):
        return True
    if distance_to_high is not None and distance_to_high <= config_float(config, "high_entry_distance_to_high_pct", 0.75):
        return True
    return range_zone in ("upper_range", "near_high", "range_high", "breakout_zone")


def high_entry_quality_check(risk_view, config, fallback_range_position):
    if not market_location_near_high(risk_view, config, fallback_range_position):
        return {
            "near_high": False,
            "allowed": True,
            "reason": None,
            "size_multiplier": 1.0,
        }
    if risk_view.get("weather_emergency_bell"):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "weather_emergency_bell",
            "size_multiplier": 0.0,
        }
    alert_level = str(risk_view.get("weather_alert_level") or "normal").lower()
    if alert_level in ("danger", "storm", "risk_off"):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_alert_level",
            "size_multiplier": 0.0,
        }
    market_risk = risk_view.get("risk_context_market_risk_score")
    if market_risk is not None and market_risk > config_float(config, "high_entry_max_market_risk_score", 0.35):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_market_risk_high",
            "size_multiplier": 0.0,
        }
    condition = str(risk_view.get("weather_condition") or "").lower()
    opportunity_tags = {
        str(tag).lower()
        for tag in (risk_view.get("weather_opportunity_tags") or [])
    }
    confirmation_score = max(
        risk_view.get("risk_context_breakout_score") or 0.0,
        risk_view.get("risk_context_rebound_score") or 0.0,
        risk_view.get("risk_context_buy_aggression_score") or 0.0,
    )
    breakout_weather = (
        condition == "breakout_tailwind"
        or "breakout_tailwind" in opportunity_tags
    )
    if (
        not breakout_weather
        and confirmation_score < config_float(config, "high_entry_min_confirmation_score", 0.65)
    ):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_confirmation_low",
            "size_multiplier": 0.0,
        }
    price_return_4h = risk_view.get("weather_market_price_return_4h_pct")
    if (
        config_bool(config, "high_entry_require_positive_4h_return", True)
        and price_return_4h is not None
        and price_return_4h <= 0
    ):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_4h_momentum_negative",
            "size_multiplier": 0.0,
        }
    return {
        "near_high": True,
        "allowed": True,
        "reason": "high_entry_breakout_confirmed",
        "size_multiplier": clamp(config_float(config, "high_entry_size_multiplier", 0.50), 0.0, 1.0),
    }


def market_entry_quality_check(risk_view, config, fallback_range_position):
    if not config_bool(config, "market_entry_quality_enabled", True):
        return {"allowed": True, "reason": None, "size_multiplier": 1.0}
    if risk_view.get("weather_emergency_bell"):
        return {
            "allowed": False,
            "reason": "weather_emergency_bell",
            "size_multiplier": 0.0,
        }

    phase = str(risk_view.get("weather_opportunity_phase") or "").strip()
    dip_phases = {
        item.strip()
        for item in str(config.get("market_entry_dip_phases", "dip_leveling_entry,early_rebound")).split(",")
        if item.strip()
    }
    avoid_phases = {
        item.strip()
        for item in str(config.get("market_entry_avoid_phases", "peak_exhaustion_watch,pullback_wait,storm_avoid")).split(",")
        if item.strip()
    }
    entry_score = risk_view.get("weather_entry_opportunity_score")
    rebound_confirmation = risk_view.get("weather_rebound_confirmation_score")
    if phase in avoid_phases:
        return {
            "allowed": False,
            "reason": f"market_entry_{phase}",
            "size_multiplier": 0.0,
        }
    if phase in dip_phases:
        entry_ok = (
            entry_score is not None
            and entry_score >= config_float(config, "market_entry_min_opportunity_score", 0.67)
        )
        rebound_ok = (
            rebound_confirmation is not None
            and rebound_confirmation >= config_float(config, "market_entry_min_rebound_confirmation_score", 0.55)
        )
        if entry_ok and rebound_ok:
            return {
                "allowed": True,
                "reason": f"market_entry_{phase}",
                "size_multiplier": clamp(config_float(config, "market_entry_dip_size_multiplier", 0.50), 0.0, 1.0),
            }

    range_position = effective_market_location_range_position(
        risk_view,
        fallback_range_position,
    )
    if (
        range_position is not None
        and range_position <= config_float(config, "market_entry_free_range_position_max", 0.25)
    ):
        return {"allowed": True, "reason": "market_entry_low_range", "size_multiplier": 1.0}
    if (
        range_position is not None
        and range_position < config_float(config, "market_entry_require_confirmation_above_range", 0.25)
    ):
        return {"allowed": True, "reason": "market_entry_lower_range", "size_multiplier": 1.0}

    buy_score = risk_view.get("risk_adjusted_buy_score")
    rebound_score = risk_view.get("risk_context_rebound_score")
    bottoming_score = risk_view.get("risk_context_bottoming_score")
    confirmation_score = max(
        buy_score or 0.0,
        rebound_score or 0.0,
        bottoming_score or 0.0,
    )
    min_confirmation = max(
        config_float(config, "market_entry_min_buy_score", 0.50),
        min(
            config_float(config, "market_entry_min_rebound_score", 0.55),
            config_float(config, "market_entry_min_bottoming_score", 0.55),
        ),
    )
    if (
        (buy_score is None or buy_score < config_float(config, "market_entry_min_buy_score", 0.50))
        and (rebound_score is None or rebound_score < config_float(config, "market_entry_min_rebound_score", 0.55))
        and (bottoming_score is None or bottoming_score < config_float(config, "market_entry_min_bottoming_score", 0.55))
    ):
        return {
            "allowed": False,
            "reason": "market_entry_confirmation_low",
            "size_multiplier": 0.0,
            "confirmation_score": confirmation_score,
            "market_entry_min_confirmation": min_confirmation,
        }

    price_return_4h = risk_view.get("weather_market_price_return_4h_pct")
    if (
        config_bool(config, "market_entry_require_positive_4h_return", False)
        and price_return_4h is not None
        and price_return_4h <= 0
    ):
        return {
            "allowed": False,
            "reason": "market_entry_4h_momentum_negative",
            "size_multiplier": 0.0,
            "confirmation_score": confirmation_score,
            "market_entry_min_confirmation": min_confirmation,
        }

    return {
        "allowed": True,
        "reason": "market_entry_confirmed",
        "size_multiplier": 1.0,
        "confirmation_score": confirmation_score,
        "market_entry_min_confirmation": min_confirmation,
    }


def evaluate_snapshot(snapshot, variant, strict_high_guard_pct, account_usd):
    at_time = snapshot_time(snapshot)
    price = snapshot_price(snapshot)
    signal = signal_payload(snapshot)
    config = variant_config(strategy_payload(snapshot), variant, strict_high_guard_pct)
    if at_time is None:
        return hold("missing_timestamp", snapshot, price, signal, config)
    if price is None:
        return hold("missing_price", snapshot, price, signal, config)
    if not signal:
        return hold("missing_signal", snapshot, price, signal, config)

    weighted, smoothed, confidence, raw_signal = weighted_signal(signal, config)
    risk_view = derived_risk_view(signal, at_time)
    risk_usable = (
        config_bool(config, "use_risk_context_policy", True)
        and risk_view.get("risk_context_available")
        and not risk_view.get("risk_context_stale")
    )
    target_profit_pct = dynamic_target_profit_pct(signal, config, weighted)
    if risk_usable and config_bool(config, "risk_context_target_profit_enabled", True):
        target_profit_pct = clamp(
            target_profit_pct * risk_view.get("suggested_take_profit_multiplier", 1.0),
            config_float(config, "min_target_profit_pct", target_profit_pct),
            config_float(config, "max_target_profit_pct", target_profit_pct),
        )

    gate_failure = signal_gate_failure(signal, config, at_time)
    if gate_failure:
        return hold(gate_failure, snapshot, price, signal, config, weighted, risk_view)
    if risk_usable and risk_view.get("weather_emergency_bell"):
        return hold("weather_emergency_bell", snapshot, price, signal, config, weighted, risk_view)

    target_candidates = target_limit_orders(signal, config, price, 1.0)
    allow_target_limit = mean_reversion_setup_allowed(signal, config) and bool(target_candidates)
    market_disabled = config_bool(config, "disable_market_buys_for_backtest", False)
    allow_market = not allow_target_limit and not market_disabled

    if not allow_target_limit and market_disabled:
        return hold("market_buy_disabled_no_target_limit", snapshot, price, signal, config, weighted, risk_view)

    if allow_market and weighted < config_float(config, "sentiment_buy_threshold", 0.03):
        return hold("local_signal_below_buy_threshold", snapshot, price, signal, config, weighted, risk_view)

    price_regime = signal.get("price_regime") if isinstance(signal.get("price_regime"), dict) else {}
    range_position = safe_float(price_regime.get("range_position_24h"))
    high_entry = high_entry_quality_check(risk_view, config, range_position)
    if allow_market and high_entry["near_high"] and not high_entry["allowed"]:
        return hold(high_entry["reason"], snapshot, price, signal, config, weighted, risk_view, high_entry)

    market_entry = market_entry_quality_check(risk_view, config, range_position)
    if allow_market and not market_entry["allowed"]:
        return hold(market_entry["reason"], snapshot, price, signal, config, weighted, risk_view, high_entry, market_entry)

    high_guard_pct = config_float(config, "high_price_buy_block_pct", 0.0005)
    price_high = safe_float(price_regime.get("price_high_24h"))
    if price_high is None:
        price_high = risk_view.get("weather_market_range_high")
    if allow_market and price_high is not None and high_guard_pct >= 0:
        max_high_buy_price = price_high * (1 - high_guard_pct)
        if price >= max_high_buy_price:
            if not (high_entry["near_high"] and high_entry["allowed"]):
                return hold("price_near_regime_high", snapshot, price, signal, config, weighted, risk_view, high_entry)

    if open_order_count(snapshot, "buy") >= config_int(config, "max_open_buy_orders", 2):
        return hold("max_open_buy_orders", snapshot, price, signal, config, weighted, risk_view)
    if open_order_count(snapshot, "sell") >= config_int(config, "max_open_sell_orders", 1):
        return hold("max_open_sell_orders", snapshot, price, signal, config, weighted, risk_view)
    if current_inventory_usd(snapshot, price) >= config_float(config, "max_inventory_usd", 250):
        return hold("max_inventory_usd", snapshot, price, signal, config, weighted, risk_view)

    trade_value = config_float(config, "position_size_pct", 0.10) * account_usd
    max_trade_usd = config_float(config, "max_trade_usd", 0)
    if max_trade_usd > 0:
        trade_value = min(trade_value, max_trade_usd)
    trade_value *= effective_risk_multiplier(signal, config)
    if risk_usable and config_bool(config, "risk_context_position_size_enabled", True):
        trade_value *= clamp(
            risk_view.get("suggested_position_size_multiplier", 1.0),
            0.0,
            config_float(config, "risk_context_max_position_size_multiplier", 1.25),
        )
    if allow_market and high_entry["near_high"] and high_entry["allowed"]:
        trade_value *= high_entry["size_multiplier"]
    if allow_market and market_entry.get("allowed"):
        trade_value *= market_entry.get("size_multiplier", 1.0)
    trade_value *= max(0.0, 1 - config_float(config, "execution_buffer_pct", 0.0025))
    if trade_value < config_float(config, "min_trade_usd", 30):
        return hold("small_trade", snapshot, price, signal, config, weighted, risk_view)

    if allow_target_limit:
        orders = target_limit_orders(signal, config, price, trade_value)
        if not orders:
            return hold("no_target_limit_orders_placed", snapshot, price, signal, config, weighted, risk_view)
        order = orders[0]
        return buy(
            "mean_reversion_target_limit_buy",
            snapshot,
            signal,
            config,
            price=order["buy_price"],
            decision_price=price,
            trade_value=order["trade_value"],
            target_profit_pct=target_profit_pct,
            weighted=weighted,
            risk_view=risk_view,
            high_entry=high_entry,
            market_entry=market_entry,
            fill_mode="limit",
        )

    return buy(
        "sentiment_buy",
        snapshot,
        signal,
        config,
        price=price,
        decision_price=price,
        trade_value=trade_value,
        target_profit_pct=target_profit_pct,
        weighted=weighted,
        risk_view=risk_view,
        high_entry=high_entry,
        market_entry=market_entry,
        fill_mode="market",
    )


def base_event(status, reason, snapshot, price, signal, config, weighted=None, risk_view=None, high_entry=None, market_entry=None):
    at_time = snapshot_time(snapshot)
    risk_view = risk_view or {}
    price_regime = signal.get("price_regime") if isinstance(signal.get("price_regime"), dict) else {}
    freshness = freshness_contract(signal)
    return {
        "status": status,
        "reason": reason,
        "captured_at": snapshot.get("captured_at"),
        "price": price,
        "weighted_signal": weighted,
        "execution_signal": safe_float(signal.get("execution_signal")),
        "confidence": safe_float(signal.get("confidence")),
        "signal_age_minutes": (
            round(signal_age_minutes(signal, at_time), 4)
            if at_time and signal
            else None
        ),
        "signal_status": signal.get("signal_status"),
        "signal_freshness_state": signal_freshness_state(signal, at_time) if at_time else None,
        "freshness_fresh_for_minutes": freshness_minutes(signal, "fresh_for_minutes"),
        "freshness_warn_after_minutes": freshness_minutes(signal, "warn_after_minutes"),
        "freshness_stale_after_minutes": freshness_minutes(signal, "stale_after_minutes"),
        "freshness_processed_at": freshness.get("processed_at"),
        "range_position_24h": safe_float(price_regime.get("range_position_24h")),
        "weather_market_range_position": risk_view.get("weather_market_range_position"),
        "weather_market_range_zone": risk_view.get("weather_market_range_zone"),
        "weather_market_distance_to_recent_high_pct": (
            risk_view.get("weather_market_distance_to_recent_high_pct")
        ),
        "weather_market_price_return_4h_pct": (
            risk_view.get("weather_market_price_return_4h_pct")
        ),
        "weather_opportunity_phase": risk_view.get("weather_opportunity_phase"),
        "weather_opportunity_bot_hint": risk_view.get("weather_opportunity_bot_hint"),
        "weather_entry_opportunity_score": risk_view.get("weather_entry_opportunity_score"),
        "weather_rebound_confirmation_score": (
            risk_view.get("weather_rebound_confirmation_score")
        ),
        "weather_exit_pressure_score": risk_view.get("weather_exit_pressure_score"),
        "weather_hold_through_score": risk_view.get("weather_hold_through_score"),
        "weather_pattern_tags": risk_view.get("weather_pattern_tags"),
        "mean_reversion_opportunity": safe_float(signal.get("mean_reversion_opportunity")),
        "weather_condition": risk_view.get("weather_condition"),
        "weather_alert_level": risk_view.get("weather_alert_level"),
        "weather_emergency_bell": risk_view.get("weather_emergency_bell"),
        "weather_trade_permission": risk_view.get("weather_trade_permission"),
        "risk_adjusted_buy_score": risk_view.get("risk_adjusted_buy_score"),
        "suggested_position_size_multiplier": risk_view.get("suggested_position_size_multiplier"),
        "suggested_take_profit_multiplier": risk_view.get("suggested_take_profit_multiplier"),
        "high_entry_near_high": (high_entry or {}).get("near_high"),
        "high_entry_quality_reason": (high_entry or {}).get("reason"),
        "high_entry_size_multiplier": (high_entry or {}).get("size_multiplier"),
        "market_entry_quality_reason": (market_entry or {}).get("reason"),
        "market_entry_size_multiplier": (market_entry or {}).get("size_multiplier"),
    }


def hold(reason, snapshot, price, signal, config, weighted=None, risk_view=None, high_entry=None, market_entry=None):
    return base_event("hold", reason, snapshot, price, signal, config, weighted, risk_view, high_entry, market_entry)


def buy(reason, snapshot, signal, config, *, price, decision_price, trade_value, target_profit_pct, weighted, risk_view, high_entry, market_entry, fill_mode):
    event = base_event("buy", reason, snapshot, decision_price, signal, config, weighted, risk_view, high_entry, market_entry)
    event.update({
        "entry_price": price,
        "decision_price": decision_price,
        "trade_value": trade_value,
        "target_profit_pct": target_profit_pct,
        "round_trip_fee_pct": config_float(config, "round_trip_fee_pct", 0.0065),
        "fill_mode": fill_mode,
    })
    return event


def simulate_trade(event, snapshots, args):
    entry_time = parse_iso8601(event.get("captured_at"))
    entry_price = safe_float(event.get("entry_price"))
    if entry_time is None or entry_price is None or entry_price <= 0:
        return None

    fill_time = entry_time
    if event.get("fill_mode") == "limit":
        fill_deadline = entry_time + timedelta(hours=args.entry_wait_hours)
        fill_time = None
        for future in snapshots:
            future_time = snapshot_time(future)
            if future_time is None or future_time < entry_time or future_time > fill_deadline:
                continue
            future_price = snapshot_price(future)
            if future_price is not None and future_price <= entry_price:
                fill_time = future_time
                break
        if fill_time is None:
            return {"filled": False, "exit_reason": "not_filled"}

    target_profit_pct = safe_float(event.get("target_profit_pct")) or 0.0
    stop_loss_pct = args.stop_loss_pct / 100.0
    target_price = entry_price * (1 + target_profit_pct)
    stop_price = entry_price * (1 - stop_loss_pct)
    hold_deadline = fill_time + timedelta(hours=args.max_hold_hours)
    max_runup_pct = 0.0
    max_drawdown_pct = 0.0
    end_price = entry_price
    exit_time = None
    exit_reason = "timeout"

    for future in snapshots:
        future_time = snapshot_time(future)
        if future_time is None or future_time <= fill_time:
            continue
        if future_time > hold_deadline:
            break
        future_price = snapshot_price(future)
        if future_price is None:
            continue
        end_price = future_price
        ret_pct = ((future_price - entry_price) / entry_price) * 100.0
        max_runup_pct = max(max_runup_pct, ret_pct)
        max_drawdown_pct = min(max_drawdown_pct, ret_pct)
        if future_price >= target_price:
            exit_time = future_time
            exit_reason = "take_profit"
            break
        if future_price <= stop_price:
            exit_time = future_time
            exit_reason = "stop_loss"
            break

    if exit_time is None:
        exit_time = hold_deadline
    net_return_pct = ((end_price - entry_price) / entry_price) * 100.0
    trade_value = safe_float(event.get("trade_value")) or 0.0
    return {
        "filled": True,
        "filled_at": fill_time.isoformat(),
        "exit_time": exit_time.isoformat(),
        "exit_reason": exit_reason,
        "exit_price": round(end_price, 8),
        "net_return_pct": round(net_return_pct, 6),
        "pnl_usd": round(trade_value * net_return_pct / 100.0, 6),
        "max_runup_pct": round(max_runup_pct, 6),
        "max_drawdown_pct": round(max_drawdown_pct, 6),
    }


def summarize_events(events):
    buys = [event for event in events if event["status"] == "buy"]
    filled = [event for event in buys if (event.get("simulation") or {}).get("filled")]
    returns = [
        event["simulation"]["net_return_pct"]
        for event in filled
        if event.get("simulation") and event["simulation"].get("net_return_pct") is not None
    ]
    pnl = [
        event["simulation"]["pnl_usd"]
        for event in filled
        if event.get("simulation") and event["simulation"].get("pnl_usd") is not None
    ]
    exit_reasons = Counter(
        (event.get("simulation") or {}).get("exit_reason")
        for event in filled
    )
    hold_reasons = Counter(
        event["reason"]
        for event in events
        if event["status"] == "hold"
    )
    buy_reasons = Counter(event["reason"] for event in buys)
    return {
        "snapshots": len(events),
        "buy_decisions": len(buys),
        "filled_trades": len(filled),
        "not_filled": sum(
            1 for event in buys
            if (event.get("simulation") or {}).get("exit_reason") == "not_filled"
        ),
        "win_rate": (
            round(sum(1 for value in returns if value > 0) / len(returns), 4)
            if returns
            else None
        ),
        "avg_net_return_pct": round(statistics.mean(returns), 6) if returns else None,
        "total_net_return_pct": round(sum(returns), 6) if returns else None,
        "estimated_total_pnl_usd": round(sum(pnl), 6) if pnl else None,
        "take_profit_count": int(exit_reasons.get("take_profit", 0)),
        "stop_loss_count": int(exit_reasons.get("stop_loss", 0)),
        "timeout_count": int(exit_reasons.get("timeout", 0)),
        "hold_reason_counts": dict(hold_reasons.most_common()),
        "buy_reason_counts": dict(buy_reasons.most_common()),
    }


def replay_variant(snapshots, variant, args):
    events = []
    for snapshot in snapshots:
        event = evaluate_snapshot(
            snapshot,
            variant,
            args.strict_high_guard_pct,
            args.starting_usd,
        )
        if event["status"] == "buy":
            event["simulation"] = simulate_trade(event, snapshots, args)
        events.append(event)
    return {
        "summary": summarize_events(events),
        "recent_events": events[-args.recent_limit:],
        "recent_buys": [
            event for event in events if event["status"] == "buy"
        ][-args.recent_limit:],
    }


def load_snapshots(paths, hours):
    rows = []
    for path in expand_snapshot_paths(paths, hours):
        rows.extend(load_jsonl(path))
    rows.sort(key=lambda item: snapshot_time(item) or datetime.min.replace(tzinfo=timezone.utc))
    cutoff = now_utc() - timedelta(hours=hours)
    return [
        row for row in rows
        if snapshot_time(row) is not None and snapshot_time(row) >= cutoff
    ]


def rotated_snapshot_path(base_path, dt):
    root, ext = os.path.splitext(base_path)
    return f"{root}_{dt.strftime('%Y%m%d')}{ext or '.jsonl'}"


def expand_snapshot_paths(paths, hours):
    expanded = []
    start = now_utc() - timedelta(hours=hours)
    end = now_utc()
    for path in paths:
        expanded.append(path)
        if is_url(path):
            # URL existence checks would cost network round trips; include likely
            # rotated names and let load_jsonl silently skip missing ones.
            current = start.date()
            while current <= end.date():
                rotated = rotated_snapshot_path(
                    path,
                    datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc),
                )
                expanded.append(rotated)
                current += timedelta(days=1)
            continue

        if os.path.exists(path):
            continue
        current = start.date()
        while current <= end.date():
            rotated = rotated_snapshot_path(
                path,
                datetime.combine(current, datetime.min.time(), tzinfo=timezone.utc),
            )
            if os.path.exists(rotated):
                expanded.append(rotated)
            current += timedelta(days=1)

    deduped = []
    seen = set()
    for path in expanded:
        if path in seen:
            continue
        seen.add(path)
        deduped.append(path)
    return deduped


def write_output(payload, path):
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
        f.write("\n")


def main(argv=None):
    args = parse_args(argv)
    variants = args.variant or [
        "current",
        "target_limit_only",
        "market_disabled",
        "strict_high_guard",
    ]
    snapshots = load_snapshots(args.snapshot_files, args.hours)
    payload = {
        "schema_version": "sentiment-bot-local-backtest-v1",
        "timestamp": now_utc().isoformat(),
        "window_hours": args.hours,
        "snapshot_files": args.snapshot_files,
        "snapshot_count": len(snapshots),
        "simulation": {
            "entry_wait_hours": args.entry_wait_hours,
            "max_hold_hours": args.max_hold_hours,
            "stop_loss_pct": args.stop_loss_pct,
            "starting_usd": args.starting_usd,
            "scope": "offline_snapshot_replay_no_private_api",
        },
        "variants": {
            variant: replay_variant(snapshots, variant, args)
            for variant in variants
        },
    }
    if args.stdout:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        write_output(payload, args.output)
        print(f"Wrote backtest: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
