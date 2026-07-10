#!/usr/bin/env python3

import csv
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from fee_config import effective_round_trip_fee_pct

from target_quality import (
    evaluate_quality_target,
    match_quality_target,
    normalize_profit_target_pct,
    parse_iso8601,
    unavailable_quality_decision,
)
from signal_normalizer import normalize_signal_payload, selected_signal_asset_id


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)
REPO_DIR = os.path.dirname(os.path.abspath(__file__))


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
SIGNAL_ASSET_ID = selected_signal_asset_id(
    pair=os.getenv("KRAKEN_PAIR", "XXBTZUSD")
)
BACKTEST_OUTPUT_FILE = os.getenv(
    "LLM_TARGET_BACKTEST_OUTPUT_FILE",
    "/var/www/html/bot/llm_target_backtest.json"
)
BACKTEST_ARCHIVE_DIR = os.getenv(
    "LLM_TARGET_BACKTEST_ARCHIVE_DIR",
    "/var/www/html/bot"
)
BACKTEST_WINDOW_HOURS = float(os.getenv("LLM_TARGET_BACKTEST_WINDOW_HOURS", "24"))
BACKTEST_ENTRY_WAIT_HOURS = float(
    os.getenv("LLM_TARGET_BACKTEST_ENTRY_WAIT_HOURS", "4")
)
BACKTEST_MAX_HOLD_HOURS = float(
    os.getenv("LLM_TARGET_BACKTEST_MAX_HOLD_HOURS", "24")
)
BACKTEST_STOP_LOSS_PCT = float(os.getenv("LLM_TARGET_BACKTEST_STOP_LOSS_PCT", "0.5"))
BACKTEST_COOLDOWN_MINUTES = float(
    os.getenv("LLM_TARGET_BACKTEST_COOLDOWN_MINUTES", "60")
)
BACKTEST_FEE_BPS = float(os.getenv("LLM_TARGET_BACKTEST_FEE_BPS", "0.0"))
BACKTEST_RECENT_LIMIT = int(os.getenv("LLM_TARGET_BACKTEST_RECENT_LIMIT", "25"))
BACKTEST_STRATEGY_SET_FILE = os.getenv(
    "LLM_TARGET_BACKTEST_STRATEGY_SET_FILE",
    ""
)
BACKTEST_STRATEGY_COMPARE_CSV_FILE = os.getenv(
    "LLM_TARGET_BACKTEST_STRATEGY_COMPARE_CSV_FILE",
    "llm_target_strategy_comparison.csv"
)
BACKTEST_STRATEGY_RANKED_CSV_FILE = os.getenv(
    "LLM_TARGET_BACKTEST_STRATEGY_RANKED_CSV_FILE",
    "llm_target_strategy_ranked.csv"
)
SNAPSHOT_ROTATE_DAILY = os.getenv(
    "LLM_TARGET_BACKTEST_ROTATE_DAILY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")
BACKTEST_SENTIMENT_DISCOUNT_WATCH_PCT = float(os.getenv(
    "LLM_TARGET_BACKTEST_SENTIMENT_DISCOUNT_WATCH_PCT",
    "0.25"
))
BACKTEST_SENTIMENT_DISCOUNT_BEARISH_PCT = float(os.getenv(
    "LLM_TARGET_BACKTEST_SENTIMENT_DISCOUNT_BEARISH_PCT",
    "0.5"
))
BACKTEST_DERIVE_TARGETS_FROM_QUALITY = os.getenv(
    "LLM_TARGET_BACKTEST_DERIVE_TARGETS_FROM_QUALITY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")


BACKTEST_STRATEGIES = {
    "with_target_quality": {},
    "sentiment_policy_only": {},
    "price_target_only": {},
    "price_target_only_tp_0_8": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.008,
    },
    "price_target_only_tp_1_0": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.01,
    },
    "price_target_only_tp_1_2": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.012,
    },
    "price_target_only_tp_1_2_hold": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.012,
        "exit_mode": "profit_only",
    },
    "price_target_only_tp_1_5": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.015,
    },
    "price_target_only_tp_1_5_hold": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.015,
        "exit_mode": "profit_only",
    },
    "price_target_only_tp_2_0": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.02,
    },
    "price_target_only_tp_2_0_hold": {
        "base_strategy": "price_target_only",
        "profit_target_pct": 0.02,
        "exit_mode": "profit_only",
    },
    "target_quality_only": {
        "base_strategy": "with_target_quality",
        "ignore_sentiment": True,
    },
    "target_quality_only_hold": {
        "base_strategy": "with_target_quality",
        "ignore_sentiment": True,
        "exit_mode": "profit_only",
    },
    "weather_target_quality": {
        "base_strategy": "with_target_quality",
        "ignore_legacy_action_gate": True,
    },
    "weather_target_quality_tp_0_8": {
        "base_strategy": "with_target_quality",
        "ignore_legacy_action_gate": True,
        "profit_target_pct": 0.008,
    },
    "sentiment_discount_with_quality": {
        "base_strategy": "with_target_quality",
        "sentiment_discount": True,
    },
    "sentiment_discount_with_quality_tp_1_0": {
        "base_strategy": "with_target_quality",
        "sentiment_discount": True,
        "profit_target_pct": 0.01,
    },
    "sentiment_discount_with_quality_tp_1_2": {
        "base_strategy": "with_target_quality",
        "sentiment_discount": True,
        "profit_target_pct": 0.012,
    },
    "sentiment_discount_with_quality_tp_1_5": {
        "base_strategy": "with_target_quality",
        "sentiment_discount": True,
        "profit_target_pct": 0.015,
    },
    "sentiment_discount_with_quality_tp_1_5_hold": {
        "base_strategy": "with_target_quality",
        "sentiment_discount": True,
        "profit_target_pct": 0.015,
        "exit_mode": "profit_only",
    },
}


def now_utc():
    return datetime.now(timezone.utc)


def numeric_or_none(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def load_jsonl(path):
    if not os.path.exists(path):
        return []

    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                rows.append(json.loads(text))
            except Exception:
                continue
    return rows


def resolve_repo_path(path):
    if not path:
        return path
    expanded = os.path.expanduser(path)
    if os.path.isabs(expanded):
        return expanded
    return os.path.join(REPO_DIR, expanded)


def load_json_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_strategy_set(path):
    resolved = resolve_repo_path(path)
    if not resolved or not os.path.exists(resolved):
        return []

    entries = []
    with open(resolved, encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            if "," in text:
                label, strategy_path = [part.strip() for part in text.split(",", 1)]
            else:
                strategy_path = text
                label = os.path.splitext(os.path.basename(strategy_path))[0]
            entries.append({
                "label": label,
                "path": resolve_repo_path(strategy_path),
            })
    return entries


def strategy_sha256(payload):
    text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def snapshot_with_strategy(snapshot, strategy_path, strategy_payload):
    cloned = dict(snapshot)
    cloned["strategy_profile"] = {
        **(snapshot.get("strategy_profile") or {}),
        "exists": True,
        "error": None,
        "path": strategy_path,
        "sha256": strategy_sha256(strategy_payload),
        "payload": strategy_payload,
    }
    return cloned


def rotated_snapshot_path(base_path, dt):
    root, ext = os.path.splitext(base_path)
    suffix = dt.strftime("%Y%m%d")
    return f"{root}_{suffix}{ext or '.jsonl'}"


def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def snapshot_source_files(base_path, since_dt, until_dt):
    rotated_files = []
    if SNAPSHOT_ROTATE_DAILY:
        for day in daterange(since_dt.date(), until_dt.date()):
            rotated_path = rotated_snapshot_path(
                base_path,
                datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            )
            if os.path.exists(rotated_path):
                rotated_files.append(os.path.abspath(rotated_path))

    if rotated_files:
        return rotated_files

    if os.path.exists(base_path):
        return [os.path.abspath(base_path)]

    return []


def expected_snapshot_source_files(base_path, since_dt, until_dt):
    if SNAPSHOT_ROTATE_DAILY:
        return [
            os.path.abspath(rotated_snapshot_path(
                base_path,
                datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            ))
            for day in daterange(since_dt.date(), until_dt.date())
        ]

    return [os.path.abspath(base_path)]


def snapshot_file_metadata(paths):
    metadata = []
    for path in paths:
        exists = os.path.exists(path)
        row_count = None
        if exists:
            try:
                with open(path, encoding="utf-8") as f:
                    row_count = sum(1 for line in f if line.strip())
            except Exception:
                row_count = None

        metadata.append({
            "path": path,
            "exists": exists,
            "size_bytes": os.path.getsize(path) if exists else None,
            "row_count": row_count,
        })

    return metadata


def strategy_config(snapshot):
    profile = snapshot.get("strategy_profile", {})
    payload = profile.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def strategy_value(snapshot, key, default):
    value = strategy_config(snapshot).get(key, default)
    return default if value is None else value


def extract_price(snapshot):
    ticker = snapshot.get("ticker", {})
    last_price = numeric_or_none(ticker.get("last_price"))
    if last_price is not None:
        return last_price

    signal_payload = (snapshot.get("signal") or {}).get("payload") or {}
    signal_price = numeric_or_none(signal_payload.get("btc_price"))
    if signal_price is not None:
        return signal_price

    target_quality_payload = (snapshot.get("target_quality") or {}).get("payload") or {}
    quality_price = numeric_or_none(target_quality_payload.get("current_price"))
    if quality_price is not None:
        return quality_price

    return None


def signal_payload(snapshot):
    payload = (snapshot.get("signal") or {}).get("payload")
    if not isinstance(payload, dict):
        return {}
    return normalize_signal_payload(payload, asset_id=SIGNAL_ASSET_ID)


def quality_payload(snapshot):
    payload = (snapshot.get("target_quality") or {}).get("payload")
    return payload if isinstance(payload, dict) else {}


def target_price_from_quality_target(target):
    if not isinstance(target, dict):
        return None

    buy_price = numeric_or_none(target.get("buy_price"))
    if buy_price is None or buy_price <= 0:
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


def derived_quality_target_prices(snapshot):
    if not BACKTEST_DERIVE_TARGETS_FROM_QUALITY:
        return []

    quality = quality_payload(snapshot)
    targets = quality.get("targets")
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


def signal_payload_with_target_fallback(snapshot):
    signal = signal_payload(snapshot)
    targets = signal.get("target_prices")
    if isinstance(targets, list) and targets:
        return signal, False

    derived = derived_quality_target_prices(snapshot)
    if not derived:
        return signal, False

    signal = dict(signal)
    signal["target_prices"] = derived
    signal["target_prices_source"] = "target_quality"
    return signal, True


RISK_CONTEXT_NUMERIC_FIELDS = {
    "market_risk_score": "sentiment_market_risk_score",
    "buy_aggression_score": "sentiment_buy_aggression_score",
    "downside_risk_score": "sentiment_downside_risk_score",
    "bottoming_score": "sentiment_bottoming_score",
    "rebound_score": "sentiment_rebound_score",
    "breakout_score": "sentiment_breakout_score",
    "position_size_multiplier": "sentiment_position_size_multiplier",
    "grid_aggression_multiplier": "sentiment_grid_aggression_multiplier",
    "target_profit_multiplier": "sentiment_target_profit_multiplier",
    "entry_discount_multiplier": "sentiment_entry_discount_multiplier",
}


WEATHER_BOT_TUNING_NUMERIC_FIELDS = {
    "position_size_multiplier": "sentiment_weather_position_size_multiplier",
    "grid_aggression_multiplier": "sentiment_weather_grid_aggression_multiplier",
    "target_profit_multiplier": "sentiment_weather_target_profit_multiplier",
    "entry_discount_multiplier": "sentiment_weather_entry_discount_multiplier",
}


def risk_context_payload(signal):
    risk_context = signal.get("risk_context")
    return risk_context if isinstance(risk_context, dict) else {}


def weather_report_payload(signal):
    weather_report = risk_context_payload(signal).get("weather_report")
    return weather_report if isinstance(weather_report, dict) else {}


def sentiment_risk_event_fields(signal):
    risk_context = risk_context_payload(signal)
    weather_report = weather_report_payload(signal)
    bot_tuning = weather_report.get("bot_tuning")
    if not isinstance(bot_tuning, dict):
        bot_tuning = {}
    flags = risk_context.get("hard_safety_flags")
    if not isinstance(flags, list):
        flags = []
    opportunity_tags = weather_report.get("opportunity_tags")
    if not isinstance(opportunity_tags, list):
        opportunity_tags = []
    risk_warnings = weather_report.get("risk_warnings")
    if not isinstance(risk_warnings, list):
        risk_warnings = []
    fields = {
        "sentiment_risk_posture": risk_context.get("recommended_posture"),
        "sentiment_hard_safety_flags": flags,
        "sentiment_weather_mode": weather_report.get("mode"),
        "sentiment_weather_bot_decision_authority": weather_report.get(
            "bot_decision_authority"
        ),
        "sentiment_weather_trade_permission": weather_report.get(
            "trade_permission"
        ),
        "sentiment_weather_condition": weather_report.get("condition"),
        "sentiment_weather_alert_level": weather_report.get("alert_level"),
        "sentiment_weather_emergency_bell": bool(
            weather_report.get("emergency_bell")
        ),
        "sentiment_weather_opportunity_tags": opportunity_tags,
        "sentiment_weather_risk_warnings": risk_warnings,
    }
    for source_key, output_key in RISK_CONTEXT_NUMERIC_FIELDS.items():
        fields[output_key] = numeric_or_none(risk_context.get(source_key))
    for source_key, output_key in WEATHER_BOT_TUNING_NUMERIC_FIELDS.items():
        fields[output_key] = numeric_or_none(bot_tuning.get(source_key))
    return fields


def apply_sentiment_risk_summary(summary, events):
    summary["sentiment_risk_sample_count"] = len(events)
    context_present_count = 0
    numeric_sample_count = 0
    posture_present_count = 0
    weather_present_count = 0
    emergency_count = 0
    posture_counts = {}
    weather_condition_counts = {}
    weather_alert_level_counts = {}
    weather_trade_permission_counts = {}
    weather_opportunity_tag_counts = {}
    weather_risk_warning_counts = {}
    hard_safety_flag_counts = {}
    numeric_field_names = list(RISK_CONTEXT_NUMERIC_FIELDS.values()) + list(
        WEATHER_BOT_TUNING_NUMERIC_FIELDS.values()
    )
    numeric_totals = {field: 0.0 for field in numeric_field_names}
    numeric_counts = {field: 0 for field in numeric_field_names}

    for event in events:
        posture = event.get("sentiment_risk_posture")
        if posture:
            posture_present_count += 1
            posture_counts[posture] = posture_counts.get(posture, 0) + 1
        weather_condition = event.get("sentiment_weather_condition")
        weather_alert_level = event.get("sentiment_weather_alert_level")
        weather_trade_permission = event.get("sentiment_weather_trade_permission")
        weather_has_content = any((
            event.get("sentiment_weather_mode"),
            weather_condition,
            weather_alert_level,
            weather_trade_permission,
            event.get("sentiment_weather_opportunity_tags"),
            event.get("sentiment_weather_risk_warnings"),
        ))
        if weather_has_content:
            weather_present_count += 1
        if event.get("sentiment_weather_emergency_bell"):
            emergency_count += 1
        if weather_condition:
            weather_condition_counts[weather_condition] = (
                weather_condition_counts.get(weather_condition, 0) + 1
            )
        if weather_alert_level:
            weather_alert_level_counts[weather_alert_level] = (
                weather_alert_level_counts.get(weather_alert_level, 0) + 1
            )
        if weather_trade_permission:
            weather_trade_permission_counts[weather_trade_permission] = (
                weather_trade_permission_counts.get(weather_trade_permission, 0) + 1
            )
        for tag in event.get("sentiment_weather_opportunity_tags") or []:
            weather_opportunity_tag_counts[tag] = (
                weather_opportunity_tag_counts.get(tag, 0) + 1
            )
        for warning in event.get("sentiment_weather_risk_warnings") or []:
            weather_risk_warning_counts[warning] = (
                weather_risk_warning_counts.get(warning, 0) + 1
            )
        flags = event.get("sentiment_hard_safety_flags") or []
        for flag in flags:
            hard_safety_flag_counts[flag] = hard_safety_flag_counts.get(flag, 0) + 1
        event_numeric_count = 0
        for field in numeric_field_names:
            value = numeric_or_none(event.get(field))
            if value is None:
                continue
            numeric_totals[field] += value
            numeric_counts[field] += 1
            event_numeric_count += 1
        if event_numeric_count > 0:
            numeric_sample_count += 1
        if posture or flags or event_numeric_count > 0:
            context_present_count += 1

    summary["sentiment_risk_context_present_count"] = context_present_count
    summary["sentiment_risk_context_missing_count"] = (
        len(events) - context_present_count
    )
    summary["sentiment_risk_context_coverage_pct"] = (
        round(context_present_count / len(events), 6)
        if events else
        None
    )
    summary["sentiment_risk_numeric_sample_count"] = numeric_sample_count
    summary["sentiment_risk_posture_present_count"] = posture_present_count
    summary["sentiment_risk_posture_missing_count"] = (
        len(events) - posture_present_count
    )
    summary["sentiment_weather_report_present_count"] = weather_present_count
    summary["sentiment_weather_report_missing_count"] = (
        len(events) - weather_present_count
    )
    summary["sentiment_weather_report_coverage_pct"] = (
        round(weather_present_count / len(events), 6)
        if events else
        None
    )
    summary["sentiment_weather_emergency_bell_count"] = emergency_count
    summary["sentiment_risk_posture_counts"] = posture_counts
    summary["sentiment_hard_safety_flag_counts"] = hard_safety_flag_counts
    summary["sentiment_weather_condition_counts"] = weather_condition_counts
    summary["sentiment_weather_alert_level_counts"] = weather_alert_level_counts
    summary["sentiment_weather_trade_permission_counts"] = (
        weather_trade_permission_counts
    )
    summary["sentiment_weather_opportunity_tag_counts"] = (
        weather_opportunity_tag_counts
    )
    summary["sentiment_weather_risk_warning_counts"] = weather_risk_warning_counts
    for field in numeric_field_names:
        summary[f"avg_{field}"] = (
            round(numeric_totals[field] / numeric_counts[field], 6)
            if numeric_counts[field] else
            None
        )
    return summary


def snapshot_timestamp(snapshot):
    return parse_iso8601(snapshot.get("captured_at"))


def effective_fee_bps(snapshot=None):
    if BACKTEST_FEE_BPS > 0:
        return BACKTEST_FEE_BPS

    if snapshot is None:
        return BACKTEST_FEE_BPS

    round_trip_fee_pct = effective_round_trip_fee_pct(
        strategy_config(snapshot),
        None
    )
    if round_trip_fee_pct is None:
        return BACKTEST_FEE_BPS

    return round_trip_fee_pct * 10000.0


def report_fee_bps(snapshots):
    if BACKTEST_FEE_BPS > 0:
        return BACKTEST_FEE_BPS
    for snapshot in snapshots:
        fee_bps = effective_fee_bps(snapshot)
        if fee_bps is not None:
            return round(fee_bps, 6)
    return BACKTEST_FEE_BPS


def strategy_options(strategy_name):
    return BACKTEST_STRATEGIES.get(strategy_name, {})


def base_strategy_name(strategy_name):
    return strategy_options(strategy_name).get("base_strategy", strategy_name)


def strategy_profit_target_override(strategy_name):
    return strategy_options(strategy_name).get("profit_target_pct")


def strategy_uses_sentiment_discount(strategy_name):
    return bool(strategy_options(strategy_name).get("sentiment_discount"))


def strategy_ignores_sentiment(strategy_name):
    return bool(strategy_options(strategy_name).get("ignore_sentiment"))


def strategy_ignores_legacy_action_gate(strategy_name):
    return bool(strategy_options(strategy_name).get("ignore_legacy_action_gate"))


def strategy_exit_mode(strategy_name):
    return strategy_options(strategy_name).get("exit_mode", "stop_loss")


def strategy_uses_profit_only_exit(strategy_name):
    return strategy_exit_mode(strategy_name) == "profit_only"


def sentiment_discount_requirement_pct(snapshot, signal):
    config = strategy_config(snapshot)
    recommendation = signal.get("action_recommendation")
    mode = (signal.get("market_interpretation") or {}).get("mode")

    watch_discount = float(config.get(
        "sentiment_discount_watch_pct",
        BACKTEST_SENTIMENT_DISCOUNT_WATCH_PCT
    ))
    bearish_discount = float(config.get(
        "sentiment_discount_bearish_pct",
        BACKTEST_SENTIMENT_DISCOUNT_BEARISH_PCT
    ))

    if recommendation == "bullish_allowed":
        return 0.0
    if recommendation in ("watch_only", "contrarian_watch") or mode in (
        "watch_only_rebound",
        "contrarian_watch",
    ):
        return watch_discount
    if recommendation in ("risk_off", "bearish_allowed"):
        return bearish_discount
    return None


def select_target_candidate(snapshot, current_price, last_sell_price=None, signal=None):
    signal = signal or signal_payload(snapshot)
    targets = signal.get("target_prices", [])
    if not isinstance(targets, list):
        return None

    max_premium_pct = float(strategy_value(snapshot, "target_limit_max_premium_pct", 0.0005))
    prevent_buy_above_last_sell = str(
        strategy_value(snapshot, "prevent_buy_above_last_sell", True)
    ).strip().lower() in ("1", "true", "yes", "on")
    buy_after_sell_discount_pct = float(
        strategy_value(snapshot, "buy_after_sell_discount_pct", 0.001)
    )

    max_price = current_price * (1 + max_premium_pct)
    max_rebuy_price = None
    if prevent_buy_above_last_sell and last_sell_price is not None:
        max_rebuy_price = last_sell_price * (1 - buy_after_sell_discount_pct)

    valid = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        buy_price = numeric_or_none(target.get("buy_price"))
        if buy_price is None or buy_price <= 0:
            continue
        if buy_price > max_price:
            continue
        if max_rebuy_price is not None and buy_price >= max_rebuy_price:
            continue
        valid.append({
            "buy_price": buy_price,
            "signal_sell_pct": target.get("sell_pct")
        })

    if not valid:
        return None

    return max(valid, key=lambda item: item["buy_price"])


def quality_decision(
    snapshot,
    candidate,
    strategy_name,
    ignore_sentiment=False,
    current_price=None
):
    effective_strategy = base_strategy_name(strategy_name)
    config = strategy_config(snapshot)

    if effective_strategy == "price_target_only":
        return {
            "allowed": True,
            "reason": "price_target_only",
            "policy": {
                "recommendation": "price_target_only",
                "reason": "Sentiment and target quality ignored for baseline."
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    signal = signal_payload(snapshot)
    action_recommendation = signal.get("action_recommendation")
    action_policy = signal.get("action_policy", {})
    if not isinstance(action_policy, dict):
        action_policy = {}

    if (
        strategy_uses_sentiment_discount(strategy_name)
        and not ignore_sentiment
        and action_recommendation != "bullish_allowed"
    ):
        required_discount_pct = sentiment_discount_requirement_pct(snapshot, signal)
        if required_discount_pct is None or current_price is None:
            return {
                "allowed": False,
                "reason": "blocked",
                "policy": {
                    "recommendation": action_recommendation,
                    "reason": action_policy.get("reason") or signal.get("reason"),
                    "sentiment_discount_required_pct": required_discount_pct,
                    "sentiment_discount_actual_pct": None,
                },
                "profit_target_pct": None
            }

        actual_discount_pct = (
            (current_price - candidate["buy_price"]) / current_price
        ) * 100.0
        if actual_discount_pct < required_discount_pct:
            return {
                "allowed": False,
                "reason": "blocked",
                "policy": {
                    "recommendation": action_recommendation,
                    "reason": action_policy.get("reason") or signal.get("reason"),
                    "sentiment_discount_required_pct": required_discount_pct,
                    "sentiment_discount_actual_pct": round(actual_discount_pct, 6),
                },
                "profit_target_pct": None
            }

    elif (
        not ignore_sentiment
        and not strategy_ignores_sentiment(strategy_name)
        and not strategy_ignores_legacy_action_gate(strategy_name)
        and action_recommendation != "bullish_allowed"
    ):
        return {
            "allowed": False,
            "reason": "blocked",
            "policy": {
                "recommendation": action_recommendation,
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": None
        }

    if effective_strategy == "sentiment_policy_only":
        return {
            "allowed": True,
            "reason": "sentiment_policy_only",
            "policy": {
                "recommendation": "bullish_allowed",
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    target_quality_enabled = str(
        config.get("target_quality_enabled", True)
    ).strip().lower() in ("1", "true", "yes", "on")
    target_quality_fail_closed = str(
        config.get("target_quality_fail_closed", False)
    ).strip().lower() in ("1", "true", "yes", "on")
    min_samples = int(config.get("target_quality_min_samples", 20))
    min_ev_pct = float(config.get("target_quality_min_ev_pct", 0.02))
    min_fill_probability = float(
        config.get("target_quality_min_4h_fill_probability", 0.35)
    )
    allowed_recommendations = {
        value.strip()
        for value in str(
            config.get("target_quality_allowed_recommendations", "buy_allowed,watch")
        ).split(",")
        if value.strip()
    }

    if not target_quality_enabled:
        return {
            "allowed": True,
            "reason": "target_quality_disabled",
            "policy": {
                "recommendation": "bullish_allowed",
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    quality = quality_payload(snapshot)
    quality_status_ok = (
        (snapshot.get("target_quality") or {}).get("ok")
        and isinstance(quality, dict)
        and quality.get("status") == "ok"
    )
    if not quality_status_ok:
        unavailable = unavailable_quality_decision(
            {"available": False, "reason": (snapshot.get("target_quality") or {}).get("error") or "target_quality_unavailable"},
            fail_closed=target_quality_fail_closed
        )
        return {
            "allowed": unavailable["allowed"],
            "reason": unavailable["reason"],
            "policy": {
                "recommendation": "bullish_allowed",
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    matched = match_quality_target(candidate["buy_price"], quality.get("targets", []))
    evaluation = evaluate_quality_target(
        matched,
        min_samples=min_samples,
        min_ev_pct=min_ev_pct,
        min_4h_fill_probability=min_fill_probability,
        allowed_recommendations=allowed_recommendations
    )
    profit_target_pct = normalize_profit_target_pct(
        evaluation.get("best_profit_target_pct")
    )
    if profit_target_pct is None:
        profit_target_pct = normalize_profit_target_pct(candidate.get("signal_sell_pct"))
    override_profit_target_pct = strategy_profit_target_override(strategy_name)
    if override_profit_target_pct is not None:
        profit_target_pct = override_profit_target_pct

    policy = {
        "recommendation": evaluation.get("recommendation"),
        "reason": evaluation["reason"],
        "matched_sample_count": evaluation.get("matched_sample_count"),
        "fill_probability_4h": evaluation.get("fill_probability_4h"),
        "best_expected_value_pct_per_signal": evaluation.get(
            "best_expected_value_pct_per_signal"
        ),
        "best_profit_target_pct": evaluation.get("best_profit_target_pct")
    }
    if strategy_uses_sentiment_discount(strategy_name):
        required_discount_pct = sentiment_discount_requirement_pct(snapshot, signal)
        actual_discount_pct = None
        if current_price is not None:
            actual_discount_pct = (
                (current_price - candidate["buy_price"]) / current_price
            ) * 100.0
        policy["sentiment_recommendation"] = action_recommendation
        policy["sentiment_discount_required_pct"] = required_discount_pct
        policy["sentiment_discount_actual_pct"] = (
            round(actual_discount_pct, 6)
            if actual_discount_pct is not None else
            None
        )

    return {
        "allowed": evaluation["allowed"],
        "reason": evaluation["reason"],
        "policy": policy,
        "profit_target_pct": profit_target_pct
    }


def exit_prices(entry_price, target_profit_pct, fee_bps=0.0):
    fee_pct = (fee_bps or 0.0) / 10000.0
    tp_price = entry_price * (1 + target_profit_pct + fee_pct)
    sl_price = entry_price * (1 - (BACKTEST_STOP_LOSS_PCT / 100.0))
    return tp_price, sl_price


def compute_trade_stats(entry_price, exit_price, high_water, low_water, fee_bps=None):
    gross_return_pct = ((exit_price - entry_price) / entry_price) * 100.0
    fee_pct = (BACKTEST_FEE_BPS if fee_bps is None else fee_bps) / 100.0
    net_return_pct = gross_return_pct - fee_pct
    max_runup_pct = ((high_water - entry_price) / entry_price) * 100.0
    max_drawdown_pct = ((low_water - entry_price) / entry_price) * 100.0
    return gross_return_pct, net_return_pct, max_runup_pct, max_drawdown_pct


def simulate_candidate_markout(
    snapshots,
    start_index,
    *,
    strategy_name,
    decision_time,
    signal_timestamp,
    decision_price,
    buy_target,
    profit_target_pct,
    fee_bps,
    signal,
    policy,
):
    risk_fields = sentiment_risk_event_fields(signal)
    entry_price = buy_target["buy_price"]
    fill_deadline = decision_time + timedelta(hours=BACKTEST_ENTRY_WAIT_HOURS)
    filled_at = None
    mark_time = decision_time
    mark_price = decision_price
    high_water = entry_price
    low_water = entry_price

    for future in snapshots[start_index + 1:]:
        timestamp = snapshot_timestamp(future)
        price = extract_price(future)
        if timestamp is None:
            continue
        if timestamp > fill_deadline:
            return {
                "strategy": strategy_name,
                "decision_time": decision_time.isoformat(),
                "signal_timestamp": signal_timestamp,
                "decision_price": decision_price,
                "buy_target": buy_target,
                "outcome": "not_filled",
                "fill_deadline": fill_deadline.isoformat(),
                "fee_bps": round(fee_bps, 6),
                "policy": policy,
                **risk_fields,
            }
        if price is not None and price <= entry_price:
            filled_at = timestamp
            mark_time = timestamp
            mark_price = price
            break

    if filled_at is None:
        return {
            "strategy": strategy_name,
            "decision_time": decision_time.isoformat(),
            "signal_timestamp": signal_timestamp,
            "decision_price": decision_price,
            "buy_target": buy_target,
            "outcome": "not_filled",
            "fill_deadline": fill_deadline.isoformat(),
            "fee_bps": round(fee_bps, 6),
            "policy": policy,
            **risk_fields,
        }

    exit_deadline = filled_at + timedelta(hours=BACKTEST_MAX_HOLD_HOURS)
    tp_price, sl_price = exit_prices(entry_price, profit_target_pct, fee_bps)

    for future in snapshots[start_index + 1:]:
        timestamp = snapshot_timestamp(future)
        price = extract_price(future)
        if timestamp is None or timestamp < filled_at or price is None:
            continue

        mark_time = timestamp
        mark_price = price
        high_water = max(high_water, price)
        low_water = min(low_water, price)

        exit_reason = None
        exit_price = None
        if price >= tp_price:
            exit_reason = "take_profit"
            exit_price = tp_price
        elif price <= sl_price:
            exit_reason = "stop_loss"
            exit_price = sl_price
        elif timestamp >= exit_deadline:
            exit_reason = "timeout"
            exit_price = price

        if exit_reason is not None:
            hold_minutes = (timestamp - filled_at).total_seconds() / 60.0
            gross_return_pct, net_return_pct, max_runup_pct, max_drawdown_pct = (
                compute_trade_stats(
                    entry_price,
                    exit_price,
                    high_water,
                    low_water,
                    fee_bps,
                )
            )
            return {
                "strategy": strategy_name,
                "decision_time": decision_time.isoformat(),
                "signal_timestamp": signal_timestamp,
                "decision_price": decision_price,
                "buy_target": buy_target,
                "outcome": exit_reason,
                "filled_at": filled_at.isoformat(),
                "entry_price": entry_price,
                "exit_time": timestamp.isoformat(),
                "exit_price": round(exit_price, 2),
                "fee_bps": round(fee_bps, 6),
                "gross_return_pct": round(gross_return_pct, 6),
                "net_return_pct": round(net_return_pct, 6),
                "max_runup_pct": round(max_runup_pct, 6),
                "max_drawdown_pct": round(max_drawdown_pct, 6),
                "hold_minutes": round(hold_minutes, 2),
                "execution_signal": numeric_or_none(signal.get("execution_signal")),
                "confidence": numeric_or_none(signal.get("confidence")),
                "contributor_count": signal.get("contributor_count"),
                "policy": policy,
                **risk_fields,
            }

    gross_return_pct, net_return_pct, max_runup_pct, max_drawdown_pct = (
        compute_trade_stats(
            entry_price,
            mark_price,
            high_water,
            low_water,
            fee_bps,
        )
    )
    hold_minutes = (mark_time - filled_at).total_seconds() / 60.0
    return {
        "strategy": strategy_name,
        "decision_time": decision_time.isoformat(),
        "signal_timestamp": signal_timestamp,
        "decision_price": decision_price,
        "buy_target": buy_target,
        "outcome": "open",
        "filled_at": filled_at.isoformat(),
        "entry_price": entry_price,
        "mark_time": mark_time.isoformat(),
        "mark_price": round(mark_price, 2) if mark_price is not None else None,
        "fee_bps": round(fee_bps, 6),
        "gross_return_pct": round(gross_return_pct, 6),
        "unrealized_net_return_pct": round(net_return_pct, 6),
        "max_runup_pct": round(max_runup_pct, 6),
        "max_drawdown_pct": round(max_drawdown_pct, 6),
        "hold_minutes": round(hold_minutes, 2),
        "execution_signal": numeric_or_none(signal.get("execution_signal")),
        "confidence": numeric_or_none(signal.get("confidence")),
        "contributor_count": signal.get("contributor_count"),
        "policy": policy,
        **risk_fields,
    }


def empty_summary():
    summary = {
        "trades": 0,
        "win_rate": None,
        "avg_net_return_pct": None,
        "avg_fee_bps": None,
        "total_net_return_pct": None,
        "marked_to_market_net_return_pct": None,
        "avg_hold_minutes": None,
        "open_position_count": 0,
        "open_position_unrealized_net_pct": None,
        "open_position_max_drawdown_pct": None,
        "open_position_max_runup_pct": None,
        "take_profit_count": 0,
        "stop_loss_count": 0,
        "timeout_count": 0,
        "max_drawdown_pct": None,
        "max_runup_pct": None,
        "raw_candidates": 0,
        "approved_candidates": 0,
        "candidate_signals": 0,
        "blocked_by_sentiment": 0,
        "blocked_by_target_quality": 0,
        "shadow_target_quality_approved": 0,
        "shadow_target_quality_rejected": 0,
        "shadow_target_quality_unavailable": 0,
        "sentiment_saved_quality_candidates": 0,
        "sentiment_saved_quality_candidate_rate": None,
        "sentiment_blocked_quality_markout_count": 0,
        "sentiment_blocked_quality_filled": 0,
        "sentiment_blocked_quality_not_filled": 0,
        "sentiment_blocked_quality_take_profit": 0,
        "sentiment_blocked_quality_stop_loss": 0,
        "sentiment_blocked_quality_timeout": 0,
        "sentiment_blocked_quality_open": 0,
        "sentiment_blocked_quality_win_rate": None,
        "sentiment_blocked_quality_total_net_return_pct": None,
        "sentiment_blocked_quality_marked_to_market_net_return_pct": None,
        "sentiment_risk_sample_count": 0,
        "sentiment_risk_context_present_count": 0,
        "sentiment_risk_context_missing_count": 0,
        "sentiment_risk_context_coverage_pct": None,
        "sentiment_risk_numeric_sample_count": 0,
        "sentiment_risk_posture_present_count": 0,
        "sentiment_risk_posture_missing_count": 0,
        "sentiment_risk_posture_counts": {},
        "sentiment_hard_safety_flag_counts": {},
        "sentiment_weather_report_present_count": 0,
        "sentiment_weather_report_missing_count": 0,
        "sentiment_weather_report_coverage_pct": None,
        "sentiment_weather_emergency_bell_count": 0,
        "sentiment_weather_condition_counts": {},
        "sentiment_weather_alert_level_counts": {},
        "sentiment_weather_trade_permission_counts": {},
        "sentiment_weather_opportunity_tag_counts": {},
        "sentiment_weather_risk_warning_counts": {},
        "signal_target_snapshots": 0,
        "quality_fallback_target_snapshots": 0,
        "missing_signal": 0,
        "missing_price": 0,
        "no_target": 0,
        "not_filled": 0,
        "skipped_during_position": 0,
        "fill_rate_after_approval": None,
        "terminal_rate_after_approval": None
    }
    for output_key in (
        list(RISK_CONTEXT_NUMERIC_FIELDS.values())
        + list(WEATHER_BOT_TUNING_NUMERIC_FIELDS.values())
    ):
        summary[f"avg_{output_key}"] = None
    return summary


def apply_blocked_quality_markout_summary(summary, markouts):
    summary["sentiment_blocked_quality_markout_count"] = len(markouts)
    summary["sentiment_blocked_quality_filled"] = sum(
        1 for markout in markouts
        if markout.get("outcome") not in (None, "not_filled")
    )
    summary["sentiment_blocked_quality_not_filled"] = sum(
        1 for markout in markouts
        if markout.get("outcome") == "not_filled"
    )
    summary["sentiment_blocked_quality_take_profit"] = sum(
        1 for markout in markouts
        if markout.get("outcome") == "take_profit"
    )
    summary["sentiment_blocked_quality_stop_loss"] = sum(
        1 for markout in markouts
        if markout.get("outcome") == "stop_loss"
    )
    summary["sentiment_blocked_quality_timeout"] = sum(
        1 for markout in markouts
        if markout.get("outcome") == "timeout"
    )
    summary["sentiment_blocked_quality_open"] = sum(
        1 for markout in markouts
        if markout.get("outcome") == "open"
    )

    terminal = [
        markout for markout in markouts
        if markout.get("net_return_pct") is not None
    ]
    open_markouts = [
        markout for markout in markouts
        if markout.get("unrealized_net_return_pct") is not None
    ]
    if terminal:
        wins = [
            markout for markout in terminal
            if markout.get("net_return_pct", 0.0) > 0.0
        ]
        summary["sentiment_blocked_quality_win_rate"] = round(
            len(wins) / len(terminal),
            4
        )
        summary["sentiment_blocked_quality_total_net_return_pct"] = round(
            sum(markout["net_return_pct"] for markout in terminal),
            6
        )
    if terminal or open_markouts:
        total_net = sum(markout.get("net_return_pct") or 0.0 for markout in terminal)
        open_net = sum(
            markout.get("unrealized_net_return_pct") or 0.0
            for markout in open_markouts
        )
        summary["sentiment_blocked_quality_marked_to_market_net_return_pct"] = round(
            total_net + open_net,
            6
        )
    return summary


def sentiment_risk_fields_from_record(record):
    fields = {
        "sentiment_risk_posture": record.get("sentiment_risk_posture"),
        "sentiment_hard_safety_flags": record.get("sentiment_hard_safety_flags") or [],
        "sentiment_weather_mode": record.get("sentiment_weather_mode"),
        "sentiment_weather_bot_decision_authority": record.get(
            "sentiment_weather_bot_decision_authority"
        ),
        "sentiment_weather_trade_permission": record.get(
            "sentiment_weather_trade_permission"
        ),
        "sentiment_weather_condition": record.get("sentiment_weather_condition"),
        "sentiment_weather_alert_level": record.get("sentiment_weather_alert_level"),
        "sentiment_weather_emergency_bell": record.get(
            "sentiment_weather_emergency_bell"
        ),
        "sentiment_weather_opportunity_tags": record.get(
            "sentiment_weather_opportunity_tags"
        ) or [],
        "sentiment_weather_risk_warnings": record.get(
            "sentiment_weather_risk_warnings"
        ) or [],
    }
    for output_key in (
        list(RISK_CONTEXT_NUMERIC_FIELDS.values())
        + list(WEATHER_BOT_TUNING_NUMERIC_FIELDS.values())
    ):
        fields[output_key] = record.get(output_key)
    return fields


def finalize_open_position(position, last_price, last_timestamp):
    if position is None or last_price is None:
        return None

    high_water = max(position["high_water"], last_price)
    low_water = min(position["low_water"], last_price)
    gross_return_pct, net_return_pct, max_runup_pct, max_drawdown_pct = (
        compute_trade_stats(
            position["entry_price"],
            last_price,
            high_water,
            low_water,
            position["fee_bps"]
        )
    )
    hold_minutes = None
    if last_timestamp is not None:
        hold_minutes = (
            last_timestamp - position["filled_at"]
        ).total_seconds() / 60.0

    return {
        "strategy": position["strategy"],
        "decision_time": position["decision_time"].isoformat(),
        "signal_timestamp": position["signal_timestamp"],
        "decision_price": position["decision_price"],
        "buy_target": position["buy_target"],
        "filled_at": position["filled_at"].isoformat(),
        "entry_price": position["entry_price"],
        "mark_time": last_timestamp.isoformat() if last_timestamp else None,
        "mark_price": round(last_price, 2),
        "fee_bps": round(position["fee_bps"], 6),
        "gross_return_pct": round(gross_return_pct, 6),
        "unrealized_net_return_pct": round(net_return_pct, 6),
        "max_runup_pct": round(max_runup_pct, 6),
        "max_drawdown_pct": round(max_drawdown_pct, 6),
        "hold_minutes": round(hold_minutes, 2) if hold_minutes is not None else None,
        "execution_signal": position["execution_signal"],
        "confidence": position["confidence"],
        "contributor_count": position["contributor_count"],
        "policy": position["policy"],
        **sentiment_risk_fields_from_record(position),
    }


def finalize_summary(summary, trades, open_positions=None):
    summary["trades"] = len(trades)
    open_positions = open_positions or []
    summary["open_position_count"] = len(open_positions)
    if open_positions:
        summary["open_position_unrealized_net_pct"] = round(
            sum(position["unrealized_net_return_pct"] for position in open_positions),
            6
        )
        summary["open_position_max_drawdown_pct"] = round(
            min(position["max_drawdown_pct"] for position in open_positions),
            6
        )
        summary["open_position_max_runup_pct"] = round(
            max(position["max_runup_pct"] for position in open_positions),
            6
        )
    completed_net = sum(trade["net_return_pct"] for trade in trades)
    open_net = sum(
        position["unrealized_net_return_pct"] for position in open_positions
    )
    if trades or open_positions:
        summary["marked_to_market_net_return_pct"] = round(
            completed_net + open_net,
            6
        )
    approved_candidates = summary["approved_candidates"]
    blocked_by_sentiment = summary.get("blocked_by_sentiment") or 0
    saved_quality_candidates = summary.get("shadow_target_quality_approved") or 0
    summary["sentiment_saved_quality_candidates"] = saved_quality_candidates
    summary["sentiment_saved_quality_candidate_rate"] = (
        round(saved_quality_candidates / blocked_by_sentiment, 6)
        if blocked_by_sentiment > 0 else
        None
    )

    if approved_candidates > 0:
        summary["fill_rate_after_approval"] = round(
            len(trades) / approved_candidates,
            4
        )
        summary["terminal_rate_after_approval"] = round(
            (len(trades) + summary["not_filled"]) / approved_candidates,
            4
        )
    else:
        summary["fill_rate_after_approval"] = None
        summary["terminal_rate_after_approval"] = None

    if not trades:
        return summary

    winning = [trade for trade in trades if trade["net_return_pct"] > 0]
    summary["win_rate"] = round(len(winning) / len(trades), 4)
    summary["avg_net_return_pct"] = round(
        sum(trade["net_return_pct"] for trade in trades) / len(trades),
        6
    )
    summary["avg_fee_bps"] = round(
        sum(trade.get("fee_bps", 0.0) for trade in trades) / len(trades),
        6
    )
    summary["total_net_return_pct"] = round(
        sum(trade["net_return_pct"] for trade in trades),
        6
    )
    summary["avg_hold_minutes"] = round(
        sum(trade["hold_minutes"] for trade in trades) / len(trades),
        2
    )
    summary["max_drawdown_pct"] = round(
        min(trade["max_drawdown_pct"] for trade in trades),
        6
    )
    summary["max_runup_pct"] = round(
        max(trade["max_runup_pct"] for trade in trades),
        6
    )
    return summary


def target_diagnostics(snapshots):
    signal_snapshots = 0
    signal_target_snapshots = 0
    quality_target_snapshots = 0
    fallback_target_snapshots = 0
    signal_target_total = 0
    quality_target_total = 0

    for snapshot in snapshots:
        signal = signal_payload(snapshot)
        if signal:
            signal_snapshots += 1
        signal_targets = signal.get("target_prices")
        signal_target_count = (
            len(signal_targets)
            if isinstance(signal_targets, list) else
            0
        )
        if signal_target_count > 0:
            signal_target_snapshots += 1
            signal_target_total += signal_target_count

        quality = quality_payload(snapshot)
        quality_targets = quality.get("targets")
        quality_target_count = (
            len(quality_targets)
            if isinstance(quality_targets, list) else
            0
        )
        if quality_target_count > 0:
            quality_target_snapshots += 1
            quality_target_total += quality_target_count
            if signal_target_count == 0 and derived_quality_target_prices(snapshot):
                fallback_target_snapshots += 1

    return {
        "selected_signal_asset_id": SIGNAL_ASSET_ID,
        "derive_targets_from_quality": BACKTEST_DERIVE_TARGETS_FROM_QUALITY,
        "snapshots": len(snapshots),
        "snapshots_with_signal": signal_snapshots,
        "snapshots_with_signal_targets": signal_target_snapshots,
        "snapshots_with_quality_targets": quality_target_snapshots,
        "snapshots_with_quality_fallback_targets": fallback_target_snapshots,
        "snapshots_without_targets": len(snapshots) - max(
            signal_target_snapshots,
            fallback_target_snapshots
        ),
        "avg_signal_target_count": round(
            signal_target_total / signal_target_snapshots,
            4
        ) if signal_target_snapshots else 0.0,
        "avg_quality_target_count": round(
            quality_target_total / quality_target_snapshots,
            4
        ) if quality_target_snapshots else 0.0,
    }


def simulate_strategy(strategy_name, snapshots):
    summary = empty_summary()
    recent_decisions = []
    blocked_quality_markouts = []
    sentiment_risk_events = []
    trades = []
    position = None
    pending_entry = None
    cooldown_until = None
    last_sell_price = None
    last_price = None
    last_timestamp = None

    for index, snapshot in enumerate(snapshots):
        timestamp = snapshot_timestamp(snapshot)
        price = extract_price(snapshot)
        signal, target_fallback_used = signal_payload_with_target_fallback(snapshot)

        if timestamp is None:
            continue
        last_timestamp = timestamp
        if price is not None:
            last_price = price

        if pending_entry is not None:
            if price is not None and price <= pending_entry["buy_target"]["buy_price"]:
                position = {
                    **pending_entry,
                    "filled_at": timestamp,
                    "entry_price": pending_entry["buy_target"]["buy_price"],
                    "high_water": pending_entry["buy_target"]["buy_price"],
                    "low_water": pending_entry["buy_target"]["buy_price"],
                    "exit_deadline": timestamp + timedelta(hours=BACKTEST_MAX_HOLD_HOURS)
                }
                pending_entry = None
            elif timestamp > pending_entry["fill_deadline"]:
                summary["not_filled"] += 1
                pending_entry = None

        if position is not None and price is not None:
            position["high_water"] = max(position["high_water"], price)
            position["low_water"] = min(position["low_water"], price)
            tp_price, sl_price = exit_prices(
                position["entry_price"],
                position["target_profit_pct"],
                position["fee_bps"]
            )
            exit_reason = None
            exit_price = None

            if price >= tp_price:
                exit_reason = "take_profit"
                exit_price = tp_price
                summary["take_profit_count"] += 1
            elif (
                not strategy_uses_profit_only_exit(strategy_name)
                and price <= sl_price
            ):
                exit_reason = "stop_loss"
                exit_price = sl_price
                summary["stop_loss_count"] += 1
            elif (
                not strategy_uses_profit_only_exit(strategy_name)
                and timestamp >= position["exit_deadline"]
            ):
                exit_reason = "timeout"
                exit_price = price
                summary["timeout_count"] += 1

            if exit_reason is not None:
                hold_minutes = (
                    timestamp - position["filled_at"]
                ).total_seconds() / 60.0
                gross_return_pct, net_return_pct, max_runup_pct, max_drawdown_pct = (
                    compute_trade_stats(
                        position["entry_price"],
                        exit_price,
                        position["high_water"],
                        position["low_water"],
                        position["fee_bps"]
                    )
                )
                trade = {
                    "strategy": strategy_name,
                    "decision_time": position["decision_time"].isoformat(),
                    "signal_timestamp": position["signal_timestamp"],
                    "decision_price": position["decision_price"],
                    "buy_target": position["buy_target"],
                    "filled_at": position["filled_at"].isoformat(),
                    "entry_price": position["entry_price"],
                    "exit_time": timestamp.isoformat(),
                    "exit_price": round(exit_price, 2),
                    "exit_reason": exit_reason,
                    "fee_bps": round(position["fee_bps"], 6),
                    "gross_return_pct": round(gross_return_pct, 6),
                    "net_return_pct": round(net_return_pct, 6),
                    "max_runup_pct": round(max_runup_pct, 6),
                    "max_drawdown_pct": round(max_drawdown_pct, 6),
                    "hold_minutes": round(hold_minutes, 2),
                    "execution_signal": position["execution_signal"],
                    "confidence": position["confidence"],
                    "contributor_count": position["contributor_count"],
                    "policy": position["policy"],
                    **sentiment_risk_fields_from_record(position),
                }
                trades.append(trade)
                last_sell_price = exit_price
                cooldown_until = timestamp + timedelta(minutes=BACKTEST_COOLDOWN_MINUTES)
                position = None

        if position is not None or pending_entry is not None:
            summary["skipped_during_position"] += 1
            continue

        if cooldown_until is not None and timestamp < cooldown_until:
            summary["skipped_during_position"] += 1
            continue

        if price is None:
            summary["missing_price"] += 1
            continue

        if not signal:
            summary["missing_signal"] += 1
            continue

        targets = signal.get("target_prices")
        if isinstance(targets, list) and targets:
            if target_fallback_used:
                summary["quality_fallback_target_snapshots"] += 1
            else:
                summary["signal_target_snapshots"] += 1

        candidate = select_target_candidate(
            snapshot,
            price,
            last_sell_price,
            signal=signal
        )
        if candidate is None:
            summary["no_target"] += 1
            continue

        summary["raw_candidates"] += 1
        risk_fields = sentiment_risk_event_fields(signal)
        sentiment_risk_events.append(risk_fields)
        decision = quality_decision(
            snapshot,
            candidate,
            strategy_name,
            current_price=price
        )
        policy = decision["policy"]
        if strategy_name != "price_target_only" and not decision["allowed"]:
            if decision["reason"] == "blocked":
                summary["blocked_by_sentiment"] += 1
                shadow_quality = None
                if strategy_name == "with_target_quality":
                    shadow_quality = quality_decision(
                        snapshot,
                        candidate,
                        strategy_name,
                        ignore_sentiment=True,
                        current_price=price
                    )
                    if shadow_quality["allowed"]:
                        summary["shadow_target_quality_approved"] += 1
                        markout = simulate_candidate_markout(
                            snapshots,
                            index,
                            strategy_name=strategy_name,
                            decision_time=timestamp,
                            signal_timestamp=signal.get("processed_at"),
                            decision_price=price,
                            buy_target=candidate,
                            profit_target_pct=(
                                shadow_quality["profit_target_pct"]
                                if shadow_quality["profit_target_pct"] is not None else
                                float(strategy_value(snapshot, "target_profit_pct", 0.005))
                            ),
                            fee_bps=effective_fee_bps(snapshot),
                            signal=signal,
                            policy=shadow_quality["policy"],
                        )
                        blocked_quality_markouts.append(markout)
                    elif shadow_quality["reason"].startswith("target_quality_unavailable"):
                        summary["shadow_target_quality_unavailable"] += 1
                    else:
                        summary["shadow_target_quality_rejected"] += 1
            else:
                summary["blocked_by_target_quality"] += 1
                shadow_quality = None
            decision_record = {
                "strategy": strategy_name,
                "decision_time": timestamp.isoformat(),
                "signal_timestamp": signal.get("processed_at"),
                "decision_price": price,
                "buy_target": candidate,
                "policy": policy,
                **risk_fields,
            }
            if shadow_quality is not None:
                decision_record["shadow_target_quality"] = {
                    "allowed": shadow_quality["allowed"],
                    "reason": shadow_quality["reason"],
                    "policy": shadow_quality["policy"],
                    "profit_target_pct": shadow_quality["profit_target_pct"],
                }
            recent_decisions.append({
                **decision_record
            })
            continue

        summary["approved_candidates"] += 1
        summary["candidate_signals"] += 1
        profit_target_pct = decision["profit_target_pct"]
        override_profit_target_pct = strategy_profit_target_override(strategy_name)
        if override_profit_target_pct is not None:
            profit_target_pct = override_profit_target_pct
        if profit_target_pct is None:
            profit_target_pct = float(
                strategy_value(snapshot, "target_profit_pct", 0.005)
            )

        pending_entry = {
            "strategy": strategy_name,
            "decision_time": timestamp,
            "signal_timestamp": signal.get("processed_at"),
            "decision_price": price,
            "buy_target": candidate,
            "policy": policy,
            "fill_deadline": timestamp + timedelta(hours=BACKTEST_ENTRY_WAIT_HOURS),
            "target_profit_pct": profit_target_pct,
            "fee_bps": effective_fee_bps(snapshot),
            "execution_signal": numeric_or_none(signal.get("execution_signal")),
            "confidence": numeric_or_none(signal.get("confidence")),
            "contributor_count": signal.get("contributor_count"),
            **risk_fields,
        }
        recent_decisions.append({
            "strategy": strategy_name,
            "decision_time": timestamp.isoformat(),
            "signal_timestamp": signal.get("processed_at"),
            "decision_price": price,
            "buy_target": candidate,
            "policy": policy,
            **risk_fields,
        })

    recent_decisions = recent_decisions[-BACKTEST_RECENT_LIMIT:]
    recent_trades = trades[-BACKTEST_RECENT_LIMIT:]
    recent_blocked_quality_markouts = blocked_quality_markouts[-BACKTEST_RECENT_LIMIT:]
    open_positions = []
    open_position = finalize_open_position(position, last_price, last_timestamp)
    if open_position is not None:
        open_positions.append(open_position)
    apply_blocked_quality_markout_summary(summary, blocked_quality_markouts)
    apply_sentiment_risk_summary(summary, sentiment_risk_events)
    return {
        "summary": finalize_summary(summary, trades, open_positions),
        "recent_decisions": recent_decisions,
        "recent_trades": recent_trades,
        "recent_sentiment_blocked_quality_markouts": recent_blocked_quality_markouts,
        "open_positions": open_positions
    }


def top_summary(strategies):
    summary_rows = []
    for name, payload in strategies.items():
        summary = payload["summary"]
        total_net = summary.get("total_net_return_pct")
        trades = summary.get("trades", 0)
        if trades > 0 and total_net is not None:
            score = (1, total_net, summary.get("win_rate") or 0.0)
        else:
            score = (0, float("-inf"), float("-inf"))
        summary_rows.append((score, name, summary))

    best_score, best_name, best_summary = max(summary_rows, key=lambda item: item[0])
    best_strategy = None
    best_strategy_reason = "No strategy produced any completed trades in this window."
    if best_score[0] > 0:
        best_strategy = best_name
        marked_to_market_net = best_summary.get("marked_to_market_net_return_pct")
        best_strategy_reason = (
            f"Best completed-trade result by total net return over the window: "
            f"{best_summary['total_net_return_pct']}% across {best_summary['trades']} trades."
        )
        if (
            best_summary.get("open_position_count", 0) > 0
            and marked_to_market_net is not None
        ):
            best_strategy_reason += (
                f" Marked-to-market including open positions: "
                f"{marked_to_market_net}%."
            )
        if best_summary["total_net_return_pct"] < 0:
            best_strategy_reason += " No-trade outperformed all completed-trade strategies."
        elif marked_to_market_net is not None and marked_to_market_net < 0:
            best_strategy_reason += " Open exposure made no-trade better on a marked-to-market basis."

    return {
        "best_strategy": best_strategy,
        "best_strategy_reason": best_strategy_reason,
        "strategy_headlines": {
            name: {
                "raw_candidates": payload["summary"].get("raw_candidates"),
                "approved_candidates": payload["summary"].get("approved_candidates"),
                "trades": payload["summary"].get("trades"),
                "open_position_count": payload["summary"].get(
                    "open_position_count"
                ),
                "open_position_unrealized_net_pct": payload["summary"].get(
                    "open_position_unrealized_net_pct"
                ),
                "win_rate": payload["summary"].get("win_rate"),
                "total_net_return_pct": payload["summary"].get("total_net_return_pct"),
                "marked_to_market_net_return_pct": payload["summary"].get(
                    "marked_to_market_net_return_pct"
                ),
                "sentiment_saved_quality_candidates": payload["summary"].get(
                    "sentiment_saved_quality_candidates"
                ),
                "sentiment_saved_quality_candidate_rate": payload["summary"].get(
                    "sentiment_saved_quality_candidate_rate"
                ),
                "sentiment_blocked_quality_markout_count": payload["summary"].get(
                    "sentiment_blocked_quality_markout_count"
                ),
                "sentiment_blocked_quality_filled": payload["summary"].get(
                    "sentiment_blocked_quality_filled"
                ),
                "sentiment_blocked_quality_not_filled": payload["summary"].get(
                    "sentiment_blocked_quality_not_filled"
                ),
                "sentiment_blocked_quality_take_profit": payload["summary"].get(
                    "sentiment_blocked_quality_take_profit"
                ),
                "sentiment_blocked_quality_stop_loss": payload["summary"].get(
                    "sentiment_blocked_quality_stop_loss"
                ),
                "sentiment_blocked_quality_open": payload["summary"].get(
                    "sentiment_blocked_quality_open"
                ),
                "sentiment_blocked_quality_total_net_return_pct": payload["summary"].get(
                    "sentiment_blocked_quality_total_net_return_pct"
                ),
                "sentiment_blocked_quality_marked_to_market_net_return_pct": payload["summary"].get(
                    "sentiment_blocked_quality_marked_to_market_net_return_pct"
                ),
                "sentiment_risk_sample_count": payload["summary"].get(
                    "sentiment_risk_sample_count"
                ),
                "sentiment_risk_context_present_count": payload["summary"].get(
                    "sentiment_risk_context_present_count"
                ),
                "sentiment_risk_context_missing_count": payload["summary"].get(
                    "sentiment_risk_context_missing_count"
                ),
                "sentiment_risk_context_coverage_pct": payload["summary"].get(
                    "sentiment_risk_context_coverage_pct"
                ),
                "sentiment_risk_numeric_sample_count": payload["summary"].get(
                    "sentiment_risk_numeric_sample_count"
                ),
                "sentiment_risk_posture_present_count": payload["summary"].get(
                    "sentiment_risk_posture_present_count"
                ),
                "sentiment_risk_posture_missing_count": payload["summary"].get(
                    "sentiment_risk_posture_missing_count"
                ),
                "sentiment_risk_posture_counts": payload["summary"].get(
                    "sentiment_risk_posture_counts"
                ),
                "sentiment_hard_safety_flag_counts": payload["summary"].get(
                    "sentiment_hard_safety_flag_counts"
                ),
                "sentiment_weather_report_present_count": payload["summary"].get(
                    "sentiment_weather_report_present_count"
                ),
                "sentiment_weather_report_missing_count": payload["summary"].get(
                    "sentiment_weather_report_missing_count"
                ),
                "sentiment_weather_report_coverage_pct": payload["summary"].get(
                    "sentiment_weather_report_coverage_pct"
                ),
                "sentiment_weather_emergency_bell_count": payload["summary"].get(
                    "sentiment_weather_emergency_bell_count"
                ),
                "sentiment_weather_condition_counts": payload["summary"].get(
                    "sentiment_weather_condition_counts"
                ),
                "sentiment_weather_alert_level_counts": payload["summary"].get(
                    "sentiment_weather_alert_level_counts"
                ),
                "sentiment_weather_trade_permission_counts": payload["summary"].get(
                    "sentiment_weather_trade_permission_counts"
                ),
                "sentiment_weather_opportunity_tag_counts": payload["summary"].get(
                    "sentiment_weather_opportunity_tag_counts"
                ),
                "sentiment_weather_risk_warning_counts": payload["summary"].get(
                    "sentiment_weather_risk_warning_counts"
                ),
                **{
                    f"avg_{field}": payload["summary"].get(f"avg_{field}")
                    for field in (
                        list(RISK_CONTEXT_NUMERIC_FIELDS.values())
                        + list(WEATHER_BOT_TUNING_NUMERIC_FIELDS.values())
                    )
                },
                "no_target": payload["summary"].get("no_target"),
                "not_filled": payload["summary"].get("not_filled"),
                "signal_target_snapshots": payload["summary"].get(
                    "signal_target_snapshots"
                ),
                "quality_fallback_target_snapshots": payload["summary"].get(
                    "quality_fallback_target_snapshots"
                ),
                "fill_rate_after_approval": payload["summary"].get(
                    "fill_rate_after_approval"
                ),
                "terminal_rate_after_approval": payload["summary"].get(
                    "terminal_rate_after_approval"
                ),
            }
            for name, payload in strategies.items()
        }
    }


def strategy_comparison_score(row):
    trades = row.get("trades") or 0
    approved = row.get("approved_candidates") or 0
    raw_candidates = row.get("raw_candidates") or 0
    win_rate = row.get("win_rate") or 0.0
    total_net = row.get("total_net_return_pct")
    marked_to_market = row.get("marked_to_market_net_return_pct")
    open_count = row.get("open_position_count") or 0
    fill_rate = row.get("fill_rate_after_approval") or 0.0
    terminal_rate = row.get("terminal_rate_after_approval") or 0.0
    candidate_efficiency = approved / raw_candidates if raw_candidates else 0.0

    no_exposure = trades == 0 and open_count == 0
    if no_exposure and approved == 0:
        return 0.0
    if no_exposure and approved > 0:
        return round(-float(approved), 6)

    total_net = total_net if total_net is not None else 0.0
    marked_to_market = marked_to_market if marked_to_market is not None else total_net
    if marked_to_market <= 0:
        score = (
            (marked_to_market * 20.0)
            + (total_net * 6.0)
            + (win_rate * 5.0)
            + (fill_rate * 2.0)
            + (terminal_rate * 2.0)
            - (open_count * 8.0)
        )
        return round(score, 6)

    score = (
        (trades * 8.0)
        + (approved * 1.0)
        + (win_rate * 20.0)
        + (max(total_net, 0.0) * 8.0)
        + (marked_to_market * 12.0)
        + (fill_rate * 5.0)
        + (terminal_rate * 5.0)
        + (candidate_efficiency * 10.0)
        - (open_count * 8.0)
    )
    return round(score, 6)


def bool_config(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def production_eligibility(strategy_payload, variant):
    reasons = []
    if bool_config(strategy_payload.get("probe_only"), False):
        reasons.append("probe_only")
    if str(variant).startswith("price_target_only"):
        reasons.append("price_target_only_variant")
    if bool_config(strategy_payload.get("target_quality_enabled"), True) is False:
        reasons.append("target_quality_disabled")

    return {
        "production_eligible": not reasons,
        "production_ineligible_reason": ",".join(reasons) if reasons else None,
    }


def build_strategy_comparison_rows(snapshots, strategy_set_file):
    rows = []
    details = []
    for entry in load_strategy_set(strategy_set_file):
        strategy_path = entry["path"]
        try:
            strategy_payload = load_json_file(strategy_path)
        except Exception as e:
            rows.append({
                "strategy_label": entry["label"],
                "strategy_file": strategy_path,
                "error": str(e),
            })
            continue

        variant = str(
            strategy_payload.get("backtest_strategy_variant")
            or "with_target_quality"
        )
        eligibility = production_eligibility(strategy_payload, variant)
        variant_snapshots = [
            snapshot_with_strategy(snapshot, strategy_path, strategy_payload)
            for snapshot in snapshots
        ]
        result = simulate_strategy(variant, variant_snapshots)
        summary = result["summary"]
        row = {
            "strategy_label": entry["label"],
            "strategy_file": strategy_path,
            "backtest_strategy_variant": variant,
            "production_eligible": eligibility["production_eligible"],
            "production_ineligible_reason": eligibility[
                "production_ineligible_reason"
            ],
            "target_profit_pct": strategy_payload.get("target_profit_pct"),
            "target_quality_enabled": strategy_payload.get("target_quality_enabled"),
            "target_quality_fail_closed": strategy_payload.get("target_quality_fail_closed"),
            "target_quality_min_samples": strategy_payload.get("target_quality_min_samples"),
            "target_quality_min_ev_pct": strategy_payload.get("target_quality_min_ev_pct"),
            "target_quality_min_4h_fill_probability": strategy_payload.get("target_quality_min_4h_fill_probability"),
            "target_quality_allowed_recommendations": strategy_payload.get("target_quality_allowed_recommendations"),
            "sentiment_discount_watch_pct": strategy_payload.get("sentiment_discount_watch_pct"),
            "sentiment_discount_bearish_pct": strategy_payload.get("sentiment_discount_bearish_pct"),
            "raw_candidates": summary.get("raw_candidates"),
            "approved_candidates": summary.get("approved_candidates"),
            "trades": summary.get("trades"),
            "win_rate": summary.get("win_rate"),
            "total_net_return_pct": summary.get("total_net_return_pct"),
            "marked_to_market_net_return_pct": summary.get("marked_to_market_net_return_pct"),
            "open_position_count": summary.get("open_position_count"),
            "open_position_unrealized_net_pct": summary.get("open_position_unrealized_net_pct"),
            "take_profit_count": summary.get("take_profit_count"),
            "stop_loss_count": summary.get("stop_loss_count"),
            "timeout_count": summary.get("timeout_count"),
            "blocked_by_sentiment": summary.get("blocked_by_sentiment"),
            "blocked_by_target_quality": summary.get("blocked_by_target_quality"),
            "shadow_target_quality_approved": summary.get("shadow_target_quality_approved"),
            "shadow_target_quality_rejected": summary.get("shadow_target_quality_rejected"),
            "sentiment_saved_quality_candidates": summary.get("sentiment_saved_quality_candidates"),
            "sentiment_saved_quality_candidate_rate": summary.get("sentiment_saved_quality_candidate_rate"),
            "sentiment_blocked_quality_markout_count": summary.get("sentiment_blocked_quality_markout_count"),
            "sentiment_blocked_quality_filled": summary.get("sentiment_blocked_quality_filled"),
            "sentiment_blocked_quality_not_filled": summary.get("sentiment_blocked_quality_not_filled"),
            "sentiment_blocked_quality_take_profit": summary.get("sentiment_blocked_quality_take_profit"),
            "sentiment_blocked_quality_stop_loss": summary.get("sentiment_blocked_quality_stop_loss"),
            "sentiment_blocked_quality_timeout": summary.get("sentiment_blocked_quality_timeout"),
            "sentiment_blocked_quality_open": summary.get("sentiment_blocked_quality_open"),
            "sentiment_blocked_quality_win_rate": summary.get("sentiment_blocked_quality_win_rate"),
            "sentiment_blocked_quality_total_net_return_pct": summary.get("sentiment_blocked_quality_total_net_return_pct"),
            "sentiment_blocked_quality_marked_to_market_net_return_pct": summary.get("sentiment_blocked_quality_marked_to_market_net_return_pct"),
            "sentiment_risk_sample_count": summary.get("sentiment_risk_sample_count"),
            "sentiment_risk_context_present_count": summary.get("sentiment_risk_context_present_count"),
            "sentiment_risk_context_missing_count": summary.get("sentiment_risk_context_missing_count"),
            "sentiment_risk_context_coverage_pct": summary.get("sentiment_risk_context_coverage_pct"),
            "sentiment_risk_numeric_sample_count": summary.get("sentiment_risk_numeric_sample_count"),
            "sentiment_risk_posture_present_count": summary.get("sentiment_risk_posture_present_count"),
            "sentiment_risk_posture_missing_count": summary.get("sentiment_risk_posture_missing_count"),
            "sentiment_risk_posture_counts": json.dumps(
                summary.get("sentiment_risk_posture_counts") or {},
                sort_keys=True
            ),
            "sentiment_hard_safety_flag_counts": json.dumps(
                summary.get("sentiment_hard_safety_flag_counts") or {},
                sort_keys=True
            ),
            "sentiment_weather_report_present_count": summary.get("sentiment_weather_report_present_count"),
            "sentiment_weather_report_missing_count": summary.get("sentiment_weather_report_missing_count"),
            "sentiment_weather_report_coverage_pct": summary.get("sentiment_weather_report_coverage_pct"),
            "sentiment_weather_emergency_bell_count": summary.get("sentiment_weather_emergency_bell_count"),
            "sentiment_weather_condition_counts": json.dumps(
                summary.get("sentiment_weather_condition_counts") or {},
                sort_keys=True
            ),
            "sentiment_weather_alert_level_counts": json.dumps(
                summary.get("sentiment_weather_alert_level_counts") or {},
                sort_keys=True
            ),
            "sentiment_weather_trade_permission_counts": json.dumps(
                summary.get("sentiment_weather_trade_permission_counts") or {},
                sort_keys=True
            ),
            "sentiment_weather_opportunity_tag_counts": json.dumps(
                summary.get("sentiment_weather_opportunity_tag_counts") or {},
                sort_keys=True
            ),
            "sentiment_weather_risk_warning_counts": json.dumps(
                summary.get("sentiment_weather_risk_warning_counts") or {},
                sort_keys=True
            ),
            **{
                f"avg_{field}": summary.get(f"avg_{field}")
                for field in (
                    list(RISK_CONTEXT_NUMERIC_FIELDS.values())
                    + list(WEATHER_BOT_TUNING_NUMERIC_FIELDS.values())
                )
            },
            "no_target": summary.get("no_target"),
            "not_filled": summary.get("not_filled"),
            "fill_rate_after_approval": summary.get("fill_rate_after_approval"),
            "terminal_rate_after_approval": summary.get("terminal_rate_after_approval"),
        }
        row["candidate_efficiency"] = round(
            (row["approved_candidates"] or 0) / (row["raw_candidates"] or 1),
            6,
        ) if row.get("raw_candidates") else 0.0
        row["practical_score"] = strategy_comparison_score(row)
        rows.append(row)
        details.append({
            "strategy_label": entry["label"],
            "strategy_file": strategy_path,
            "strategy_payload": strategy_payload,
            "backtest_strategy_variant": variant,
            "summary": summary,
            "recent_decisions": result.get("recent_decisions", []),
            "recent_trades": result.get("recent_trades", []),
            "recent_sentiment_blocked_quality_markouts": result.get(
                "recent_sentiment_blocked_quality_markouts", []
            ),
            "open_positions": result.get("open_positions", []),
        })

    rows.sort(
        key=lambda row: (
            -(row.get("practical_score") or -999999),
            -(row.get("approved_candidates") or 0),
            -((row.get("marked_to_market_net_return_pct") or -999999)),
            row.get("strategy_label") or "",
        )
    )
    return {
        "strategy_set_file": resolve_repo_path(strategy_set_file),
        "count": len(rows),
        "rows": rows,
        "details": details,
    }


def write_strategy_comparison_csv(comparison, output_path, ranked=False):
    rows = comparison.get("rows") or []
    if not rows:
        return None

    output_file = resolve_repo_path(output_path)
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    fieldnames = [
        "strategy_label",
        "practical_score",
        "production_eligible",
        "production_ineligible_reason",
        "backtest_strategy_variant",
        "trades",
        "win_rate",
        "total_net_return_pct",
        "marked_to_market_net_return_pct",
        "open_position_count",
        "approved_candidates",
        "candidate_efficiency",
        "raw_candidates",
        "fill_rate_after_approval",
        "terminal_rate_after_approval",
        "take_profit_count",
        "stop_loss_count",
        "timeout_count",
        "blocked_by_sentiment",
        "blocked_by_target_quality",
        "shadow_target_quality_approved",
        "shadow_target_quality_rejected",
        "sentiment_saved_quality_candidates",
        "sentiment_saved_quality_candidate_rate",
        "sentiment_blocked_quality_markout_count",
        "sentiment_blocked_quality_filled",
        "sentiment_blocked_quality_not_filled",
        "sentiment_blocked_quality_take_profit",
        "sentiment_blocked_quality_stop_loss",
        "sentiment_blocked_quality_timeout",
        "sentiment_blocked_quality_open",
        "sentiment_blocked_quality_win_rate",
        "sentiment_blocked_quality_total_net_return_pct",
        "sentiment_blocked_quality_marked_to_market_net_return_pct",
        "sentiment_risk_sample_count",
        "sentiment_risk_context_present_count",
        "sentiment_risk_context_missing_count",
        "sentiment_risk_context_coverage_pct",
        "sentiment_risk_numeric_sample_count",
        "sentiment_risk_posture_present_count",
        "sentiment_risk_posture_missing_count",
        "sentiment_risk_posture_counts",
        "sentiment_hard_safety_flag_counts",
        "sentiment_weather_report_present_count",
        "sentiment_weather_report_missing_count",
        "sentiment_weather_report_coverage_pct",
        "sentiment_weather_emergency_bell_count",
        "sentiment_weather_condition_counts",
        "sentiment_weather_alert_level_counts",
        "sentiment_weather_trade_permission_counts",
        "sentiment_weather_opportunity_tag_counts",
        "sentiment_weather_risk_warning_counts",
        *[
            f"avg_{field}"
            for field in (
                list(RISK_CONTEXT_NUMERIC_FIELDS.values())
                + list(WEATHER_BOT_TUNING_NUMERIC_FIELDS.values())
            )
        ],
        "no_target",
        "not_filled",
        "target_profit_pct",
        "target_quality_enabled",
        "target_quality_fail_closed",
        "target_quality_min_samples",
        "target_quality_min_ev_pct",
        "target_quality_min_4h_fill_probability",
        "target_quality_allowed_recommendations",
        "sentiment_discount_watch_pct",
        "sentiment_discount_bearish_pct",
        "strategy_file",
    ]
    output_rows = list(rows)
    if ranked:
        output_rows.sort(
            key=lambda row: (
                -(row.get("practical_score") or -999999),
                -(row.get("approved_candidates") or 0),
                -((row.get("marked_to_market_net_return_pct") or -999999)),
                row.get("strategy_label") or "",
            )
        )

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in output_rows:
            writer.writerow({field: row.get(field) for field in fieldnames})
    return output_file


def build_report():
    now = now_utc()
    since_dt = now - timedelta(hours=BACKTEST_WINDOW_HOURS)
    expected_snapshot_files = expected_snapshot_source_files(
        SNAPSHOT_LOG_FILE,
        since_dt,
        now
    )
    snapshot_files = snapshot_source_files(SNAPSHOT_LOG_FILE, since_dt, now)
    all_snapshots = []
    for path in snapshot_files:
        all_snapshots.extend(load_jsonl(path))
    snapshots = [
        snapshot
        for snapshot in all_snapshots
        if (snapshot_timestamp(snapshot) or datetime.min.replace(tzinfo=timezone.utc)) >= since_dt
    ]
    snapshots.sort(key=lambda snapshot: snapshot_timestamp(snapshot) or datetime.min.replace(tzinfo=timezone.utc))
    filtered_out_by_window = len(all_snapshots) - len(snapshots)

    strategies = {
        name: simulate_strategy(name, snapshots)
        for name in BACKTEST_STRATEGIES
    }
    strategy_comparison = None
    if BACKTEST_STRATEGY_SET_FILE:
        strategy_comparison = build_strategy_comparison_rows(
            snapshots,
            BACKTEST_STRATEGY_SET_FILE,
        )

    report = {
        "timestamp": now.isoformat(),
        "resolved_inputs": {
            "env_file": os.path.abspath(ENV_FILE),
            "env_file_exists": os.path.exists(ENV_FILE),
            "snapshot_file_base": os.path.abspath(SNAPSHOT_LOG_FILE),
            "snapshot_dir": os.path.dirname(os.path.abspath(SNAPSHOT_LOG_FILE)) or ".",
            "snapshot_basename": os.path.basename(SNAPSHOT_LOG_FILE),
            "rotate_daily": SNAPSHOT_ROTATE_DAILY,
            "strategy_set_file": (
                resolve_repo_path(BACKTEST_STRATEGY_SET_FILE)
                if BACKTEST_STRATEGY_SET_FILE else
                None
            ),
            "output_file": os.path.abspath(BACKTEST_OUTPUT_FILE),
            "archive_dir": os.path.abspath(BACKTEST_ARCHIVE_DIR),
            "strategy_comparison_csv_file": resolve_repo_path(
                BACKTEST_STRATEGY_COMPARE_CSV_FILE
            ),
            "strategy_ranked_csv_file": resolve_repo_path(
                BACKTEST_STRATEGY_RANKED_CSV_FILE
            ),
        },
        "snapshot_file": os.path.abspath(SNAPSHOT_LOG_FILE),
        "snapshot_files": snapshot_files,
        "snapshot_diagnostics": {
            "rotate_daily": SNAPSHOT_ROTATE_DAILY,
            "expected_snapshot_files": expected_snapshot_files,
            "found_snapshot_files": snapshot_files,
            "expected_file_metadata": snapshot_file_metadata(expected_snapshot_files),
            "loaded_snapshot_count": len(all_snapshots),
            "filtered_out_by_window": filtered_out_by_window,
            "empty_window_reason": (
                "no snapshot files found"
                if not snapshot_files else
                "snapshot files had no rows"
                if not all_snapshots else
                "all loaded snapshots were older than the report window"
                if not snapshots else
                None
            ),
        },
        "target_diagnostics": target_diagnostics(snapshots),
        "since": since_dt.isoformat(),
        "snapshot_count": len(snapshots),
        "simulation": {
            "side": "long_only",
            "clock": "snapshot timestamps",
            "entry": "limit buy at highest eligible llm target below current price",
            "entry_wait_hours": BACKTEST_ENTRY_WAIT_HOURS,
            "stop_loss_pct": BACKTEST_STOP_LOSS_PCT,
            "max_hold_hours": BACKTEST_MAX_HOLD_HOURS,
            "cooldown_minutes": BACKTEST_COOLDOWN_MINUTES,
            "fee_bps": report_fee_bps(snapshots),
            "fee_source": (
                "LLM_TARGET_BACKTEST_FEE_BPS"
                if BACKTEST_FEE_BPS > 0 else
                "strategy_profile.round_trip_fee_pct"
            ),
            "strategy_variants": {
                name: options
                for name, options in BACKTEST_STRATEGIES.items()
                if options
            },
            "sentiment_discount_watch_pct": BACKTEST_SENTIMENT_DISCOUNT_WATCH_PCT,
            "sentiment_discount_bearish_pct": BACKTEST_SENTIMENT_DISCOUNT_BEARISH_PCT,
        },
        "top_summary": top_summary(strategies),
        "strategies": strategies,
        "bot_outputs": {
            name: payload["summary"]
            for name, payload in strategies.items()
        }
    }
    if strategy_comparison is not None:
        report["strategy_comparison"] = strategy_comparison
    return report


def write_report(report):
    output_dir = os.path.dirname(BACKTEST_OUTPUT_FILE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    comparison_csv_file = None
    ranked_comparison_csv_file = None
    if report.get("strategy_comparison"):
        comparison_csv_file = write_strategy_comparison_csv(
            report["strategy_comparison"],
            BACKTEST_STRATEGY_COMPARE_CSV_FILE,
        )
        ranked_comparison_csv_file = write_strategy_comparison_csv(
            report["strategy_comparison"],
            BACKTEST_STRATEGY_RANKED_CSV_FILE,
            ranked=True,
        )
    report["written_outputs"] = {
        "strategy_comparison_csv_file": comparison_csv_file,
        "strategy_ranked_csv_file": ranked_comparison_csv_file,
    }

    with open(BACKTEST_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    os.makedirs(BACKTEST_ARCHIVE_DIR, exist_ok=True)
    archive_file = os.path.join(
        BACKTEST_ARCHIVE_DIR,
        f"llm_target_backtest_{now_utc().strftime('%Y%m%d')}.json"
    )
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return archive_file


def main():
    report = build_report()
    archive_file = write_report(report)
    written_outputs = report.get("written_outputs", {})
    print(json.dumps({
        "timestamp": report["timestamp"],
        "output_file": BACKTEST_OUTPUT_FILE,
        "archive_file": archive_file,
        "snapshot_count": report["snapshot_count"],
        "strategy_comparison_csv_file": written_outputs.get(
            "strategy_comparison_csv_file"
        ),
        "strategy_ranked_csv_file": written_outputs.get(
            "strategy_ranked_csv_file"
        ),
    }))


if __name__ == "__main__":
    main()
