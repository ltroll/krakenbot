#!/usr/bin/env python3

import argparse
import copy
import csv
import json
import os
import statistics
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from signal_normalizer import normalize_signal_payload


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

SNAPSHOT_LOG_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_SNAPSHOT_FILE",
    "range_grid_backtest_snapshot_log.jsonl"
)
SNAPSHOT_ROTATE_DAILY = os.getenv(
    "RANGE_GRID_BACKTEST_ROTATE_DAILY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")
TRADE_LOG_FILE = (
    os.getenv("RANGE_GRID_TRADE_LOG_FILE")
    or os.getenv("TRADE_LOG_FILE")
    or "trade_log.jsonl"
)
ACTIVITY_LOG_FILE = (
    os.getenv("RANGE_GRID_ACTIVITY_LOG_FILE")
    or ""
).strip()
ACTIVITY_LOG_ROTATE_DAILY = os.getenv(
    "RANGE_GRID_ACTIVITY_LOG_ROTATE_DAILY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")
BACKTEST_HTTP_TIMEOUT_SECONDS = float(os.getenv(
    "RANGE_GRID_BACKTEST_HTTP_TIMEOUT_SECONDS",
    "10"
))
BACKTEST_OUTPUT_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_OUTPUT_FILE",
    "/var/www/html/bot/range_grid_backtest.json"
)
BACKTEST_ARCHIVE_DIR = os.getenv(
    "RANGE_GRID_BACKTEST_ARCHIVE_DIR",
    "/var/www/html/bot/range_grid_backtest"
)
BACKTEST_WINDOW_HOURS = float(os.getenv("RANGE_GRID_BACKTEST_WINDOW_HOURS", "24"))
BACKTEST_RECENT_LIMIT = int(os.getenv("RANGE_GRID_BACKTEST_RECENT_LIMIT", "25"))
BACKTEST_POTENTIAL_MAX_HOLD_HOURS = float(
    os.getenv("RANGE_GRID_BACKTEST_POTENTIAL_MAX_HOLD_HOURS", "24")
)
BACKTEST_STRATEGY_SET_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_STRATEGY_SET_FILE",
    ""
).strip()
BACKTEST_STRATEGY_COMPARE_CSV_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_STRATEGY_COMPARE_CSV_FILE",
    "/var/www/html/bot/range_grid_backtest_strategy_compare.csv"
)
BACKTEST_STRATEGY_RANKED_CSV_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_STRATEGY_RANKED_CSV_FILE",
    "/var/www/html/bot/range_grid_backtest_strategy_ranked.csv"
)
BACKTEST_ANCHOR_WINNERS_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_ANCHOR_WINNERS_FILE",
    "/var/www/html/bot/range_grid_anchor_winners.json"
)
BACKTEST_ANCHOR_WINNER_MIN_APPROVED = int(os.getenv(
    "RANGE_GRID_BACKTEST_ANCHOR_WINNER_MIN_APPROVED",
    "1"
))
BACKTEST_ANCHOR_WINNER_MIN_AVG_END_RETURN_PCT = float(os.getenv(
    "RANGE_GRID_BACKTEST_ANCHOR_WINNER_MIN_AVG_END_RETURN_PCT",
    "0"
))
BACKTEST_ANCHOR_WINNER_MAX_AVG_DRAWDOWN_PCT = float(os.getenv(
    "RANGE_GRID_BACKTEST_ANCHOR_WINNER_MAX_AVG_DRAWDOWN_PCT",
    "-3"
))
BACKTEST_ANCHOR_WINNER_STRATEGY_DIR = os.getenv(
    "RANGE_GRID_BACKTEST_ANCHOR_WINNER_STRATEGY_DIR",
    ""
).strip()

ANCHOR_WINNER_SOURCES = {
    "low": "approved_range_low",
    "median": "approved_range_median",
    "high": "approved_range_high_band",
}


def now_utc():
    return datetime.now(timezone.utc)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Replay and summarize range grid backtest snapshots."
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        default=None,
        help="Override the backtest lookback window in hours.",
    )
    return parser.parse_args(argv)


def parse_iso8601(value):
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except Exception:
        return None


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def is_http_url(value):
    text = str(value or "").strip().lower()
    return text.startswith("http://") or text.startswith("https://")


def display_source_path(path):
    if is_http_url(path):
        return path
    return os.path.abspath(path)


def load_jsonl(path):
    rows = []
    if is_http_url(path):
        try:
            with urllib.request.urlopen(
                path,
                timeout=BACKTEST_HTTP_TIMEOUT_SECONDS
            ) as response:
                lines = response.read().decode("utf-8").splitlines()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return []
            return []
        except Exception:
            return []
    else:
        if not os.path.exists(path):
            return []
        with open(path, encoding="utf-8") as f:
            lines = list(f)

    for line in lines:
        text = line.strip()
        if not text:
            continue
        try:
            rows.append(json.loads(text))
        except Exception:
            continue
    return rows


def load_json_file(path):
    if not path or not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def rotated_snapshot_path(base_path, dt):
    root, ext = os.path.splitext(base_path)
    suffix = dt.strftime("%Y%m%d")
    return f"{root}_{suffix}{ext or '.jsonl'}"


def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def rotated_source_files(base_path, since_dt, until_dt, rotate_daily):
    rotated_files = []
    if rotate_daily:
        for day in daterange(since_dt.date(), until_dt.date()):
            rotated_path = rotated_snapshot_path(
                base_path,
                datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            )
            if is_http_url(rotated_path) or os.path.exists(rotated_path):
                rotated_files.append(display_source_path(rotated_path))

    if rotated_files:
        return rotated_files

    if is_http_url(base_path) or os.path.exists(base_path):
        return [display_source_path(base_path)]

    return []


def snapshot_source_files(base_path, since_dt, until_dt):
    return rotated_source_files(
        base_path,
        since_dt,
        until_dt,
        SNAPSHOT_ROTATE_DAILY,
    )


def trade_event_source_files(since_dt, until_dt):
    if ACTIVITY_LOG_FILE:
        return rotated_source_files(
            ACTIVITY_LOG_FILE,
            since_dt,
            until_dt,
            ACTIVITY_LOG_ROTATE_DAILY,
        )
    return rotated_source_files(TRADE_LOG_FILE, since_dt, until_dt, False)


def snapshot_timestamp(snapshot):
    return parse_iso8601(snapshot.get("captured_at"))


def repo_root():
    return os.path.dirname(os.path.abspath(__file__))


def resolve_repo_path(path):
    if not path:
        return None
    expanded = os.path.expanduser(str(path).strip())
    if os.path.isabs(expanded):
        return expanded
    return os.path.abspath(os.path.join(repo_root(), expanded))


def load_strategy_set_entries(path):
    resolved = resolve_repo_path(path)
    if not resolved or not os.path.exists(resolved):
        return []

    if resolved.endswith(".json"):
        data = load_json_file(resolved)
        if isinstance(data, list):
            return [item for item in data if item]
        if isinstance(data, dict):
            items = data.get("strategies")
            if isinstance(items, list):
                return [item for item in items if item]
        return []

    entries = []
    with open(resolved, encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            entries.append(text)
    return entries


def parse_strategy_set_entry(entry):
    if isinstance(entry, dict):
        path = entry.get("path") or entry.get("file") or entry.get("strategy_file")
        label = entry.get("label") or entry.get("name")
    else:
        path = str(entry).strip()
        label = None

    resolved_path = resolve_repo_path(path)
    if not resolved_path:
        return None
    if not label:
        label = os.path.splitext(os.path.basename(resolved_path))[0]
    return {
        "label": label,
        "path": resolved_path,
    }


def load_strategy_profile_from_file(path):
    resolved = resolve_repo_path(path)
    payload = load_json_file(resolved)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid strategy profile: {path}")
    return resolved, payload


def strategy_modes_from_payload(payload):
    return parse_strategy_modes(payload.get("grid_anchor"))


def snapshot_with_strategy(snapshot, strategy_label, strategy_path, strategy_payload):
    cloned = copy.deepcopy(snapshot)
    cloned["strategy_profile"] = {
        "path": strategy_path,
        "label": strategy_label,
        "payload": copy.deepcopy(strategy_payload),
    }
    context = cloned.get("strategy_context")
    if not isinstance(context, dict):
        context = {}
    context["grid_anchor"] = strategy_payload.get("grid_anchor")
    context["strategy_modes"] = strategy_modes_from_payload(strategy_payload)
    cloned["strategy_context"] = context
    return cloned


def signal_payload(snapshot):
    signal = snapshot.get("signal") or {}
    payload = signal.get("payload")
    if not isinstance(payload, dict):
        payload = signal.get("raw_payload")
    if not isinstance(payload, dict):
        return {}
    return normalize_signal_payload(payload)


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


def risk_context_payload(signal):
    risk_context = signal.get("risk_context")
    return risk_context if isinstance(risk_context, dict) else {}


def weather_report_payload(risk_context):
    if not isinstance(risk_context, dict):
        return {}
    weather = risk_context.get("weather_report")
    return weather if isinstance(weather, dict) else {}


def weather_bot_tuning(weather_report):
    tuning = weather_report.get("bot_tuning") if isinstance(weather_report, dict) else {}
    return tuning if isinstance(tuning, dict) else {}


def weather_list(value):
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def weather_bot_decides(weather_report):
    if not isinstance(weather_report, dict):
        return False
    return (
        weather_report.get("mode") == "weather_report"
        and weather_report.get("bot_decision_authority") == "bot"
        and weather_report.get("trade_permission") == "bot_decides"
    )


def sentiment_risk_event_fields(signal):
    risk_context = risk_context_payload(signal)
    flags = risk_context.get("hard_safety_flags")
    if not isinstance(flags, list):
        flags = []
    weather = weather_report_payload(risk_context)
    bot_tuning = weather_bot_tuning(weather)
    fields = {
        "sentiment_risk_posture": risk_context.get("recommended_posture"),
        "sentiment_hard_safety_flags": flags,
        "weather_condition": weather.get("condition"),
        "weather_alert_level": weather.get("alert_level"),
        "weather_trade_permission": weather.get("trade_permission"),
        "weather_bot_decision_authority": weather.get("bot_decision_authority"),
        "weather_emergency_bell": bool(weather.get("emergency_bell")),
        "weather_opportunity_tags": weather_list(weather.get("opportunity_tags")),
        "weather_risk_warnings": weather_list(weather.get("risk_warnings")),
        "weather_position_size_multiplier": bot_tuning.get(
            "position_size_multiplier"
        ),
        "weather_grid_aggression_multiplier": bot_tuning.get(
            "grid_aggression_multiplier"
        ),
        "weather_target_profit_multiplier": bot_tuning.get(
            "target_profit_multiplier"
        ),
        "weather_entry_discount_multiplier": bot_tuning.get(
            "entry_discount_multiplier"
        ),
    }
    for source_key, output_key in RISK_CONTEXT_NUMERIC_FIELDS.items():
        fields[output_key] = safe_float(risk_context.get(source_key))
    return fields


def summarize_sentiment_risk_events(events):
    numeric_totals = Counter()
    numeric_counts = Counter()
    posture_counts = Counter()
    hard_safety_flag_counts = Counter()
    samples = 0

    for event in events or []:
        posture = event.get("sentiment_risk_posture")
        flags = event.get("sentiment_hard_safety_flags")
        if not isinstance(flags, list):
            flags = []
        has_numeric_value = any(
            safe_float(event.get(key)) is not None
            for key in RISK_CONTEXT_NUMERIC_FIELDS.values()
        )
        has_risk_value = bool(posture) or bool(flags) or has_numeric_value
        if not has_risk_value:
            continue
        samples += 1
        posture_counts[posture or "unknown"] += 1
        for output_key in RISK_CONTEXT_NUMERIC_FIELDS.values():
            value = safe_float(event.get(output_key))
            if value is None:
                continue
            numeric_totals[output_key] += value
            numeric_counts[output_key] += 1
        risk_size_multiplier = safe_float(
            event.get("risk_context_position_size_effective_multiplier")
        )
        if risk_size_multiplier is not None:
            numeric_totals[
                "risk_context_position_size_effective_multiplier"
            ] += risk_size_multiplier
            numeric_counts[
                "risk_context_position_size_effective_multiplier"
            ] += 1
        for flag in flags:
            hard_safety_flag_counts[str(flag)] += 1

    summary = {
        "sentiment_risk_sample_count": samples,
        "sentiment_risk_posture_counts": dict(posture_counts.most_common()),
        "sentiment_hard_safety_flag_counts": dict(
            hard_safety_flag_counts.most_common()
        ),
        "sentiment_hard_safety_flag_event_count": sum(
            1
            for event in events or []
            if isinstance(event.get("sentiment_hard_safety_flags"), list)
            and event.get("sentiment_hard_safety_flags")
        ),
    }
    for output_key in RISK_CONTEXT_NUMERIC_FIELDS.values():
        avg_key = f"avg_{output_key}"
        if numeric_counts[output_key]:
            summary[avg_key] = round(
                numeric_totals[output_key] / numeric_counts[output_key],
                6,
            )
        else:
            summary[avg_key] = None
    output_key = "risk_context_position_size_effective_multiplier"
    if numeric_counts[output_key]:
        summary[f"avg_{output_key}"] = round(
            numeric_totals[output_key] / numeric_counts[output_key],
            6,
        )
    else:
        summary[f"avg_{output_key}"] = None
    return summary


def state_payload(snapshot):
    payload = snapshot.get("state") or {}
    return payload if isinstance(payload, dict) else {}


def state_summary(snapshot):
    summary = state_payload(snapshot).get("summary")
    return summary if isinstance(summary, dict) else {}


def strategy_payload(snapshot):
    payload = (snapshot.get("strategy_profile") or {}).get("payload")
    return payload if isinstance(payload, dict) else {}


def strategy_context(snapshot):
    context = snapshot.get("strategy_context") or {}
    return context if isinstance(context, dict) else {}


def runtime_status_payload(snapshot):
    payload = snapshot.get("runtime_status") or {}
    return payload if isinstance(payload, dict) else {}


def runtime_status_summary(snapshot):
    summary = runtime_status_payload(snapshot).get("summary")
    return summary if isinstance(summary, dict) else {}


def runtime_status_is_fresh(snapshot, event_ts, max_age_seconds=180):
    runtime_status = runtime_status_summary(snapshot)
    status_ts = parse_iso8601(runtime_status.get("timestamp"))
    if status_ts is None or event_ts is None:
        return False
    return abs((event_ts - status_ts).total_seconds()) <= max_age_seconds


def positive_or_default(value, default=None):
    numeric = safe_float(value)
    if numeric is None:
        return default
    return numeric


def normalized_source_config_map(config, key):
    raw_value = config.get(key, {})
    if not isinstance(raw_value, dict):
        return {}

    normalized = {}
    for source, value in raw_value.items():
        source_key = str(source or "").strip().lower()
        try:
            normalized[source_key] = float(value)
        except Exception:
            continue
    return normalized


def range_momentum_entry_tolerance_pct(config, buy_source, fallback_config=None):
    if buy_source not in ("range_low", "range_mean", "range_median"):
        return 0.0

    source_values = normalized_source_config_map(
        config,
        "momentum_entry_tolerance_pct_by_source",
    )
    source_value = source_values.get(buy_source)
    if source_value is not None:
        return max(0.0, source_value)

    if "momentum_entry_tolerance_pct" in config:
        return max(0.0, safe_float(config.get("momentum_entry_tolerance_pct")) or 0.0)

    if fallback_config:
        fallback_source_values = normalized_source_config_map(
            fallback_config,
            "momentum_entry_tolerance_pct_by_source",
        )
        fallback_source_value = fallback_source_values.get(buy_source)
        if fallback_source_value is not None:
            return max(0.0, fallback_source_value)
        return max(
            0.0,
            safe_float(fallback_config.get("momentum_entry_tolerance_pct")) or 0.0
        )

    return 0.0


def price_is_above_allowed_entry(
    price,
    level,
    config,
    buy_source,
    fallback_config=None,
):
    if buy_source == "llm_target":
        return False
    tolerance_pct = range_momentum_entry_tolerance_pct(
        config,
        buy_source,
        fallback_config,
    )
    return price > (level * (1 + tolerance_pct))


def source_sell_policy(config, buy_source):
    source_key = str(buy_source or "").strip().lower()
    configured_profit_target = safe_float(config.get("profit_target_pct")) or 0.0
    configured_min_profit_target = safe_float(
        config.get("min_profit_target_pct", configured_profit_target)
    ) or 0.0
    return {
        "profit_target_offset_pct": normalized_source_config_map(
            config,
            "sell_target_offset_pct_by_source",
        ).get(source_key, 0.0),
        "aging_start_minutes": normalized_source_config_map(
            config,
            "aging_start_minutes_by_source",
        ).get(
            source_key,
            int(config.get("aging_start_minutes", 999999) or 999999),
        ),
        "aging_step_minutes": normalized_source_config_map(
            config,
            "aging_step_minutes_by_source",
        ).get(
            source_key,
            int(config.get("aging_step_minutes", 60) or 60),
        ),
        "aging_profit_reduction_pct": normalized_source_config_map(
            config,
            "aging_profit_reduction_pct_by_source",
        ).get(
            source_key,
            safe_float(config.get("aging_profit_reduction_pct")) or 0.0,
        ),
        "min_profit_target_pct": normalized_source_config_map(
            config,
            "min_profit_target_pct_by_source",
        ).get(
            source_key,
            configured_min_profit_target,
        ),
    }


def sell_policy_regime(config, signal):
    action_recommendation = signal.get("action_recommendation") or "neutral"
    action_policy = signal.get("action_policy")
    operating_mode = str(
        config.get("operating_mode", "range_plus_llm") or "range_plus_llm"
    ).strip().lower()
    sentiment_control_mode = normalize_sentiment_control_mode(
        config.get("sentiment_control_mode"),
        operating_mode,
    )
    execution_signal = safe_float(signal.get("execution_signal")) or 0.0
    execution_signal_threshold = (
        safe_float(config.get("execution_signal_threshold")) or 0.0
    )
    sentiment_defensive_threshold = (
        safe_float(config.get("sentiment_defensive_threshold")) or 0.03
    )
    sentiment_risk_on_threshold = (
        safe_float(config.get("sentiment_risk_on_threshold")) or 0.12
    )
    normalized_recommendation = str(action_recommendation).strip().lower()
    risk_modulated_core_override = (
        execution_signal < execution_signal_threshold
        and sentiment_control_mode == "risk_modulated"
        and operating_mode in ("range_plus_llm", "range_only")
        and normalized_recommendation in ("blocked", "contrarian_watch")
        and not is_risk_off_block(action_recommendation, action_policy)
    )

    if execution_signal < execution_signal_threshold:
        if risk_modulated_core_override:
            return {
                "name": "risk_modulated_defensive",
                "profit_target_offset_pct": (
                    safe_float(
                        config.get("sentiment_defensive_profit_target_offset_pct")
                    )
                    or -0.0005
                ),
                "extra_aging_reduction_pct": (
                    safe_float(
                        config.get("sentiment_defensive_extra_aging_reduction_pct")
                    )
                    or 0.001
                ),
            }
        return {
            "name": "paused",
            "profit_target_offset_pct": (
                safe_float(config.get("sentiment_paused_profit_target_offset_pct"))
                or -0.001
            ),
            "extra_aging_reduction_pct": (
                safe_float(
                    config.get("sentiment_defensive_extra_aging_reduction_pct")
                )
                or 0.001
            ),
        }

    if execution_signal < sentiment_defensive_threshold:
        return {
            "name": "defensive",
            "profit_target_offset_pct": (
                safe_float(
                    config.get("sentiment_defensive_profit_target_offset_pct")
                )
                or -0.0005
            ),
            "extra_aging_reduction_pct": (
                safe_float(
                    config.get("sentiment_defensive_extra_aging_reduction_pct")
                )
                or 0.001
            ),
        }

    if execution_signal >= sentiment_risk_on_threshold:
        return {
            "name": "risk_on",
            "profit_target_offset_pct": (
                safe_float(config.get("sentiment_risk_on_profit_target_offset_pct"))
                or 0.0005
            ),
            "extra_aging_reduction_pct": 0.0,
        }

    return {
        "name": "neutral",
        "profit_target_offset_pct": 0.0,
        "extra_aging_reduction_pct": 0.0,
    }


def effective_sell_profit_target_pct(
    config,
    *,
    buy_source=None,
    base_profit_target=None,
    age_minutes=0,
    regime=None,
):
    regime = regime or {}
    policy = source_sell_policy(config, buy_source)
    starting_profit_target = (
        safe_float(config.get("profit_target_pct")) or 0.0
        if base_profit_target is None
        else float(base_profit_target)
    )
    starting_profit_target += policy["profit_target_offset_pct"]
    starting_profit_target += float(regime.get("profit_target_offset_pct", 0.0) or 0.0)
    starting_profit_target = max(policy["min_profit_target_pct"], starting_profit_target)

    if (
        age_minutes is None
        or policy["aging_profit_reduction_pct"] <= 0
        or age_minutes < policy["aging_start_minutes"]
    ):
        return starting_profit_target

    reduction_steps = (
        int(
            (age_minutes - policy["aging_start_minutes"])
            // max(1, int(policy["aging_step_minutes"]))
        )
        + 1
    )
    adjusted_profit = (
        starting_profit_target
        - reduction_steps * policy["aging_profit_reduction_pct"]
        - float(regime.get("extra_aging_reduction_pct", 0.0) or 0.0)
    )
    return max(policy["min_profit_target_pct"], adjusted_profit)


def source_status_allows_trading(signal_status, source_status, require_fresh_signal, min_signal_status):
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


def is_liquidity_confidence_block(action_recommendation, action_policy):
    normalized = (action_recommendation or "neutral").strip().lower()
    if normalized != "blocked" or not isinstance(action_policy, dict):
        return False

    reason = str(action_policy.get("reason") or "").lower()
    if "liquidity risk" in reason and "confidence" in reason:
        return True

    return action_policy.get("max_liquidity_risk") is not None and "confidence" in reason


def normalize_sentiment_control_mode(raw_value, operating_mode=None):
    normalized = str(raw_value or "").strip().lower()
    if normalized in ("strict_sentiment", "risk_modulated", "price_first"):
        return normalized
    return "strict_sentiment"


def is_risk_off_block(action_recommendation, action_policy):
    normalized = (action_recommendation or "neutral").strip().lower()
    if normalized == "risk_off":
        return True
    if not isinstance(action_policy, dict):
        return False

    reason = str(action_policy.get("reason") or "").lower()
    if "block new long entries" in reason:
        return True
    if (
        action_policy.get("risk_off_blocks_longs")
        and normalized in ("blocked", "risk_off")
        and "bearish sentiment" in reason
    ):
        return True
    return False


def sentiment_buy_permissions(
    action_recommendation,
    action_policy=None,
    *,
    operating_mode=None,
    allow_range_buy_on_confidence_block=False,
    sentiment_control_mode=None,
    weather_report=None,
):
    normalized = (action_recommendation or "neutral").strip().lower()
    sentiment_control_mode = normalize_sentiment_control_mode(
        sentiment_control_mode,
        operating_mode,
    )
    llm_buys_allowed = normalized == "bullish_allowed"
    weather_bot_authority = weather_bot_decides(weather_report)
    weather_emergency = bool(
        weather_report.get("emergency_bell")
        if isinstance(weather_report, dict)
        else False
    )
    range_core_buys_allowed = normalized in (
        "bullish_allowed",
        "neutral",
        "watch_only",
    )
    range_high_buys_allowed = range_core_buys_allowed
    if weather_bot_authority:
        range_core_buys_allowed = not weather_emergency
        range_high_buys_allowed = not weather_emergency
    elif (
        not range_core_buys_allowed
        and allow_range_buy_on_confidence_block
        and operating_mode == "range_only"
        and is_liquidity_confidence_block(action_recommendation, action_policy)
    ):
        range_core_buys_allowed = True
        range_high_buys_allowed = True
    elif (
        normalized in ("blocked", "contrarian_watch")
        and sentiment_control_mode == "risk_modulated"
        and not is_risk_off_block(action_recommendation, action_policy)
    ):
        range_core_buys_allowed = True
        range_high_buys_allowed = False
    elif (
        normalized == "blocked"
        and sentiment_control_mode == "price_first"
    ):
        range_core_buys_allowed = not is_risk_off_block(
            action_recommendation,
            action_policy,
        )
        range_high_buys_allowed = range_core_buys_allowed
    range_buys_allowed = (
        range_core_buys_allowed or range_high_buys_allowed
    )
    return {
        "llm_buys_allowed": llm_buys_allowed,
        "range_core_buys_allowed": range_core_buys_allowed,
        "range_high_buys_allowed": range_high_buys_allowed,
        "range_buys_allowed": range_buys_allowed,
        "any_buys_allowed": llm_buys_allowed or range_buys_allowed,
    }


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


def effective_entry_step_pct(base_entry_step_pct, volatility_pct, config):
    base_step = safe_float(base_entry_step_pct) or 0.0
    if base_step <= 0:
        return 0.0
    if not str(config.get("volatility_adaptive_entry_step_enabled", False)).strip().lower() in (
        "1", "true", "yes", "on"
    ):
        return base_step

    volatility = safe_float(volatility_pct)
    if volatility is None or volatility <= 0:
        return base_step

    reference_pct = safe_float(config.get("volatility_reference_pct"))
    if reference_pct is None or reference_pct <= 0:
        reference_pct = 0.02
    min_multiplier = safe_float(config.get("volatility_min_step_multiplier"))
    if min_multiplier is None or min_multiplier <= 0:
        min_multiplier = 1.0
    max_multiplier = safe_float(config.get("volatility_max_step_multiplier"))
    if max_multiplier is None or max_multiplier < min_multiplier:
        max_multiplier = max(min_multiplier, 2.0)

    raw_multiplier = volatility / reference_pct
    step_multiplier = max(min_multiplier, min(max_multiplier, raw_multiplier))
    return base_step * step_multiplier


def inventory_pressure_adjustment(
    deployed_inventory_usd,
    effective_max_inventory_usd,
    config
):
    enabled = str(
        config.get("inventory_pressure_size_scaling_enabled", False)
    ).strip().lower() in ("1", "true", "yes", "on")
    if not enabled:
        return {
            "usage_ratio": 0.0,
            "size_multiplier": 1.0,
        }

    max_inventory = safe_float(effective_max_inventory_usd) or 0.0
    deployed_inventory = safe_float(deployed_inventory_usd) or 0.0
    if max_inventory <= 0:
        return {
            "usage_ratio": 1.0,
            "size_multiplier": 0.0,
        }

    usage_ratio = max(0.0, deployed_inventory / max_inventory)
    start_ratio = safe_float(config.get("inventory_pressure_start_usage_pct"))
    if start_ratio is None:
        start_ratio = 0.5
    min_multiplier = safe_float(config.get("inventory_pressure_min_size_multiplier"))
    if min_multiplier is None:
        min_multiplier = 0.25
    start_ratio = max(0.0, min(1.0, start_ratio))
    min_multiplier = max(0.0, min(1.0, min_multiplier))

    if usage_ratio <= start_ratio:
        size_multiplier = 1.0
    elif usage_ratio >= 1.0:
        size_multiplier = min_multiplier
    else:
        progress = (usage_ratio - start_ratio) / (1.0 - start_ratio)
        size_multiplier = 1.0 - ((1.0 - min_multiplier) * progress)

    return {
        "usage_ratio": usage_ratio,
        "size_multiplier": size_multiplier,
    }


def strategy_bool(config, key, default=False):
    value = config.get(key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def strategy_float(config, key, default):
    try:
        value = config.get(key, default)
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def apply_operating_mode_to_strategy_modes(base_modes, operating_mode):
    if operating_mode in ("sell_only", "observe_only"):
        return []
    if operating_mode == "range_only":
        return [mode for mode in base_modes if mode != "llm_target"]
    return list(base_modes)


def select_dynamic_strategy_modes(base_modes, operating_mode, range_position, config):
    active_modes = apply_operating_mode_to_strategy_modes(base_modes, operating_mode)
    if not active_modes:
        return []

    llm_enabled = "llm_target" in active_modes
    range_modes = [mode for mode in active_modes if mode != "llm_target"]
    if not range_modes:
        return active_modes

    if (
        not strategy_bool(config, "dynamic_anchor_mode", False)
        or range_position is None
    ):
        return active_modes

    low_band_max = strategy_float(config, "dynamic_anchor_low_band_max", 0.35)
    high_band_min = strategy_float(config, "dynamic_anchor_high_band_min", 0.75)
    midpoint_split = strategy_float(config, "dynamic_anchor_midpoint_split", 0.5)
    mid_mode = str(config.get("dynamic_anchor_mid_mode", "median") or "median").strip().lower()
    alt_mid_mode = "mean" if mid_mode == "median" else "median"

    if range_position <= low_band_max:
        preferred_modes = ["low", mid_mode, alt_mid_mode, "high"]
    elif range_position >= high_band_min:
        preferred_modes = ["high", mid_mode, alt_mid_mode, "low"]
    else:
        if mid_mode in range_modes or alt_mid_mode in range_modes:
            preferred_modes = [mid_mode, alt_mid_mode, "low", "high"]
        elif range_position >= midpoint_split:
            preferred_modes = ["high", "low", mid_mode, alt_mid_mode]
        else:
            preferred_modes = ["low", "high", mid_mode, alt_mid_mode]

    selected_range_mode = next(
        (mode for mode in preferred_modes if mode in range_modes),
        range_modes[0],
    )
    selected_modes = [selected_range_mode]
    if llm_enabled:
        selected_modes.insert(0, "llm_target")
    return selected_modes


def compute_grid(anchor, entry_step_pct, max_grid_size):
    return sorted(
        [
            anchor * (1 - (entry_step_pct * (i + 1)))
            for i in range(max_grid_size)
        ],
        reverse=True
    )


def snapshot_price(snapshot):
    price = safe_float((snapshot.get("ticker") or {}).get("last_price"))
    if price is not None:
        return price
    signal = signal_payload(snapshot)
    return safe_float(signal.get("btc_price"))


def sell_backlog_oldest_minutes(snapshot):
    open_sell_orders = (state_payload(snapshot).get("open_sell_orders") or [])
    captured_at = snapshot_timestamp(snapshot)
    if captured_at is None:
        return 0.0

    oldest = 0.0
    for order in open_sell_orders:
        if not isinstance(order, dict):
            continue
        placed_at = parse_iso8601(order.get("placed_at"))
        if placed_at is None:
            continue
        age_minutes = max(0.0, (captured_at - placed_at).total_seconds() / 60.0)
        oldest = max(oldest, age_minutes)
    return round(oldest, 2)


def approved_event_profit_target_pct(snapshot, event):
    config = strategy_payload(snapshot)
    signal = signal_payload(snapshot)
    return effective_sell_profit_target_pct(
        config,
        buy_source=event.get("buy_source"),
        base_profit_target=safe_float(event.get("sell_pct_override")),
        age_minutes=0,
        regime=sell_policy_regime(config, signal),
    )


def infer_live_only_blockers(snapshot, event):
    blockers = []
    config = strategy_payload(snapshot)
    state_info = state_summary(snapshot)
    runtime_status = runtime_status_summary(snapshot)
    captured_at = snapshot_timestamp(snapshot)
    runtime_status_fresh = runtime_status_is_fresh(snapshot, captured_at)

    runtime_block_reason = (
        runtime_status.get("runtime_block_reason")
        if runtime_status_fresh
        else None
    )
    if runtime_block_reason:
        blockers.append(str(runtime_block_reason))

    operating_mode = str(
        runtime_status.get("operating_mode")
        or config.get("operating_mode", "range_plus_llm")
        or "range_plus_llm"
    ).strip().lower()
    if operating_mode not in ("range_plus_llm", "range_only"):
        blockers.append(f"operating_mode_{operating_mode}")

    backlog_limit = int(config.get("disable_new_buys_on_sell_backlog_count", 0) or 0)
    open_sell_count = int(
        runtime_status.get("open_sell_count")
        or state_info.get("open_sell_count")
        or 0
    )
    if backlog_limit > 0 and open_sell_count >= backlog_limit:
        blockers.append("sell_backlog_count")

    backlog_minutes_limit = float(
        config.get("disable_new_buys_on_sell_backlog_minutes", 0) or 0
    )
    oldest_sell_age = safe_float(
        runtime_status.get("sell_backlog_oldest_minutes")
    )
    if oldest_sell_age is None:
        oldest_sell_age = sell_backlog_oldest_minutes(snapshot)
    if backlog_minutes_limit > 0 and oldest_sell_age >= backlog_minutes_limit:
        blockers.append("sell_backlog_age_minutes")

    effective_position_size_pct = safe_float(
        runtime_status.get("effective_position_size_pct")
    )
    if (
        runtime_status_fresh
        and effective_position_size_pct is not None
        and effective_position_size_pct <= 0
    ):
        blockers.append("effective_position_size_pct_zero")

    effective_max_inventory_usd = safe_float(
        runtime_status.get("effective_max_inventory_usd")
    )
    if (
        runtime_status_fresh
        and
        effective_max_inventory_usd is not None
        and effective_max_inventory_usd <= 0
    ):
        blockers.append("effective_max_inventory_usd_zero")

    effective_max_open_sell_orders = runtime_status.get(
        "effective_max_open_sell_orders"
    )
    try:
        if effective_max_open_sell_orders is not None:
            effective_max_open_sell_orders = int(effective_max_open_sell_orders)
    except Exception:
        effective_max_open_sell_orders = None

    if (
        runtime_status_fresh
        and
        effective_max_open_sell_orders is not None
        and open_sell_count >= effective_max_open_sell_orders
    ):
        blockers.append("effective_max_open_sell_orders")

    buy_source = str(event.get("buy_source") or "")
    high_anchor_enabled = runtime_status.get("high_anchor_enabled")
    if (
        buy_source == "range_high_band"
        and high_anchor_enabled is False
    ):
        blockers.append("high_anchor_disabled")

    private_api_backoff_until = parse_iso8601(
        state_info.get("private_api_backoff_until")
    )
    if (
        captured_at is not None
        and private_api_backoff_until is not None
        and private_api_backoff_until > captured_at
    ):
        blockers.append("private_api_backoff_active")

    insufficient_funds_backoff_until = parse_iso8601(
        state_info.get("sell_insufficient_funds_backoff_until")
    )
    if (
        captured_at is not None
        and insufficient_funds_backoff_until is not None
        and insufficient_funds_backoff_until > captured_at
    ):
        blockers.append("sell_insufficient_funds_backoff_active")

    return list(dict.fromkeys(blockers))


def simulate_missed_opportunity(snapshot, event, snapshots):
    entry_time = parse_iso8601(event.get("captured_at"))
    entry_price = safe_float(event.get("level")) or safe_float(event.get("price"))
    if entry_time is None or entry_price is None or entry_price <= 0:
        return None

    hold_end = entry_time + timedelta(hours=BACKTEST_POTENTIAL_MAX_HOLD_HOURS)
    target_profit_pct = approved_event_profit_target_pct(snapshot, event)
    target_return_pct = target_profit_pct * 100.0
    max_runup_pct = 0.0
    max_drawdown_pct = 0.0
    end_return_pct = None
    take_profit_reached_at = None

    for future_snapshot in snapshots:
        future_time = snapshot_timestamp(future_snapshot)
        if future_time is None or future_time <= entry_time:
            continue
        if future_time > hold_end:
            break
        future_price = snapshot_price(future_snapshot)
        if future_price is None:
            continue
        return_pct = ((future_price - entry_price) / entry_price) * 100.0
        max_runup_pct = max(max_runup_pct, return_pct)
        max_drawdown_pct = min(max_drawdown_pct, return_pct)
        end_return_pct = return_pct
        if take_profit_reached_at is None and return_pct >= target_return_pct:
            take_profit_reached_at = future_time.isoformat()

    return {
        "target_profit_pct": round(target_profit_pct * 100.0, 4),
        "take_profit_reached": take_profit_reached_at is not None,
        "take_profit_reached_at": take_profit_reached_at,
        "max_runup_pct": round(max_runup_pct, 6),
        "max_drawdown_pct": round(max_drawdown_pct, 6),
        "end_return_pct": round(end_return_pct, 6) if end_return_pct is not None else None,
        "hold_window_hours": BACKTEST_POTENTIAL_MAX_HOLD_HOURS,
    }


def summarize_potential_from_approved_events(replay, snapshots):
    snapshot_by_timestamp = {}
    for snapshot in snapshots or []:
        captured_at = snapshot.get("captured_at")
        if captured_at:
            snapshot_by_timestamp[captured_at] = snapshot

    potential_results = []
    for event in replay.get("approved_events") or []:
        snapshot = snapshot_by_timestamp.get(event.get("captured_at"))
        if not snapshot:
            continue
        potential = simulate_missed_opportunity(snapshot, event, snapshots or [])
        if potential:
            size_multiplier = safe_float(
                event.get("risk_context_position_size_effective_multiplier")
            )
            if size_multiplier is None:
                size_multiplier = 1.0
            potential["risk_context_position_size_effective_multiplier"] = (
                size_multiplier
            )
            potential_results.append(potential)

    end_returns = [
        result["end_return_pct"]
        for result in potential_results
        if result.get("end_return_pct") is not None
    ]
    max_runups = [
        result["max_runup_pct"]
        for result in potential_results
        if result.get("max_runup_pct") is not None
    ]
    max_drawdowns = [
        result["max_drawdown_pct"]
        for result in potential_results
        if result.get("max_drawdown_pct") is not None
    ]
    take_profit_count = sum(
        1 for result in potential_results if result.get("take_profit_reached")
    )
    risk_sized_end_returns = [
        result["end_return_pct"]
        * result.get("risk_context_position_size_effective_multiplier", 1.0)
        for result in potential_results
        if result.get("end_return_pct") is not None
    ]
    risk_sized_max_runups = [
        result["max_runup_pct"]
        * result.get("risk_context_position_size_effective_multiplier", 1.0)
        for result in potential_results
        if result.get("max_runup_pct") is not None
    ]
    risk_sized_max_drawdowns = [
        result["max_drawdown_pct"]
        * result.get("risk_context_position_size_effective_multiplier", 1.0)
        for result in potential_results
        if result.get("max_drawdown_pct") is not None
    ]
    size_multipliers = [
        result.get("risk_context_position_size_effective_multiplier", 1.0)
        for result in potential_results
        if result.get("risk_context_position_size_effective_multiplier") is not None
    ]

    return {
        "evaluated_count": len(potential_results),
        "take_profit_reached_count": take_profit_count,
        "take_profit_reached_rate": (
            round(take_profit_count / len(potential_results), 4)
            if potential_results
            else None
        ),
        "avg_end_return_pct": (
            round(statistics.mean(end_returns), 6)
            if end_returns
            else None
        ),
        "median_end_return_pct": (
            round(statistics.median(end_returns), 6)
            if end_returns
            else None
        ),
        "best_end_return_pct": max(end_returns) if end_returns else None,
        "worst_end_return_pct": min(end_returns) if end_returns else None,
        "avg_max_runup_pct": (
            round(statistics.mean(max_runups), 6)
            if max_runups
            else None
        ),
        "avg_max_drawdown_pct": (
            round(statistics.mean(max_drawdowns), 6)
            if max_drawdowns
            else None
        ),
        "avg_risk_size_multiplier": (
            round(statistics.mean(size_multipliers), 6)
            if size_multipliers
            else None
        ),
        "risk_sized_avg_end_return_pct": (
            round(statistics.mean(risk_sized_end_returns), 6)
            if risk_sized_end_returns
            else None
        ),
        "risk_sized_avg_max_runup_pct": (
            round(statistics.mean(risk_sized_max_runups), 6)
            if risk_sized_max_runups
            else None
        ),
        "risk_sized_avg_max_drawdown_pct": (
            round(statistics.mean(risk_sized_max_drawdowns), 6)
            if risk_sized_max_drawdowns
            else None
        ),
    }


def build_strategy_comparison_rows(snapshots, strategy_set_file):
    entries = [
        parsed
        for parsed in (
            parse_strategy_set_entry(entry)
            for entry in load_strategy_set_entries(strategy_set_file)
        )
        if parsed
    ]
    rows = []
    detailed = []

    for entry in entries:
        strategy_path, strategy_payload = load_strategy_profile_from_file(entry["path"])
        variant_snapshots = [
            snapshot_with_strategy(
                snapshot,
                entry["label"],
                strategy_path,
                strategy_payload,
            )
            for snapshot in snapshots
        ]
        replay = replay_from_snapshots(variant_snapshots)
        potential = summarize_potential_from_approved_events(replay, variant_snapshots)
        summary = replay["summary"]
        risk_summary = summary.get("approved_sentiment_risk") or {}
        row = {
            "strategy_label": entry["label"],
            "strategy_file": strategy_path,
            "grid_anchor": strategy_payload.get("grid_anchor"),
            "operating_mode": strategy_payload.get("operating_mode"),
            "sentiment_control_mode": strategy_payload.get("sentiment_control_mode"),
            "dynamic_anchor_mode": strategy_payload.get("dynamic_anchor_mode"),
            "entry_step_pct": strategy_payload.get("entry_step_pct"),
            "volatility_reference_pct": strategy_payload.get("volatility_reference_pct"),
            "raw_candidates": summary.get("raw_candidates", 0),
            "approved_candidates": summary.get("approved_candidates", 0),
            "hold_snapshots": summary.get("hold_snapshots", 0),
            "approved_llm_target": summary.get("approved_counts_by_source", {}).get("llm_target", 0),
            "approved_range_low": summary.get("approved_counts_by_source", {}).get("range_low", 0),
            "approved_range_median": summary.get("approved_counts_by_source", {}).get("range_median", 0),
            "approved_range_high_band": summary.get("approved_counts_by_source", {}).get("range_high_band", 0),
            "blocked_price_above_level": summary.get("blocked_reason_counts", {}).get("price_above_level", 0),
            "blocked_sentiment_high": summary.get("blocked_reason_counts", {}).get("sentiment_action_not_high_range_permitted", 0),
            "potential_evaluated_count": potential.get("evaluated_count"),
            "potential_take_profit_reached_rate": potential.get("take_profit_reached_rate"),
            "potential_avg_end_return_pct": potential.get("avg_end_return_pct"),
            "potential_avg_max_runup_pct": potential.get("avg_max_runup_pct"),
            "potential_avg_max_drawdown_pct": potential.get("avg_max_drawdown_pct"),
            "potential_avg_risk_size_multiplier": potential.get(
                "avg_risk_size_multiplier"
            ),
            "potential_risk_sized_avg_end_return_pct": potential.get(
                "risk_sized_avg_end_return_pct"
            ),
            "potential_risk_sized_avg_max_runup_pct": potential.get(
                "risk_sized_avg_max_runup_pct"
            ),
            "potential_risk_sized_avg_max_drawdown_pct": potential.get(
                "risk_sized_avg_max_drawdown_pct"
            ),
            "approved_sentiment_risk_samples": risk_summary.get(
                "sentiment_risk_sample_count"
            ),
            "approved_sentiment_risk_postures": json.dumps(
                risk_summary.get("sentiment_risk_posture_counts") or {},
                sort_keys=True,
            ),
            "approved_sentiment_hard_safety_flag_events": risk_summary.get(
                "sentiment_hard_safety_flag_event_count"
            ),
            "approved_sentiment_hard_safety_flags": json.dumps(
                risk_summary.get("sentiment_hard_safety_flag_counts") or {},
                sort_keys=True,
            ),
            "approved_avg_sentiment_market_risk_score": risk_summary.get(
                "avg_sentiment_market_risk_score"
            ),
            "approved_avg_sentiment_buy_aggression_score": risk_summary.get(
                "avg_sentiment_buy_aggression_score"
            ),
            "approved_avg_sentiment_downside_risk_score": risk_summary.get(
                "avg_sentiment_downside_risk_score"
            ),
            "approved_avg_sentiment_bottoming_score": risk_summary.get(
                "avg_sentiment_bottoming_score"
            ),
            "approved_avg_sentiment_rebound_score": risk_summary.get(
                "avg_sentiment_rebound_score"
            ),
            "approved_avg_sentiment_breakout_score": risk_summary.get(
                "avg_sentiment_breakout_score"
            ),
            "approved_avg_sentiment_position_size_multiplier": risk_summary.get(
                "avg_sentiment_position_size_multiplier"
            ),
            "approved_avg_sentiment_grid_aggression_multiplier": risk_summary.get(
                "avg_sentiment_grid_aggression_multiplier"
            ),
            "approved_avg_sentiment_target_profit_multiplier": risk_summary.get(
                "avg_sentiment_target_profit_multiplier"
            ),
            "approved_avg_sentiment_entry_discount_multiplier": risk_summary.get(
                "avg_sentiment_entry_discount_multiplier"
            ),
            "approved_avg_risk_context_position_size_effective_multiplier": (
                risk_summary.get(
                    "avg_risk_context_position_size_effective_multiplier"
                )
            ),
        }
        rows.append(row)
        detailed.append({
            "strategy_label": entry["label"],
            "strategy_file": strategy_path,
            "strategy_payload": strategy_payload,
            "replay_summary": summary,
            "potential_summary": potential,
            "recent_replay_events": replay.get("recent_replay_events", []),
            "recent_approved_events": replay.get("recent_approved_events", []),
        })

    rows.sort(
        key=lambda row: (
            -(row.get("approved_candidates") or 0),
            -((row.get("potential_take_profit_reached_rate") or 0)),
            -((row.get("potential_avg_end_return_pct") or -999999)),
            row.get("strategy_label") or "",
        )
    )
    return {
        "strategy_set_file": resolve_repo_path(strategy_set_file),
        "count": len(rows),
        "rows": rows,
        "details": detailed,
    }


def practical_strategy_score(row):
    approved = row.get("approved_candidates") or 0
    take_profit_rate = row.get("potential_take_profit_reached_rate") or 0.0
    avg_end_return = row.get("potential_avg_end_return_pct")
    avg_drawdown = row.get("potential_avg_max_drawdown_pct")
    hold_snapshots = row.get("hold_snapshots") or 0
    raw_candidates = row.get("raw_candidates") or 0
    candidate_efficiency = (approved / raw_candidates) if raw_candidates > 0 else 0.0
    blocked_high = row.get("blocked_sentiment_high") or 0

    if approved <= 0:
        # A strategy that stays out should not rank below strategies that prove
        # negative expectancy in the same window.
        return round(
            0.0
            - (hold_snapshots * 0.002)
            - (blocked_high * 0.005),
            6,
        )

    avg_end_return = avg_end_return if avg_end_return is not None else -2.0
    avg_drawdown = avg_drawdown if avg_drawdown is not None else min(avg_end_return, 0.0)
    positive_expectancy = avg_end_return > 0
    activity_reward = (
        min(approved, 50) * 0.25
        if positive_expectancy else
        -min(approved, 50) * 0.1
    )
    efficiency_reward = candidate_efficiency * (20.0 if positive_expectancy else 5.0)

    score = (
        (avg_end_return * 120.0)
        + (avg_drawdown * 35.0)
        + (take_profit_rate * 30.0)
        + activity_reward
        + efficiency_reward
        - (hold_snapshots * 0.002)
        - (blocked_high * 0.005)
    )
    return round(score, 6)


def build_ranked_strategy_rows(comparison):
    ranked_rows = []
    for row in comparison.get("rows") or []:
        ranked = dict(row)
        ranked["candidate_efficiency"] = round(
            ((row.get("approved_candidates") or 0) / (row.get("raw_candidates") or 1)),
            6,
        ) if (row.get("raw_candidates") or 0) > 0 else 0.0
        ranked["practical_score"] = practical_strategy_score(row)
        ranked_rows.append(ranked)

    ranked_rows.sort(
        key=lambda row: (
            -(row.get("practical_score") or -999999),
            -(row.get("approved_candidates") or 0),
            -((row.get("potential_take_profit_reached_rate") or 0)),
            -((row.get("potential_avg_end_return_pct") or -999999)),
            row.get("strategy_label") or "",
        )
    )
    return ranked_rows


def anchor_winner_rejection_reasons(row, anchor, criteria):
    reasons = []
    source_field = ANCHOR_WINNER_SOURCES[anchor]
    anchor_approved = row.get(source_field) or 0
    avg_end_return = row.get("potential_avg_end_return_pct")
    avg_drawdown = row.get("potential_avg_max_drawdown_pct")

    if anchor_approved < criteria["min_anchor_approved"]:
        reasons.append("anchor_approved_below_min")
    if avg_end_return is None:
        reasons.append("missing_avg_end_return")
    elif avg_end_return < criteria["min_avg_end_return_pct"]:
        reasons.append("avg_end_return_below_min")
    if avg_drawdown is None:
        reasons.append("missing_avg_drawdown")
    elif avg_drawdown < criteria["max_avg_drawdown_pct"]:
        reasons.append("avg_drawdown_below_max")
    if (row.get("practical_score") or 0) <= 0:
        reasons.append("practical_score_not_positive")

    return reasons


def strategy_detail_by_file(comparison):
    details = {}
    for detail in comparison.get("details") or []:
        strategy_file = detail.get("strategy_file")
        if strategy_file:
            details[strategy_file] = detail
    return details


def anchor_winner_strategy_file(path, strategy_dir):
    if not strategy_dir or not path:
        return path

    filename = os.path.basename(str(path))
    if not filename:
        return path

    return os.path.join(strategy_dir, filename)


def build_anchor_winners(
    comparison,
    *,
    min_anchor_approved=BACKTEST_ANCHOR_WINNER_MIN_APPROVED,
    min_avg_end_return_pct=BACKTEST_ANCHOR_WINNER_MIN_AVG_END_RETURN_PCT,
    max_avg_drawdown_pct=BACKTEST_ANCHOR_WINNER_MAX_AVG_DRAWDOWN_PCT,
    strategy_dir=BACKTEST_ANCHOR_WINNER_STRATEGY_DIR,
):
    ranked_rows = build_ranked_strategy_rows(comparison)
    details_by_file = strategy_detail_by_file(comparison)
    criteria = {
        "min_anchor_approved": max(0, int(min_anchor_approved)),
        "min_avg_end_return_pct": float(min_avg_end_return_pct),
        "max_avg_drawdown_pct": float(max_avg_drawdown_pct),
    }
    winners = {}

    for anchor, source_field in ANCHOR_WINNER_SOURCES.items():
        candidates = []
        for row in ranked_rows:
            if (row.get(source_field) or 0) <= 0:
                continue

            reasons = anchor_winner_rejection_reasons(row, anchor, criteria)
            source_strategy_file = row.get("strategy_file")
            strategy_file = anchor_winner_strategy_file(
                source_strategy_file,
                strategy_dir,
            )
            detail = details_by_file.get(source_strategy_file, {})
            candidates.append({
                "strategy_label": row.get("strategy_label"),
                "strategy_file": strategy_file,
                "source_strategy_file": source_strategy_file,
                "practical_score": row.get("practical_score"),
                "anchor_approved_candidates": row.get(source_field) or 0,
                "approved_candidates": row.get("approved_candidates") or 0,
                "candidate_efficiency": row.get("candidate_efficiency"),
                "potential_take_profit_reached_rate": row.get(
                    "potential_take_profit_reached_rate"
                ),
                "potential_avg_end_return_pct": row.get(
                    "potential_avg_end_return_pct"
                ),
                "potential_avg_max_runup_pct": row.get(
                    "potential_avg_max_runup_pct"
                ),
                "potential_avg_max_drawdown_pct": row.get(
                    "potential_avg_max_drawdown_pct"
                ),
                "entry_step_pct": row.get("entry_step_pct"),
                "volatility_reference_pct": row.get("volatility_reference_pct"),
                "grid_anchor": row.get("grid_anchor"),
                "operating_mode": row.get("operating_mode"),
                "sentiment_control_mode": row.get("sentiment_control_mode"),
                "eligible": not reasons,
                "rejection_reasons": reasons,
                "strategy_payload": detail.get("strategy_payload"),
            })

        selected = next((candidate for candidate in candidates if candidate["eligible"]), None)
        winners[anchor] = {
            "source_field": source_field,
            "selected": selected,
            "candidates": candidates[:5],
        }

    return {
        "generated_at": now_utc().isoformat(),
        "strategy_set_file": comparison.get("strategy_set_file"),
        "criteria": criteria,
        "strategy_dir": strategy_dir or None,
        "winners": winners,
    }


def write_anchor_winners_json(comparison, output_path):
    payload = build_anchor_winners(comparison)
    resolved = resolve_repo_path(output_path)
    output_dir = os.path.dirname(resolved)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(resolved, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return resolved


def write_strategy_comparison_csv(comparison, output_path):
    rows = comparison.get("rows") or []
    if not rows:
        return None

    resolved = resolve_repo_path(output_path)
    output_dir = os.path.dirname(resolved)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    fieldnames = [
        "strategy_label",
        "strategy_file",
        "grid_anchor",
        "operating_mode",
        "sentiment_control_mode",
        "dynamic_anchor_mode",
        "entry_step_pct",
        "volatility_reference_pct",
        "raw_candidates",
        "approved_candidates",
        "hold_snapshots",
        "approved_llm_target",
        "approved_range_low",
        "approved_range_median",
        "approved_range_high_band",
        "blocked_price_above_level",
        "blocked_sentiment_high",
        "potential_evaluated_count",
        "potential_take_profit_reached_rate",
        "potential_avg_end_return_pct",
        "potential_avg_max_runup_pct",
        "potential_avg_max_drawdown_pct",
        "potential_avg_risk_size_multiplier",
        "potential_risk_sized_avg_end_return_pct",
        "potential_risk_sized_avg_max_runup_pct",
        "potential_risk_sized_avg_max_drawdown_pct",
        "approved_sentiment_risk_samples",
        "approved_sentiment_risk_postures",
        "approved_sentiment_hard_safety_flag_events",
        "approved_sentiment_hard_safety_flags",
        "approved_avg_sentiment_market_risk_score",
        "approved_avg_sentiment_buy_aggression_score",
        "approved_avg_sentiment_downside_risk_score",
        "approved_avg_sentiment_bottoming_score",
        "approved_avg_sentiment_rebound_score",
        "approved_avg_sentiment_breakout_score",
        "approved_avg_sentiment_position_size_multiplier",
        "approved_avg_sentiment_grid_aggression_multiplier",
        "approved_avg_sentiment_target_profit_multiplier",
        "approved_avg_sentiment_entry_discount_multiplier",
        "approved_avg_risk_context_position_size_effective_multiplier",
    ]
    with open(resolved, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return resolved


def write_ranked_strategy_csv(comparison, output_path):
    rows = build_ranked_strategy_rows(comparison)
    if not rows:
        return None

    resolved = resolve_repo_path(output_path)
    output_dir = os.path.dirname(resolved)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    fieldnames = [
        "strategy_label",
        "practical_score",
        "approved_candidates",
        "candidate_efficiency",
        "approved_llm_target",
        "dynamic_anchor_mode",
        "potential_evaluated_count",
        "potential_take_profit_reached_rate",
        "potential_avg_end_return_pct",
        "potential_avg_max_runup_pct",
        "potential_avg_max_drawdown_pct",
        "potential_avg_risk_size_multiplier",
        "potential_risk_sized_avg_end_return_pct",
        "potential_risk_sized_avg_max_runup_pct",
        "potential_risk_sized_avg_max_drawdown_pct",
        "approved_sentiment_risk_samples",
        "approved_sentiment_risk_postures",
        "approved_sentiment_hard_safety_flag_events",
        "approved_sentiment_hard_safety_flags",
        "approved_avg_sentiment_market_risk_score",
        "approved_avg_sentiment_buy_aggression_score",
        "approved_avg_sentiment_downside_risk_score",
        "approved_avg_sentiment_bottoming_score",
        "approved_avg_sentiment_rebound_score",
        "approved_avg_sentiment_breakout_score",
        "approved_avg_sentiment_position_size_multiplier",
        "approved_avg_sentiment_grid_aggression_multiplier",
        "approved_avg_sentiment_target_profit_multiplier",
        "approved_avg_sentiment_entry_discount_multiplier",
        "approved_avg_risk_context_position_size_effective_multiplier",
        "raw_candidates",
        "hold_snapshots",
        "approved_range_low",
        "approved_range_median",
        "approved_range_high_band",
        "blocked_price_above_level",
        "blocked_sentiment_high",
        "entry_step_pct",
        "volatility_reference_pct",
        "grid_anchor",
        "operating_mode",
        "sentiment_control_mode",
        "strategy_file",
    ]
    with open(resolved, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return resolved


def compute_high_anchor_grid(high, price, entry_step_pct):
    lower_bound = high * (1 - entry_step_pct)
    if lower_bound <= price <= high:
        return [price]
    return []


def flow_adjustment(config, flow_pressure, buy_source):
    if flow_pressure is None:
        return {"size_multiplier": 1.0, "block_buy": False, "reason": None}

    flow_defensive_size_multiplier = safe_float(
        config.get("flow_defensive_size_multiplier")
    ) or 0.75
    flow_block_llm_only_below = safe_float(
        config.get("flow_block_llm_only_below")
    ) or -0.5
    flow_block_threshold = safe_float(config.get("flow_block_threshold")) or -0.4
    flow_defensive_threshold = safe_float(config.get("flow_defensive_threshold")) or -0.2
    flow_block_high_only = bool(config.get("flow_block_high_only", True))

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

    return {"size_multiplier": 1.0, "block_buy": False, "reason": None}


def risk_context_high_band_guard(config, risk_context):
    if not strategy_bool(config, "risk_context_high_band_guard_enabled", False):
        return {"allowed": True, "reason": None}

    if not isinstance(risk_context, dict):
        risk_context = {}

    flags = risk_context.get("hard_safety_flags")
    if not isinstance(flags, list):
        flags = []

    market_risk = safe_float(risk_context.get("market_risk_score"))
    buy_aggression = safe_float(risk_context.get("buy_aggression_score"))
    rebound = safe_float(risk_context.get("rebound_score"))
    breakout = safe_float(risk_context.get("breakout_score"))

    max_market_risk = strategy_float(
        config,
        "risk_context_high_band_max_market_risk_score",
        0.40,
    )
    min_buy_aggression = strategy_float(
        config,
        "risk_context_high_band_min_buy_aggression_score",
        0.50,
    )
    min_rebound = strategy_float(
        config,
        "risk_context_high_band_min_rebound_score",
        0.50,
    )
    min_breakout = strategy_float(
        config,
        "risk_context_high_band_min_breakout_score",
        0.50,
    )

    if flags:
        return {
            "allowed": False,
            "reason": "risk_context_high_band_hard_safety_flag",
        }
    if market_risk is None:
        return {
            "allowed": False,
            "reason": "risk_context_high_band_missing_market_risk",
        }
    if market_risk > max_market_risk:
        return {
            "allowed": False,
            "reason": "risk_context_high_band_market_risk_high",
        }
    if (
        (buy_aggression is None or buy_aggression < min_buy_aggression)
        and (rebound is None or rebound < min_rebound)
        and (breakout is None or breakout < min_breakout)
    ):
        return {
            "allowed": False,
            "reason": "risk_context_high_band_confirmation_low",
        }

    return {"allowed": True, "reason": None}


def risk_context_position_size_adjustment(config, risk_context):
    if not strategy_bool(config, "risk_context_position_sizing_enabled", False):
        return {
            "enabled": False,
            "raw_multiplier": None,
            "clamped_multiplier": 1.0,
            "blend": 0.0,
            "effective_multiplier": 1.0,
        }

    if not isinstance(risk_context, dict):
        risk_context = {}

    weather = weather_report_payload(risk_context)
    bot_tuning = weather_bot_tuning(weather)
    blend = max(
        0.0,
        min(
            1.0,
            strategy_float(config, "risk_context_position_size_blend", 0.5),
        ),
    )
    if weather.get("emergency_bell"):
        return {
            "enabled": True,
            "raw_multiplier": 0.0,
            "clamped_multiplier": 0.0,
            "blend": blend,
            "effective_multiplier": 0.0,
        }
    raw_multiplier = safe_float(bot_tuning.get("position_size_multiplier"))
    if raw_multiplier is None:
        raw_multiplier = safe_float(risk_context.get("position_size_multiplier"))
    if raw_multiplier is None:
        return {
            "enabled": True,
            "raw_multiplier": None,
            "clamped_multiplier": 1.0,
            "blend": blend,
            "effective_multiplier": 1.0,
        }

    min_multiplier = strategy_float(
        config,
        "risk_context_position_size_min_multiplier",
        0.35,
    )
    max_multiplier = strategy_float(
        config,
        "risk_context_position_size_max_multiplier",
        1.0,
    )
    lower = min(min_multiplier, max_multiplier)
    upper = max(min_multiplier, max_multiplier)
    clamped_multiplier = max(lower, min(upper, raw_multiplier))
    effective_multiplier = 1.0 + ((clamped_multiplier - 1.0) * blend)
    return {
        "enabled": True,
        "raw_multiplier": raw_multiplier,
        "clamped_multiplier": clamped_multiplier,
        "blend": blend,
        "effective_multiplier": effective_multiplier,
    }


def find_llm_target(signal, price, llm_target_proximity_pct):
    targets = signal.get("target_prices")
    if not isinstance(targets, list):
        return None

    eligible = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        buy_price = safe_float(target.get("buy_price"))
        if buy_price is None or buy_price <= 0:
            continue
        if buy_price > price * (1 + llm_target_proximity_pct):
            continue
        eligible.append({
            "buy_price": buy_price,
            "sell_pct": safe_float(target.get("sell_pct"))
        })

    if not eligible:
        return None

    return max(eligible, key=lambda item: item["buy_price"])


def derive_range_values(snapshot):
    signal = signal_payload(snapshot)
    price_regime = signal.get("price_regime", {})
    if not isinstance(price_regime, dict):
        price_regime = {}
    state_info = state_summary(snapshot)

    low = positive_or_default(price_regime.get("price_low_24h"), positive_or_default(state_info.get("range_low")))
    high = positive_or_default(price_regime.get("price_high_24h"), positive_or_default(state_info.get("range_high")))
    mean = positive_or_default(price_regime.get("price_mean_24h"), positive_or_default(state_info.get("range_mean")))
    median = positive_or_default(price_regime.get("price_median_24h"), positive_or_default(state_info.get("range_median")))
    return low, high, mean, median


def build_candidates(snapshot, price):
    signal = signal_payload(snapshot)
    config = strategy_payload(snapshot)
    context = strategy_context(snapshot)
    state_info = state_summary(snapshot)
    action_recommendation = signal.get("action_recommendation") or "neutral"
    signal_status = signal.get("signal_status")
    source_status = signal.get("source_status", {})
    source_guard_allows_trading = signal.get("bot_action_allowed")
    if source_guard_allows_trading is None:
        source_guard_allows_trading = True
    require_fresh_signal = bool(config.get("require_fresh_signal", True))
    min_signal_status = config.get("min_signal_status", "fresh")
    operating_mode = str(
        config.get("operating_mode", "range_plus_llm") or "range_plus_llm"
    ).strip().lower()
    allow_range_buy_on_confidence_block = bool(
        config.get("allow_range_buy_on_confidence_block", False)
    )
    sentiment_control_mode = normalize_sentiment_control_mode(
        config.get("sentiment_control_mode"),
        operating_mode,
    )
    freshness_allows_trading, freshness_block_reason = source_status_allows_trading(
        signal_status,
        source_status,
        require_fresh_signal,
        min_signal_status
    )
    risk_context = risk_context_payload(signal)
    weather_report = weather_report_payload(risk_context)
    buy_permissions = sentiment_buy_permissions(
        action_recommendation,
        signal.get("action_policy"),
        operating_mode=operating_mode,
        allow_range_buy_on_confidence_block=(
            allow_range_buy_on_confidence_block
        ),
        sentiment_control_mode=sentiment_control_mode,
        weather_report=weather_report,
    )
    llm_buys_allowed = buy_permissions["llm_buys_allowed"]
    range_core_buys_allowed = buy_permissions["range_core_buys_allowed"]
    range_high_buys_allowed = buy_permissions["range_high_buys_allowed"]
    range_buys_allowed = buy_permissions["range_buys_allowed"]
    base_any_buys_allowed = buy_permissions["any_buys_allowed"]

    low, high, mean, median = derive_range_values(snapshot)
    base_strategy_modes = (
        context.get("strategy_modes")
        or parse_strategy_modes(context.get("grid_anchor"))
    )
    strategy_modes = select_dynamic_strategy_modes(
        base_strategy_modes,
        str(config.get("operating_mode", "range_plus_llm") or "range_plus_llm").strip().lower(),
        safe_float(signal.get("price_regime", {}).get("range_position_24h")),
        config,
    )
    range_modes_enabled = any(mode != "llm_target" for mode in strategy_modes)
    allow_range_fallback_without_sentiment = bool(
        config.get("allow_range_fallback_without_sentiment", True)
    )
    llm_signal_gates_allow = (
        freshness_allows_trading and bool(source_guard_allows_trading)
    )
    range_fallback_active = (
        allow_range_fallback_without_sentiment
        and range_modes_enabled
        and not llm_signal_gates_allow
    )
    range_signal_gates_allow = (
        llm_signal_gates_allow or range_fallback_active
    )
    any_buys_allowed = (
        (llm_buys_allowed and llm_signal_gates_allow)
        or (range_buys_allowed and range_signal_gates_allow)
    )
    llm_target_proximity_pct = safe_float(config.get("llm_target_proximity_pct")) or safe_float(config.get("entry_step_pct")) or 0.0
    llm_target = find_llm_target(signal, price, llm_target_proximity_pct)
    mean_reversion_min_opportunity = safe_float(config.get("mean_reversion_min_opportunity")) or 0.0
    mean_reversion_opportunity = safe_float(signal.get("mean_reversion_opportunity"))
    if mean_reversion_opportunity is None:
        mean_reversion_opportunity = 0.0
    base_entry_step_pct = safe_float(config.get("entry_step_pct")) or 0.0
    effective_step_pct = effective_entry_step_pct(
        base_entry_step_pct,
        safe_float(signal.get("price_regime", {}).get("realized_volatility_24h_pct")),
        config,
    )
    effective_max_inventory_usd = safe_float(config.get("max_inventory_usd")) or float("inf")
    deployed_inventory_usd = safe_float(state_info.get("deployed_inventory_usd")) or 0.0
    inventory_pressure = inventory_pressure_adjustment(
        deployed_inventory_usd,
        effective_max_inventory_usd,
        config,
    )
    risk_context_size_adjustment = risk_context_position_size_adjustment(
        config,
        risk_context,
    )

    result = {
        "freshness_allows_trading": freshness_allows_trading,
        "freshness_block_reason": freshness_block_reason,
        "source_guard_allows_trading": bool(source_guard_allows_trading),
        "llm_buys_allowed": llm_buys_allowed,
        "range_core_buys_allowed": range_core_buys_allowed,
        "range_high_buys_allowed": range_high_buys_allowed,
        "range_buys_allowed": range_buys_allowed,
        "any_buys_allowed": any_buys_allowed,
        "range_fallback_active": range_fallback_active,
        "action_recommendation": action_recommendation,
        "strategy_modes": strategy_modes,
        "effective_entry_step_pct": effective_step_pct,
        "inventory_pressure_usage_ratio": inventory_pressure["usage_ratio"],
        "inventory_pressure_size_multiplier": inventory_pressure["size_multiplier"],
        "risk_context_position_sizing_enabled": (
            risk_context_size_adjustment["enabled"]
        ),
        "risk_context_position_size_raw_multiplier": (
            risk_context_size_adjustment["raw_multiplier"]
        ),
        "risk_context_position_size_clamped_multiplier": (
            risk_context_size_adjustment["clamped_multiplier"]
        ),
        "risk_context_position_size_blend": (
            risk_context_size_adjustment["blend"]
        ),
        "risk_context_position_size_effective_multiplier": (
            risk_context_size_adjustment["effective_multiplier"]
        ),
        "sentiment_control_mode": sentiment_control_mode,
        "low": low,
        "high": high,
        "mean": mean,
        "median": median,
        "llm_target": llm_target,
        "llm_buy_allowed": False,
        "raw_candidates": [],
        "hold_reason": None,
        "unknown_balance_required": False,
    }

    llm_buy_allowed = (
        "llm_target" in strategy_modes
        and low and high
        and llm_target is not None
        and llm_signal_gates_allow
        and llm_buys_allowed
        and mean_reversion_opportunity >= mean_reversion_min_opportunity
    )
    result["llm_buy_allowed"] = llm_buy_allowed

    if not (
        range_signal_gates_allow
        and strategy_modes
        and low and high
        and base_any_buys_allowed
    ):
        result["hold_reason"] = (
            "buy_modes_disabled"
            if not strategy_modes
            else (
                None
                if any_buys_allowed
                else f"action_recommendation_{action_recommendation}"
            )
            or (
                None
                if range_fallback_active
                else freshness_block_reason
            )
            or (
                None
                if source_guard_allows_trading
                else "source_guard_blocked"
            )
            or "signal_below_threshold_or_range_unavailable"
        )
        return result

    max_grid_size = int(config.get("max_grid_size", 4))
    sentiment_disable_high_anchor_below = safe_float(
        config.get("sentiment_disable_high_anchor_below")
    )
    btc_sentiment = safe_float(signal.get("btc_sentiment"))
    allow_high_anchor = True
    if (
        sentiment_disable_high_anchor_below is not None
        and btc_sentiment is not None
        and btc_sentiment < sentiment_disable_high_anchor_below
    ):
        allow_high_anchor = False

    candidate_levels = []
    if llm_buy_allowed:
        candidate_levels = [{
            "level": llm_target["buy_price"],
            "sell_pct_override": llm_target["sell_pct"],
            "buy_source": "llm_target",
            "strategy_mode": "llm_target",
        }]
    else:
        for strategy_mode in strategy_modes:
            if strategy_mode == "mean" and mean is not None:
                grid = compute_grid(mean, effective_step_pct, max_grid_size)
                sell_pct_override = None
                buy_source = "range_mean"
            elif strategy_mode == "median" and median is not None:
                grid = compute_grid(median, effective_step_pct, max_grid_size)
                sell_pct_override = None
                buy_source = "range_median"
            elif strategy_mode == "high":
                if not allow_high_anchor:
                    continue
                grid = compute_high_anchor_grid(high, price, effective_step_pct)
                sell_pct_override = safe_float(config.get("high_anchor_profit_target_pct"))
                buy_source = "range_high_band"
            else:
                grid = compute_grid(low, effective_step_pct, max_grid_size)
                sell_pct_override = None
                buy_source = "range_low"

            for level in grid:
                candidate_levels.append({
                    "level": level,
                    "sell_pct_override": sell_pct_override,
                    "buy_source": buy_source,
                    "strategy_mode": strategy_mode,
                })

    deduped_candidates = []
    seen_levels = set()
    for candidate in candidate_levels:
        rounded_level = round(candidate["level"], 2)
        if rounded_level in seen_levels:
            continue
        seen_levels.add(rounded_level)
        deduped_candidates.append(candidate)

    result["raw_candidates"] = deduped_candidates
    if not deduped_candidates:
        result["hold_reason"] = "no_candidates"
    return result


def evaluate_candidate(snapshot, candidate, price):
    config = strategy_payload(snapshot)
    state_info = state_summary(snapshot)
    state_data = state_payload(snapshot)
    signal = signal_payload(snapshot)
    risk_context = risk_context_payload(signal)
    weather_report = weather_report_payload(risk_context)
    buy_permissions = sentiment_buy_permissions(
        signal.get("action_recommendation") or "neutral",
        signal.get("action_policy"),
        operating_mode=str(
            config.get("operating_mode", "range_plus_llm") or "range_plus_llm"
        ).strip().lower(),
        allow_range_buy_on_confidence_block=bool(
            config.get("allow_range_buy_on_confidence_block", False)
        ),
        sentiment_control_mode=config.get("sentiment_control_mode"),
        weather_report=weather_report,
    )
    llm_buys_allowed = buy_permissions["llm_buys_allowed"]
    range_core_buys_allowed = buy_permissions["range_core_buys_allowed"]
    range_high_buys_allowed = buy_permissions["range_high_buys_allowed"]
    allow_range_fallback_without_sentiment = bool(
        config.get("allow_range_fallback_without_sentiment", True)
    )
    signal_status = signal.get("signal_status")
    source_status = signal.get("source_status", {})
    source_guard_allows_trading = signal.get("bot_action_allowed")
    if source_guard_allows_trading is None:
        source_guard_allows_trading = True
    require_fresh_signal = bool(config.get("require_fresh_signal", True))
    min_signal_status = config.get("min_signal_status", "fresh")
    freshness_allows_trading, freshness_block_reason = source_status_allows_trading(
        signal_status,
        source_status,
        require_fresh_signal,
        min_signal_status
    )
    llm_signal_gates_allow = (
        freshness_allows_trading and bool(source_guard_allows_trading)
    )
    range_fallback_active = (
        allow_range_fallback_without_sentiment
        and any(mode != "llm_target" for mode in (strategy_context(snapshot).get("strategy_modes") or []))
        and not llm_signal_gates_allow
    )
    range_signal_gates_allow = llm_signal_gates_allow or range_fallback_active

    open_buy_orders = state_data.get("open_buy_orders") or []
    open_sell_orders = state_data.get("open_sell_orders") or []
    open_buy_levels = {str(order.get("level")) for order in open_buy_orders if order.get("level") is not None}
    open_sell_levels = {str(order.get("level")) for order in open_sell_orders if order.get("level") is not None}
    buy_source = candidate["buy_source"]
    level = safe_float(candidate["level"]) or 0.0
    key = str(candidate["level"])

    last_sell_price = positive_or_default(state_info.get("last_sell_price"))
    prevent_buy_above_last_sell = bool(config.get("prevent_buy_above_last_sell", True))
    buy_after_sell_discount_pct = safe_float(config.get("buy_after_sell_discount_pct")) or 0.001
    mean_reversion_opportunity = safe_float(signal.get("mean_reversion_opportunity")) or 0.0
    mean_reversion_min_opportunity = safe_float(config.get("mean_reversion_min_opportunity")) or 0.0
    flow_pressure = safe_float(signal.get("flow_pressure"))
    llm_buy_cooldown_minutes_after_sell = int(config.get("llm_buy_cooldown_minutes_after_sell", 30))
    high_anchor_buy_cooldown_minutes = int(config.get("high_anchor_buy_cooldown_minutes", 15))
    max_open_high_anchor_orders = int(config.get("max_open_high_anchor_orders", 3))
    max_open_sell_orders = int(config.get("max_open_sell_orders", 999999))
    max_inventory_usd = safe_float(config.get("max_inventory_usd")) or float("inf")
    deployed_inventory_usd = safe_float(state_info.get("deployed_inventory_usd")) or 0.0

    if key in open_buy_levels:
        return False, "open_buy_order"
    if key in open_sell_levels:
        return False, "open_sell_order"
    if (
        prevent_buy_above_last_sell
        and last_sell_price is not None
        and level > (last_sell_price * (1 - buy_after_sell_discount_pct))
    ):
        return False, "above_last_sell_discount"
    if mean_reversion_opportunity < mean_reversion_min_opportunity:
        return False, "mean_reversion_opportunity_below_min"
    if price_is_above_allowed_entry(price, level, config, buy_source):
        return False, "price_above_level"
    if int(state_info.get("open_sell_count") or 0) >= max_open_sell_orders:
        return False, "max_open_sell_orders"
    if deployed_inventory_usd >= max_inventory_usd:
        return False, "max_inventory_usd"
    if buy_source == "llm_target" and not llm_signal_gates_allow:
        return False, freshness_block_reason or "source_guard_blocked"
    if buy_source != "llm_target" and not range_signal_gates_allow:
        return False, freshness_block_reason or "source_guard_blocked"
    if buy_source == "llm_target" and not llm_buys_allowed:
        return False, "sentiment_action_not_bullish_allowed"
    if buy_source == "range_high_band" and not range_high_buys_allowed:
        return False, "sentiment_action_not_high_range_permitted"
    if buy_source != "llm_target" and not range_core_buys_allowed:
        return False, "sentiment_action_not_range_permitted"
    if buy_source == "range_high_band":
        high_band_guard = risk_context_high_band_guard(
            config,
            risk_context,
        )
        if not high_band_guard["allowed"]:
            return False, high_band_guard["reason"]

    flow_control = flow_adjustment(config, flow_pressure, buy_source)
    if flow_control["block_buy"]:
        return False, flow_control["reason"]

    now = snapshot_timestamp(snapshot)
    last_llm_sell_at = parse_iso8601(state_info.get("last_llm_sell_at"))
    if (
        buy_source == "llm_target"
        and last_llm_sell_at is not None
        and now is not None
        and (now - last_llm_sell_at).total_seconds() / 60 < llm_buy_cooldown_minutes_after_sell
    ):
        return False, "llm_sell_cooldown"

    last_high_anchor_buy_at = parse_iso8601(state_info.get("last_high_anchor_buy_at"))
    if (
        buy_source == "range_high_band"
        and last_high_anchor_buy_at is not None
        and now is not None
        and (now - last_high_anchor_buy_at).total_seconds() / 60 < high_anchor_buy_cooldown_minutes
    ):
        return False, "high_anchor_cooldown"

    high_anchor_order_count = sum(
        1
        for order in open_buy_orders + open_sell_orders
        if order.get("buy_source") == "range_high_band"
    )
    if (
        buy_source == "range_high_band"
        and high_anchor_order_count >= max_open_high_anchor_orders
    ):
        return False, "max_open_high_anchor_orders"

    return True, None


def empty_replay_summary():
    return {
        "snapshots": 0,
        "valid_price_snapshots": 0,
        "missing_signal": 0,
        "missing_price": 0,
        "hold_snapshots": 0,
        "raw_candidates": 0,
        "approved_candidates": 0,
        "unknown_balance_candidates": 0,
        "hold_reason_counts": {},
        "hold_action_recommendation_counts": {},
        "hold_action_policy_reason_counts": {},
        "hold_signal_status_counts": {},
        "hold_active_strategy_mode_counts": {},
        "blocked_reason_counts": {},
        "candidate_counts_by_source": {},
        "candidate_counts_by_strategy_mode": {},
        "approved_counts_by_source": {},
        "approved_counts_by_strategy_mode": {},
    }


def replay_from_snapshots(snapshots):
    summary = empty_replay_summary()
    recent = []
    recent_approved = []
    approved_events = []
    hold_reason_counts = Counter()
    hold_action_recommendation_counts = Counter()
    hold_action_policy_reason_counts = Counter()
    hold_signal_status_counts = Counter()
    hold_active_strategy_mode_counts = Counter()
    blocked_reason_counts = Counter()
    candidate_counts_by_source = Counter()
    candidate_counts_by_strategy_mode = Counter()
    approved_counts_by_source = Counter()
    approved_counts_by_strategy_mode = Counter()

    for snapshot in snapshots:
        summary["snapshots"] += 1
        price = safe_float((snapshot.get("ticker") or {}).get("last_price"))
        if price is None:
            signal = signal_payload(snapshot)
            price = safe_float(signal.get("btc_price"))
        if price is None:
            summary["missing_price"] += 1
            continue
        summary["valid_price_snapshots"] += 1

        signal = signal_payload(snapshot)
        if not signal:
            summary["missing_signal"] += 1
            continue
        sentiment_risk_fields = sentiment_risk_event_fields(signal)

        built = build_candidates(snapshot, price)
        if built["hold_reason"] is not None:
            summary["hold_snapshots"] += 1
            hold_reason_counts[built["hold_reason"]] += 1
            action_recommendation = built.get("action_recommendation") or "unknown"
            hold_action_recommendation_counts[action_recommendation] += 1
            signal_status = signal.get("signal_status") or "unknown"
            hold_signal_status_counts[signal_status] += 1
            active_strategy_modes = built.get("strategy_modes") or []
            if active_strategy_modes:
                for strategy_mode in active_strategy_modes:
                    hold_active_strategy_mode_counts[strategy_mode] += 1
            else:
                hold_active_strategy_mode_counts["none"] += 1
            action_policy = signal.get("action_policy")
            if isinstance(action_policy, dict):
                action_policy_reason = action_policy.get("reason")
            else:
                action_policy_reason = None
            if action_policy_reason:
                hold_action_policy_reason_counts[str(action_policy_reason)] += 1
            recent.append({
                "captured_at": snapshot.get("captured_at"),
                "price": price,
                "action_recommendation": action_recommendation,
                "action_policy_reason": action_policy_reason,
                "signal_status": signal_status,
                "active_strategy_modes": active_strategy_modes,
                "effective_entry_step_pct": built.get("effective_entry_step_pct"),
                "inventory_pressure_usage_ratio": built.get(
                    "inventory_pressure_usage_ratio"
                ),
                "inventory_pressure_size_multiplier": built.get(
                    "inventory_pressure_size_multiplier"
                ),
                "risk_context_position_sizing_enabled": built.get(
                    "risk_context_position_sizing_enabled"
                ),
                "risk_context_position_size_effective_multiplier": built.get(
                    "risk_context_position_size_effective_multiplier"
                ),
                "hold_reason": built["hold_reason"],
                "raw_candidate_count": len(built["raw_candidates"]),
                **sentiment_risk_fields,
            })
            continue

        for candidate in built["raw_candidates"]:
            summary["raw_candidates"] += 1
            candidate_counts_by_source[candidate["buy_source"]] += 1
            candidate_counts_by_strategy_mode[candidate["strategy_mode"]] += 1
            approved, reason = evaluate_candidate(snapshot, candidate, price)
            if approved:
                summary["approved_candidates"] += 1
                approved_counts_by_source[candidate["buy_source"]] += 1
                approved_counts_by_strategy_mode[candidate["strategy_mode"]] += 1
                approved_event = {
                    "captured_at": snapshot.get("captured_at"),
                    "price": price,
                    "active_strategy_modes": built.get("strategy_modes") or [],
                    "effective_entry_step_pct": built.get("effective_entry_step_pct"),
                    "inventory_pressure_usage_ratio": built.get(
                        "inventory_pressure_usage_ratio"
                    ),
                    "inventory_pressure_size_multiplier": built.get(
                        "inventory_pressure_size_multiplier"
                    ),
                    "risk_context_position_sizing_enabled": built.get(
                        "risk_context_position_sizing_enabled"
                    ),
                    "risk_context_position_size_effective_multiplier": built.get(
                        "risk_context_position_size_effective_multiplier"
                    ),
                    "buy_source": candidate["buy_source"],
                    "strategy_mode": candidate["strategy_mode"],
                    "level": round(candidate["level"], 2),
                    "sell_pct_override": candidate.get("sell_pct_override"),
                    "status": "approved_gate_only",
                    "reason": None,
                    **sentiment_risk_fields,
                }
                recent.append(approved_event)
                recent_approved.append(approved_event)
                approved_events.append(approved_event)
            else:
                blocked_reason_counts[reason] += 1
                recent.append({
                    "captured_at": snapshot.get("captured_at"),
                    "price": price,
                    "active_strategy_modes": built.get("strategy_modes") or [],
                    "effective_entry_step_pct": built.get("effective_entry_step_pct"),
                    "inventory_pressure_usage_ratio": built.get(
                        "inventory_pressure_usage_ratio"
                    ),
                    "inventory_pressure_size_multiplier": built.get(
                        "inventory_pressure_size_multiplier"
                    ),
                    "risk_context_position_sizing_enabled": built.get(
                        "risk_context_position_sizing_enabled"
                    ),
                    "risk_context_position_size_effective_multiplier": built.get(
                        "risk_context_position_size_effective_multiplier"
                    ),
                    "buy_source": candidate["buy_source"],
                    "strategy_mode": candidate["strategy_mode"],
                    "level": round(candidate["level"], 2),
                    "status": "blocked_gate_only",
                    "reason": reason,
                    **sentiment_risk_fields,
                })

    summary["hold_reason_counts"] = dict(hold_reason_counts.most_common())
    summary["hold_action_recommendation_counts"] = dict(
        hold_action_recommendation_counts.most_common()
    )
    summary["hold_action_policy_reason_counts"] = dict(
        hold_action_policy_reason_counts.most_common()
    )
    summary["hold_signal_status_counts"] = dict(
        hold_signal_status_counts.most_common()
    )
    summary["hold_active_strategy_mode_counts"] = dict(
        hold_active_strategy_mode_counts.most_common()
    )
    summary["blocked_reason_counts"] = dict(blocked_reason_counts.most_common())
    summary["candidate_counts_by_source"] = dict(candidate_counts_by_source.most_common())
    summary["candidate_counts_by_strategy_mode"] = dict(
        candidate_counts_by_strategy_mode.most_common()
    )
    summary["approved_counts_by_source"] = dict(approved_counts_by_source.most_common())
    summary["approved_counts_by_strategy_mode"] = dict(
        approved_counts_by_strategy_mode.most_common()
    )
    summary["approved_sentiment_risk"] = summarize_sentiment_risk_events(
        approved_events
    )

    return {
        "summary": summary,
        "recent_replay_events": recent[-BACKTEST_RECENT_LIMIT:],
        "recent_approved_events": recent_approved[-BACKTEST_RECENT_LIMIT:],
        "approved_events": approved_events,
        "replay_scope": "gate_only_no_private_balance_or_fill_simulation",
    }


def summarize_actual_trades(events):
    summary = {
        "events": len(events),
        "buy_orders_placed": 0,
        "buy_orders_filled": 0,
        "sell_orders_placed": 0,
        "sell_orders_filled": 0,
        "order_rejected": 0,
        "sell_order_repriced": 0,
        "risk_context_paper_buys_planned": 0,
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0,
        "buy_orders_placed_by_source": {},
        "risk_context_paper_buys_by_source": {},
        "buy_filled_by_source": {},
        "sell_filled_by_source": {},
        "rejected_by_side": {},
        "recent_fills": [],
        "recent_buy_orders": [],
        "recent_risk_context_paper_buys": [],
    }
    buy_orders_placed_by_source = Counter()
    risk_context_paper_buys_by_source = Counter()
    buy_filled_by_source = Counter()
    sell_filled_by_source = Counter()
    rejected_by_side = Counter()
    hold_minutes = []

    for event in events:
        name = event.get("event")
        if name == "BUY_ORDER_PLACED":
            summary["buy_orders_placed"] += 1
            buy_orders_placed_by_source[event.get("buy_source") or "unknown"] += 1
            summary["recent_buy_orders"].append({
                "ts": event.get("ts"),
                "buy_source": event.get("buy_source"),
                "price": event.get("price"),
                "volume": event.get("volume"),
            })
        elif name == "RISK_CONTEXT_PAPER_BUY_PLANNED":
            summary["risk_context_paper_buys_planned"] += 1
            risk_context_paper_buys_by_source[
                event.get("buy_source") or "unknown"
            ] += 1
            summary["recent_risk_context_paper_buys"].append({
                "ts": event.get("ts"),
                "buy_source": event.get("buy_source"),
                "level": event.get("level"),
                "volume": event.get("volume"),
                "trade_notional_usd": event.get("trade_notional_usd"),
                "risk_context_position_size_effective_multiplier": (
                    event.get(
                        "risk_context_position_size_effective_multiplier"
                    )
                ),
                "sentiment_risk_posture": event.get("sentiment_risk_posture"),
            })
        elif name == "BUY_ORDER_FILLED":
            summary["buy_orders_filled"] += 1
            buy_filled_by_source[event.get("buy_source") or "unknown"] += 1
        elif name == "SELL_ORDER_PLACED":
            summary["sell_orders_placed"] += 1
        elif name == "SELL_ORDER_FILLED":
            summary["sell_orders_filled"] += 1
            sell_filled_by_source[event.get("buy_source") or "unknown"] += 1
            summary["realized_gross_pnl"] += safe_float(event.get("gross_pnl")) or 0.0
            summary["realized_estimated_net_pnl"] += safe_float(event.get("estimated_net_pnl")) or 0.0
            if safe_float(event.get("hold_minutes")) is not None:
                hold_minutes.append(float(event["hold_minutes"]))
            summary["recent_fills"].append({
                "ts": event.get("ts"),
                "side": "sell",
                "buy_source": event.get("buy_source"),
                "level": event.get("level"),
                "gross_pnl": event.get("gross_pnl"),
                "estimated_net_pnl": event.get("estimated_net_pnl"),
                "hold_minutes": event.get("hold_minutes"),
            })
        elif name == "ORDER_REJECTED":
            summary["order_rejected"] += 1
            rejected_by_side[event.get("side") or "unknown"] += 1
        elif name == "SELL_ORDER_REPRICED":
            summary["sell_order_repriced"] += 1

    summary["buy_orders_placed_by_source"] = dict(
        buy_orders_placed_by_source.most_common()
    )
    summary["risk_context_paper_buys_by_source"] = dict(
        risk_context_paper_buys_by_source.most_common()
    )
    summary["buy_filled_by_source"] = dict(buy_filled_by_source.most_common())
    summary["sell_filled_by_source"] = dict(sell_filled_by_source.most_common())
    summary["rejected_by_side"] = dict(rejected_by_side.most_common())
    summary["realized_gross_pnl"] = round(summary["realized_gross_pnl"], 8)
    summary["realized_estimated_net_pnl"] = round(summary["realized_estimated_net_pnl"], 8)
    summary["average_hold_minutes"] = round(statistics.mean(hold_minutes), 2) if hold_minutes else None
    summary["recent_fills"] = summary["recent_fills"][-BACKTEST_RECENT_LIMIT:]
    summary["recent_buy_orders"] = summary["recent_buy_orders"][-BACKTEST_RECENT_LIMIT:]
    summary["recent_risk_context_paper_buys"] = (
        summary["recent_risk_context_paper_buys"][-BACKTEST_RECENT_LIMIT:]
    )
    return summary


def summarize_missed_approved_opportunities(replay, actual, snapshots=None):
    approved_by_source = replay["summary"].get("approved_counts_by_source", {})
    live_buys_by_source = actual.get("buy_orders_placed_by_source", {})

    missing_by_source = {}
    for source, approved_count in approved_by_source.items():
        live_count = int(live_buys_by_source.get(source, 0) or 0)
        missing_count = max(0, int(approved_count or 0) - live_count)
        if missing_count:
            missing_by_source[source] = missing_count

    total_missing = sum(missing_by_source.values())
    total_approved = int(replay["summary"].get("approved_candidates", 0) or 0)
    placement_rate = (
        round(actual.get("buy_orders_placed", 0) / total_approved, 4)
        if total_approved > 0
        else None
    )
    remaining_by_source = dict(missing_by_source)
    snapshot_by_timestamp = {}
    for snapshot in snapshots or []:
        captured_at = snapshot.get("captured_at")
        if captured_at:
            snapshot_by_timestamp[captured_at] = snapshot
    approved_events = replay.get("approved_events") or []
    blocker_counter = Counter()
    potential_results = []
    missed_examples = []
    for event in approved_events:
        source = event.get("buy_source") or "unknown"
        if int(missing_by_source.get(source, 0) or 0) <= 0:
            continue
        snapshot = snapshot_by_timestamp.get(event.get("captured_at"))
        blockers = infer_live_only_blockers(snapshot, event) if snapshot else []
        for blocker in blockers:
            blocker_counter[blocker] += 1
        potential = (
            simulate_missed_opportunity(snapshot, event, snapshots or [])
            if snapshot
            else None
        )
        if potential:
            potential_results.append(potential)
        missed_examples.append({
            "captured_at": event.get("captured_at"),
            "buy_source": source,
            "price": event.get("price"),
            "level": event.get("level"),
            "status": "approved_but_not_placed",
            "likely_live_blockers": blockers,
            "potential": potential,
        })

    recent_examples = []
    recent_approved_events = (
        replay.get("recent_approved_events") or replay.get("recent_replay_events", [])
    )
    for event in reversed(recent_approved_events):
        if event.get("status") != "approved_gate_only":
            continue
        source = event.get("buy_source") or "unknown"
        remaining = int(remaining_by_source.get(source, 0) or 0)
        if remaining <= 0:
            continue
        snapshot = snapshot_by_timestamp.get(event.get("captured_at"))
        blockers = infer_live_only_blockers(snapshot, event) if snapshot else []
        potential = (
            simulate_missed_opportunity(snapshot, event, snapshots or [])
            if snapshot
            else None
        )
        recent_examples.append({
            "captured_at": event.get("captured_at"),
            "buy_source": source,
            "price": event.get("price"),
            "level": event.get("level"),
            "status": "approved_but_not_placed",
            "likely_live_blockers": blockers,
            "potential": potential,
        })
        remaining_by_source[source] = remaining - 1
        if sum(remaining_by_source.values()) <= 0:
            break
    recent_examples.reverse()

    end_returns = [
        result["end_return_pct"]
        for result in potential_results
        if result.get("end_return_pct") is not None
    ]
    max_runups = [
        result["max_runup_pct"]
        for result in potential_results
        if result.get("max_runup_pct") is not None
    ]
    max_drawdowns = [
        result["max_drawdown_pct"]
        for result in potential_results
        if result.get("max_drawdown_pct") is not None
    ]
    take_profit_count = sum(
        1 for result in potential_results if result.get("take_profit_reached")
    )
    profitable_end_count = sum(1 for value in end_returns if value > 0)
    potential_summary = {
        "evaluated_count": len(potential_results),
        "take_profit_reached_count": take_profit_count,
        "take_profit_reached_rate": (
            round(take_profit_count / len(potential_results), 4)
            if potential_results
            else None
        ),
        "profitable_at_window_end_count": profitable_end_count,
        "profitable_at_window_end_rate": (
            round(profitable_end_count / len(end_returns), 4)
            if end_returns
            else None
        ),
        "avg_end_return_pct": (
            round(statistics.mean(end_returns), 6)
            if end_returns
            else None
        ),
        "median_end_return_pct": (
            round(statistics.median(end_returns), 6)
            if end_returns
            else None
        ),
        "best_end_return_pct": max(end_returns) if end_returns else None,
        "worst_end_return_pct": min(end_returns) if end_returns else None,
        "avg_max_runup_pct": (
            round(statistics.mean(max_runups), 6)
            if max_runups
            else None
        ),
        "avg_max_drawdown_pct": (
            round(statistics.mean(max_drawdowns), 6)
            if max_drawdowns
            else None
        ),
        "assumptions": [
            "Entry assumed at the approved replay level.",
            f"Opportunity path measured over the next {BACKTEST_POTENTIAL_MAX_HOLD_HOURS:g} hours of captured snapshots.",
            "Potential takes profit when the configured target is first reached; otherwise end_return_pct is marked to the end of the hold window.",
            "This does not model exchange fills, fees, slippage, or stop-loss exits."
        ],
    }

    return {
        "approved_candidates": total_approved,
        "actual_buy_orders_placed": actual.get("buy_orders_placed", 0),
        "approved_but_not_placed": total_missing,
        "approved_but_not_placed_by_source": missing_by_source,
        "likely_live_blockers": dict(blocker_counter.most_common()),
        "potential_summary": potential_summary,
        "placement_rate_vs_approved": placement_rate,
        "recent_approved_but_not_placed": recent_examples[-BACKTEST_RECENT_LIMIT:],
        "notes": [
            "This is a gate-level comparison only.",
            "A missed approved opportunity means replay approved a buy candidate but no corresponding live BUY_ORDER_PLACED was seen in the reporting window.",
            "This does not yet model exchange fills, private-balance constraints, or one-to-one candidate-to-order matching."
        ],
    }


def build_report(window_hours=None):
    now = now_utc()
    effective_window_hours = (
        BACKTEST_WINDOW_HOURS
        if window_hours is None
        else float(window_hours)
    )
    since_dt = now - timedelta(hours=effective_window_hours)
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

    trade_event_files = trade_event_source_files(since_dt, now)
    all_events = []
    for path in trade_event_files:
        all_events.extend(load_jsonl(path))
    events = [
        event
        for event in all_events
        if (parse_iso8601(event.get("ts")) or datetime.min.replace(tzinfo=timezone.utc)) >= since_dt
    ]

    replay = replay_from_snapshots(snapshots)
    actual = summarize_actual_trades(events)
    missed = summarize_missed_approved_opportunities(replay, actual, snapshots)
    replay_output = dict(replay)
    replay_output.pop("approved_events", None)
    strategy_comparison = None
    if BACKTEST_STRATEGY_SET_FILE:
        strategy_comparison = build_strategy_comparison_rows(
            snapshots,
            BACKTEST_STRATEGY_SET_FILE,
        )

    report = {
        "timestamp": now.isoformat(),
        "snapshot_file_base": display_source_path(SNAPSHOT_LOG_FILE),
        "snapshot_files": snapshot_files,
        "trade_log_file": display_source_path(TRADE_LOG_FILE),
        "activity_log_file": display_source_path(ACTIVITY_LOG_FILE) if ACTIVITY_LOG_FILE else None,
        "trade_event_files": trade_event_files,
        "since": since_dt.isoformat(),
        "snapshot_count": len(snapshots),
        "trade_event_count": len(events),
        "replay": replay_output,
        "actual_live": actual,
        "missed_opportunities": missed,
        "top_summary": {
            "replay_scope": replay["replay_scope"],
            "approved_candidates": replay["summary"]["approved_candidates"],
            "raw_candidates": replay["summary"]["raw_candidates"],
            "actual_buy_orders_placed": actual["buy_orders_placed"],
            "approved_but_not_placed": missed["approved_but_not_placed"],
            "placement_rate_vs_approved": missed["placement_rate_vs_approved"],
            "actual_buy_orders_filled": actual["buy_orders_filled"],
            "actual_sell_orders_filled": actual["sell_orders_filled"],
            "actual_realized_estimated_net_pnl": actual["realized_estimated_net_pnl"],
            "notes": [
                "Replay is gate-level only and does not model private-balance checks or exchange fill mechanics.",
                "Use this to understand opportunity flow, blocking reasons, and live-vs-opportunity comparisons."
            ],
        },
    }
    if strategy_comparison is not None:
        report["strategy_comparison"] = strategy_comparison
    return report


def write_report(report):
    output_dir = os.path.dirname(BACKTEST_OUTPUT_FILE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(BACKTEST_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    os.makedirs(BACKTEST_ARCHIVE_DIR, exist_ok=True)
    archive_file = os.path.join(
        BACKTEST_ARCHIVE_DIR,
        f"range_grid_backtest_{now_utc().strftime('%Y%m%d')}.json"
    )
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    comparison_csv_file = None
    ranked_comparison_csv_file = None
    anchor_winners_file = None
    if report.get("strategy_comparison"):
        comparison_csv_file = write_strategy_comparison_csv(
            report["strategy_comparison"],
            BACKTEST_STRATEGY_COMPARE_CSV_FILE,
        )
        ranked_comparison_csv_file = write_ranked_strategy_csv(
            report["strategy_comparison"],
            BACKTEST_STRATEGY_RANKED_CSV_FILE,
        )
        anchor_winners_file = write_anchor_winners_json(
            report["strategy_comparison"],
            BACKTEST_ANCHOR_WINNERS_FILE,
        )

    return (
        archive_file,
        comparison_csv_file,
        ranked_comparison_csv_file,
        anchor_winners_file,
    )


def main():
    args = parse_args()
    report = build_report(window_hours=args.window_hours)
    (
        archive_file,
        comparison_csv_file,
        ranked_comparison_csv_file,
        anchor_winners_file,
    ) = write_report(report)
    print(json.dumps({
        "timestamp": report["timestamp"],
        "output_file": BACKTEST_OUTPUT_FILE,
        "archive_file": archive_file,
        "snapshot_count": report["snapshot_count"],
        "trade_event_count": report["trade_event_count"],
        "window_hours": args.window_hours if args.window_hours is not None else BACKTEST_WINDOW_HOURS,
        "strategy_comparison_csv_file": comparison_csv_file,
        "strategy_ranked_csv_file": ranked_comparison_csv_file,
        "anchor_winners_file": anchor_winners_file,
    }))


if __name__ == "__main__":
    main()
