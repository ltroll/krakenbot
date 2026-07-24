#!/usr/bin/env python3

import argparse
import bisect
import copy
import csv
import glob
import json
import os
import statistics
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from signal_normalizer import normalize_signal_payload


ENV_FILE = (
    os.getenv("RANGE_GRID_ENV_FILE")
    or os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
)
load_dotenv(dotenv_path=ENV_FILE, override=False)

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
BACKTEST_INCLUDE_STRATEGY_DETAILS = os.getenv(
    "RANGE_GRID_BACKTEST_INCLUDE_STRATEGY_DETAILS",
    "true"
).strip().lower() in ("1", "true", "yes", "on")
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
    parser.add_argument(
        "--strategy-summary-only",
        action="store_true",
        help=(
            "Write strategy CSV/ranked output without bulky per-strategy "
            "details in the JSON report."
        ),
    )
    parser.add_argument(
        "--include-strategy-details",
        action="store_true",
        help="Force full per-strategy details in the JSON report.",
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


def size_rotated_source_files(base_path):
    if not base_path or is_http_url(base_path):
        return []

    root, ext = os.path.splitext(os.path.abspath(base_path))
    pattern = f"{root}_[0-9][0-9][0-9][0-9]*{ext or '.jsonl'}"
    paths = [
        path
        for path in glob.glob(pattern)
        if os.path.isfile(path)
    ]
    paths.sort(key=lambda path: os.path.getmtime(path))
    return [display_source_path(path) for path in paths]


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

    size_rotated_files = size_rotated_source_files(base_path)
    if size_rotated_files:
        files = list(size_rotated_files)
        if is_http_url(base_path) or os.path.exists(base_path):
            files.append(display_source_path(base_path))
        return list(dict.fromkeys(files))

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


def buy_above_last_sell_guard_active(last_sell_at, now, guard_minutes):
    if guard_minutes <= 0:
        return True
    if last_sell_at is None or now is None:
        return True
    elapsed_minutes = (now - last_sell_at).total_seconds() / 60
    return elapsed_minutes <= guard_minutes


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
    cloned = dict(snapshot)
    cloned["strategy_profile"] = {
        "path": strategy_path,
        "label": strategy_label,
        "payload": copy.deepcopy(strategy_payload),
    }
    context = cloned.get("strategy_context")
    if not isinstance(context, dict):
        context = {}
    else:
        context = dict(context)
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


def weather_market_stability(weather_report):
    stability = (
        weather_report.get("market_stability")
        if isinstance(weather_report, dict)
        else {}
    )
    return stability if isinstance(stability, dict) else {}


def weather_trend_pressure(weather_report):
    pressure = (
        weather_report.get("trend_pressure")
        if isinstance(weather_report, dict)
        else {}
    )
    return pressure if isinstance(pressure, dict) else {}


def weather_market_opportunity(weather_report):
    opportunity = (
        weather_report.get("market_opportunity")
        if isinstance(weather_report, dict)
        else {}
    )
    return opportunity if isinstance(opportunity, dict) else {}


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


def weather_high_anchor_tailwind(weather_report):
    if not weather_bot_decides(weather_report):
        return False
    if weather_report.get("emergency_bell"):
        return False
    if weather_report.get("alert_level") == "danger":
        return False
    condition = str(weather_report.get("condition") or "").strip().lower()
    tags = set(weather_list(weather_report.get("opportunity_tags")))
    constructive = {
        "constructive",
        "rebound_tailwind",
        "breakout_tailwind",
    }
    return condition in constructive or bool(tags & constructive)


def weather_leveling_score(weather_report):
    stability = weather_market_stability(weather_report)
    state = str(stability.get("leveling_state") or "").strip().lower()
    score = safe_float(stability.get("leveling_score"))
    return state, score


def config_token_set(config, key, default=""):
    raw = config.get(key, default) if isinstance(config, dict) else default
    if isinstance(raw, (list, tuple, set)):
        return {
            str(item).strip().lower()
            for item in raw
            if str(item).strip()
        }
    return {
        token.strip().lower()
        for token in str(raw or "").split(",")
        if token.strip()
    }


def high_band_leveling_size_multiplier(config, buy_source, weather_report):
    if buy_source != "range_high_band":
        return 1.0
    state, score = weather_leveling_score(weather_report)
    if state != "leveling" or score is None:
        return 1.0
    threshold = strategy_float(
        config,
        "weather_leveling_high_band_size_threshold",
        1.01,
    )
    if score < threshold:
        return 1.0
    return max(
        0.0,
        min(
            1.0,
            strategy_float(
                config,
                "weather_leveling_high_band_size_multiplier",
                1.0,
            ),
        ),
    )


def weather_leveling_blocks_high_band_bypass(config, weather_report):
    state, score = weather_leveling_score(weather_report)
    if state != "leveling" or score is None:
        return False
    threshold = strategy_float(
        config,
        "weather_leveling_high_band_bypass_block_threshold",
        1.01,
    )
    return score >= threshold


def allow_above_last_sell_for_candidate(config, buy_source, weather_report):
    if buy_source != "range_high_band":
        return False
    if not strategy_bool(
        config,
        "allow_high_band_breakout_above_last_sell",
        False,
    ):
        return False
    if weather_leveling_blocks_high_band_bypass(config, weather_report):
        return False
    if not weather_high_anchor_tailwind(weather_report):
        return False
    if not strategy_bool(
        config,
        "high_band_breakout_bypass_quality_gate_enabled",
        False,
    ):
        return True

    risk_context = weather_report.get("_risk_context") or {}
    market_opportunity = weather_market_opportunity(weather_report)
    phase = str(market_opportunity.get("cycle_phase") or "").strip().lower()
    breakout_score = safe_float(risk_context.get("breakout_score")) or 0.0
    rebound_score = safe_float(risk_context.get("rebound_score")) or 0.0
    exit_pressure_score = safe_float(
        market_opportunity.get("exit_pressure_score")
    )
    hold_through_score = safe_float(
        market_opportunity.get("hold_through_score")
    )
    exit_pressure_score = (
        1.0 if exit_pressure_score is None else exit_pressure_score
    )
    hold_through_score = 0.0 if hold_through_score is None else hold_through_score

    if exit_pressure_score > strategy_float(
        config,
        "high_band_breakout_bypass_max_exit_pressure_score",
        0.60,
    ):
        return False
    if hold_through_score < strategy_float(
        config,
        "high_band_breakout_bypass_min_hold_through_score",
        0.50,
    ):
        return False
    if phase == "momentum_ride" and hold_through_score < strategy_float(
        config,
        "high_band_breakout_bypass_momentum_min_hold_through_score",
        0.58,
    ):
        return False
    return (
        breakout_score >= strategy_float(
            config,
            "high_band_breakout_bypass_min_breakout_score",
            0.62,
        )
        or rebound_score >= strategy_float(
            config,
            "high_band_breakout_bypass_min_rebound_score",
            0.68,
        )
    )


def sentiment_risk_event_fields(signal):
    risk_context = risk_context_payload(signal)
    flags = risk_context.get("hard_safety_flags")
    if not isinstance(flags, list):
        flags = []
    weather = weather_report_payload(risk_context)
    bot_tuning = weather_bot_tuning(weather)
    market_stability = weather_market_stability(weather)
    trend_pressure = weather_trend_pressure(weather)
    market_opportunity = weather_market_opportunity(weather)
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
        "weather_leveling_state": market_stability.get("leveling_state"),
        "weather_leveling_score": safe_float(
            market_stability.get("leveling_score")
        ),
        "weather_stabilization_score": safe_float(
            market_stability.get("stabilization_score")
        ),
        "weather_short_term_direction": trend_pressure.get(
            "short_term_direction"
        ),
        "weather_downtrend_strength": safe_float(
            trend_pressure.get("downtrend_strength")
        ),
        "weather_uptrend_strength": safe_float(
            trend_pressure.get("uptrend_strength")
        ),
        "weather_lower_highs_lower_lows": (
            bool(trend_pressure.get("lower_highs_lower_lows"))
            if "lower_highs_lower_lows" in trend_pressure
            else None
        ),
        "weather_falling_tape": (
            bool(trend_pressure.get("falling_tape"))
            if "falling_tape" in trend_pressure
            else None
        ),
        "weather_opportunity_phase": market_opportunity.get("cycle_phase"),
        "weather_opportunity_bot_hint": market_opportunity.get("bot_hint"),
        "weather_entry_opportunity_score": safe_float(
            market_opportunity.get("entry_opportunity_score")
        ),
        "weather_rebound_confirmation_score": safe_float(
            market_opportunity.get("rebound_confirmation_score")
        ),
        "weather_exit_pressure_score": safe_float(
            market_opportunity.get("exit_pressure_score")
        ),
        "weather_hold_through_score": safe_float(
            market_opportunity.get("hold_through_score")
        ),
        "weather_failed_rebound_risk": safe_float(
            market_opportunity.get("failed_rebound_risk")
        ),
        "weather_long_entry_noise_risk": safe_float(
            market_opportunity.get("long_entry_noise_risk")
        ),
        "weather_pattern_tags": weather_list(
            market_opportunity.get("pattern_tags")
        ),
    }
    for source_key, output_key in RISK_CONTEXT_NUMERIC_FIELDS.items():
        fields[output_key] = safe_float(risk_context.get(source_key))
    return fields


def summarize_sentiment_risk_events(events):
    numeric_totals = Counter()
    numeric_counts = Counter()
    posture_counts = Counter()
    leveling_state_counts = Counter()
    trend_direction_counts = Counter()
    falling_tape_counts = Counter()
    opportunity_phase_counts = Counter()
    opportunity_bot_hint_counts = Counter()
    opportunity_pattern_tag_counts = Counter()
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
        has_weather_stability = bool(event.get("weather_leveling_state")) or (
            safe_float(event.get("weather_leveling_score")) is not None
            or safe_float(event.get("weather_stabilization_score")) is not None
        )
        has_weather_trend = (
            bool(event.get("weather_short_term_direction"))
            or safe_float(event.get("weather_downtrend_strength")) is not None
            or safe_float(event.get("weather_uptrend_strength")) is not None
            or event.get("weather_falling_tape") is not None
        )
        has_weather_opportunity = (
            bool(event.get("weather_opportunity_phase"))
            or bool(event.get("weather_opportunity_bot_hint"))
            or bool(event.get("weather_pattern_tags"))
            or safe_float(event.get("weather_entry_opportunity_score")) is not None
            or safe_float(event.get("weather_rebound_confirmation_score")) is not None
            or safe_float(event.get("weather_exit_pressure_score")) is not None
            or safe_float(event.get("weather_hold_through_score")) is not None
            or safe_float(event.get("weather_failed_rebound_risk")) is not None
            or safe_float(event.get("weather_long_entry_noise_risk")) is not None
        )
        has_risk_value = (
            bool(posture)
            or bool(flags)
            or has_numeric_value
            or has_weather_stability
            or has_weather_trend
            or has_weather_opportunity
        )
        if not has_risk_value:
            continue
        samples += 1
        posture_counts[posture or "unknown"] += 1
        leveling_state = event.get("weather_leveling_state")
        if leveling_state:
            leveling_state_counts[str(leveling_state)] += 1
        short_term_direction = event.get("weather_short_term_direction")
        if short_term_direction:
            trend_direction_counts[str(short_term_direction)] += 1
        falling_tape = event.get("weather_falling_tape")
        if falling_tape is not None:
            falling_tape_counts[str(bool(falling_tape)).lower()] += 1
        opportunity_phase = event.get("weather_opportunity_phase")
        if opportunity_phase:
            opportunity_phase_counts[str(opportunity_phase)] += 1
        opportunity_bot_hint = event.get("weather_opportunity_bot_hint")
        if opportunity_bot_hint:
            opportunity_bot_hint_counts[str(opportunity_bot_hint)] += 1
        pattern_tags = event.get("weather_pattern_tags")
        if not isinstance(pattern_tags, list):
            pattern_tags = []
        for tag in pattern_tags:
            opportunity_pattern_tag_counts[str(tag)] += 1
        for output_key in RISK_CONTEXT_NUMERIC_FIELDS.values():
            value = safe_float(event.get(output_key))
            if value is None:
                continue
            numeric_totals[output_key] += value
            numeric_counts[output_key] += 1
        leveling_score = safe_float(event.get("weather_leveling_score"))
        if leveling_score is not None:
            numeric_totals["weather_leveling_score"] += leveling_score
            numeric_counts["weather_leveling_score"] += 1
        for output_key in (
            "weather_stabilization_score",
            "weather_downtrend_strength",
            "weather_uptrend_strength",
        ):
            value = safe_float(event.get(output_key))
            if value is None:
                continue
            numeric_totals[output_key] += value
            numeric_counts[output_key] += 1
        for output_key in (
            "weather_entry_opportunity_score",
            "weather_rebound_confirmation_score",
            "weather_exit_pressure_score",
            "weather_hold_through_score",
            "weather_failed_rebound_risk",
            "weather_long_entry_noise_risk",
        ):
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
        "weather_leveling_state_counts": dict(
            leveling_state_counts.most_common()
        ),
        "weather_short_term_direction_counts": dict(
            trend_direction_counts.most_common()
        ),
        "weather_falling_tape_counts": dict(
            falling_tape_counts.most_common()
        ),
        "weather_opportunity_phase_counts": dict(
            opportunity_phase_counts.most_common()
        ),
        "weather_opportunity_bot_hint_counts": dict(
            opportunity_bot_hint_counts.most_common()
        ),
        "weather_pattern_tag_counts": dict(
            opportunity_pattern_tag_counts.most_common()
        ),
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
    output_key = "weather_leveling_score"
    if numeric_counts[output_key]:
        summary[f"avg_{output_key}"] = round(
            numeric_totals[output_key] / numeric_counts[output_key],
            6,
        )
    else:
        summary[f"avg_{output_key}"] = None
    for output_key in (
        "weather_stabilization_score",
        "weather_downtrend_strength",
        "weather_uptrend_strength",
    ):
        if numeric_counts[output_key]:
            summary[f"avg_{output_key}"] = round(
                numeric_totals[output_key] / numeric_counts[output_key],
                6,
            )
        else:
            summary[f"avg_{output_key}"] = None
    for output_key in (
        "weather_entry_opportunity_score",
        "weather_rebound_confirmation_score",
        "weather_exit_pressure_score",
        "weather_hold_through_score",
        "weather_failed_rebound_risk",
        "weather_long_entry_noise_risk",
    ):
        if numeric_counts[output_key]:
            summary[f"avg_{output_key}"] = round(
                numeric_totals[output_key] / numeric_counts[output_key],
                6,
            )
        else:
            summary[f"avg_{output_key}"] = None
    return summary


def summarize_stale_level_reanchor_events(events):
    approved_count = 0
    reanchor_count = 0
    source_counts = Counter()
    phase_counts = Counter()
    above_level_pcts = []

    for event in events or []:
        approved_count += 1
        if not event.get("stale_level_reanchor_applied"):
            continue
        reanchor_count += 1
        source_counts[str(event.get("buy_source") or "unknown")] += 1
        phase_counts[str(event.get("weather_opportunity_phase") or "unknown")] += 1
        above_level_pct = safe_float(
            event.get("stale_level_reanchor_above_level_pct")
        )
        if above_level_pct is not None:
            above_level_pcts.append(above_level_pct)

    return {
        "approved_stale_level_reanchor_count": reanchor_count,
        "approved_stale_level_reanchor_rate": (
            round(reanchor_count / approved_count, 4)
            if approved_count
            else 0.0
        ),
        "approved_stale_level_reanchor_by_source": dict(
            source_counts.most_common()
        ),
        "approved_stale_level_reanchor_by_phase": dict(
            phase_counts.most_common()
        ),
        "approved_avg_stale_level_reanchor_above_level_pct": (
            round(statistics.mean(above_level_pcts), 6)
            if above_level_pcts
            else None
        ),
        "approved_max_stale_level_reanchor_above_level_pct": (
            round(max(above_level_pcts), 6)
            if above_level_pcts
            else None
        ),
    }


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


def buy_source_bucket(buy_source):
    mapping = {
        "llm_target": "llm_target",
        "range_low": "range_low",
        "range_mean": "range_mean",
        "range_median": "range_median",
        "range_high_band": "range_high_band",
    }
    return mapping.get(buy_source or "", "unknown")


def buy_cooldown_minutes_for_source(config, buy_source):
    source_key = buy_source_bucket(buy_source)
    source_map = normalized_source_config_map(config, "buy_cooldown_minutes_by_source")
    if source_key in source_map:
        return max(0.0, source_map[source_key])
    if buy_source in source_map:
        return max(0.0, source_map[buy_source])
    return max(
        0.0,
        strategy_float(config, "buy_cooldown_minutes", 0.0),
    )


def buy_cooldown_status(config, buy_source, now, last_buy_at, last_buy_at_by_source):
    if now is None:
        return {
            "remaining_minutes": 0.0,
            "global_remaining_minutes": 0.0,
            "source_remaining_minutes": 0.0,
            "global_cooldown_minutes": 0.0,
            "source_cooldown_minutes": 0.0,
        }

    global_minutes = max(
        0.0,
        strategy_float(config, "buy_cooldown_minutes", 0.0),
    )
    source_minutes = buy_cooldown_minutes_for_source(config, buy_source)
    source_key = buy_source_bucket(buy_source)
    source_last_buy_at = last_buy_at_by_source.get(buy_source)
    if source_last_buy_at is None:
        source_last_buy_at = last_buy_at_by_source.get(source_key)

    global_remaining = 0.0
    if global_minutes > 0 and last_buy_at is not None:
        elapsed_minutes = (now - last_buy_at).total_seconds() / 60
        global_remaining = max(0.0, global_minutes - elapsed_minutes)

    source_remaining = 0.0
    if source_minutes > 0 and source_last_buy_at is not None:
        elapsed_minutes = (now - source_last_buy_at).total_seconds() / 60
        source_remaining = max(0.0, source_minutes - elapsed_minutes)

    return {
        "remaining_minutes": max(global_remaining, source_remaining),
        "global_remaining_minutes": global_remaining,
        "source_remaining_minutes": source_remaining,
        "global_cooldown_minutes": global_minutes,
        "source_cooldown_minutes": source_minutes,
    }


def buy_cooldown_after_sell_fill_minutes_for_source(config, buy_source):
    source_key = buy_source_bucket(buy_source)
    source_map = normalized_source_config_map(
        config,
        "buy_cooldown_after_sell_fill_minutes_by_source",
    )
    if buy_source in source_map:
        return max(0.0, source_map[buy_source])
    if source_key in source_map:
        return max(0.0, source_map[source_key])
    if buy_source == "range_low" and strategy_bool(
        config,
        "buy_cooldown_after_sell_fill_low_bypass",
        True,
    ):
        return 0.0
    if buy_source == "range_high_band":
        return max(
            0.0,
            strategy_float(
                config,
                "buy_cooldown_after_sell_fill_high_band_minutes",
                45.0,
            ),
        )
    return max(
        0.0,
        strategy_float(config, "buy_cooldown_after_sell_fill_minutes", 20.0),
    )


def buy_cooldown_after_sell_fill_weather_bypass_allowed(config, weather_report):
    if not strategy_bool(
        config,
        "buy_cooldown_after_sell_fill_weather_bypass",
        True,
    ):
        return False
    if not isinstance(weather_report, dict):
        return False
    if weather_report.get("emergency_bell"):
        return False

    opportunity = weather_report.get("market_opportunity") or {}
    condition = str(weather_report.get("condition") or "").strip().lower()
    phase = str(opportunity.get("cycle_phase") or "").strip().lower()
    rebound_confirmation = safe_float(
        opportunity.get("rebound_confirmation_score")
    )
    hold_through = safe_float(opportunity.get("hold_through_score"))
    exit_pressure = safe_float(opportunity.get("exit_pressure_score"))
    rebound_confirmation = rebound_confirmation if rebound_confirmation is not None else 0.0
    hold_through = hold_through if hold_through is not None else 0.0
    exit_pressure = exit_pressure if exit_pressure is not None else 1.0

    continuation_conditions = {
        "breakout_tailwind",
        "constructive",
        "rebound_tailwind",
    }
    continuation_phases = {
        "early_rebound",
        "momentum_ride",
    }
    return (
        (condition in continuation_conditions or phase in continuation_phases)
        and rebound_confirmation >= strategy_float(
            config,
            "buy_cooldown_after_sell_fill_weather_min_rebound_confirmation",
            0.55,
        )
        and hold_through >= strategy_float(
            config,
            "buy_cooldown_after_sell_fill_weather_min_hold_through",
            0.50,
        )
        and exit_pressure <= strategy_float(
            config,
            "buy_cooldown_after_sell_fill_weather_max_exit_pressure",
            0.40,
        )
    )


def buy_cooldown_after_sell_fill_status(
    config,
    buy_source,
    weather_report,
    now,
    last_sell_at,
):
    configured_minutes = buy_cooldown_after_sell_fill_minutes_for_source(
        config,
        buy_source,
    )
    remaining = 0.0
    if configured_minutes > 0 and last_sell_at is not None and now is not None:
        elapsed_minutes = (now - last_sell_at).total_seconds() / 60
        remaining = max(0.0, configured_minutes - elapsed_minutes)

    weather_bypass_allowed = (
        remaining > 0
        and buy_cooldown_after_sell_fill_weather_bypass_allowed(
            config,
            weather_report,
        )
    )
    return {
        "remaining_minutes": 0.0 if weather_bypass_allowed else remaining,
        "raw_remaining_minutes": remaining,
        "configured_minutes": configured_minutes,
        "weather_bypass_allowed": weather_bypass_allowed,
    }


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


def stale_level_reanchor_entry(price, level, config, buy_source, weather_report):
    if buy_source not in ("range_low", "range_mean", "range_median"):
        return {"allowed": False, "reason": "unsupported_source"}
    if not strategy_bool(config, "stale_level_reanchor_enabled", False):
        return {"allowed": False, "reason": "disabled"}
    if price is None or level is None or price <= 0 or level <= 0 or price <= level:
        return {"allowed": False, "reason": "not_above_level"}

    above_pct = (price / level) - 1
    min_above_pct = strategy_float(
        config,
        "stale_level_reanchor_min_above_level_pct",
        0.0,
    )
    max_above_pct = strategy_float(
        config,
        "stale_level_reanchor_max_above_level_pct",
        0.0,
    )
    if max_above_pct <= 0:
        return {"allowed": False, "reason": "max_above_disabled"}
    if above_pct < min_above_pct:
        return {"allowed": False, "reason": "below_min_above_pct"}
    if above_pct > max_above_pct:
        return {"allowed": False, "reason": "above_max_above_pct"}

    allowed_sources = config_token_set(
        config,
        "stale_level_reanchor_sources",
        "range_low,range_mean,range_median",
    )
    if buy_source not in allowed_sources:
        return {"allowed": False, "reason": "source_not_enabled"}

    require_weather = strategy_bool(
        config,
        "stale_level_reanchor_require_weather",
        True,
    )
    if require_weather and not weather_bot_decides(weather_report):
        return {"allowed": False, "reason": "weather_unavailable"}
    if weather_report.get("emergency_bell") or weather_report.get("alert_level") == "danger":
        return {"allowed": False, "reason": "weather_danger"}

    opportunity = weather_market_opportunity(weather_report)
    phase = str(opportunity.get("cycle_phase") or "").strip().lower()
    allowed_phases = config_token_set(
        config,
        "stale_level_reanchor_weather_phases",
        "dip_leveling_entry,early_rebound,range_chop",
    )
    if require_weather and phase not in allowed_phases:
        return {"allowed": False, "reason": "phase_not_enabled"}

    entry_score = safe_float(opportunity.get("entry_opportunity_score"))
    rebound_score = safe_float(opportunity.get("rebound_confirmation_score"))
    exit_pressure_score = safe_float(opportunity.get("exit_pressure_score"))
    min_entry_score = strategy_float(
        config,
        "stale_level_reanchor_min_entry_opportunity_score",
        0.0,
    )
    min_rebound_score = strategy_float(
        config,
        "stale_level_reanchor_min_rebound_confirmation_score",
        0.0,
    )
    max_exit_pressure_score = strategy_float(
        config,
        "stale_level_reanchor_max_exit_pressure_score",
        1.0,
    )
    if entry_score is not None and entry_score < min_entry_score:
        return {"allowed": False, "reason": "entry_score_low"}
    if rebound_score is not None and rebound_score < min_rebound_score:
        return {"allowed": False, "reason": "rebound_score_low"}
    if exit_pressure_score is not None and exit_pressure_score > max_exit_pressure_score:
        return {"allowed": False, "reason": "exit_pressure_high"}

    return {
        "allowed": True,
        "reason": None,
        "reanchor_price": price,
        "original_level": level,
        "above_level_pct": above_pct,
        "weather_opportunity_phase": phase,
        "weather_entry_opportunity_score": entry_score,
        "weather_rebound_confirmation_score": rebound_score,
        "weather_exit_pressure_score": exit_pressure_score,
    }


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


def sell_backlog_effective_count(snapshot):
    open_sell_orders = (state_payload(snapshot).get("open_sell_orders") or [])
    captured_at = snapshot_timestamp(snapshot)
    config = strategy_payload(snapshot)
    soft_release_minutes = max(
        0.0,
        safe_float(config.get("sell_backlog_soft_release_minutes")) or 0.0,
    )
    old_order_weight = safe_float(config.get("sell_backlog_old_order_weight"))
    if old_order_weight is None:
        old_order_weight = 1.0
    old_order_weight = max(0.0, min(float(old_order_weight or 0.0), 1.0))

    effective_count = 0.0
    for order in open_sell_orders:
        if not isinstance(order, dict):
            effective_count += 1.0
            continue
        placed_at = parse_iso8601(order.get("placed_at"))
        age_minutes = (
            max(0.0, (captured_at - placed_at).total_seconds() / 60.0)
            if captured_at is not None and placed_at is not None
            else 0.0
        )
        if soft_release_minutes > 0 and age_minutes >= soft_release_minutes:
            effective_count += old_order_weight
        else:
            effective_count += 1.0

    return effective_count


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
    sell_backlog_count = safe_float(
        runtime_status.get("sell_backlog_effective_count")
    )
    if sell_backlog_count is None:
        sell_backlog_count = sell_backlog_effective_count(snapshot)
    if backlog_limit > 0 and sell_backlog_count >= backlog_limit:
        blockers.append("sell_backlog_count")

    open_sell_count = int(
        runtime_status.get("open_sell_count")
        or state_info.get("open_sell_count")
        or 0
    )

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


def build_snapshot_price_index(snapshots):
    indexed = []
    for snapshot in snapshots or []:
        ts = snapshot_timestamp(snapshot)
        price = snapshot_price(snapshot)
        if ts is None or price is None:
            continue
        indexed.append((ts, price))
    indexed.sort(key=lambda item: item[0])
    return {
        "times": [item[0] for item in indexed],
        "prices": [item[1] for item in indexed],
    }


def simulate_missed_opportunity(snapshot, event, snapshots, snapshot_price_index=None):
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

    if snapshot_price_index is None:
        snapshot_price_index = build_snapshot_price_index(snapshots)
    times = snapshot_price_index.get("times") or []
    prices = snapshot_price_index.get("prices") or []
    start_idx = bisect.bisect_right(times, entry_time)
    end_idx = bisect.bisect_right(times, hold_end)

    for idx in range(start_idx, end_idx):
        future_time = times[idx]
        future_price = prices[idx]
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
    snapshot_price_index = build_snapshot_price_index(snapshots)

    potential_results = []
    potential_results_by_phase = defaultdict(list)
    potential_results_by_reanchor = defaultdict(list)
    potential_results_by_modeled_reanchor_profit_guard = defaultdict(list)
    modeled_reanchor_profit_guard_counts = Counter()
    modeled_reanchor_profit_guard_reason_counts = Counter()
    modeled_profit_history = []
    for event in replay.get("approved_events") or []:
        snapshot = snapshot_by_timestamp.get(event.get("captured_at"))
        if not snapshot:
            continue
        potential = simulate_missed_opportunity(
            snapshot,
            event,
            snapshots or [],
            snapshot_price_index,
        )
        if potential:
            event_time = parse_iso8601(event.get("captured_at"))
            config = strategy_payload(snapshot)
            size_multiplier = safe_float(
                event.get("risk_context_position_size_effective_multiplier")
            )
            if size_multiplier is None:
                size_multiplier = 1.0
            potential["risk_context_position_size_effective_multiplier"] = (
                size_multiplier
            )
            phase = str(event.get("weather_opportunity_phase") or "unknown")
            potential["weather_opportunity_phase"] = phase
            reanchor_key = (
                "reanchored"
                if event.get("stale_level_reanchor_applied")
                else "normal"
            )
            potential["stale_level_reanchor_group"] = reanchor_key
            modeled_profit_guard_key = "not_applicable"
            if event.get("stale_level_reanchor_applied") and strategy_bool(
                config,
                "stale_level_reanchor_profit_guard_enabled",
                False,
            ):
                modeled_reanchor_profit_guard_counts["enabled"] += 1
                lookback_hours = strategy_float(
                    config,
                    "stale_level_reanchor_profit_lookback_hours",
                    24.0,
                )
                min_samples = int(
                    strategy_float(
                        config,
                        "stale_level_reanchor_profit_min_samples",
                        1,
                    )
                )
                min_projected_avg = strategy_float(
                    config,
                    "stale_level_reanchor_min_projected_avg_net_return_pct",
                    0.0,
                )
                assumed_return = strategy_float(
                    config,
                    "stale_level_reanchor_assumed_net_return_pct",
                    0.0,
                )
                fail_closed = strategy_bool(
                    config,
                    "stale_level_reanchor_profit_guard_fail_closed",
                    True,
                )
                if event_time is not None and lookback_hours > 0:
                    history_start = event_time - timedelta(hours=lookback_hours)
                    active_history = [
                        return_pct
                        for history_time, return_pct in modeled_profit_history
                        if history_time >= history_start
                    ]
                else:
                    active_history = [
                        return_pct for _, return_pct in modeled_profit_history
                    ]
                sample_count = len(active_history)
                avg_return = (
                    statistics.mean(active_history)
                    if active_history
                    else None
                )
                if sample_count < min_samples or avg_return is None:
                    allowed = not fail_closed
                    reason = "insufficient_modeled_profit_samples"
                    projected_avg = None
                else:
                    projected_avg = (
                        (avg_return * sample_count) + assumed_return
                    ) / (sample_count + 1)
                    allowed = projected_avg >= min_projected_avg
                    reason = (
                        None
                        if allowed
                        else "projected_modeled_avg_return_below_target"
                    )

                modeled_profit_guard_key = "allowed" if allowed else "blocked"
                modeled_reanchor_profit_guard_counts[modeled_profit_guard_key] += 1
                if reason:
                    modeled_reanchor_profit_guard_reason_counts[reason] += 1
                potential["modeled_stale_level_reanchor_profit_guard_allowed"] = (
                    allowed
                )
                potential["modeled_stale_level_reanchor_profit_guard_reason"] = (
                    reason
                )
                potential["modeled_stale_level_reanchor_profit_sample_count"] = (
                    sample_count
                )
                potential[
                    "modeled_stale_level_reanchor_recent_avg_return_pct"
                ] = (
                    round(avg_return, 6)
                    if avg_return is not None
                    else None
                )
                potential[
                    "modeled_stale_level_reanchor_projected_avg_return_pct"
                ] = (
                    round(projected_avg, 6)
                    if projected_avg is not None
                    else None
                )
            else:
                modeled_reanchor_profit_guard_counts[modeled_profit_guard_key] += 1

            potential_results.append(potential)
            potential_results_by_phase[phase].append(potential)
            potential_results_by_reanchor[reanchor_key].append(potential)
            potential_results_by_modeled_reanchor_profit_guard[
                modeled_profit_guard_key
            ].append(potential)
            end_return_pct = potential.get("end_return_pct")
            if event_time is not None and end_return_pct is not None:
                modeled_profit_history.append((event_time, end_return_pct))

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

    def summarize_phase_results(results):
        phase_end_returns = [
            result["end_return_pct"]
            for result in results
            if result.get("end_return_pct") is not None
        ]
        phase_max_runups = [
            result["max_runup_pct"]
            for result in results
            if result.get("max_runup_pct") is not None
        ]
        phase_max_drawdowns = [
            result["max_drawdown_pct"]
            for result in results
            if result.get("max_drawdown_pct") is not None
        ]
        phase_take_profit_count = sum(
            1 for result in results if result.get("take_profit_reached")
        )
        return {
            "evaluated_count": len(results),
            "take_profit_reached_count": phase_take_profit_count,
            "take_profit_reached_rate": (
                round(phase_take_profit_count / len(results), 4)
                if results
                else None
            ),
            "avg_end_return_pct": (
                round(statistics.mean(phase_end_returns), 6)
                if phase_end_returns
                else None
            ),
            "avg_max_runup_pct": (
                round(statistics.mean(phase_max_runups), 6)
                if phase_max_runups
                else None
            ),
            "avg_max_drawdown_pct": (
                round(statistics.mean(phase_max_drawdowns), 6)
                if phase_max_drawdowns
                else None
            ),
        }

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
        "by_weather_opportunity_phase": {
            phase: summarize_phase_results(results)
            for phase, results in sorted(potential_results_by_phase.items())
        },
        "by_stale_level_reanchor": {
            reanchor_group: summarize_phase_results(results)
            for reanchor_group, results in sorted(
                potential_results_by_reanchor.items()
            )
        },
        "modeled_stale_level_reanchor_profit_guard_counts": dict(
            modeled_reanchor_profit_guard_counts.most_common()
        ),
        "modeled_stale_level_reanchor_profit_guard_reason_counts": dict(
            modeled_reanchor_profit_guard_reason_counts.most_common()
        ),
        "by_modeled_stale_level_reanchor_profit_guard": {
            guard_group: summarize_phase_results(results)
            for guard_group, results in sorted(
                potential_results_by_modeled_reanchor_profit_guard.items()
            )
        },
    }


def build_strategy_comparison_rows(
    snapshots,
    strategy_set_file,
    include_details=BACKTEST_INCLUDE_STRATEGY_DETAILS,
):
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
        reanchor_summary = summarize_stale_level_reanchor_events(
            replay.get("approved_events") or []
        )
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
            "approved_weather_leveling_states": json.dumps(
                risk_summary.get("weather_leveling_state_counts") or {},
                sort_keys=True,
            ),
            "approved_avg_weather_leveling_score": risk_summary.get(
                "avg_weather_leveling_score"
            ),
            "approved_avg_weather_stabilization_score": risk_summary.get(
                "avg_weather_stabilization_score"
            ),
            "approved_weather_short_term_directions": json.dumps(
                risk_summary.get("weather_short_term_direction_counts") or {},
                sort_keys=True,
            ),
            "approved_weather_falling_tape_counts": json.dumps(
                risk_summary.get("weather_falling_tape_counts") or {},
                sort_keys=True,
            ),
            "approved_avg_weather_downtrend_strength": risk_summary.get(
                "avg_weather_downtrend_strength"
            ),
            "approved_avg_weather_uptrend_strength": risk_summary.get(
                "avg_weather_uptrend_strength"
            ),
            "approved_weather_opportunity_phases": json.dumps(
                risk_summary.get("weather_opportunity_phase_counts") or {},
                sort_keys=True,
            ),
            "approved_weather_opportunity_bot_hints": json.dumps(
                risk_summary.get("weather_opportunity_bot_hint_counts") or {},
                sort_keys=True,
            ),
            "approved_avg_weather_entry_opportunity_score": risk_summary.get(
                "avg_weather_entry_opportunity_score"
            ),
            "approved_avg_weather_rebound_confirmation_score": risk_summary.get(
                "avg_weather_rebound_confirmation_score"
            ),
            "approved_avg_weather_exit_pressure_score": risk_summary.get(
                "avg_weather_exit_pressure_score"
            ),
            "approved_avg_weather_hold_through_score": risk_summary.get(
                "avg_weather_hold_through_score"
            ),
            "approved_avg_weather_failed_rebound_risk": risk_summary.get(
                "avg_weather_failed_rebound_risk"
            ),
            "approved_avg_weather_long_entry_noise_risk": risk_summary.get(
                "avg_weather_long_entry_noise_risk"
            ),
            "approved_weather_pattern_tags": json.dumps(
                risk_summary.get("weather_pattern_tag_counts") or {},
                sort_keys=True,
            ),
            "potential_by_weather_opportunity_phase": json.dumps(
                potential.get("by_weather_opportunity_phase") or {},
                sort_keys=True,
            ),
            "approved_stale_level_reanchor_count": reanchor_summary.get(
                "approved_stale_level_reanchor_count"
            ),
            "approved_stale_level_reanchor_rate": reanchor_summary.get(
                "approved_stale_level_reanchor_rate"
            ),
            "approved_stale_level_reanchor_by_source": json.dumps(
                reanchor_summary.get(
                    "approved_stale_level_reanchor_by_source"
                ) or {},
                sort_keys=True,
            ),
            "approved_stale_level_reanchor_by_phase": json.dumps(
                reanchor_summary.get(
                    "approved_stale_level_reanchor_by_phase"
                ) or {},
                sort_keys=True,
            ),
            "approved_avg_stale_level_reanchor_above_level_pct": (
                reanchor_summary.get(
                    "approved_avg_stale_level_reanchor_above_level_pct"
                )
            ),
            "approved_max_stale_level_reanchor_above_level_pct": (
                reanchor_summary.get(
                    "approved_max_stale_level_reanchor_above_level_pct"
                )
            ),
            "potential_by_stale_level_reanchor": json.dumps(
                potential.get("by_stale_level_reanchor") or {},
                sort_keys=True,
            ),
            "modeled_stale_level_reanchor_profit_guard_counts": json.dumps(
                potential.get(
                    "modeled_stale_level_reanchor_profit_guard_counts"
                ) or {},
                sort_keys=True,
            ),
            "modeled_stale_level_reanchor_profit_guard_blocked": (
                (
                    potential.get(
                        "modeled_stale_level_reanchor_profit_guard_counts"
                    ) or {}
                ).get("blocked", 0)
            ),
            "modeled_stale_level_reanchor_profit_guard_allowed": (
                (
                    potential.get(
                        "modeled_stale_level_reanchor_profit_guard_counts"
                    ) or {}
                ).get("allowed", 0)
            ),
            "modeled_stale_level_reanchor_profit_guard_reasons": json.dumps(
                potential.get(
                    "modeled_stale_level_reanchor_profit_guard_reason_counts"
                ) or {},
                sort_keys=True,
            ),
            "potential_by_modeled_stale_level_reanchor_profit_guard": json.dumps(
                potential.get("by_modeled_stale_level_reanchor_profit_guard") or {},
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
        if include_details:
            detailed.append({
                "strategy_label": entry["label"],
                "strategy_file": strategy_path,
                "strategy_payload": strategy_payload,
                "replay_summary": summary,
                "potential_summary": potential,
                "recent_replay_events": replay.get("recent_replay_events", []),
                "recent_approved_events": replay.get(
                    "recent_approved_events",
                    [],
                ),
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
        "details_included": bool(include_details),
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
    payload_by_file = {
        strategy_file: detail.get("strategy_payload")
        for strategy_file, detail in details_by_file.items()
        if detail.get("strategy_payload") is not None
    }
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
            strategy_payload = payload_by_file.get(source_strategy_file)
            strategy_payload_load_error = None
            if strategy_payload is None and source_strategy_file:
                try:
                    _, strategy_payload = load_strategy_profile_from_file(
                        source_strategy_file
                    )
                    payload_by_file[source_strategy_file] = strategy_payload
                except Exception as exc:
                    strategy_payload_load_error = str(exc)
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
                "strategy_payload": strategy_payload,
                "strategy_payload_load_error": strategy_payload_load_error,
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
        "approved_weather_leveling_states",
        "approved_avg_weather_leveling_score",
        "approved_avg_weather_stabilization_score",
        "approved_weather_short_term_directions",
        "approved_weather_falling_tape_counts",
        "approved_avg_weather_downtrend_strength",
        "approved_avg_weather_uptrend_strength",
        "approved_weather_opportunity_phases",
        "approved_weather_opportunity_bot_hints",
        "approved_avg_weather_entry_opportunity_score",
        "approved_avg_weather_rebound_confirmation_score",
        "approved_avg_weather_exit_pressure_score",
        "approved_avg_weather_hold_through_score",
        "approved_avg_weather_failed_rebound_risk",
        "approved_avg_weather_long_entry_noise_risk",
        "approved_weather_pattern_tags",
        "potential_by_weather_opportunity_phase",
        "approved_stale_level_reanchor_count",
        "approved_stale_level_reanchor_rate",
        "approved_stale_level_reanchor_by_source",
        "approved_stale_level_reanchor_by_phase",
        "approved_avg_stale_level_reanchor_above_level_pct",
        "approved_max_stale_level_reanchor_above_level_pct",
        "potential_by_stale_level_reanchor",
        "modeled_stale_level_reanchor_profit_guard_counts",
        "modeled_stale_level_reanchor_profit_guard_blocked",
        "modeled_stale_level_reanchor_profit_guard_allowed",
        "modeled_stale_level_reanchor_profit_guard_reasons",
        "potential_by_modeled_stale_level_reanchor_profit_guard",
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
        "approved_weather_leveling_states",
        "approved_avg_weather_leveling_score",
        "approved_avg_weather_stabilization_score",
        "approved_weather_short_term_directions",
        "approved_weather_falling_tape_counts",
        "approved_avg_weather_downtrend_strength",
        "approved_avg_weather_uptrend_strength",
        "approved_weather_opportunity_phases",
        "approved_weather_opportunity_bot_hints",
        "approved_avg_weather_entry_opportunity_score",
        "approved_avg_weather_rebound_confirmation_score",
        "approved_avg_weather_exit_pressure_score",
        "approved_avg_weather_hold_through_score",
        "approved_avg_weather_failed_rebound_risk",
        "approved_avg_weather_long_entry_noise_risk",
        "approved_weather_pattern_tags",
        "potential_by_weather_opportunity_phase",
        "approved_stale_level_reanchor_count",
        "approved_stale_level_reanchor_rate",
        "approved_stale_level_reanchor_by_source",
        "approved_stale_level_reanchor_by_phase",
        "approved_avg_stale_level_reanchor_above_level_pct",
        "approved_max_stale_level_reanchor_above_level_pct",
        "potential_by_stale_level_reanchor",
        "modeled_stale_level_reanchor_profit_guard_counts",
        "modeled_stale_level_reanchor_profit_guard_blocked",
        "modeled_stale_level_reanchor_profit_guard_allowed",
        "modeled_stale_level_reanchor_profit_guard_reasons",
        "potential_by_modeled_stale_level_reanchor_profit_guard",
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


def compute_high_anchor_grid(
    high,
    price,
    entry_step_pct,
    breakout_extension_pct=0.0,
    allow_breakout_extension=False,
):
    lower_bound = high * (1 - entry_step_pct)
    if lower_bound <= price <= high:
        return [price]
    if (
        allow_breakout_extension
        and breakout_extension_pct > 0
        and high < price <= high * (1 + breakout_extension_pct)
    ):
        return [price]
    return []


def high_anchor_backlog_exposure(
    open_buy_orders,
    open_sell_orders,
    now,
    soft_release_minutes=0,
    old_order_weight=1.0,
):
    soft_release_minutes = max(0.0, float(soft_release_minutes or 0.0))
    old_order_weight = max(0.0, min(float(old_order_weight or 0.0), 1.0))
    open_buy_count = sum(
        1
        for order in open_buy_orders or []
        if order.get("buy_source") == "range_high_band"
    )
    fresh_sell_count = 0
    aged_sell_count = 0

    for order in open_sell_orders or []:
        if order.get("buy_source") != "range_high_band":
            continue
        placed_at = parse_iso8601(order.get("placed_at"))
        age_minutes = (
            ((now - placed_at).total_seconds() / 60)
            if now is not None and placed_at is not None
            else 0.0
        )
        if soft_release_minutes > 0 and age_minutes >= soft_release_minutes:
            aged_sell_count += 1
        else:
            fresh_sell_count += 1

    raw_count = open_buy_count + fresh_sell_count + aged_sell_count
    effective_count = (
        open_buy_count
        + fresh_sell_count
        + (aged_sell_count * old_order_weight)
    )
    return {
        "raw_count": raw_count,
        "effective_count": effective_count,
        "open_buy_count": open_buy_count,
        "fresh_sell_count": fresh_sell_count,
        "aged_sell_count": aged_sell_count,
        "soft_release_minutes": soft_release_minutes,
        "old_order_weight": old_order_weight,
    }


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


def risk_context_high_band_guard(config, risk_context, weather_report=None):
    if not strategy_bool(config, "risk_context_high_band_guard_enabled", False):
        return {"allowed": True, "reason": None}

    if not isinstance(risk_context, dict):
        risk_context = {}
    if not isinstance(weather_report, dict):
        weather_report = weather_report_payload(risk_context)
    market_opportunity = weather_report.get("market_opportunity")
    if not isinstance(market_opportunity, dict):
        market_opportunity = {}
    opportunity_phase = market_opportunity.get("cycle_phase")

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
    neutral_phase_guard_enabled = strategy_bool(
        config,
        "risk_context_high_band_neutral_phase_guard_enabled",
        False,
    )
    neutral_min_breakout = strategy_float(
        config,
        "risk_context_high_band_neutral_min_breakout_score",
        0.65,
    )
    neutral_min_rebound = strategy_float(
        config,
        "risk_context_high_band_neutral_min_rebound_score",
        0.70,
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
    if (
        neutral_phase_guard_enabled
        and opportunity_phase == "neutral"
        and (rebound is None or rebound < neutral_min_rebound)
        and (breakout is None or breakout < neutral_min_breakout)
    ):
        return {
            "allowed": False,
            "reason": "risk_context_high_band_neutral_confirmation_low",
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
                grid = compute_high_anchor_grid(
                    high,
                    price,
                    effective_step_pct,
                    safe_float(config.get("high_anchor_breakout_extension_pct"))
                    or 0.0,
                    weather_high_anchor_tailwind(weather_report),
                )
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
    stale_reanchor = {"allowed": False}

    last_sell_price = positive_or_default(state_info.get("last_sell_price"))
    last_sell_at = parse_iso8601(state_info.get("last_sell_at"))
    prevent_buy_above_last_sell = bool(config.get("prevent_buy_above_last_sell", True))
    buy_after_sell_discount_pct = safe_float(config.get("buy_after_sell_discount_pct")) or 0.001
    buy_above_last_sell_guard_minutes = (
        safe_float(config.get("buy_above_last_sell_guard_minutes")) or 0.0
    )
    mean_reversion_opportunity = safe_float(signal.get("mean_reversion_opportunity")) or 0.0
    mean_reversion_min_opportunity = safe_float(config.get("mean_reversion_min_opportunity")) or 0.0
    flow_pressure = safe_float(signal.get("flow_pressure"))
    llm_buy_cooldown_minutes_after_sell = int(config.get("llm_buy_cooldown_minutes_after_sell", 30))
    high_anchor_buy_cooldown_minutes = int(config.get("high_anchor_buy_cooldown_minutes", 15))
    max_open_high_anchor_orders = int(config.get("max_open_high_anchor_orders", 3))
    high_anchor_backlog_soft_release_minutes = safe_float(
        config.get("high_anchor_backlog_soft_release_minutes")
    ) or 0.0
    high_anchor_backlog_old_order_weight = safe_float(
        config.get("high_anchor_backlog_old_order_weight")
    )
    if high_anchor_backlog_old_order_weight is None:
        high_anchor_backlog_old_order_weight = 1.0
    max_inventory_usd = safe_float(config.get("max_inventory_usd")) or float("inf")
    deployed_inventory_usd = safe_float(state_info.get("deployed_inventory_usd")) or 0.0

    if key in open_buy_levels:
        return False, "open_buy_order"
    if key in open_sell_levels:
        return False, "open_sell_order"
    if (
        prevent_buy_above_last_sell
        and not allow_above_last_sell_for_candidate(
            config,
            buy_source,
            {
                **weather_report,
                "_risk_context": risk_context,
            },
        )
        and last_sell_price is not None
        and buy_above_last_sell_guard_active(
            last_sell_at,
            snapshot_timestamp(snapshot),
            buy_above_last_sell_guard_minutes,
        )
        and level > (last_sell_price * (1 - buy_after_sell_discount_pct))
    ):
        return False, "above_last_sell_discount"
    if mean_reversion_opportunity < mean_reversion_min_opportunity:
        return False, "mean_reversion_opportunity_below_min"
    if price_is_above_allowed_entry(price, level, config, buy_source):
        stale_reanchor = stale_level_reanchor_entry(
            price,
            level,
            config,
            buy_source,
            weather_report,
        )
        if not stale_reanchor["allowed"]:
            return False, "price_above_level"
        candidate["original_level"] = level
        candidate["level"] = stale_reanchor["reanchor_price"]
        candidate["stale_level_reanchor_applied"] = True
        candidate["stale_level_reanchor_above_level_pct"] = (
            stale_reanchor["above_level_pct"]
        )
        candidate["stale_level_reanchor_reason"] = "market_accepted_higher_level"
        level = safe_float(candidate["level"]) or level
        key = str(candidate["level"])
        if key in open_buy_levels:
            return False, "open_buy_order"
        if key in open_sell_levels:
            return False, "open_sell_order"
    sell_fill_cooldown = buy_cooldown_after_sell_fill_status(
        config,
        buy_source,
        weather_report,
        snapshot_timestamp(snapshot),
        last_sell_at,
    )
    if sell_fill_cooldown["remaining_minutes"] > 0:
        return False, "buy_after_sell_fill_cooldown"
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
            weather_report,
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

    high_anchor_exposure = high_anchor_backlog_exposure(
        open_buy_orders,
        open_sell_orders,
        now,
        high_anchor_backlog_soft_release_minutes,
        high_anchor_backlog_old_order_weight,
    )
    if (
        buy_source == "range_high_band"
        and high_anchor_exposure["effective_count"] >= max_open_high_anchor_orders
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
    last_replay_buy_at = None
    last_replay_buy_at_by_source = {}

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
            leveling_size_multiplier = high_band_leveling_size_multiplier(
                strategy_payload(snapshot),
                candidate["buy_source"],
                weather_report_payload(
                    risk_context_payload(signal_payload(snapshot))
                ),
            )
            base_size_multiplier = built.get(
                "risk_context_position_size_effective_multiplier"
            )
            effective_size_multiplier = (
                base_size_multiplier * leveling_size_multiplier
                if base_size_multiplier is not None
                else leveling_size_multiplier
            )
            leveling_bypass_blocked = (
                candidate["buy_source"] == "range_high_band"
                and weather_leveling_blocks_high_band_bypass(
                    strategy_payload(snapshot),
                    weather_report_payload(
                        risk_context_payload(signal_payload(snapshot))
                    ),
                )
            )
            approved, reason = evaluate_candidate(snapshot, candidate, price)
            cooldown = buy_cooldown_status(
                strategy_payload(snapshot),
                candidate["buy_source"],
                snapshot_timestamp(snapshot),
                last_replay_buy_at,
                last_replay_buy_at_by_source,
            )
            sell_fill_cooldown = buy_cooldown_after_sell_fill_status(
                strategy_payload(snapshot),
                candidate["buy_source"],
                weather_report_payload(
                    risk_context_payload(signal_payload(snapshot))
                ),
                snapshot_timestamp(snapshot),
                parse_iso8601(state_summary(snapshot).get("last_sell_at")),
            )
            if approved and cooldown["remaining_minutes"] > 0:
                approved = False
                reason = "buy_cooldown"
            if approved and sell_fill_cooldown["remaining_minutes"] > 0:
                approved = False
                reason = "buy_after_sell_fill_cooldown"
            if approved:
                approved_at = snapshot_timestamp(snapshot)
                if approved_at is not None:
                    last_replay_buy_at = approved_at
                    last_replay_buy_at_by_source[candidate["buy_source"]] = (
                        approved_at
                    )
                    last_replay_buy_at_by_source[
                        buy_source_bucket(candidate["buy_source"])
                    ] = approved_at
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
                    "risk_context_position_size_effective_multiplier": (
                        effective_size_multiplier
                    ),
                    "weather_leveling_high_band_size_multiplier": (
                        leveling_size_multiplier
                    ),
                    "weather_leveling_bypass_blocked": leveling_bypass_blocked,
                    "buy_cooldown_remaining_minutes": round(
                        cooldown["remaining_minutes"],
                        2,
                    ),
                    "buy_cooldown_minutes": (
                        cooldown["global_cooldown_minutes"]
                    ),
                    "buy_cooldown_source_minutes": (
                        cooldown["source_cooldown_minutes"]
                    ),
                    "buy_after_sell_fill_cooldown_remaining_minutes": round(
                        sell_fill_cooldown["remaining_minutes"],
                        2,
                    ),
                    "buy_after_sell_fill_cooldown_raw_remaining_minutes": round(
                        sell_fill_cooldown["raw_remaining_minutes"],
                        2,
                    ),
                    "buy_after_sell_fill_cooldown_minutes": (
                        sell_fill_cooldown["configured_minutes"]
                    ),
                    "buy_after_sell_fill_cooldown_weather_bypass": (
                        sell_fill_cooldown["weather_bypass_allowed"]
                    ),
                    "buy_source": candidate["buy_source"],
                    "strategy_mode": candidate["strategy_mode"],
                    "level": round(candidate["level"], 2),
                    "original_level": (
                        round(candidate["original_level"], 2)
                        if candidate.get("original_level") is not None
                        else None
                    ),
                    "stale_level_reanchor_applied": bool(
                        candidate.get("stale_level_reanchor_applied")
                    ),
                    "stale_level_reanchor_above_level_pct": (
                        round(
                            candidate["stale_level_reanchor_above_level_pct"],
                            6,
                        )
                        if candidate.get(
                            "stale_level_reanchor_above_level_pct"
                        ) is not None
                        else None
                    ),
                    "stale_level_reanchor_reason": candidate.get(
                        "stale_level_reanchor_reason"
                    ),
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
                    "risk_context_position_size_effective_multiplier": (
                        effective_size_multiplier
                    ),
                    "weather_leveling_high_band_size_multiplier": (
                        leveling_size_multiplier
                    ),
                    "weather_leveling_bypass_blocked": leveling_bypass_blocked,
                    "buy_cooldown_remaining_minutes": round(
                        cooldown["remaining_minutes"],
                        2,
                    ),
                    "buy_cooldown_minutes": (
                        cooldown["global_cooldown_minutes"]
                    ),
                    "buy_cooldown_source_minutes": (
                        cooldown["source_cooldown_minutes"]
                    ),
                    "buy_after_sell_fill_cooldown_remaining_minutes": round(
                        sell_fill_cooldown["remaining_minutes"],
                        2,
                    ),
                    "buy_after_sell_fill_cooldown_raw_remaining_minutes": round(
                        sell_fill_cooldown["raw_remaining_minutes"],
                        2,
                    ),
                    "buy_after_sell_fill_cooldown_minutes": (
                        sell_fill_cooldown["configured_minutes"]
                    ),
                    "buy_after_sell_fill_cooldown_weather_bypass": (
                        sell_fill_cooldown["weather_bypass_allowed"]
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
    def event_source(event):
        return (
            event.get("buy_source")
            or event.get("source")
            or event.get("level_source")
            or "unknown"
        )

    def event_notional(event):
        direct = safe_float(event.get("trade_notional_usd"))
        if direct is not None:
            return direct
        price = safe_float(event.get("price"))
        if price is None:
            price = safe_float(event.get("sell_price"))
        if price is None:
            price = safe_float(event.get("buy_price"))
        volume = safe_float(event.get("volume"))
        if price is None or volume is None:
            return None
        return price * volume

    def rate(numerator, denominator):
        return round(numerator / denominator, 4) if denominator else None

    def capped_rate(numerator, denominator):
        value = rate(numerator, denominator)
        return min(value, 1.0) if value is not None else None

    def event_trade_id(event, *, side=None):
        trade_id = event.get("trade_id") or event.get("buy_txid")
        if trade_id:
            return str(trade_id)
        if side == "buy" and event.get("txid"):
            return str(event["txid"])
        return None

    def event_time(event):
        return parse_iso8601(event.get("ts")) or parse_iso8601(event.get("cycle_id"))

    def close_enough(left, right, tolerance=0.00000001):
        left_value = safe_float(left)
        right_value = safe_float(right)
        if left_value is None or right_value is None:
            return False
        return abs(left_value - right_value) <= tolerance

    def find_fallback_cohort(sell_event, cohorts):
        sell_source = event_source(sell_event)
        sell_buy_price = safe_float(sell_event.get("buy_price"))
        sell_volume = safe_float(sell_event.get("volume"))
        if sell_buy_price is None or sell_volume is None:
            return None
        for cohort in cohorts.values():
            if cohort.get("sell_event") is not None:
                continue
            if cohort.get("buy_filled_event") is None:
                continue
            if cohort.get("buy_source") != sell_source:
                continue
            if not close_enough(cohort.get("buy_price"), sell_buy_price):
                continue
            if not close_enough(cohort.get("volume"), sell_volume):
                continue
            return cohort
        return None

    summary = {
        "events": len(events),
        "buy_orders_placed": 0,
        "buy_orders_filled": 0,
        "sell_orders_placed": 0,
        "sell_orders_filled": 0,
        "order_rejected": 0,
        "sell_order_repriced": 0,
        "risk_context_paper_buys_planned": 0,
        "sell_extension_shadow_decisions": 0,
        "activity_summary_count": 0,
        "latest_activity_summary": None,
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0,
        "candidate_skip_reason_counts": {},
        "candidate_skip_reasons_by_source": {},
        "buy_orders_placed_by_source": {},
        "risk_context_paper_buys_by_source": {},
        "buy_filled_by_source": {},
        "sell_filled_by_source": {},
        "rejected_by_side": {},
        "recent_fills": [],
        "recent_buy_orders": [],
        "recent_risk_context_paper_buys": [],
        "recent_sell_extension_shadow_decisions": [],
        "recent_activity_summaries": [],
    }
    buy_orders_placed_by_source = Counter()
    risk_context_paper_buys_by_source = Counter()
    buy_filled_by_source = Counter()
    sell_filled_by_source = Counter()
    rejected_by_side = Counter()
    candidate_skip_reason_counts = Counter()
    candidate_skip_reasons_by_source = defaultdict(Counter)
    capital_by_source = defaultdict(lambda: {
        "risk_context_paper_buys_planned": 0,
        "buy_orders_placed": 0,
        "buy_orders_filled": 0,
        "sell_orders_filled": 0,
        "paper_notional_usd": 0.0,
        "buy_notional_usd": 0.0,
        "sell_notional_usd": 0.0,
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0,
        "hold_minutes": [],
    })
    hold_minutes = []
    buy_cohorts = {}
    anonymous_cohort_count = 0
    reference_time = None
    latest_activity_summary_time = None

    for event in events:
        ts_value = event_time(event)
        if ts_value is not None and (
            reference_time is None or ts_value > reference_time
        ):
            reference_time = ts_value
        name = event.get("event")
        if name == "BUY_ORDER_PLACED":
            summary["buy_orders_placed"] += 1
            source = event_source(event)
            trade_id = event_trade_id(event, side="buy")
            buy_orders_placed_by_source[source] += 1
            capital_by_source[source]["buy_orders_placed"] += 1
            notional = event_notional(event)
            if notional is not None:
                capital_by_source[source]["buy_notional_usd"] += notional
            if not trade_id:
                anonymous_cohort_count += 1
                trade_id = f"anonymous_buy_{anonymous_cohort_count}"
            buy_cohorts.setdefault(trade_id, {
                "trade_id": trade_id,
                "buy_source": source,
                "buy_placed_event": None,
                "buy_filled_event": None,
                "sell_event": None,
                "match_method": None,
                "buy_price": None,
                "volume": None,
            })
            buy_cohorts[trade_id]["buy_placed_event"] = event
            buy_cohorts[trade_id]["buy_source"] = source
            buy_cohorts[trade_id]["buy_price"] = safe_float(event.get("price"))
            buy_cohorts[trade_id]["volume"] = safe_float(event.get("volume"))
            summary["recent_buy_orders"].append({
                "ts": event.get("ts"),
                "buy_source": event.get("buy_source"),
                "trade_id": trade_id,
                "price": event.get("price"),
                "volume": event.get("volume"),
                "trade_notional_usd": event.get("trade_notional_usd"),
                "weather_opportunity_phase": event.get(
                    "weather_opportunity_phase"
                ),
                "weather_opportunity_bot_hint": event.get(
                    "weather_opportunity_bot_hint"
                ),
            })
        elif name == "RISK_CONTEXT_PAPER_BUY_PLANNED":
            summary["risk_context_paper_buys_planned"] += 1
            source = event_source(event)
            risk_context_paper_buys_by_source[source] += 1
            capital_by_source[source]["risk_context_paper_buys_planned"] += 1
            notional = event_notional(event)
            if notional is not None:
                capital_by_source[source]["paper_notional_usd"] += notional
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
                "weather_opportunity_phase": event.get(
                    "weather_opportunity_phase"
                ),
                "weather_opportunity_bot_hint": event.get(
                    "weather_opportunity_bot_hint"
                ),
                "weather_entry_opportunity_score": event.get(
                    "weather_entry_opportunity_score"
                ),
            })
        elif name == "ACTIVITY_SUMMARY":
            summary["activity_summary_count"] += 1
            if (
                summary["latest_activity_summary"] is None
                or (
                    ts_value is not None
                    and (
                        latest_activity_summary_time is None
                        or ts_value >= latest_activity_summary_time
                    )
                )
            ):
                latest_activity_summary_time = ts_value
                summary["latest_activity_summary"] = event
            summary["recent_activity_summaries"].append({
                "ts": event.get("ts"),
                "cycle_id": event.get("cycle_id"),
                "price": event.get("price"),
                "strategy_profile": event.get("strategy_profile"),
                "strategy_modes": event.get("strategy_modes"),
                "runtime_block_reason": event.get("runtime_block_reason"),
                "open_buy_count": event.get("open_buy_count"),
                "open_sell_count": event.get("open_sell_count"),
                "sell_backlog_count": event.get("sell_backlog_count"),
                "sell_backlog_effective_count": event.get(
                    "sell_backlog_effective_count"
                ),
                "deployed_inventory_usd": event.get("deployed_inventory_usd"),
                "last_buy_at": event.get("last_buy_at"),
                "last_buy_at_by_source": event.get("last_buy_at_by_source"),
                "last_sell_at": event.get("last_sell_at"),
                "weather_condition": event.get("weather_condition"),
                "weather_alert_level": event.get("weather_alert_level"),
                "weather_opportunity_phase": event.get(
                    "weather_opportunity_phase"
                ),
                "weather_stabilization_score": event.get(
                    "weather_stabilization_score"
                ),
            })
        elif name == "BUY_ORDER_FILLED":
            summary["buy_orders_filled"] += 1
            source = event_source(event)
            trade_id = event_trade_id(event, side="buy")
            buy_filled_by_source[source] += 1
            capital_by_source[source]["buy_orders_filled"] += 1
            if not trade_id:
                anonymous_cohort_count += 1
                trade_id = f"anonymous_fill_{anonymous_cohort_count}"
            buy_cohorts.setdefault(trade_id, {
                "trade_id": trade_id,
                "buy_source": source,
                "buy_placed_event": None,
                "buy_filled_event": None,
                "sell_event": None,
                "match_method": None,
                "buy_price": None,
                "volume": None,
            })
            buy_cohorts[trade_id]["buy_filled_event"] = event
            buy_cohorts[trade_id]["buy_source"] = source
            if buy_cohorts[trade_id]["buy_price"] is None:
                buy_cohorts[trade_id]["buy_price"] = safe_float(event.get("price"))
            if buy_cohorts[trade_id]["volume"] is None:
                buy_cohorts[trade_id]["volume"] = safe_float(event.get("volume"))
        elif name == "SELL_ORDER_PLACED":
            summary["sell_orders_placed"] += 1
        elif name == "SELL_ORDER_FILLED":
            summary["sell_orders_filled"] += 1
            source = event_source(event)
            sell_filled_by_source[source] += 1
            gross_pnl = safe_float(event.get("gross_pnl")) or 0.0
            net_pnl = safe_float(event.get("estimated_net_pnl")) or 0.0
            summary["realized_gross_pnl"] += gross_pnl
            summary["realized_estimated_net_pnl"] += net_pnl
            source_stats = capital_by_source[source]
            source_stats["sell_orders_filled"] += 1
            source_stats["realized_gross_pnl"] += gross_pnl
            source_stats["realized_estimated_net_pnl"] += net_pnl
            sell_notional = event_notional(event)
            if sell_notional is not None:
                source_stats["sell_notional_usd"] += sell_notional
            hold_value = safe_float(event.get("hold_minutes"))
            if hold_value is not None:
                hold_minutes.append(hold_value)
                source_stats["hold_minutes"].append(hold_value)
            trade_id = event_trade_id(event, side="sell")
            matched_cohort = buy_cohorts.get(trade_id) if trade_id else None
            match_method = "trade_id" if matched_cohort is not None else None
            if matched_cohort is None:
                matched_cohort = find_fallback_cohort(event, buy_cohorts)
                match_method = "source_price_volume" if matched_cohort else None
            if matched_cohort is not None:
                matched_cohort["sell_event"] = event
                matched_cohort["match_method"] = match_method
            summary["recent_fills"].append({
                "ts": event.get("ts"),
                "side": "sell",
                "buy_source": event.get("buy_source"),
                "trade_id": trade_id,
                "level": event.get("level"),
                "gross_pnl": event.get("gross_pnl"),
                "estimated_net_pnl": event.get("estimated_net_pnl"),
                "hold_minutes": event.get("hold_minutes"),
                "weather_opportunity_phase": event.get(
                    "weather_opportunity_phase"
                ),
            })
        elif name == "ORDER_REJECTED":
            summary["order_rejected"] += 1
            rejected_by_side[event.get("side") or "unknown"] += 1
        elif name == "SELL_ORDER_REPRICED":
            summary["sell_order_repriced"] += 1
        elif name == "SELL_EXTENSION_SHADOW_DECISION":
            summary["sell_extension_shadow_decisions"] += 1
            summary["recent_sell_extension_shadow_decisions"].append({
                "ts": event.get("ts"),
                "txid": event.get("txid"),
                "trade_id": event.get("trade_id"),
                "buy_source": event.get("buy_source"),
                "buy_price": event.get("buy_price"),
                "current_sell_price": event.get("current_sell_price"),
                "proposed_sell_price": event.get("proposed_sell_price"),
                "current_price": event.get("current_price"),
                "extension_pct": event.get("extension_pct"),
                "additional_gross_pnl": event.get("additional_gross_pnl"),
                "age_minutes": event.get("age_minutes"),
                "reason": event.get("reason"),
                "weather_condition": event.get("weather_condition"),
                "weather_opportunity_phase": event.get(
                    "weather_opportunity_phase"
                ),
                "weather_hold_through_score": event.get(
                    "weather_hold_through_score"
                ),
                "weather_exit_pressure_score": event.get(
                    "weather_exit_pressure_score"
                ),
            })
        elif name == "BUY_CANDIDATE_SKIPPED":
            source = event_source(event)
            reason = (
                event.get("reason")
                or event.get("skip_reason")
                or event.get("blocked_reason")
                or "unknown"
            )
            candidate_skip_reason_counts[reason] += 1
            candidate_skip_reasons_by_source[source][reason] += 1

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
    summary["candidate_skip_reason_counts"] = dict(
        candidate_skip_reason_counts.most_common()
    )
    summary["candidate_skip_reasons_by_source"] = {
        source: dict(counter.most_common())
        for source, counter in sorted(candidate_skip_reasons_by_source.items())
    }
    summary["average_hold_minutes"] = (
        round(statistics.mean(hold_minutes), 2) if hold_minutes else None
    )
    summary["max_hold_minutes"] = round(max(hold_minutes), 2) if hold_minutes else None
    summary["recent_fills"] = summary["recent_fills"][-BACKTEST_RECENT_LIMIT:]
    summary["recent_buy_orders"] = summary["recent_buy_orders"][-BACKTEST_RECENT_LIMIT:]
    summary["recent_risk_context_paper_buys"] = (
        summary["recent_risk_context_paper_buys"][-BACKTEST_RECENT_LIMIT:]
    )
    summary["recent_sell_extension_shadow_decisions"] = (
        summary["recent_sell_extension_shadow_decisions"][-BACKTEST_RECENT_LIMIT:]
    )
    summary["recent_activity_summaries"] = (
        summary["recent_activity_summaries"][-BACKTEST_RECENT_LIMIT:]
    )
    summary["shadow_to_real_summary"] = {
        "shadow_buys_planned": summary["risk_context_paper_buys_planned"],
        "real_buys_placed": summary["buy_orders_placed"],
        "real_buys_filled": summary["buy_orders_filled"],
        "sell_exits": summary["sell_orders_filled"],
        "paper_to_real_placement_rate": rate(
            summary["buy_orders_placed"],
            summary["risk_context_paper_buys_planned"],
        ),
        "real_placement_to_fill_rate": rate(
            summary["buy_orders_filled"],
            summary["buy_orders_placed"],
        ),
        "real_fill_to_exit_rate": capped_rate(
            summary["sell_orders_filled"],
            summary["buy_orders_filled"],
        ),
        "placed_not_filled_count_estimate": max(
            0,
            summary["buy_orders_placed"] - summary["buy_orders_filled"],
        ),
        "unexited_buy_count_estimate": max(
            0,
            summary["buy_orders_filled"] - summary["sell_orders_filled"],
        ),
    }

    capital_sources = {}
    total_buy_notional = 0.0
    total_paper_notional = 0.0
    total_sell_notional = 0.0
    for source, source_stats in sorted(capital_by_source.items()):
        buy_notional = source_stats["buy_notional_usd"]
        paper_notional = source_stats["paper_notional_usd"]
        sell_notional = source_stats["sell_notional_usd"]
        source_hold_minutes = source_stats["hold_minutes"]
        total_buy_notional += buy_notional
        total_paper_notional += paper_notional
        total_sell_notional += sell_notional
        capital_sources[source] = {
            "risk_context_paper_buys_planned": source_stats[
                "risk_context_paper_buys_planned"
            ],
            "buy_orders_placed": source_stats["buy_orders_placed"],
            "buy_orders_filled": source_stats["buy_orders_filled"],
            "sell_orders_filled": source_stats["sell_orders_filled"],
            "paper_notional_usd": round(paper_notional, 8),
            "buy_notional_usd": round(buy_notional, 8),
            "sell_notional_usd": round(sell_notional, 8),
            "realized_gross_pnl": round(source_stats["realized_gross_pnl"], 8),
            "realized_estimated_net_pnl": round(
                source_stats["realized_estimated_net_pnl"],
                8,
            ),
            "realized_net_pnl_per_buy_notional_pct": (
                round(
                    source_stats["realized_estimated_net_pnl"] / buy_notional * 100,
                    4,
                )
                if buy_notional
                else None
            ),
            "average_hold_minutes": (
                round(statistics.mean(source_hold_minutes), 2)
                if source_hold_minutes
                else None
            ),
            "max_hold_minutes": (
                round(max(source_hold_minutes), 2)
                if source_hold_minutes
                else None
            ),
            "paper_to_real_placement_rate": rate(
                source_stats["buy_orders_placed"],
                source_stats["risk_context_paper_buys_planned"],
            ),
            "real_placement_to_fill_rate": rate(
                source_stats["buy_orders_filled"],
                source_stats["buy_orders_placed"],
            ),
            "real_fill_to_exit_rate": capped_rate(
                source_stats["sell_orders_filled"],
                source_stats["buy_orders_filled"],
            ),
        }

    summary["shadow_vs_real_by_source"] = {
        source: {
            "shadow_planned": values["risk_context_paper_buys_planned"],
            "real_buys_placed": values["buy_orders_placed"],
            "real_buys_filled": values["buy_orders_filled"],
            "sell_exits": values["sell_orders_filled"],
            "paper_to_real_placement_rate": values[
                "paper_to_real_placement_rate"
            ],
            "paper_to_real_placement_delta": (
                values["buy_orders_placed"]
                - values["risk_context_paper_buys_planned"]
            ),
            "real_placement_to_fill_rate": values[
                "real_placement_to_fill_rate"
            ],
            "real_fill_to_exit_rate": values["real_fill_to_exit_rate"],
        }
        for source, values in capital_sources.items()
    }
    summary["capital_recycling_summary"] = {
        "total_paper_notional_usd": round(total_paper_notional, 8),
        "total_buy_notional_usd": round(total_buy_notional, 8),
        "total_sell_notional_usd": round(total_sell_notional, 8),
        "avg_buy_notional_usd": (
            round(total_buy_notional / summary["buy_orders_placed"], 8)
            if summary["buy_orders_placed"]
            else None
        ),
        "realized_estimated_net_pnl": summary["realized_estimated_net_pnl"],
        "realized_net_pnl_per_buy_notional_pct": (
            round(
                summary["realized_estimated_net_pnl"] / total_buy_notional * 100,
                4,
            )
            if total_buy_notional
            else None
        ),
        "average_hold_minutes": summary["average_hold_minutes"],
        "max_hold_minutes": summary["max_hold_minutes"],
        "placed_not_filled_count_estimate": summary[
            "shadow_to_real_summary"
        ]["placed_not_filled_count_estimate"],
        "unexited_buy_count_estimate": summary[
            "shadow_to_real_summary"
        ]["unexited_buy_count_estimate"],
        "by_source": capital_sources,
    }

    cohort_by_source = defaultdict(lambda: {
        "buy_cohorts_started": 0,
        "buy_cohorts_filled": 0,
        "matched_sell_exits": 0,
        "open_filled_cohorts": 0,
        "matched_buy_notional_usd": 0.0,
        "matched_sell_notional_usd": 0.0,
        "matched_realized_gross_pnl": 0.0,
        "matched_realized_estimated_net_pnl": 0.0,
        "matched_hold_minutes": [],
        "match_methods": Counter(),
    })
    completed_round_trips = []
    open_filled_cohorts = []
    matched_sell_count = 0
    matched_buy_notional = 0.0
    matched_sell_notional = 0.0
    matched_gross_pnl = 0.0
    matched_net_pnl = 0.0
    matched_hold_minutes = []
    match_methods = Counter()
    for cohort in buy_cohorts.values():
        source = cohort.get("buy_source") or "unknown"
        source_stats = cohort_by_source[source]
        if cohort.get("buy_placed_event") is not None:
            source_stats["buy_cohorts_started"] += 1
        if cohort.get("buy_filled_event") is not None:
            source_stats["buy_cohorts_filled"] += 1
        if (
            cohort.get("buy_filled_event") is not None
            and cohort.get("sell_event") is None
        ):
            source_stats["open_filled_cohorts"] += 1
            fill_event = cohort.get("buy_filled_event")
            fill_time = event_time(fill_event)
            age_minutes = None
            if fill_time is not None and reference_time is not None:
                age_minutes = max(
                    0.0,
                    (reference_time - fill_time).total_seconds() / 60,
                )
            open_filled_cohorts.append({
                "trade_id": cohort.get("trade_id"),
                "buy_source": source,
                "buy_ts": fill_event.get("ts"),
                "buy_price": fill_event.get("price") or cohort.get("buy_price"),
                "volume": fill_event.get("volume") or cohort.get("volume"),
                "age_minutes": round(age_minutes, 2)
                if age_minutes is not None
                else None,
            })
        sell_event = cohort.get("sell_event")
        if sell_event is None:
            continue
        buy_event = cohort.get("buy_placed_event") or cohort.get("buy_filled_event")
        buy_notional = event_notional(buy_event or {}) or 0.0
        sell_notional = event_notional(sell_event) or 0.0
        gross_pnl = safe_float(sell_event.get("gross_pnl")) or 0.0
        net_pnl = safe_float(sell_event.get("estimated_net_pnl")) or 0.0
        hold_value = safe_float(sell_event.get("hold_minutes"))
        matched_sell_count += 1
        matched_buy_notional += buy_notional
        matched_sell_notional += sell_notional
        matched_gross_pnl += gross_pnl
        matched_net_pnl += net_pnl
        source_stats["matched_sell_exits"] += 1
        source_stats["matched_buy_notional_usd"] += buy_notional
        source_stats["matched_sell_notional_usd"] += sell_notional
        source_stats["matched_realized_gross_pnl"] += gross_pnl
        source_stats["matched_realized_estimated_net_pnl"] += net_pnl
        if hold_value is not None:
            matched_hold_minutes.append(hold_value)
            source_stats["matched_hold_minutes"].append(hold_value)
        method = cohort.get("match_method") or "unknown"
        match_methods[method] += 1
        source_stats["match_methods"][method] += 1
        completed_round_trips.append({
            "trade_id": cohort.get("trade_id"),
            "buy_source": source,
            "buy_ts": (buy_event or {}).get("ts"),
            "sell_ts": sell_event.get("ts"),
            "buy_price": (buy_event or {}).get("price") or sell_event.get("buy_price"),
            "sell_price": sell_event.get("sell_price") or sell_event.get("price"),
            "volume": sell_event.get("volume") or (buy_event or {}).get("volume"),
            "buy_notional_usd": round(buy_notional, 8),
            "sell_notional_usd": round(sell_notional, 8),
            "gross_pnl": gross_pnl,
            "estimated_net_pnl": net_pnl,
            "hold_minutes": hold_value,
            "match_method": method,
        })

    cohort_source_output = {}
    for source, source_stats in sorted(cohort_by_source.items()):
        source_buy_notional = source_stats["matched_buy_notional_usd"]
        source_hold_minutes = source_stats["matched_hold_minutes"]
        cohort_source_output[source] = {
            "buy_cohorts_started": source_stats["buy_cohorts_started"],
            "buy_cohorts_filled": source_stats["buy_cohorts_filled"],
            "matched_sell_exits": source_stats["matched_sell_exits"],
            "open_filled_cohorts": source_stats["open_filled_cohorts"],
            "cohort_fill_rate": rate(
                source_stats["buy_cohorts_filled"],
                source_stats["buy_cohorts_started"],
            ),
            "cohort_exit_rate": rate(
                source_stats["matched_sell_exits"],
                source_stats["buy_cohorts_filled"],
            ),
            "matched_buy_notional_usd": round(source_buy_notional, 8),
            "matched_sell_notional_usd": round(
                source_stats["matched_sell_notional_usd"],
                8,
            ),
            "matched_realized_gross_pnl": round(
                source_stats["matched_realized_gross_pnl"],
                8,
            ),
            "matched_realized_estimated_net_pnl": round(
                source_stats["matched_realized_estimated_net_pnl"],
                8,
            ),
            "matched_net_pnl_per_buy_notional_pct": (
                round(
                    (
                        source_stats["matched_realized_estimated_net_pnl"]
                        / source_buy_notional
                        * 100
                    ),
                    4,
                )
                if source_buy_notional
                else None
            ),
            "average_matched_hold_minutes": (
                round(statistics.mean(source_hold_minutes), 2)
                if source_hold_minutes
                else None
            ),
            "max_matched_hold_minutes": (
                round(max(source_hold_minutes), 2)
                if source_hold_minutes
                else None
            ),
            "match_methods": dict(source_stats["match_methods"].most_common()),
        }

    summary["round_trip_cohort_summary"] = {
        "buy_cohorts_started": sum(
            1 for cohort in buy_cohorts.values()
            if cohort.get("buy_placed_event") is not None
        ),
        "buy_cohorts_filled": sum(
            1 for cohort in buy_cohorts.values()
            if cohort.get("buy_filled_event") is not None
        ),
        "matched_sell_exits": matched_sell_count,
        "unmatched_sell_exits": max(0, summary["sell_orders_filled"] - matched_sell_count),
        "open_filled_cohorts": sum(
            1 for cohort in buy_cohorts.values()
            if (
                cohort.get("buy_filled_event") is not None
                and cohort.get("sell_event") is None
            )
        ),
        "cohort_fill_rate": rate(
            sum(
                1 for cohort in buy_cohorts.values()
                if cohort.get("buy_filled_event") is not None
            ),
            sum(
                1 for cohort in buy_cohorts.values()
                if cohort.get("buy_placed_event") is not None
            ),
        ),
        "cohort_exit_rate": rate(
            matched_sell_count,
            sum(
                1 for cohort in buy_cohorts.values()
                if cohort.get("buy_filled_event") is not None
            ),
        ),
        "matched_buy_notional_usd": round(matched_buy_notional, 8),
        "matched_sell_notional_usd": round(matched_sell_notional, 8),
        "matched_realized_gross_pnl": round(matched_gross_pnl, 8),
        "matched_realized_estimated_net_pnl": round(matched_net_pnl, 8),
        "matched_net_pnl_per_buy_notional_pct": (
            round(matched_net_pnl / matched_buy_notional * 100, 4)
            if matched_buy_notional
            else None
        ),
        "average_matched_hold_minutes": (
            round(statistics.mean(matched_hold_minutes), 2)
            if matched_hold_minutes
            else None
        ),
        "max_matched_hold_minutes": (
            round(max(matched_hold_minutes), 2) if matched_hold_minutes else None
        ),
        "match_methods": dict(match_methods.most_common()),
        "by_source": cohort_source_output,
        "recent_completed_round_trips": completed_round_trips[-BACKTEST_RECENT_LIMIT:],
        "oldest_open_filled_cohort_age_minutes": (
            max(
                (
                    item["age_minutes"]
                    for item in open_filled_cohorts
                    if item.get("age_minutes") is not None
                ),
                default=None,
            )
        ),
        "recent_open_filled_cohorts": open_filled_cohorts[-BACKTEST_RECENT_LIMIT:],
    }
    return summary


def build_backtest_watchlist(replay, actual, missed):
    def add_item(items, severity, code, message, **fields):
        item = {
            "severity": severity,
            "code": code,
            "message": message,
        }
        item.update(fields)
        items.append(item)

    items = []
    shadow = actual.get("shadow_to_real_summary") or {}
    cohort = actual.get("round_trip_cohort_summary") or {}

    shadow_planned = int(shadow.get("shadow_buys_planned") or 0)
    real_placed = int(shadow.get("real_buys_placed") or 0)
    real_filled = int(shadow.get("real_buys_filled") or 0)
    placed_not_filled = int(shadow.get("placed_not_filled_count_estimate") or 0)
    open_filled = int(cohort.get("open_filled_cohorts") or 0)
    unmatched_sells = int(cohort.get("unmatched_sell_exits") or 0)
    matched_exits = int(cohort.get("matched_sell_exits") or 0)
    cohort_exit_rate = safe_float(cohort.get("cohort_exit_rate"))
    oldest_open_age = safe_float(
        cohort.get("oldest_open_filled_cohort_age_minutes")
    )

    if shadow_planned and real_placed == 0:
        add_item(
            items,
            "warning",
            "shadow_without_real_orders",
            "Risk-context shadow buys were planned, but no live buys were placed.",
            shadow_buys_planned=shadow_planned,
            real_buys_placed=real_placed,
        )

    if placed_not_filled > 0:
        add_item(
            items,
            "warning",
            "placed_orders_not_filled",
            "Some live buy orders were placed but have not filled in the report window.",
            placed_not_filled_count=placed_not_filled,
        )

    if open_filled > 0:
        severity = "warning" if oldest_open_age and oldest_open_age >= 720 else "info"
        add_item(
            items,
            severity,
            "open_filled_cohorts",
            "Filled buy cohorts are still waiting for matched sell exits.",
            open_filled_cohorts=open_filled,
            oldest_open_filled_cohort_age_minutes=oldest_open_age,
        )

    if unmatched_sells > 0:
        add_item(
            items,
            "info",
            "unmatched_sell_exits",
            "Some sell exits were from older inventory or logs without a cohort match.",
            unmatched_sell_exits=unmatched_sells,
            matched_sell_exits=matched_exits,
            match_methods=cohort.get("match_methods") or {},
        )

    if real_filled >= 3 and cohort_exit_rate is not None and cohort_exit_rate < 0.5:
        add_item(
            items,
            "info",
            "low_same_window_exit_rate",
            "Same-window matched cohort exit rate is below 50%.",
            cohort_exit_rate=cohort_exit_rate,
            buy_cohorts_filled=real_filled,
            matched_sell_exits=matched_exits,
        )

    match_methods = cohort.get("match_methods") or {}
    if matched_exits > 0 and not match_methods.get("trade_id"):
        add_item(
            items,
            "info",
            "cohort_matches_without_trade_id",
            "Matched round trips are using fallback matching; future logs should move to trade_id matches.",
            match_methods=match_methods,
        )

    approved_but_not_placed = int(missed.get("approved_but_not_placed") or 0)
    approved_candidates = int(missed.get("approved_candidates") or 0)
    if approved_candidates and approved_but_not_placed > real_placed * 10:
        add_item(
            items,
            "info",
            "large_replay_live_gap",
            "Replay approved far more candidates than live execution placed.",
            approved_candidates=approved_candidates,
            approved_but_not_placed=approved_but_not_placed,
            real_buys_placed=real_placed,
            placement_rate_vs_approved=missed.get("placement_rate_vs_approved"),
        )

    return {
        "status": "attention" if any(
            item["severity"] == "warning" for item in items
        ) else "ok",
        "item_count": len(items),
        "warning_count": sum(1 for item in items if item["severity"] == "warning"),
        "info_count": sum(1 for item in items if item["severity"] == "info"),
        "items": items,
        "summary": {
            "shadow_buys_planned": shadow_planned,
            "real_buys_placed": real_placed,
            "real_buys_filled": real_filled,
            "matched_sell_exits": matched_exits,
            "unmatched_sell_exits": unmatched_sells,
            "open_filled_cohorts": open_filled,
            "oldest_open_filled_cohort_age_minutes": oldest_open_age,
            "cohort_exit_rate": cohort_exit_rate,
        },
        "notes": [
            "Watchlist items are report-only diagnostics and do not affect live trading.",
            "Cohort matching is strongest when activity events include trade_id.",
        ],
    }


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
    snapshot_price_index = build_snapshot_price_index(snapshots)
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
            simulate_missed_opportunity(
                snapshot,
                event,
                snapshots or [],
                snapshot_price_index,
            )
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
            simulate_missed_opportunity(
                snapshot,
                event,
                snapshots or [],
                snapshot_price_index,
            )
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


def build_report(window_hours=None, include_strategy_details=None):
    if include_strategy_details is None:
        include_strategy_details = BACKTEST_INCLUDE_STRATEGY_DETAILS
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
    watchlist = build_backtest_watchlist(replay, actual, missed)
    replay_output = dict(replay)
    replay_output.pop("approved_events", None)
    strategy_comparison = None
    if BACKTEST_STRATEGY_SET_FILE:
        strategy_comparison = build_strategy_comparison_rows(
            snapshots,
            BACKTEST_STRATEGY_SET_FILE,
            include_details=include_strategy_details,
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
        "watchlist": watchlist,
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
    include_strategy_details = BACKTEST_INCLUDE_STRATEGY_DETAILS
    if args.strategy_summary_only:
        include_strategy_details = False
    if args.include_strategy_details:
        include_strategy_details = True
    report = build_report(
        window_hours=args.window_hours,
        include_strategy_details=include_strategy_details,
    )
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
