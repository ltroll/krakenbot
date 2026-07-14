#!/usr/bin/env python3

# =====================================================
# RANGE GRID SENTIMENT BOT (RESTORED STABLE VERSION)
# =====================================================

import atexit
import base64
import fcntl
import hashlib
import hmac
import json
import os
import socket
import statistics
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import krakenex
import requests
from dotenv import load_dotenv
from fee_config import effective_round_trip_fee_pct
from range_grid_guardrails import (
    runtime_buy_block_reason,
    summarize_sell_backlog,
    validate_strategy_config,
)
from signal_normalizer import normalize_signal_payload

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

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
LOG_FILE = (
    os.getenv("RANGE_GRID_TRADE_LOG_FILE")
    or os.getenv("TRADE_LOG_FILE")
    or "trade_log.jsonl"
)
ACTIVITY_LOG_FILE = (
    os.getenv("RANGE_GRID_ACTIVITY_LOG_FILE")
    or "/var/www/html/bot/range_grid_activity.jsonl"
)
ACTIVITY_LOG_ROTATE_DAILY = (
    os.getenv("RANGE_GRID_ACTIVITY_LOG_ROTATE_DAILY", "true")
    .strip()
    .lower()
    in ("1", "true", "yes", "on")
)
INSTANCE_LOCK_FILE = (
    os.getenv("RANGE_GRID_LOCK_FILE")
    or os.getenv("BOT_LOCK_FILE")
    or f"{STATE_FILE}.lock"
)
STATUS_FILE = (
    os.getenv("RANGE_GRID_STATUS_FILE")
    or os.getenv("BOT_STATUS_FILE")
    or "range_grid_status.json"
)
ALERT_LOG_FILE = (
    os.getenv("RANGE_GRID_ALERT_LOG_FILE")
    or os.getenv("BOT_ALERT_LOG_FILE")
    or "range_grid_alerts.jsonl"
)
ANCHOR_STRATEGY_ROUTER_FILE = (
    os.getenv("RANGE_GRID_ANCHOR_ROUTER_FILE")
    or "/var/www/html/bot/range_grid_anchor_winners.json"
)

KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")
PRICE_LOG_URL = os.getenv("PRICE_LOG_URL")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
KRAKEN_NONCE_RETRIES = int(os.getenv("KRAKEN_NONCE_RETRIES", "2"))
KRAKEN_LOCKOUT_COOLDOWN_SECONDS = int(
    os.getenv("KRAKEN_LOCKOUT_COOLDOWN_SECONDS", "300")
)
SELL_INSUFFICIENT_FUNDS_COOLDOWN_SECONDS = int(
    os.getenv(
        "SELL_INSUFFICIENT_FUNDS_COOLDOWN_SECONDS",
        str(KRAKEN_LOCKOUT_COOLDOWN_SECONDS)
    )
)
PROCESSED_FILL_CACHE_LIMIT = int(
    os.getenv("PROCESSED_FILL_CACHE_LIMIT", "2000")
)
ALERT_DEDUP_MINUTES = int(os.getenv("RANGE_GRID_ALERT_DEDUP_MINUTES", "30"))


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

    for mode in raw_value.split(","):
        normalized_mode = mode.strip().lower()
        if not normalized_mode:
            continue
        if normalized_mode in disabled_values:
            continue
        normalized_mode = alias_map.get(normalized_mode, normalized_mode)
        if normalized_mode in valid_modes and normalized_mode not in normalized_modes:
            normalized_modes.append(normalized_mode)
    return normalized_modes


def normalize_operating_mode(raw_value):
    normalized = str(raw_value or "range_plus_llm").strip().lower()
    valid_modes = {
        "range_plus_llm",
        "range_only",
        "sell_only",
        "observe_only",
    }
    return normalized if normalized in valid_modes else "range_plus_llm"


def apply_operating_mode_to_strategy_modes(base_modes, operating_mode):
    if operating_mode in ("sell_only", "observe_only"):
        return []
    if operating_mode == "range_only":
        return [mode for mode in base_modes if mode != "llm_target"]
    return list(base_modes)


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


def anchor_for_buy_source(buy_source):
    return {
        "range_low": "low",
        "range_mean": "median",
        "range_median": "median",
        "range_high_band": "high",
    }.get(buy_source)


def effective_entry_step_pct(base_entry_step_pct, volatility_pct, config):
    base_step = numeric_or_default(base_entry_step_pct, 0.0)
    if base_step <= 0:
        return 0.0
    if not strategy_bool(config, "volatility_adaptive_entry_step_enabled", False):
        return base_step

    volatility = numeric_or_default(volatility_pct, None)
    if volatility is None or volatility <= 0:
        return base_step

    reference_pct = strategy_float(config, "volatility_reference_pct", 0.02)
    if reference_pct <= 0:
        reference_pct = 0.02
    min_multiplier = strategy_float(
        config,
        "volatility_min_step_multiplier",
        1.0
    )
    if min_multiplier <= 0:
        min_multiplier = 1.0
    max_multiplier = strategy_float(
        config,
        "volatility_max_step_multiplier",
        max(min_multiplier, 2.0)
    )
    if max_multiplier < min_multiplier:
        max_multiplier = min_multiplier

    raw_multiplier = volatility / reference_pct
    step_multiplier = max(min_multiplier, min(max_multiplier, raw_multiplier))
    return base_step * step_multiplier


def inventory_pressure_adjustment(
    deployed_inventory_usd,
    effective_max_inventory_usd,
    config
):
    if not strategy_bool(
        config,
        "inventory_pressure_size_scaling_enabled",
        False
    ):
        return {
            "usage_ratio": 0.0,
            "size_multiplier": 1.0,
        }

    max_inventory = numeric_or_default(effective_max_inventory_usd, 0.0)
    deployed_inventory = numeric_or_default(deployed_inventory_usd, 0.0)
    if max_inventory <= 0:
        return {
            "usage_ratio": 1.0,
            "size_multiplier": 0.0,
        }

    usage_ratio = max(0.0, deployed_inventory / max_inventory)
    start_ratio = strategy_float(
        config,
        "inventory_pressure_start_usage_pct",
        0.5
    )
    min_multiplier = strategy_float(
        config,
        "inventory_pressure_min_size_multiplier",
        0.25
    )
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


def select_dynamic_strategy_modes(base_modes, operating_mode, range_position, strategy_config):
    active_modes = apply_operating_mode_to_strategy_modes(base_modes, operating_mode)
    if not active_modes:
        return []

    llm_enabled = "llm_target" in active_modes
    range_modes = [mode for mode in active_modes if mode != "llm_target"]
    if not range_modes:
        return active_modes

    if (
        not strategy_bool(strategy_config, "dynamic_anchor_mode", False)
        or range_position is None
    ):
        return active_modes

    low_band_max = strategy_float(
        strategy_config,
        "dynamic_anchor_low_band_max",
        0.35,
    )
    high_band_min = strategy_float(
        strategy_config,
        "dynamic_anchor_high_band_min",
        0.75,
    )
    midpoint_split = strategy_float(
        strategy_config,
        "dynamic_anchor_midpoint_split",
        0.5,
    )
    mid_mode = str(
        strategy_config.get("dynamic_anchor_mid_mode", "median") or "median"
    ).strip().lower()
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


def operating_mode_allows_sell_execution(operating_mode):
    return operating_mode in ("range_plus_llm", "range_only", "sell_only")


def operating_mode_allows_buy_execution(operating_mode):
    return operating_mode in ("range_plus_llm", "range_only")

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


def resolve_local_path(path):
    expanded = os.path.expanduser(str(path or ""))
    if not expanded:
        return ""
    if os.path.isabs(expanded):
        return expanded
    return os.path.abspath(expanded)


def env_default_bool(name, default=False):
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def is_http_url(value):
    return str(value or "").strip().lower().startswith(("http://", "https://"))


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


def optional_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def profile_str(name, default=""):
    value = strategy_config.get(name, default)
    return default if value is None else str(value)


def profile_bool(name, default):
    value = strategy_config.get(name, default)
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


strategy_config = load_strategy_config()
strategy_config_errors = validate_strategy_config(strategy_config)
if strategy_config_errors:
    raise RuntimeError(
        "Invalid strategy configuration: "
        + "; ".join(strategy_config_errors)
    )
bucket_inventory_caps_config = strategy_config.get("max_inventory_usd_by_bucket", {})
if not isinstance(bucket_inventory_caps_config, dict):
    bucket_inventory_caps_config = {}

order_tracker_url = (
    os.getenv("RANGE_GRID_ORDER_TRACKER_URL")
    or os.getenv("ORDER_TRACKER_URL")
    or os.getenv("EXTERNAL_ORDER_TRACKER_URL")
)
order_tracker_user_agent = (
    os.getenv("RANGE_GRID_ORDER_TRACKER_USER_AGENT")
    or os.getenv("ORDER_TRACKER_USER_AGENT")
)
order_owner_tag_source = (
    os.getenv("ORDER_OWNER_TAG_SOURCE")
    or order_tracker_user_agent
    or socket.gethostname()
)
order_tracker_symbol = (
    os.getenv("ORDER_TRACKER_SYMBOL")
    or profile_str("order_tracker_symbol", KRAKEN_PAIR)
)
order_tracker_timeout_seconds = profile_float("order_tracker_timeout_seconds", 5)
order_tracker_checkin_timeout_seconds = profile_float(
    "order_tracker_checkin_timeout_seconds",
    min(order_tracker_timeout_seconds, 5)
)
order_tracker_skip_logged = False
order_tracker_checkin_skip_logged = False

range_window_hours = profile_int("range_window_hours", 24)
max_grid_size = profile_int("max_grid_size", 4)
profit_target_pct = profile_float("profit_target_pct", 0.01)
entry_step_pct = profile_float("entry_step_pct", profit_target_pct / 2)
round_trip_fee_pct = effective_round_trip_fee_pct(strategy_config, 0.0032)
position_size_pct = profile_float("position_size_pct", 0.10)
min_buy_notional_usd = profile_float("min_buy_notional_usd", 8.0)
min_buy_volume_btc = profile_float("min_buy_volume_btc", 0.00010)
execution_signal_threshold = profile_float("execution_signal_threshold", 0.0)
llm_target_proximity_pct = profile_float(
    "llm_target_proximity_pct",
    entry_step_pct
)
llm_target_min_signal = profile_float(
    "llm_target_min_signal",
    min(-0.05, execution_signal_threshold)
)
low_min_signal = profile_float(
    "low_min_signal",
    min(-0.05, execution_signal_threshold)
)
mean_min_signal = profile_float(
    "mean_min_signal",
    execution_signal_threshold
)
median_min_signal = profile_float(
    "median_min_signal",
    execution_signal_threshold
)
high_min_signal = profile_float(
    "high_min_signal",
    max(0.05, execution_signal_threshold)
)
min_signal_status = strategy_config.get("min_signal_status", "fresh")
risk_multiplier_floor = profile_float("risk_multiplier_floor", 0.75)
risk_multiplier_ceiling = profile_float("risk_multiplier_ceiling", 1.15)
flow_defensive_threshold = profile_float("flow_defensive_threshold", -0.20)
flow_block_threshold = profile_float("flow_block_threshold", -0.40)
flow_defensive_size_multiplier = profile_float(
    "flow_defensive_size_multiplier",
    0.75
)
flow_block_high_only = bool(
    strategy_config.get("flow_block_high_only", True)
)
flow_block_llm_only_below = profile_float("flow_block_llm_only_below", -0.50)
mean_reversion_min_opportunity = profile_float(
    "mean_reversion_min_opportunity",
    0.0
)
allow_range_fallback_without_sentiment = profile_bool(
    "allow_range_fallback_without_sentiment",
    True
)
require_fresh_signal = bool(strategy_config.get("require_fresh_signal", True))
price_check_interval_seconds = profile_int("price_check_interval_seconds", 120)
range_refresh_interval_minutes = profile_int("range_refresh_interval_minutes", 60)
max_open_sell_orders = profile_int("max_open_sell_orders", 999999)
max_inventory_usd = profile_float("max_inventory_usd", 1e18)
aging_start_minutes = profile_int("aging_start_minutes", 999999)
aging_step_minutes = profile_int("aging_step_minutes", 60)
aging_profit_reduction_pct = profile_float("aging_profit_reduction_pct", 0.0)
min_profit_target_pct = profile_float(
    "min_profit_target_pct",
    profit_target_pct
)
high_anchor_buy_cooldown_minutes = profile_int(
    "high_anchor_buy_cooldown_minutes",
    15
)
max_open_high_anchor_orders = profile_int("max_open_high_anchor_orders", 3)
high_anchor_backlog_soft_release_minutes = profile_int(
    "high_anchor_backlog_soft_release_minutes",
    0
)
high_anchor_backlog_old_order_weight = profile_float(
    "high_anchor_backlog_old_order_weight",
    1.0
)
high_anchor_profit_target_pct = profile_float(
    "high_anchor_profit_target_pct",
    profit_target_pct
)
prevent_buy_above_last_sell = bool(
    strategy_config.get("prevent_buy_above_last_sell", True)
)
allow_high_band_breakout_above_last_sell = profile_bool(
    "allow_high_band_breakout_above_last_sell",
    False
)
buy_after_sell_discount_pct = profile_float(
    "buy_after_sell_discount_pct",
    0.001
)
llm_buy_cooldown_minutes_after_sell = profile_int(
    "llm_buy_cooldown_minutes_after_sell",
    30
)
source_profit_target_offset_pct_by_source = normalized_source_config_map(
    strategy_config,
    "sell_target_offset_pct_by_source"
)
source_aging_start_minutes_by_source = normalized_source_config_map(
    strategy_config,
    "aging_start_minutes_by_source"
)
source_aging_step_minutes_by_source = normalized_source_config_map(
    strategy_config,
    "aging_step_minutes_by_source"
)
source_aging_profit_reduction_pct_by_source = normalized_source_config_map(
    strategy_config,
    "aging_profit_reduction_pct_by_source"
)
source_min_profit_target_pct_by_source = normalized_source_config_map(
    strategy_config,
    "min_profit_target_pct_by_source"
)
sentiment_defensive_threshold = profile_float(
    "sentiment_defensive_threshold",
    max(0.03, execution_signal_threshold)
)
sentiment_risk_on_threshold = profile_float("sentiment_risk_on_threshold", 0.12)
sentiment_paused_profit_target_offset_pct = profile_float(
    "sentiment_paused_profit_target_offset_pct",
    -0.001
)
sentiment_paused_size_multiplier = profile_float(
    "sentiment_paused_size_multiplier",
    0.0
)
sentiment_paused_inventory_multiplier = profile_float(
    "sentiment_paused_inventory_multiplier",
    0.0
)
sentiment_paused_open_sell_multiplier = profile_float(
    "sentiment_paused_open_sell_multiplier",
    0.0
)
sentiment_defensive_profit_target_offset_pct = profile_float(
    "sentiment_defensive_profit_target_offset_pct",
    -0.0005
)
sentiment_risk_on_profit_target_offset_pct = profile_float(
    "sentiment_risk_on_profit_target_offset_pct",
    0.0005
)
sentiment_defensive_size_multiplier = profile_float(
    "sentiment_defensive_size_multiplier",
    0.65
)
sentiment_risk_on_size_multiplier = profile_float(
    "sentiment_risk_on_size_multiplier",
    1.2
)
sentiment_defensive_inventory_multiplier = profile_float(
    "sentiment_defensive_inventory_multiplier",
    0.7
)
sentiment_risk_on_inventory_multiplier = profile_float(
    "sentiment_risk_on_inventory_multiplier",
    1.2
)
sentiment_defensive_open_sell_multiplier = profile_float(
    "sentiment_defensive_open_sell_multiplier",
    0.75
)
sentiment_risk_on_open_sell_multiplier = profile_float(
    "sentiment_risk_on_open_sell_multiplier",
    1.25
)
sentiment_disable_high_anchor_below = profile_float(
    "sentiment_disable_high_anchor_below",
    0.05
)
sentiment_defensive_extra_aging_reduction_pct = profile_float(
    "sentiment_defensive_extra_aging_reduction_pct",
    0.001
)
risk_context_high_band_guard_enabled = profile_bool(
    "risk_context_high_band_guard_enabled",
    False
)
risk_context_high_band_min_buy_aggression_score = profile_float(
    "risk_context_high_band_min_buy_aggression_score",
    0.50
)
risk_context_high_band_min_breakout_score = profile_float(
    "risk_context_high_band_min_breakout_score",
    0.50
)
risk_context_high_band_min_rebound_score = profile_float(
    "risk_context_high_band_min_rebound_score",
    0.50
)
risk_context_high_band_max_market_risk_score = profile_float(
    "risk_context_high_band_max_market_risk_score",
    0.35
)
risk_context_position_sizing_enabled = profile_bool(
    "risk_context_position_sizing_enabled",
    False
)
risk_context_position_size_min_multiplier = profile_float(
    "risk_context_position_size_min_multiplier",
    0.35
)
risk_context_position_size_max_multiplier = profile_float(
    "risk_context_position_size_max_multiplier",
    1.0
)
risk_context_position_size_blend = profile_float(
    "risk_context_position_size_blend",
    0.5
)
range_fallback_execution_signal = profile_float(
    "range_fallback_execution_signal",
    max(execution_signal_threshold, sentiment_defensive_threshold)
)
max_daily_loss_usd = profile_float("max_daily_loss_usd", 0.0)
disable_new_buys_on_sell_backlog_count = profile_int(
    "disable_new_buys_on_sell_backlog_count",
    0
)
disable_new_buys_on_sell_backlog_minutes = profile_int(
    "disable_new_buys_on_sell_backlog_minutes",
    0
)
reconcile_state_interval_minutes = profile_int(
    "reconcile_state_interval_minutes",
    30
)
max_consecutive_loop_errors = profile_int("max_consecutive_loop_errors", 10)
max_consecutive_private_api_failures = profile_int(
    "max_consecutive_private_api_failures",
    10
)
execution_quality_alerts_enabled = profile_bool(
    "execution_quality_alerts_enabled",
    True
)
execution_quality_min_approved_candidates = profile_int(
    "execution_quality_min_approved_candidates",
    25
)
execution_quality_min_buy_orders_placed = profile_int(
    "execution_quality_min_buy_orders_placed",
    10
)
execution_quality_min_buy_orders_filled_for_exit_alert = profile_int(
    "execution_quality_min_buy_orders_filled_for_exit_alert",
    10
)
execution_quality_min_approval_to_placement_rate = profile_float(
    "execution_quality_min_approval_to_placement_rate",
    0.9
)
execution_quality_min_placement_to_fill_rate = profile_float(
    "execution_quality_min_placement_to_fill_rate",
    0.4
)
execution_quality_min_fill_to_exit_rate = profile_float(
    "execution_quality_min_fill_to_exit_rate",
    0.25
)
execution_quality_max_buy_rejections = profile_int(
    "execution_quality_max_buy_rejections",
    5
)
execution_quality_exit_backlog_min_age_minutes = profile_int(
    "execution_quality_exit_backlog_min_age_minutes",
    360
)
grid_anchor = strategy_config.get("grid_anchor", "low").strip().lower()
operating_mode = normalize_operating_mode(
    strategy_config.get("operating_mode", "range_plus_llm")
)
paper_trading_enabled = profile_bool("paper_trading_enabled", False)
risk_context_shadow_buy_enabled = profile_bool(
    "risk_context_shadow_buy_enabled",
    env_default_bool("RANGE_GRID_RISK_CONTEXT_SHADOW_BUY_ENABLED", False)
)
anchor_strategy_router_enabled = profile_bool(
    "anchor_strategy_router_enabled",
    env_default_bool("RANGE_GRID_ANCHOR_ROUTER_ENABLED", False)
)
anchor_strategy_router_file = (
    os.getenv("RANGE_GRID_ANCHOR_ROUTER_FILE")
    or profile_str("anchor_strategy_router_file", ANCHOR_STRATEGY_ROUTER_FILE)
)
anchor_strategy_router_refresh_seconds = profile_int(
    "anchor_strategy_router_refresh_seconds",
    300
)
anchor_strategy_router_timeout_seconds = profile_float(
    "anchor_strategy_router_timeout_seconds",
    float(os.getenv("RANGE_GRID_ANCHOR_ROUTER_TIMEOUT_SECONDS", "5"))
)
anchor_strategy_router_fail_closed = profile_bool(
    "anchor_strategy_router_fail_closed",
    False
)
configured_strategy_modes = parse_strategy_modes(grid_anchor)
strategy_modes = apply_operating_mode_to_strategy_modes(
    configured_strategy_modes,
    operating_mode
)
anchor_strategy_router_cache = {
    "loaded_at": 0,
    "path": None,
    "routes": {},
    "error": None,
}

# ----------------------
# KRAKEN INIT
# ----------------------

api = krakenex.API()
api.uri = KRAKEN_API_URL
api.key = os.getenv("KRAKEN_API_KEY")
api.secret = os.getenv("KRAKEN_API_SECRET")

pair_info = api.query_public("AssetPairs")["result"]["XXBTZUSD"]

PRICE_DECIMALS = pair_info["pair_decimals"]
VOLUME_DECIMALS = pair_info["lot_decimals"]
INSTANCE_LOCK_HANDLE = None

# ----------------------
# LOGGING
# ----------------------


def log_event(event, **kwargs):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "message": kwargs.pop("message", "")
    }

    record.update(kwargs)

    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
    except Exception as e:
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] "
            f"LOG_WRITE_ERROR: {e}"
        )


def console(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def log_and_console(event, message="", **kwargs):
    log_event(event, message=message, **kwargs)

    if message:
        console(f"{event}: {message}")
    else:
        console(event)


def append_jsonl(path, record):
    path = os.path.abspath(path)
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


def rotated_log_path(base_path, dt):
    root, ext = os.path.splitext(base_path)
    suffix = dt.strftime("%Y%m%d")
    return f"{root}_{suffix}{ext or '.jsonl'}"


def log_trade_activity(event, **kwargs):
    now = datetime.now(timezone.utc)
    record = {
        "ts": now.isoformat(),
        "event": event,
    }
    record.update(kwargs)

    try:
        path = (
            rotated_log_path(ACTIVITY_LOG_FILE, now)
            if ACTIVITY_LOG_ROTATE_DAILY else
            ACTIVITY_LOG_FILE
        )
        append_jsonl(path, record)
    except Exception as e:
        log_event("ACTIVITY_LOG_WRITE_ERROR", message=str(e), activity_event=event)


def write_json_file(path, payload):
    path = os.path.abspath(path)
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def load_anchor_router_payload(source):
    if is_http_url(source):
        response = requests.get(
            source,
            timeout=anchor_strategy_router_timeout_seconds
        )
        response.raise_for_status()
        return response.json()

    return load_json_file(resolve_local_path(source))


def load_anchor_strategy_routes(force=False):
    if not anchor_strategy_router_enabled:
        return {}

    now = time.time()
    path = (
        anchor_strategy_router_file
        if is_http_url(anchor_strategy_router_file)
        else resolve_local_path(anchor_strategy_router_file)
    )
    cache_age = now - anchor_strategy_router_cache.get("loaded_at", 0)
    if (
        not force
        and anchor_strategy_router_cache.get("path") == path
        and cache_age < anchor_strategy_router_refresh_seconds
    ):
        return anchor_strategy_router_cache.get("routes", {})

    routes = {}
    error = None
    try:
        payload = load_anchor_router_payload(path)
        winners = payload.get("winners", {}) if isinstance(payload, dict) else {}
        if not isinstance(winners, dict):
            raise RuntimeError("anchor router file winners must be an object")

        for anchor in ("low", "median", "high"):
            selected = (winners.get(anchor) or {}).get("selected")
            if not isinstance(selected, dict) or not selected.get("eligible"):
                continue

            strategy_payload = selected.get("strategy_payload")
            if not isinstance(strategy_payload, dict):
                log_event(
                    "ANCHOR_STRATEGY_ROUTE_SKIPPED",
                    anchor=anchor,
                    strategy_label=selected.get("strategy_label"),
                    reason="missing_strategy_payload",
                    router_file=path,
                )
                continue

            errors = validate_strategy_config(strategy_payload)
            if errors:
                log_event(
                    "ANCHOR_STRATEGY_ROUTE_SKIPPED",
                    anchor=anchor,
                    strategy_label=selected.get("strategy_label"),
                    reason="invalid_strategy_payload",
                    validation_errors=errors,
                    router_file=path,
                )
                continue

            if (
                not paper_trading_enabled
                and (
                    strategy_bool(strategy_payload, "paper_trading_enabled", False)
                    or strategy_bool(strategy_payload, "probe_only", False)
                )
            ):
                log_event(
                    "ANCHOR_STRATEGY_ROUTE_SKIPPED",
                    anchor=anchor,
                    strategy_label=selected.get("strategy_label"),
                    reason="paper_or_probe_profile_on_live_bot",
                    router_file=path,
                )
                continue

            route = dict(selected)
            route["anchor"] = anchor
            route["strategy_config"] = strategy_payload
            routes[anchor] = route
    except Exception as exc:
        error = str(exc)

    previous_error = anchor_strategy_router_cache.get("error")
    anchor_strategy_router_cache.update({
        "loaded_at": now,
        "path": path,
        "routes": routes,
        "error": error,
    })

    if error and error != previous_error:
        log_event(
            "ANCHOR_STRATEGY_ROUTER_LOAD_ERROR",
            message=error,
            router_file=path,
            fail_closed=anchor_strategy_router_fail_closed,
        )
    elif not error:
        log_event(
            "ANCHOR_STRATEGY_ROUTER_LOADED",
            router_file=path,
            route_count=len(routes),
            anchors=sorted(routes),
        )

    return routes


def anchor_strategy_route_for_source(buy_source):
    anchor = anchor_for_buy_source(buy_source)
    if not anchor or not anchor_strategy_router_enabled:
        return {
            "anchor": anchor,
            "route": None,
            "block_reason": None,
        }

    routes = load_anchor_strategy_routes()
    route = routes.get(anchor)
    block_reason = None
    if route is None and anchor_strategy_router_fail_closed:
        block_reason = "anchor_strategy_router_no_route"

    return {
        "anchor": anchor,
        "route": route,
        "block_reason": block_reason,
    }


def routed_effective_entry_step_pct(route_config, volatility_pct):
    return effective_entry_step_pct(
        strategy_float(route_config, "entry_step_pct", entry_step_pct),
        volatility_pct,
        route_config,
    )


def routed_effective_limits(route_config, regime, risk_multiplier):
    route_position_size_pct = (
        strategy_float(route_config, "position_size_pct", position_size_pct)
        * regime["position_size_multiplier"]
        * risk_multiplier
    )
    route_max_inventory_usd = (
        strategy_float(route_config, "max_inventory_usd", max_inventory_usd)
        * regime["inventory_multiplier"]
        * risk_multiplier
    )
    route_max_open_sell_orders = max(
        1,
        int(round(
            strategy_float(route_config, "max_open_sell_orders", max_open_sell_orders)
            * regime["open_sell_multiplier"]
            * risk_multiplier
        ))
    )
    return {
        "position_size_pct": route_position_size_pct,
        "max_inventory_usd": route_max_inventory_usd,
        "max_open_sell_orders": route_max_open_sell_orders,
    }


def emit_alert(alert_type, severity, message, **kwargs):
    now = datetime.now(timezone.utc)
    dedup_key = f"{alert_type}:{severity}:{message}"
    last_alerts = state.setdefault("last_alerts", {})
    previous_at = parse_iso8601(last_alerts.get(dedup_key))
    if previous_at is not None:
        age_minutes = (now - previous_at).total_seconds() / 60.0
        if age_minutes < ALERT_DEDUP_MINUTES:
            return

    record = {
        "ts": now.isoformat(),
        "event": "ALERT",
        "alert_type": alert_type,
        "severity": severity,
        "message": message,
    }
    record.update(kwargs)
    log_event("ALERT", alert_type=alert_type, severity=severity, message=message, **kwargs)
    try:
        append_jsonl(ALERT_LOG_FILE, record)
    except Exception as e:
        log_event("ALERT_LOG_WRITE_ERROR", message=str(e), alert_type=alert_type)

    last_alerts[dedup_key] = now.isoformat()
    save_state(state)


def realized_pnl_for_utc_day(day_start):
    total = 0.0
    try:
        with open(LOG_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except Exception:
                    continue
                if record.get("event") != "SELL_ORDER_FILLED":
                    continue
                ts = parse_iso8601(record.get("ts"))
                if ts is None or ts < day_start:
                    continue
                try:
                    total += float(record.get("estimated_net_pnl") or 0.0)
                except Exception:
                    continue
    except FileNotFoundError:
        return 0.0
    except Exception as e:
        log_event("DAILY_PNL_SCAN_ERROR", message=str(e))
    return total


def write_status_snapshot(payload):
    try:
        write_json_file(STATUS_FILE, payload)
    except Exception as e:
        log_event("STATUS_FILE_WRITE_ERROR", message=str(e), status_file=os.path.abspath(STATUS_FILE))


def increment_source_stat(stats, field, buy_source):
    source_counts = stats.setdefault(field, {})
    if not isinstance(source_counts, dict):
        source_counts = {}
        stats[field] = source_counts
    source_key = str(buy_source or "unknown")
    source_counts[source_key] = int(source_counts.get(source_key, 0) or 0) + 1


def safe_rate(numerator, denominator):
    try:
        denominator_value = float(denominator or 0)
        if denominator_value <= 0:
            return None
        return round(float(numerator or 0) / denominator_value, 4)
    except Exception:
        return None


def source_sell_policy(buy_source):
    source_key = str(buy_source or "").strip().lower()
    return {
        "profit_target_offset_pct": source_profit_target_offset_pct_by_source.get(
            source_key,
            0.0
        ),
        "aging_start_minutes": source_aging_start_minutes_by_source.get(
            source_key,
            aging_start_minutes
        ),
        "aging_step_minutes": source_aging_step_minutes_by_source.get(
            source_key,
            aging_step_minutes
        ),
        "aging_profit_reduction_pct": (
            source_aging_profit_reduction_pct_by_source.get(
                source_key,
                aging_profit_reduction_pct
            )
        ),
        "min_profit_target_pct": source_min_profit_target_pct_by_source.get(
            source_key,
            min_profit_target_pct
        ),
    }


def effective_sell_profit_target(
    *,
    age_minutes,
    base_profit_target=None,
    buy_source=None,
    regime=None
):
    regime = regime or {}
    policy = source_sell_policy(buy_source)
    starting_profit_target = (
        profit_target_pct
        if base_profit_target is None
        else base_profit_target
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


def build_execution_quality_snapshot(stats):
    stats = stats or {}
    return {
        "approval_to_placement_rate": safe_rate(
            stats.get("buy_orders_placed"),
            stats.get("approved_buy_candidates")
        ),
        "placement_to_fill_rate": safe_rate(
            stats.get("buy_orders_filled"),
            stats.get("buy_orders_placed")
        ),
        "fill_to_exit_rate": safe_rate(
            stats.get("sell_orders_filled"),
            stats.get("buy_orders_filled")
        ),
    }


def emit_execution_quality_alerts(
    *,
    cycle_id,
    stats,
    execution_quality,
    sell_backlog,
):
    if not execution_quality_alerts_enabled:
        return

    approved_candidates = int(stats.get("approved_buy_candidates", 0) or 0)
    buy_rejections = int(stats.get("buy_order_rejections", 0) or 0)
    buy_orders_placed = int(stats.get("buy_orders_placed", 0) or 0)
    buy_orders_filled = int(stats.get("buy_orders_filled", 0) or 0)
    sell_orders_filled = int(stats.get("sell_orders_filled", 0) or 0)

    approval_to_placement_rate = execution_quality.get(
        "approval_to_placement_rate"
    )
    placement_to_fill_rate = execution_quality.get("placement_to_fill_rate")
    fill_to_exit_rate = execution_quality.get("fill_to_exit_rate")

    if (
        approved_candidates >= execution_quality_min_approved_candidates
        and approval_to_placement_rate is not None
        and approval_to_placement_rate < execution_quality_min_approval_to_placement_rate
    ):
        emit_alert(
            "execution_quality_approval_to_placement",
            "warning",
            "Approval to placement conversion is below threshold",
            cycle_id=cycle_id,
            approved_buy_candidates=approved_candidates,
            buy_orders_placed=buy_orders_placed,
            buy_order_rejections=buy_rejections,
            approval_to_placement_rate=approval_to_placement_rate,
            min_approval_to_placement_rate=(
                execution_quality_min_approval_to_placement_rate
            ),
        )

    if (
        buy_orders_placed >= execution_quality_min_buy_orders_placed
        and placement_to_fill_rate is not None
        and placement_to_fill_rate < execution_quality_min_placement_to_fill_rate
    ):
        emit_alert(
            "execution_quality_placement_to_fill",
            "warning",
            "Placement to fill conversion is below threshold",
            cycle_id=cycle_id,
            buy_orders_placed=buy_orders_placed,
            buy_orders_filled=buy_orders_filled,
            placement_to_fill_rate=placement_to_fill_rate,
            min_placement_to_fill_rate=(
                execution_quality_min_placement_to_fill_rate
            ),
        )

    if (
        execution_quality_max_buy_rejections > 0
        and buy_rejections >= execution_quality_max_buy_rejections
    ):
        emit_alert(
            "execution_quality_buy_rejections",
            "warning",
            "Buy order rejections exceeded threshold",
            cycle_id=cycle_id,
            buy_order_rejections=buy_rejections,
            max_buy_rejections=execution_quality_max_buy_rejections,
            approved_buy_candidates=approved_candidates,
            buy_orders_placed=buy_orders_placed,
        )

    if (
        buy_orders_filled >= execution_quality_min_buy_orders_filled_for_exit_alert
        and fill_to_exit_rate is not None
        and fill_to_exit_rate < execution_quality_min_fill_to_exit_rate
        and (
            float(sell_backlog.get("oldest_age_minutes") or 0.0)
            >= execution_quality_exit_backlog_min_age_minutes
        )
    ):
        emit_alert(
            "execution_quality_fill_to_exit",
            "warning",
            "Fill to exit conversion is below threshold with aged sell backlog",
            cycle_id=cycle_id,
            buy_orders_filled=buy_orders_filled,
            sell_orders_filled=sell_orders_filled,
            sell_backlog_count=sell_backlog.get("count"),
            sell_backlog_oldest_minutes=round(
                float(sell_backlog.get("oldest_age_minutes") or 0.0),
                2
            ),
            fill_to_exit_rate=fill_to_exit_rate,
            min_fill_to_exit_rate=execution_quality_min_fill_to_exit_rate,
            min_buy_orders_filled=(
                execution_quality_min_buy_orders_filled_for_exit_alert
            ),
            min_sell_backlog_oldest_minutes=(
                execution_quality_exit_backlog_min_age_minutes
            ),
        )


def key_fingerprint(value):
    if not value:
        return "missing"

    digest = hashlib.sha256(value.encode()).hexdigest()[:12]
    return f"sha256:{digest}"


def kraken_userref_for_value(value):
    normalized = str(value or "").strip()
    if not normalized:
        normalized = socket.gethostname()

    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    userref = int.from_bytes(digest[:8], "big") % 2147483646
    return userref + 1


BOT_ORDER_USERREF = kraken_userref_for_value(order_owner_tag_source)


def runtime_identity():
    return {
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "state_file": os.path.abspath(STATE_FILE),
        "lock_file": os.path.abspath(INSTANCE_LOCK_FILE),
        "log_file": os.path.abspath(LOG_FILE),
        "strategy_profile": STRATEGY_PROFILE,
        "api_key_fingerprint": key_fingerprint(KRAKEN_API_KEY),
        "order_owner_tag_source": order_owner_tag_source,
        "order_owner_userref": BOT_ORDER_USERREF,
    }


def release_instance_lock():
    global INSTANCE_LOCK_HANDLE

    if INSTANCE_LOCK_HANDLE is None:
        return

    try:
        fcntl.flock(INSTANCE_LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
    except Exception:
        pass

    try:
        INSTANCE_LOCK_HANDLE.close()
    except Exception:
        pass

    INSTANCE_LOCK_HANDLE = None


def acquire_instance_lock():
    global INSTANCE_LOCK_HANDLE

    lock_path = os.path.abspath(INSTANCE_LOCK_FILE)
    lock_dir = os.path.dirname(lock_path)
    if lock_dir:
        os.makedirs(lock_dir, exist_ok=True)

    lock_handle = open(lock_path, "a+", encoding="utf-8")

    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        lock_handle.seek(0)
        existing_metadata = lock_handle.read().strip()
        lock_handle.close()
        metadata = runtime_identity()
        log_and_console(
            "INSTANCE_LOCKED",
            message="Another range grid bot instance is already running",
            existing_lock_metadata=existing_metadata or None,
            **metadata
        )
        raise SystemExit(1)

    metadata = runtime_identity()
    lock_handle.seek(0)
    lock_handle.truncate()
    lock_handle.write(json.dumps({
        **metadata,
        "locked_at": datetime.now(timezone.utc).isoformat()
    }))
    lock_handle.flush()

    INSTANCE_LOCK_HANDLE = lock_handle
    atexit.register(release_instance_lock)
    return metadata


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
    global order_tracker_skip_logged

    if not order_tracker_url or not order_tracker_user_agent:
        if not order_tracker_skip_logged:
            order_tracker_skip_logged = True
            log_event(
                "ORDER_TRACKER_SKIPPED",
                reason="missing_tracker_config",
                order_tracker_url_configured=bool(order_tracker_url),
                order_tracker_user_agent_configured=(
                    bool(order_tracker_user_agent)
                ),
                side=side,
                order_id=order_id
            )
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
        "symbol": order_tracker_symbol,
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
            order_tracker_url,
            data=payload,
            headers={"User-Agent": order_tracker_user_agent},
            timeout=order_tracker_timeout_seconds
        )
        response.raise_for_status()
        log_event(
            "ORDER_TRACKER_UPDATED",
            trade_id=trade_id,
            side=side,
            order_id=order_id,
            status_code=response.status_code
        )
    except Exception as e:
        log_event(
            "ORDER_TRACKER_ERROR",
            message=str(e),
            trade_id=trade_id,
            side=side,
            order_id=order_id
        )
        emit_alert(
            "order_tracker_error",
            "warning",
            "Order tracker update failed",
            trade_id=trade_id,
            side=side,
            order_id=order_id,
            error=short_error_summary(e)
        )


def short_error_summary(error):
    return str(error).replace("\n", " ")[:200]


def send_checkin(status="ok", loop_count=None, message="loop_complete"):
    global order_tracker_checkin_skip_logged

    if not order_tracker_url or not order_tracker_user_agent:
        if not order_tracker_checkin_skip_logged:
            order_tracker_checkin_skip_logged = True
            log_event(
                "ORDER_TRACKER_CHECKIN_SKIPPED",
                reason="missing_tracker_config",
                order_tracker_url_configured=bool(order_tracker_url),
                order_tracker_user_agent_configured=(
                    bool(order_tracker_user_agent)
                ),
                status=status
            )
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
            order_tracker_url,
            data=payload,
            headers={"User-Agent": order_tracker_user_agent},
            timeout=order_tracker_checkin_timeout_seconds
        )
        response.raise_for_status()
    except Exception as e:
        log_event("ORDER_TRACKER_CHECKIN_ERROR", message=str(e), status=status)
        emit_alert(
            "order_tracker_checkin_error",
            "warning",
            "Order tracker check-in failed",
            status=status,
            error=short_error_summary(e)
        )


# ----------------------
# SAFE KRAKEN WRAPPER
# ----------------------

def kraken_call(label, fn, *args, **kwargs):
    try:
        resp = fn(*args, **kwargs)
    except Exception as e:
        log_event("KRAKEN_EXCEPTION", operation=label, message=str(e))
        return None

    if not isinstance(resp, dict):
        log_event("KRAKEN_BAD_RESPONSE", operation=label)
        return None

    if resp.get("error"):
        log_event("KRAKEN_API_ERROR", operation=label, error=resp["error"])
        return resp

    return resp


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


def kraken_private(endpoint, data=None):
    payload = dict(data or {})
    payload["nonce"] = next_nonce()
    url = KRAKEN_API_URL.rstrip("/") + endpoint
    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": kraken_signature(endpoint, payload)
    }

    response = requests.post(
        url,
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
    backoff_until = float(state.get("private_api_backoff_until", 0) or 0)
    now_ts = time.time()
    if backoff_until > now_ts:
        log_event(
            "KRAKEN_BACKOFF_ACTIVE",
            operation=label,
            wait_seconds=round(backoff_until - now_ts, 2)
        )
        return None

    attempts = max(1, KRAKEN_NONCE_RETRIES + 1)

    for attempt in range(1, attempts + 1):
        try:
            result = kraken_private(endpoint, data)
            if state.get("consecutive_private_api_failures", 0):
                state["consecutive_private_api_failures"] = 0
                save_state(state)
            return result
        except Exception as e:
            message = str(e)
            state["consecutive_private_api_failures"] = int(
                state.get("consecutive_private_api_failures", 0) or 0
            ) + 1
            save_state(state)
            log_event(
                "KRAKEN_EXCEPTION",
                operation=label,
                message=message,
                attempt=attempt
            )
            if (
                max_consecutive_private_api_failures > 0
                and state["consecutive_private_api_failures"]
                >= max_consecutive_private_api_failures
            ):
                emit_alert(
                    "kraken_private_failures",
                    "critical",
                    "Kraken private API failures exceeded configured threshold",
                    operation=label,
                    consecutive_private_api_failures=state[
                        "consecutive_private_api_failures"
                    ]
                )

            if "Temporary lockout" in message:
                state["private_api_backoff_until"] = (
                    time.time() + KRAKEN_LOCKOUT_COOLDOWN_SECONDS
                )
                save_state(state)
                emit_alert(
                    "kraken_lockout",
                    "warning",
                    "Kraken private API entered temporary lockout",
                    operation=label,
                    cooldown_seconds=KRAKEN_LOCKOUT_COOLDOWN_SECONDS
                )
                return None

            if "Invalid nonce" not in message or attempt >= attempts:
                return None

            state["last_nonce"] = max(
                int(state.get("last_nonce", 0)),
                int(time.time() * 1000)
            ) + 1000
            save_state(state)
            time.sleep(1)

    return None


# ----------------------
# STATE
# ----------------------

def load_state():
    default = {
        "open_buy_orders": {},
        "open_sell_orders": {},
        "processed_fills": {
            "buy": {},
            "sell": {}
        },
        "last_nonce": 0,
        "private_api_backoff_until": 0,
        "sell_insufficient_funds_backoff_until": 0,
        "range_low": None,
        "range_high": None,
        "range_mean": None,
        "range_median": None,
        "last_range_refresh": None,
        "last_high_anchor_buy_at": None,
        "last_sell_price": None,
        "last_sell_at": None,
        "last_llm_sell_at": None,
        "last_alerts": {},
        "last_state_reconcile_at": None,
        "consecutive_loop_errors": 0,
        "consecutive_private_api_failures": 0,
        "stats": {
            "approved_buy_candidates": 0,
            "buy_orders_placed": 0,
            "buy_orders_filled": 0,
            "sell_orders_placed": 0,
            "sell_orders_filled": 0,
            "buy_order_rejections": 0,
            "realized_gross_pnl": 0.0,
            "realized_estimated_net_pnl": 0.0,
            "approved_counts_by_source": {},
            "buy_orders_placed_by_source": {},
            "buy_orders_filled_by_source": {},
            "sell_orders_placed_by_source": {},
            "sell_orders_filled_by_source": {}
        }
    }

    if not os.path.exists(STATE_FILE):
        return default

    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)

    for key in default:
        if key not in state:
            state[key] = default[key]

    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def normalize_state(state):
    normalized_buy_orders = {}
    normalized_sell_orders = {}
    state.setdefault("stats", {})
    state.setdefault("processed_fills", {})
    state.setdefault("last_alerts", {})
    if not isinstance(state["last_alerts"], dict):
        state["last_alerts"] = {}
    state["consecutive_loop_errors"] = int(state.get("consecutive_loop_errors", 0) or 0)
    state["consecutive_private_api_failures"] = int(
        state.get("consecutive_private_api_failures", 0) or 0
    )
    if not isinstance(state["processed_fills"], dict):
        state["processed_fills"] = {}
    for side in ("buy", "sell"):
        side_cache = state["processed_fills"].get(side)
        if not isinstance(side_cache, dict):
            state["processed_fills"][side] = {}

    for key, default_value in {
        "approved_buy_candidates": 0,
        "buy_orders_placed": 0,
        "buy_orders_filled": 0,
        "sell_orders_placed": 0,
        "sell_orders_filled": 0,
        "buy_order_rejections": 0,
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0,
        "approved_counts_by_source": {},
        "buy_orders_placed_by_source": {},
        "buy_orders_filled_by_source": {},
        "sell_orders_placed_by_source": {},
        "sell_orders_filled_by_source": {}
    }.items():
        state["stats"].setdefault(key, default_value)

    for level, order in state["open_buy_orders"].items():
        if isinstance(order, dict):
            normalized_buy_orders[level] = {
                "txid": order.get("txid"),
                "volume": order.get("volume"),
                "price": order.get("price", float(level)),
                "placed_at": order.get("placed_at"),
                "sell_pct_override": order.get("sell_pct_override"),
                "buy_source": order.get("buy_source"),
                "trade_id": order.get("trade_id") or order.get("txid"),
                "filled_at": order.get("filled_at"),
                "sell_retry_after": order.get("sell_retry_after"),
                "sell_failure_reason": order.get("sell_failure_reason")
            }
        else:
            normalized_buy_orders[level] = {
                "txid": None,
                "volume": None,
                "price": float(level),
                "placed_at": None,
                "sell_pct_override": None,
                "buy_source": None,
                "trade_id": None,
                "filled_at": None,
                "sell_retry_after": None,
                "sell_failure_reason": None
            }

    for txid, order in state["open_sell_orders"].items():
        if isinstance(order, dict) and "level" in order:
            normalized_sell_orders[txid] = {
                "level": order.get("level"),
                "volume": order.get("volume"),
                "buy_price": order.get("buy_price"),
                "sell_price": order.get("sell_price"),
                "placed_at": order.get("placed_at"),
                "sell_pct_override": order.get("sell_pct_override"),
                "buy_source": order.get("buy_source"),
                "trade_id": order.get("trade_id") or order.get("buy_txid") or txid
            }
        else:
            normalized_sell_orders[txid] = {
                "level": None,
                "volume": order.get("volume"),
                "buy_price": None,
                "sell_price": None,
                "placed_at": None,
                "sell_pct_override": None,
                "buy_source": None,
                "trade_id": txid
            }

    state["open_buy_orders"] = normalized_buy_orders
    state["open_sell_orders"] = normalized_sell_orders
    return state


def parse_iso8601(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def trim_processed_fill_cache():
    processed_fills = state.setdefault("processed_fills", {})

    for side in ("buy", "sell"):
        cache = processed_fills.get(side)
        if not isinstance(cache, dict):
            processed_fills[side] = {}
            continue
        if len(cache) <= PROCESSED_FILL_CACHE_LIMIT:
            continue

        ordered_items = sorted(
            cache.items(),
            key=lambda item: parse_iso8601(item[1]) or datetime.min.replace(
                tzinfo=timezone.utc
            )
        )
        remove_count = len(cache) - PROCESSED_FILL_CACHE_LIMIT
        for txid, _ in ordered_items[:remove_count]:
            cache.pop(txid, None)


def fill_already_processed(side, txid):
    if not txid:
        return False

    processed_fills = state.get("processed_fills", {})
    side_cache = processed_fills.get(side, {})
    return txid in side_cache


def mark_fill_processed(side, txid, processed_at):
    if not txid:
        return

    processed_fills = state.setdefault("processed_fills", {})
    side_cache = processed_fills.setdefault(side, {})
    side_cache[txid] = processed_at
    trim_processed_fill_cache()


state = normalize_state(load_state())


# ----------------------
# PRICE
# ----------------------

def get_price():
    try:
        if KRAKEN_TICKER_URL:
            r = requests.get(KRAKEN_TICKER_URL, timeout=5)
            data = r.json()

            if "result" in data:
                pair = list(data["result"].keys())[0]
                return float(data["result"][pair]["c"][0])

        r = api.query_public("Ticker", {"pair": "XXBTZUSD"})
        return float(r["result"]["XXBTZUSD"]["c"][0])
    except Exception as e:
        log_event("PRICE_ERROR", message=str(e))
        return None


# ----------------------
# SENTIMENT
# ----------------------

def get_sentiment():
    try:
        r = requests.get(LLM_SIGNAL_URL, timeout=5)
        data = r.json()

        if isinstance(data, dict):
            normalized = normalize_signal_payload(data, pair=KRAKEN_PAIR)
            normalized["action_reason"] = (
                (
                    normalized.get("action_policy", {}).get("reason")
                    if isinstance(normalized.get("action_policy"), dict)
                    else None
                )
                or normalized.get("reason")
            )
            normalized["source"] = data.get("source")
            normalized["trend_snapshot"] = (
                normalized.get("trend_snapshot")
                if isinstance(normalized.get("trend_snapshot"), dict)
                else {}
            )
            normalized["kraken_flow"] = (
                normalized.get("kraken_flow")
                if isinstance(normalized.get("kraken_flow"), dict)
                else {}
            )
            return normalized

        return {
            "schema_version": None,
            "execution_signal": float(data),
            "target_prices": [],
            "price_regime": {},
            "trend_snapshot": {},
            "kraken_flow": {},
            "source_status": {},
            "action_policy": {},
            "source": None,
            "processed_at": None,
            "active_observation_count": None
        }
    except Exception as e:
        log_event("SENTIMENT_ERROR", message=str(e))
        return None


def synthetic_range_sentiment_payload(reason, price):
    return {
        "schema_version": "range_fallback_v1",
        "execution_signal": range_fallback_execution_signal,
        "target_prices": [],
        "risk_multiplier": 1.0,
        "smoothed_risk_multiplier": 1.0,
        "btc_sentiment": None,
        "regulatory_risk": None,
        "macro_tightening_bias": None,
        "confidence": None,
        "direction_bias": None,
        "raw_btc_sentiment": None,
        "raw_regulatory_risk": None,
        "raw_macro_tightening_bias": None,
        "raw_confidence": None,
        "raw_direction_bias": None,
        "btc_price": price,
        "fear_greed_index": None,
        "flow_pressure": None,
        "mean_reversion_opportunity": mean_reversion_min_opportunity,
        "signal_status": "fallback",
        "source_status": {},
        "action_recommendation": "neutral",
        "action_policy": {
            "recommendation": "neutral",
            "reason": reason,
        },
        "contributor_count": None,
        "active_observation_count": None,
        "bot_action_allowed": True,
        "action_reason": reason,
        "source": "range_fallback",
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "price_regime": {},
        "trend_snapshot": {},
        "kraken_flow": {},
        "risk_context": {},
    }


def sentiment_risk_log_fields(risk_context):
    if not isinstance(risk_context, dict):
        risk_context = {}

    flags = risk_context.get("hard_safety_flags")
    if not isinstance(flags, list):
        flags = []
    weather = weather_report_payload(risk_context)
    bot_tuning = weather_bot_tuning(weather)
    market_stability = weather_market_stability(weather)

    return {
        "sentiment_risk_posture": risk_context.get("recommended_posture"),
        "weather_condition": weather.get("condition"),
        "weather_alert_level": weather.get("alert_level"),
        "weather_trade_permission": weather.get("trade_permission"),
        "weather_bot_decision_authority": (
            weather.get("bot_decision_authority")
        ),
        "weather_emergency_bell": bool(weather.get("emergency_bell")),
        "weather_opportunity_tags": weather_list(weather.get("opportunity_tags")),
        "weather_risk_warnings": weather_list(weather.get("risk_warnings")),
        "weather_position_size_multiplier": (
            bot_tuning.get("position_size_multiplier")
        ),
        "weather_grid_aggression_multiplier": (
            bot_tuning.get("grid_aggression_multiplier")
        ),
        "weather_target_profit_multiplier": (
            bot_tuning.get("target_profit_multiplier")
        ),
        "weather_entry_discount_multiplier": (
            bot_tuning.get("entry_discount_multiplier")
        ),
        "weather_leveling_state": market_stability.get("leveling_state"),
        "weather_leveling_score": optional_float(
            market_stability.get("leveling_score")
        ),
        "sentiment_market_risk_score": risk_context.get("market_risk_score"),
        "sentiment_buy_aggression_score": risk_context.get("buy_aggression_score"),
        "sentiment_downside_risk_score": risk_context.get("downside_risk_score"),
        "sentiment_bottoming_score": risk_context.get("bottoming_score"),
        "sentiment_rebound_score": risk_context.get("rebound_score"),
        "sentiment_breakout_score": risk_context.get("breakout_score"),
        "sentiment_position_size_multiplier": (
            risk_context.get("position_size_multiplier")
        ),
        "sentiment_grid_aggression_multiplier": (
            risk_context.get("grid_aggression_multiplier")
        ),
        "sentiment_target_profit_multiplier": (
            risk_context.get("target_profit_multiplier")
        ),
        "sentiment_entry_discount_multiplier": (
            risk_context.get("entry_discount_multiplier")
        ),
        "sentiment_hard_safety_flags": flags,
    }


def weather_market_location(weather_report):
    location = (
        weather_report.get("market_location")
        if isinstance(weather_report, dict)
        else {}
    )
    return location if isinstance(location, dict) else {}


def weather_status_fields(risk_context):
    fields = sentiment_risk_log_fields(risk_context)
    weather = weather_report_payload(risk_context)
    location = weather_market_location(weather)
    fields.update({
        "weather_report_available": bool(weather),
        "weather_market_current_price": optional_float(
            location.get("current_price")
        ),
        "weather_market_range_high": optional_float(
            location.get("range_high")
        ),
        "weather_market_range_low": optional_float(
            location.get("range_low")
        ),
        "weather_market_range_position": optional_float(
            location.get("range_position")
        ),
        "weather_market_range_zone": location.get("range_zone"),
        "weather_market_distance_to_recent_high_pct": optional_float(
            location.get("distance_to_recent_high_pct")
        ),
        "weather_market_distance_from_recent_low_pct": optional_float(
            location.get("distance_from_recent_low_pct")
        ),
        "weather_market_price_return_24h_pct": optional_float(
            location.get("price_return_24h_pct")
        ),
        "weather_market_price_return_4h_pct": optional_float(
            location.get("price_return_4h_pct")
        ),
    })
    return fields


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
    score = optional_float(stability.get("leveling_score"))
    return state, score


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
    return clamp(
        strategy_float(
            config,
            "weather_leveling_high_band_size_multiplier",
            1.0,
        ),
        0.0,
        1.0,
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
        allow_high_band_breakout_above_last_sell
    ):
        return False
    if weather_leveling_blocks_high_band_bypass(config, weather_report):
        return False
    return weather_high_anchor_tailwind(weather_report)


# ----------------------
# RANGE REFRESH
# ----------------------

def refresh_range():
    try:
        r = requests.get(PRICE_LOG_URL, timeout=10)

        records = []
        for line in r.text.splitlines():
            if not line.strip():
                continue

            try:
                records.append(json.loads(line))
            except Exception:
                continue

        cutoff = datetime.now(timezone.utc) - timedelta(hours=range_window_hours)
        prices = []

        for record in records:
            try:
                ts = datetime.fromisoformat(record["timestamp"])

                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                if ts >= cutoff:
                    prices.append(float(record["btc_price_usd"]))
            except Exception:
                continue

        if prices:
            state["range_low"] = min(prices)
            state["range_high"] = max(prices)
            state["range_mean"] = sum(prices) / len(prices)
            state["range_median"] = statistics.median(prices)
            state["last_range_refresh"] = datetime.now(timezone.utc).isoformat()

            save_state(state)

            log_and_console(
                "RANGE_REFRESH",
                message=(
                    f"low={state['range_low']} "
                    f"high={state['range_high']} "
                    f"mean={round(state['range_mean'], 2)} "
                    f"median={round(state['range_median'], 2)}"
                ),
                range_low=state["range_low"],
                range_high=state["range_high"],
                range_mean=state["range_mean"],
                range_median=state["range_median"],
                samples=len(prices)
            )
        else:
            log_event(
                "RANGE_REFRESH_SKIPPED",
                message="No prices available in configured range window"
            )
    except Exception as e:
        log_event("RANGE_REFRESH_ERROR", message=str(e))


# ----------------------
# GRID
# ----------------------

def compute_grid(anchor, step_pct, grid_size):
    return sorted(
        [
            anchor * (1 - (step_pct * (i + 1)))
            for i in range(grid_size)
        ],
        reverse=True
    )


def compute_high_anchor_grid(
    high,
    price,
    step_pct,
    breakout_extension_pct=0.0,
    allow_breakout_extension=False,
):
    lower_bound = high * (1 - step_pct)

    if lower_bound <= price <= high:
        return [price]
    if (
        allow_breakout_extension
        and breakout_extension_pct > 0
        and high < price <= high * (1 + breakout_extension_pct)
    ):
        return [price]

    return []


def high_anchor_backlog_exposure(now, soft_release_minutes=None, old_order_weight=None):
    soft_release_minutes = (
        high_anchor_backlog_soft_release_minutes
        if soft_release_minutes is None
        else soft_release_minutes
    )
    old_order_weight = (
        high_anchor_backlog_old_order_weight
        if old_order_weight is None
        else old_order_weight
    )
    soft_release_minutes = max(0.0, float(soft_release_minutes or 0.0))
    old_order_weight = max(0.0, min(float(old_order_weight or 0.0), 1.0))

    open_buy_count = sum(
        1
        for order in state["open_buy_orders"].values()
        if order.get("buy_source") == "range_high_band"
    )
    fresh_sell_count = 0
    aged_sell_count = 0

    for order in state["open_sell_orders"].values():
        if order.get("buy_source") != "range_high_band":
            continue
        placed_at = parse_iso8601(order.get("placed_at"))
        age_minutes = (
            ((now - placed_at).total_seconds() / 60)
            if placed_at is not None
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


def high_anchor_open_order_count():
    return high_anchor_backlog_exposure(datetime.now(timezone.utc))["raw_count"]


def high_anchor_cooldown_remaining_minutes(now):
    last_buy_at = parse_iso8601(state.get("last_high_anchor_buy_at"))

    if last_buy_at is None:
        return 0

    elapsed_minutes = (now - last_buy_at).total_seconds() / 60
    return max(0, high_anchor_buy_cooldown_minutes - elapsed_minutes)


def llm_sell_cooldown_remaining_minutes(now):
    last_sell_at = parse_iso8601(state.get("last_llm_sell_at"))

    if last_sell_at is None:
        return 0

    elapsed_minutes = (now - last_sell_at).total_seconds() / 60
    return max(0, llm_buy_cooldown_minutes_after_sell - elapsed_minutes)


# ----------------------
# ORDER HELPERS
# ----------------------

def place_buy(price, volume):
    return safe_kraken_private("BUY", "/0/private/AddOrder", {
        "pair": "XXBTZUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS)),
        "userref": str(BOT_ORDER_USERREF),
    })


def place_sell(price, volume):
    return safe_kraken_private("SELL", "/0/private/AddOrder", {
        "pair": "XXBTZUSD",
        "type": "sell",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS)),
        "userref": str(BOT_ORDER_USERREF),
    })


def cancel_order(txid):
    return safe_kraken_private("CANCEL_ORDER", "/0/private/CancelOrder", {"txid": txid})


def order_filled(txid):
    try:
        r = safe_kraken_private("ORDER_FILLED", "/0/private/QueryOrders", {"txid": txid})
        if not r:
            return False

        return r["result"][txid]["status"] == "closed"
    except Exception:
        return False


def get_order_status(txid):
    try:
        r = safe_kraken_private(
            "ORDER_STATUS",
            "/0/private/QueryOrders",
            {"txid": txid}
        )

        if not r or "result" not in r or txid not in r["result"]:
            log_event("ORDER_STATUS_ERROR", txid=txid, message="missing_result")
            return None

        return r["result"][txid]["status"]
    except Exception as e:
        log_event("ORDER_STATUS_ERROR", txid=txid, message=str(e))
        return None


def get_order_statuses(txids):
    unique_txids = [txid for txid in dict.fromkeys(txids) if txid]
    if not unique_txids:
        return {}

    response = safe_kraken_private(
        "ORDER_STATUS_BATCH",
        "/0/private/QueryOrders",
        {"txid": ",".join(unique_txids)}
    )
    if not response or "result" not in response:
        return {}

    status_map = {}
    for txid in unique_txids:
        order = response["result"].get(txid)
        if not isinstance(order, dict):
            log_event("ORDER_STATUS_ERROR", txid=txid, message="missing_result")
            continue
        status_map[txid] = order.get("status")

    return status_map


def compute_sell_target_price(buy_price, profit_target_override=None):
    profit_target = (
        profit_target_pct
        if profit_target_override is None
        else profit_target_override
    )
    return buy_price * (1 + profit_target + round_trip_fee_pct)


def sell_backoff_remaining_seconds():
    backoff_until = float(
        state.get("sell_insufficient_funds_backoff_until", 0) or 0
    )
    return max(0.0, backoff_until - time.time())


def find_existing_sell_for_buy(level, order):
    buy_trade_id = order.get("trade_id") or order.get("txid")
    buy_source = order.get("buy_source")
    buy_volume = positive_float(order.get("volume"))
    buy_price = positive_float(order.get("price", level))
    level_str = str(level)

    for sell_txid, sell_order in state["open_sell_orders"].items():
        sell_trade_id = sell_order.get("trade_id")
        if buy_trade_id and sell_trade_id == buy_trade_id:
            return sell_txid, sell_order, "trade_id"

        sell_level = str(sell_order.get("level"))
        sell_source = sell_order.get("buy_source")
        sell_volume = positive_float(sell_order.get("volume"))
        sell_buy_price = positive_float(sell_order.get("buy_price"))

        if (
            sell_level == level_str
            and sell_source == buy_source
            and buy_volume is not None
            and sell_volume is not None
            and round(sell_volume, VOLUME_DECIMALS)
            == round(buy_volume, VOLUME_DECIMALS)
            and buy_price is not None
            and sell_buy_price is not None
            and round(sell_buy_price, PRICE_DECIMALS)
            == round(buy_price, PRICE_DECIMALS)
        ):
            return sell_txid, sell_order, "level_volume_price"

    return None, None, None


def normalize_llm_sell_pct(raw_sell_pct):
    if raw_sell_pct is None:
        return None

    try:
        return float(raw_sell_pct) / 100.0
    except Exception:
        return None


def select_llm_target(target_prices, current_price):
    valid_targets = []

    for target in target_prices:
        if not isinstance(target, dict):
            continue

        buy_price = target.get("buy_price")
        sell_pct = normalize_llm_sell_pct(target.get("sell_pct"))

        if buy_price is None or sell_pct is None:
            continue

        try:
            buy_price = float(buy_price)
        except Exception:
            continue

        distance_pct = abs(current_price - buy_price) / buy_price
        if distance_pct > llm_target_proximity_pct:
            continue

        valid_targets.append(
            {
                "buy_price": buy_price,
                "sell_pct": sell_pct,
                "distance_pct": distance_pct
            }
        )

    if not valid_targets:
        return None

    return min(valid_targets, key=lambda target: target["distance_pct"])


def numeric_or_default(value, default):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def compute_adjusted_profit_target(age_minutes, base_profit_target=None):
    return effective_sell_profit_target(
        age_minutes=age_minutes,
        base_profit_target=base_profit_target,
    )


def sentiment_regime(
    execution_signal,
    *,
    action_recommendation="neutral",
    action_policy=None,
    operating_mode=None,
    sentiment_control_mode=None
):
    normalized_recommendation = (
        str(action_recommendation or "neutral").strip().lower()
    )
    normalized_control_mode = normalize_sentiment_control_mode(
        sentiment_control_mode,
        operating_mode
    )
    risk_modulated_core_override = (
        execution_signal < execution_signal_threshold
        and normalized_control_mode == "risk_modulated"
        and operating_mode in ("range_plus_llm", "range_only")
        and normalized_recommendation in ("blocked", "contrarian_watch")
        and not is_risk_off_block(action_recommendation, action_policy)
    )

    if execution_signal < execution_signal_threshold:
        if risk_modulated_core_override:
            return {
                "name": "risk_modulated_defensive",
                "position_size_multiplier": sentiment_defensive_size_multiplier,
                "inventory_multiplier": sentiment_defensive_inventory_multiplier,
                "open_sell_multiplier": sentiment_defensive_open_sell_multiplier,
                "allow_high_anchor": False,
                "profit_target_offset_pct": (
                    sentiment_defensive_profit_target_offset_pct
                ),
                "extra_aging_reduction_pct": (
                    sentiment_defensive_extra_aging_reduction_pct
                )
            }
        return {
            "name": "paused",
            "position_size_multiplier": sentiment_paused_size_multiplier,
            "inventory_multiplier": sentiment_paused_inventory_multiplier,
            "open_sell_multiplier": sentiment_paused_open_sell_multiplier,
            "allow_high_anchor": False,
            "profit_target_offset_pct": (
                sentiment_paused_profit_target_offset_pct
            ),
            "extra_aging_reduction_pct": sentiment_defensive_extra_aging_reduction_pct
        }

    if execution_signal < sentiment_defensive_threshold:
        return {
            "name": "defensive",
            "position_size_multiplier": sentiment_defensive_size_multiplier,
            "inventory_multiplier": sentiment_defensive_inventory_multiplier,
            "open_sell_multiplier": sentiment_defensive_open_sell_multiplier,
            "allow_high_anchor": (
                execution_signal >= sentiment_disable_high_anchor_below
            ),
            "profit_target_offset_pct": (
                sentiment_defensive_profit_target_offset_pct
            ),
            "extra_aging_reduction_pct": sentiment_defensive_extra_aging_reduction_pct
        }

    if execution_signal >= sentiment_risk_on_threshold:
        return {
            "name": "risk_on",
            "position_size_multiplier": sentiment_risk_on_size_multiplier,
            "inventory_multiplier": sentiment_risk_on_inventory_multiplier,
            "open_sell_multiplier": sentiment_risk_on_open_sell_multiplier,
            "allow_high_anchor": True,
            "profit_target_offset_pct": (
                sentiment_risk_on_profit_target_offset_pct
            ),
            "extra_aging_reduction_pct": 0.0
        }

    return {
        "name": "neutral",
        "position_size_multiplier": 1.0,
        "inventory_multiplier": 1.0,
        "open_sell_multiplier": 1.0,
        "allow_high_anchor": True,
        "profit_target_offset_pct": 0.0,
        "extra_aging_reduction_pct": 0.0
    }


def min_signal_for_buy_source(buy_source):
    if buy_source == "llm_target":
        return llm_target_min_signal
    if buy_source == "range_mean":
        return mean_min_signal
    if buy_source == "range_median":
        return median_min_signal
    if buy_source == "range_high_band":
        return high_min_signal
    return low_min_signal


def range_momentum_entry_tolerance_pct(config, buy_source, fallback_config=None):
    if buy_source not in ("range_low", "range_mean", "range_median"):
        return 0.0

    source_values = normalized_source_config_map(
        config,
        "momentum_entry_tolerance_pct_by_source"
    )
    source_value = source_values.get(buy_source)
    if source_value is not None:
        return max(0.0, float(source_value))

    if "momentum_entry_tolerance_pct" in config:
        return max(
            0.0,
            strategy_float(config, "momentum_entry_tolerance_pct", 0.0)
        )

    if fallback_config:
        fallback_source_values = normalized_source_config_map(
            fallback_config,
            "momentum_entry_tolerance_pct_by_source"
        )
        fallback_source_value = fallback_source_values.get(buy_source)
        if fallback_source_value is not None:
            return max(0.0, float(fallback_source_value))

        return max(
            0.0,
            strategy_float(
                fallback_config,
                "momentum_entry_tolerance_pct",
                0.0
            )
        )

    return 0.0


def price_is_above_allowed_entry(
    price,
    level,
    config,
    buy_source,
    fallback_config=None
):
    if buy_source == "llm_target":
        return False
    tolerance_pct = range_momentum_entry_tolerance_pct(
        config,
        buy_source,
        fallback_config
    )
    return price > (level * (1 + tolerance_pct))


def strategy_bool_with_fallback(config, fallback_config, key, default=False):
    if key in config:
        return strategy_bool(config, key, default)
    if isinstance(fallback_config, dict) and key in fallback_config:
        return strategy_bool(fallback_config, key, default)
    return bool(default)


def strategy_float_with_fallback(config, fallback_config, key, default):
    if key in config:
        return strategy_float(config, key, default)
    if isinstance(fallback_config, dict) and key in fallback_config:
        return strategy_float(fallback_config, key, default)
    return float(default)


def optional_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def risk_context_high_band_guard(
    config,
    risk_context,
    fallback_config=None
):
    if not strategy_bool_with_fallback(
        config,
        fallback_config,
        "risk_context_high_band_guard_enabled",
        False
    ):
        return {"allowed": True, "reason": None}

    if not isinstance(risk_context, dict):
        risk_context = {}

    flags = risk_context.get("hard_safety_flags")
    if not isinstance(flags, list):
        flags = []

    market_risk = optional_float(risk_context.get("market_risk_score"))
    buy_aggression = optional_float(risk_context.get("buy_aggression_score"))
    rebound = optional_float(risk_context.get("rebound_score"))
    breakout = optional_float(risk_context.get("breakout_score"))

    max_market_risk = strategy_float_with_fallback(
        config,
        fallback_config,
        "risk_context_high_band_max_market_risk_score",
        0.40
    )
    min_buy_aggression = strategy_float_with_fallback(
        config,
        fallback_config,
        "risk_context_high_band_min_buy_aggression_score",
        0.55
    )
    min_rebound = strategy_float_with_fallback(
        config,
        fallback_config,
        "risk_context_high_band_min_rebound_score",
        0.45
    )
    min_breakout = strategy_float_with_fallback(
        config,
        fallback_config,
        "risk_context_high_band_min_breakout_score",
        0.45
    )

    details = {
        "risk_context_high_band_max_market_risk_score": max_market_risk,
        "risk_context_high_band_min_buy_aggression_score": min_buy_aggression,
        "risk_context_high_band_min_rebound_score": min_rebound,
        "risk_context_high_band_min_breakout_score": min_breakout,
        "sentiment_market_risk_score": market_risk,
        "sentiment_buy_aggression_score": buy_aggression,
        "sentiment_rebound_score": rebound,
        "sentiment_breakout_score": breakout,
        "sentiment_hard_safety_flags": flags,
    }

    if flags:
        return {
            "allowed": False,
            "reason": "risk_context_high_band_hard_safety_flag",
            **details,
        }
    if market_risk is None:
        return {
            "allowed": False,
            "reason": "risk_context_high_band_missing_market_risk",
            **details,
        }
    if market_risk > max_market_risk:
        return {
            "allowed": False,
            "reason": "risk_context_high_band_market_risk_high",
            **details,
        }
    if (
        (buy_aggression is None or buy_aggression < min_buy_aggression)
        and (rebound is None or rebound < min_rebound)
        and (breakout is None or breakout < min_breakout)
    ):
        return {
            "allowed": False,
            "reason": "risk_context_high_band_confirmation_low",
            **details,
        }

    return {"allowed": True, "reason": None, **details}


def risk_context_position_size_adjustment(risk_context):
    if not risk_context_position_sizing_enabled:
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
    if weather.get("emergency_bell"):
        return {
            "enabled": True,
            "raw_multiplier": 0.0,
            "clamped_multiplier": 0.0,
            "blend": risk_context_position_size_blend,
            "effective_multiplier": 0.0,
        }
    else:
        raw_multiplier = optional_float(
            bot_tuning.get("position_size_multiplier")
        )
        if raw_multiplier is None:
            raw_multiplier = optional_float(
                risk_context.get("position_size_multiplier")
            )
    if raw_multiplier is None:
        return {
            "enabled": True,
            "raw_multiplier": None,
            "clamped_multiplier": 1.0,
            "blend": risk_context_position_size_blend,
            "effective_multiplier": 1.0,
        }

    min_multiplier = min(
        risk_context_position_size_min_multiplier,
        risk_context_position_size_max_multiplier,
    )
    max_multiplier = max(
        risk_context_position_size_min_multiplier,
        risk_context_position_size_max_multiplier,
    )
    clamped_multiplier = clamp(raw_multiplier, min_multiplier, max_multiplier)
    blend = clamp(risk_context_position_size_blend, 0.0, 1.0)
    effective_multiplier = 1.0 + ((clamped_multiplier - 1.0) * blend)
    return {
        "enabled": True,
        "raw_multiplier": raw_multiplier,
        "clamped_multiplier": clamped_multiplier,
        "blend": blend,
        "effective_multiplier": effective_multiplier,
    }


def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def source_status_allows_trading(signal_status, source_status):
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
    weather_report=None
):
    normalized = (action_recommendation or "neutral").strip().lower()
    sentiment_control_mode = normalize_sentiment_control_mode(
        sentiment_control_mode,
        operating_mode
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
        "watch_only"
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
            action_policy
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
        "any_buys_allowed": llm_buys_allowed or range_buys_allowed
    }


def flow_adjustment(flow_pressure, buy_source):
    if flow_pressure is None:
        return {
            "size_multiplier": 1.0,
            "block_buy": False,
            "reason": None
        }

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

    return {
        "size_multiplier": 1.0,
        "block_buy": False,
        "reason": None
    }


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


def buy_source_bucket(buy_source):
    mapping = {
        "llm_target": "llm_target",
        "range_low": "range_low",
        "range_mean": "range_mean",
        "range_median": "range_median",
        "range_high_band": "range_high_band"
    }
    return mapping.get(buy_source or "", "unknown")


def inventory_usd_by_bucket(current_price):
    buckets = {}

    for order in state["open_buy_orders"].values():
        bucket = buy_source_bucket(order.get("buy_source"))
        notional = (order.get("price") or current_price) * (order.get("volume") or 0)
        buckets[bucket] = buckets.get(bucket, 0.0) + notional

    for order in state["open_sell_orders"].values():
        bucket = buy_source_bucket(order.get("buy_source"))
        notional = (order.get("buy_price") or current_price) * (order.get("volume") or 0)
        buckets[bucket] = buckets.get(bucket, 0.0) + notional

    return buckets


def bucket_inventory_cap_usd(bucket_name, effective_max_inventory_usd):
    configured = positive_float(bucket_inventory_caps_config.get(bucket_name))
    if configured is None:
        return effective_max_inventory_usd
    return min(configured, effective_max_inventory_usd)


def reserved_buy_capital_usd():
    return sum(
        (order.get("price") or 0) * (order.get("volume") or 0)
        for order in state["open_buy_orders"].values()
    )


def kraken_btc_balance(balance_result):
    if not isinstance(balance_result, dict):
        return None

    for key in ("XXBT", "XBT", "BTC"):
        value = positive_float(balance_result.get(key))
        if value is not None:
            return value

    return None


def kraken_reserved_sell_volume(open_orders_result):
    if not isinstance(open_orders_result, dict):
        return None

    open_orders = open_orders_result.get("open")
    if not isinstance(open_orders, dict):
        return None

    reserved_volume = 0.0
    for order in open_orders.values():
        if not isinstance(order, dict):
            continue
        if not kraken_order_belongs_to_this_bot(order):
            continue

        descr = order.get("descr", {})
        if not isinstance(descr, dict):
            continue

        if descr.get("type") != "sell":
            continue

        pair = descr.get("pair")
        normalized_pair = (
            str(pair or "")
            .upper()
            .replace("/", "")
            .replace("-", "")
        )
        if not (
            normalized_pair == KRAKEN_PAIR.upper()
            or ("XBT" in normalized_pair and "USD" in normalized_pair)
            or ("XXBT" in normalized_pair and "ZUSD" in normalized_pair)
        ):
            continue

        remaining = positive_float(order.get("vol")) or 0.0
        executed = positive_float(order.get("vol_exec")) or 0.0
        reserved_volume += max(0.0, remaining - executed)

    return reserved_volume


def kraken_pair_matches(pair):
    normalized_pair = (
        str(pair or "")
        .upper()
        .replace("/", "")
        .replace("-", "")
    )
    return (
        normalized_pair == KRAKEN_PAIR.upper()
        or ("XBT" in normalized_pair and "USD" in normalized_pair)
        or ("XXBT" in normalized_pair and "ZUSD" in normalized_pair)
    )


def kraken_order_userref(order):
    if not isinstance(order, dict):
        return None

    candidates = [order.get("userref")]
    descr = order.get("descr")
    if isinstance(descr, dict):
        candidates.append(descr.get("userref"))

    for value in candidates:
        try:
            if value is None or value == "":
                continue
            return int(value)
        except (TypeError, ValueError):
            continue

    return None


def kraken_order_belongs_to_this_bot(order):
    return kraken_order_userref(order) == BOT_ORDER_USERREF


def startup_reconcile_state():
    open_order_txids = set()
    kraken_open_orders = {}
    own_kraken_open_orders = {}
    state_changed = False

    open_orders_resp = safe_kraken_private(
        "OPEN_ORDERS_STARTUP_RECONCILE",
        "/0/private/OpenOrders"
    )
    if open_orders_resp and "result" in open_orders_resp:
        kraken_open_orders = open_orders_resp["result"].get("open", {}) or {}
        own_kraken_open_orders = {
            txid: order
            for txid, order in kraken_open_orders.items()
            if kraken_order_belongs_to_this_bot(order)
        }
        open_order_txids = set(own_kraken_open_orders.keys())
    else:
        log_event(
            "STARTUP_RECONCILE_SKIPPED",
            reason="open_orders_fetch_failed"
        )
        return

    tracked_buy_txids = [
        order.get("txid")
        for order in state["open_buy_orders"].values()
        if order.get("txid")
    ]
    tracked_sell_txids = list(state["open_sell_orders"].keys())
    tracked_txids = tracked_sell_txids + tracked_buy_txids
    status_map = get_order_statuses(tracked_txids)

    for level, order in list(state["open_buy_orders"].items()):
        txid = order.get("txid")
        if not txid:
            state["open_buy_orders"].pop(level, None)
            state_changed = True
            log_event(
                "STARTUP_RECONCILE_DROP_BUY",
                level=level,
                reason="missing_txid",
                buy_source=order.get("buy_source")
            )
            continue

        status = status_map.get(txid)
        if txid in open_order_txids:
            status = "open"

        if status in ("open", "closed", None):
            continue

        if status in ("canceled", "expired"):
            state["open_buy_orders"].pop(level, None)
            state_changed = True
            log_event(
                "STARTUP_RECONCILE_DROP_BUY",
                level=level,
                txid=txid,
                reason=f"status_{status}",
                buy_source=order.get("buy_source")
            )

    for txid, order in list(state["open_sell_orders"].items()):
        status = status_map.get(txid)
        if txid in open_order_txids:
            status = "open"

        if status == "open":
            continue

        if status == "closed":
            log_event(
                "STARTUP_RECONCILE_SELL_PENDING_FILL",
                txid=txid,
                level=order.get("level"),
                buy_source=order.get("buy_source")
            )
            continue

        if status in ("canceled", "expired"):
            state["open_sell_orders"].pop(txid, None)
            state_changed = True
            log_event(
                "STARTUP_RECONCILE_DROP_SELL",
                txid=txid,
                level=order.get("level"),
                reason=f"status_{status}",
                buy_source=order.get("buy_source")
            )

    tracked_known_txids = set(
        tracked_sell_txids
        + [
            txid
            for txid in tracked_buy_txids
            if txid
        ]
    )
    for txid, order in own_kraken_open_orders.items():
        if txid in tracked_known_txids:
            continue
        if not isinstance(order, dict):
            continue

        descr = order.get("descr", {})
        if not isinstance(descr, dict):
            continue

        if not kraken_pair_matches(descr.get("pair")):
            continue

        log_event(
            "STARTUP_RECONCILE_UNTRACKED_KRAKEN_ORDER",
            txid=txid,
            side=descr.get("type"),
            ordertype=descr.get("ordertype"),
            pair=descr.get("pair"),
            price=positive_float(descr.get("price")),
            volume=positive_float(order.get("vol")),
            volume_executed=positive_float(order.get("vol_exec"))
        )

    foreign_open_order_count = max(
        0,
        len(kraken_open_orders) - len(own_kraken_open_orders)
    )
    if foreign_open_order_count:
        log_event(
            "STARTUP_RECONCILE_FOREIGN_KRAKEN_ORDERS",
            foreign_open_order_count=foreign_open_order_count,
            tracked_open_order_count=len(own_kraken_open_orders),
            order_owner_userref=BOT_ORDER_USERREF,
            order_owner_tag_source=order_owner_tag_source
        )

    if state_changed:
        save_state(state)

    log_event(
        "STARTUP_RECONCILE_COMPLETE",
        tracked_open_buy_count=len(state["open_buy_orders"]),
        tracked_open_sell_count=len(state["open_sell_orders"]),
        kraken_open_order_count=len(kraken_open_orders),
        kraken_owned_open_order_count=len(own_kraken_open_orders),
        kraken_foreign_open_order_count=foreign_open_order_count,
        state_changed=state_changed
    )


def reconcile_btc_inventory(cycle_id):
    if not state["open_buy_orders"]:
        return []

    balance_resp = safe_kraken_private("BALANCE_RECONCILE", "/0/private/Balance")
    if not balance_resp or "result" not in balance_resp:
        log_event(
            "BTC_RECONCILE_SKIPPED",
            cycle_id=cycle_id,
            reason="balance_fetch_failed"
        )
        return []

    actual_btc = kraken_btc_balance(balance_resp["result"])
    if actual_btc is None:
        log_event(
            "BTC_RECONCILE_SKIPPED",
            cycle_id=cycle_id,
            reason="btc_balance_missing"
        )
        return []

    open_orders_resp = safe_kraken_private(
        "OPEN_ORDERS_RECONCILE",
        "/0/private/OpenOrders"
    )
    if not open_orders_resp or "result" not in open_orders_resp:
        log_event(
            "BTC_RECONCILE_SKIPPED",
            cycle_id=cycle_id,
            reason="open_orders_fetch_failed"
        )
        return []

    kraken_reserved_sell = kraken_reserved_sell_volume(open_orders_resp["result"])
    if kraken_reserved_sell is None:
        log_event(
            "BTC_RECONCILE_SKIPPED",
            cycle_id=cycle_id,
            reason="reserved_sell_volume_missing"
        )
        return []

    open_sell_volume = sum(
        positive_float(order.get("volume")) or 0.0
        for order in state["open_sell_orders"].values()
    )
    open_buy_volume = sum(
        positive_float(order.get("volume")) or 0.0
        for order in state["open_buy_orders"].values()
    )
    tracked_total_volume = open_sell_volume + open_buy_volume
    available_btc_for_new_sells = max(0.0, actual_btc - kraken_reserved_sell)
    tolerance = max(10 ** (-VOLUME_DECIMALS), 0.00000002)
    excess_volume = open_buy_volume - available_btc_for_new_sells

    log_event(
        "BTC_RECONCILE_CHECK",
        cycle_id=cycle_id,
        actual_btc=round(actual_btc, 8),
        kraken_reserved_sell_volume=round(kraken_reserved_sell, 8),
        available_btc_for_new_sells=round(available_btc_for_new_sells, 8),
        tracked_open_buy_volume=round(open_buy_volume, 8),
        tracked_open_sell_volume=round(open_sell_volume, 8),
        tracked_total_volume=round(tracked_total_volume, 8),
        excess_volume=round(excess_volume, 8)
    )

    if excess_volume <= tolerance:
        return []

    removable_orders = []
    for level, order in state["open_buy_orders"].items():
        order_volume = positive_float(order.get("volume")) or 0.0
        placed_at = parse_iso8601(order.get("placed_at")) or datetime.min.replace(
            tzinfo=timezone.utc
        )
        removable_orders.append((placed_at, level, order, order_volume))

    removable_orders.sort(key=lambda item: item[0])
    reconciled_levels = []
    removed_volume = 0.0

    for _, level, order, order_volume in removable_orders:
        if excess_volume - removed_volume <= tolerance:
            break
        removed_volume += order_volume
        reconciled_levels.append(level)
        log_event(
            "BTC_RECONCILE_DROP_BUY",
            cycle_id=cycle_id,
            level=level,
            txid=order.get("txid"),
            volume=round(order_volume, VOLUME_DECIMALS),
            buy_price=round(
                positive_float(order.get("price", level)) or float(level),
                PRICE_DECIMALS
            ),
            buy_source=order.get("buy_source"),
            reason="tracked_volume_exceeds_actual_btc"
        )

    for level in reconciled_levels:
        state["open_buy_orders"].pop(level, None)

    if reconciled_levels:
        save_state(state)
        emit_alert(
            "btc_reconciled",
            "critical",
            "Tracked buy volume exceeded available BTC and was repaired",
            cycle_id=cycle_id,
            removed_levels=reconciled_levels,
            removed_volume=round(removed_volume, 8),
            actual_btc=round(actual_btc, 8),
            available_btc_for_new_sells=round(available_btc_for_new_sells, 8),
        )
        log_and_console(
            "BTC_RECONCILED",
            message=(
                f"Removed {len(reconciled_levels)} stale filled buys after "
                f"BTC inventory mismatch"
            ),
            cycle_id=cycle_id,
            removed_levels=reconciled_levels,
            removed_volume=round(removed_volume, 8),
            actual_btc=round(actual_btc, 8),
            kraken_reserved_sell_volume=round(kraken_reserved_sell, 8),
            available_btc_for_new_sells=round(available_btc_for_new_sells, 8),
            tracked_total_volume=round(tracked_total_volume, 8)
        )

    return reconciled_levels


def maybe_periodic_state_reconcile(now, cycle_id):
    if reconcile_state_interval_minutes <= 0:
        return False

    last_reconcile_at = parse_iso8601(state.get("last_state_reconcile_at"))
    if last_reconcile_at is not None:
        elapsed_minutes = (now - last_reconcile_at).total_seconds() / 60.0
        if elapsed_minutes < reconcile_state_interval_minutes:
            return False

    startup_reconcile_state()
    state["last_state_reconcile_at"] = cycle_id
    save_state(state)
    return True


# ----------------------
# MAIN LOOP
# ----------------------

def main():
    instance_identity = acquire_instance_lock()
    log_and_console(
        "BOT_START",
        message="Range Grid Average bot starting",
        **instance_identity,
        config_file=CONFIG_FILE,
        operating_mode=operating_mode,
        paper_trading_enabled=paper_trading_enabled,
        risk_context_shadow_buy_enabled=risk_context_shadow_buy_enabled,
        grid_anchor=grid_anchor,
        configured_strategy_modes=configured_strategy_modes,
        strategy_modes=strategy_modes,
        range_window_hours=range_window_hours,
        max_grid_size=max_grid_size,
        profit_target_pct=profit_target_pct,
        entry_step_pct=entry_step_pct,
        round_trip_fee_pct=round_trip_fee_pct,
        position_size_pct=position_size_pct,
        min_buy_notional_usd=min_buy_notional_usd,
        min_buy_volume_btc=min_buy_volume_btc,
        execution_signal_threshold=execution_signal_threshold,
        llm_target_proximity_pct=llm_target_proximity_pct,
        llm_target_min_signal=llm_target_min_signal,
        low_min_signal=low_min_signal,
        mean_min_signal=mean_min_signal,
        median_min_signal=median_min_signal,
        high_min_signal=high_min_signal,
        request_timeout=REQUEST_TIMEOUT,
        kraken_nonce_retries=KRAKEN_NONCE_RETRIES,
        kraken_lockout_cooldown_seconds=KRAKEN_LOCKOUT_COOLDOWN_SECONDS,
        min_signal_status=min_signal_status,
        require_fresh_signal=require_fresh_signal,
        status_file=os.path.abspath(STATUS_FILE),
        alert_log_file=os.path.abspath(ALERT_LOG_FILE),
        activity_log_file=os.path.abspath(ACTIVITY_LOG_FILE),
        activity_log_rotate_daily=ACTIVITY_LOG_ROTATE_DAILY,
        order_tracker_configured=bool(
            order_tracker_url and order_tracker_user_agent
        ),
        order_tracker_url_configured=bool(order_tracker_url),
        order_tracker_user_agent_configured=bool(order_tracker_user_agent),
        order_tracker_symbol=order_tracker_symbol,
        order_tracker_timeout_seconds=order_tracker_timeout_seconds,
        order_tracker_checkin_timeout_seconds=(
            order_tracker_checkin_timeout_seconds
        ),
        max_daily_loss_usd=max_daily_loss_usd,
        disable_new_buys_on_sell_backlog_count=(
            disable_new_buys_on_sell_backlog_count
        ),
        disable_new_buys_on_sell_backlog_minutes=(
            disable_new_buys_on_sell_backlog_minutes
        ),
        reconcile_state_interval_minutes=reconcile_state_interval_minutes,
        max_consecutive_loop_errors=max_consecutive_loop_errors,
        max_consecutive_private_api_failures=(
            max_consecutive_private_api_failures
        ),
        risk_multiplier_floor=risk_multiplier_floor,
        risk_multiplier_ceiling=risk_multiplier_ceiling,
        flow_defensive_threshold=flow_defensive_threshold,
        flow_block_threshold=flow_block_threshold,
        flow_defensive_size_multiplier=flow_defensive_size_multiplier,
        flow_block_high_only=flow_block_high_only,
        flow_block_llm_only_below=flow_block_llm_only_below,
        mean_reversion_min_opportunity=mean_reversion_min_opportunity,
        allow_range_fallback_without_sentiment=(
            allow_range_fallback_without_sentiment
        ),
        range_fallback_execution_signal=range_fallback_execution_signal,
        max_inventory_usd_by_bucket=bucket_inventory_caps_config,
        price_check_interval_seconds=price_check_interval_seconds,
        range_refresh_interval_minutes=range_refresh_interval_minutes,
        max_open_sell_orders=max_open_sell_orders,
        max_inventory_usd=max_inventory_usd,
        aging_start_minutes=aging_start_minutes,
        aging_step_minutes=aging_step_minutes,
        aging_profit_reduction_pct=aging_profit_reduction_pct,
        min_profit_target_pct=min_profit_target_pct,
        high_anchor_buy_cooldown_minutes=high_anchor_buy_cooldown_minutes,
        max_open_high_anchor_orders=max_open_high_anchor_orders,
        high_anchor_backlog_soft_release_minutes=(
            high_anchor_backlog_soft_release_minutes
        ),
        high_anchor_backlog_old_order_weight=(
            high_anchor_backlog_old_order_weight
        ),
        high_anchor_profit_target_pct=high_anchor_profit_target_pct,
        prevent_buy_above_last_sell=prevent_buy_above_last_sell,
        buy_after_sell_discount_pct=buy_after_sell_discount_pct,
        allow_high_band_breakout_above_last_sell=(
            allow_high_band_breakout_above_last_sell
        ),
        llm_buy_cooldown_minutes_after_sell=(
            llm_buy_cooldown_minutes_after_sell
        ),
        sentiment_defensive_threshold=sentiment_defensive_threshold,
        sentiment_risk_on_threshold=sentiment_risk_on_threshold,
        sentiment_paused_size_multiplier=sentiment_paused_size_multiplier,
        sentiment_paused_inventory_multiplier=(
            sentiment_paused_inventory_multiplier
        ),
        sentiment_paused_open_sell_multiplier=(
            sentiment_paused_open_sell_multiplier
        ),
        sentiment_defensive_size_multiplier=(
            sentiment_defensive_size_multiplier
        ),
        sentiment_risk_on_size_multiplier=sentiment_risk_on_size_multiplier,
        sentiment_defensive_inventory_multiplier=(
            sentiment_defensive_inventory_multiplier
        ),
        sentiment_risk_on_inventory_multiplier=(
            sentiment_risk_on_inventory_multiplier
        ),
        sentiment_defensive_open_sell_multiplier=(
            sentiment_defensive_open_sell_multiplier
        ),
        sentiment_risk_on_open_sell_multiplier=(
            sentiment_risk_on_open_sell_multiplier
        ),
        sentiment_disable_high_anchor_below=(
            sentiment_disable_high_anchor_below
        ),
        sentiment_defensive_extra_aging_reduction_pct=(
            sentiment_defensive_extra_aging_reduction_pct
        ),
        risk_context_high_band_guard_enabled=(
            risk_context_high_band_guard_enabled
        ),
        risk_context_high_band_min_buy_aggression_score=(
            risk_context_high_band_min_buy_aggression_score
        ),
        risk_context_high_band_min_breakout_score=(
            risk_context_high_band_min_breakout_score
        ),
        risk_context_high_band_min_rebound_score=(
            risk_context_high_band_min_rebound_score
        ),
        risk_context_high_band_max_market_risk_score=(
            risk_context_high_band_max_market_risk_score
        ),
        risk_context_position_sizing_enabled=(
            risk_context_position_sizing_enabled
        ),
        risk_context_position_size_min_multiplier=(
            risk_context_position_size_min_multiplier
        ),
        risk_context_position_size_max_multiplier=(
            risk_context_position_size_max_multiplier
        ),
        risk_context_position_size_blend=(
            risk_context_position_size_blend
        ),
        anchor_strategy_router_enabled=anchor_strategy_router_enabled,
        anchor_strategy_router_file=anchor_strategy_router_file,
        anchor_strategy_router_refresh_seconds=(
            anchor_strategy_router_refresh_seconds
        ),
        anchor_strategy_router_timeout_seconds=(
            anchor_strategy_router_timeout_seconds
        ),
        anchor_strategy_router_fail_closed=anchor_strategy_router_fail_closed,
    )
    if anchor_strategy_router_enabled:
        routes = load_anchor_strategy_routes(force=True)
        log_event(
            "ANCHOR_STRATEGY_ROUTER_STARTUP_CHECK",
            router_file=anchor_strategy_router_cache.get("path"),
            route_count=len(routes),
            anchors=sorted(routes),
            error=anchor_strategy_router_cache.get("error"),
            fail_closed=anchor_strategy_router_fail_closed,
        )
    startup_reconcile_state()

    loop_count = 0
    while True:
        try:
            loop_count += 1
            now = datetime.now(timezone.utc)
            cycle_id = now.isoformat()
            actions = []
            deduped_candidates = []
            active_strategy_modes = list(strategy_modes)

            # Periodically resync tracked state against Kraken as source of truth.
            if maybe_periodic_state_reconcile(now, cycle_id):
                actions.append("state_reconciled")

            price = get_price()
            sentiment_payload = get_sentiment()

            if price is None:
                state["consecutive_loop_errors"] = 0
                save_state(state)
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    execution_signal=None,
                    reason="missing_price",
                    cycle_id=cycle_id
                )
                send_checkin(loop_count=loop_count, message="loop_complete")
                time.sleep(price_check_interval_seconds)
                continue

            if sentiment_payload is None:
                range_modes_enabled = any(
                    mode != "llm_target" for mode in active_strategy_modes
                )
                if (
                    allow_range_fallback_without_sentiment
                    and range_modes_enabled
                ):
                    fallback_reason = "sentiment_unavailable_range_fallback"
                    sentiment_payload = synthetic_range_sentiment_payload(
                        fallback_reason,
                        price
                    )
                    actions.append("sentiment_fallback")
                    log_event(
                        "SENTIMENT_FALLBACK_ACTIVE",
                        cycle_id=cycle_id,
                        reason=fallback_reason,
                        fallback_execution_signal=range_fallback_execution_signal,
                        strategy_modes=active_strategy_modes,
                        llm_buys_disabled=True
                    )
                else:
                    state["consecutive_loop_errors"] = 0
                    save_state(state)
                    log_event(
                        "TRADE_DECISION",
                        side="hold",
                        price=price,
                        execution_signal=None,
                        reason="missing_signal",
                        cycle_id=cycle_id
                    )
                    send_checkin(loop_count=loop_count, message="loop_complete")
                    time.sleep(price_check_interval_seconds)
                    continue

            execution_signal = sentiment_payload["execution_signal"]
            target_prices = sentiment_payload.get("target_prices", [])
            llm_target = select_llm_target(target_prices, price)
            price_regime = sentiment_payload.get("price_regime", {})
            trend_snapshot = sentiment_payload.get("trend_snapshot", {})
            kraken_flow = sentiment_payload.get("kraken_flow", {})
            price_regime_range_position = numeric_or_default(
                price_regime.get("range_position_24h"),
                None
            )
            active_strategy_modes = select_dynamic_strategy_modes(
                configured_strategy_modes,
                operating_mode,
                price_regime_range_position,
                strategy_config
            )
            flow_pressure = numeric_or_default(
                sentiment_payload.get("flow_pressure"),
                None
            )
            mean_reversion_opportunity = numeric_or_default(
                sentiment_payload.get("mean_reversion_opportunity"),
                0.0
            )
            smoothed_risk_multiplier = clamp(
                numeric_or_default(
                    sentiment_payload.get("smoothed_risk_multiplier"),
                    1.0
                ),
                risk_multiplier_floor,
                risk_multiplier_ceiling
            )
            signal_status = sentiment_payload.get("signal_status")
            source_status = sentiment_payload.get("source_status", {})
            freshness_allows_trading, freshness_block_reason = (
                source_status_allows_trading(signal_status, source_status)
            )
            source_guard_allows_trading = sentiment_payload.get(
                "bot_action_allowed"
            )
            if source_guard_allows_trading is None:
                source_guard_allows_trading = True
            action_recommendation = (
                sentiment_payload.get("action_recommendation") or "neutral"
            )
            action_policy = sentiment_payload.get("action_policy", {})
            risk_context = sentiment_payload.get("risk_context")
            weather_report = weather_report_payload(risk_context)
            sentiment_risk_fields = sentiment_risk_log_fields(
                risk_context
            )
            allow_range_buy_on_confidence_block = profile_bool(
                "allow_range_buy_on_confidence_block",
                False
            )
            sentiment_control_mode = normalize_sentiment_control_mode(
                profile_str("sentiment_control_mode", None),
                operating_mode
            )
            buy_permissions = sentiment_buy_permissions(
                action_recommendation,
                action_policy,
                operating_mode=operating_mode,
                allow_range_buy_on_confidence_block=(
                    allow_range_buy_on_confidence_block
                ),
                sentiment_control_mode=sentiment_control_mode,
                weather_report=weather_report
            )
            llm_buys_allowed = buy_permissions["llm_buys_allowed"]
            range_core_buys_allowed = buy_permissions[
                "range_core_buys_allowed"
            ]
            range_high_buys_allowed = buy_permissions[
                "range_high_buys_allowed"
            ]
            range_buys_allowed = buy_permissions["range_buys_allowed"]
            base_any_buys_allowed = buy_permissions["any_buys_allowed"]
            range_modes_enabled = any(
                mode != "llm_target" for mode in active_strategy_modes
            )
            daily_pnl_start = now.astimezone(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            realized_pnl_today = realized_pnl_for_utc_day(daily_pnl_start)
            sell_backlog = summarize_sell_backlog(state["open_sell_orders"], now)
            runtime_block_reason = runtime_buy_block_reason(
                operating_mode=operating_mode,
                realized_pnl_today=realized_pnl_today,
                max_daily_loss_usd=max_daily_loss_usd,
                sell_backlog_count=sell_backlog["count"],
                sell_backlog_limit=disable_new_buys_on_sell_backlog_count,
                sell_backlog_oldest_minutes=sell_backlog["oldest_age_minutes"],
                sell_backlog_minutes_limit=disable_new_buys_on_sell_backlog_minutes,
                consecutive_loop_errors=int(state.get("consecutive_loop_errors", 0) or 0),
                max_consecutive_loop_errors=max_consecutive_loop_errors,
                consecutive_private_api_failures=int(
                    state.get("consecutive_private_api_failures", 0) or 0
                ),
                max_consecutive_private_api_failures=(
                    max_consecutive_private_api_failures
                ),
            )
            if runtime_block_reason:
                emit_alert(
                    "buy_guardrail_blocked",
                    "warning",
                    "New buys blocked by runtime guardrail",
                    cycle_id=cycle_id,
                    operating_mode=operating_mode,
                    reason=runtime_block_reason,
                    sell_backlog_count=sell_backlog["count"],
                    sell_backlog_oldest_minutes=round(
                        sell_backlog["oldest_age_minutes"],
                        2
                    ),
                    realized_pnl_today=round(realized_pnl_today, 8),
                )
            llm_signal_gates_allow = (
                freshness_allows_trading and source_guard_allows_trading
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
            if runtime_block_reason:
                llm_buys_allowed = False
                range_buys_allowed = False
                base_any_buys_allowed = False
                any_buys_allowed = False
            external_block_reason = sentiment_payload.get("action_reason")
            price_regime_volatility_pct = numeric_or_default(
                price_regime.get("realized_volatility_24h_pct"),
                None
            )
            current_entry_step_pct = effective_entry_step_pct(
                entry_step_pct,
                price_regime_volatility_pct,
                strategy_config
            )
            regime = sentiment_regime(
                execution_signal,
                action_recommendation=action_recommendation,
                action_policy=action_policy,
                operating_mode=operating_mode,
                sentiment_control_mode=sentiment_control_mode
            )
            effective_position_size_pct = (
                position_size_pct * regime["position_size_multiplier"]
            )
            effective_max_inventory_usd = (
                max_inventory_usd * regime["inventory_multiplier"]
            )
            effective_max_open_sell_orders = max(
                1,
                int(round(
                    max_open_sell_orders * regime["open_sell_multiplier"]
                ))
            )
            effective_position_size_pct *= smoothed_risk_multiplier
            effective_max_inventory_usd *= smoothed_risk_multiplier
            effective_max_open_sell_orders = max(
                1,
                int(round(
                    effective_max_open_sell_orders * smoothed_risk_multiplier
                ))
            )
            risk_context_size_adjustment = risk_context_position_size_adjustment(
                risk_context
            )
            risk_context_size_multiplier = risk_context_size_adjustment[
                "effective_multiplier"
            ]
            effective_position_size_pct *= risk_context_size_multiplier
            deployed_inventory_usd = current_inventory_usd(price)
            inventory_pressure = inventory_pressure_adjustment(
                deployed_inventory_usd,
                effective_max_inventory_usd,
                strategy_config
            )
            inventory_pressure_usage_ratio = inventory_pressure["usage_ratio"]
            inventory_pressure_size_multiplier = inventory_pressure[
                "size_multiplier"
            ]
            effective_position_size_pct *= inventory_pressure_size_multiplier
            weather_high_anchor_allowed = weather_high_anchor_tailwind(
                weather_report
            )
            effective_high_anchor_enabled = (
                regime["allow_high_anchor"] or weather_high_anchor_allowed
            )

            log_event(
                "SIGNAL_UPDATE",
                execution_signal=execution_signal,
                price=price,
                operating_mode=operating_mode,
                configured_strategy_modes=configured_strategy_modes,
                strategy_modes=active_strategy_modes,
                dynamic_anchor_mode=strategy_bool(
                    strategy_config,
                    "dynamic_anchor_mode",
                    False
                ),
                btc_sentiment=sentiment_payload.get("btc_sentiment"),
                confidence=sentiment_payload.get("confidence"),
                raw_btc_sentiment=sentiment_payload.get("raw_btc_sentiment"),
                raw_confidence=sentiment_payload.get("raw_confidence"),
                direction_bias=sentiment_payload.get("direction_bias"),
                raw_direction_bias=sentiment_payload.get("raw_direction_bias"),
                fear_greed_index=sentiment_payload.get("fear_greed_index"),
                schema_version=sentiment_payload.get("schema_version"),
                sentiment_source=sentiment_payload.get("source"),
                sentiment_processed_at=sentiment_payload.get("processed_at"),
                contributor_count=sentiment_payload.get("contributor_count"),
                active_observation_count=sentiment_payload.get(
                    "active_observation_count"
                ),
                llm_target_count=len(target_prices),
                active_llm_target=(
                    None if llm_target is None
                    else round(llm_target["buy_price"], PRICE_DECIMALS)
                ),
                action_recommendation=action_recommendation,
                action_policy_reason=action_policy.get("reason"),
                **sentiment_risk_fields,
                signal_status=signal_status,
                signal_allows_trading=any_buys_allowed,
                llm_buys_allowed=llm_buys_allowed,
                range_buys_allowed=range_buys_allowed,
                range_fallback_active=range_fallback_active,
                source_guard_allows_trading=source_guard_allows_trading,
                freshness_allows_trading=freshness_allows_trading,
                freshness_block_reason=freshness_block_reason,
                smoothed_risk_multiplier=smoothed_risk_multiplier,
                risk_context_position_sizing_enabled=(
                    risk_context_size_adjustment["enabled"]
                ),
                risk_context_position_size_raw_multiplier=(
                    risk_context_size_adjustment["raw_multiplier"]
                ),
                risk_context_position_size_clamped_multiplier=(
                    risk_context_size_adjustment["clamped_multiplier"]
                ),
                risk_context_position_size_blend=(
                    risk_context_size_adjustment["blend"]
                ),
                risk_context_position_size_effective_multiplier=(
                    risk_context_size_multiplier
                ),
                flow_pressure=flow_pressure,
                mean_reversion_opportunity=mean_reversion_opportunity,
                inventory_pressure_usage_ratio=inventory_pressure_usage_ratio,
                inventory_pressure_size_multiplier=(
                    inventory_pressure_size_multiplier
                ),
                price_regime_range_position=price_regime_range_position,
                price_regime_volatility_pct=price_regime.get(
                    "realized_volatility_24h_pct"
                ),
                trend_range_position_change=(
                    trend_snapshot.get("price_regime_trends", {})
                    .get("range_position_24h_change")
                ),
                sentiment_regime=regime["name"],
                effective_position_size_pct=effective_position_size_pct,
                effective_max_inventory_usd=effective_max_inventory_usd,
                effective_max_open_sell_orders=effective_max_open_sell_orders,
                high_anchor_enabled=effective_high_anchor_enabled,
                weather_high_anchor_allowed=weather_high_anchor_allowed
            )
            console(f"Price: {price} | Signal: {execution_signal}")

            if (
                state["last_range_refresh"] is None
                or (
                    now - datetime.fromisoformat(state["last_range_refresh"])
                ).total_seconds() > (range_refresh_interval_minutes * 60)
            ):
                refresh_range()

            low = state["range_low"]
            high = state["range_high"]
            mean = state["range_mean"]
            median = state["range_median"]

            low = numeric_or_default(
                price_regime.get("price_low_24h"),
                low
            )
            high = numeric_or_default(
                price_regime.get("price_high_24h"),
                high
            )
            mean = numeric_or_default(
                price_regime.get("price_mean_24h"),
                mean
            )
            median = numeric_or_default(
                price_regime.get("price_median_24h"),
                median
            )

            reconciled_levels = reconcile_btc_inventory(cycle_id)
            if reconciled_levels:
                actions.append("btc_reconciled")

            order_status_map = get_order_statuses(
                list(state["open_sell_orders"].keys())
                + [
                    order.get("txid")
                    for order in state["open_buy_orders"].values()
                ]
            )

            # SELL EXIT CHECK
            for txid, order in list(state["open_sell_orders"].items()):
                status = order_status_map.get(txid)

                if status is None:
                    continue

                if status == "closed":
                    if fill_already_processed("sell", txid):
                        del state["open_sell_orders"][txid]
                        save_state(state)
                        actions.append("sell_fill_already_processed")
                        log_event(
                            "SELL_FILL_ALREADY_PROCESSED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_source=order.get("buy_source")
                        )
                        continue

                    buy_price = order.get("buy_price")
                    sell_price = order.get("sell_price")
                    volume = order.get("volume", 0)
                    gross_pnl = None
                    estimated_net_pnl = None
                    hold_minutes = None

                    if (
                        buy_price is not None
                        and sell_price is not None
                        and volume is not None
                    ):
                        gross_pnl = volume * (sell_price - buy_price)
                        estimated_net_pnl = gross_pnl - (
                            volume * buy_price * round_trip_fee_pct
                        )

                    placed_at = parse_iso8601(order.get("placed_at"))
                    if placed_at is not None:
                        hold_minutes = (
                            now - placed_at
                        ).total_seconds() / 60

                    sell_level = order.get("level")
                    del state["open_sell_orders"][txid]
                    state["last_sell_price"] = sell_price
                    state["last_sell_at"] = cycle_id
                    if order.get("buy_source") == "llm_target":
                        state["last_llm_sell_at"] = cycle_id
                    state["stats"]["sell_orders_filled"] += 1
                    increment_source_stat(
                        state["stats"],
                        "sell_orders_filled_by_source",
                        order.get("buy_source")
                    )
                    if gross_pnl is not None:
                        state["stats"]["realized_gross_pnl"] += gross_pnl
                    if estimated_net_pnl is not None:
                        state["stats"]["realized_estimated_net_pnl"] += (
                            estimated_net_pnl
                        )
                    mark_fill_processed("sell", txid, cycle_id)
                    save_state(state)
                    actions.append("sell_filled")

                    log_and_console(
                        "SELL_ORDER_FILLED",
                        message=f"SELL filled for level {sell_level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=sell_level,
                        volume=volume,
                        buy_price=buy_price,
                        sell_price=sell_price,
                        buy_source=order.get("buy_source"),
                        gross_pnl=gross_pnl,
                        estimated_net_pnl=estimated_net_pnl,
                        hold_minutes=hold_minutes
                    )
                    log_trade_activity(
                        "SELL_ORDER_FILLED",
                        mode="live",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=sell_level,
                        volume=volume,
                        buy_price=buy_price,
                        sell_price=sell_price,
                        buy_source=order.get("buy_source"),
                        gross_pnl=gross_pnl,
                        estimated_net_pnl=estimated_net_pnl,
                        hold_minutes=hold_minutes
                    )
                    continue

                if status == "open":
                    if not operating_mode_allows_sell_execution(operating_mode):
                        actions.append("sell_management_disabled")
                        log_event(
                            "SELL_REPRICE_SKIPPED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_price=order.get("buy_price"),
                            current_sell_price=order.get("sell_price"),
                            operating_mode=operating_mode,
                            reason="operating_mode_blocks_sell_management"
                        )
                        continue

                    buy_price = order.get("buy_price")
                    current_sell_price = order.get("sell_price")
                    sell_pct_override = order.get("sell_pct_override")
                    placed_at = parse_iso8601(order.get("placed_at"))
                    age_minutes = None
                    if placed_at is not None:
                        age_minutes = (
                            now - placed_at
                        ).total_seconds() / 60

                    adjusted_profit_target = effective_sell_profit_target(
                        age_minutes=age_minutes,
                        base_profit_target=sell_pct_override,
                        buy_source=order.get("buy_source"),
                        regime=regime,
                    )

                    if buy_price is None or current_sell_price is None:
                        continue

                    adjusted_sell_price = compute_sell_target_price(
                        buy_price,
                        adjusted_profit_target
                    )

                    if round(adjusted_sell_price, PRICE_DECIMALS) >= round(
                        current_sell_price,
                        PRICE_DECIMALS
                    ):
                        continue

                    cancel_resp = kraken_call(
                        "CANCEL_SELL",
                        cancel_order,
                        txid
                    )

                    if not cancel_resp or cancel_resp.get("error"):
                        actions.append("sell_reprice_cancel_failed")
                        log_event(
                            "SELL_REPRICE_SKIPPED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_price=buy_price,
                            current_sell_price=current_sell_price,
                            adjusted_sell_price=adjusted_sell_price,
                            age_minutes=age_minutes,
                            adjusted_profit_target_pct=adjusted_profit_target,
                            reason="cancel_failed"
                        )
                        continue

                    replace_resp = kraken_call(
                        "REPRICE_SELL",
                        place_sell,
                        adjusted_sell_price,
                        order["volume"]
                    )

                    if not replace_resp or replace_resp.get("error"):
                        actions.append("sell_reprice_replace_failed")
                        log_event(
                            "SELL_REPRICE_SKIPPED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_price=buy_price,
                            current_sell_price=current_sell_price,
                            adjusted_sell_price=adjusted_sell_price,
                            age_minutes=age_minutes,
                            adjusted_profit_target_pct=adjusted_profit_target,
                            reason="replace_failed"
                        )
                        continue

                    new_txid = replace_resp["result"]["txid"][0]
                    state["open_sell_orders"][new_txid] = {
                        "level": order.get("level"),
                        "volume": order["volume"],
                        "buy_price": buy_price,
                        "sell_price": adjusted_sell_price,
                        "placed_at": order.get("placed_at"),
                        "sell_pct_override": sell_pct_override,
                        "buy_source": order.get("buy_source"),
                        "trade_id": order.get("trade_id") or txid
                    }
                    del state["open_sell_orders"][txid]
                    save_state(state)
                    actions.append("sell_repriced")

                    log_and_console(
                        "SELL_ORDER_REPRICED",
                        message=(
                            f"SELL repriced from "
                            f"{round(current_sell_price, PRICE_DECIMALS)} to "
                            f"{round(adjusted_sell_price, PRICE_DECIMALS)}"
                        ),
                        cycle_id=cycle_id,
                        old_txid=txid,
                        txid=new_txid,
                        level=order.get("level"),
                        volume=order.get("volume"),
                        buy_price=buy_price,
                        old_sell_price=current_sell_price,
                        sell_price=adjusted_sell_price,
                        age_minutes=age_minutes,
                        adjusted_profit_target_pct=adjusted_profit_target,
                        sell_pct_override=sell_pct_override,
                        buy_source=order.get("buy_source")
                    )
                    notify_order_tracker(
                        trade_id=order.get("trade_id") or txid,
                        side="sell",
                        price=round(adjusted_sell_price, PRICE_DECIMALS),
                        quantity=round(order["volume"], VOLUME_DECIMALS),
                        order_id=new_txid,
                        timestamp=cycle_id,
                        notes="sell_reprice"
                    )
                    continue

                if status in ("canceled", "expired"):
                    sell_level = order.get("level")
                    del state["open_sell_orders"][txid]
                    save_state(state)
                    actions.append(f"sell_{status}")

                    log_and_console(
                        "ORDER_" + status.upper(),
                        message=f"SELL order {status} for level {sell_level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        side="sell",
                        level=sell_level,
                        volume=order.get("volume"),
                        buy_price=order.get("buy_price"),
                        sell_price=order.get("sell_price")
                    )
                    log_trade_activity(
                        "SELL_ORDER_" + status.upper(),
                        mode="live",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=sell_level,
                        volume=order.get("volume"),
                        buy_price=order.get("buy_price"),
                        sell_price=order.get("sell_price"),
                        buy_source=order.get("buy_source")
                    )
                    notify_order_tracker(
                        trade_id=order.get("trade_id") or txid,
                        side="sell",
                        price=order.get("sell_price"),
                        quantity=order.get("volume"),
                        order_id=txid,
                        timestamp=cycle_id,
                        notes=f"order_status={status}",
                        status=status
                    )

            # SELL CHECK
            for level, order in list(state["open_buy_orders"].items()):
                txid = order["txid"]
                status = order_status_map.get(txid)

                if status is None:
                    continue

                if status in ("canceled", "expired"):
                    del state["open_buy_orders"][level]
                    save_state(state)
                    actions.append(f"buy_{status}")

                    log_and_console(
                        "ORDER_" + status.upper(),
                        message=f"BUY order {status} for level {level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        side="buy",
                        level=level,
                        volume=order.get("volume"),
                        price=order.get("price", float(level))
                    )
                    log_trade_activity(
                        "BUY_ORDER_" + status.upper(),
                        mode="live",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=level,
                        volume=order.get("volume"),
                        price=order.get("price", float(level)),
                        buy_source=order.get("buy_source")
                    )
                    notify_order_tracker(
                        trade_id=order.get("trade_id") or txid,
                        side="buy",
                        price=order.get("price", float(level)),
                        quantity=order.get("volume"),
                        order_id=txid,
                        timestamp=cycle_id,
                        notes=f"order_status={status}",
                        status=status
                    )
                    continue

                if status != "closed":
                    continue

                sell_backoff_remaining = sell_backoff_remaining_seconds()
                order_sell_retry_after = parse_iso8601(
                    order.get("sell_retry_after")
                )
                if sell_backoff_remaining > 0:
                    actions.append("sell_backoff_active")
                    log_event(
                        "SELL_BACKOFF_ACTIVE",
                        cycle_id=cycle_id,
                        level=level,
                        txid=txid,
                        wait_seconds=round(sell_backoff_remaining, 2),
                        reason=order.get("sell_failure_reason")
                        or "insufficient_funds"
                    )
                    continue
                if (
                    order_sell_retry_after is not None
                    and now < order_sell_retry_after
                ):
                    actions.append("sell_retry_deferred")
                    log_event(
                        "SELL_RETRY_DEFERRED",
                        cycle_id=cycle_id,
                        level=level,
                        txid=txid,
                        retry_after=order.get("sell_retry_after"),
                        reason=order.get("sell_failure_reason")
                    )
                    continue

                buy_price = float(level)
                sell_pct_override = order.get("sell_pct_override")
                buy_source = order.get("buy_source")
                target_profit_pct = effective_sell_profit_target(
                    age_minutes=0,
                    base_profit_target=sell_pct_override,
                    buy_source=buy_source,
                    regime=regime,
                )
                sell_price = compute_sell_target_price(
                    buy_price,
                    target_profit_pct
                )
                placed_at = parse_iso8601(order.get("placed_at"))
                hold_minutes = None
                if placed_at is not None:
                    hold_minutes = (
                        now - placed_at
                    ).total_seconds() / 60

                first_buy_fill_processing = not fill_already_processed("buy", txid)
                if first_buy_fill_processing:
                    state["stats"]["buy_orders_filled"] += 1
                    increment_source_stat(
                        state["stats"],
                        "buy_orders_filled_by_source",
                        buy_source
                    )
                    order["filled_at"] = cycle_id
                    mark_fill_processed("buy", txid, cycle_id)
                    actions.append("buy_filled")

                    log_and_console(
                        "BUY_ORDER_FILLED",
                        message=f"BUY filled @ {round(buy_price, PRICE_DECIMALS)}",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        price=round(buy_price, PRICE_DECIMALS),
                        hold_minutes=hold_minutes,
                        sell_pct_override=sell_pct_override
                    )
                    log_trade_activity(
                        "BUY_ORDER_FILLED",
                        mode="live",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        price=round(buy_price, PRICE_DECIMALS),
                        buy_source=buy_source,
                        hold_minutes=hold_minutes,
                        sell_pct_override=sell_pct_override
                    )
                else:
                    actions.append("buy_fill_already_processed")
                    log_event(
                        "BUY_FILL_ALREADY_PROCESSED",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        price=round(buy_price, PRICE_DECIMALS),
                        buy_source=buy_source
                    )
                order.pop("sell_retry_after", None)
                order.pop("sell_failure_reason", None)
                save_state(state)

                log_event(
                    "TRADE_DECISION",
                    cycle_id=cycle_id,
                    side="sell",
                    volume=round(order["volume"], VOLUME_DECIMALS),
                        price=round(sell_price, PRICE_DECIMALS),
                        buy_price=round(buy_price, PRICE_DECIMALS),
                        sell_pct_override=target_profit_pct,
                        buy_source=buy_source
                    )

                existing_sell_txid, existing_sell_order, match_reason = (
                    find_existing_sell_for_buy(level, order)
                )
                if existing_sell_txid:
                    del state["open_buy_orders"][level]
                    save_state(state)
                    actions.append("buy_reconciled_to_existing_sell")

                    log_and_console(
                        "BUY_RECONCILED_TO_OPEN_SELL",
                        message=(
                            f"BUY level {level} already tracked by open sell "
                            f"{existing_sell_txid}"
                        ),
                        cycle_id=cycle_id,
                        buy_txid=order.get("txid"),
                        sell_txid=existing_sell_txid,
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        buy_price=round(buy_price, PRICE_DECIMALS),
                        sell_price=round(
                            existing_sell_order.get("sell_price", sell_price),
                            PRICE_DECIMALS
                        ),
                        buy_source=buy_source,
                        match_reason=match_reason
                    )
                    continue

                if not operating_mode_allows_sell_execution(operating_mode):
                    defer_reason = f"operating_mode_{operating_mode}"
                    existing_reason = order.get("sell_failure_reason")
                    retry_after = (
                        now + timedelta(seconds=price_check_interval_seconds)
                    ).isoformat()
                    order["sell_retry_after"] = retry_after
                    order["sell_failure_reason"] = defer_reason
                    save_state(state)
                    actions.append("sell_execution_disabled")
                    if existing_reason != defer_reason:
                        log_event(
                            "SELL_EXECUTION_DEFERRED",
                            cycle_id=cycle_id,
                            level=level,
                            txid=order.get("txid"),
                            retry_after=retry_after,
                            operating_mode=operating_mode,
                            buy_source=buy_source,
                            reason=defer_reason
                        )
                    continue

                sell_resp = kraken_call(
                    "SELL",
                    place_sell,
                    sell_price,
                    order["volume"]
                )

                if not sell_resp or sell_resp.get("error"):
                    actions.append("sell_rejected")
                    emit_alert(
                        "sell_rejected",
                        "critical",
                        "Filled buy could not place its sell order",
                        cycle_id=cycle_id,
                        level=level,
                        buy_source=buy_source,
                        sell_price=round(sell_price, PRICE_DECIMALS),
                        error=(
                            None if not sell_resp
                            else sell_resp.get("error")
                        )
                    )
                    log_event(
                        "ORDER_REJECTED",
                        cycle_id=cycle_id,
                        side="sell",
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        buy_price=round(buy_price, PRICE_DECIMALS),
                        sell_price=round(sell_price, PRICE_DECIMALS),
                        sell_pct_override=sell_pct_override,
                        buy_source=buy_source,
                        error=(
                            None if not sell_resp
                            else sell_resp.get("error")
                        )
                    )
                    log_trade_activity(
                        "ORDER_REJECTED",
                        mode="live",
                        cycle_id=cycle_id,
                        side="sell",
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        buy_price=round(buy_price, PRICE_DECIMALS),
                        sell_price=round(sell_price, PRICE_DECIMALS),
                        sell_pct_override=sell_pct_override,
                        buy_source=buy_source,
                        error=(
                            None if not sell_resp
                            else sell_resp.get("error")
                        )
                    )
                    cooldown_seconds = (
                        SELL_INSUFFICIENT_FUNDS_COOLDOWN_SECONDS
                    )
                    failure_reason = "sell_rejected"
                    if (
                        not sell_resp
                        and "insufficient funds"
                        in (order.get("sell_failure_reason") or "").lower()
                    ):
                        failure_reason = "insufficient_funds"
                    if not sell_resp:
                        failure_reason = "insufficient_funds"
                    retry_after = (
                        now + timedelta(seconds=cooldown_seconds)
                    ).isoformat()
                    order["sell_retry_after"] = retry_after
                    order["sell_failure_reason"] = failure_reason
                    if failure_reason == "insufficient_funds":
                        state["sell_insufficient_funds_backoff_until"] = max(
                            float(state.get(
                                "sell_insufficient_funds_backoff_until",
                                0
                            ) or 0),
                            time.time() + cooldown_seconds
                        )
                    save_state(state)
                    log_event(
                        "SELL_RETRY_SCHEDULED",
                        cycle_id=cycle_id,
                        level=level,
                        txid=order.get("txid"),
                        retry_after=retry_after,
                        reason=failure_reason
                    )
                    existing_sell_txid, existing_sell_order, match_reason = (
                        find_existing_sell_for_buy(level, order)
                    )
                    if existing_sell_txid:
                        del state["open_buy_orders"][level]
                        save_state(state)
                        actions.append("buy_reconciled_after_sell_reject")
                        log_and_console(
                            "BUY_RECONCILED_AFTER_SELL_REJECT",
                            message=(
                                f"BUY level {level} already tracked by open sell "
                                f"{existing_sell_txid} after rejected sell attempt"
                            ),
                            cycle_id=cycle_id,
                            buy_txid=order.get("txid"),
                            sell_txid=existing_sell_txid,
                            level=level,
                            volume=round(order["volume"], VOLUME_DECIMALS),
                            buy_price=round(buy_price, PRICE_DECIMALS),
                            sell_price=round(
                                existing_sell_order.get("sell_price", sell_price),
                                PRICE_DECIMALS
                            ),
                            buy_source=buy_source,
                            match_reason=match_reason
                        )
                    continue

                txid = sell_resp["result"]["txid"][0]
                state["open_sell_orders"][txid] = {
                    "level": level,
                    "volume": order["volume"],
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "placed_at": cycle_id,
                    "sell_pct_override": target_profit_pct,
                    "buy_source": buy_source,
                    "trade_id": order.get("trade_id") or order.get("txid")
                }
                del state["open_buy_orders"][level]
                state["stats"]["sell_orders_placed"] += 1
                increment_source_stat(
                    state["stats"],
                    "sell_orders_placed_by_source",
                    buy_source
                )
                save_state(state)
                actions.append("sell_placed")

                log_and_console(
                    "SELL_ORDER_PLACED",
                    message=f"SELL placed @ {round(sell_price, PRICE_DECIMALS)}",
                    cycle_id=cycle_id,
                    txid=txid,
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(sell_price, PRICE_DECIMALS),
                    buy_price=round(buy_price, PRICE_DECIMALS),
                    sell_pct_override=sell_pct_override,
                    buy_source=buy_source
                )
                log_trade_activity(
                    "SELL_ORDER_PLACED",
                    mode="live",
                    cycle_id=cycle_id,
                    txid=txid,
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(sell_price, PRICE_DECIMALS),
                    buy_price=round(buy_price, PRICE_DECIMALS),
                    sell_pct_override=sell_pct_override,
                    buy_source=buy_source
                )
                notify_order_tracker(
                    trade_id=order.get("trade_id") or order.get("txid"),
                    side="sell",
                    price=round(sell_price, PRICE_DECIMALS),
                    quantity=round(order["volume"], VOLUME_DECIMALS),
                    order_id=txid,
                    timestamp=cycle_id,
                    notes=buy_source
                )

            llm_buy_allowed = (
                "llm_target" in active_strategy_modes
                and low and high
                and llm_target is not None
                and llm_signal_gates_allow
                and llm_buys_allowed
                and mean_reversion_opportunity >= mean_reversion_min_opportunity
            )

            # BUY CANDIDATES
            if (
                range_signal_gates_allow
                and effective_position_size_pct > 0
                and active_strategy_modes
                and low and high
                and base_any_buys_allowed
            ):
                candidate_levels = []
                if llm_buy_allowed:
                    candidate_levels = [
                        {
                            "level": llm_target["buy_price"],
                            "sell_pct_override": llm_target["sell_pct"],
                            "buy_source": "llm_target"
                        }
                    ]
                else:
                    for strategy_mode in active_strategy_modes:
                        if strategy_mode == "llm_target":
                            continue
                        if strategy_mode == "mean":
                            buy_source = "range_mean"
                            route_context = anchor_strategy_route_for_source(
                                buy_source
                            )
                            route = route_context["route"]
                            route_config = (
                                route.get("strategy_config")
                                if route else strategy_config
                            )
                            route_entry_step_pct = routed_effective_entry_step_pct(
                                route_config,
                                price_regime_volatility_pct,
                            )
                            grid = compute_grid(
                                mean,
                                route_entry_step_pct,
                                int(strategy_float(
                                    route_config,
                                    "max_grid_size",
                                    max_grid_size,
                                ))
                            )
                            sell_pct_override = None
                        elif strategy_mode == "median" and median is not None:
                            buy_source = "range_median"
                            route_context = anchor_strategy_route_for_source(
                                buy_source
                            )
                            route = route_context["route"]
                            route_config = (
                                route.get("strategy_config")
                                if route else strategy_config
                            )
                            route_entry_step_pct = routed_effective_entry_step_pct(
                                route_config,
                                price_regime_volatility_pct,
                            )
                            grid = compute_grid(
                                median,
                                route_entry_step_pct,
                                int(strategy_float(
                                    route_config,
                                    "max_grid_size",
                                    max_grid_size,
                                ))
                            )
                            sell_pct_override = None
                        elif strategy_mode == "high":
                            if not effective_high_anchor_enabled:
                                continue
                            buy_source = "range_high_band"
                            route_context = anchor_strategy_route_for_source(
                                buy_source
                            )
                            route = route_context["route"]
                            route_config = (
                                route.get("strategy_config")
                                if route else strategy_config
                            )
                            route_entry_step_pct = routed_effective_entry_step_pct(
                                route_config,
                                price_regime_volatility_pct,
                            )
                            grid = compute_high_anchor_grid(
                                high,
                                price,
                                route_entry_step_pct,
                                strategy_float(
                                    route_config,
                                    "high_anchor_breakout_extension_pct",
                                    strategy_float(
                                        strategy_config,
                                        "high_anchor_breakout_extension_pct",
                                        0.0,
                                    ),
                                ),
                                weather_high_anchor_allowed,
                            )
                            sell_pct_override = strategy_float(
                                route_config,
                                "high_anchor_profit_target_pct",
                                high_anchor_profit_target_pct,
                            )
                        else:
                            buy_source = "range_low"
                            route_context = anchor_strategy_route_for_source(
                                buy_source
                            )
                            route = route_context["route"]
                            route_config = (
                                route.get("strategy_config")
                                if route else strategy_config
                            )
                            route_entry_step_pct = routed_effective_entry_step_pct(
                                route_config,
                                price_regime_volatility_pct,
                            )
                            grid = compute_grid(
                                low,
                                route_entry_step_pct,
                                int(strategy_float(
                                    route_config,
                                    "max_grid_size",
                                    max_grid_size,
                                ))
                            )
                            sell_pct_override = None

                        if sell_pct_override is None:
                            sell_pct_override = strategy_float(
                                route_config,
                                "profit_target_pct",
                                profit_target_pct,
                            )

                        for level in grid:
                            candidate_levels.append(
                                {
                                    "level": level,
                                    "sell_pct_override": sell_pct_override,
                                    "buy_source": buy_source,
                                    "anchor_router_anchor": route_context["anchor"],
                                    "anchor_router_route": route,
                                    "anchor_router_block_reason": (
                                        route_context["block_reason"]
                                    ),
                                    "route_strategy_config": route_config,
                                    "route_entry_step_pct": route_entry_step_pct,
                                }
                            )

                deduped_candidates = []
                seen_levels = set()
                for candidate in candidate_levels:
                    rounded_level = round(candidate["level"], PRICE_DECIMALS)
                    if rounded_level in seen_levels:
                        continue
                    seen_levels.add(rounded_level)
                    deduped_candidates.append(candidate)

                bal = safe_kraken_private("BALANCE", "/0/private/Balance")

                if not bal or "result" not in bal:
                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="hold",
                        price=price,
                        execution_signal=execution_signal,
                        reason="balance_fetch_failed"
                    )
                    actions.append("hold_balance_fetch_failed")
                    send_checkin(loop_count=loop_count, message="loop_complete")
                    time.sleep(price_check_interval_seconds)
                    continue

                usd = float(bal["result"].get("ZUSD", 0))
                reserved_buy_usd = reserved_buy_capital_usd()
                available_usd = max(0.0, usd - reserved_buy_usd)
                reserved_sell_levels = {
                    sell_order.get("level")
                    for sell_order in state["open_sell_orders"].values()
                }
                last_sell_price = positive_float(state.get("last_sell_price"))
                llm_sell_cooldown_remaining = (
                    llm_sell_cooldown_remaining_minutes(now)
                )
                deployed_inventory_usd = current_inventory_usd(price)
                bucket_inventory_usd_map = inventory_usd_by_bucket(price)
                high_anchor_exposure = high_anchor_backlog_exposure(now)
                high_anchor_order_count = high_anchor_exposure["raw_count"]
                high_anchor_effective_order_count = (
                    high_anchor_exposure["effective_count"]
                )
                high_anchor_cooldown_remaining = (
                    high_anchor_cooldown_remaining_minutes(now)
                )

                for candidate in deduped_candidates:
                    level = candidate["level"]
                    active_sell_pct_override = candidate["sell_pct_override"]
                    buy_source = candidate["buy_source"]
                    route_config = candidate.get("route_strategy_config") or strategy_config
                    route = candidate.get("anchor_router_route")
                    route_anchor = candidate.get("anchor_router_anchor")
                    route_block_reason = candidate.get(
                        "anchor_router_block_reason"
                    )
                    route_limits = routed_effective_limits(
                        route_config,
                        regime,
                        smoothed_risk_multiplier * risk_context_size_multiplier,
                    )
                    route_inventory_pressure = inventory_pressure_adjustment(
                        deployed_inventory_usd,
                        route_limits["max_inventory_usd"],
                        route_config,
                    )
                    candidate_effective_position_size_pct = (
                        route_limits["position_size_pct"]
                        * route_inventory_pressure["size_multiplier"]
                    )
                    leveling_size_multiplier = (
                        high_band_leveling_size_multiplier(
                            route_config,
                            buy_source,
                            weather_report,
                        )
                    )
                    candidate_effective_position_size_pct *= (
                        leveling_size_multiplier
                    )
                    leveling_bypass_blocked = (
                        buy_source == "range_high_band"
                        and weather_leveling_blocks_high_band_bypass(
                            route_config,
                            weather_report,
                        )
                    )
                    candidate_effective_max_inventory_usd = (
                        route_limits["max_inventory_usd"]
                    )
                    candidate_effective_max_open_sell_orders = (
                        route_limits["max_open_sell_orders"]
                    )
                    candidate_min_buy_notional_usd = strategy_float(
                        route_config,
                        "min_buy_notional_usd",
                        min_buy_notional_usd,
                    )
                    candidate_min_buy_volume_btc = strategy_float(
                        route_config,
                        "min_buy_volume_btc",
                        min_buy_volume_btc,
                    )
                    candidate_max_open_high_anchor_orders = int(strategy_float(
                        route_config,
                        "max_open_high_anchor_orders",
                        max_open_high_anchor_orders,
                    ))
                    candidate_high_anchor_soft_release_minutes = strategy_float(
                        route_config,
                        "high_anchor_backlog_soft_release_minutes",
                        high_anchor_backlog_soft_release_minutes,
                    )
                    candidate_high_anchor_old_order_weight = strategy_float(
                        route_config,
                        "high_anchor_backlog_old_order_weight",
                        high_anchor_backlog_old_order_weight,
                    )
                    if buy_source == "range_high_band":
                        high_anchor_exposure = high_anchor_backlog_exposure(
                            now,
                            candidate_high_anchor_soft_release_minutes,
                            candidate_high_anchor_old_order_weight,
                        )
                        high_anchor_order_count = high_anchor_exposure[
                            "raw_count"
                        ]
                        high_anchor_effective_order_count = (
                            high_anchor_exposure["effective_count"]
                        )
                    bucket_name = buy_source_bucket(buy_source)
                    momentum_entry_tolerance_pct = (
                        range_momentum_entry_tolerance_pct(
                            route_config,
                            buy_source,
                            strategy_config
                        )
                    )
                    momentum_entry_max_price = level * (
                        1 + momentum_entry_tolerance_pct
                    )
                    bucket_inventory_usd = bucket_inventory_usd_map.get(
                        bucket_name,
                        0.0
                    )
                    bucket_cap_usd = bucket_inventory_cap_usd(
                        bucket_name,
                        candidate_effective_max_inventory_usd
                    )
                    flow_control = flow_adjustment(flow_pressure, buy_source)
                    key = str(level)
                    skip_reason = None
                    high_band_guard = {"allowed": True, "reason": None}
                    above_last_sell_breakout_bypass = (
                        allow_above_last_sell_for_candidate(
                            route_config,
                            buy_source,
                            weather_report
                        )
                    )

                    if key in state["open_buy_orders"]:
                        skip_reason = "open_buy_order"
                    elif route_block_reason:
                        skip_reason = route_block_reason
                    elif key in reserved_sell_levels:
                        skip_reason = "open_sell_order"
                    elif (
                        prevent_buy_above_last_sell
                        and not above_last_sell_breakout_bypass
                        and last_sell_price is not None
                        and level > (
                            last_sell_price * (1 - buy_after_sell_discount_pct)
                        )
                    ):
                        skip_reason = "above_last_sell_discount"
                    elif (
                        mean_reversion_opportunity < mean_reversion_min_opportunity
                    ):
                        skip_reason = "mean_reversion_opportunity_below_min"
                    elif price_is_above_allowed_entry(
                        price,
                        level,
                        route_config,
                        buy_source,
                        strategy_config
                    ):
                        skip_reason = "price_above_level"
                    elif (
                        len(state["open_sell_orders"])
                        >= candidate_effective_max_open_sell_orders
                    ):
                        skip_reason = "max_open_sell_orders"
                    elif (
                        deployed_inventory_usd
                        >= candidate_effective_max_inventory_usd
                    ):
                        skip_reason = "max_inventory_usd"
                    elif (
                        buy_source == "llm_target"
                        and "llm_target" not in active_strategy_modes
                    ):
                        skip_reason = "llm_target_disabled_in_strategy"
                    elif (
                        buy_source == "llm_target"
                        and not llm_signal_gates_allow
                    ):
                        skip_reason = (
                            freshness_block_reason
                            or "source_guard_blocked"
                        )
                    elif (
                        buy_source != "llm_target"
                        and not range_signal_gates_allow
                    ):
                        skip_reason = (
                            freshness_block_reason
                            or "source_guard_blocked"
                        )
                    elif (
                        buy_source == "llm_target"
                        and not llm_buys_allowed
                    ):
                        skip_reason = "sentiment_action_not_bullish_allowed"
                    elif (
                        buy_source == "range_high_band"
                        and not range_high_buys_allowed
                    ):
                        skip_reason = (
                            "sentiment_action_not_high_range_permitted"
                        )
                    elif (
                        buy_source != "llm_target"
                        and not range_core_buys_allowed
                    ):
                        skip_reason = (
                            "sentiment_action_not_range_permitted"
                        )
                    elif buy_source == "range_high_band":
                        high_band_guard = risk_context_high_band_guard(
                            route_config,
                            sentiment_payload.get("risk_context"),
                            strategy_config
                        )
                        if not high_band_guard["allowed"]:
                            skip_reason = high_band_guard["reason"]
                    elif flow_control["block_buy"]:
                        skip_reason = flow_control["reason"]
                    elif (
                        buy_source == "llm_target"
                        and llm_sell_cooldown_remaining > 0
                    ):
                        skip_reason = "llm_sell_cooldown"
                    elif (
                        buy_source == "range_high_band"
                        and high_anchor_cooldown_remaining > 0
                    ):
                        skip_reason = "high_anchor_cooldown"
                    elif (
                        buy_source == "range_high_band"
                        and high_anchor_effective_order_count >= (
                            candidate_max_open_high_anchor_orders
                        )
                    ):
                        skip_reason = "max_open_high_anchor_orders"
                    elif available_usd <= 0:
                        skip_reason = "insufficient_available_usd"

                    volume = (
                        available_usd
                        * candidate_effective_position_size_pct
                        * flow_control["size_multiplier"]
                    ) / level
                    trade_notional_usd = level * volume
                    projected_inventory_usd = deployed_inventory_usd + (
                        level * volume
                    )
                    projected_bucket_inventory_usd = (
                        bucket_inventory_usd + (level * volume)
                    )

                    if (
                        skip_reason is None
                        and trade_notional_usd < candidate_min_buy_notional_usd
                    ):
                        skip_reason = "below_min_notional"

                    if (
                        skip_reason is None
                        and projected_inventory_usd > (
                            candidate_effective_max_inventory_usd
                        )
                    ):
                        skip_reason = "max_inventory_usd"

                    if (
                        skip_reason is None
                        and projected_bucket_inventory_usd > bucket_cap_usd
                    ):
                        skip_reason = "bucket_max_inventory_usd"

                    if skip_reason is None and volume < candidate_min_buy_volume_btc:
                        skip_reason = "below_min_volume"

                    if skip_reason is not None:
                        log_event(
                            "BUY_CANDIDATE_SKIPPED",
                            cycle_id=cycle_id,
                            level=round(level, PRICE_DECIMALS),
                            market_price=price,
                            execution_signal=execution_signal,
                            action_recommendation=action_recommendation,
                            action_policy_reason=action_policy.get("reason"),
                            **sentiment_risk_fields,
                            signal_status=signal_status,
                            freshness_allows_trading=freshness_allows_trading,
                            freshness_block_reason=freshness_block_reason,
                            source_guard_allows_trading=source_guard_allows_trading,
                            llm_signal_gates_allow=llm_signal_gates_allow,
                            range_signal_gates_allow=range_signal_gates_allow,
                            range_fallback_active=range_fallback_active,
                            llm_buys_allowed=llm_buys_allowed,
                            range_core_buys_allowed=range_core_buys_allowed,
                            range_high_buys_allowed=range_high_buys_allowed,
                            range_buys_allowed=range_buys_allowed,
                            risk_context_high_band_guard_allowed=(
                                high_band_guard.get("allowed")
                            ),
                            risk_context_high_band_guard_reason=(
                                high_band_guard.get("reason")
                            ),
                            weather_leveling_high_band_size_multiplier=(
                                leveling_size_multiplier
                            ),
                            weather_leveling_bypass_blocked=(
                                leveling_bypass_blocked
                            ),
                            risk_context_high_band_max_market_risk_score=(
                                high_band_guard.get(
                                    "risk_context_high_band_max_market_risk_score"
                                )
                            ),
                            risk_context_high_band_min_buy_aggression_score=(
                                high_band_guard.get(
                                    "risk_context_high_band_min_buy_aggression_score"
                                )
                            ),
                            risk_context_high_band_min_rebound_score=(
                                high_band_guard.get(
                                    "risk_context_high_band_min_rebound_score"
                                )
                            ),
                            risk_context_high_band_min_breakout_score=(
                                high_band_guard.get(
                                    "risk_context_high_band_min_breakout_score"
                                )
                            ),
                            operating_mode=operating_mode,
                            sentiment_control_mode=sentiment_control_mode,
                            anchor_strategy_router_enabled=(
                                anchor_strategy_router_enabled
                            ),
                            anchor_strategy_router_anchor=route_anchor,
                            anchor_strategy_router_strategy_label=(
                                route.get("strategy_label") if route else None
                            ),
                            anchor_strategy_router_strategy_file=(
                                route.get("strategy_file") if route else None
                            ),
                            runtime_block_reason=runtime_block_reason,
                            buy_source=buy_source,
                            bucket_name=bucket_name,
                            high_anchor_order_count=high_anchor_order_count,
                            high_anchor_effective_order_count=round(
                                high_anchor_effective_order_count,
                                4
                            ),
                            high_anchor_open_buy_count=(
                                high_anchor_exposure["open_buy_count"]
                            ),
                            high_anchor_fresh_sell_count=(
                                high_anchor_exposure["fresh_sell_count"]
                            ),
                            high_anchor_aged_sell_count=(
                                high_anchor_exposure["aged_sell_count"]
                            ),
                            high_anchor_backlog_soft_release_minutes=(
                                high_anchor_exposure["soft_release_minutes"]
                            ),
                            high_anchor_backlog_old_order_weight=(
                                high_anchor_exposure["old_order_weight"]
                            ),
                            high_anchor_cooldown_remaining_minutes=round(
                                high_anchor_cooldown_remaining,
                                2
                            ),
                            llm_sell_cooldown_remaining_minutes=round(
                                llm_sell_cooldown_remaining,
                                2
                            ),
                            open_buy_count=len(state["open_buy_orders"]),
                            open_sell_count=len(state["open_sell_orders"]),
                            usd_balance=usd,
                            available_usd_balance=round(available_usd, 8),
                            reserved_buy_capital_usd=round(reserved_buy_usd, 8),
                            trade_notional_usd=round(trade_notional_usd, 8),
                            deployed_inventory_usd=deployed_inventory_usd,
                            projected_inventory_usd=round(
                                projected_inventory_usd,
                                8
                            ),
                            bucket_inventory_usd=round(bucket_inventory_usd, 8),
                            projected_bucket_inventory_usd=round(
                                projected_bucket_inventory_usd,
                                8
                            ),
                            bucket_inventory_cap_usd=round(bucket_cap_usd, 8),
                            effective_position_size_pct=(
                                candidate_effective_position_size_pct
                            ),
                            effective_max_inventory_usd=(
                                candidate_effective_max_inventory_usd
                            ),
                            effective_max_open_sell_orders=(
                                candidate_effective_max_open_sell_orders
                            ),
                            smoothed_risk_multiplier=smoothed_risk_multiplier,
                            risk_context_position_sizing_enabled=(
                                risk_context_size_adjustment["enabled"]
                            ),
                            risk_context_position_size_raw_multiplier=(
                                risk_context_size_adjustment["raw_multiplier"]
                            ),
                            risk_context_position_size_clamped_multiplier=(
                                risk_context_size_adjustment["clamped_multiplier"]
                            ),
                            risk_context_position_size_blend=(
                                risk_context_size_adjustment["blend"]
                            ),
                            risk_context_position_size_effective_multiplier=(
                                risk_context_size_multiplier
                            ),
                            flow_pressure=flow_pressure,
                            flow_control_reason=flow_control["reason"],
                            mean_reversion_opportunity=(
                                mean_reversion_opportunity
                            ),
                            effective_entry_step_pct=(
                                candidate.get("route_entry_step_pct")
                                or current_entry_step_pct
                            ),
                            momentum_entry_tolerance_pct=(
                                momentum_entry_tolerance_pct
                            ),
                            momentum_entry_max_price=round(
                                momentum_entry_max_price,
                                PRICE_DECIMALS
                            ),
                            inventory_pressure_usage_ratio=(
                                route_inventory_pressure["usage_ratio"]
                            ),
                            inventory_pressure_size_multiplier=(
                                route_inventory_pressure["size_multiplier"]
                            ),
                            last_sell_price=last_sell_price,
                            above_last_sell_breakout_bypass=(
                                above_last_sell_breakout_bypass
                            ),
                            candidate_volume_btc=round(volume, VOLUME_DECIMALS),
                            candidate_sell_pct_override=active_sell_pct_override,
                            reason=skip_reason
                        )
                        log_event(
                            "GRID_LEVEL_EVAL",
                            cycle_id=cycle_id,
                            level=round(level, PRICE_DECIMALS),
                            market_price=price,
                            execution_signal=execution_signal,
                            usd_balance=usd,
                            available_usd_balance=round(available_usd, 8),
                            reserved_buy_capital_usd=round(reserved_buy_usd, 8),
                            trade_notional_usd=round(trade_notional_usd, 8),
                            deployed_inventory_usd=deployed_inventory_usd,
                            bucket_name=bucket_name,
                            bucket_inventory_usd=round(bucket_inventory_usd, 8),
                            bucket_inventory_cap_usd=round(bucket_cap_usd, 8),
                            high_anchor_order_count=high_anchor_order_count,
                            high_anchor_effective_order_count=round(
                                high_anchor_effective_order_count,
                                4
                            ),
                            high_anchor_open_buy_count=(
                                high_anchor_exposure["open_buy_count"]
                            ),
                            high_anchor_fresh_sell_count=(
                                high_anchor_exposure["fresh_sell_count"]
                            ),
                            high_anchor_aged_sell_count=(
                                high_anchor_exposure["aged_sell_count"]
                            ),
                            high_anchor_cooldown_remaining_minutes=round(
                                high_anchor_cooldown_remaining,
                                2
                            ),
                            llm_sell_cooldown_remaining_minutes=round(
                                llm_sell_cooldown_remaining,
                                2
                            ),
                            last_sell_price=last_sell_price,
                            above_last_sell_breakout_bypass=(
                                above_last_sell_breakout_bypass
                            ),
                            action_recommendation=action_recommendation,
                            action_policy_reason=action_policy.get("reason"),
                            **sentiment_risk_fields,
                            sentiment_regime=regime["name"],
                            effective_position_size_pct=(
                                candidate_effective_position_size_pct
                            ),
                            effective_max_inventory_usd=(
                                candidate_effective_max_inventory_usd
                            ),
                            effective_max_open_sell_orders=(
                                candidate_effective_max_open_sell_orders
                            ),
                            smoothed_risk_multiplier=smoothed_risk_multiplier,
                            risk_context_position_sizing_enabled=(
                                risk_context_size_adjustment["enabled"]
                            ),
                            risk_context_position_size_raw_multiplier=(
                                risk_context_size_adjustment["raw_multiplier"]
                            ),
                            risk_context_position_size_clamped_multiplier=(
                                risk_context_size_adjustment["clamped_multiplier"]
                            ),
                            risk_context_position_size_blend=(
                                risk_context_size_adjustment["blend"]
                            ),
                            risk_context_position_size_effective_multiplier=(
                                risk_context_size_multiplier
                            ),
                            flow_pressure=flow_pressure,
                            flow_control_reason=flow_control["reason"],
                            mean_reversion_opportunity=(
                                mean_reversion_opportunity
                            ),
                            effective_entry_step_pct=(
                                candidate.get("route_entry_step_pct")
                                or current_entry_step_pct
                            ),
                            momentum_entry_tolerance_pct=(
                                momentum_entry_tolerance_pct
                            ),
                            momentum_entry_max_price=round(
                                momentum_entry_max_price,
                                PRICE_DECIMALS
                            ),
                            inventory_pressure_usage_ratio=(
                                route_inventory_pressure["usage_ratio"]
                            ),
                            inventory_pressure_size_multiplier=(
                                route_inventory_pressure["size_multiplier"]
                            ),
                            buy_source=buy_source,
                            anchor_strategy_router_enabled=(
                                anchor_strategy_router_enabled
                            ),
                            anchor_strategy_router_anchor=route_anchor,
                            anchor_strategy_router_strategy_label=(
                                route.get("strategy_label") if route else None
                            ),
                            anchor_strategy_router_strategy_file=(
                                route.get("strategy_file") if route else None
                            ),
                            reason=skip_reason
                        )
                        continue

                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="buy",
                        volume=round(volume, VOLUME_DECIMALS),
                        trade_notional_usd=round(trade_notional_usd, 8),
                        price=round(level, PRICE_DECIMALS),
                        execution_signal=execution_signal,
                        usd_balance=usd,
                        available_usd_balance=round(available_usd, 8),
                        reserved_buy_capital_usd=round(reserved_buy_usd, 8),
                        action_recommendation=action_recommendation,
                        action_policy_reason=action_policy.get("reason"),
                        **sentiment_risk_fields,
                        range_low=low,
                        range_high=high,
                        range_mean=mean,
                        range_median=median,
                        sentiment_regime=regime["name"],
                        effective_position_size_pct=(
                            candidate_effective_position_size_pct
                        ),
                        smoothed_risk_multiplier=smoothed_risk_multiplier,
                        risk_context_position_sizing_enabled=(
                            risk_context_size_adjustment["enabled"]
                        ),
                        risk_context_position_size_raw_multiplier=(
                            risk_context_size_adjustment["raw_multiplier"]
                        ),
                        risk_context_position_size_clamped_multiplier=(
                            risk_context_size_adjustment["clamped_multiplier"]
                        ),
                        risk_context_position_size_blend=(
                            risk_context_size_adjustment["blend"]
                        ),
                        risk_context_position_size_effective_multiplier=(
                            risk_context_size_multiplier
                        ),
                        flow_pressure=flow_pressure,
                        flow_control_reason=flow_control["reason"],
                        mean_reversion_opportunity=mean_reversion_opportunity,
                        effective_entry_step_pct=(
                            candidate.get("route_entry_step_pct")
                            or current_entry_step_pct
                        ),
                        momentum_entry_tolerance_pct=(
                            momentum_entry_tolerance_pct
                        ),
                        momentum_entry_max_price=round(
                            momentum_entry_max_price,
                            PRICE_DECIMALS
                        ),
                        inventory_pressure_usage_ratio=(
                            route_inventory_pressure["usage_ratio"]
                        ),
                        inventory_pressure_size_multiplier=(
                            route_inventory_pressure["size_multiplier"]
                        ),
                        buy_source=buy_source,
                        bucket_name=bucket_name,
                        bucket_inventory_usd=round(bucket_inventory_usd, 8),
                        bucket_inventory_cap_usd=round(bucket_cap_usd, 8),
                        high_anchor_order_count=high_anchor_order_count,
                        high_anchor_effective_order_count=round(
                            high_anchor_effective_order_count,
                            4
                        ),
                        high_anchor_open_buy_count=(
                            high_anchor_exposure["open_buy_count"]
                        ),
                        high_anchor_fresh_sell_count=(
                            high_anchor_exposure["fresh_sell_count"]
                        ),
                        high_anchor_aged_sell_count=(
                            high_anchor_exposure["aged_sell_count"]
                        ),
                        weather_leveling_high_band_size_multiplier=(
                            leveling_size_multiplier
                        ),
                        weather_leveling_bypass_blocked=(
                            leveling_bypass_blocked
                        ),
                        last_sell_price=last_sell_price,
                        above_last_sell_breakout_bypass=(
                            above_last_sell_breakout_bypass
                        ),
                        sell_pct_override=active_sell_pct_override,
                        anchor_strategy_router_enabled=(
                            anchor_strategy_router_enabled
                        ),
                        anchor_strategy_router_anchor=route_anchor,
                        anchor_strategy_router_strategy_label=(
                            route.get("strategy_label") if route else None
                        ),
                        anchor_strategy_router_strategy_file=(
                            route.get("strategy_file") if route else None
                        )
                    )

                    if risk_context_shadow_buy_enabled:
                        shadow_sell_price = compute_sell_target_price(
                            level,
                            active_sell_pct_override
                        )
                        shadow_payload = {
                            "mode": "shadow",
                            "cycle_id": cycle_id,
                            "level": round(level, PRICE_DECIMALS),
                            "volume": round(volume, VOLUME_DECIMALS),
                            "trade_notional_usd": round(trade_notional_usd, 8),
                            "market_price": price,
                            "shadow_sell_price": round(
                                shadow_sell_price,
                                PRICE_DECIMALS
                            ),
                            "execution_signal": execution_signal,
                            "buy_source": buy_source,
                            "sell_pct_override": active_sell_pct_override,
                            "effective_position_size_pct": (
                                candidate_effective_position_size_pct
                            ),
                            "risk_context_position_sizing_enabled": (
                                risk_context_size_adjustment["enabled"]
                            ),
                            "risk_context_position_size_raw_multiplier": (
                                risk_context_size_adjustment["raw_multiplier"]
                            ),
                            "risk_context_position_size_clamped_multiplier": (
                                risk_context_size_adjustment[
                                    "clamped_multiplier"
                                ]
                            ),
                            "risk_context_position_size_blend": (
                                risk_context_size_adjustment["blend"]
                            ),
                            "risk_context_position_size_effective_multiplier": (
                                risk_context_size_multiplier
                            ),
                            "sentiment_regime": regime["name"],
                            "operating_mode": operating_mode,
                            "sentiment_control_mode": sentiment_control_mode,
                            "anchor_strategy_router_enabled": (
                                anchor_strategy_router_enabled
                            ),
                            "anchor_strategy_router_anchor": route_anchor,
                            "anchor_strategy_router_strategy_label": (
                                route.get("strategy_label") if route else None
                            ),
                            "anchor_strategy_router_strategy_file": (
                                route.get("strategy_file") if route else None
                            ),
                            "paper_trading_enabled": paper_trading_enabled,
                            "above_last_sell_breakout_bypass": (
                                above_last_sell_breakout_bypass
                            ),
                            "weather_leveling_high_band_size_multiplier": (
                                leveling_size_multiplier
                            ),
                            "weather_leveling_bypass_blocked": (
                                leveling_bypass_blocked
                            ),
                            "shadow_reason": "approved_risk_scored_candidate",
                        }
                        shadow_payload.update(sentiment_risk_fields)
                        log_event(
                            "RISK_CONTEXT_PAPER_BUY_PLANNED",
                            **shadow_payload,
                        )
                        log_trade_activity(
                            "RISK_CONTEXT_PAPER_BUY_PLANNED",
                            **shadow_payload,
                        )

                    state["stats"]["approved_buy_candidates"] += 1
                    increment_source_stat(
                        state["stats"],
                        "approved_counts_by_source",
                        buy_source
                    )
                    if paper_trading_enabled:
                        actions.append("paper_buy_planned")
                        log_event(
                            "PAPER_BUY_ORDER_PLANNED",
                            cycle_id=cycle_id,
                            level=round(level, PRICE_DECIMALS),
                            volume=round(volume, VOLUME_DECIMALS),
                            trade_notional_usd=round(trade_notional_usd, 8),
                            market_price=price,
                            execution_signal=execution_signal,
                            buy_source=buy_source,
                            sell_pct_override=active_sell_pct_override,
                            above_last_sell_breakout_bypass=(
                                above_last_sell_breakout_bypass
                            ),
                            weather_leveling_high_band_size_multiplier=(
                                leveling_size_multiplier
                            ),
                            weather_leveling_bypass_blocked=(
                                leveling_bypass_blocked
                            ),
                            effective_position_size_pct=(
                                candidate_effective_position_size_pct
                            ),
                            risk_context_position_sizing_enabled=(
                                risk_context_size_adjustment["enabled"]
                            ),
                            risk_context_position_size_effective_multiplier=(
                                risk_context_size_multiplier
                            ),
                            operating_mode=operating_mode,
                            llm_target_active=(buy_source == "llm_target"),
                            anchor_strategy_router_enabled=(
                                anchor_strategy_router_enabled
                            ),
                            anchor_strategy_router_anchor=route_anchor,
                            anchor_strategy_router_strategy_label=(
                                route.get("strategy_label") if route else None
                            ),
                            anchor_strategy_router_strategy_file=(
                                route.get("strategy_file") if route else None
                            ),
                            paper_trading_enabled=True
                        )
                        log_trade_activity(
                            "PAPER_BUY_ORDER_PLANNED",
                            mode="paper",
                            cycle_id=cycle_id,
                            level=round(level, PRICE_DECIMALS),
                            volume=round(volume, VOLUME_DECIMALS),
                            trade_notional_usd=round(trade_notional_usd, 8),
                            market_price=price,
                            execution_signal=execution_signal,
                            buy_source=buy_source,
                            sell_pct_override=active_sell_pct_override,
                            above_last_sell_breakout_bypass=(
                                above_last_sell_breakout_bypass
                            ),
                            weather_leveling_high_band_size_multiplier=(
                                leveling_size_multiplier
                            ),
                            weather_leveling_bypass_blocked=(
                                leveling_bypass_blocked
                            ),
                            effective_position_size_pct=(
                                candidate_effective_position_size_pct
                            ),
                            risk_context_position_sizing_enabled=(
                                risk_context_size_adjustment["enabled"]
                            ),
                            risk_context_position_size_effective_multiplier=(
                                risk_context_size_multiplier
                            ),
                            operating_mode=operating_mode,
                            anchor_strategy_router_enabled=(
                                anchor_strategy_router_enabled
                            ),
                            anchor_strategy_router_anchor=route_anchor,
                            anchor_strategy_router_strategy_label=(
                                route.get("strategy_label") if route else None
                            ),
                            anchor_strategy_router_strategy_file=(
                                route.get("strategy_file") if route else None
                            ),
                        )
                        reserved_buy_usd += level * volume
                        available_usd = max(0.0, usd - reserved_buy_usd)
                        continue

                    buy_resp = kraken_call(
                        "BUY",
                        place_buy,
                        level,
                        volume
                    )

                    if not buy_resp or buy_resp.get("error"):
                        actions.append("buy_rejected")
                        state["stats"]["buy_order_rejections"] += 1
                        save_state(state)
                        log_event(
                            "ORDER_REJECTED",
                            cycle_id=cycle_id,
                            side="buy",
                            level=round(level, PRICE_DECIMALS),
                            volume=round(volume, VOLUME_DECIMALS),
                            execution_signal=execution_signal,
                            buy_source=buy_source,
                            sell_pct_override=active_sell_pct_override,
                            effective_position_size_pct=(
                                candidate_effective_position_size_pct
                            ),
                            risk_context_position_sizing_enabled=(
                                risk_context_size_adjustment["enabled"]
                            ),
                            risk_context_position_size_effective_multiplier=(
                                risk_context_size_multiplier
                            ),
                            anchor_strategy_router_enabled=(
                                anchor_strategy_router_enabled
                            ),
                            anchor_strategy_router_anchor=route_anchor,
                            anchor_strategy_router_strategy_label=(
                                route.get("strategy_label") if route else None
                            ),
                            anchor_strategy_router_strategy_file=(
                                route.get("strategy_file") if route else None
                            ),
                            error=(
                                None if not buy_resp
                                else buy_resp.get("error")
                            )
                        )
                        log_trade_activity(
                            "ORDER_REJECTED",
                            mode="live",
                            cycle_id=cycle_id,
                            side="buy",
                            level=round(level, PRICE_DECIMALS),
                            volume=round(volume, VOLUME_DECIMALS),
                            execution_signal=execution_signal,
                            buy_source=buy_source,
                            sell_pct_override=active_sell_pct_override,
                            effective_position_size_pct=(
                                candidate_effective_position_size_pct
                            ),
                            risk_context_position_sizing_enabled=(
                                risk_context_size_adjustment["enabled"]
                            ),
                            risk_context_position_size_effective_multiplier=(
                                risk_context_size_multiplier
                            ),
                            anchor_strategy_router_enabled=(
                                anchor_strategy_router_enabled
                            ),
                            anchor_strategy_router_anchor=route_anchor,
                            anchor_strategy_router_strategy_label=(
                                route.get("strategy_label") if route else None
                            ),
                            anchor_strategy_router_strategy_file=(
                                route.get("strategy_file") if route else None
                            ),
                            error=(
                                None if not buy_resp
                                else buy_resp.get("error")
                            )
                        )
                        continue

                    txid = buy_resp["result"]["txid"][0]
                    state["open_buy_orders"][key] = {
                        "txid": txid,
                        "volume": volume,
                        "price": level,
                        "placed_at": cycle_id,
                        "sell_pct_override": active_sell_pct_override,
                        "buy_source": buy_source,
                        "trade_id": txid,
                        "anchor_strategy_router_anchor": route_anchor,
                        "anchor_strategy_router_strategy_label": (
                            route.get("strategy_label") if route else None
                        ),
                        "anchor_strategy_router_strategy_file": (
                            route.get("strategy_file") if route else None
                        ),
                    }
                    if buy_source == "range_high_band":
                        state["last_high_anchor_buy_at"] = cycle_id
                        high_anchor_order_count += 1
                        high_anchor_effective_order_count += 1
                        high_anchor_exposure["raw_count"] = (
                            high_anchor_order_count
                        )
                        high_anchor_exposure["effective_count"] = (
                            high_anchor_effective_order_count
                        )
                        high_anchor_exposure["open_buy_count"] += 1
                        high_anchor_cooldown_remaining = (
                            high_anchor_buy_cooldown_minutes
                        )
                    state["stats"]["buy_orders_placed"] += 1
                    increment_source_stat(
                        state["stats"],
                        "buy_orders_placed_by_source",
                        buy_source
                    )
                    save_state(state)
                    actions.append("buy_placed")
                    deployed_inventory_usd = projected_inventory_usd
                    bucket_inventory_usd_map[bucket_name] = (
                        projected_bucket_inventory_usd
                    )
                    reserved_buy_usd += level * volume
                    available_usd = max(0.0, usd - reserved_buy_usd)

                    log_and_console(
                        "BUY_ORDER_PLACED",
                        message=f"BUY placed @ {round(level, PRICE_DECIMALS)}",
                        cycle_id=cycle_id,
                        txid=txid,
                        volume=round(volume, VOLUME_DECIMALS),
                        price=round(level, PRICE_DECIMALS),
                        buy_source=buy_source,
                        sell_pct_override=active_sell_pct_override,
                        above_last_sell_breakout_bypass=(
                            above_last_sell_breakout_bypass
                        ),
                        weather_leveling_high_band_size_multiplier=(
                            leveling_size_multiplier
                        ),
                        weather_leveling_bypass_blocked=(
                            leveling_bypass_blocked
                        ),
                        effective_position_size_pct=(
                            candidate_effective_position_size_pct
                        ),
                        risk_context_position_sizing_enabled=(
                            risk_context_size_adjustment["enabled"]
                        ),
                        risk_context_position_size_effective_multiplier=(
                            risk_context_size_multiplier
                        ),
                        **sentiment_risk_fields,
                        anchor_strategy_router_enabled=(
                            anchor_strategy_router_enabled
                        ),
                        anchor_strategy_router_anchor=route_anchor,
                        anchor_strategy_router_strategy_label=(
                            route.get("strategy_label") if route else None
                        ),
                        anchor_strategy_router_strategy_file=(
                            route.get("strategy_file") if route else None
                        )
                    )
                    log_trade_activity(
                        "BUY_ORDER_PLACED",
                        mode="live",
                        cycle_id=cycle_id,
                        txid=txid,
                        volume=round(volume, VOLUME_DECIMALS),
                        price=round(level, PRICE_DECIMALS),
                        trade_notional_usd=round(level * volume, 8),
                        buy_source=buy_source,
                        sell_pct_override=active_sell_pct_override,
                        above_last_sell_breakout_bypass=(
                            above_last_sell_breakout_bypass
                        ),
                        weather_leveling_high_band_size_multiplier=(
                            leveling_size_multiplier
                        ),
                        weather_leveling_bypass_blocked=(
                            leveling_bypass_blocked
                        ),
                        effective_position_size_pct=(
                            candidate_effective_position_size_pct
                        ),
                        risk_context_position_sizing_enabled=(
                            risk_context_size_adjustment["enabled"]
                        ),
                        risk_context_position_size_effective_multiplier=(
                            risk_context_size_multiplier
                        ),
                        **sentiment_risk_fields,
                        anchor_strategy_router_enabled=(
                            anchor_strategy_router_enabled
                        ),
                        anchor_strategy_router_anchor=route_anchor,
                        anchor_strategy_router_strategy_label=(
                            route.get("strategy_label") if route else None
                        ),
                        anchor_strategy_router_strategy_file=(
                            route.get("strategy_file") if route else None
                        )
                    )
                    notify_order_tracker(
                        trade_id=txid,
                        side="buy",
                        price=round(level, PRICE_DECIMALS),
                        quantity=round(volume, VOLUME_DECIMALS),
                        order_id=txid,
                        timestamp=cycle_id,
                        notes=buy_source
                    )
            else:
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    operating_mode=operating_mode,
                    execution_signal=execution_signal,
                    threshold=execution_signal_threshold,
                    llm_target_min_signal=llm_target_min_signal,
                    low_min_signal=low_min_signal,
                    mean_min_signal=mean_min_signal,
                    median_min_signal=median_min_signal,
                    high_min_signal=high_min_signal,
                    schema_version=sentiment_payload.get("schema_version"),
                    sentiment_source=sentiment_payload.get("source"),
                    sentiment_processed_at=sentiment_payload.get("processed_at"),
                    signal_status=signal_status,
                    freshness_allows_trading=freshness_allows_trading,
                    freshness_block_reason=freshness_block_reason,
                    signal_allows_trading=any_buys_allowed,
                    llm_buys_allowed=llm_buys_allowed,
                    range_core_buys_allowed=range_core_buys_allowed,
                    range_high_buys_allowed=range_high_buys_allowed,
                    range_buys_allowed=range_buys_allowed,
                    range_fallback_active=range_fallback_active,
                    sentiment_control_mode=sentiment_control_mode,
                    source_guard_allows_trading=source_guard_allows_trading,
                    runtime_block_reason=runtime_block_reason,
                    realized_pnl_today=round(realized_pnl_today, 8),
                    sell_backlog_count=sell_backlog["count"],
                    sell_backlog_oldest_minutes=round(
                        sell_backlog["oldest_age_minutes"],
                        2
                    ),
                    action_recommendation=action_recommendation,
                    action_policy_reason=action_policy.get("reason"),
                    **sentiment_risk_fields,
                    contributor_count=sentiment_payload.get("contributor_count"),
                    active_observation_count=sentiment_payload.get(
                        "active_observation_count"
                    ),
                    external_block_reason=external_block_reason,
                    smoothed_risk_multiplier=smoothed_risk_multiplier,
                    risk_context_position_sizing_enabled=(
                        risk_context_size_adjustment["enabled"]
                    ),
                    risk_context_position_size_raw_multiplier=(
                        risk_context_size_adjustment["raw_multiplier"]
                    ),
                    risk_context_position_size_clamped_multiplier=(
                        risk_context_size_adjustment["clamped_multiplier"]
                    ),
                    risk_context_position_size_blend=(
                        risk_context_size_adjustment["blend"]
                    ),
                    risk_context_position_size_effective_multiplier=(
                        risk_context_size_multiplier
                    ),
                    flow_pressure=flow_pressure,
                    mean_reversion_opportunity=mean_reversion_opportunity,
                    effective_entry_step_pct=current_entry_step_pct,
                    inventory_pressure_usage_ratio=inventory_pressure_usage_ratio,
                    inventory_pressure_size_multiplier=(
                        inventory_pressure_size_multiplier
                    ),
                    effective_position_size_pct=effective_position_size_pct,
                    range_low=low,
                    range_high=high,
                    range_mean=mean,
                    range_median=median,
                    sentiment_regime=regime["name"],
                    reason=(
                        (
                            f"operating_mode_{operating_mode}"
                            if not operating_mode_allows_buy_execution(
                                operating_mode
                            )
                            else "buy_modes_disabled"
                        )
                        if not active_strategy_modes
                        else
                        (
                            None
                            if any_buys_allowed
                            else (
                                runtime_block_reason
                                or f"action_recommendation_{action_recommendation}"
                            )
                        )
                        or (
                            None
                            if range_fallback_active
                            else freshness_block_reason
                        )
                        or external_block_reason
                        or "signal_below_threshold_or_range_unavailable"
                    ),
                    cycle_id=cycle_id,
                    price_regime_range_position=price_regime_range_position,
                    price_regime_volatility_pct=price_regime.get(
                        "realized_volatility_24h_pct"
                    )
                )
                actions.append("hold")

            cycle_high_anchor_exposure = high_anchor_backlog_exposure(now)
            log_event(
                "CYCLE_SUMMARY",
                cycle_id=cycle_id,
                price=price,
                operating_mode=operating_mode,
                execution_signal=execution_signal,
                threshold=execution_signal_threshold,
                llm_target_min_signal=llm_target_min_signal,
                low_min_signal=low_min_signal,
                mean_min_signal=mean_min_signal,
                median_min_signal=median_min_signal,
                high_min_signal=high_min_signal,
                schema_version=sentiment_payload.get("schema_version"),
                sentiment_source=sentiment_payload.get("source"),
                sentiment_processed_at=sentiment_payload.get("processed_at"),
                signal_status=signal_status,
                freshness_allows_trading=freshness_allows_trading,
                freshness_block_reason=freshness_block_reason,
                signal_allows_trading=any_buys_allowed,
                llm_buys_allowed=llm_buys_allowed,
                range_core_buys_allowed=range_core_buys_allowed,
                range_high_buys_allowed=range_high_buys_allowed,
                range_buys_allowed=range_buys_allowed,
                range_fallback_active=range_fallback_active,
                sentiment_control_mode=sentiment_control_mode,
                source_guard_allows_trading=source_guard_allows_trading,
                runtime_block_reason=runtime_block_reason,
                realized_pnl_today=round(realized_pnl_today, 8),
                sell_backlog_count=sell_backlog["count"],
                sell_backlog_oldest_minutes=round(
                    sell_backlog["oldest_age_minutes"],
                    2
                ),
                action_recommendation=action_recommendation,
                action_policy_reason=action_policy.get("reason"),
                **sentiment_risk_fields,
                contributor_count=sentiment_payload.get("contributor_count"),
                active_observation_count=sentiment_payload.get(
                    "active_observation_count"
                ),
                external_block_reason=external_block_reason,
                smoothed_risk_multiplier=smoothed_risk_multiplier,
                risk_context_position_sizing_enabled=(
                    risk_context_size_adjustment["enabled"]
                ),
                risk_context_position_size_raw_multiplier=(
                    risk_context_size_adjustment["raw_multiplier"]
                ),
                risk_context_position_size_clamped_multiplier=(
                    risk_context_size_adjustment["clamped_multiplier"]
                ),
                risk_context_position_size_blend=(
                    risk_context_size_adjustment["blend"]
                ),
                risk_context_position_size_effective_multiplier=(
                    risk_context_size_multiplier
                ),
                flow_pressure=flow_pressure,
                mean_reversion_opportunity=mean_reversion_opportunity,
                effective_entry_step_pct=current_entry_step_pct,
                inventory_pressure_usage_ratio=inventory_pressure_usage_ratio,
                inventory_pressure_size_multiplier=(
                    inventory_pressure_size_multiplier
                ),
                range_low=low,
                range_high=high,
                range_mean=mean,
                range_median=median,
                price_regime_timestamp=price_regime.get("timestamp"),
                price_regime_range_position=price_regime_range_position,
                price_regime_volatility_pct=price_regime.get(
                    "realized_volatility_24h_pct"
                ),
                price_regime_mean_reversion_buy_target=price_regime.get(
                    "mean_reversion_buy_target"
                ),
                price_regime_median_reversion_buy_target=price_regime.get(
                    "median_reversion_buy_target"
                ),
                trend_range_position_change=(
                    trend_snapshot.get("price_regime_trends", {})
                    .get("range_position_24h_change")
                ),
                trend_distance_from_mean_change=(
                    trend_snapshot.get("price_regime_trends", {})
                    .get("distance_from_mean_pct_change")
                ),
                kraken_flow_trade_imbalance_pct=kraken_flow.get(
                    "trade_imbalance_pct"
                ),
                sentiment_regime=regime["name"],
                effective_position_size_pct=effective_position_size_pct,
                effective_max_inventory_usd=effective_max_inventory_usd,
                effective_max_open_sell_orders=effective_max_open_sell_orders,
                high_anchor_enabled=effective_high_anchor_enabled,
                weather_high_anchor_allowed=weather_high_anchor_allowed,
                grid_anchor=grid_anchor,
                configured_strategy_modes=configured_strategy_modes,
                buy_source=(
                    "llm_target"
                    if llm_buy_allowed
                    else ",".join(active_strategy_modes) or "disabled"
                ),
                strategy_modes=active_strategy_modes,
                grid_levels=(
                    [
                        round(candidate["level"], PRICE_DECIMALS)
                        for candidate in deduped_candidates
                    ]
                    if (
                        range_signal_gates_allow
                        and base_any_buys_allowed
                        and active_strategy_modes
                        and low
                        and high
                    )
                    else []
                ),
                high_anchor_order_count=cycle_high_anchor_exposure["raw_count"],
                high_anchor_effective_order_count=round(
                    cycle_high_anchor_exposure["effective_count"],
                    4
                ),
                high_anchor_open_buy_count=(
                    cycle_high_anchor_exposure["open_buy_count"]
                ),
                high_anchor_fresh_sell_count=(
                    cycle_high_anchor_exposure["fresh_sell_count"]
                ),
                high_anchor_aged_sell_count=(
                    cycle_high_anchor_exposure["aged_sell_count"]
                ),
                high_anchor_backlog_soft_release_minutes=(
                    cycle_high_anchor_exposure["soft_release_minutes"]
                ),
                high_anchor_backlog_old_order_weight=(
                    cycle_high_anchor_exposure["old_order_weight"]
                ),
                high_anchor_cooldown_remaining_minutes=round(
                    high_anchor_cooldown_remaining_minutes(now),
                    2
                ),
                deployed_inventory_usd=round(
                    current_inventory_usd(price), 8
                ),
                open_buy_count=len(state["open_buy_orders"]),
                open_sell_count=len(state["open_sell_orders"]),
                open_buy_volume=sum(
                    order.get("volume", 0) or 0
                    for order in state["open_buy_orders"].values()
                ),
                open_sell_volume=sum(
                    order.get("volume", 0) or 0
                    for order in state["open_sell_orders"].values()
                ),
                inventory_buckets_usd={
                    bucket: round(value, 8)
                    for bucket, value in inventory_usd_by_bucket(price).items()
                },
                buy_orders_placed=state["stats"]["buy_orders_placed"],
                buy_orders_filled=state["stats"]["buy_orders_filled"],
                sell_orders_placed=state["stats"]["sell_orders_placed"],
                sell_orders_filled=state["stats"]["sell_orders_filled"],
                realized_gross_pnl=round(
                    state["stats"]["realized_gross_pnl"], 8
                ),
                realized_estimated_net_pnl=round(
                    state["stats"]["realized_estimated_net_pnl"], 8
                ),
                actions=actions or ["no_action"]
            )
            execution_quality = build_execution_quality_snapshot(
                state["stats"]
            )
            emit_execution_quality_alerts(
                cycle_id=cycle_id,
                stats=state["stats"],
                execution_quality=execution_quality,
                sell_backlog=sell_backlog,
            )
            write_status_snapshot({
                "timestamp": cycle_id,
                "operating_mode": operating_mode,
                "sentiment_control_mode": sentiment_control_mode,
                "strategy_profile": STRATEGY_PROFILE,
                "grid_anchor": grid_anchor,
                "configured_strategy_modes": configured_strategy_modes,
                "strategy_modes": active_strategy_modes,
                "dynamic_anchor_mode": strategy_bool(
                    strategy_config,
                    "dynamic_anchor_mode",
                    False
                ),
                "price_regime_range_position": price_regime_range_position,
                "effective_entry_step_pct": current_entry_step_pct,
                "inventory_pressure_usage_ratio": (
                    inventory_pressure_usage_ratio
                ),
                "inventory_pressure_size_multiplier": (
                    inventory_pressure_size_multiplier
                ),
                "price": price,
                "execution_signal": execution_signal,
                "signal_status": signal_status,
                "action_recommendation": action_recommendation,
                "runtime_block_reason": runtime_block_reason,
                "effective_position_size_pct": effective_position_size_pct,
                "effective_max_inventory_usd": effective_max_inventory_usd,
                "effective_max_open_sell_orders": (
                    effective_max_open_sell_orders
                ),
                "high_anchor_enabled": effective_high_anchor_enabled,
                "weather_high_anchor_allowed": weather_high_anchor_allowed,
                "realized_pnl_today": round(realized_pnl_today, 8),
                "sell_backlog_count": sell_backlog["count"],
                "sell_backlog_oldest_minutes": round(
                    sell_backlog["oldest_age_minutes"],
                    2
                ),
                "range_fallback_active": range_fallback_active,
                "open_buy_count": len(state["open_buy_orders"]),
                "open_sell_count": len(state["open_sell_orders"]),
                "deployed_inventory_usd": round(current_inventory_usd(price), 8),
                "inventory_buckets_usd": {
                    bucket: round(value, 8)
                    for bucket, value in inventory_usd_by_bucket(price).items()
                },
                **weather_status_fields(risk_context),
                "stats": {
                    "approved_buy_candidates": state["stats"][
                        "approved_buy_candidates"
                    ],
                    "buy_orders_placed": state["stats"]["buy_orders_placed"],
                    "buy_orders_filled": state["stats"]["buy_orders_filled"],
                    "sell_orders_placed": state["stats"]["sell_orders_placed"],
                    "sell_orders_filled": state["stats"]["sell_orders_filled"],
                    "buy_order_rejections": state["stats"][
                        "buy_order_rejections"
                    ],
                    "realized_gross_pnl": round(
                        state["stats"]["realized_gross_pnl"], 8
                    ),
                    "realized_estimated_net_pnl": round(
                        state["stats"]["realized_estimated_net_pnl"], 8
                    ),
                    "approved_counts_by_source": state["stats"].get(
                        "approved_counts_by_source",
                        {}
                    ),
                    "buy_orders_placed_by_source": state["stats"].get(
                        "buy_orders_placed_by_source",
                        {}
                    ),
                    "buy_orders_filled_by_source": state["stats"].get(
                        "buy_orders_filled_by_source",
                        {}
                    ),
                    "sell_orders_placed_by_source": state["stats"].get(
                        "sell_orders_placed_by_source",
                        {}
                    ),
                    "sell_orders_filled_by_source": state["stats"].get(
                        "sell_orders_filled_by_source",
                        {}
                    ),
                },
                "execution_quality": execution_quality,
                "actions": actions or ["no_action"],
            })
            if state.get("consecutive_loop_errors", 0):
                state["consecutive_loop_errors"] = 0
                save_state(state)

            send_checkin(loop_count=loop_count, message="loop_complete")
            time.sleep(price_check_interval_seconds)
        except Exception as e:
            state["consecutive_loop_errors"] = int(
                state.get("consecutive_loop_errors", 0) or 0
            ) + 1
            save_state(state)
            log_event("LOOP_ERROR", message=str(e))
            if (
                max_consecutive_loop_errors > 0
                and state["consecutive_loop_errors"] >= max_consecutive_loop_errors
            ):
                emit_alert(
                    "loop_errors",
                    "critical",
                    "Loop errors exceeded configured threshold",
                    consecutive_loop_errors=state["consecutive_loop_errors"],
                    error=short_error_summary(e)
                )
            console(f"Loop error: {e}")
            send_checkin(
                status="error",
                loop_count=loop_count,
                message=short_error_summary(e)
            )
            time.sleep(price_check_interval_seconds)


if __name__ == "__main__":
    main()
