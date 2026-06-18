#!/usr/bin/env python3

import json
import os
import statistics
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


def now_utc():
    return datetime.now(timezone.utc)


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


def snapshot_timestamp(snapshot):
    return parse_iso8601(snapshot.get("captured_at"))


def signal_payload(snapshot):
    signal = snapshot.get("signal") or {}
    payload = signal.get("payload")
    if not isinstance(payload, dict):
        payload = signal.get("raw_payload")
    if not isinstance(payload, dict):
        return {}
    return normalize_signal_payload(payload)


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


def positive_or_default(value, default=None):
    numeric = safe_float(value)
    if numeric is None:
        return default
    return numeric


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
):
    normalized = (action_recommendation or "neutral").strip().lower()
    sentiment_control_mode = normalize_sentiment_control_mode(
        sentiment_control_mode,
        operating_mode,
    )
    llm_buys_allowed = normalized == "bullish_allowed"
    range_core_buys_allowed = normalized in (
        "bullish_allowed",
        "neutral",
        "watch_only",
    )
    range_high_buys_allowed = range_core_buys_allowed
    if (
        not range_core_buys_allowed
        and allow_range_buy_on_confidence_block
        and operating_mode == "range_only"
        and is_liquidity_confidence_block(action_recommendation, action_policy)
    ):
        range_core_buys_allowed = True
        range_high_buys_allowed = True
    elif (
        normalized == "blocked"
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
    configured_profit_target = safe_float(
        strategy_payload(snapshot).get("profit_target_pct")
    ) or 0.0
    if event.get("buy_source") == "range_high_band":
        return safe_float(
            strategy_payload(snapshot).get("high_anchor_profit_target_pct")
        ) or configured_profit_target
    return safe_float(event.get("sell_pct_override")) or configured_profit_target


def infer_live_only_blockers(snapshot, event):
    blockers = []
    config = strategy_payload(snapshot)
    state_info = state_summary(snapshot)
    runtime_status = runtime_status_summary(snapshot)
    captured_at = snapshot_timestamp(snapshot)

    runtime_block_reason = runtime_status.get("runtime_block_reason")
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
    if effective_position_size_pct is not None and effective_position_size_pct <= 0:
        blockers.append("effective_position_size_pct_zero")

    effective_max_inventory_usd = safe_float(
        runtime_status.get("effective_max_inventory_usd")
    )
    if (
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
    buy_permissions = sentiment_buy_permissions(
        action_recommendation,
        signal.get("action_policy"),
        operating_mode=operating_mode,
        allow_range_buy_on_confidence_block=(
            allow_range_buy_on_confidence_block
        ),
        sentiment_control_mode=sentiment_control_mode,
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
    if buy_source != "llm_target" and price > level:
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
                "hold_reason": built["hold_reason"],
                "raw_candidate_count": len(built["raw_candidates"]),
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
                    "buy_source": candidate["buy_source"],
                    "strategy_mode": candidate["strategy_mode"],
                    "level": round(candidate["level"], 2),
                    "sell_pct_override": candidate.get("sell_pct_override"),
                    "status": "approved_gate_only",
                    "reason": None,
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
                    "buy_source": candidate["buy_source"],
                    "strategy_mode": candidate["strategy_mode"],
                    "level": round(candidate["level"], 2),
                    "status": "blocked_gate_only",
                    "reason": reason,
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
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0,
        "buy_orders_placed_by_source": {},
        "buy_filled_by_source": {},
        "sell_filled_by_source": {},
        "rejected_by_side": {},
        "recent_fills": [],
        "recent_buy_orders": [],
    }
    buy_orders_placed_by_source = Counter()
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
    summary["buy_filled_by_source"] = dict(buy_filled_by_source.most_common())
    summary["sell_filled_by_source"] = dict(sell_filled_by_source.most_common())
    summary["rejected_by_side"] = dict(rejected_by_side.most_common())
    summary["realized_gross_pnl"] = round(summary["realized_gross_pnl"], 8)
    summary["realized_estimated_net_pnl"] = round(summary["realized_estimated_net_pnl"], 8)
    summary["average_hold_minutes"] = round(statistics.mean(hold_minutes), 2) if hold_minutes else None
    summary["recent_fills"] = summary["recent_fills"][-BACKTEST_RECENT_LIMIT:]
    summary["recent_buy_orders"] = summary["recent_buy_orders"][-BACKTEST_RECENT_LIMIT:]
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


def build_report():
    now = now_utc()
    since_dt = now - timedelta(hours=BACKTEST_WINDOW_HOURS)
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

    all_events = load_jsonl(TRADE_LOG_FILE)
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

    return {
        "timestamp": now.isoformat(),
        "snapshot_file_base": os.path.abspath(SNAPSHOT_LOG_FILE),
        "snapshot_files": snapshot_files,
        "trade_log_file": os.path.abspath(TRADE_LOG_FILE),
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

    return archive_file


def main():
    report = build_report()
    archive_file = write_report(report)
    print(json.dumps({
        "timestamp": report["timestamp"],
        "output_file": BACKTEST_OUTPUT_FILE,
        "archive_file": archive_file,
        "snapshot_count": report["snapshot_count"],
        "trade_event_count": report["trade_event_count"],
    }))


if __name__ == "__main__":
    main()
