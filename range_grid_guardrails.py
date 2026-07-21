from datetime import datetime, timezone


VALID_OPERATING_MODES = {
    "range_plus_llm",
    "range_only",
    "sell_only",
    "observe_only",
}

VALID_GRID_MODE_TOKENS = {
    "low",
    "mean",
    "median",
    "high",
    "llm",
    "sentiment",
    "llm_target",
    "false",
    "none",
    "off",
    "disabled",
    "no",
    "",
}

VALID_BUY_SOURCES = {
    "llm_target",
    "range_low",
    "range_mean",
    "range_median",
    "range_high_band",
}


def parse_iso8601(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def validate_strategy_config(strategy_config):
    errors = []

    if not isinstance(strategy_config, dict):
        return ["strategy_config must be a JSON object"]

    grid_anchor = str(strategy_config.get("grid_anchor", "low") or "").strip().lower()
    invalid_tokens = [
        token.strip().lower()
        for token in grid_anchor.split(",")
        if token.strip().lower() not in VALID_GRID_MODE_TOKENS
    ]
    if invalid_tokens:
        errors.append(
            f"grid_anchor contains unsupported modes: {', '.join(sorted(set(invalid_tokens)))}"
        )

    operating_mode = str(
        strategy_config.get("operating_mode", "range_plus_llm") or ""
    ).strip().lower()
    if operating_mode not in VALID_OPERATING_MODES:
        errors.append(
            "operating_mode must be one of "
            "range_plus_llm, range_only, sell_only, observe_only"
        )

    positive_numeric_fields = (
        "range_window_hours",
        "max_grid_size",
        "profit_target_pct",
        "entry_step_pct",
        "round_trip_fee_pct",
        "position_size_pct",
        "min_buy_notional_usd",
        "min_buy_volume_btc",
        "price_check_interval_seconds",
        "range_refresh_interval_minutes",
        "max_open_sell_orders",
        "max_inventory_usd",
        "aging_step_minutes",
        "high_anchor_buy_cooldown_minutes",
        "max_open_high_anchor_orders",
    )
    for field in positive_numeric_fields:
        value = strategy_config.get(field)
        if value is None:
            continue
        try:
            numeric = float(value)
        except Exception:
            errors.append(f"{field} must be numeric")
            continue
        if numeric <= 0:
            errors.append(f"{field} must be > 0")

    non_negative_numeric_fields = (
        "llm_target_proximity_pct",
        "momentum_entry_tolerance_pct",
        "aging_profit_reduction_pct",
        "min_profit_target_pct",
        "buy_after_sell_discount_pct",
        "llm_buy_cooldown_minutes_after_sell",
        "buy_cooldown_after_sell_fill_minutes",
        "buy_cooldown_after_sell_fill_high_band_minutes",
        "high_anchor_backlog_soft_release_minutes",
        "sell_backlog_soft_release_minutes",
        "mean_reversion_min_opportunity",
        "stale_level_reanchor_min_above_level_pct",
        "stale_level_reanchor_max_above_level_pct",
        "stale_level_reanchor_profit_lookback_hours",
        "stale_level_reanchor_profit_min_samples",
    )
    for field in non_negative_numeric_fields:
        value = strategy_config.get(field)
        if value is None:
            continue
        try:
            numeric = float(value)
        except Exception:
            errors.append(f"{field} must be numeric")
            continue
        if numeric < 0:
            errors.append(f"{field} must be >= 0")

    value = strategy_config.get("execution_signal_threshold")
    if value is not None:
        try:
            numeric = float(value)
        except Exception:
            errors.append("execution_signal_threshold must be numeric")
        else:
            if numeric < -1 or numeric > 1:
                errors.append("execution_signal_threshold must be between -1 and 1")

    bounded_score_fields = (
        "risk_context_high_band_min_buy_aggression_score",
        "risk_context_high_band_min_breakout_score",
        "risk_context_high_band_min_rebound_score",
        "risk_context_high_band_max_market_risk_score",
        "risk_context_position_size_min_multiplier",
        "risk_context_position_size_max_multiplier",
        "risk_context_position_size_blend",
        "high_anchor_backlog_old_order_weight",
        "sell_backlog_old_order_weight",
        "weather_leveling_high_band_size_threshold",
        "weather_leveling_high_band_size_multiplier",
        "weather_leveling_high_band_bypass_block_threshold",
        "high_band_breakout_bypass_min_breakout_score",
        "high_band_breakout_bypass_min_rebound_score",
        "high_band_breakout_bypass_max_exit_pressure_score",
        "high_band_breakout_bypass_min_hold_through_score",
        "high_band_breakout_bypass_momentum_min_hold_through_score",
        "stale_level_reanchor_min_entry_opportunity_score",
        "stale_level_reanchor_min_rebound_confirmation_score",
        "stale_level_reanchor_max_exit_pressure_score",
        "buy_cooldown_after_sell_fill_weather_min_rebound_confirmation",
        "buy_cooldown_after_sell_fill_weather_min_hold_through",
        "buy_cooldown_after_sell_fill_weather_max_exit_pressure",
    )
    for field in bounded_score_fields:
        value = strategy_config.get(field)
        if value is None:
            continue
        try:
            numeric = float(value)
        except Exception:
            errors.append(f"{field} must be numeric")
            continue
        if numeric < 0 or numeric > 1:
            errors.append(f"{field} must be between 0 and 1")

    try:
        profit_target_pct = float(strategy_config.get("profit_target_pct", 0.01))
        min_profit_target_pct = float(
            strategy_config.get("min_profit_target_pct", profit_target_pct)
        )
        if min_profit_target_pct > profit_target_pct:
            errors.append("min_profit_target_pct cannot exceed profit_target_pct")
    except Exception:
        pass

    try:
        risk_floor = float(strategy_config.get("risk_multiplier_floor", 0.75))
        risk_ceiling = float(strategy_config.get("risk_multiplier_ceiling", 1.15))
        if risk_floor <= 0:
            errors.append("risk_multiplier_floor must be > 0")
        if risk_ceiling < risk_floor:
            errors.append("risk_multiplier_ceiling must be >= risk_multiplier_floor")
    except Exception:
        errors.append("risk_multiplier_floor and risk_multiplier_ceiling must be numeric")

    bucket_caps = strategy_config.get("max_inventory_usd_by_bucket")
    if bucket_caps is not None:
        if not isinstance(bucket_caps, dict):
            errors.append("max_inventory_usd_by_bucket must be an object")
        else:
            for bucket, value in bucket_caps.items():
                try:
                    numeric = float(value)
                except Exception:
                    errors.append(f"max_inventory_usd_by_bucket.{bucket} must be numeric")
                    continue
                if numeric <= 0:
                    errors.append(f"max_inventory_usd_by_bucket.{bucket} must be > 0")

    source_numeric_maps = {
        "sell_target_offset_pct_by_source": ("numeric", None),
        "momentum_entry_tolerance_pct_by_source": ("non_negative", None),
        "aging_start_minutes_by_source": ("positive", None),
        "aging_step_minutes_by_source": ("positive", None),
        "aging_profit_reduction_pct_by_source": ("non_negative", None),
        "min_profit_target_pct_by_source": ("non_negative", None),
        "buy_cooldown_after_sell_fill_minutes_by_source": ("non_negative", None),
    }
    for field, (kind, _) in source_numeric_maps.items():
        value = strategy_config.get(field)
        if value is None:
            continue
        if not isinstance(value, dict):
            errors.append(f"{field} must be an object")
            continue
        for source, raw in value.items():
            normalized_source = str(source or "").strip().lower()
            if normalized_source not in VALID_BUY_SOURCES:
                errors.append(
                    f"{field}.{source} must use a supported source key"
                )
                continue
            try:
                numeric = float(raw)
            except Exception:
                errors.append(f"{field}.{source} must be numeric")
                continue
            if kind == "positive" and numeric <= 0:
                errors.append(f"{field}.{source} must be > 0")
            elif kind == "non_negative" and numeric < 0:
                errors.append(f"{field}.{source} must be >= 0")

    return errors


def summarize_sell_backlog(
    open_sell_orders,
    now=None,
    soft_release_minutes=0,
    old_order_weight=1.0,
):
    now = now or datetime.now(timezone.utc)
    backlog_count = 0
    fresh_count = 0
    aged_count = 0
    effective_count = 0.0
    oldest_age_minutes = 0.0
    soft_release_minutes = max(0.0, float(soft_release_minutes or 0.0))
    old_order_weight = max(0.0, min(float(old_order_weight or 0.0), 1.0))

    for order in open_sell_orders.values():
        backlog_count += 1
        placed_at = parse_iso8601(order.get("placed_at")) if isinstance(order, dict) else None
        age_minutes = 0.0
        if placed_at is None:
            fresh_count += 1
            effective_count += 1.0
        else:
            age_minutes = max(0.0, (now - placed_at).total_seconds() / 60.0)
            if soft_release_minutes > 0 and age_minutes >= soft_release_minutes:
                aged_count += 1
                effective_count += old_order_weight
            else:
                fresh_count += 1
                effective_count += 1.0
        oldest_age_minutes = max(oldest_age_minutes, age_minutes)

    return {
        "count": backlog_count,
        "effective_count": effective_count,
        "fresh_count": fresh_count,
        "aged_count": aged_count,
        "oldest_age_minutes": oldest_age_minutes,
        "soft_release_minutes": soft_release_minutes,
        "old_order_weight": old_order_weight,
    }


def runtime_buy_block_reason(
    *,
    operating_mode,
    realized_pnl_today,
    max_daily_loss_usd,
    sell_backlog_count,
    sell_backlog_limit,
    sell_backlog_oldest_minutes,
    sell_backlog_minutes_limit,
    consecutive_loop_errors,
    max_consecutive_loop_errors,
    consecutive_private_api_failures,
    max_consecutive_private_api_failures,
):
    if operating_mode not in ("range_plus_llm", "range_only"):
        return f"operating_mode_{operating_mode}"

    if max_daily_loss_usd > 0 and realized_pnl_today <= -abs(max_daily_loss_usd):
        return "max_daily_loss_usd"

    if (
        sell_backlog_limit > 0
        and sell_backlog_count >= sell_backlog_limit
    ):
        return "sell_backlog_count"

    if (
        sell_backlog_minutes_limit > 0
        and sell_backlog_oldest_minutes >= sell_backlog_minutes_limit
    ):
        return "sell_backlog_age_minutes"

    if (
        max_consecutive_loop_errors > 0
        and consecutive_loop_errors >= max_consecutive_loop_errors
    ):
        return "consecutive_loop_errors"

    if (
        max_consecutive_private_api_failures > 0
        and consecutive_private_api_failures >= max_consecutive_private_api_failures
    ):
        return "consecutive_private_api_failures"

    return None
