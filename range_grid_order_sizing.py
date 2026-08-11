"""Shared order-sizing helpers for live range-grid and backtest execution."""

from datetime import datetime


def _strategy_bool(config, key, default=False):
    value = (config or {}).get(key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _strategy_float(config, key, default):
    try:
        value = (config or {}).get(key, default)
        return float(default if value is None else value)
    except (TypeError, ValueError):
        return float(default)


def _source_tokens(value):
    if isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = str(value or "").split(",")
    return {
        str(item or "").strip().lower()
        for item in values
        if str(item or "").strip()
    }


def _parse_timestamp(value):
    if isinstance(value, datetime):
        return value
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def minimum_order_floor_decision(
    config,
    *,
    buy_source,
    available_usd,
    calculated_notional_usd,
    min_buy_notional_usd,
    order_price=None,
    min_buy_volume_asset=0.0,
    now=None,
    last_floor_at_by_source=None,
):
    """Decide whether a sub-minimum approved order may be rounded up safely.

    This helper never approves a trading candidate. It only adjusts sizing after
    all of the caller's normal entry gates have approved the candidate.
    """

    available_usd = max(0.0, float(available_usd or 0.0))
    calculated_notional_usd = max(
        0.0,
        float(calculated_notional_usd or 0.0),
    )
    min_buy_notional_usd = max(
        0.0,
        float(min_buy_notional_usd or 0.0),
    )
    order_price = max(0.0, float(order_price or 0.0))
    min_buy_volume_asset = max(
        0.0,
        float(min_buy_volume_asset or 0.0),
    )
    enabled = _strategy_bool(config, "minimum_order_floor_enabled", False)
    sources = _source_tokens(
        (config or {}).get("minimum_order_floor_sources", "range_low")
    )
    source = str(buy_source or "").strip().lower()
    configured_floor = _strategy_float(
        config,
        "minimum_order_floor_usd",
        min_buy_notional_usd,
    )
    min_volume_notional_usd = order_price * min_buy_volume_asset
    floor_notional_usd = max(
        min_buy_notional_usd,
        configured_floor,
        min_volume_notional_usd,
    )
    reserve_usd = max(
        0.0,
        _strategy_float(config, "minimum_order_floor_cash_reserve_usd", 0.0),
    )
    cooldown_minutes = max(
        0.0,
        _strategy_float(config, "minimum_order_floor_cooldown_minutes", 0.0),
    )

    details = {
        "enabled": enabled,
        "eligible_source": source in sources,
        "applied": False,
        "reason": None,
        "calculated_notional_usd": calculated_notional_usd,
        "floor_notional_usd": floor_notional_usd,
        "min_volume_notional_usd": min_volume_notional_usd,
        "min_buy_volume_asset": min_buy_volume_asset,
        "cash_reserve_usd": reserve_usd,
        "cooldown_minutes": cooldown_minutes,
        "cooldown_remaining_minutes": 0.0,
    }

    if calculated_notional_usd >= floor_notional_usd:
        details["reason"] = "not_needed"
        return details
    if not enabled:
        details["reason"] = "disabled"
        return details
    if source not in sources:
        details["reason"] = "source_not_enabled"
        return details
    if floor_notional_usd <= 0:
        details["reason"] = "invalid_floor"
        return details
    if available_usd < floor_notional_usd:
        details["reason"] = "insufficient_available_usd"
        return details
    if available_usd - floor_notional_usd < reserve_usd:
        details["reason"] = "cash_reserve"
        return details

    last_floor_map = (
        last_floor_at_by_source
        if isinstance(last_floor_at_by_source, dict)
        else {}
    )
    current_time = _parse_timestamp(now)
    last_floor_at = _parse_timestamp(last_floor_map.get(source))
    if (
        current_time is not None
        and last_floor_at is not None
        and cooldown_minutes > 0
    ):
        elapsed_minutes = (current_time - last_floor_at).total_seconds() / 60.0
        cooldown_remaining = max(0.0, cooldown_minutes - elapsed_minutes)
        details["cooldown_remaining_minutes"] = cooldown_remaining
        if cooldown_remaining > 0:
            details["reason"] = "cooldown"
            return details

    details["applied"] = True
    details["reason"] = "minimum_order_floor"
    return details
