"""Shared entry-placement policy for live range-grid and replay.

The legacy ``triggered`` mode waits until price reaches a calculated level
before submitting the limit order.  ``resting_grid`` submits a nearby level in
advance so low/median entries behave like an actual grid instead of a market
timing trigger.
"""


VALID_ENTRY_PLACEMENT_MODES = {
    "triggered",
    "resting_grid",
}

RESTING_GRID_BUY_SOURCES = {
    "range_low",
    "range_mean",
    "range_median",
}


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _source_map_value(config, field, buy_source):
    if not isinstance(config, dict):
        return None
    values = config.get(field)
    if not isinstance(values, dict):
        return None
    source_key = str(buy_source or "").strip().lower()
    for source, value in values.items():
        if str(source or "").strip().lower() == source_key:
            return value
    return None


def entry_placement_mode(config, buy_source, fallback_config=None):
    """Return the source's placement mode, defaulting to legacy triggering."""

    source_key = str(buy_source or "").strip().lower()
    for candidate_config in (config, fallback_config):
        raw_mode = _source_map_value(
            candidate_config,
            "entry_placement_mode_by_source",
            source_key,
        )
        if raw_mode is None:
            continue
        mode = str(raw_mode or "").strip().lower()
        if mode in VALID_ENTRY_PLACEMENT_MODES:
            return mode
    return "triggered"


def resting_grid_max_above_level_pct(
    config,
    buy_source,
    fallback_config=None,
):
    """Return how far above a level price may be when resting it."""

    for candidate_config in (config, fallback_config):
        raw_value = _source_map_value(
            candidate_config,
            "resting_grid_max_above_level_pct_by_source",
            buy_source,
        )
        numeric = _safe_float(raw_value)
        if numeric is not None:
            return max(0.0, numeric)
    return 0.0


def entry_price_placement_decision(
    price,
    level,
    config,
    buy_source,
    *,
    fallback_config=None,
    triggered_tolerance_pct=0.0,
):
    """Decide whether a calculated level may be submitted at ``price``."""

    source = str(buy_source or "").strip().lower()
    mode = entry_placement_mode(config, source, fallback_config)
    price = _safe_float(price)
    level = _safe_float(level)
    triggered_tolerance_pct = max(
        0.0,
        _safe_float(triggered_tolerance_pct) or 0.0,
    )

    if source == "llm_target":
        return {
            "allowed": True,
            "reason": None,
            "mode": "triggered",
            "max_above_level_pct": triggered_tolerance_pct,
            "above_level_pct": 0.0,
        }

    if source not in RESTING_GRID_BUY_SOURCES:
        triggered_tolerance_pct = 0.0

    if price is None or level is None or price <= 0 or level <= 0:
        return {
            "allowed": False,
            "reason": "invalid_entry_price",
            "mode": mode,
            "max_above_level_pct": 0.0,
            "above_level_pct": None,
        }

    above_level_pct = max(0.0, (price / level) - 1.0)
    if mode == "resting_grid":
        max_above_level_pct = resting_grid_max_above_level_pct(
            config,
            source,
            fallback_config,
        )
        allowed = source in RESTING_GRID_BUY_SOURCES and (
            price <= level * (1.0 + max_above_level_pct)
        )
        return {
            "allowed": allowed,
            "reason": (
                None
                if allowed
                else "resting_grid_price_too_far_above_level"
            ),
            "mode": mode,
            "max_above_level_pct": max_above_level_pct,
            "above_level_pct": above_level_pct,
        }

    allowed = price <= level * (1.0 + triggered_tolerance_pct)
    return {
        "allowed": allowed,
        "reason": None if allowed else "price_above_level",
        "mode": mode,
        "max_above_level_pct": triggered_tolerance_pct,
        "above_level_pct": above_level_pct,
    }
