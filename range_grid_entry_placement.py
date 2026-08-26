"""Shared entry-placement policy for live range-grid and replay.

The legacy ``triggered`` mode waits until price reaches a calculated level
before submitting the limit order.  ``resting_grid`` submits a nearby level in
advance so low/median entries behave like an actual grid instead of a market
timing trigger.
"""

import math


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


def entry_grid_levels(
    anchor,
    step_pct,
    grid_size,
    config,
    buy_source,
    *,
    fallback_config=None,
):
    """Build anchor-based levels with semantics matching placement mode.

    Triggered entries preserve the legacy behavior where the first level is
    one step below the anchor.  A resting grid includes the anchor as its
    first level so it can actually be placed in advance of a price crossing.
    """

    mode = entry_placement_mode(config, buy_source, fallback_config)
    first_step = 0 if mode == "resting_grid" else 1
    return sorted(
        [
            anchor * (1 - (step_pct * (index + first_step)))
            for index in range(grid_size)
        ],
        reverse=True,
    )


def entry_grid_slot(
    buy_source,
    level,
    step_pct,
    grid_depth,
    config,
    *,
    fallback_config=None,
):
    """Return a stable slot identity for an entry-grid level.

    Legacy triggered grids retain source-and-depth identities. Resting grids
    use a logarithmic price band whose width matches the configured entry
    step. This lets a materially lower band trade while an older sell waits,
    without allowing rolling-anchor drift inside the same band to duplicate
    the position.
    """

    source = str(buy_source or "").strip().lower()
    depth = max(1, int(grid_depth))
    mode = entry_placement_mode(config, source, fallback_config)
    if mode != "resting_grid":
        return f"{source}:{depth}"

    numeric_level = _safe_float(level)
    numeric_step = _safe_float(step_pct)
    if (
        numeric_level is None
        or numeric_level <= 0
        or numeric_step is None
        or numeric_step <= 0
        or numeric_step >= 1
    ):
        return f"{source}:price_band:invalid:{depth}"

    band_width = -math.log1p(-numeric_step)
    band_index = math.floor(math.log(numeric_level) / band_width)
    return f"{source}:price_band:{band_index}"


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


def resting_grid_near_touch_offset_pct(
    config,
    buy_source,
    fallback_config=None,
):
    """Return the configured top-rung discount from the current price.

    A missing value keeps the legacy behavior where the range anchor is also
    the submitted limit price.  Near-touch pricing is deliberately opt-in per
    source so existing strategies and live orders do not change implicitly.
    """

    for candidate_config in (config, fallback_config):
        raw_value = _source_map_value(
            candidate_config,
            "resting_grid_near_touch_offset_pct_by_source",
            buy_source,
        )
        numeric = _safe_float(raw_value)
        if numeric is not None:
            return max(0.0, numeric)
    return 0.0


def entry_order_price_decision(
    price,
    level,
    config,
    buy_source,
    *,
    grid_depth=1,
    fallback_config=None,
):
    """Choose the submitted buy price after an anchor passes eligibility.

    The range level remains the safety/slot anchor.  When a resting-grid
    source opts in, only its first rung is moved near the current price.  The
    deeper rungs remain at their anchor prices, preserving the actual grid.
    Live callers submit an applied near-touch price as post-only.
    """

    source = str(buy_source or "").strip().lower()
    mode = entry_placement_mode(config, source, fallback_config)
    numeric_price = _safe_float(price)
    numeric_level = _safe_float(level)
    try:
        depth = max(1, int(grid_depth))
    except (TypeError, ValueError):
        depth = 1
    offset_pct = resting_grid_near_touch_offset_pct(
        config,
        source,
        fallback_config,
    )
    applied = (
        mode == "resting_grid"
        and source in RESTING_GRID_BUY_SOURCES
        and depth == 1
        and 0.0 < offset_pct < 1.0
        and numeric_price is not None
        and numeric_price > 0
        and numeric_level is not None
        and numeric_level > 0
    )
    order_price = (
        numeric_price * (1.0 - offset_pct)
        if applied
        else numeric_level
    )
    return {
        "anchor_level": numeric_level,
        "order_price": order_price,
        "near_touch_enabled": offset_pct > 0.0,
        "near_touch_applied": applied,
        "near_touch_offset_pct": offset_pct,
        "post_only": applied,
        "grid_depth": depth,
    }


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
