#!/usr/bin/env python3


def numeric_or_none(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def strategy_bool(config, key, default=False):
    value = config.get(key) if isinstance(config, dict) else None
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def normalized_sources(value):
    if isinstance(value, str):
        values = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        values = value
    else:
        values = []
    return {
        str(source or "").strip().lower()
        for source in values
        if str(source or "").strip()
    }


def fear_greed_profit_target_policy(config):
    config = config if isinstance(config, dict) else {}
    max_by_source = config.get(
        "fear_greed_profit_target_max_multiplier_by_source"
    )
    if not isinstance(max_by_source, dict):
        max_by_source = {}
    return {
        "fear_greed_profit_target_enabled": strategy_bool(
            config,
            "fear_greed_profit_target_enabled",
            False,
        ),
        "fear_greed_profit_target_sources": sorted(normalized_sources(
            config.get("fear_greed_profit_target_sources")
        )),
        "fear_greed_profit_target_greed_start_index": numeric_or_none(
            config.get("fear_greed_profit_target_greed_start_index")
        ),
        "fear_greed_profit_target_full_greed_index": numeric_or_none(
            config.get("fear_greed_profit_target_full_greed_index")
        ),
        "fear_greed_profit_target_max_multiplier": numeric_or_none(
            config.get("fear_greed_profit_target_max_multiplier")
        ),
        "fear_greed_profit_target_max_multiplier_by_source": {
            str(source or "").strip().lower(): numeric_or_none(value)
            for source, value in max_by_source.items()
        },
    }


def fear_greed_profit_target_adjustment(
    config,
    *,
    buy_source,
    fear_greed_index,
    base_profit_target_pct,
):
    policy = fear_greed_profit_target_policy(config)
    base_target = max(0.0, numeric_or_none(base_profit_target_pct) or 0.0)
    source = str(buy_source or "").strip().lower()
    sources = set(policy["fear_greed_profit_target_sources"])
    index = numeric_or_none(fear_greed_index)
    greed_start = policy["fear_greed_profit_target_greed_start_index"]
    full_greed = policy["fear_greed_profit_target_full_greed_index"]
    default_max = policy["fear_greed_profit_target_max_multiplier"]
    source_max = policy[
        "fear_greed_profit_target_max_multiplier_by_source"
    ].get(source)

    greed_start = 50.0 if greed_start is None else greed_start
    full_greed = 75.0 if full_greed is None else full_greed
    max_multiplier = (
        source_max
        if source_max is not None
        else (default_max if default_max is not None else 1.0)
    )
    max_multiplier = max(1.0, max_multiplier)

    reason = "applied"
    multiplier = 1.0
    progress = 0.0
    if not policy["fear_greed_profit_target_enabled"]:
        reason = "disabled"
    elif source not in sources:
        reason = "source_not_enabled"
    elif index is None:
        reason = "missing_fear_greed_index"
    elif full_greed <= greed_start:
        reason = "invalid_index_range"
    elif max_multiplier <= 1.0:
        reason = "multiplier_not_above_one"
    else:
        normalized_index = max(0.0, min(index, 100.0))
        progress = max(
            0.0,
            min(
                (normalized_index - greed_start) / (full_greed - greed_start),
                1.0,
            ),
        )
        multiplier = 1.0 + (progress * (max_multiplier - 1.0))
        if progress <= 0:
            reason = "below_greed_start"

    effective_target = base_target * multiplier
    return {
        "enabled": policy["fear_greed_profit_target_enabled"],
        "applied": multiplier > 1.0,
        "reason": reason,
        "buy_source": source,
        "fear_greed_index": index,
        "greed_start_index": greed_start,
        "full_greed_index": full_greed,
        "greed_progress": progress,
        "max_multiplier": max_multiplier,
        "multiplier": multiplier,
        "base_profit_target_pct": base_target,
        "effective_profit_target_pct": effective_target,
        "policy": policy,
    }
