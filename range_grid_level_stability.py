"""Stateful, execution-facing stabilization for range-grid market levels.

The sentiment engine should remain free to publish responsive raw market
structure.  This module gives the trading process a slower, persistent view of
that structure without changing the raw signal.  It is intentionally usable in
both the live bot and the snapshot replay.
"""

from datetime import datetime, timezone
import statistics


def _float(value, default=None):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed


def _bool(config, key, default=False):
    value = (config or {}).get(key, default)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _config_float(config, key, default):
    parsed = _float((config or {}).get(key))
    return float(default) if parsed is None else parsed


def _moment(value):
    if isinstance(value, datetime):
        parsed = value
    elif value:
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    else:
        parsed = None
    if parsed is None:
        parsed = datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_minutes(value, now):
    if not value:
        return 0.0
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return max(0.0, (now - parsed.astimezone(timezone.utc)).total_seconds() / 60.0)


def _raw_level(level):
    if not isinstance(level, dict):
        return None
    price = _float(level.get("price"))
    if price is None or price <= 0:
        return None
    return {
        "price": price,
        "type": level.get("type"),
        "label": level.get("label"),
        "source": level.get("source"),
        "distance_pct": _float(level.get("distance_pct")),
    }


def _within_tolerance(left, right, tolerance_pct):
    left = _float(left)
    right = _float(right)
    if left is None or right is None or left <= 0 or right <= 0:
        return False
    return abs((left / right) - 1.0) <= max(0.0, tolerance_pct)


def _confirmation_policy(config, side, direction):
    if side == "support" and direction == "lower":
        return (
            max(1, int(_config_float(
                config,
                "level_stability_support_lower_confirm_samples",
                2,
            ))),
            max(0.0, _config_float(
                config,
                "level_stability_support_lower_confirm_minutes",
                5.0,
            )),
        )
    if side == "support":
        return (
            max(1, int(_config_float(
                config,
                "level_stability_support_raise_confirm_samples",
                4,
            ))),
            max(0.0, _config_float(
                config,
                "level_stability_support_raise_confirm_minutes",
                240.0,
            )),
        )
    if direction == "lower":
        return (
            max(1, int(_config_float(
                config,
                "level_stability_resistance_lower_confirm_samples",
                2,
            ))),
            max(0.0, _config_float(
                config,
                "level_stability_resistance_lower_confirm_minutes",
                5.0,
            )),
        )
    return (
        max(1, int(_config_float(
            config,
            "level_stability_resistance_raise_confirm_samples",
            3,
        ))),
        max(0.0, _config_float(
            config,
            "level_stability_resistance_raise_confirm_minutes",
            60.0,
        )),
    )


def _promote(side_state, raw, price, now, reason):
    previous_price = _float((side_state.get("stable") or {}).get("price"))
    side_state["stable"] = {
        **raw,
        "price": price,
        "confirmed_at": now.isoformat(),
        "previous_price": previous_price,
        "change_reason": reason,
    }
    side_state["candidate"] = None
    side_state["break_candidate"] = None
    side_state["status"] = "active"
    side_state["status_reason"] = reason
    side_state["status_changed_at"] = now.isoformat()
    side_state["broken_at"] = None
    side_state["broken_market_price"] = None
    side_state["broken_breach_pct"] = None
    side_state["change_count"] = int(side_state.get("change_count", 0) or 0) + (
        1 if previous_price is not None else 0
    )
    return {
        "kind": "promotion",
        "previous_price": previous_price,
        "price": price,
        "reason": reason,
    }


def _update_side(side_state, raw_level, now, config, side):
    raw = _raw_level(raw_level)
    previous_raw_price = _float(side_state.get("last_raw_price"))
    previous_observed_at = side_state.get("observed_at")
    side_state["raw"] = raw
    side_state["observed_at"] = now.isoformat()
    if raw is None:
        if not side_state.get("last_valid_raw_at"):
            side_state["last_valid_raw_at"] = (
                previous_observed_at
                or (side_state.get("stable") or {}).get("confirmed_at")
            )
        if not side_state.get("raw_missing_since"):
            side_state["raw_missing_since"] = now.isoformat()
        side_state["last_decision"] = "raw_level_unavailable"
        return None

    side_state["last_valid_raw_at"] = now.isoformat()
    side_state["raw_missing_since"] = None
    side_state["last_raw_price"] = raw["price"]
    side_state["raw_observation_count"] = int(
        side_state.get("raw_observation_count", 0) or 0
    ) + 1
    side_state["raw_min_price"] = min(
        raw["price"],
        _float(side_state.get("raw_min_price"), raw["price"]),
    )
    side_state["raw_max_price"] = max(
        raw["price"],
        _float(side_state.get("raw_max_price"), raw["price"]),
    )

    tolerance = max(0.0, _config_float(
        config,
        "level_stability_cluster_tolerance_pct",
        0.0035,
    ))
    if (
        previous_raw_price is not None
        and not _within_tolerance(
            raw["price"],
            previous_raw_price,
            tolerance,
        )
    ):
        side_state["raw_material_change_count"] = int(
            side_state.get("raw_material_change_count", 0) or 0
        ) + 1

    stable = side_state.get("stable")
    if not isinstance(stable, dict) or _float(stable.get("price")) is None:
        side_state["last_decision"] = "initial_level_confirmed"
        return _promote(
            side_state,
            raw,
            raw["price"],
            now,
            "initial_level_confirmed",
        )

    stable_price = float(stable["price"])
    raw_stable_divergence_pct = abs(
        ((raw["price"] / stable_price) - 1.0) * 100.0
    )
    side_state["max_abs_raw_stable_divergence_pct"] = max(
        raw_stable_divergence_pct,
        _float(
            side_state.get("max_abs_raw_stable_divergence_pct"),
            0.0,
        ),
    )
    if _within_tolerance(raw["price"], stable_price, tolerance):
        if side_state.get("candidate"):
            side_state["pending_reset_count"] = int(
                side_state.get("pending_reset_count", 0) or 0
            ) + 1
        side_state["candidate"] = None
        side_state["last_stable_seen_at"] = now.isoformat()
        side_state["last_decision"] = "within_stable_zone"
        return None

    candidate = side_state.get("candidate")
    candidate_price = (
        _float(candidate.get("price")) if isinstance(candidate, dict) else None
    )
    if candidate_price is None or not _within_tolerance(
        raw["price"],
        candidate_price,
        tolerance,
    ):
        if candidate_price is not None:
            side_state["pending_reset_count"] = int(
                side_state.get("pending_reset_count", 0) or 0
            ) + 1
        side_state["pending_started_count"] = int(
            side_state.get("pending_started_count", 0) or 0
        ) + 1
        candidate = {
            **raw,
            "first_seen_at": now.isoformat(),
            "last_seen_at": now.isoformat(),
            "samples": 1,
            "prices": [raw["price"]],
        }
    else:
        prices = [
            price
            for price in (candidate.get("prices") or [])
            if _float(price) is not None and _float(price) > 0
        ]
        prices.append(raw["price"])
        prices = prices[-60:]
        candidate.update(raw)
        candidate.update({
            "price": statistics.median(prices),
            "last_seen_at": now.isoformat(),
            "samples": int(candidate.get("samples", 0) or 0) + 1,
            "prices": prices,
        })
    side_state["candidate"] = candidate

    direction = "lower" if candidate["price"] < stable_price else "higher"
    required_samples, required_minutes = _confirmation_policy(
        config,
        side,
        direction,
    )
    age_minutes = _age_minutes(candidate.get("first_seen_at"), now)
    candidate["direction"] = direction
    candidate["age_minutes"] = round(age_minutes, 2)
    candidate["required_samples"] = required_samples
    candidate["required_minutes"] = required_minutes

    if (
        int(candidate.get("samples", 0) or 0) >= required_samples
        and age_minutes >= required_minutes
    ):
        reason = f"{side}_{direction}_confirmed"
        side_state["last_decision"] = reason
        promoted = {
            **raw,
            "price": float(candidate["price"]),
        }
        return _promote(
            side_state,
            promoted,
            float(candidate["price"]),
            now,
            reason,
        )

    side_state["last_decision"] = f"{side}_{direction}_pending"
    return None


def _set_status(side_state, status, now, reason):
    previous = side_state.get("status")
    side_state["status"] = status
    side_state["status_reason"] = reason
    if previous != status:
        side_state["status_changed_at"] = now.isoformat()
    return previous


def _update_side_lifecycle(side_state, market_price, now, config, side):
    stable_price = _float((side_state.get("stable") or {}).get("price"))
    if stable_price is None or stable_price <= 0:
        _set_status(
            side_state,
            "unavailable",
            now,
            "stable_level_unavailable",
        )
        return None

    price = _float(market_price)
    break_tolerance = max(0.0, _config_float(
        config,
        "level_stability_break_tolerance_pct",
        _config_float(
            config,
            "level_stability_cluster_tolerance_pct",
            0.0035,
        ),
    ))
    breached = False
    breach_pct = None
    if price is not None and price > 0:
        if side == "support" and price < stable_price:
            breach_pct = (stable_price / price) - 1.0
            breached = breach_pct > break_tolerance
        elif side == "resistance" and price > stable_price:
            breach_pct = (price / stable_price) - 1.0
            breached = breach_pct > break_tolerance

    if breached:
        candidate = side_state.get("break_candidate")
        if not isinstance(candidate, dict):
            candidate = {
                "first_seen_at": now.isoformat(),
                "last_seen_at": now.isoformat(),
                "samples": 1,
                "market_price": price,
                "breach_pct": breach_pct,
            }
        else:
            candidate.update({
                "last_seen_at": now.isoformat(),
                "samples": int(candidate.get("samples", 0) or 0) + 1,
                "market_price": price,
                "breach_pct": max(
                    breach_pct,
                    _float(candidate.get("breach_pct"), 0.0),
                ),
            })
        required_samples = max(1, int(_config_float(
            config,
            "level_stability_break_confirm_samples",
            2,
        )))
        required_minutes = max(0.0, _config_float(
            config,
            "level_stability_break_confirm_minutes",
            5.0,
        ))
        candidate["age_minutes"] = round(
            _age_minutes(candidate.get("first_seen_at"), now),
            2,
        )
        candidate["required_samples"] = required_samples
        candidate["required_minutes"] = required_minutes
        side_state["break_candidate"] = candidate

        if side_state.get("status") == "broken":
            return None
        if (
            candidate["samples"] >= required_samples
            and candidate["age_minutes"] >= required_minutes
        ):
            reason = f"{side}_market_break_confirmed"
            previous = _set_status(
                side_state,
                "broken",
                now,
                reason,
            )
            side_state["broken_at"] = now.isoformat()
            side_state["broken_market_price"] = price
            side_state["broken_breach_pct"] = breach_pct
            side_state["invalidation_count"] = int(
                side_state.get("invalidation_count", 0) or 0
            ) + 1
            return {
                "kind": "invalidation",
                "previous_status": previous,
                "status": "broken",
                "price": stable_price,
                "market_price": price,
                "breach_pct": breach_pct,
                "reason": reason,
            }
        _set_status(
            side_state,
            "pending_break",
            now,
            f"{side}_market_break_pending",
        )
        return None

    side_state["break_candidate"] = None
    raw_available = isinstance(side_state.get("raw"), dict)
    if raw_available:
        previous_status = side_state.get("status")
        raw_price = _float(side_state["raw"].get("price"))
        cluster_tolerance = max(0.0, _config_float(
            config,
            "level_stability_cluster_tolerance_pct",
            0.0035,
        ))
        if (
            previous_status in {"broken", "stale"}
            and not _within_tolerance(
                raw_price,
                stable_price,
                cluster_tolerance,
            )
        ):
            side_state["status_reason"] = (
                "replacement_level_pending_confirmation"
            )
            return None
        previous = _set_status(
            side_state,
            "active",
            now,
            "valid_raw_level_observed",
        )
        if previous in {"broken", "stale"}:
            side_state["reactivation_count"] = int(
                side_state.get("reactivation_count", 0) or 0
            ) + 1
            return {
                "kind": "reactivation",
                "previous_status": previous,
                "status": "active",
                "price": stable_price,
                "reason": f"{side}_valid_raw_level_returned",
            }
        return None

    if side_state.get("status") == "broken":
        return None
    stale_after_minutes = max(0.0, _config_float(
        config,
        "level_stability_raw_max_age_minutes",
        180.0,
    ))
    raw_age_minutes = _age_minutes(
        side_state.get("last_valid_raw_at"),
        now,
    )
    if raw_age_minutes >= stale_after_minutes:
        previous = _set_status(
            side_state,
            "stale",
            now,
            "raw_level_expired",
        )
        if previous != "stale":
            side_state["stale_count"] = int(
                side_state.get("stale_count", 0) or 0
            ) + 1
            return {
                "kind": "stale",
                "previous_status": previous,
                "status": "stale",
                "price": stable_price,
                "raw_age_minutes": raw_age_minutes,
                "reason": f"{side}_raw_level_expired",
            }
        return None
    _set_status(
        side_state,
        "missing_raw",
        now,
        "raw_level_temporarily_unavailable",
    )
    return None


def _mode_from_modes(modes):
    for mode in modes or []:
        if mode != "llm_target":
            return mode
    return None


def update_anchor_hysteresis(
    anchor_state,
    *,
    configured_modes,
    raw_active_modes,
    range_position,
    now,
    config,
):
    """Update a sticky shadow anchor without altering live mode selection."""
    moment = _moment(now)
    available = [mode for mode in (configured_modes or []) if mode != "llm_target"]
    llm_enabled = "llm_target" in (configured_modes or [])
    raw_mode = _mode_from_modes(raw_active_modes)
    position = _float(range_position)
    previous = anchor_state.get("mode")
    if previous not in available:
        previous = raw_mode if raw_mode in available else (available[0] if available else None)

    mid_mode = str((config or {}).get("dynamic_anchor_mid_mode", "median") or "median").strip().lower()
    alternate_mid = "mean" if mid_mode == "median" else "median"
    middle = next(
        (mode for mode in (mid_mode, alternate_mid) if mode in available),
        None,
    )
    low_enter = _config_float(
        config,
        "dynamic_anchor_hysteresis_low_enter",
        max(0.0, _config_float(config, "dynamic_anchor_low_band_max", 0.35) - 0.05),
    )
    low_exit = _config_float(
        config,
        "dynamic_anchor_hysteresis_low_exit",
        min(1.0, _config_float(config, "dynamic_anchor_low_band_max", 0.35) + 0.08),
    )
    high_enter = _config_float(
        config,
        "dynamic_anchor_hysteresis_high_enter",
        _config_float(config, "dynamic_anchor_high_band_min", 0.75),
    )
    high_exit = _config_float(
        config,
        "dynamic_anchor_hysteresis_high_exit",
        max(0.0, high_enter - 0.08),
    )

    selected = previous
    if position is not None and available:
        if previous == "low":
            if "high" in available and position >= high_enter:
                selected = "high"
            elif middle and position > low_exit:
                selected = middle
        elif previous == "high":
            if "low" in available and position <= low_enter:
                selected = "low"
            elif middle and position < high_exit:
                selected = middle
        elif previous in {"mean", "median"}:
            if "low" in available and position <= low_enter:
                selected = "low"
            elif "high" in available and position >= high_enter:
                selected = "high"
            elif middle:
                selected = middle
        elif raw_mode in available:
            selected = raw_mode

    changed = selected != anchor_state.get("mode")
    if changed:
        anchor_state["previous_mode"] = anchor_state.get("mode")
        anchor_state["mode"] = selected
        anchor_state["changed_at"] = moment.isoformat()
        anchor_state["change_count"] = int(anchor_state.get("change_count", 0) or 0) + (
            1 if anchor_state.get("previous_mode") is not None else 0
        )
    anchor_state.update({
        "raw_mode": raw_mode,
        "range_position": position,
        "observed_at": moment.isoformat(),
        "low_enter": low_enter,
        "low_exit": low_exit,
        "high_enter": high_enter,
        "high_exit": high_exit,
    })
    selected_modes = [selected] if selected else []
    if llm_enabled:
        selected_modes.insert(0, "llm_target")
    return {
        "changed": changed,
        "raw_mode": raw_mode,
        "stable_mode": selected,
        "stable_modes": selected_modes,
    }


def update_level_stability(
    state,
    *,
    raw_support,
    raw_resistance,
    market_price=None,
    now,
    config,
):
    """Update support/resistance state and return material promotions."""
    moment = _moment(now)
    if not isinstance(state, dict):
        raise TypeError("level stability state must be a dictionary")
    state.setdefault("support", {})
    state.setdefault("resistance", {})
    state.setdefault("anchor", {})
    changes = []
    support_change = _update_side(
        state["support"],
        raw_support,
        moment,
        config,
        "support",
    )
    if support_change:
        changes.append({"side": "support", **support_change})
    support_lifecycle_change = _update_side_lifecycle(
        state["support"],
        market_price,
        moment,
        config,
        "support",
    )
    if support_lifecycle_change:
        changes.append({"side": "support", **support_lifecycle_change})
    resistance_change = _update_side(
        state["resistance"],
        raw_resistance,
        moment,
        config,
        "resistance",
    )
    if resistance_change:
        changes.append({"side": "resistance", **resistance_change})
    resistance_lifecycle_change = _update_side_lifecycle(
        state["resistance"],
        market_price,
        moment,
        config,
        "resistance",
    )
    if resistance_lifecycle_change:
        changes.append({"side": "resistance", **resistance_lifecycle_change})
    state["updated_at"] = moment.isoformat()
    state["market_price"] = _float(market_price)
    return changes


def level_stability_snapshot(
    state,
    config,
    operator_control=None,
    *,
    current_price=None,
):
    state = state if isinstance(state, dict) else {}
    support = state.get("support") if isinstance(state.get("support"), dict) else {}
    resistance = state.get("resistance") if isinstance(state.get("resistance"), dict) else {}
    anchor = state.get("anchor") if isinstance(state.get("anchor"), dict) else {}
    stable_support = _float((support.get("stable") or {}).get("price"))
    moment = _moment(state.get("updated_at"))
    market_price = _float(current_price, _float(state.get("market_price")))
    buffer_pct = max(0.0, _config_float(
        config,
        "level_stability_support_buy_buffer_pct",
        0.0025,
    ))
    unfiltered_automatic_ceiling = (
        stable_support * (1.0 + buffer_pct)
        if stable_support is not None
        else None
    )
    support_status = support.get("status") or (
        "active" if stable_support is not None else "unavailable"
    )
    fail_open_when_stale = _bool(
        config,
        "level_stability_fail_open_when_stale",
        True,
    )
    automatic_ceiling_reason = "active_stable_support"
    automatic_ceiling = unfiltered_automatic_ceiling
    if stable_support is None:
        automatic_ceiling_reason = "stable_support_unavailable"
    elif support_status == "broken":
        automatic_ceiling = None
        automatic_ceiling_reason = "broken_support_fail_open"
    elif support_status == "stale" and fail_open_when_stale:
        automatic_ceiling = None
        automatic_ceiling_reason = "stale_support_fail_open"
    elif support_status == "stale":
        automatic_ceiling_reason = "stale_support_retained"
    elif support_status == "missing_raw":
        automatic_ceiling_reason = "temporary_raw_gap_using_stable_support"
    elif support_status == "pending_break":
        automatic_ceiling_reason = "pending_support_break_using_stable_support"
    operator_control = operator_control if isinstance(operator_control, dict) else {}
    operator_ceiling = None
    if operator_control.get("buy_price_ceiling_enabled"):
        parsed = _float(operator_control.get("buy_price_ceiling_usd"))
        if parsed is not None and parsed > 0:
            operator_ceiling = parsed
    ceiling_candidates = [
        value for value in (automatic_ceiling, operator_ceiling) if value is not None
    ]
    effective_ceiling = min(ceiling_candidates) if ceiling_candidates else None

    def side_snapshot(side_state, side):
        stable = side_state.get("stable") or {}
        raw = side_state.get("raw") or {}
        candidate = side_state.get("candidate") or {}
        break_candidate = side_state.get("break_candidate") or {}
        stable_price = _float(stable.get("price"))
        raw_price = _float(raw.get("price"))
        raw_age_minutes = (
            _age_minutes(side_state.get("last_valid_raw_at"), moment)
            if side_state.get("last_valid_raw_at")
            else None
        )
        raw_missing_age_minutes = (
            _age_minutes(side_state.get("raw_missing_since"), moment)
            if side_state.get("raw_missing_since")
            else None
        )
        raw_stable_divergence_pct = None
        if raw_price is not None and stable_price is not None:
            raw_stable_divergence_pct = (
                (raw_price / stable_price) - 1.0
            ) * 100.0
        market_breach_pct = None
        market_relation = None
        if market_price is not None and stable_price is not None:
            if side == "support":
                market_relation = (
                    "above" if market_price >= stable_price else "below"
                )
                if market_price < stable_price:
                    market_breach_pct = (
                        (stable_price / market_price) - 1.0
                    ) * 100.0
            else:
                market_relation = (
                    "below" if market_price <= stable_price else "above"
                )
                if market_price > stable_price:
                    market_breach_pct = (
                        (market_price / stable_price) - 1.0
                    ) * 100.0
        return {
            "status": side_state.get("status") or (
                "active" if stable_price is not None else "unavailable"
            ),
            "status_reason": side_state.get("status_reason"),
            "status_changed_at": side_state.get("status_changed_at"),
            "raw_price": raw_price,
            "raw_type": raw.get("type"),
            "raw_label": raw.get("label"),
            "raw_observation_count": int(
                side_state.get("raw_observation_count", 0) or 0
            ),
            "raw_material_change_count": int(
                side_state.get("raw_material_change_count", 0) or 0
            ),
            "raw_min_price": _float(side_state.get("raw_min_price")),
            "raw_max_price": _float(side_state.get("raw_max_price")),
            "last_valid_raw_at": side_state.get("last_valid_raw_at"),
            "raw_age_minutes": raw_age_minutes,
            "raw_missing_since": side_state.get("raw_missing_since"),
            "raw_missing_age_minutes": raw_missing_age_minutes,
            "raw_stable_divergence_pct": raw_stable_divergence_pct,
            "max_abs_raw_stable_divergence_pct": _float(
                side_state.get("max_abs_raw_stable_divergence_pct"),
                0.0,
            ),
            "stable_price": stable_price,
            "stable_type": stable.get("type"),
            "stable_label": stable.get("label"),
            "stable_since": stable.get("confirmed_at"),
            "previous_price": _float(stable.get("previous_price")),
            "change_reason": stable.get("change_reason"),
            "pending_price": _float(candidate.get("price")),
            "pending_direction": candidate.get("direction"),
            "pending_samples": int(candidate.get("samples", 0) or 0),
            "pending_age_minutes": _float(candidate.get("age_minutes")),
            "pending_required_samples": candidate.get("required_samples"),
            "pending_required_minutes": candidate.get("required_minutes"),
            "pending_started_count": int(
                side_state.get("pending_started_count", 0) or 0
            ),
            "pending_reset_count": int(
                side_state.get("pending_reset_count", 0) or 0
            ),
            "break_pending_samples": int(
                break_candidate.get("samples", 0) or 0
            ),
            "break_pending_age_minutes": _float(
                break_candidate.get("age_minutes")
            ),
            "break_required_samples": break_candidate.get(
                "required_samples"
            ),
            "break_required_minutes": break_candidate.get(
                "required_minutes"
            ),
            "broken_at": side_state.get("broken_at"),
            "broken_market_price": _float(
                side_state.get("broken_market_price")
            ),
            "broken_breach_pct": (
                _float(side_state.get("broken_breach_pct")) * 100.0
                if _float(side_state.get("broken_breach_pct")) is not None
                else None
            ),
            "invalidation_count": int(
                side_state.get("invalidation_count", 0) or 0
            ),
            "stale_count": int(side_state.get("stale_count", 0) or 0),
            "reactivation_count": int(
                side_state.get("reactivation_count", 0) or 0
            ),
            "market_relation": market_relation,
            "market_breach_pct": market_breach_pct,
            "last_decision": side_state.get("last_decision"),
            "change_count": int(side_state.get("change_count", 0) or 0),
        }

    return {
        "enabled": _bool(config, "level_stability_shadow_enabled", False),
        "shadow_only": True,
        "updated_at": state.get("updated_at"),
        "cluster_tolerance_pct": _config_float(
            config,
            "level_stability_cluster_tolerance_pct",
            0.0035,
        ),
        "break_tolerance_pct": _config_float(
            config,
            "level_stability_break_tolerance_pct",
            _config_float(
                config,
                "level_stability_cluster_tolerance_pct",
                0.0035,
            ),
        ),
        "break_confirm_samples": max(1, int(_config_float(
            config,
            "level_stability_break_confirm_samples",
            2,
        ))),
        "break_confirm_minutes": max(0.0, _config_float(
            config,
            "level_stability_break_confirm_minutes",
            5.0,
        )),
        "raw_max_age_minutes": max(0.0, _config_float(
            config,
            "level_stability_raw_max_age_minutes",
            180.0,
        )),
        "fail_open_when_stale": fail_open_when_stale,
        "support_buy_buffer_pct": buffer_pct,
        "unfiltered_automatic_buy_ceiling": unfiltered_automatic_ceiling,
        "automatic_buy_ceiling": automatic_ceiling,
        "automatic_buy_ceiling_active": automatic_ceiling is not None,
        "automatic_buy_ceiling_reason": automatic_ceiling_reason,
        "operator_buy_ceiling": operator_ceiling,
        "effective_shadow_buy_ceiling": effective_ceiling,
        "support": side_snapshot(support, "support"),
        "resistance": side_snapshot(resistance, "resistance"),
        "anchor": {
            "raw_mode": anchor.get("raw_mode"),
            "stable_mode": anchor.get("mode"),
            "previous_mode": anchor.get("previous_mode"),
            "range_position": _float(anchor.get("range_position")),
            "changed_at": anchor.get("changed_at"),
            "change_count": int(anchor.get("change_count", 0) or 0),
            "low_enter": _float(anchor.get("low_enter")),
            "low_exit": _float(anchor.get("low_exit")),
            "high_enter": _float(anchor.get("high_enter")),
            "high_exit": _float(anchor.get("high_exit")),
        },
    }


def shadow_price_rule_reason(level, stability_snapshot, operator_control=None):
    """Return the reason a candidate would fail the shadow permission zone."""
    price = _float(level)
    if price is None or price <= 0:
        return "level_stability_invalid_candidate_price"
    snapshot = stability_snapshot if isinstance(stability_snapshot, dict) else {}
    ceiling = _float(snapshot.get("effective_shadow_buy_ceiling"))
    if ceiling is not None and price > ceiling:
        return "level_stability_shadow_above_ceiling"
    control = operator_control if isinstance(operator_control, dict) else {}
    if control.get("buy_price_floor_enabled"):
        floor = _float(control.get("buy_price_floor_usd"))
        if floor is not None and floor > 0 and price < floor:
            return "operator_buy_price_below_floor"
    return None
