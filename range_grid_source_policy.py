"""Source-specific entry authority and sizing for the range-grid bot."""


VALID_ENTRY_AUTHORITIES = {
    "sentiment_confirmed",
    "price_first",
    "stabilization_preferred",
    "chop_confirmed",
}

ENTRY_POLICY_NUMERIC_FIELDS = {
    "position_size_multiplier",
    "missing_weather_size_multiplier",
    "weak_setup_size_multiplier",
    "falling_tape_size_multiplier",
    "min_stabilization_score",
    "min_entry_opportunity_score",
    "min_rebound_confirmation_score",
    "max_exit_pressure_score",
    "min_hold_through_score",
    "max_downtrend_strength",
    "min_actionable_resistance_room_pct",
}

ENTRY_POLICY_ALLOWED_FIELDS = ENTRY_POLICY_NUMERIC_FIELDS | {
    "authority",
    "allowed_phases",
    "hard_block_falling_tape",
    "weather_bypassable_hard_safety_flags",
}


def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def _tokens(value):
    if isinstance(value, (list, tuple, set)):
        return {
            str(item).strip().lower()
            for item in value
            if str(item).strip()
        }
    return {
        token.strip().lower()
        for token in str(value or "").split(",")
        if token.strip()
    }


def _source_policy(config, buy_source):
    if not isinstance(config, dict):
        return None
    policies = config.get("entry_policy_by_source")
    if not isinstance(policies, dict):
        return None
    source_key = str(buy_source or "").strip().lower()
    for source, policy in policies.items():
        if str(source or "").strip().lower() != source_key:
            continue
        return policy if isinstance(policy, dict) else None
    return None


def entry_policy_for_source(config, buy_source, fallback_config=None):
    policy = _source_policy(config, buy_source)
    if policy is None and fallback_config is not config:
        policy = _source_policy(fallback_config, buy_source)
    return dict(policy) if isinstance(policy, dict) else None


def source_entry_step_pct(config, buy_source, default, fallback_config=None):
    source_key = str(buy_source or "").strip().lower()
    for candidate_config in (config, fallback_config):
        if not isinstance(candidate_config, dict):
            continue
        values = candidate_config.get("entry_step_pct_by_source")
        if not isinstance(values, dict):
            continue
        for source, value in values.items():
            if str(source or "").strip().lower() != source_key:
                continue
            numeric = _safe_float(value)
            if numeric is not None and numeric > 0:
                return numeric
    return float(default or 0.0)


def _weather_bot_decides(weather_report):
    return (
        isinstance(weather_report, dict)
        and weather_report.get("mode") == "weather_report"
        and weather_report.get("bot_decision_authority") == "bot"
        and weather_report.get("trade_permission") == "bot_decides"
    )


def _hard_risk_blocked(
    action_recommendation,
    _action_policy,
    risk_context,
    weather_report,
    policy,
):
    weather_report = weather_report if isinstance(weather_report, dict) else {}
    risk_context = risk_context if isinstance(risk_context, dict) else {}
    policy = policy if isinstance(policy, dict) else {}
    if weather_report.get("emergency_bell"):
        return True
    if str(weather_report.get("alert_level") or "").strip().lower() == "danger":
        return True
    # A generic sentiment recommendation of "blocked" is not an emergency;
    # source-specific authority may bypass it. Explicit risk_off never can.
    normalized_action = str(action_recommendation or "").strip().lower()
    if normalized_action == "risk_off":
        return True

    hard_safety_flags = _tokens(risk_context.get("hard_safety_flags"))
    bypassable_flags = _tokens(
        policy.get("weather_bypassable_hard_safety_flags")
    )
    weather_bypasses_flags = bool(
        hard_safety_flags
        and _weather_bot_decides(weather_report)
        and hard_safety_flags.issubset(bypassable_flags)
    )

    recommended_posture = str(
        risk_context.get("recommended_posture") or ""
    ).strip().lower()
    if recommended_posture == "emergency_bell":
        return True
    if recommended_posture == "risk_off" and not weather_bypasses_flags:
        return True
    if hard_safety_flags and not weather_bypasses_flags:
        return True

    return False


def _bounded_multiplier(policy, key, default):
    value = _safe_float(policy.get(key))
    if value is None:
        value = default
    return max(0.0, min(float(value), 1.0))


def _weather_setup_failures(policy, weather_report):
    stability = weather_report.get("market_stability")
    if not isinstance(stability, dict):
        stability = {}
    trend = weather_report.get("trend_pressure")
    if not isinstance(trend, dict):
        trend = {}
    opportunity = weather_report.get("market_opportunity")
    if not isinstance(opportunity, dict):
        opportunity = {}

    phase = str(opportunity.get("cycle_phase") or "").strip().lower()
    allowed_phases = _tokens(policy.get("allowed_phases"))
    if allowed_phases and phase not in allowed_phases:
        return "source_policy_phase"
    if _bool(policy.get("hard_block_falling_tape"), False) and bool(
        trend.get("falling_tape")
    ):
        return "source_policy_falling_tape"

    threshold_checks = (
        (
            "min_stabilization_score",
            stability.get("stabilization_score"),
            "source_policy_stabilization",
            "min",
        ),
        (
            "min_entry_opportunity_score",
            opportunity.get("entry_opportunity_score"),
            "source_policy_entry_score",
            "min",
        ),
        (
            "min_rebound_confirmation_score",
            opportunity.get("rebound_confirmation_score"),
            "source_policy_rebound",
            "min",
        ),
        (
            "max_exit_pressure_score",
            opportunity.get("exit_pressure_score"),
            "source_policy_exit_pressure",
            "max",
        ),
        (
            "min_hold_through_score",
            opportunity.get("hold_through_score"),
            "source_policy_hold_through",
            "min",
        ),
        (
            "max_downtrend_strength",
            trend.get("downtrend_strength"),
            "source_policy_downtrend",
            "max",
        ),
    )
    for key, raw_value, reason, direction in threshold_checks:
        threshold = _safe_float(policy.get(key))
        if threshold is None:
            continue
        value = _safe_float(raw_value)
        if value is None:
            return reason
        if direction == "min" and value < threshold:
            return reason
        if direction == "max" and value > threshold:
            return reason

    min_room = _safe_float(policy.get("min_actionable_resistance_room_pct"))
    if min_room is not None and min_room > 0:
        location = weather_report.get("market_location")
        if not isinstance(location, dict):
            location = {}
        resistance = location.get("actionable_resistance")
        if not isinstance(resistance, dict):
            resistance = location.get("nearest_resistance")
        if not isinstance(resistance, dict):
            resistance = {}
        distance_pct = _safe_float(resistance.get("distance_pct"))
        if distance_pct is None or distance_pct < min_room * 100.0:
            return "source_policy_resistance_room"

    return None


def source_entry_policy_decision(
    config,
    buy_source,
    *,
    action_recommendation=None,
    action_policy=None,
    risk_context=None,
    weather_report=None,
    fallback_config=None,
):
    enabled = _bool(
        (config or {}).get("source_entry_policy_enabled")
        if isinstance(config, dict)
        else None,
        _bool(
            (fallback_config or {}).get("source_entry_policy_enabled")
            if isinstance(fallback_config, dict)
            else None,
            False,
        ),
    )
    policy = entry_policy_for_source(config, buy_source, fallback_config)
    if not enabled or policy is None:
        return {
            "policy_enabled": False,
            "authority": "legacy",
            "allowed": True,
            "reason": None,
            "bypass_sentiment_gate": False,
            "size_multiplier": 1.0,
            "weather_available": _weather_bot_decides(weather_report),
            "setup_confirmed": None,
        }

    authority = str(
        policy.get("authority") or "sentiment_confirmed"
    ).strip().lower()
    base_multiplier = _bounded_multiplier(
        policy,
        "position_size_multiplier",
        1.0,
    )
    weather_report = weather_report if isinstance(weather_report, dict) else {}
    if not isinstance(risk_context, dict):
        embedded_risk = weather_report.get("_risk_context")
        risk_context = embedded_risk if isinstance(embedded_risk, dict) else {}
    weather_available = _weather_bot_decides(weather_report)

    if _hard_risk_blocked(
        action_recommendation,
        action_policy,
        risk_context,
        weather_report,
        policy,
    ):
        return {
            "policy_enabled": True,
            "authority": authority,
            "allowed": False,
            "reason": "source_policy_hard_risk",
            "bypass_sentiment_gate": False,
            "size_multiplier": 0.0,
            "weather_available": weather_available,
            "setup_confirmed": False,
        }

    if authority == "sentiment_confirmed":
        return {
            "policy_enabled": True,
            "authority": authority,
            "allowed": True,
            "reason": "source_policy_sentiment_confirmed",
            "bypass_sentiment_gate": False,
            "size_multiplier": base_multiplier,
            "weather_available": weather_available,
            "setup_confirmed": None,
        }

    trend = weather_report.get("trend_pressure")
    if not isinstance(trend, dict):
        trend = {}
    falling_tape = bool(trend.get("falling_tape"))

    if authority == "price_first":
        multiplier = base_multiplier
        reason = "source_policy_price_first"
        if not weather_available:
            multiplier *= _bounded_multiplier(
                policy,
                "missing_weather_size_multiplier",
                1.0,
            )
            reason = "source_policy_price_first_weather_missing"
        elif falling_tape:
            if _bool(policy.get("hard_block_falling_tape"), False):
                return {
                    "policy_enabled": True,
                    "authority": authority,
                    "allowed": False,
                    "reason": "source_policy_falling_tape",
                    "bypass_sentiment_gate": False,
                    "size_multiplier": 0.0,
                    "weather_available": True,
                    "setup_confirmed": False,
                }
            multiplier *= _bounded_multiplier(
                policy,
                "falling_tape_size_multiplier",
                0.5,
            )
            reason = "source_policy_price_first_falling_tape_reduced"
        return {
            "policy_enabled": True,
            "authority": authority,
            "allowed": multiplier > 0,
            "reason": reason,
            "bypass_sentiment_gate": True,
            "size_multiplier": multiplier,
            "weather_available": weather_available,
            "setup_confirmed": not falling_tape if weather_available else None,
        }

    if not weather_available:
        if authority == "chop_confirmed":
            return {
                "policy_enabled": True,
                "authority": authority,
                "allowed": False,
                "reason": "source_policy_weather_required",
                "bypass_sentiment_gate": False,
                "size_multiplier": 0.0,
                "weather_available": False,
                "setup_confirmed": False,
            }
        multiplier = base_multiplier * _bounded_multiplier(
            policy,
            "missing_weather_size_multiplier",
            0.5,
        )
        return {
            "policy_enabled": True,
            "authority": authority,
            "allowed": multiplier > 0,
            "reason": "source_policy_preferred_weather_missing",
            "bypass_sentiment_gate": True,
            "size_multiplier": multiplier,
            "weather_available": False,
            "setup_confirmed": None,
        }

    failure_reason = _weather_setup_failures(policy, weather_report)
    if failure_reason == "source_policy_falling_tape" and _bool(
        policy.get("hard_block_falling_tape"),
        False,
    ):
        return {
            "policy_enabled": True,
            "authority": authority,
            "allowed": False,
            "reason": failure_reason,
            "bypass_sentiment_gate": False,
            "size_multiplier": 0.0,
            "weather_available": True,
            "setup_confirmed": False,
        }
    if failure_reason and authority == "chop_confirmed":
        return {
            "policy_enabled": True,
            "authority": authority,
            "allowed": False,
            "reason": failure_reason,
            "bypass_sentiment_gate": False,
            "size_multiplier": 0.0,
            "weather_available": True,
            "setup_confirmed": False,
        }
    if failure_reason:
        multiplier = base_multiplier * _bounded_multiplier(
            policy,
            "weak_setup_size_multiplier",
            0.5,
        )
        return {
            "policy_enabled": True,
            "authority": authority,
            "allowed": multiplier > 0,
            "reason": "source_policy_preferred_weak_setup",
            "setup_failure_reason": failure_reason,
            "bypass_sentiment_gate": True,
            "size_multiplier": multiplier,
            "weather_available": True,
            "setup_confirmed": False,
        }

    return {
        "policy_enabled": True,
        "authority": authority,
        "allowed": True,
        "reason": f"source_policy_{authority}",
        "bypass_sentiment_gate": True,
        "size_multiplier": base_multiplier,
        "weather_available": True,
        "setup_confirmed": True,
    }
