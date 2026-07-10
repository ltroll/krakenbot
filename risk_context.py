from datetime import datetime, timezone


def clamp(value, lower=0.0, upper=1.0):
    return max(lower, min(upper, value))


def numeric_or_none(value):
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def numeric_or_default(value, default):
    parsed = numeric_or_none(value)
    return default if parsed is None else parsed


def parse_iso8601(value):
    if not isinstance(value, str) or not value:
        return None
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def list_value(value):
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def dict_value(value):
    return value if isinstance(value, dict) else {}


def risk_context_available(risk_context):
    return isinstance(risk_context, dict) and bool(risk_context)


def risk_context_source_processed_at(risk_context, fallback=None):
    if not isinstance(risk_context, dict):
        return fallback
    return (
        risk_context.get("processed_at")
        or risk_context.get("source_processed_at")
        or fallback
    )


def risk_context_age_minutes(risk_context, fallback_processed_at=None, now=None):
    processed_at = parse_iso8601(
        risk_context_source_processed_at(risk_context, fallback_processed_at)
    )
    if processed_at is None:
        return None
    now = now or datetime.now(timezone.utc)
    return (now.astimezone(timezone.utc) - processed_at).total_seconds() / 60


def derive_risk_context(risk_context, *, fallback_processed_at=None, stale=False, now=None):
    if not risk_context_available(risk_context):
        return {
            "risk_context_available": False,
            "risk_context_stale": False,
            "weather_report_available": False,
            "weather_trade_permission": None,
            "weather_bot_decision_authority": None,
            "weather_condition": None,
            "weather_alert_level": None,
            "weather_emergency_bell": False,
            "weather_opportunity_tags": [],
            "weather_risk_warnings": [],
            "weather_market_current_price": None,
            "weather_market_range_high": None,
            "weather_market_range_low": None,
            "weather_market_range_position": None,
            "weather_market_range_zone": None,
            "weather_market_distance_to_recent_high_pct": None,
            "weather_market_distance_from_recent_low_pct": None,
            "weather_market_price_return_24h_pct": None,
            "weather_market_price_return_4h_pct": None,
            "risk_adjusted_buy_score": None,
            "risk_adjusted_market_score": None,
            "risk_adjusted_posture": None,
            "risk_adjusted_reason": "risk_context_missing",
            "suggested_position_size_multiplier": 1.0,
            "suggested_grid_aggression_multiplier": 1.0,
            "suggested_entry_discount_multiplier": 1.0,
            "suggested_take_profit_multiplier": 1.0,
            "risk_context_source_processed_at": fallback_processed_at,
            "risk_context_age_minutes": None,
            "risk_context_hard_safety_flags": [],
        }

    weather = dict_value(risk_context.get("weather_report"))
    bot_tuning = dict_value(weather.get("bot_tuning"))
    market_location = dict_value(weather.get("market_location"))
    market_risk = clamp(numeric_or_default(risk_context.get("market_risk_score"), 0.5))
    buy_aggression = clamp(numeric_or_default(risk_context.get("buy_aggression_score"), 0.0))
    downside_risk = clamp(numeric_or_default(risk_context.get("downside_risk_score"), 0.5))
    bottoming = clamp(numeric_or_default(risk_context.get("bottoming_score"), 0.0))
    rebound = clamp(numeric_or_default(risk_context.get("rebound_score"), 0.0))
    breakout = clamp(numeric_or_default(risk_context.get("breakout_score"), 0.0))
    hard_safety_flags = [str(flag) for flag in list_value(risk_context.get("hard_safety_flags")) if str(flag)]

    buy_score = clamp(
        buy_aggression * 0.45
        + bottoming * 0.20
        + rebound * 0.20
        - market_risk * 0.25
        - downside_risk * 0.20
    )
    market_score = clamp(market_risk * 0.70 + downside_risk * 0.30)

    if weather:
        condition = weather.get("condition") or "neutral"
        alert_level = weather.get("alert_level") or "normal"
        emergency_bell = bool(weather.get("emergency_bell"))
        posture = "emergency_bell" if emergency_bell else str(condition)
        reason = (
            f"weather_report condition={condition}; "
            f"alert_level={alert_level}; trade_permission={weather.get('trade_permission', 'n/a')}"
        )
    elif hard_safety_flags:
        posture = "risk_off"
        reason = "hard safety flags present: " + ", ".join(hard_safety_flags)
    elif buy_score >= 0.65 and market_risk < 0.55:
        posture = "constructive_buy"
        reason = "buy score is high while market risk is low/moderate"
    elif buy_score >= 0.50 and bottoming >= 0.50:
        posture = "cautious_accumulation"
        reason = "possible bottoming setup with defensive sizing"
    elif downside_risk >= 0.70:
        posture = "defensive_watch"
        reason = "downside risk is elevated"
    elif breakout >= 0.65:
        posture = "breakout_watch"
        reason = "breakout or range expansion risk is elevated"
    else:
        posture = "neutral_watch"
        reason = "risk context is not strong enough for a buy"

    if stale:
        reason = "risk context stale; bot-local policy should be used"

    position_multiplier = numeric_or_default(
        bot_tuning.get(
            "position_size_multiplier",
            risk_context.get("position_size_multiplier")
        ),
        1.0
    )
    if weather:
        if bool(weather.get("emergency_bell")):
            position_multiplier = 0.0
        elif weather.get("alert_level") == "watch":
            position_multiplier = min(position_multiplier, 0.75)
        elif weather.get("alert_level") == "caution":
            position_multiplier = min(position_multiplier, 0.50)
        elif weather.get("alert_level") == "danger":
            position_multiplier = min(position_multiplier, 0.25)
    elif hard_safety_flags:
        position_multiplier = 0.0
    elif posture in ("defensive_watch", "neutral_watch", "breakout_watch"):
        position_multiplier = min(position_multiplier, 0.5)
    elif posture == "cautious_accumulation":
        position_multiplier = min(position_multiplier, 0.75)

    return {
        "risk_context_available": True,
        "risk_context_stale": bool(stale),
        "risk_context_recommended_posture": risk_context.get("recommended_posture"),
        "risk_context_market_risk_score": market_risk,
        "risk_context_buy_aggression_score": buy_aggression,
        "risk_context_downside_risk_score": downside_risk,
        "risk_context_bottoming_score": bottoming,
        "risk_context_rebound_score": rebound,
        "risk_context_breakout_score": breakout,
        "risk_context_hard_safety_flags": hard_safety_flags,
        "risk_context_active_strategy": risk_context.get("active_strategy") or {},
        "risk_context_legacy_action_recommendation": risk_context.get("legacy_action_recommendation"),
        "risk_context_legacy_action_reason": risk_context.get("legacy_action_reason"),
        "weather_report_available": bool(weather),
        "weather_trade_permission": weather.get("trade_permission"),
        "weather_bot_decision_authority": weather.get("bot_decision_authority"),
        "weather_condition": weather.get("condition"),
        "weather_alert_level": weather.get("alert_level"),
        "weather_emergency_bell": bool(weather.get("emergency_bell")),
        "weather_opportunity_tags": [
            str(tag) for tag in list_value(weather.get("opportunity_tags")) if str(tag)
        ],
        "weather_risk_warnings": [
            str(warning) for warning in list_value(weather.get("risk_warnings")) if str(warning)
        ],
        "weather_market_current_price": numeric_or_none(market_location.get("current_price")),
        "weather_market_range_high": numeric_or_none(market_location.get("range_high")),
        "weather_market_range_low": numeric_or_none(market_location.get("range_low")),
        "weather_market_range_position": numeric_or_none(market_location.get("range_position")),
        "weather_market_range_zone": market_location.get("range_zone"),
        "weather_market_distance_to_recent_high_pct": numeric_or_none(
            market_location.get("distance_to_recent_high_pct")
        ),
        "weather_market_distance_from_recent_low_pct": numeric_or_none(
            market_location.get("distance_from_recent_low_pct")
        ),
        "weather_market_price_return_24h_pct": numeric_or_none(
            market_location.get("price_return_24h_pct")
        ),
        "weather_market_price_return_4h_pct": numeric_or_none(
            market_location.get("price_return_4h_pct")
        ),
        "risk_adjusted_buy_score": round(buy_score, 6),
        "risk_adjusted_market_score": round(market_score, 6),
        "risk_adjusted_posture": posture,
        "risk_adjusted_reason": reason,
        "suggested_position_size_multiplier": round(clamp(position_multiplier, 0.0, 2.0), 6),
        "suggested_grid_aggression_multiplier": round(
            clamp(
                numeric_or_default(bot_tuning.get("grid_aggression_multiplier"), 1.0),
                0.1,
                3.0,
            ),
            6,
        ),
        "suggested_entry_discount_multiplier": round(
            clamp(
                numeric_or_default(
                    bot_tuning.get(
                        "entry_discount_multiplier",
                        risk_context.get("entry_discount_multiplier")
                    ),
                    1.0
                ),
                0.0,
                3.0,
            ),
            6,
        ),
        "suggested_take_profit_multiplier": round(
            clamp(
                numeric_or_default(
                    bot_tuning.get(
                        "target_profit_multiplier",
                        risk_context.get("target_profit_multiplier")
                    ),
                    1.0
                ),
                0.1,
                3.0,
            ),
            6,
        ),
        "risk_context_source_processed_at": risk_context_source_processed_at(
            risk_context,
            fallback_processed_at
        ),
        "risk_context_age_minutes": risk_context_age_minutes(
            risk_context,
            fallback_processed_at,
            now=now,
        ),
    }
