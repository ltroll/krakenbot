#!/usr/bin/env python3


def numeric_or_none(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def effective_round_trip_fee_pct(config, fallback=None):
    if not isinstance(config, dict):
        return fallback

    maker_fee_pct = numeric_or_none(config.get("maker_fee_pct"))
    taker_fee_pct = numeric_or_none(config.get("taker_fee_pct"))
    if maker_fee_pct is not None and taker_fee_pct is not None:
        return maker_fee_pct + taker_fee_pct

    round_trip_fee_pct = numeric_or_none(config.get("round_trip_fee_pct"))
    if round_trip_fee_pct is not None:
        return round_trip_fee_pct

    return fallback
