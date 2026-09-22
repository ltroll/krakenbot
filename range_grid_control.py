#!/usr/bin/env python3

"""Persistent operator controls shared by the range-grid bot and HTTP UI."""

import json
import os
from datetime import datetime, timezone

from range_grid_order_safety import atomic_write_json, load_json_with_backup
from range_grid_strategy_catalog import normalize_strategy_filename


CONTROL_SCHEMA_VERSION = 4
MIN_BUY_PRICE_USD = 1.0
MAX_BUY_PRICE_USD = 10_000_000.0
MAX_NET_PROFIT_TARGET_PCT = 0.25


class ControlStateError(ValueError):
    pass


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def default_control_state():
    return {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "revision": 0,
        "buying_paused": False,
        "cancel_open_buys_on_hold": True,
        "buy_price_floor_enabled": False,
        "buy_price_floor_usd": None,
        "buy_price_ceiling_enabled": False,
        "buy_price_ceiling_usd": None,
        "profit_target_override_enabled": False,
        "net_profit_target_pct": None,
        "strategy_profile_override": None,
        "updated_at": None,
        "updated_by": None,
    }


def _bool_value(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _optional_price(value, field, enabled):
    if value in (None, ""):
        if enabled:
            raise ControlStateError(f"{field} is required while enabled")
        return None
    try:
        price = float(value)
    except (TypeError, ValueError) as exc:
        raise ControlStateError(f"{field} must be numeric") from exc
    if not MIN_BUY_PRICE_USD <= price <= MAX_BUY_PRICE_USD:
        raise ControlStateError(
            f"{field} must be between "
            f"{MIN_BUY_PRICE_USD} and {MAX_BUY_PRICE_USD}"
        )
    return round(price, 2)


def _optional_net_profit_target(value, enabled):
    if value in (None, ""):
        if enabled:
            raise ControlStateError(
                "net_profit_target_pct is required while enabled"
            )
        return None
    try:
        target = float(value)
    except (TypeError, ValueError) as exc:
        raise ControlStateError(
            "net_profit_target_pct must be numeric"
        ) from exc
    if not 0.0 <= target <= MAX_NET_PROFIT_TARGET_PCT:
        raise ControlStateError(
            "net_profit_target_pct must be between 0 and "
            f"{MAX_NET_PROFIT_TARGET_PCT}"
        )
    return round(target, 8)


def normalize_control_state(payload):
    if payload is None:
        return default_control_state()
    if not isinstance(payload, dict):
        raise ControlStateError("control state must be a JSON object")

    legacy_prices = []
    if _bool_value(payload.get("manual_targets_enabled"), False):
        for target in payload.get("buy_targets", []):
            if not isinstance(target, dict):
                continue
            if not _bool_value(target.get("enabled"), True):
                continue
            try:
                legacy_price = float(target.get("buy_price"))
            except (TypeError, ValueError):
                continue
            if MIN_BUY_PRICE_USD <= legacy_price <= MAX_BUY_PRICE_USD:
                legacy_prices.append(legacy_price)

    floor_enabled = _bool_value(
        payload.get("buy_price_floor_enabled"),
        bool(legacy_prices),
    )
    ceiling_enabled = _bool_value(
        payload.get("buy_price_ceiling_enabled"),
        bool(legacy_prices),
    )
    floor_price = _optional_price(
        payload.get(
            "buy_price_floor_usd",
            min(legacy_prices) if legacy_prices else None,
        ),
        "buy_price_floor_usd",
        floor_enabled,
    )
    ceiling_price = _optional_price(
        payload.get(
            "buy_price_ceiling_usd",
            max(legacy_prices) if legacy_prices else None,
        ),
        "buy_price_ceiling_usd",
        ceiling_enabled,
    )
    if (
        floor_enabled
        and ceiling_enabled
        and floor_price > ceiling_price
    ):
        raise ControlStateError(
            "buy_price_floor_usd cannot be above buy_price_ceiling_usd"
        )

    profit_target_override_enabled = _bool_value(
        payload.get("profit_target_override_enabled"),
        False,
    )
    net_profit_target_pct = _optional_net_profit_target(
        payload.get("net_profit_target_pct"),
        profit_target_override_enabled,
    )

    try:
        strategy_profile_override = normalize_strategy_filename(
            payload.get("strategy_profile_override")
        )
    except ValueError as exc:
        raise ControlStateError(str(exc)) from exc

    normalized = default_control_state()
    normalized.update({
        "revision": max(0, int(payload.get("revision", 0) or 0)),
        "buying_paused": _bool_value(payload.get("buying_paused"), False),
        "cancel_open_buys_on_hold": _bool_value(
            payload.get("cancel_open_buys_on_hold"),
            True,
        ),
        "buy_price_floor_enabled": floor_enabled,
        "buy_price_floor_usd": floor_price,
        "buy_price_ceiling_enabled": ceiling_enabled,
        "buy_price_ceiling_usd": ceiling_price,
        "profit_target_override_enabled": (
            profit_target_override_enabled
        ),
        "net_profit_target_pct": net_profit_target_pct,
        "strategy_profile_override": strategy_profile_override,
        "updated_at": payload.get("updated_at"),
        "updated_by": payload.get("updated_by"),
    })
    return normalized


def merge_control_update(current, update, *, updated_by=None):
    if not isinstance(update, dict):
        raise ControlStateError("control update must be a JSON object")
    current = normalize_control_state(current)
    allowed_fields = {
        "buying_paused",
        "cancel_open_buys_on_hold",
        "buy_price_floor_enabled",
        "buy_price_floor_usd",
        "buy_price_ceiling_enabled",
        "buy_price_ceiling_usd",
        "profit_target_override_enabled",
        "net_profit_target_pct",
        "strategy_profile_override",
    }
    merged = dict(current)
    for key in allowed_fields:
        if key in update:
            merged[key] = update[key]
    merged["revision"] = current["revision"] + 1
    merged["updated_at"] = utc_now_iso()
    merged["updated_by"] = str(updated_by or "http")[:120]
    return normalize_control_state(merged)


def load_control_state(path):
    resolved = os.path.abspath(os.path.expanduser(path))
    payload, _, _ = load_json_with_backup(resolved, f"{resolved}.bak")
    return normalize_control_state(payload)


def load_control_state_fail_safe(path):
    try:
        state = load_control_state(path)
        state["load_error"] = None
        return state
    except Exception as exc:
        state = default_control_state()
        state.update({
            "buying_paused": True,
            "load_error": str(exc),
        })
        return state


def save_control_state(path, state):
    resolved = os.path.abspath(os.path.expanduser(path))
    normalized = normalize_control_state(state)
    atomic_write_json(resolved, normalized, f"{resolved}.bak")
    return normalized


def _positive_float(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def operator_buy_price_rule_reason(state, price):
    normalized = normalize_control_state(state)
    if not (
        normalized["buy_price_floor_enabled"]
        or normalized["buy_price_ceiling_enabled"]
    ):
        return None
    order_price = _positive_float(price)
    if order_price is None:
        return "operator_buy_price_missing"
    if (
        normalized["buy_price_floor_enabled"]
        and order_price < normalized["buy_price_floor_usd"]
    ):
        return "operator_buy_price_below_floor"
    if (
        normalized["buy_price_ceiling_enabled"]
        and order_price > normalized["buy_price_ceiling_usd"]
    ):
        return "operator_buy_price_above_ceiling"
    return None


def operator_buy_cancel_reason(state, order):
    """Return why a pending buy conflicts with the current operator state."""
    normalized = normalize_control_state(state)
    order = order if isinstance(order, dict) else {}
    if (
        normalized["buying_paused"]
        and normalized["cancel_open_buys_on_hold"]
    ):
        return "operator_buy_hold"
    return operator_buy_price_rule_reason(normalized, order.get("price"))


def operator_net_profit_target_pct(state):
    """Return the exact operator net target, or None when strategy-controlled."""
    normalized = normalize_control_state(state)
    if not normalized["profit_target_override_enabled"]:
        return None
    return normalized["net_profit_target_pct"]


def control_status(state):
    normalized = normalize_control_state(state)
    return {
        "revision": normalized["revision"],
        "buying_paused": normalized["buying_paused"],
        "cancel_open_buys_on_hold": normalized["cancel_open_buys_on_hold"],
        "buy_price_floor_enabled": normalized["buy_price_floor_enabled"],
        "buy_price_floor_usd": normalized["buy_price_floor_usd"],
        "buy_price_ceiling_enabled": normalized["buy_price_ceiling_enabled"],
        "buy_price_ceiling_usd": normalized["buy_price_ceiling_usd"],
        "profit_target_override_enabled": normalized[
            "profit_target_override_enabled"
        ],
        "net_profit_target_pct": normalized["net_profit_target_pct"],
        "strategy_profile_override": normalized[
            "strategy_profile_override"
        ],
        "updated_at": normalized["updated_at"],
        "updated_by": normalized["updated_by"],
        "load_error": state.get("load_error") if isinstance(state, dict) else None,
    }
