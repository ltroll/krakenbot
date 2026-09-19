#!/usr/bin/env python3

"""Persistent operator controls shared by the range-grid bot and HTTP UI."""

import json
import os
import re
from datetime import datetime, timezone

from range_grid_order_safety import atomic_write_json, load_json_with_backup


CONTROL_SCHEMA_VERSION = 1
MAX_CONTROL_TARGETS = 8
MIN_BUY_PRICE_USD = 1.0
MAX_BUY_PRICE_USD = 10_000_000.0
MIN_PROFIT_TARGET_PCT = 0.0
MAX_PROFIT_TARGET_PCT = 0.25
TARGET_ID_PATTERN = re.compile(r"[^a-zA-Z0-9_-]+")


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
        "manual_targets_enabled": False,
        "buy_targets": [],
        "updated_at": None,
        "updated_by": None,
    }


def _bool_value(value, default=False):
    if isinstance(value, bool):
        return value
    if value is None:
        return bool(default)
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _float_value(value, field):
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ControlStateError(f"{field} must be numeric") from exc


def _target_id(value, index):
    normalized = TARGET_ID_PATTERN.sub("-", str(value or "").strip()).strip("-")
    return normalized[:48] or f"target-{index + 1}"


def normalize_buy_targets(raw_targets):
    if raw_targets is None:
        return []
    if not isinstance(raw_targets, list):
        raise ControlStateError("buy_targets must be a list")
    if len(raw_targets) > MAX_CONTROL_TARGETS:
        raise ControlStateError(
            f"buy_targets cannot contain more than {MAX_CONTROL_TARGETS} targets"
        )

    normalized = []
    seen_ids = set()
    for index, raw_target in enumerate(raw_targets):
        if not isinstance(raw_target, dict):
            raise ControlStateError(f"buy_targets[{index}] must be an object")

        target_id = _target_id(raw_target.get("id"), index)
        if target_id in seen_ids:
            raise ControlStateError(f"duplicate buy target id: {target_id}")
        seen_ids.add(target_id)

        buy_price = _float_value(
            raw_target.get("buy_price"),
            f"buy_targets[{index}].buy_price",
        )
        if not MIN_BUY_PRICE_USD <= buy_price <= MAX_BUY_PRICE_USD:
            raise ControlStateError(
                f"buy_targets[{index}].buy_price must be between "
                f"{MIN_BUY_PRICE_USD} and {MAX_BUY_PRICE_USD}"
            )

        profit_target_pct = _float_value(
            raw_target.get("profit_target_pct"),
            f"buy_targets[{index}].profit_target_pct",
        )
        if not MIN_PROFIT_TARGET_PCT <= profit_target_pct <= MAX_PROFIT_TARGET_PCT:
            raise ControlStateError(
                f"buy_targets[{index}].profit_target_pct must be between "
                f"{MIN_PROFIT_TARGET_PCT} and {MAX_PROFIT_TARGET_PCT}"
            )

        label = str(raw_target.get("label") or f"Target {index + 1}").strip()
        normalized.append({
            "id": target_id,
            "label": label[:80],
            "enabled": _bool_value(raw_target.get("enabled"), True),
            "buy_price": round(buy_price, 2),
            "profit_target_pct": round(profit_target_pct, 8),
        })
    return normalized


def normalize_control_state(payload):
    if payload is None:
        return default_control_state()
    if not isinstance(payload, dict):
        raise ControlStateError("control state must be a JSON object")

    manual_targets_enabled = _bool_value(
        payload.get("manual_targets_enabled"),
        False,
    )
    buy_targets = normalize_buy_targets(payload.get("buy_targets", []))
    if manual_targets_enabled and not any(
        target["enabled"] for target in buy_targets
    ):
        raise ControlStateError(
            "manual target mode requires at least one enabled buy target"
        )

    normalized = default_control_state()
    normalized.update({
        "revision": max(0, int(payload.get("revision", 0) or 0)),
        "buying_paused": _bool_value(payload.get("buying_paused"), False),
        "cancel_open_buys_on_hold": _bool_value(
            payload.get("cancel_open_buys_on_hold"),
            True,
        ),
        "manual_targets_enabled": manual_targets_enabled,
        "buy_targets": buy_targets,
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
        "manual_targets_enabled",
        "buy_targets",
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


def active_buy_targets(state):
    normalized = normalize_control_state(state)
    if not normalized["manual_targets_enabled"]:
        return []
    return [target for target in normalized["buy_targets"] if target["enabled"]]


def operator_grid_slot(target_id):
    return f"operator:{_target_id(target_id, 0)}"


def _positive_float(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _non_negative_float(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def operator_buy_cancel_reason(state, order, *, price_decimals=2):
    """Return why a pending buy conflicts with the current operator state."""
    normalized = normalize_control_state(state)
    order = order if isinstance(order, dict) else {}
    grid_slot = str(order.get("grid_slot") or "")
    operator_order = bool(order.get("operator_controlled")) or (
        grid_slot.startswith("operator:")
    )

    if (
        normalized["buying_paused"]
        and normalized["cancel_open_buys_on_hold"]
    ):
        return "operator_buy_hold"
    if normalized["manual_targets_enabled"] and not operator_order:
        return "operator_manual_mode_replaces_automatic"
    if not normalized["manual_targets_enabled"] and operator_order:
        return "operator_manual_mode_disabled"
    if not normalized["manual_targets_enabled"]:
        return None

    desired_targets = {
        operator_grid_slot(target["id"]): target
        for target in active_buy_targets(normalized)
    }
    desired = desired_targets.get(grid_slot)
    if desired is None:
        return "operator_target_disabled"

    order_price = _positive_float(order.get("price"))
    desired_price = _positive_float(desired.get("buy_price"))
    if (
        order_price is None
        or desired_price is None
        or round(order_price, price_decimals)
        != round(desired_price, price_decimals)
    ):
        return "operator_target_price_changed"

    order_profit = _non_negative_float(order.get("sell_pct_override"))
    desired_profit = _non_negative_float(desired.get("profit_target_pct"))
    if (
        order_profit is None
        or desired_profit is None
        or round(order_profit, 8) != round(desired_profit, 8)
    ):
        return "operator_target_profit_changed"
    return None


def control_status(state):
    normalized = normalize_control_state(state)
    return {
        "revision": normalized["revision"],
        "buying_paused": normalized["buying_paused"],
        "cancel_open_buys_on_hold": normalized["cancel_open_buys_on_hold"],
        "manual_targets_enabled": normalized["manual_targets_enabled"],
        "active_target_count": len(active_buy_targets(normalized)),
        "buy_targets": normalized["buy_targets"],
        "updated_at": normalized["updated_at"],
        "updated_by": normalized["updated_by"],
        "load_error": state.get("load_error") if isinstance(state, dict) else None,
    }
