"""Asset-instance isolation and safety helpers for the range-grid services."""

import os
import re

from range_grid_assets import (
    infer_asset_id_from_pair,
    kraken_pair_matches,
    normalize_kraken_pair,
)


INSTANCE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,31}$")


def normalize_instance_id(value):
    if value in (None, ""):
        return ""
    normalized = str(value).strip().lower()
    if not INSTANCE_ID_PATTERN.fullmatch(normalized):
        raise ValueError(
            "RANGE_GRID_INSTANCE_ID must contain only lowercase letters, "
            "numbers, underscores, and hyphens"
        )
    return normalized


def instance_runtime_path(instance_id, filename, runtime_root="instances"):
    normalized = normalize_instance_id(instance_id)
    if not normalized:
        return filename
    return os.path.join(runtime_root, normalized, filename)


def instance_web_path(
    instance_id,
    filename,
    *,
    legacy_path,
    web_root="/var/www/html/bot",
):
    normalized = normalize_instance_id(instance_id)
    if not normalized:
        return legacy_path
    return os.path.join(web_root, normalized, filename)


def parse_optional_positive_float(value, name):
    if value in (None, ""):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return parsed


def effective_inventory_cap(configured_cap, capital_allocation):
    configured = float(configured_cap)
    if capital_allocation is None:
        return configured
    return min(configured, float(capital_allocation))


def available_quote_cash(quote_balance, reserved_buy_cash, cash_reserve=0.0):
    return max(
        0.0,
        float(quote_balance or 0.0)
        - float(reserved_buy_cash or 0.0)
        - max(0.0, float(cash_reserve or 0.0)),
    )


def price_from_record(record, asset_id, configured_field="asset_price_usd"):
    if not isinstance(record, dict):
        return None
    normalized_asset = str(asset_id or "").strip().lower()
    candidates = [
        configured_field,
        "asset_price_usd",
        f"{normalized_asset}_price_usd" if normalized_asset else None,
        "price_usd",
        "price",
        "btc_price_usd",
    ]
    for field in candidates:
        if not field or field not in record:
            continue
        try:
            price = float(record[field])
        except (TypeError, ValueError):
            continue
        if price > 0:
            return price
    return None


def strategy_asset_id(strategy):
    if not isinstance(strategy, dict):
        return ""
    return str(
        strategy.get("asset_id")
        or strategy.get("signal_asset_id")
        or ""
    ).strip().upper()


def strategy_pair(strategy):
    if not isinstance(strategy, dict):
        return ""
    return str(strategy.get("kraken_pair") or strategy.get("pair") or "").strip()


def validate_instance_configuration(
    *,
    instance_id,
    asset_id,
    kraken_pair,
    strategy,
    order_tracker_symbol=None,
    paper_trading_enabled=True,
    live_enabled=False,
    live_confirmation="",
    require_instance_id_for_non_btc=True,
):
    errors = []
    normalized_instance = normalize_instance_id(instance_id)
    normalized_asset = str(asset_id or "").strip().upper()
    normalized_pair = normalize_kraken_pair(kraken_pair)
    inferred_asset = infer_asset_id_from_pair(normalized_pair)

    if not normalized_asset:
        errors.append("SIGNAL_ASSET_ID is required")
    if not normalized_pair:
        errors.append("KRAKEN_PAIR is required")
    if (
        normalized_asset
        and inferred_asset
        and inferred_asset != normalized_asset
    ):
        errors.append(
            f"SIGNAL_ASSET_ID={normalized_asset} does not match "
            f"KRAKEN_PAIR={kraken_pair}"
        )
    if (
        require_instance_id_for_non_btc
        and normalized_asset
        and normalized_asset != "BTC"
        and not normalized_instance
    ):
        errors.append(
            "RANGE_GRID_INSTANCE_ID is required for non-BTC instances"
        )

    declared_asset = strategy_asset_id(strategy)
    if normalized_asset != "BTC" and not declared_asset:
        errors.append(
            "non-BTC strategy profiles must declare asset_id"
        )
    elif declared_asset and declared_asset != normalized_asset:
        errors.append(
            f"strategy asset_id={declared_asset} does not match "
            f"SIGNAL_ASSET_ID={normalized_asset}"
        )

    declared_pair = strategy_pair(strategy)
    if declared_pair and not kraken_pair_matches(
        declared_pair,
        normalized_pair,
        asset_id=normalized_asset,
    ):
        errors.append(
            f"strategy kraken_pair={declared_pair} does not match "
            f"KRAKEN_PAIR={kraken_pair}"
        )

    tracker_symbol = normalize_kraken_pair(order_tracker_symbol)
    if tracker_symbol and not kraken_pair_matches(
        tracker_symbol,
        normalized_pair,
        asset_id=normalized_asset,
    ):
        errors.append(
            f"order tracker symbol {order_tracker_symbol} does not match "
            f"asset {normalized_asset}"
        )

    if not paper_trading_enabled:
        if not live_enabled:
            errors.append(
                "live strategy requires RANGE_GRID_LIVE_ENABLED=true"
            )
        if normalize_kraken_pair(live_confirmation) != normalized_pair:
            errors.append(
                "live strategy requires RANGE_GRID_LIVE_CONFIRMATION to "
                f"equal {kraken_pair}"
            )

    return errors
