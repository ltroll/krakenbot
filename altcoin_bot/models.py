from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Tuple


SUPPORTED_SCHEMA_VERSIONS = {"asset-scout-decision-v1"}


class ContractError(ValueError):
    pass


def parse_time(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ContractError(f"{field} must be a non-empty ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContractError(f"{field} is not valid ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ContractError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def positive_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ContractError(f"{field} must be numeric")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ContractError(f"{field} must be numeric") from exc
    if not number.is_finite() or number <= 0:
        raise ContractError(f"{field} must be > 0")
    return number


def nonnegative_decimal(value: Any, field: str) -> Decimal:
    if isinstance(value, bool):
        raise ContractError(f"{field} must be numeric")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ContractError(f"{field} must be numeric") from exc
    if not number.is_finite() or number < 0:
        raise ContractError(f"{field} must be >= 0")
    return number


@dataclass(frozen=True)
class EntryPlan:
    limit_price: Decimal
    quantity: Decimal
    expires_at: datetime
    reference_price: Decimal
    spread_bps: Decimal
    slippage_bps: Decimal


@dataclass(frozen=True)
class ScoutDecision:
    schema_version: str
    decision_id: str
    status: str
    generated_at: datetime
    asset: str
    decision: str
    entry_plan: Optional[EntryPlan]

    @classmethod
    def parse(cls, payload: Any) -> "ScoutDecision":
        if not isinstance(payload, dict):
            raise ContractError("payload must be a JSON object")
        schema = payload.get("schema_version")
        if schema not in SUPPORTED_SCHEMA_VERSIONS:
            raise ContractError(f"unsupported schema_version: {schema!r}")
        decision_id = payload.get("decision_id")
        if not isinstance(decision_id, str) or not decision_id.strip():
            raise ContractError("decision_id must be a non-empty string")
        asset = payload.get("asset")
        if isinstance(asset, dict):
            asset = asset.get("symbol")
        if not isinstance(asset, str) or not asset.strip():
            raise ContractError("asset must be a symbol string")
        raw_plan = payload.get("limit_entry_plan")
        plan = None
        if raw_plan is not None:
            if not isinstance(raw_plan, dict):
                raise ContractError("limit_entry_plan must be an object")
            plan = EntryPlan(
                limit_price=positive_decimal(raw_plan.get("limit_price"), "limit_entry_plan.limit_price"),
                quantity=positive_decimal(raw_plan.get("quantity"), "limit_entry_plan.quantity"),
                expires_at=parse_time(raw_plan.get("expires_at"), "limit_entry_plan.expires_at"),
                reference_price=positive_decimal(raw_plan.get("reference_price"), "limit_entry_plan.reference_price"),
                spread_bps=nonnegative_decimal(raw_plan.get("spread_bps", 0), "limit_entry_plan.spread_bps"),
                slippage_bps=nonnegative_decimal(raw_plan.get("slippage_bps", 0), "limit_entry_plan.slippage_bps"),
            )
        return cls(
            schema_version=schema,
            decision_id=decision_id.strip(),
            status=str(payload.get("status", "")),
            generated_at=parse_time(payload.get("generated_at"), "generated_at"),
            asset=asset.strip().upper(),
            decision=str(payload.get("decision", "")),
            entry_plan=plan,
        )


@dataclass(frozen=True)
class PairRules:
    pair: str
    price_decimals: int
    quantity_decimals: int
    minimum_quantity: Decimal
    minimum_notional: Decimal

    def validate(self, price: Decimal, quantity: Decimal) -> Tuple[str, ...]:
        failures = []
        if quantity < self.minimum_quantity:
            failures.append("below_minimum_quantity")
        if price * quantity < self.minimum_notional:
            failures.append("below_minimum_notional")
        if price != price.quantize(Decimal(1).scaleb(-self.price_decimals)):
            failures.append("invalid_price_precision")
        if quantity != quantity.quantize(Decimal(1).scaleb(-self.quantity_decimals)):
            failures.append("invalid_quantity_precision")
        return tuple(failures)


@dataclass(frozen=True)
class Evaluation:
    allowed: bool
    failures: Tuple[str, ...]
    expected_net_profit: Decimal
    expected_net_profit_pct: Decimal
    client_order_id: Optional[str]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
