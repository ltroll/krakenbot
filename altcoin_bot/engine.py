from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from .audit import append_jsonl
from .config import Strategy
from .models import Evaluation, PairRules, ScoutDecision
from .store import Store


BPS = Decimal("10000")


def deterministic_client_order_id(decision_id: str, asset: str) -> str:
    digest = hashlib.sha256(f"{asset}:{decision_id}".encode()).hexdigest()[:16]
    return "ak" + digest


class Engine:
    def __init__(self, store: Store, strategy: Strategy, mode: str, event_file=None):
        if mode not in {"observer", "dry_run"}:
            raise ValueError("live execution is intentionally unavailable")
        self.store = store
        self.strategy = strategy
        self.mode = mode
        self.event_file = event_file

    def emit(self, event_type: str, severity: str = "info", decision_id: Optional[str] = None,
             **details: Any) -> None:
        record = self.store.event(event_type, severity, decision_id, **details)
        if self.event_file:
            append_jsonl(self.event_file, record)

    def reconcile_local(self) -> bool:
        """Repair restart-safe dry-run protection; return True on unsafe mismatch."""
        positions = {p["decision_id"]: p for p in self.store.open_positions()}
        take_profits = {o["decision_id"]: o for o in self.store.active_orders()
                        if o["kind"] == "take_profit"}
        mismatch = False
        for decision_id, order in take_profits.items():
            if decision_id not in positions:
                self.store.update_order(order["client_order_id"], "cancelled",
                                        Decimal(order["filled_quantity"]))
                self.emit("restart_orphan_order_cancelled", "warning", decision_id,
                          client_order_id=order["client_order_id"])
        for decision_id, position in positions.items():
            if decision_id in take_profits:
                continue
            if self.mode != "dry_run":
                mismatch = True
                self.emit("exchange_state_mismatch", "critical", decision_id,
                          reason="open_position_without_take_profit")
                continue
            entry = Decimal(position["entry_price"])
            price = entry * (Decimal("1") + self.strategy.d("take_profit_pct"))
            client_id = deterministic_client_order_id(decision_id, position["asset"]) + "t"
            self.store.upsert_take_profit(client_id, decision_id, position["asset"], price,
                                          Decimal(position["remaining_quantity"]))
            self.emit("restart_take_profit_restored", "warning", decision_id,
                      client_order_id=client_id, simulated=True)
        return mismatch

    def evaluate(self, decision: ScoutDecision, rules: PairRules, now: datetime,
                 kill_switch: bool = False, exchange_mismatch: bool = False,
                 consecutive_errors: int = 0, current_ask: Optional[Decimal] = None) -> Evaluation:
        failures = []
        plan = decision.entry_plan
        age = now.astimezone(timezone.utc) - decision.generated_at
        maximum_age = timedelta(minutes=float(self.strategy.raw["maximum_signal_age_minutes"]))
        if decision.status != "ok": failures.append("status_not_ok")
        if age.total_seconds() < 0: failures.append("source_timestamp_in_future")
        if age > maximum_age: failures.append("stale_source")
        if not self.strategy.asset_enabled(decision.asset): failures.append("asset_disabled")
        if decision.decision != "allow_scout_long": failures.append("decision_not_allow_scout_long")
        if plan is None:
            failures.append("missing_limit_entry_plan")
        elif plan.expires_at <= now:
            failures.append("expired_limit_entry_plan")
        elif plan.expires_at > now + timedelta(minutes=float(self.strategy.raw.get("entry_plan_max_rest_minutes", 20))):
            failures.append("entry_plan_expiry_too_distant")
        if plan is not None and current_ask is not None and plan.limit_price >= current_ask:
            failures.append("post_only_would_cross")
        if kill_switch: failures.append("global_kill_switch")
        if exchange_mismatch: failures.append("exchange_state_mismatch")
        if consecutive_errors >= int(self.strategy.limits["maximum_consecutive_errors"]):
            failures.append("consecutive_error_circuit_breaker")
        if self.store.decision_exists(decision.decision_id): failures.append("duplicate_decision")

        open_positions = self.store.open_positions()
        active_entry_orders = [o for o in self.store.active_orders() if o["kind"] == "entry"]
        if len(open_positions) + len(active_entry_orders) >= int(self.strategy.limits["maximum_open_positions"]):
            failures.append("maximum_open_positions")
        today = now.date().isoformat()
        if self.store.filled_entries_today(today) >= int(self.strategy.limits["maximum_filled_trades_per_utc_day"]):
            failures.append("daily_trade_count")
        daily_pnl = self.store.realized_pnl_today(today)
        if daily_pnl <= -Decimal(str(self.strategy.limits["maximum_daily_loss_usd"])):
            failures.append("daily_loss_circuit_breaker")
        last_closed = self.store.last_closed_at(decision.asset)
        if last_closed:
            closed_at = datetime.fromisoformat(last_closed)
            cooldown = timedelta(minutes=float(self.strategy.limits["cooldown_minutes"]))
            if now - closed_at < cooldown: failures.append("cooldown_active")

        expected_profit = Decimal("0")
        expected_pct = Decimal("0")
        client_id = deterministic_client_order_id(decision.decision_id, decision.asset)
        if plan:
            failures.extend(rules.validate(plan.limit_price, plan.quantity))
            notional = plan.limit_price * plan.quantity
            if notional > Decimal(str(self.strategy.limits["maximum_position_usd"])):
                failures.append("maximum_position_notional")
            tp_price = plan.limit_price * (Decimal("1") + self.strategy.d("take_profit_pct"))
            gross = (tp_price - plan.limit_price) * plan.quantity
            fees = self.strategy.raw["fees_bps"]
            entry_fee = notional * Decimal(str(fees["maker_entry"])) / BPS
            tp_fee = tp_price * plan.quantity * Decimal(str(fees["maker_take_profit"])) / BPS
            market_cost = notional * (plan.spread_bps + plan.slippage_bps) / BPS
            expected_profit = gross - entry_fee - tp_fee - market_cost
            expected_pct = expected_profit / notional
            if expected_profit <= 0: failures.append("non_positive_expected_net_profit")
        return Evaluation(not failures, tuple(dict.fromkeys(failures)), expected_profit,
                          expected_pct, client_id)

    def process_decision(self, decision: ScoutDecision, rules: PairRules, now: datetime,
                         kill_switch: bool = False, exchange_mismatch: bool = False,
                         consecutive_errors: int = 0,
                         current_ask: Optional[Decimal] = None) -> Evaluation:
        evaluation = self.evaluate(decision, rules, now, kill_switch, exchange_mismatch,
                                   consecutive_errors, current_ask)
        details = {"asset": decision.asset, "allowed": evaluation.allowed,
                   "failures": evaluation.failures,
                   "expected_net_profit": str(evaluation.expected_net_profit),
                   "expected_net_profit_pct": str(evaluation.expected_net_profit_pct), "mode": self.mode}
        if not evaluation.allowed:
            self.store.record_decision(decision.decision_id, decision.asset,
                                       decision.generated_at.isoformat(), "rejected",
                                       evaluation.failures, evaluation.client_order_id)
            self.emit("decision_rejected", "warning", decision.decision_id, **details)
            return evaluation
        if self.mode == "observer":
            self.store.record_decision(decision.decision_id, decision.asset,
                                       decision.generated_at.isoformat(), "observed",
                                       (), evaluation.client_order_id)
            self.emit("decision_observed", decision_id=decision.decision_id, **details)
            return evaluation
        plan = decision.entry_plan
        assert plan is not None
        accepted = self.store.accept_candidate(decision.decision_id, decision.asset,
                                               decision.generated_at.isoformat(),
                                               evaluation.client_order_id,
                                               plan.limit_price, plan.quantity,
                                               plan.expires_at.isoformat())
        if not accepted:
            self.emit("decision_duplicate_race", "warning", decision.decision_id)
            return Evaluation(False, ("duplicate_decision",), evaluation.expected_net_profit,
                              evaluation.expected_net_profit_pct, evaluation.client_order_id)
        self.emit("entry_order_created", decision_id=decision.decision_id,
                  client_order_id=evaluation.client_order_id, post_only=True,
                  price=str(plan.limit_price), quantity=str(plan.quantity), simulated=True)
        return evaluation

    def advance_dry_run(self, bid: Decimal, ask: Decimal, now: datetime) -> None:
        if self.mode != "dry_run":
            return
        fraction = Decimal(str(self.strategy.raw["dry_run"]["entry_fill_fraction_per_cycle"]))
        for order in list(self.store.active_orders()):
            if order["kind"] != "entry":
                continue
            if order.get("expires_at") and datetime.fromisoformat(order["expires_at"]) <= now:
                self.store.update_order(order["client_order_id"], "cancelled",
                                        Decimal(order["filled_quantity"]))
                self.emit("entry_order_cancelled", decision_id=order["decision_id"],
                          reason="entry_plan_expired", simulated=True)
                continue
            price = Decimal(order["price"])
            if ask > price:
                continue
            quantity = Decimal(order["quantity"])
            already = Decimal(order["filled_quantity"])
            fill = min(quantity - already, quantity * fraction)
            if fill <= 0:
                continue
            total_filled = already + fill
            status = "filled" if total_filled >= quantity else "partial"
            self.store.update_order(order["client_order_id"], status, total_filled)
            self.store.create_or_grow_position(order["decision_id"], order["asset"], price, fill, now.isoformat())
            self.emit("entry_fill", decision_id=order["decision_id"], quantity=str(fill),
                      cumulative_quantity=str(total_filled), status=status, simulated=True)
            tp_price = price * (Decimal("1") + self.strategy.d("take_profit_pct"))
            tp_id = order["client_order_id"] + "t"
            self.store.upsert_take_profit(tp_id, order["decision_id"], order["asset"],
                                          tp_price, total_filled)
            self.emit("take_profit_order_created", decision_id=order["decision_id"],
                      client_order_id=tp_id, price=str(tp_price), quantity=str(total_filled),
                      post_only=False, resting_limit=True, simulated=True)
        self._advance_exits(bid, now)

    def _advance_exits(self, bid: Decimal, now: datetime) -> None:
        for position in list(self.store.open_positions()):
            entry = Decimal(position["entry_price"])
            quantity = Decimal(position["remaining_quantity"])
            age = now - datetime.fromisoformat(position["opened_at"])
            tp = entry * (Decimal("1") + self.strategy.d("take_profit_pct"))
            stop = entry * (Decimal("1") - self.strategy.d("stop_loss_pct"))
            reason = None
            exit_price = bid
            fee_bps = Decimal(str(self.strategy.raw["fees_bps"]["taker_exit"]))
            if bid >= tp:
                reason, exit_price = "take_profit", tp
                fee_bps = Decimal(str(self.strategy.raw["fees_bps"]["maker_take_profit"]))
            elif bid <= stop:
                reason = "stop"
            elif age >= timedelta(minutes=float(self.strategy.raw["maximum_hold_minutes"])):
                reason = "maximum_hold"
            if not reason:
                continue
            if reason == "take_profit":
                for order in self.store.active_orders():
                    if order["decision_id"] == position["decision_id"] and order["kind"] == "take_profit":
                        self.store.update_order(order["client_order_id"], "filled", quantity)
            else:
                self.store.cancel_active_orders(position["decision_id"], ("take_profit",))
                exit_id = deterministic_client_order_id(position["decision_id"], position["asset"]) + "x"
                self.store.create_order(exit_id, position["decision_id"], position["asset"],
                                        "stop" if reason == "stop" else "forced_exit", exit_price, quantity,
                                        status="filled")
                self.store.update_order(exit_id, "filled", quantity)
            entry_fee = entry * quantity * Decimal(str(self.strategy.raw["fees_bps"]["maker_entry"])) / BPS
            exit_fee = exit_price * quantity * fee_bps / BPS
            pnl = (exit_price - entry) * quantity - entry_fee - exit_fee
            self.store.close_position(position["decision_id"], exit_price, reason, pnl, now.isoformat())
            self.emit("position_closed", decision_id=position["decision_id"], reason=reason,
                      execution="maker" if reason == "take_profit" else "taker",
                      exit_price=str(exit_price), realized_net_pnl=str(pnl), simulated=True)
