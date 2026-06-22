import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from altcoin_bot.config import Strategy
from altcoin_bot.engine import Engine, deterministic_client_order_id
from altcoin_bot.models import ContractError, PairRules, ScoutDecision
from altcoin_bot.store import Store


NOW = datetime(2026, 6, 22, 14, 0, tzinfo=timezone.utc)


def payload(decision_id="sol-1", generated_at=None, expires_at=None, **updates):
    value = {
        "schema_version": "asset-scout-decision-v1",
        "decision_id": decision_id,
        "status": "ok",
        "generated_at": (generated_at or NOW).isoformat(),
        "asset": "SOL",
        "decision": "allow_scout_long",
        "limit_entry_plan": {
            "limit_price": "100.00", "quantity": "0.200",
            "reference_price": "100.05", "spread_bps": "0",
            "slippage_bps": "0",
            "expires_at": (expires_at or NOW + timedelta(minutes=15)).isoformat(),
        },
        "adaptive_policy_overlay": {"decision": "allow_scout_long", "shadow_only": True},
    }
    value.update(updates)
    return value


def strategy(fill_fraction=1):
    return Strategy({
        "assets": {"SOL": {"enabled": True}, "ETH": {"enabled": False}},
        "take_profit_pct": 0.015, "stop_loss_pct": 0.005,
        "maximum_hold_minutes": 1440, "maximum_signal_age_minutes": 20,
        "fees_bps": {"maker_entry": 40, "maker_take_profit": 40, "taker_exit": 80},
        "limits": {"maximum_position_usd": 25, "maximum_open_positions": 1,
                   "maximum_filled_trades_per_utc_day": 3,
                   "maximum_daily_loss_usd": 5, "cooldown_minutes": 60,
                   "maximum_consecutive_errors": 5},
        "dry_run": {"entry_fill_fraction_per_cycle": fill_fraction},
    })


RULES = PairRules("SOLUSD", 2, 3, Decimal("0.02"), Decimal("5"))


class AltcoinContractTests(unittest.TestCase):
    def test_parses_versioned_contract_and_ignores_shadow_overlay(self):
        value = payload(decision="hold")
        value["adaptive_policy_overlay"]["decision"] = "allow_scout_long"
        parsed = ScoutDecision.parse(value)
        self.assertEqual(parsed.decision, "hold")

    def test_rejects_unsupported_or_malformed_contract(self):
        with self.assertRaises(ContractError):
            ScoutDecision.parse(payload(schema_version="future-v9"))
        broken = payload()
        broken["limit_entry_plan"]["spread_bps"] = "not-a-number"
        with self.assertRaises(ContractError):
            ScoutDecision.parse(broken)


class AltcoinEngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "state.sqlite3"
        self.events = Path(self.tmp.name) / "events.jsonl"
        self.store = Store(self.path)

    def tearDown(self):
        self.tmp.cleanup()

    def engine(self, mode="dry_run", fill_fraction=1):
        return Engine(self.store, strategy(fill_fraction), mode, self.events)

    def test_stale_future_disabled_expired_and_kill_switch_fail_closed(self):
        cases = [
            (payload(generated_at=NOW - timedelta(minutes=21)), "stale_source"),
            (payload(generated_at=NOW + timedelta(seconds=1)), "source_timestamp_in_future"),
            (payload(asset="ETH"), "asset_disabled"),
            (payload(expires_at=NOW), "expired_limit_entry_plan"),
        ]
        for index, (raw, reason) in enumerate(cases):
            raw["decision_id"] = f"case-{index}"
            result = self.engine().evaluate(ScoutDecision.parse(raw), RULES, NOW)
            self.assertIn(reason, result.failures)
        result = self.engine().evaluate(ScoutDecision.parse(payload("kill")), RULES, NOW, kill_switch=True)
        self.assertIn("global_kill_switch", result.failures)

    def test_observer_records_but_creates_no_order(self):
        result = self.engine("observer").process_decision(ScoutDecision.parse(payload()), RULES, NOW)
        self.assertTrue(result.allowed)
        self.assertEqual(self.store.active_orders(), [])

    def test_duplicate_decision_is_idempotent_across_restart(self):
        decision = ScoutDecision.parse(payload())
        self.assertTrue(self.engine().process_decision(decision, RULES, NOW).allowed)
        restarted = Engine(Store(self.path), strategy(), "dry_run", self.events)
        result = restarted.process_decision(decision, RULES, NOW)
        self.assertFalse(result.allowed)
        self.assertIn("duplicate_decision", result.failures)
        self.assertEqual(len(self.store.active_orders()), 1)

    def test_partial_fills_are_persisted_and_protected_by_take_profit(self):
        engine = self.engine(fill_fraction=Decimal("0.5"))
        engine.process_decision(ScoutDecision.parse(payload()), RULES, NOW)
        engine.advance_dry_run(Decimal("100"), Decimal("100"), NOW)
        orders = self.store.active_orders()
        entry = next(o for o in orders if o["kind"] == "entry")
        tp = next(o for o in orders if o["kind"] == "take_profit")
        self.assertEqual(entry["status"], "partial")
        self.assertEqual(Decimal(entry["filled_quantity"]), Decimal("0.100"))
        self.assertEqual(Decimal(tp["quantity"]), Decimal("0.100"))
        Engine(Store(self.path), strategy(Decimal("0.5")), "dry_run").advance_dry_run(
            Decimal("100"), Decimal("100"), NOW + timedelta(minutes=1))
        self.assertEqual(Decimal(self.store.open_positions()[0]["quantity"]), Decimal("0.200"))

    def test_entry_expires_and_is_cancelled(self):
        engine = self.engine()
        engine.process_decision(ScoutDecision.parse(payload()), RULES, NOW)
        engine.advance_dry_run(Decimal("101"), Decimal("101"), NOW + timedelta(minutes=16))
        self.assertEqual(self.store.active_orders(), [])
        self.assertEqual(self.store.open_positions(), [])

    def test_fee_math_requires_positive_net_profit(self):
        profitable = self.engine().evaluate(ScoutDecision.parse(payload()), RULES, NOW)
        self.assertGreater(profitable.expected_net_profit, 0)
        costly = payload("costly")
        costly["limit_entry_plan"]["spread_bps"] = "100"
        costly["limit_entry_plan"]["slippage_bps"] = "100"
        rejected = self.engine().evaluate(ScoutDecision.parse(costly), RULES, NOW)
        self.assertIn("non_positive_expected_net_profit", rejected.failures)

    def test_stop_and_forced_exit_are_taker_and_net_of_fees(self):
        engine = self.engine()
        engine.process_decision(ScoutDecision.parse(payload()), RULES, NOW)
        engine.advance_dry_run(Decimal("100"), Decimal("100"), NOW)
        engine.advance_dry_run(Decimal("99.40"), Decimal("99.50"), NOW + timedelta(minutes=1))
        closed = self.store.open_positions()
        self.assertEqual(closed, [])
        event = [e for e in self.store.recent_events(20) if e["event_type"] == "position_closed"][-1]
        self.assertEqual(event["details"]["execution"], "taker")
        self.assertLess(Decimal(event["details"]["realized_net_pnl"]), 0)

    def test_precision_notional_and_error_circuit_breakers(self):
        raw = payload()
        raw["limit_entry_plan"]["quantity"] = "0.2001"
        result = self.engine().evaluate(ScoutDecision.parse(raw), RULES, NOW, consecutive_errors=5)
        self.assertIn("invalid_quantity_precision", result.failures)
        self.assertIn("consecutive_error_circuit_breaker", result.failures)

    def test_post_only_entry_may_not_cross_current_ask(self):
        result = self.engine().evaluate(ScoutDecision.parse(payload()), RULES, NOW,
                                        current_ask=Decimal("99.99"))
        self.assertIn("post_only_would_cross", result.failures)

    def test_daily_loss_circuit_breaker(self):
        self.store.record_decision("old", "SOL", (NOW - timedelta(hours=2)).isoformat(),
                                   "accepted", (), "old-order")
        self.store.create_or_grow_position("old", "SOL", Decimal("100"), Decimal("0.1"),
                                           (NOW - timedelta(hours=2)).isoformat())
        self.store.close_position("old", Decimal("40"), "stop", Decimal("-6"),
                                  (NOW - timedelta(hours=1)).isoformat())
        result = self.engine().evaluate(ScoutDecision.parse(payload("new")), RULES, NOW)
        self.assertIn("daily_loss_circuit_breaker", result.failures)

    def test_unknown_order_survives_restart_until_reconciled(self):
        engine = self.engine()
        engine.process_decision(ScoutDecision.parse(payload()), RULES, NOW)
        order = self.store.active_orders()[0]
        self.store.update_order(order["client_order_id"], "unknown", Decimal("0"))
        restarted = Store(self.path)
        active = restarted.active_orders()
        self.assertEqual(active[0]["status"], "unknown")
        self.assertEqual(active[0]["client_order_id"], deterministic_client_order_id("sol-1", "SOL"))

    def test_append_only_audit_contains_transitions(self):
        self.engine().process_decision(ScoutDecision.parse(payload()), RULES, NOW)
        lines = self.events.read_text(encoding="utf-8").splitlines()
        self.assertEqual(json.loads(lines[-1])["event_type"], "entry_order_created")


if __name__ == "__main__":
    unittest.main()
