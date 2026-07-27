import json
import os
import tempfile
import unittest

from range_grid_order_safety import (
    StateRecoveryError,
    allocate_position_costs,
    atomic_write_json,
    load_json_with_backup,
    order_execution,
    order_limit_price,
)


class RangeGridOrderSafetyTests(unittest.TestCase):
    def test_atomic_state_write_keeps_last_valid_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")
            atomic_write_json(state_path, {"version": 1})
            atomic_write_json(state_path, {"version": 2})

            with open(state_path, encoding="utf-8") as handle:
                self.assertEqual(json.load(handle), {"version": 2})
            with open(f"{state_path}.bak", encoding="utf-8") as handle:
                self.assertEqual(json.load(handle), {"version": 1})

    def test_load_state_recovers_from_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")
            backup_path = f"{state_path}.bak"
            with open(state_path, "w", encoding="utf-8") as handle:
                handle.write("{")
            with open(backup_path, "w", encoding="utf-8") as handle:
                json.dump({"recovered": True}, handle)

            payload, source, errors = load_json_with_backup(state_path)

            self.assertEqual(payload, {"recovered": True})
            self.assertEqual(source, "backup")
            self.assertEqual(errors[0]["source"], "primary")

    def test_load_state_fails_when_primary_and_backup_are_invalid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = os.path.join(tmpdir, "state.json")
            with open(state_path, "w", encoding="utf-8") as handle:
                handle.write("{")
            with open(f"{state_path}.bak", "w", encoding="utf-8") as handle:
                handle.write("[]")

            with self.assertRaises(StateRecoveryError):
                load_json_with_backup(state_path)

    def test_order_execution_uses_actual_partial_fill_values(self):
        execution = order_execution({
            "status": "canceled",
            "vol": "0.010",
            "vol_exec": "0.004",
            "cost": "260.00",
            "fee": "0.67",
            "price": "64900.0",
        })

        self.assertEqual(execution["status"], "canceled")
        self.assertAlmostEqual(execution["executed_volume"], 0.004)
        self.assertAlmostEqual(execution["remaining_volume"], 0.006)
        self.assertAlmostEqual(execution["average_price"], 65000.0)
        self.assertAlmostEqual(execution["fee"], 0.67)

    def test_order_execution_uses_fallbacks_for_legacy_details(self):
        execution = order_execution(
            {"status": "closed", "vol_exec": "0.002"},
            fallback_volume=0.002,
            fallback_price=62000,
        )

        self.assertAlmostEqual(execution["cost"], 124.0)
        self.assertAlmostEqual(execution["average_price"], 62000.0)
        self.assertAlmostEqual(execution["remaining_volume"], 0.0)

    def test_order_limit_price_reads_open_order_description(self):
        self.assertEqual(
            order_limit_price({"descr": {"price": "63123.4"}}),
            63123.4,
        )

    def test_position_costs_are_prorated_for_partial_exit(self):
        allocation = allocate_position_costs(
            original_volume=0.01,
            executed_volume=0.004,
            total_cost=620.0,
            total_fee=1.55,
        )

        self.assertAlmostEqual(allocation["execution_ratio"], 0.4)
        self.assertAlmostEqual(allocation["executed_cost"], 248.0)
        self.assertAlmostEqual(allocation["executed_fee"], 0.62)
        self.assertAlmostEqual(allocation["remaining_cost"], 372.0)
        self.assertAlmostEqual(allocation["remaining_fee"], 0.93)

    def test_position_cost_allocation_clamps_overfill(self):
        allocation = allocate_position_costs(0.01, 0.02, 620.0, 1.55)

        self.assertEqual(allocation["execution_ratio"], 1.0)
        self.assertEqual(allocation["remaining_cost"], 0.0)
        self.assertEqual(allocation["remaining_fee"], 0.0)


if __name__ == "__main__":
    unittest.main()
