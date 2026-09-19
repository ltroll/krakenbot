import json
import os
import tempfile
import unittest

from range_grid_control import (
    ControlStateError,
    active_buy_targets,
    default_control_state,
    load_control_state,
    load_control_state_fail_safe,
    merge_control_update,
    normalize_control_state,
    operator_buy_cancel_reason,
    operator_grid_slot,
    save_control_state,
)


class RangeGridControlTests(unittest.TestCase):
    def test_defaults_leave_automatic_buying_unchanged(self):
        state = default_control_state()

        self.assertFalse(state["buying_paused"])
        self.assertFalse(state["manual_targets_enabled"])
        self.assertTrue(state["cancel_open_buys_on_hold"])
        self.assertEqual(active_buy_targets(state), [])

    def test_normalizes_enabled_targets_and_decimal_profit(self):
        state = normalize_control_state({
            "manual_targets_enabled": True,
            "buy_targets": [
                {
                    "id": "dip one",
                    "label": "First dip",
                    "enabled": True,
                    "buy_price": "76500.126",
                    "profit_target_pct": "0.009",
                },
                {
                    "id": "deep",
                    "enabled": False,
                    "buy_price": 74000,
                    "profit_target_pct": 0.015,
                },
            ],
        })

        self.assertEqual(state["buy_targets"][0]["id"], "dip-one")
        self.assertEqual(state["buy_targets"][0]["buy_price"], 76500.13)
        self.assertEqual(state["buy_targets"][0]["profit_target_pct"], 0.009)
        self.assertEqual(len(active_buy_targets(state)), 1)
        self.assertEqual(operator_grid_slot("dip one"), "operator:dip-one")

    def test_rejects_unsafe_target_values(self):
        for payload in (
            {"buy_targets": [{"buy_price": 0, "profit_target_pct": 0.01}]},
            {"buy_targets": [{"buy_price": 75000, "profit_target_pct": -0.0001}]},
            {"buy_targets": [{"buy_price": 75000, "profit_target_pct": 0.5}]},
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(ControlStateError):
                    normalize_control_state(payload)

    def test_zero_net_profit_target_is_fee_adjusted_break_even(self):
        state = normalize_control_state({
            "manual_targets_enabled": True,
            "buy_targets": [{
                "id": "break-even",
                "buy_price": 75000,
                "profit_target_pct": 0,
            }],
        })

        self.assertEqual(state["buy_targets"][0]["profit_target_pct"], 0)
        self.assertIsNone(operator_buy_cancel_reason(
            state,
            {
                "operator_controlled": True,
                "grid_slot": "operator:break-even",
                "price": 75000,
                "sell_pct_override": 0,
            },
        ))

    def test_manual_mode_requires_an_enabled_target(self):
        with self.assertRaisesRegex(
            ControlStateError,
            "at least one enabled buy target",
        ):
            normalize_control_state({
                "manual_targets_enabled": True,
                "buy_targets": [{
                    "id": "disabled",
                    "enabled": False,
                    "buy_price": 75000,
                    "profit_target_pct": 0.01,
                }],
            })

    def test_merge_increments_revision_and_preserves_other_fields(self):
        current = normalize_control_state({
            "revision": 4,
            "manual_targets_enabled": True,
            "buy_targets": [{
                "id": "one",
                "buy_price": 76000,
                "profit_target_pct": 0.01,
            }],
        })

        updated = merge_control_update(
            current,
            {"buying_paused": True},
            updated_by="test",
        )

        self.assertEqual(updated["revision"], 5)
        self.assertTrue(updated["buying_paused"])
        self.assertTrue(updated["manual_targets_enabled"])
        self.assertEqual(len(updated["buy_targets"]), 1)
        self.assertEqual(updated["updated_by"], "test")

    def test_round_trip_and_corruption_fail_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "control.json")
            expected = merge_control_update(
                default_control_state(),
                {"buying_paused": True},
                updated_by="test",
            )
            save_control_state(path, expected)
            self.assertEqual(load_control_state(path), expected)

            with open(path, "w", encoding="utf-8") as handle:
                handle.write("not json")
            with open(f"{path}.bak", "w", encoding="utf-8") as handle:
                json.dump([], handle)

            fail_safe = load_control_state_fail_safe(path)
            self.assertTrue(fail_safe["buying_paused"])
            self.assertIsNotNone(fail_safe["load_error"])

    def test_hold_and_manual_mode_cancel_only_conflicting_pending_buys(self):
        matching_operator_order = {
            "operator_controlled": True,
            "grid_slot": "operator:dip",
            "price": 75000,
            "sell_pct_override": 0.01,
        }
        manual_state = normalize_control_state({
            "manual_targets_enabled": True,
            "buy_targets": [{
                "id": "dip",
                "enabled": True,
                "buy_price": 75000,
                "profit_target_pct": 0.01,
            }],
        })

        self.assertIsNone(operator_buy_cancel_reason(
            manual_state,
            matching_operator_order,
        ))
        self.assertEqual(
            operator_buy_cancel_reason(
                manual_state,
                {"grid_slot": "range_low:1", "price": 74000},
            ),
            "operator_manual_mode_replaces_automatic",
        )
        self.assertEqual(
            operator_buy_cancel_reason(
                {**manual_state, "buying_paused": True},
                matching_operator_order,
            ),
            "operator_buy_hold",
        )
        self.assertEqual(
            operator_buy_cancel_reason(
                manual_state,
                {**matching_operator_order, "sell_pct_override": 0.02},
            ),
            "operator_target_profit_changed",
        )


if __name__ == "__main__":
    unittest.main()
