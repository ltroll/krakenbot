import json
import os
import tempfile
import unittest

from range_grid_control import (
    ControlStateError,
    default_control_state,
    load_control_state,
    load_control_state_fail_safe,
    merge_control_update,
    normalize_control_state,
    operator_buy_cancel_reason,
    operator_buy_price_rule_reason,
    save_control_state,
)


class RangeGridControlTests(unittest.TestCase):
    def test_defaults_leave_automatic_buying_unchanged(self):
        state = default_control_state()

        self.assertFalse(state["buying_paused"])
        self.assertFalse(state["buy_price_floor_enabled"])
        self.assertFalse(state["buy_price_ceiling_enabled"])
        self.assertIsNone(operator_buy_price_rule_reason(state, 75000))

    def test_normalizes_enabled_buy_price_zone(self):
        state = normalize_control_state({
            "buy_price_floor_enabled": True,
            "buy_price_floor_usd": "75000.126",
            "buy_price_ceiling_enabled": True,
            "buy_price_ceiling_usd": "80500.444",
        })

        self.assertEqual(state["buy_price_floor_usd"], 75000.13)
        self.assertEqual(state["buy_price_ceiling_usd"], 80500.44)
        self.assertIsNone(operator_buy_price_rule_reason(state, 75000.13))
        self.assertIsNone(operator_buy_price_rule_reason(state, 80500.44))
        self.assertEqual(
            operator_buy_price_rule_reason(state, 75000.12),
            "operator_buy_price_below_floor",
        )
        self.assertEqual(
            operator_buy_price_rule_reason(state, 80500.45),
            "operator_buy_price_above_ceiling",
        )

    def test_enabled_boundary_requires_valid_price(self):
        for payload in (
            {"buy_price_floor_enabled": True},
            {
                "buy_price_ceiling_enabled": True,
                "buy_price_ceiling_usd": 0,
            },
            {
                "buy_price_floor_enabled": True,
                "buy_price_floor_usd": "invalid",
            },
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(ControlStateError):
                    normalize_control_state(payload)

    def test_floor_cannot_be_above_ceiling(self):
        with self.assertRaisesRegex(ControlStateError, "cannot be above"):
            normalize_control_state({
                "buy_price_floor_enabled": True,
                "buy_price_floor_usd": 81000,
                "buy_price_ceiling_enabled": True,
                "buy_price_ceiling_usd": 79000,
            })

    def test_disabled_boundary_value_is_preserved_for_ui(self):
        state = normalize_control_state({
            "buy_price_floor_enabled": False,
            "buy_price_floor_usd": 74000,
        })

        self.assertFalse(state["buy_price_floor_enabled"])
        self.assertEqual(state["buy_price_floor_usd"], 74000)
        self.assertIsNone(operator_buy_price_rule_reason(state, 70000))

    def test_old_manual_targets_migrate_to_operator_price_zone(self):
        state = normalize_control_state({
            "schema_version": 1,
            "manual_targets_enabled": True,
            "buy_targets": [{
                "id": "old-target",
                "buy_price": 75000,
                "profit_target_pct": 0.01,
            }],
        })

        self.assertEqual(state["schema_version"], 2)
        self.assertNotIn("manual_targets_enabled", state)
        self.assertNotIn("buy_targets", state)
        self.assertTrue(state["buy_price_floor_enabled"])
        self.assertEqual(state["buy_price_floor_usd"], 75000)
        self.assertTrue(state["buy_price_ceiling_enabled"])
        self.assertEqual(state["buy_price_ceiling_usd"], 75000)

    def test_merge_increments_revision_and_preserves_other_fields(self):
        current = normalize_control_state({
            "revision": 4,
            "buy_price_floor_enabled": True,
            "buy_price_floor_usd": 75000,
        })

        updated = merge_control_update(
            current,
            {
                "buying_paused": True,
                "buy_price_ceiling_enabled": True,
                "buy_price_ceiling_usd": 80000,
            },
            updated_by="test",
        )

        self.assertEqual(updated["revision"], 5)
        self.assertTrue(updated["buying_paused"])
        self.assertEqual(updated["buy_price_floor_usd"], 75000)
        self.assertEqual(updated["buy_price_ceiling_usd"], 80000)
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

    def test_hold_and_price_zone_cancel_only_conflicting_pending_buys(self):
        state = normalize_control_state({
            "buy_price_floor_enabled": True,
            "buy_price_floor_usd": 75000,
            "buy_price_ceiling_enabled": True,
            "buy_price_ceiling_usd": 80000,
        })

        self.assertIsNone(operator_buy_cancel_reason(
            state,
            {"price": 77500},
        ))
        self.assertEqual(
            operator_buy_cancel_reason(state, {"price": 74000}),
            "operator_buy_price_below_floor",
        )
        self.assertEqual(
            operator_buy_cancel_reason(state, {"price": 81000}),
            "operator_buy_price_above_ceiling",
        )
        self.assertEqual(
            operator_buy_cancel_reason(
                {**state, "buying_paused": True},
                {"price": 77500},
            ),
            "operator_buy_hold",
        )


if __name__ == "__main__":
    unittest.main()
