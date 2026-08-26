import json
import os
import unittest

from range_grid_entry_placement import (
    entry_grid_levels,
    entry_grid_slot,
    entry_order_price_decision,
    entry_placement_mode,
    entry_price_placement_decision,
)
from range_grid_guardrails import validate_strategy_config
from range_grid_source_policy import hard_safety_entry_decision


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def weather_report(*, danger=False, falling_tape=False):
    return {
        "mode": "weather_report",
        "bot_decision_authority": "bot",
        "trade_permission": "bot_decides",
        "alert_level": "danger" if danger else "watch",
        "emergency_bell": False,
        "trend_pressure": {"falling_tape": falling_tape},
        "market_opportunity": {
            "cycle_phase": "post_jump_distribution",
            "entry_opportunity_score": 0.0,
            "exit_pressure_score": 1.0,
        },
    }


class RangeGridEntryPlacementTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "entry_placement_mode_by_source": {
                "range_low": "resting_grid",
                "range_median": "resting_grid",
                "range_high_band": "triggered",
            },
            "resting_grid_max_above_level_pct_by_source": {
                "range_low": 0.0035,
                "range_median": 0.0055,
            },
        }

    def test_resting_grid_places_nearby_level_before_price_crosses(self):
        decision = entry_price_placement_decision(
            100.3,
            100.0,
            self.config,
            "range_low",
        )

        self.assertTrue(decision["allowed"])
        self.assertEqual(decision["mode"], "resting_grid")
        self.assertAlmostEqual(decision["max_above_level_pct"], 0.0035)

    def test_resting_grid_levels_include_anchor_as_first_slot(self):
        levels = entry_grid_levels(
            100.0,
            0.0028,
            3,
            self.config,
            "range_low",
        )

        self.assertEqual(len(levels), 3)
        self.assertAlmostEqual(levels[0], 100.0)
        self.assertAlmostEqual(levels[1], 99.72)
        self.assertAlmostEqual(levels[2], 99.44)

    def test_triggered_grid_levels_keep_legacy_first_step_discount(self):
        levels = entry_grid_levels(
            100.0,
            0.0028,
            3,
            {},
            "range_low",
        )

        self.assertEqual(len(levels), 3)
        self.assertAlmostEqual(levels[0], 99.72)
        self.assertAlmostEqual(levels[1], 99.44)
        self.assertAlmostEqual(levels[2], 99.16)

    def test_resting_grid_slot_is_stable_within_price_band(self):
        anchor_slot = entry_grid_slot(
            "range_low",
            100.0,
            0.0028,
            1,
            self.config,
        )
        drifted_slot = entry_grid_slot(
            "range_low",
            100.01,
            0.0028,
            3,
            self.config,
        )

        self.assertEqual(anchor_slot, drifted_slot)
        self.assertTrue(anchor_slot.startswith("range_low:price_band:"))

    def test_resting_grid_slot_reopens_at_next_lower_price_band(self):
        anchor_slot = entry_grid_slot(
            "range_low",
            100.0,
            0.0028,
            1,
            self.config,
        )
        lower_slot = entry_grid_slot(
            "range_low",
            99.72,
            0.0028,
            2,
            self.config,
        )

        self.assertNotEqual(anchor_slot, lower_slot)

    def test_triggered_grid_slot_keeps_source_and_depth_identity(self):
        slot = entry_grid_slot(
            "range_low",
            100.0,
            0.0028,
            3,
            {},
        )

        self.assertEqual(slot, "range_low:3")

    def test_resting_grid_rejects_level_outside_nearby_zone(self):
        decision = entry_price_placement_decision(
            100.6,
            100.0,
            self.config,
            "range_low",
        )

        self.assertFalse(decision["allowed"])
        self.assertEqual(
            decision["reason"],
            "resting_grid_price_too_far_above_level",
        )

    def test_near_touch_moves_only_first_resting_grid_rung(self):
        self.config["resting_grid_near_touch_offset_pct_by_source"] = {
            "range_low": 0.0005,
        }

        first = entry_order_price_decision(
            100.3,
            100.0,
            self.config,
            "range_low",
            grid_depth=1,
        )
        second = entry_order_price_decision(
            100.3,
            99.72,
            self.config,
            "range_low",
            grid_depth=2,
        )

        self.assertTrue(first["near_touch_applied"])
        self.assertTrue(first["post_only"])
        self.assertAlmostEqual(first["anchor_level"], 100.0)
        self.assertAlmostEqual(first["order_price"], 100.24985)
        self.assertFalse(second["near_touch_applied"])
        self.assertFalse(second["post_only"])
        self.assertAlmostEqual(second["order_price"], 99.72)

    def test_near_touch_is_opt_in_and_resting_grid_only(self):
        legacy = entry_order_price_decision(
            100.3,
            100.0,
            self.config,
            "range_low",
        )
        triggered_config = {
            "entry_placement_mode_by_source": {"range_low": "triggered"},
            "resting_grid_near_touch_offset_pct_by_source": {
                "range_low": 0.0005,
            },
        }
        triggered = entry_order_price_decision(
            100.3,
            100.0,
            triggered_config,
            "range_low",
        )

        self.assertFalse(legacy["near_touch_applied"])
        self.assertEqual(legacy["order_price"], 100.0)
        self.assertFalse(triggered["near_touch_applied"])
        self.assertEqual(triggered["order_price"], 100.0)

    def test_triggered_mode_remains_default_and_high_stays_strict(self):
        self.assertEqual(entry_placement_mode({}, "range_low"), "triggered")
        decision = entry_price_placement_decision(
            100.1,
            100.0,
            self.config,
            "range_high_band",
            triggered_tolerance_pct=0.01,
        )

        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["reason"], "price_above_level")

    def test_hard_safety_only_ignores_forecast_opinions(self):
        decision = hard_safety_entry_decision(
            {
                "entry_policy_by_source": {
                    "range_median": {
                        "authority": "chop_confirmed",
                        "hard_block_falling_tape": True,
                        "min_entry_opportunity_score": 0.9,
                    }
                }
            },
            "range_median",
            action_recommendation="blocked",
            weather_report=weather_report(falling_tape=True),
        )

        self.assertTrue(decision["allowed"])
        self.assertTrue(decision["bypass_sentiment_gate"])
        self.assertEqual(decision["size_multiplier"], 1.0)
        self.assertEqual(decision["reason"], "resting_grid_hard_safety_only")

    def test_hard_safety_only_keeps_explicit_risk_off_and_danger(self):
        risk_off = hard_safety_entry_decision(
            {},
            "range_low",
            action_recommendation="risk_off",
        )
        danger = hard_safety_entry_decision(
            {},
            "range_low",
            weather_report=weather_report(danger=True),
        )

        self.assertFalse(risk_off["allowed"])
        self.assertFalse(danger["allowed"])
        self.assertEqual(risk_off["reason"], "source_policy_hard_risk")
        self.assertEqual(danger["reason"], "source_policy_hard_risk")

    def test_resting_profile_is_valid_and_removes_soft_low_median_gates(self):
        path = os.path.join(
            ROOT,
            "range_grid_strategy_recovery_resting_low_median.json",
        )
        with open(path, encoding="utf-8") as handle:
            profile = json.load(handle)

        self.assertEqual(validate_strategy_config(profile), [])
        self.assertFalse(profile["dynamic_anchor_mode"])
        self.assertFalse(profile["volatility_adaptive_entry_step_enabled"])
        self.assertFalse(profile["stale_level_reanchor_enabled"])
        self.assertEqual(
            profile["entry_placement_mode_by_source"]["range_low"],
            "resting_grid",
        )
        self.assertEqual(
            profile["entry_placement_mode_by_source"]["range_median"],
            "resting_grid",
        )
        self.assertEqual(
            profile["entry_placement_mode_by_source"]["range_high_band"],
            "triggered",
        )
        for source in ("range_low", "range_median"):
            policy = profile["entry_policy_by_source"][source]
            self.assertEqual(set(policy), {
                "authority",
                "position_size_multiplier",
                "weather_bypassable_hard_safety_flags",
            })

    def test_guardrails_reject_unsafe_or_incomplete_resting_modes(self):
        errors = validate_strategy_config({
            "entry_placement_mode_by_source": {
                "range_low": "resting_grid",
                "range_high_band": "resting_grid",
            },
            "resting_grid_max_above_level_pct_by_source": {
                "range_low": 0,
                "range_high_band": 0.01,
            },
        })

        self.assertTrue(any("range_low must be > 0" in error for error in errors))
        self.assertTrue(any("range_high_band cannot use" in error for error in errors))

    def test_guardrails_validate_near_touch_offsets_and_mode(self):
        invalid = validate_strategy_config({
            "entry_placement_mode_by_source": {
                "range_low": "triggered",
                "range_median": "resting_grid",
            },
            "resting_grid_max_above_level_pct_by_source": {
                "range_median": 0.0055,
            },
            "resting_grid_near_touch_offset_pct_by_source": {
                "range_low": 0.0005,
                "range_median": 1.0,
            },
        })

        self.assertTrue(any(
            "range_low requires resting_grid" in error
            for error in invalid
        ))
        self.assertTrue(any(
            "range_median must be > 0 and < 1" in error
            for error in invalid
        ))


if __name__ == "__main__":
    unittest.main()
