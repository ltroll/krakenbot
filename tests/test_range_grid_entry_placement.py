import json
import os
import unittest

from range_grid_entry_placement import (
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


if __name__ == "__main__":
    unittest.main()

