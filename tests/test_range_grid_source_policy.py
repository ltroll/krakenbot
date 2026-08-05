import json
import os
import unittest

from range_grid_guardrails import validate_strategy_config
from range_grid_source_policy import (
    source_entry_policy_decision,
    source_entry_step_pct,
)


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def weather_report(*, phase="range_chop_accumulation", falling_tape=False):
    return {
        "mode": "weather_report",
        "bot_decision_authority": "bot",
        "trade_permission": "bot_decides",
        "alert_level": "watch",
        "emergency_bell": False,
        "market_stability": {
            "stabilization_score": 0.7,
        },
        "trend_pressure": {
            "falling_tape": falling_tape,
            "downtrend_strength": 0.2,
        },
        "market_opportunity": {
            "cycle_phase": phase,
            "entry_opportunity_score": 0.65,
            "rebound_confirmation_score": 0.55,
            "exit_pressure_score": 0.2,
            "hold_through_score": 0.6,
        },
    }


class RangeGridSourcePolicyTests(unittest.TestCase):
    def test_price_first_bypasses_normal_sentiment_block_and_reduces_falling_tape(self):
        config = {
            "source_entry_policy_enabled": True,
            "entry_policy_by_source": {
                "range_low": {
                    "authority": "price_first",
                    "position_size_multiplier": 0.6,
                    "falling_tape_size_multiplier": 0.25,
                }
            },
        }

        decision = source_entry_policy_decision(
            config,
            "range_low",
            action_recommendation="blocked",
            weather_report=weather_report(falling_tape=True),
        )

        self.assertTrue(decision["allowed"])
        self.assertTrue(decision["bypass_sentiment_gate"])
        self.assertAlmostEqual(decision["size_multiplier"], 0.15)
        self.assertEqual(
            decision["reason"],
            "source_policy_price_first_falling_tape_reduced",
        )

    def test_price_first_never_bypasses_hard_risk(self):
        config = {
            "source_entry_policy_enabled": True,
            "entry_policy_by_source": {
                "range_low": {"authority": "price_first"}
            },
        }

        decision = source_entry_policy_decision(
            config,
            "range_low",
            action_recommendation="risk_off",
            risk_context={"recommended_posture": "risk_off"},
            weather_report=weather_report(),
        )

        self.assertFalse(decision["allowed"])
        self.assertFalse(decision["bypass_sentiment_gate"])
        self.assertEqual(decision["reason"], "source_policy_hard_risk")

    def test_stabilization_preferred_allows_weak_probe_at_reduced_size(self):
        config = {
            "source_entry_policy_enabled": True,
            "entry_policy_by_source": {
                "range_median": {
                    "authority": "stabilization_preferred",
                    "position_size_multiplier": 0.5,
                    "weak_setup_size_multiplier": 0.4,
                    "min_stabilization_score": 0.8,
                }
            },
        }

        decision = source_entry_policy_decision(
            config,
            "range_median",
            action_recommendation="blocked",
            weather_report=weather_report(),
        )

        self.assertTrue(decision["allowed"])
        self.assertTrue(decision["bypass_sentiment_gate"])
        self.assertFalse(decision["setup_confirmed"])
        self.assertAlmostEqual(decision["size_multiplier"], 0.2)
        self.assertEqual(
            decision["setup_failure_reason"],
            "source_policy_stabilization",
        )

    def test_stabilization_preferred_can_hard_block_falling_tape(self):
        config = {
            "source_entry_policy_enabled": True,
            "entry_policy_by_source": {
                "range_median": {
                    "authority": "stabilization_preferred",
                    "hard_block_falling_tape": True,
                }
            },
        }

        decision = source_entry_policy_decision(
            config,
            "range_median",
            weather_report=weather_report(falling_tape=True),
        )

        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["reason"], "source_policy_falling_tape")

    def test_chop_confirmed_requires_weather_and_matching_phase(self):
        config = {
            "source_entry_policy_enabled": True,
            "entry_policy_by_source": {
                "range_high_band": {
                    "authority": "chop_confirmed",
                    "position_size_multiplier": 0.35,
                    "allowed_phases": "range_chop_accumulation",
                    "min_stabilization_score": 0.6,
                    "max_exit_pressure_score": 0.35,
                }
            },
        }

        missing = source_entry_policy_decision(
            config,
            "range_high_band",
            weather_report={},
        )
        wrong_phase = source_entry_policy_decision(
            config,
            "range_high_band",
            weather_report=weather_report(phase="early_rebound"),
        )
        confirmed = source_entry_policy_decision(
            config,
            "range_high_band",
            action_recommendation="blocked",
            weather_report=weather_report(),
        )

        self.assertEqual(missing["reason"], "source_policy_weather_required")
        self.assertEqual(wrong_phase["reason"], "source_policy_phase")
        self.assertTrue(confirmed["allowed"])
        self.assertTrue(confirmed["bypass_sentiment_gate"])
        self.assertAlmostEqual(confirmed["size_multiplier"], 0.35)

    def test_source_entry_step_uses_source_override_and_fallback(self):
        config = {
            "entry_step_pct_by_source": {
                "range_low": 0.006,
            }
        }

        self.assertEqual(
            source_entry_step_pct(config, "range_low", 0.0045),
            0.006,
        )
        self.assertEqual(
            source_entry_step_pct(config, "range_median", 0.0045),
            0.0045,
        )

    def test_guardrails_reject_invalid_source_policy(self):
        errors = validate_strategy_config({
            "entry_step_pct_by_source": {"range_low": -0.1},
            "entry_policy_by_source": {
                "range_low": {
                    "authority": "guessing",
                    "position_size_multiplier": 1.5,
                    "typo_field": 0.5,
                }
            },
        })

        self.assertTrue(any("entry_step_pct_by_source.range_low" in error for error in errors))
        self.assertTrue(any("authority" in error for error in errors))
        self.assertTrue(any("position_size_multiplier" in error for error in errors))
        self.assertTrue(any("typo_field" in error for error in errors))

    def test_candidate_profile_is_valid_paper_only_and_active_profile_is_unchanged(self):
        candidate_path = os.path.join(
            ROOT,
            "range_grid_strategy_price_first_source_policy_candidate.json",
        )
        active_path = os.path.join(
            ROOT,
            "range_grid_strategy_recovery_range_only.json",
        )
        with open(candidate_path, encoding="utf-8") as handle:
            candidate = json.load(handle)
        with open(active_path, encoding="utf-8") as handle:
            active = json.load(handle)

        self.assertEqual(validate_strategy_config(candidate), [])
        self.assertTrue(candidate["paper_trading_enabled"])
        self.assertTrue(candidate["source_entry_policy_enabled"])
        self.assertEqual(
            candidate["entry_policy_by_source"]["range_high_band"]["authority"],
            "chop_confirmed",
        )
        self.assertNotIn("source_entry_policy_enabled", active)


if __name__ == "__main__":
    unittest.main()
