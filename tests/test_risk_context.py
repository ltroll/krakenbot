import unittest

from risk_context import derive_risk_context


class RiskContextTests(unittest.TestCase):
    def test_missing_context_falls_back(self):
        derived = derive_risk_context({})

        self.assertFalse(derived["risk_context_available"])
        self.assertEqual(derived["risk_adjusted_reason"], "risk_context_missing")
        self.assertEqual(derived["suggested_position_size_multiplier"], 1.0)

    def test_constructive_context_maps_to_buy_posture(self):
        derived = derive_risk_context(
            {
                "recommended_posture": "constructive_accumulation",
                "market_risk_score": 0.2,
                "buy_aggression_score": 1.0,
                "downside_risk_score": 0.1,
                "bottoming_score": 0.8,
                "rebound_score": 0.8,
                "breakout_score": 0.2,
                "position_size_multiplier": 1.2,
            }
        )

        self.assertTrue(derived["risk_context_available"])
        self.assertEqual(derived["risk_adjusted_posture"], "constructive_buy")
        self.assertGreaterEqual(derived["risk_adjusted_buy_score"], 0.65)
        self.assertEqual(derived["suggested_position_size_multiplier"], 1.2)

    def test_hard_safety_flags_force_risk_off_and_zero_size(self):
        derived = derive_risk_context(
            {
                "market_risk_score": 0.1,
                "buy_aggression_score": 1.0,
                "downside_risk_score": 0.1,
                "bottoming_score": 1.0,
                "rebound_score": 1.0,
                "hard_safety_flags": ["source_health_block"],
                "position_size_multiplier": 1.0,
            }
        )

        self.assertEqual(derived["risk_adjusted_posture"], "risk_off")
        self.assertEqual(derived["suggested_position_size_multiplier"], 0.0)
        self.assertIn("source_health_block", derived["risk_context_hard_safety_flags"])

    def test_weather_report_is_advisory_and_applies_tuning(self):
        derived = derive_risk_context(
            {
                "recommended_posture": "risk_off",
                "hard_safety_flags": ["risk_off"],
                "weather_report": {
                    "mode": "weather_report",
                    "bot_decision_authority": "bot",
                    "trade_permission": "bot_decides",
                    "condition": "breakout_tailwind",
                    "alert_level": "watch",
                    "emergency_bell": False,
                    "opportunity_tags": ["breakout_tailwind"],
                    "risk_warnings": ["source_health_degraded"],
                    "bot_tuning": {
                        "position_size_multiplier": 0.8,
                        "grid_aggression_multiplier": 0.68,
                        "target_profit_multiplier": 1.08,
                        "entry_discount_multiplier": 0.96,
                    },
                },
            }
        )

        self.assertTrue(derived["weather_report_available"])
        self.assertFalse(derived["weather_emergency_bell"])
        self.assertEqual(derived["risk_adjusted_posture"], "breakout_tailwind")
        self.assertEqual(derived["suggested_position_size_multiplier"], 0.75)
        self.assertEqual(derived["suggested_grid_aggression_multiplier"], 0.68)
        self.assertEqual(derived["suggested_entry_discount_multiplier"], 0.96)
        self.assertEqual(derived["suggested_take_profit_multiplier"], 1.08)

    def test_weather_emergency_bell_forces_zero_size(self):
        derived = derive_risk_context(
            {
                "weather_report": {
                    "condition": "storm_warning",
                    "alert_level": "danger",
                    "emergency_bell": True,
                    "bot_tuning": {
                        "position_size_multiplier": 1.0,
                    },
                },
            }
        )

        self.assertEqual(derived["risk_adjusted_posture"], "emergency_bell")
        self.assertTrue(derived["weather_emergency_bell"])
        self.assertEqual(derived["suggested_position_size_multiplier"], 0.0)


if __name__ == "__main__":
    unittest.main()
