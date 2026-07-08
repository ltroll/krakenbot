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


if __name__ == "__main__":
    unittest.main()
