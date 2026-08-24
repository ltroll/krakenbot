import unittest

from range_grid_profit_target import fear_greed_profit_target_adjustment


class FearGreedProfitTargetTests(unittest.TestCase):
    def setUp(self):
        self.config = {
            "fear_greed_profit_target_enabled": True,
            "fear_greed_profit_target_sources": "range_low,range_median",
            "fear_greed_profit_target_greed_start_index": 50,
            "fear_greed_profit_target_full_greed_index": 75,
            "fear_greed_profit_target_max_multiplier_by_source": {
                "range_low": 1.5,
                "range_median": 2.0,
            },
        }

    def test_below_greed_start_keeps_base_target(self):
        result = fear_greed_profit_target_adjustment(
            self.config,
            buy_source="range_median",
            fear_greed_index=27,
            base_profit_target_pct=0.0085,
        )

        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "below_greed_start")
        self.assertEqual(result["multiplier"], 1.0)
        self.assertEqual(result["effective_profit_target_pct"], 0.0085)

    def test_greed_interpolates_to_source_maximum(self):
        result = fear_greed_profit_target_adjustment(
            self.config,
            buy_source="range_median",
            fear_greed_index=73,
            base_profit_target_pct=0.0085,
        )

        self.assertTrue(result["applied"])
        self.assertAlmostEqual(result["greed_progress"], 0.92)
        self.assertAlmostEqual(result["multiplier"], 1.92)
        self.assertAlmostEqual(
            result["effective_profit_target_pct"],
            0.01632,
        )

    def test_full_greed_caps_at_source_maximum(self):
        result = fear_greed_profit_target_adjustment(
            self.config,
            buy_source="range_low",
            fear_greed_index=100,
            base_profit_target_pct=0.009,
        )

        self.assertEqual(result["multiplier"], 1.5)
        self.assertAlmostEqual(result["effective_profit_target_pct"], 0.0135)

    def test_missing_index_fails_open_to_base_target(self):
        result = fear_greed_profit_target_adjustment(
            self.config,
            buy_source="range_low",
            fear_greed_index=None,
            base_profit_target_pct=0.009,
        )

        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "missing_fear_greed_index")
        self.assertEqual(result["effective_profit_target_pct"], 0.009)

    def test_unlisted_source_keeps_base_target(self):
        result = fear_greed_profit_target_adjustment(
            self.config,
            buy_source="range_high_band",
            fear_greed_index=75,
            base_profit_target_pct=0.005,
        )

        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "source_not_enabled")
        self.assertEqual(result["effective_profit_target_pct"], 0.005)


if __name__ == "__main__":
    unittest.main()
