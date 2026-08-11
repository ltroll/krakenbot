from datetime import datetime, timedelta, timezone
import unittest

from range_grid_order_sizing import minimum_order_floor_decision


class MinimumOrderFloorTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
        self.config = {
            "minimum_order_floor_enabled": True,
            "minimum_order_floor_sources": "range_low",
            "minimum_order_floor_usd": 8.0,
            "minimum_order_floor_cash_reserve_usd": 100.0,
            "minimum_order_floor_cooldown_minutes": 60,
        }

    def decide(self, **overrides):
        kwargs = {
            "config": self.config,
            "buy_source": "range_low",
            "available_usd": 200.0,
            "calculated_notional_usd": 4.0,
            "min_buy_notional_usd": 8.0,
            "now": self.now,
            "last_floor_at_by_source": {},
        }
        kwargs.update(overrides)
        return minimum_order_floor_decision(**kwargs)

    def test_applies_exact_exchange_minimum_to_low_band(self):
        result = self.decide()

        self.assertTrue(result["applied"])
        self.assertEqual(result["floor_notional_usd"], 8.0)

    def test_does_not_expand_an_order_that_already_meets_minimum(self):
        result = self.decide(calculated_notional_usd=9.0)

        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "not_needed")

    def test_does_not_apply_to_unconfigured_source(self):
        result = self.decide(buy_source="range_high_band")

        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "source_not_enabled")

    def test_floor_meets_asset_minimum_volume_at_high_price(self):
        result = self.decide(
            calculated_notional_usd=9.0,
            order_price=120_000.0,
            min_buy_volume_asset=0.0001,
        )

        self.assertTrue(result["applied"])
        self.assertEqual(result["floor_notional_usd"], 12.0)
        self.assertEqual(result["min_volume_notional_usd"], 12.0)

    def test_preserves_cash_reserve(self):
        result = self.decide(available_usd=107.99)

        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "cash_reserve")

    def test_enforces_per_source_cooldown(self):
        result = self.decide(
            last_floor_at_by_source={
                "range_low": (self.now - timedelta(minutes=30)).isoformat()
            }
        )

        self.assertFalse(result["applied"])
        self.assertEqual(result["reason"], "cooldown")
        self.assertAlmostEqual(result["cooldown_remaining_minutes"], 30.0)

    def test_allows_again_after_cooldown(self):
        result = self.decide(
            last_floor_at_by_source={
                "range_low": (self.now - timedelta(minutes=60)).isoformat()
            }
        )

        self.assertTrue(result["applied"])


if __name__ == "__main__":
    unittest.main()
