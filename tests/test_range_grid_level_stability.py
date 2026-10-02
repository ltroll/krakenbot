import unittest
from datetime import datetime, timedelta, timezone

from range_grid_level_stability import (
    level_stability_snapshot,
    shadow_price_rule_reason,
    update_anchor_hysteresis,
    update_level_stability,
)


class RangeGridLevelStabilityTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 10, 2, 12, 0, tzinfo=timezone.utc)
        self.config = {
            "level_stability_shadow_enabled": True,
            "level_stability_cluster_tolerance_pct": 0.0035,
            "level_stability_support_buy_buffer_pct": 0.0025,
            "level_stability_support_lower_confirm_samples": 2,
            "level_stability_support_lower_confirm_minutes": 5,
            "level_stability_support_raise_confirm_samples": 3,
            "level_stability_support_raise_confirm_minutes": 240,
            "level_stability_resistance_lower_confirm_samples": 2,
            "level_stability_resistance_lower_confirm_minutes": 5,
            "level_stability_resistance_raise_confirm_samples": 3,
            "level_stability_resistance_raise_confirm_minutes": 60,
            "dynamic_anchor_mid_mode": "median",
            "dynamic_anchor_low_band_max": 0.5,
            "dynamic_anchor_high_band_min": 0.92,
            "dynamic_anchor_hysteresis_low_enter": 0.45,
            "dynamic_anchor_hysteresis_low_exit": 0.58,
            "dynamic_anchor_hysteresis_high_enter": 0.92,
            "dynamic_anchor_hysteresis_high_exit": 0.84,
        }

    @staticmethod
    def level(price, level_type):
        return {
            "price": price,
            "type": level_type,
            "label": level_type.replace("_", " "),
            "source": "test",
        }

    def update(self, state, when, support, resistance=86000):
        return update_level_stability(
            state,
            raw_support=self.level(support, "recent_low"),
            raw_resistance=self.level(resistance, "recent_high"),
            now=when,
            config=self.config,
        )

    def test_initial_levels_are_confirmed_immediately(self):
        state = {}
        changes = self.update(state, self.start, 83000)

        self.assertEqual(state["support"]["stable"]["price"], 83000)
        self.assertEqual(state["resistance"]["stable"]["price"], 86000)
        self.assertEqual({change["side"] for change in changes}, {
            "support",
            "resistance",
        })

    def test_lower_support_is_adopted_after_short_confirmation(self):
        state = {}
        self.update(state, self.start, 83000)
        self.update(state, self.start + timedelta(minutes=1), 82000)
        changes = self.update(state, self.start + timedelta(minutes=6), 82020)

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]["side"], "support")
        self.assertEqual(changes[0]["reason"], "support_lower_confirmed")
        self.assertAlmostEqual(state["support"]["stable"]["price"], 82010)

    def test_higher_support_waits_for_full_confirmation_window(self):
        state = {}
        self.update(state, self.start, 83000)
        self.update(state, self.start + timedelta(minutes=1), 84000)
        self.update(state, self.start + timedelta(minutes=120), 84020)
        early = self.update(state, self.start + timedelta(minutes=239), 83980)

        self.assertEqual(early, [])
        self.assertEqual(state["support"]["stable"]["price"], 83000)

        changes = self.update(
            state,
            self.start + timedelta(minutes=241),
            84010,
        )
        self.assertEqual(changes[0]["reason"], "support_higher_confirmed")
        self.assertAlmostEqual(state["support"]["stable"]["price"], 84005)

    def test_stable_zone_ignores_small_raw_level_movement(self):
        state = {}
        self.update(state, self.start, 83000)
        changes = self.update(
            state,
            self.start + timedelta(minutes=10),
            83150,
        )

        self.assertEqual(changes, [])
        self.assertEqual(state["support"]["stable"]["price"], 83000)
        self.assertIsNone(state["support"]["candidate"])

    def test_shadow_ceiling_uses_lower_of_stable_and_operator_limits(self):
        state = {}
        self.update(state, self.start, 83000)
        snapshot = level_stability_snapshot(
            state,
            self.config,
            {
                "buy_price_ceiling_enabled": True,
                "buy_price_ceiling_usd": 83600,
            },
        )

        self.assertAlmostEqual(snapshot["automatic_buy_ceiling"], 83207.5)
        self.assertAlmostEqual(snapshot["effective_shadow_buy_ceiling"], 83207.5)
        self.assertIsNone(shadow_price_rule_reason(83200, snapshot))
        self.assertEqual(
            shadow_price_rule_reason(83300, snapshot),
            "level_stability_shadow_above_ceiling",
        )

    def test_anchor_hysteresis_prevents_low_median_flapping(self):
        state = {}
        configured = ["low", "median"]
        first = update_anchor_hysteresis(
            state,
            configured_modes=configured,
            raw_active_modes=["low"],
            range_position=0.44,
            now=self.start,
            config=self.config,
        )
        noisy = update_anchor_hysteresis(
            state,
            configured_modes=configured,
            raw_active_modes=["median"],
            range_position=0.52,
            now=self.start + timedelta(minutes=1),
            config=self.config,
        )
        exited = update_anchor_hysteresis(
            state,
            configured_modes=configured,
            raw_active_modes=["median"],
            range_position=0.59,
            now=self.start + timedelta(minutes=2),
            config=self.config,
        )

        self.assertEqual(first["stable_mode"], "low")
        self.assertEqual(noisy["stable_mode"], "low")
        self.assertFalse(noisy["changed"])
        self.assertEqual(exited["stable_mode"], "median")
        self.assertTrue(exited["changed"])


if __name__ == "__main__":
    unittest.main()
