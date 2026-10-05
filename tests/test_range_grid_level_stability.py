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
            "level_stability_break_tolerance_pct": 0.0035,
            "level_stability_break_confirm_samples": 2,
            "level_stability_break_confirm_minutes": 5,
            "level_stability_raw_max_age_minutes": 180,
            "level_stability_fail_open_when_stale": True,
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

    def update(
        self,
        state,
        when,
        support,
        resistance=86000,
        market_price=84000,
    ):
        return update_level_stability(
            state,
            raw_support=(
                self.level(support, "recent_low")
                if support is not None
                else None
            ),
            raw_resistance=(
                self.level(resistance, "recent_high")
                if resistance is not None
                else None
            ),
            market_price=market_price,
            now=when,
            config=self.config,
        )

    def test_initial_levels_are_confirmed_immediately(self):
        state = {}
        changes = self.update(state, self.start, 83000)

        self.assertEqual(state["support"]["stable"]["price"], 83000)
        self.assertEqual(state["resistance"]["stable"]["price"], 86000)
        self.assertEqual(state["support"]["status"], "active")
        self.assertEqual(state["resistance"]["status"], "active")
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

    def test_temporarily_missing_support_keeps_ceiling_until_expiry(self):
        state = {}
        self.update(state, self.start, 83000)
        self.update(
            state,
            self.start + timedelta(minutes=60),
            None,
        )

        snapshot = level_stability_snapshot(
            state,
            self.config,
            current_price=84000,
        )

        self.assertEqual(snapshot["support"]["status"], "missing_raw")
        self.assertTrue(snapshot["automatic_buy_ceiling_active"])
        self.assertEqual(
            snapshot["automatic_buy_ceiling_reason"],
            "temporary_raw_gap_using_stable_support",
        )

    def test_stale_support_fails_open_without_removing_operator_ceiling(self):
        state = {}
        self.update(state, self.start, 83000)
        changes = self.update(
            state,
            self.start + timedelta(minutes=181),
            None,
        )
        snapshot = level_stability_snapshot(
            state,
            self.config,
            {
                "buy_price_ceiling_enabled": True,
                "buy_price_ceiling_usd": 85000,
            },
            current_price=84000,
        )

        self.assertTrue(any(change["kind"] == "stale" for change in changes))
        self.assertEqual(snapshot["support"]["status"], "stale")
        self.assertFalse(snapshot["automatic_buy_ceiling_active"])
        self.assertIsNone(snapshot["automatic_buy_ceiling"])
        self.assertEqual(snapshot["effective_shadow_buy_ceiling"], 85000)
        self.assertEqual(
            snapshot["automatic_buy_ceiling_reason"],
            "stale_support_fail_open",
        )

    def test_confirmed_resistance_break_invalidates_old_level(self):
        state = {}
        self.update(state, self.start, 83000)
        pending = self.update(
            state,
            self.start + timedelta(minutes=1),
            83000,
            resistance=None,
            market_price=86500,
        )
        changes = self.update(
            state,
            self.start + timedelta(minutes=6),
            83000,
            resistance=None,
            market_price=86520,
        )

        self.assertEqual(pending, [])
        self.assertEqual(state["resistance"]["status"], "broken")
        invalidation = next(
            change for change in changes
            if change.get("kind") == "invalidation"
        )
        self.assertEqual(
            invalidation["reason"],
            "resistance_market_break_confirmed",
        )

    def test_broken_support_disables_automatic_ceiling(self):
        state = {}
        self.update(state, self.start, 83000)
        self.update(
            state,
            self.start + timedelta(minutes=1),
            None,
            market_price=82500,
        )
        self.update(
            state,
            self.start + timedelta(minutes=6),
            None,
            market_price=82450,
        )
        snapshot = level_stability_snapshot(
            state,
            self.config,
            current_price=82450,
        )

        self.assertEqual(snapshot["support"]["status"], "broken")
        self.assertIsNone(snapshot["automatic_buy_ceiling"])
        self.assertFalse(snapshot["automatic_buy_ceiling_active"])
        self.assertEqual(
            snapshot["automatic_buy_ceiling_reason"],
            "broken_support_fail_open",
        )

    def test_valid_raw_level_reactivates_broken_resistance(self):
        state = {}
        self.update(state, self.start, 83000)
        self.update(
            state,
            self.start + timedelta(minutes=1),
            83000,
            resistance=None,
            market_price=86500,
        )
        self.update(
            state,
            self.start + timedelta(minutes=6),
            83000,
            resistance=None,
            market_price=86520,
        )
        changes = self.update(
            state,
            self.start + timedelta(minutes=7),
            83000,
            resistance=86000,
            market_price=85000,
        )

        self.assertEqual(state["resistance"]["status"], "active")
        self.assertTrue(any(
            change.get("kind") == "reactivation"
            for change in changes
        ))

    def test_different_raw_level_must_confirm_before_stale_reactivation(self):
        state = {}
        self.update(state, self.start, 83000)
        self.update(
            state,
            self.start + timedelta(minutes=181),
            None,
        )
        changes = self.update(
            state,
            self.start + timedelta(minutes=182),
            84000,
            market_price=84500,
        )
        snapshot = level_stability_snapshot(
            state,
            self.config,
            current_price=84500,
        )

        self.assertEqual(changes, [])
        self.assertEqual(state["support"]["status"], "stale")
        self.assertEqual(
            state["support"]["status_reason"],
            "replacement_level_pending_confirmation",
        )
        self.assertEqual(state["support"]["candidate"]["price"], 84000)
        self.assertFalse(snapshot["automatic_buy_ceiling_active"])

    def test_snapshot_reports_raw_level_movement_metrics(self):
        state = {}
        self.update(state, self.start, 83000)
        self.update(state, self.start + timedelta(minutes=1), 83100)
        self.update(state, self.start + timedelta(minutes=2), 84000)
        snapshot = level_stability_snapshot(
            state,
            self.config,
            current_price=84500,
        )

        support = snapshot["support"]
        self.assertEqual(support["raw_observation_count"], 3)
        self.assertEqual(support["raw_material_change_count"], 1)
        self.assertEqual(support["raw_min_price"], 83000)
        self.assertEqual(support["raw_max_price"], 84000)
        self.assertGreater(support["max_abs_raw_stable_divergence_pct"], 1)
        self.assertEqual(support["pending_started_count"], 1)

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
