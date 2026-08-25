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
    def test_ranked_profiles_use_observed_kraken_fee_schedule(self):
        strategy_set_path = os.path.join(
            ROOT,
            "range_grid_strategy_test_set.txt",
        )
        with open(strategy_set_path, encoding="utf-8") as handle:
            strategy_files = [
                line.strip()
                for line in handle
                if line.strip() and not line.lstrip().startswith("#")
            ]

        mismatches = []
        for strategy_file in strategy_files:
            strategy_path = os.path.join(ROOT, strategy_file)
            with open(strategy_path, encoding="utf-8") as handle:
                strategy = json.load(handle)
            actual = (
                strategy.get("maker_fee_pct"),
                strategy.get("taker_fee_pct"),
                strategy.get("round_trip_fee_pct"),
            )
            if actual != (0.004, 0.008, 0.012):
                mismatches.append((strategy_file, actual))

        self.assertEqual(mismatches, [])

    def test_hybrid_fear_greed_candidates_are_paper_only_and_bounded(self):
        safe_name = (
            "range_grid_strategy_recovery_"
            "hybrid_active_low_median_fear_greed.json"
        )
        probe_name = (
            "range_grid_strategy_recovery_"
            "hybrid_active_low_median_high_probe.json"
        )
        profiles = {}
        for strategy_name in (safe_name, probe_name):
            with open(
                os.path.join(ROOT, strategy_name),
                encoding="utf-8",
            ) as handle:
                profiles[strategy_name] = json.load(handle)

        with open(
            os.path.join(ROOT, "range_grid_strategy_test_set.txt"),
            encoding="utf-8",
        ) as handle:
            ranked_names = {
                line.strip()
                for line in handle
                if line.strip() and not line.lstrip().startswith("#")
            }

        for strategy_name, profile in profiles.items():
            self.assertIn(strategy_name, ranked_names)
            self.assertEqual(validate_strategy_config(profile), [])
            self.assertTrue(profile["paper_trading_enabled"])
            self.assertEqual(profile["backtest_starting_cash_usd"], 600)
            self.assertEqual(profile["max_grid_size"], 4)
            self.assertEqual(profile["dynamic_anchor_low_band_max"], 0.5)
            self.assertEqual(
                profile["entry_placement_mode_by_source"],
                {
                    "range_low": "resting_grid",
                    "range_median": "resting_grid",
                    "range_high_band": "triggered",
                },
            )
            self.assertEqual(
                profile["resting_grid_max_above_level_pct_by_source"],
                {"range_low": 0.0035, "range_median": 0.0055},
            )
            self.assertNotIn("momentum_entry_tolerance_pct_by_source", profile)
            self.assertFalse(profile["volatility_adaptive_entry_step_enabled"])
            self.assertFalse(profile["stale_level_reanchor_enabled"])
            self.assertEqual(
                profile["entry_policy_by_source"]["range_low"]["authority"],
                "price_first",
            )
            self.assertEqual(
                profile["entry_policy_by_source"]["range_median"]["authority"],
                "price_first",
            )
            for source in ("range_low", "range_median"):
                self.assertEqual(
                    set(profile["entry_policy_by_source"][source]),
                    {
                        "authority",
                        "position_size_multiplier",
                        "weather_bypassable_hard_safety_flags",
                    },
                )
            self.assertTrue(profile["minimum_order_floor_require_full_size"])
            self.assertEqual(
                profile["minimum_order_floor_sources"],
                "range_low,range_median",
            )
            self.assertEqual(profile["minimum_order_floor_usd"], 100)
            self.assertEqual(
                profile["minimum_order_floor_cash_reserve_usd"],
                100,
            )
            self.assertFalse(profile["inventory_hard_cap_enabled"])
            self.assertFalse(profile["bucket_inventory_hard_caps_enabled"])
            self.assertFalse(profile["sell_backlog_hard_block_enabled"])
            self.assertFalse(profile["open_sell_hard_cap_enabled"])
            self.assertFalse(profile["flow_hard_block_enabled"])
            self.assertFalse(profile["sell_repricing_enabled"])
            self.assertNotIn("aging_start_minutes", profile)
            self.assertNotIn("aging_step_minutes", profile)
            self.assertNotIn("aging_profit_reduction_pct", profile)
            self.assertEqual(profile["maker_fee_pct"], 0.004)
            self.assertEqual(profile["taker_fee_pct"], 0.008)
            self.assertEqual(profile["round_trip_fee_pct"], 0.012)
            self.assertEqual(
                profile["fear_greed_profit_target_max_multiplier_by_source"],
                {"range_low": 2, "range_median": 2},
            )

        safe = profiles[safe_name]
        probe = profiles[probe_name]
        self.assertEqual(safe["grid_anchor"], "low,median")
        self.assertEqual(safe["max_open_high_anchor_orders"], 0)
        self.assertEqual(probe["grid_anchor"], "low,median,high")
        self.assertEqual(probe["max_open_high_anchor_orders"], 1)
        self.assertEqual(probe["high_anchor_buy_cooldown_minutes"], 60)
        self.assertEqual(
            probe["buy_cooldown_minutes_by_source"]["range_high_band"],
            60,
        )
        self.assertEqual(
            probe["entry_policy_by_source"]["range_high_band"]["authority"],
            "chop_confirmed",
        )
        self.assertNotIn(
            "range_high_band",
            probe["minimum_order_floor_sources"].split(","),
        )

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
            action_policy={"risk_off_blocks_longs": True},
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

    def test_safe_weather_bypasses_only_configured_hard_safety_flags(self):
        config = {
            "source_entry_policy_enabled": True,
            "entry_policy_by_source": {
                "range_low": {
                    "authority": "price_first",
                    "weather_bypassable_hard_safety_flags": (
                        "source_health_block"
                    ),
                }
            },
        }
        source_health_risk = {
            "recommended_posture": "risk_off",
            "hard_safety_flags": ["source_health_block"],
        }

        allowed = source_entry_policy_decision(
            config,
            "range_low",
            action_recommendation="blocked",
            action_policy={"risk_off_blocks_longs": True},
            risk_context=source_health_risk,
            weather_report=weather_report(),
        )
        self.assertTrue(allowed["allowed"])
        self.assertTrue(allowed["bypass_sentiment_gate"])

        explicit_risk_off = source_entry_policy_decision(
            config,
            "range_low",
            action_recommendation="risk_off",
            risk_context=source_health_risk,
            weather_report=weather_report(),
        )
        self.assertFalse(explicit_risk_off["allowed"])
        self.assertEqual(
            explicit_risk_off["reason"],
            "source_policy_hard_risk",
        )

        mixed_flags = source_entry_policy_decision(
            config,
            "range_low",
            action_recommendation="blocked",
            risk_context={
                **source_health_risk,
                "hard_safety_flags": [
                    "source_health_block",
                    "exchange_connectivity_failed",
                ],
            },
            weather_report=weather_report(),
        )
        self.assertFalse(mixed_flags["allowed"])
        self.assertEqual(mixed_flags["reason"], "source_policy_hard_risk")

        danger_weather = weather_report()
        danger_weather["alert_level"] = "danger"
        danger = source_entry_policy_decision(
            config,
            "range_low",
            action_recommendation="blocked",
            risk_context=source_health_risk,
            weather_report=danger_weather,
        )
        self.assertFalse(danger["allowed"])
        self.assertEqual(danger["reason"], "source_policy_hard_risk")

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
                    "weather_bypassable_hard_safety_flags": 0.5,
                    "typo_field": 0.5,
                }
            },
        })

        self.assertTrue(any("entry_step_pct_by_source.range_low" in error for error in errors))
        self.assertTrue(any("authority" in error for error in errors))
        self.assertTrue(any("position_size_multiplier" in error for error in errors))
        self.assertTrue(
            any("weather_bypassable_hard_safety_flags" in error for error in errors)
        )
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
        self.assertFalse(candidate["inventory_hard_cap_enabled"])
        self.assertFalse(candidate["bucket_inventory_hard_caps_enabled"])
        self.assertFalse(candidate["sell_backlog_hard_block_enabled"])
        self.assertFalse(candidate["open_sell_hard_cap_enabled"])
        self.assertFalse(candidate["flow_hard_block_enabled"])
        self.assertEqual(candidate["max_open_sell_orders"], 999)
        self.assertTrue(candidate["stale_level_reanchor_enabled"])
        self.assertEqual(candidate["stale_level_reanchor_sources"], "range_low")
        self.assertEqual(
            candidate["stale_level_reanchor_weather_phases"],
            "dip_leveling_entry",
        )
        self.assertTrue(candidate["stale_level_reanchor_require_weather"])
        self.assertTrue(candidate["stale_level_reanchor_profit_guard_enabled"])
        self.assertAlmostEqual(
            candidate["entry_step_pct_by_source"]["range_low"],
            0.0028,
        )
        self.assertAlmostEqual(candidate["profit_target_pct"], 0.009)
        self.assertEqual(candidate["max_open_high_anchor_orders"], 2)
        self.assertAlmostEqual(candidate["dynamic_anchor_low_band_max"], 0.35)
        self.assertEqual(
            candidate["entry_policy_by_source"]["range_low"][
                "weather_bypassable_hard_safety_flags"
            ],
            "source_health_block",
        )
        self.assertEqual(
            candidate["entry_policy_by_source"]["range_median"][
                "weather_bypassable_hard_safety_flags"
            ],
            "source_health_block",
        )
        self.assertNotIn(
            "weather_bypassable_hard_safety_flags",
            candidate["entry_policy_by_source"]["range_high_band"],
        )
        strong_high_weather = weather_report()
        high_with_source_health_block = source_entry_policy_decision(
            candidate,
            "range_high_band",
            action_recommendation="blocked",
            risk_context={
                "recommended_posture": "risk_off",
                "hard_safety_flags": ["source_health_block"],
            },
            weather_report=strong_high_weather,
        )
        self.assertFalse(high_with_source_health_block["allowed"])
        self.assertEqual(
            high_with_source_health_block["reason"],
            "source_policy_hard_risk",
        )
        weak_high_weather = weather_report()
        weak_high_weather["market_opportunity"][
            "entry_opportunity_score"
        ] = 0.4
        weak_high = source_entry_policy_decision(
            candidate,
            "range_high_band",
            action_recommendation="blocked",
            weather_report=weak_high_weather,
        )
        self.assertFalse(weak_high["allowed"])
        self.assertEqual(
            weak_high["reason"],
            "source_policy_entry_score",
        )
        self.assertEqual(weak_high["size_multiplier"], 0.0)
        self.assertNotIn("source_entry_policy_enabled", active)

    def test_production_source_policy_profile_matches_proven_paper_candidate(self):
        candidate_path = os.path.join(
            ROOT,
            "range_grid_strategy_price_first_source_policy_candidate.json",
        )
        production_path = os.path.join(
            ROOT,
            "range_grid_strategy_production_recovery_source_policy.json",
        )
        with open(candidate_path, encoding="utf-8") as handle:
            candidate = json.load(handle)
        with open(production_path, encoding="utf-8") as handle:
            production = json.load(handle)

        self.assertEqual(validate_strategy_config(production), [])
        self.assertTrue(production["minimum_order_floor_enabled"])
        self.assertTrue(production["minimum_order_floor_require_full_size"])
        self.assertEqual(production["min_buy_notional_usd"], 8.0)
        self.assertEqual(
            production["minimum_order_floor_sources"],
            "range_low,range_median",
        )
        self.assertEqual(production["minimum_order_floor_usd"], 100.0)
        self.assertEqual(
            production["minimum_order_floor_cooldown_minutes"],
            0,
        )
        self.assertEqual(
            production["minimum_order_floor_cash_reserve_usd"],
            100.0,
        )
        self.assertEqual(production["maker_fee_pct"], 0.004)
        self.assertEqual(production["taker_fee_pct"], 0.008)
        self.assertEqual(production["round_trip_fee_pct"], 0.012)
        self.assertTrue(candidate.pop("paper_trading_enabled"))
        self.assertFalse(production.pop("paper_trading_enabled"))
        self.assertEqual(production, candidate)


if __name__ == "__main__":
    unittest.main()
