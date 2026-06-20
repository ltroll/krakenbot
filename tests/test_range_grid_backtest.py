import json
import os
import tempfile
import unittest

import range_grid_backtest as backtest


def make_snapshot(
    captured_at,
    price,
    *,
    action_recommendation="bullish_allowed",
    strategy_modes=None,
    target_prices=None,
    strategy_overrides=None,
    state_summary_overrides=None,
    open_sell_orders=None,
    runtime_status_overrides=None,
):
    strategy_payload = {
        "grid_anchor": "low,high",
        "entry_step_pct": 0.01,
        "max_grid_size": 2,
        "require_fresh_signal": True,
        "min_signal_status": "fresh",
        "mean_reversion_min_opportunity": 0.0,
        "prevent_buy_above_last_sell": True,
        "buy_after_sell_discount_pct": 0.001,
        "flow_defensive_threshold": -0.2,
        "flow_block_threshold": -0.4,
        "flow_defensive_size_multiplier": 0.75,
        "flow_block_high_only": True,
        "flow_block_llm_only_below": -0.5,
        "llm_buy_cooldown_minutes_after_sell": 30,
        "high_anchor_buy_cooldown_minutes": 15,
        "max_open_high_anchor_orders": 3,
        "max_open_sell_orders": 12,
        "max_inventory_usd": 750,
        "high_anchor_profit_target_pct": 0.006,
    }
    if strategy_overrides:
        strategy_payload.update(strategy_overrides)

    signal_payload = {
        "processed_at": captured_at,
        "action_recommendation": action_recommendation,
        "signal_status": "fresh",
        "bot_action_allowed": True,
        "mean_reversion_opportunity": 0.5,
        "flow_pressure": 0.1,
        "target_prices": target_prices or [{"buy_price": 98.0, "sell_pct": 0.5}],
        "price_regime": {
            "price_low_24h": 95.0,
            "price_high_24h": 105.0,
            "price_mean_24h": 100.0,
            "price_median_24h": 99.5,
        },
        "source_status": {
            "market_data": {"status": "fresh"},
        },
    }

    return {
        "captured_at": captured_at,
        "ticker": {"last_price": price},
        "signal": {"payload": signal_payload},
        "strategy_profile": {"payload": strategy_payload},
        "strategy_context": {
            "grid_anchor": ",".join(strategy_modes or ["low", "high"]),
            "strategy_modes": strategy_modes or ["low", "high"],
        },
        "state": {
            "summary": {
                "open_buy_count": 0,
                "open_sell_count": len(open_sell_orders or []),
                "open_buy_volume": 0.0,
                "open_sell_volume": 0.0,
                "deployed_inventory_usd": 0.0,
                "last_sell_price": 120.0,
                "last_llm_sell_at": None,
                "last_high_anchor_buy_at": None,
            } | (state_summary_overrides or {}),
            "open_buy_orders": [],
            "open_sell_orders": open_sell_orders or [],
        },
        "runtime_status": {
            "summary": {
                "timestamp": captured_at,
                "operating_mode": strategy_payload.get("operating_mode", "range_plus_llm"),
                "runtime_block_reason": None,
                "open_sell_count": len(open_sell_orders or []),
                "sell_backlog_oldest_minutes": None,
            } | (runtime_status_overrides or {})
        },
    }


class RangeGridBacktestTests(unittest.TestCase):
    def setUp(self):
        self.original_rotate_daily = backtest.SNAPSHOT_ROTATE_DAILY

    def tearDown(self):
        backtest.SNAPSHOT_ROTATE_DAILY = self.original_rotate_daily

    def test_snapshot_source_files_prefers_rotated_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = os.path.join(tmpdir, "range_grid_backtest_snapshot_log.jsonl")
            june12 = os.path.join(tmpdir, "range_grid_backtest_snapshot_log_20260612.jsonl")
            june13 = os.path.join(tmpdir, "range_grid_backtest_snapshot_log_20260613.jsonl")
            for path in (june12, june13):
                with open(path, "w", encoding="utf-8") as f:
                    f.write("")

            backtest.SNAPSHOT_ROTATE_DAILY = True
            files = backtest.snapshot_source_files(
                base_path,
                backtest.parse_iso8601("2026-06-12T12:00:00+00:00"),
                backtest.parse_iso8601("2026-06-13T12:00:00+00:00"),
            )

            self.assertEqual(files, [os.path.abspath(june12), os.path.abspath(june13)])

    def test_load_strategy_set_entries_supports_comment_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            strategy_set = os.path.join(tmpdir, "strategy_set.txt")
            with open(strategy_set, "w", encoding="utf-8") as f:
                f.write("# comment\n")
                f.write("\n")
                f.write("foo.json\n")
                f.write("bar.json\n")

            entries = backtest.load_strategy_set_entries(strategy_set)

            self.assertEqual(entries, ["foo.json", "bar.json"])

    def test_replay_blocks_when_sentiment_not_bullish(self):
        snapshots = [make_snapshot("2026-06-13T12:00:00+00:00", 100.0, action_recommendation="blocked")]
        snapshots[0]["signal"]["payload"]["action_policy"] = {
            "reason": "Bearish actions disabled for this asset."
        }

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(result["summary"]["approved_candidates"], 0)
        self.assertEqual(result["summary"]["hold_snapshots"], 1)
        self.assertIn("action_recommendation_blocked", result["summary"]["hold_reason_counts"])
        self.assertEqual(
            result["summary"]["hold_action_recommendation_counts"],
            {"blocked": 1},
        )
        self.assertEqual(
            result["summary"]["hold_action_policy_reason_counts"],
            {"Bearish actions disabled for this asset.": 1},
        )
        self.assertEqual(
            result["summary"]["hold_signal_status_counts"],
            {"fresh": 1},
        )
        self.assertEqual(
            result["recent_replay_events"][0]["action_policy_reason"],
            "Bearish actions disabled for this asset.",
        )

    def test_replay_approves_llm_target_candidate(self):
        snapshots = [make_snapshot("2026-06-13T12:00:00+00:00", 100.0, strategy_modes=["llm_target"])]

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(result["summary"]["raw_candidates"], 1)
        self.assertEqual(result["summary"]["approved_candidates"], 1)
        self.assertEqual(result["summary"]["approved_counts_by_source"]["llm_target"], 1)

    def test_replay_accepts_multi_asset_signal_payload(self):
        snapshot = make_snapshot(
            "2026-06-13T12:00:00+00:00",
            104.5,
            action_recommendation="watch_only",
            strategy_modes=["high"],
        )
        snapshot["signal"]["payload"] = {
            "schema_version": "multi-asset-sentiment-v1",
            "single_asset_schema_version": "web-sentiment-v2",
            "processed_at": "2026-06-13T12:00:00+00:00",
            "freshness": {
                "fresh_for_minutes": 10,
                "warn_after_minutes": 12,
                "stale_after_minutes": 20,
            },
            "assets": {
                "BTC": {
                    "asset_id": "BTC",
                    "asset": {"symbol": "BTC", "name": "Bitcoin"},
                    "processed_at": "2026-06-13T12:00:00+00:00",
                    "asset_price": 104.5,
                    "execution_signal": 0.02,
                    "confidence": 0.5,
                    "action_recommendation": "watch_only",
                    "bot_action_allowed": True,
                    "signal_status": "fresh",
                    "source_status": {
                        "asset_price": {"status": "fresh"},
                        "asset_price_regime": {"status": "fresh"},
                    },
                    "asset_price_regime": {
                        "price_low": 95.0,
                        "price_high": 105.0,
                        "price_mean": 100.0,
                        "price_median": 99.5,
                        "range_position": 0.95,
                    },
                    "mean_reversion_opportunity": 0.5,
                }
            },
        }

        result = backtest.replay_from_snapshots([snapshot])

        self.assertGreaterEqual(result["summary"]["raw_candidates"], 1)
        self.assertGreaterEqual(result["summary"]["approved_candidates"], 1)
        self.assertEqual(result["summary"]["hold_snapshots"], 0)

    def test_watch_only_allows_range_permissions(self):
        permissions = backtest.sentiment_buy_permissions("watch_only")
        self.assertFalse(permissions["llm_buys_allowed"])
        self.assertTrue(permissions["range_buys_allowed"])
        self.assertTrue(permissions["any_buys_allowed"])

    def test_replay_allows_high_band_candidate_during_watch_only(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="watch_only",
                strategy_modes=["high"],
            )
        ]

        result = backtest.replay_from_snapshots(snapshots)

        self.assertGreaterEqual(result["summary"]["raw_candidates"], 1)
        self.assertGreaterEqual(result["summary"]["approved_candidates"], 1)
        self.assertEqual(result["summary"]["hold_snapshots"], 0)

    def test_dynamic_anchor_selects_high_in_upper_range(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="watch_only",
                strategy_modes=["low", "high"],
                strategy_overrides={
                    "operating_mode": "range_only",
                    "dynamic_anchor_mode": True,
                    "dynamic_anchor_high_band_min": 0.75,
                    "dynamic_anchor_low_band_max": 0.35,
                    "dynamic_anchor_midpoint_split": 0.5,
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["price_regime"]["range_position_24h"] = 0.82

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(
            result["summary"]["candidate_counts_by_source"].get("range_high_band"),
            1,
        )
        self.assertEqual(result["summary"]["candidate_counts_by_source"].get("range_low"), None)

    def test_dynamic_anchor_falls_back_to_low_in_mid_range_when_only_low_high_configured(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                96.0,
                action_recommendation="watch_only",
                strategy_modes=["low", "high"],
                strategy_overrides={
                    "operating_mode": "range_only",
                    "dynamic_anchor_mode": True,
                    "dynamic_anchor_high_band_min": 0.75,
                    "dynamic_anchor_low_band_max": 0.35,
                    "dynamic_anchor_midpoint_split": 0.5,
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["price_regime"]["range_position_24h"] = 0.45

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(
            result["summary"]["candidate_counts_by_source"].get("range_low"),
            2,
        )
        self.assertIsNone(
            result["summary"]["candidate_counts_by_source"].get("range_high_band")
        )

    def test_dynamic_anchor_mid_range_reports_median_strategy_mode(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                99.0,
                action_recommendation="watch_only",
                strategy_modes=["low", "median", "high"],
                strategy_overrides={
                    "operating_mode": "range_only",
                    "dynamic_anchor_mode": True,
                    "dynamic_anchor_high_band_min": 0.75,
                    "dynamic_anchor_low_band_max": 0.35,
                    "dynamic_anchor_midpoint_split": 0.5,
                    "dynamic_anchor_mid_mode": "median",
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["price_regime"]["range_position_24h"] = 0.5

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(
            result["summary"]["candidate_counts_by_source"].get("range_median"),
            2,
        )
        self.assertEqual(
            result["summary"]["candidate_counts_by_strategy_mode"].get("median"),
            2,
        )
        self.assertEqual(
            result["recent_replay_events"][0]["active_strategy_modes"],
            ["median"],
        )
        self.assertEqual(
            result["recent_replay_events"][0]["strategy_mode"],
            "median",
        )

    def test_hold_summary_reports_active_strategy_modes(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                100.0,
                action_recommendation="risk_off",
                strategy_modes=["low", "median", "high"],
                strategy_overrides={
                    "operating_mode": "range_only",
                    "dynamic_anchor_mode": True,
                    "dynamic_anchor_high_band_min": 0.75,
                    "dynamic_anchor_low_band_max": 0.35,
                    "dynamic_anchor_midpoint_split": 0.5,
                    "dynamic_anchor_mid_mode": "median",
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["price_regime"]["range_position_24h"] = 0.5
        snapshots[0]["signal"]["payload"]["action_policy"] = {
            "reason": "Bearish sentiment is strong enough to block new long entries or require a stricter price discount.",
            "risk_off_blocks_longs": True,
        }

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(
            result["summary"]["hold_active_strategy_mode_counts"].get("median"),
            1,
        )
        self.assertEqual(
            result["recent_replay_events"][0]["active_strategy_modes"],
            ["median"],
        )

    def test_watch_only_still_blocks_llm_permissions(self):
        permissions = backtest.sentiment_buy_permissions("watch_only")
        self.assertFalse(permissions["llm_buys_allowed"])

    def test_effective_entry_step_pct_widens_with_volatility(self):
        widened = backtest.effective_entry_step_pct(
            0.01,
            0.04,
            {
                "volatility_adaptive_entry_step_enabled": True,
                "volatility_reference_pct": 0.02,
                "volatility_min_step_multiplier": 1.0,
                "volatility_max_step_multiplier": 2.0,
            },
        )
        self.assertEqual(widened, 0.02)

    def test_inventory_pressure_adjustment_reduces_size_near_inventory_cap(self):
        adjustment = backtest.inventory_pressure_adjustment(
            600.0,
            750.0,
            {
                "inventory_pressure_size_scaling_enabled": True,
                "inventory_pressure_start_usage_pct": 0.5,
                "inventory_pressure_min_size_multiplier": 0.25,
            },
        )
        self.assertAlmostEqual(adjustment["usage_ratio"], 0.8)
        self.assertAlmostEqual(adjustment["size_multiplier"], 0.55)

    def test_inventory_pressure_adjustment_defaults_to_full_size_when_disabled(self):
        adjustment = backtest.inventory_pressure_adjustment(
            600.0,
            750.0,
            {},
        )
        self.assertEqual(adjustment["usage_ratio"], 0.0)
        self.assertEqual(adjustment["size_multiplier"], 1.0)

    def test_build_candidates_uses_wider_spacing_when_volatility_is_high(self):
        snapshot = make_snapshot(
            "2026-06-13T12:00:00+00:00",
            95.0,
            action_recommendation="watch_only",
            strategy_modes=["low"],
            strategy_overrides={
                "volatility_adaptive_entry_step_enabled": True,
                "volatility_reference_pct": 0.02,
                "volatility_min_step_multiplier": 1.0,
                "volatility_max_step_multiplier": 2.0,
            },
        )
        snapshot["signal"]["payload"]["price_regime"]["realized_volatility_24h_pct"] = 0.04

        result = backtest.build_candidates(snapshot, 95.0)

        self.assertEqual(result["effective_entry_step_pct"], 0.02)
        self.assertEqual(
            [round(candidate["level"], 2) for candidate in result["raw_candidates"]],
            [93.1, 91.2],
        )

    def test_confidence_liquidity_block_can_allow_range_only_override(self):
        permissions = backtest.sentiment_buy_permissions(
            "blocked",
            {
                "reason": "Elevated liquidity risk 0.450 requires confidence >= 0.600 and 12 contributors.",
                "max_liquidity_risk": 0.8,
            },
            operating_mode="range_only",
            allow_range_buy_on_confidence_block=True,
        )
        self.assertFalse(permissions["llm_buys_allowed"])
        self.assertTrue(permissions["range_buys_allowed"])
        self.assertTrue(permissions["any_buys_allowed"])

    def test_confidence_liquidity_block_does_not_override_without_flag(self):
        permissions = backtest.sentiment_buy_permissions(
            "blocked",
            {
                "reason": "Elevated liquidity risk 0.450 requires confidence >= 0.600 and 12 contributors.",
                "max_liquidity_risk": 0.8,
            },
            operating_mode="range_only",
            allow_range_buy_on_confidence_block=False,
        )
        self.assertFalse(permissions["llm_buys_allowed"])
        self.assertFalse(permissions["range_buys_allowed"])
        self.assertFalse(permissions["any_buys_allowed"])

    def test_risk_modulated_block_allows_core_range_but_not_high_or_llm(self):
        permissions = backtest.sentiment_buy_permissions(
            "blocked",
            {
                "reason": "BTC bullish utility is disabled by recent backtest calibration.",
            },
            operating_mode="range_plus_llm",
            sentiment_control_mode="risk_modulated",
        )
        self.assertFalse(permissions["llm_buys_allowed"])
        self.assertTrue(permissions["range_core_buys_allowed"])
        self.assertFalse(permissions["range_high_buys_allowed"])
        self.assertTrue(permissions["range_buys_allowed"])

    def test_risk_modulated_contrarian_watch_allows_core_range_but_not_high_or_llm(self):
        permissions = backtest.sentiment_buy_permissions(
            "contrarian_watch",
            {
                "reason": "Extreme bearish sentiment is watch-only; recent backtests show possible rebound or exhaustion risk.",
            },
            operating_mode="range_plus_llm",
            sentiment_control_mode="risk_modulated",
        )
        self.assertFalse(permissions["llm_buys_allowed"])
        self.assertTrue(permissions["range_core_buys_allowed"])
        self.assertFalse(permissions["range_high_buys_allowed"])
        self.assertTrue(permissions["range_buys_allowed"])

    def test_risk_modulated_risk_off_still_blocks_all_range_buys(self):
        permissions = backtest.sentiment_buy_permissions(
            "risk_off",
            {
                "reason": "Bearish sentiment is strong enough to block new long entries or require a stricter price discount.",
                "risk_off_blocks_longs": True,
            },
            operating_mode="range_plus_llm",
            sentiment_control_mode="risk_modulated",
        )
        self.assertFalse(permissions["llm_buys_allowed"])
        self.assertFalse(permissions["range_core_buys_allowed"])
        self.assertFalse(permissions["range_high_buys_allowed"])
        self.assertFalse(permissions["range_buys_allowed"])

    def test_replay_allows_range_only_override_on_confidence_liquidity_block(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="blocked",
                strategy_modes=["high"],
                strategy_overrides={
                    "operating_mode": "range_only",
                    "allow_range_buy_on_confidence_block": True,
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["action_policy"] = {
            "reason": "Elevated liquidity risk 0.450 requires confidence >= 0.600 and 12 contributors.",
            "max_liquidity_risk": 0.8,
        }

        result = backtest.replay_from_snapshots(snapshots)

        self.assertGreaterEqual(result["summary"]["raw_candidates"], 1)
        self.assertGreaterEqual(result["summary"]["approved_candidates"], 1)
        self.assertEqual(result["summary"]["hold_snapshots"], 0)

    def test_replay_risk_modulated_allows_low_range_during_blocked_calibration(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                94.0,
                action_recommendation="blocked",
                strategy_modes=["low"],
                strategy_overrides={
                    "operating_mode": "range_plus_llm",
                    "sentiment_control_mode": "risk_modulated",
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["action_policy"] = {
            "reason": "BTC bullish utility is disabled by recent backtest calibration."
        }

        result = backtest.replay_from_snapshots(snapshots)

        self.assertGreaterEqual(result["summary"]["raw_candidates"], 1)
        self.assertGreaterEqual(result["summary"]["approved_candidates"], 1)

    def test_replay_risk_modulated_allows_low_range_during_contrarian_watch(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                94.0,
                action_recommendation="contrarian_watch",
                strategy_modes=["low"],
                strategy_overrides={
                    "operating_mode": "range_plus_llm",
                    "sentiment_control_mode": "risk_modulated",
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["action_policy"] = {
            "reason": "Extreme bearish sentiment is watch-only; recent backtests show possible rebound or exhaustion risk."
        }

        result = backtest.replay_from_snapshots(snapshots)

        self.assertGreaterEqual(result["summary"]["raw_candidates"], 1)
        self.assertGreaterEqual(result["summary"]["approved_candidates"], 1)

    def test_replay_risk_modulated_blocks_high_range_during_blocked_calibration(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="blocked",
                strategy_modes=["high"],
                strategy_overrides={
                    "operating_mode": "range_plus_llm",
                    "sentiment_control_mode": "risk_modulated",
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["action_policy"] = {
            "reason": "BTC bullish utility is disabled by recent backtest calibration."
        }

        result = backtest.replay_from_snapshots(snapshots)

        self.assertGreaterEqual(result["summary"]["raw_candidates"], 1)
        self.assertEqual(result["summary"]["approved_candidates"], 0)

    def test_missed_opportunities_reports_approved_but_not_placed(self):
        replay = {
            "summary": {
                "approved_candidates": 3,
                "approved_counts_by_source": {
                    "range_high_band": 2,
                    "range_low": 1,
                },
            },
            "recent_replay_events": [
                {
                    "captured_at": "2026-06-13T12:00:00+00:00",
                    "buy_source": "range_high_band",
                    "price": 104.5,
                    "level": 104.5,
                    "status": "approved_gate_only",
                },
                {
                    "captured_at": "2026-06-13T12:01:00+00:00",
                    "buy_source": "range_high_band",
                    "price": 104.4,
                    "level": 104.4,
                    "status": "approved_gate_only",
                },
                {
                    "captured_at": "2026-06-13T12:02:00+00:00",
                    "buy_source": "range_low",
                    "price": 95.0,
                    "level": 95.0,
                    "status": "approved_gate_only",
                },
            ],
        }
        actual = {
            "buy_orders_placed": 1,
            "buy_orders_placed_by_source": {
                "range_high_band": 1,
            }
        }

        summary = backtest.summarize_missed_approved_opportunities(
            replay,
            actual,
        )

        self.assertEqual(summary["approved_but_not_placed"], 2)
        self.assertEqual(
            summary["approved_but_not_placed_by_source"],
            {"range_high_band": 1, "range_low": 1},
        )
        self.assertAlmostEqual(summary["placement_rate_vs_approved"], 0.3333, places=4)
        self.assertEqual(
            summary["recent_approved_but_not_placed"],
            [
                {
                    "captured_at": "2026-06-13T12:01:00+00:00",
                    "buy_source": "range_high_band",
                    "price": 104.4,
                    "level": 104.4,
                    "status": "approved_but_not_placed",
                    "likely_live_blockers": [],
                    "potential": None,
                },
                {
                    "captured_at": "2026-06-13T12:02:00+00:00",
                    "buy_source": "range_low",
                    "price": 95.0,
                    "level": 95.0,
                    "status": "approved_but_not_placed",
                    "likely_live_blockers": [],
                    "potential": None,
                },
            ],
        )

    def test_missed_opportunities_uses_recent_approved_events_when_recent_window_has_only_holds(self):
        replay = {
            "summary": {
                "approved_candidates": 2,
                "approved_counts_by_source": {
                    "range_high_band": 2,
                },
            },
            "recent_replay_events": [
                {
                    "captured_at": "2026-06-13T12:10:00+00:00",
                    "price": 106.0,
                    "action_recommendation": "blocked",
                    "hold_reason": "action_recommendation_blocked",
                    "raw_candidate_count": 0,
                },
                {
                    "captured_at": "2026-06-13T12:11:00+00:00",
                    "price": 106.1,
                    "action_recommendation": "blocked",
                    "hold_reason": "action_recommendation_blocked",
                    "raw_candidate_count": 0,
                },
            ],
            "recent_approved_events": [
                {
                    "captured_at": "2026-06-13T12:01:00+00:00",
                    "buy_source": "range_high_band",
                    "price": 104.4,
                    "level": 104.4,
                    "status": "approved_gate_only",
                    "reason": None,
                },
                {
                    "captured_at": "2026-06-13T12:02:00+00:00",
                    "buy_source": "range_high_band",
                    "price": 104.5,
                    "level": 104.5,
                    "status": "approved_gate_only",
                    "reason": None,
                },
            ],
        }
        actual = {
            "buy_orders_placed": 0,
            "buy_orders_placed_by_source": {},
        }

        summary = backtest.summarize_missed_approved_opportunities(
            replay,
            actual,
        )

        self.assertEqual(
            summary["recent_approved_but_not_placed"],
            [
                {
                    "captured_at": "2026-06-13T12:01:00+00:00",
                    "buy_source": "range_high_band",
                    "price": 104.4,
                    "level": 104.4,
                    "status": "approved_but_not_placed",
                    "likely_live_blockers": [],
                    "potential": None,
                },
                {
                    "captured_at": "2026-06-13T12:02:00+00:00",
                    "buy_source": "range_high_band",
                    "price": 104.5,
                    "level": 104.5,
                    "status": "approved_but_not_placed",
                    "likely_live_blockers": [],
                    "potential": None,
                },
            ],
        )

    def test_missed_opportunities_include_blockers_and_potential_summary(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="watch_only",
                strategy_modes=["high"],
                strategy_overrides={
                    "disable_new_buys_on_sell_backlog_count": 1,
                    "operating_mode": "range_only",
                },
                open_sell_orders=[
                    {
                        "placed_at": "2026-06-13T00:00:00+00:00",
                    }
                ],
            ),
            make_snapshot(
                "2026-06-13T12:10:00+00:00",
                105.3,
                action_recommendation="watch_only",
                strategy_modes=["high"],
            ),
        ]

        replay = backtest.replay_from_snapshots(snapshots)
        actual = {
            "buy_orders_placed": 0,
            "buy_orders_placed_by_source": {},
        }

        summary = backtest.summarize_missed_approved_opportunities(
            replay,
            actual,
            snapshots,
        )

        self.assertEqual(summary["approved_but_not_placed"], 1)
        self.assertEqual(summary["likely_live_blockers"], {"sell_backlog_count": 1})
        self.assertEqual(
            summary["recent_approved_but_not_placed"][0]["likely_live_blockers"],
            ["sell_backlog_count"],
        )
        self.assertTrue(
            summary["recent_approved_but_not_placed"][0]["potential"]["take_profit_reached"]
        )
        self.assertEqual(summary["potential_summary"]["evaluated_count"], 1)
        self.assertEqual(summary["potential_summary"]["take_profit_reached_count"], 1)

    def test_missed_opportunities_prefer_runtime_status_block_reason(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="watch_only",
                strategy_modes=["high"],
                runtime_status_overrides={
                    "runtime_block_reason": "sell_backlog_count",
                    "operating_mode": "range_only",
                    "open_sell_count": 4,
                    "sell_backlog_oldest_minutes": 500.0,
                },
            ),
            make_snapshot(
                "2026-06-13T12:10:00+00:00",
                105.3,
                action_recommendation="watch_only",
                strategy_modes=["high"],
            ),
        ]
        replay = backtest.replay_from_snapshots(snapshots)
        actual = {
            "buy_orders_placed": 0,
            "buy_orders_placed_by_source": {},
        }

        summary = backtest.summarize_missed_approved_opportunities(
            replay,
            actual,
            snapshots,
        )

        self.assertEqual(summary["likely_live_blockers"], {"sell_backlog_count": 1})
        self.assertEqual(
            summary["recent_approved_but_not_placed"][0]["likely_live_blockers"],
            ["sell_backlog_count"],
        )

    def test_missed_opportunities_ignore_stale_runtime_execution_blockers(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="watch_only",
                strategy_modes=["high"],
                strategy_overrides={
                    "operating_mode": "range_only",
                },
                runtime_status_overrides={
                    "timestamp": "2026-06-13T11:50:00+00:00",
                    "operating_mode": "range_only",
                    "effective_position_size_pct": 0.0,
                    "effective_max_inventory_usd": 0.0,
                    "effective_max_open_sell_orders": 1,
                    "open_sell_count": 1,
                },
            ),
            make_snapshot(
                "2026-06-13T12:10:00+00:00",
                105.3,
                action_recommendation="watch_only",
                strategy_modes=["high"],
            ),
        ]
        replay = backtest.replay_from_snapshots(snapshots)
        actual = {
            "buy_orders_placed": 0,
            "buy_orders_placed_by_source": {},
        }

        summary = backtest.summarize_missed_approved_opportunities(
            replay,
            actual,
            snapshots,
        )

        self.assertEqual(summary["likely_live_blockers"], {})
        self.assertEqual(
            summary["recent_approved_but_not_placed"][0]["likely_live_blockers"],
            [],
        )

    def test_missed_opportunities_include_runtime_execution_blockers(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                104.5,
                action_recommendation="watch_only",
                strategy_modes=["high"],
                strategy_overrides={
                    "operating_mode": "range_only",
                },
                runtime_status_overrides={
                    "operating_mode": "range_only",
                    "effective_position_size_pct": 0.0,
                    "effective_max_inventory_usd": 0.0,
                    "effective_max_open_sell_orders": 1,
                    "open_sell_count": 1,
                    "high_anchor_enabled": False,
                },
            ),
            make_snapshot(
                "2026-06-13T12:10:00+00:00",
                105.3,
                action_recommendation="watch_only",
                strategy_modes=["high"],
            ),
        ]

        replay = backtest.replay_from_snapshots(snapshots)
        actual = {
            "buy_orders_placed": 0,
            "buy_orders_placed_by_source": {},
        }

        summary = backtest.summarize_missed_approved_opportunities(
            replay,
            actual,
            snapshots,
        )

        self.assertEqual(
            summary["likely_live_blockers"],
            {
                "effective_position_size_pct_zero": 1,
                "effective_max_inventory_usd_zero": 1,
                "effective_max_open_sell_orders": 1,
                "high_anchor_disabled": 1,
            },
        )
        self.assertEqual(
            summary["recent_approved_but_not_placed"][0]["likely_live_blockers"],
            [
                "effective_position_size_pct_zero",
                "effective_max_inventory_usd_zero",
                "effective_max_open_sell_orders",
                "high_anchor_disabled",
            ],
        )

    def test_build_strategy_comparison_rows_compares_variants(self):
        snapshots = [
            make_snapshot(
                "2026-06-13T12:00:00+00:00",
                99.0,
                action_recommendation="watch_only",
                strategy_modes=["low", "median", "high"],
                strategy_overrides={
                    "grid_anchor": "low,median,high",
                    "operating_mode": "range_only",
                    "dynamic_anchor_mode": True,
                    "dynamic_anchor_mid_mode": "median",
                    "dynamic_anchor_low_band_max": 0.35,
                    "dynamic_anchor_high_band_min": 0.75,
                    "dynamic_anchor_midpoint_split": 0.5,
                },
            )
        ]
        snapshots[0]["signal"]["payload"]["price_regime"]["range_position_24h"] = 0.5

        with tempfile.TemporaryDirectory() as tmpdir:
            base_strategy = os.path.join(tmpdir, "base.json")
            tighter_strategy = os.path.join(tmpdir, "tighter.json")
            strategy_set = os.path.join(tmpdir, "strategy_set.txt")

            with open(base_strategy, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "grid_anchor": "low,median,high",
                        "operating_mode": "range_only",
                        "dynamic_anchor_mode": True,
                        "dynamic_anchor_mid_mode": "median",
                        "dynamic_anchor_low_band_max": 0.35,
                        "dynamic_anchor_high_band_min": 0.75,
                        "dynamic_anchor_midpoint_split": 0.5,
                        "entry_step_pct": 0.01,
                        "max_grid_size": 2,
                        "require_fresh_signal": True,
                        "min_signal_status": "fresh",
                        "mean_reversion_min_opportunity": 0.0,
                        "prevent_buy_above_last_sell": True,
                        "buy_after_sell_discount_pct": 0.001,
                        "flow_defensive_threshold": -0.2,
                        "flow_block_threshold": -0.4,
                        "flow_defensive_size_multiplier": 0.75,
                        "flow_block_high_only": True,
                        "flow_block_llm_only_below": -0.5,
                        "llm_buy_cooldown_minutes_after_sell": 30,
                        "high_anchor_buy_cooldown_minutes": 15,
                        "max_open_high_anchor_orders": 3,
                        "max_open_sell_orders": 12,
                        "max_inventory_usd": 750,
                        "high_anchor_profit_target_pct": 0.006,
                    },
                    f,
                )
            with open(tighter_strategy, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "grid_anchor": "low,median,high",
                        "operating_mode": "range_only",
                        "dynamic_anchor_mode": True,
                        "dynamic_anchor_mid_mode": "median",
                        "dynamic_anchor_low_band_max": 0.35,
                        "dynamic_anchor_high_band_min": 0.75,
                        "dynamic_anchor_midpoint_split": 0.5,
                        "entry_step_pct": 0.005,
                        "max_grid_size": 2,
                        "require_fresh_signal": True,
                        "min_signal_status": "fresh",
                        "mean_reversion_min_opportunity": 0.0,
                        "prevent_buy_above_last_sell": True,
                        "buy_after_sell_discount_pct": 0.001,
                        "flow_defensive_threshold": -0.2,
                        "flow_block_threshold": -0.4,
                        "flow_defensive_size_multiplier": 0.75,
                        "flow_block_high_only": True,
                        "flow_block_llm_only_below": -0.5,
                        "llm_buy_cooldown_minutes_after_sell": 30,
                        "high_anchor_buy_cooldown_minutes": 15,
                        "max_open_high_anchor_orders": 3,
                        "max_open_sell_orders": 12,
                        "max_inventory_usd": 750,
                        "high_anchor_profit_target_pct": 0.006,
                    },
                    f,
                )
            with open(strategy_set, "w", encoding="utf-8") as f:
                f.write(base_strategy + "\n")
                f.write(tighter_strategy + "\n")

            comparison = backtest.build_strategy_comparison_rows(
                snapshots,
                strategy_set,
            )

            self.assertEqual(comparison["count"], 2)
            labels = [row["strategy_label"] for row in comparison["rows"]]
            self.assertIn("base", labels)
            self.assertIn("tighter", labels)
            for row in comparison["rows"]:
                self.assertIn("approved_candidates", row)
                self.assertIn("potential_take_profit_reached_rate", row)

    def test_write_strategy_comparison_csv_outputs_digestible_table(self):
        comparison = {
            "rows": [
                {
                    "strategy_label": "baseline",
                    "strategy_file": "/tmp/baseline.json",
                    "grid_anchor": "low,median,high",
                    "operating_mode": "range_only",
                    "sentiment_control_mode": "risk_modulated",
                    "dynamic_anchor_mode": True,
                    "entry_step_pct": 0.0045,
                    "volatility_reference_pct": 0.02,
                    "raw_candidates": 10,
                    "approved_candidates": 2,
                    "hold_snapshots": 5,
                    "approved_llm_target": 0,
                    "approved_range_low": 0,
                    "approved_range_median": 2,
                    "approved_range_high_band": 0,
                    "blocked_price_above_level": 8,
                    "blocked_sentiment_high": 0,
                    "potential_evaluated_count": 2,
                    "potential_take_profit_reached_rate": 0.5,
                    "potential_avg_end_return_pct": 0.12,
                    "potential_avg_max_runup_pct": 0.4,
                    "potential_avg_max_drawdown_pct": -0.2,
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "compare.csv")
            resolved = backtest.write_strategy_comparison_csv(comparison, output_path)

            self.assertEqual(resolved, output_path)
            with open(output_path, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("strategy_label", text)
            self.assertIn("baseline", text)
            self.assertIn("approved_candidates", text)

    def test_build_ranked_strategy_rows_sorts_best_candidate_first(self):
        comparison = {
            "rows": [
                {
                    "strategy_label": "slower",
                    "strategy_file": "/tmp/slower.json",
                    "grid_anchor": "low,median,high",
                    "operating_mode": "range_only",
                    "sentiment_control_mode": "risk_modulated",
                    "entry_step_pct": 0.0045,
                    "volatility_reference_pct": 0.02,
                    "raw_candidates": 20,
                    "approved_candidates": 1,
                    "hold_snapshots": 10,
                    "approved_range_low": 0,
                    "approved_range_median": 1,
                    "approved_range_high_band": 0,
                    "blocked_price_above_level": 19,
                    "blocked_sentiment_high": 0,
                    "potential_take_profit_reached_rate": 0.25,
                    "potential_avg_end_return_pct": 0.03,
                    "potential_avg_max_runup_pct": 0.2,
                    "potential_avg_max_drawdown_pct": -0.3,
                },
                {
                    "strategy_label": "better",
                    "strategy_file": "/tmp/better.json",
                    "grid_anchor": "low,median,high",
                    "operating_mode": "range_only",
                    "sentiment_control_mode": "risk_modulated",
                    "entry_step_pct": 0.0045,
                    "volatility_reference_pct": 0.0235,
                    "raw_candidates": 10,
                    "approved_candidates": 3,
                    "hold_snapshots": 5,
                    "approved_range_low": 0,
                    "approved_range_median": 3,
                    "approved_range_high_band": 0,
                    "blocked_price_above_level": 7,
                    "blocked_sentiment_high": 0,
                    "potential_take_profit_reached_rate": 0.66,
                    "potential_avg_end_return_pct": 0.11,
                    "potential_avg_max_runup_pct": 0.35,
                    "potential_avg_max_drawdown_pct": -0.18,
                },
            ]
        }

        ranked = backtest.build_ranked_strategy_rows(comparison)

        self.assertEqual(ranked[0]["strategy_label"], "better")
        self.assertGreater(ranked[0]["practical_score"], ranked[1]["practical_score"])
        self.assertIn("candidate_efficiency", ranked[0])

    def test_write_ranked_strategy_csv_outputs_ranked_table(self):
        comparison = {
            "rows": [
                {
                    "strategy_label": "baseline",
                    "strategy_file": "/tmp/baseline.json",
                    "grid_anchor": "low,median,high",
                    "operating_mode": "range_only",
                    "sentiment_control_mode": "risk_modulated",
                    "entry_step_pct": 0.0045,
                    "volatility_reference_pct": 0.02,
                    "raw_candidates": 10,
                    "approved_candidates": 2,
                    "hold_snapshots": 5,
                    "approved_range_low": 0,
                    "approved_range_median": 2,
                    "approved_range_high_band": 0,
                    "blocked_price_above_level": 8,
                    "blocked_sentiment_high": 0,
                    "potential_take_profit_reached_rate": 0.5,
                    "potential_avg_end_return_pct": 0.12,
                    "potential_avg_max_runup_pct": 0.4,
                    "potential_avg_max_drawdown_pct": -0.2,
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "ranked.csv")
            resolved = backtest.write_ranked_strategy_csv(comparison, output_path)

            self.assertEqual(resolved, output_path)
            with open(output_path, encoding="utf-8") as f:
                text = f.read()
            self.assertIn("practical_score", text)
            self.assertIn("candidate_efficiency", text)
            self.assertIn("baseline", text)


if __name__ == "__main__":
    unittest.main()
