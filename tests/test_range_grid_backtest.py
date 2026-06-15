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

    def test_replay_blocks_when_sentiment_not_bullish(self):
        snapshots = [make_snapshot("2026-06-13T12:00:00+00:00", 100.0, action_recommendation="blocked")]

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(result["summary"]["approved_candidates"], 0)
        self.assertEqual(result["summary"]["hold_snapshots"], 1)
        self.assertIn("action_recommendation_blocked", result["summary"]["hold_reason_counts"])

    def test_replay_approves_llm_target_candidate(self):
        snapshots = [make_snapshot("2026-06-13T12:00:00+00:00", 100.0, strategy_modes=["llm_target"])]

        result = backtest.replay_from_snapshots(snapshots)

        self.assertEqual(result["summary"]["raw_candidates"], 1)
        self.assertEqual(result["summary"]["approved_candidates"], 1)
        self.assertEqual(result["summary"]["approved_counts_by_source"]["llm_target"], 1)

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

    def test_watch_only_still_blocks_llm_permissions(self):
        permissions = backtest.sentiment_buy_permissions("watch_only")
        self.assertFalse(permissions["llm_buys_allowed"])

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


if __name__ == "__main__":
    unittest.main()
