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
                "open_sell_count": 0,
                "open_buy_volume": 0.0,
                "open_sell_volume": 0.0,
                "deployed_inventory_usd": 0.0,
                "last_sell_price": 120.0,
                "last_llm_sell_at": None,
                "last_high_anchor_buy_at": None,
            },
            "open_buy_orders": [],
            "open_sell_orders": [],
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


if __name__ == "__main__":
    unittest.main()
