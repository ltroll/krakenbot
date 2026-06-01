import json
import os
import tempfile
import unittest

import llm_target_backtest as backtest


def make_snapshot(
    captured_at,
    price,
    *,
    action_recommendation="bullish_allowed",
    target_prices=None,
    quality_targets=None,
    strategy_overrides=None,
):
    strategy_payload = {
        "target_profit_pct": 0.005,
        "target_limit_max_premium_pct": 0.01,
        "prevent_buy_above_last_sell": True,
        "buy_after_sell_discount_pct": 0.001,
        "target_quality_enabled": True,
        "target_quality_fail_closed": False,
        "target_quality_min_samples": 20,
        "target_quality_min_ev_pct": 0.02,
        "target_quality_min_4h_fill_probability": 0.35,
        "target_quality_allowed_recommendations": "buy_allowed,watch",
    }
    if strategy_overrides:
        strategy_payload.update(strategy_overrides)

    targets = target_prices or [{"buy_price": 100.0, "sell_pct": 0.5}]
    quality = quality_targets or [{
        "buy_price": 100.0,
        "matched_sample_count": 30,
        "fill_probability": {
            "4h": {"sample_count": 30, "fill_probability": 0.6}
        },
        "best_profit_target_pct": 0.7,
        "best_expected_value_pct_per_signal": 0.08,
        "recommendation": "buy_allowed",
    }]

    return {
        "captured_at": captured_at,
        "signal": {
            "ok": True,
            "error": None,
            "payload": {
                "processed_at": captured_at,
                "action_recommendation": action_recommendation,
                "action_policy": {"reason": "policy test"},
                "execution_signal": 0.07,
                "confidence": 0.6,
                "contributor_count": 12,
                "target_prices": targets,
            },
        },
        "target_quality": {
            "ok": True,
            "error": None,
            "payload": {
                "status": "ok",
                "current_price": price,
                "targets": quality,
            },
        },
        "ticker": {
            "ok": True,
            "error": None,
            "last_price": price,
            "payload": {},
        },
        "strategy_profile": {
            "payload": strategy_payload,
        },
    }


class LlmTargetBacktestTests(unittest.TestCase):
    def setUp(self):
        self.original_snapshot_log_file = backtest.SNAPSHOT_LOG_FILE
        self.original_output_file = backtest.BACKTEST_OUTPUT_FILE
        self.original_archive_dir = backtest.BACKTEST_ARCHIVE_DIR
        self.original_window_hours = backtest.BACKTEST_WINDOW_HOURS
        self.original_entry_wait_hours = backtest.BACKTEST_ENTRY_WAIT_HOURS
        self.original_max_hold_hours = backtest.BACKTEST_MAX_HOLD_HOURS
        self.original_stop_loss_pct = backtest.BACKTEST_STOP_LOSS_PCT
        self.original_cooldown_minutes = backtest.BACKTEST_COOLDOWN_MINUTES
        self.original_fee_bps = backtest.BACKTEST_FEE_BPS

    def tearDown(self):
        backtest.SNAPSHOT_LOG_FILE = self.original_snapshot_log_file
        backtest.BACKTEST_OUTPUT_FILE = self.original_output_file
        backtest.BACKTEST_ARCHIVE_DIR = self.original_archive_dir
        backtest.BACKTEST_WINDOW_HOURS = self.original_window_hours
        backtest.BACKTEST_ENTRY_WAIT_HOURS = self.original_entry_wait_hours
        backtest.BACKTEST_MAX_HOLD_HOURS = self.original_max_hold_hours
        backtest.BACKTEST_STOP_LOSS_PCT = self.original_stop_loss_pct
        backtest.BACKTEST_COOLDOWN_MINUTES = self.original_cooldown_minutes
        backtest.BACKTEST_FEE_BPS = self.original_fee_bps

    def test_with_target_quality_uses_quality_profit_target(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 100.8),
        ]

        result = backtest.simulate_strategy("with_target_quality", snapshots)

        self.assertEqual(result["summary"]["trades"], 1)
        trade = result["recent_trades"][0]
        self.assertEqual(trade["exit_reason"], "take_profit")
        self.assertAlmostEqual(trade["exit_price"], 100.7, places=2)

    def test_with_target_quality_blocks_low_sample_target(self):
        quality_targets = [{
            "buy_price": 100.0,
            "matched_sample_count": 5,
            "fill_probability": {
                "4h": {"sample_count": 5, "fill_probability": 0.6}
            },
            "best_profit_target_pct": 0.7,
            "best_expected_value_pct_per_signal": 0.08,
            "recommendation": "buy_allowed",
        }]
        snapshots = [make_snapshot("2026-05-30T12:00:00+00:00", 101.0, quality_targets=quality_targets)]

        result = backtest.simulate_strategy("with_target_quality", snapshots)

        self.assertEqual(result["summary"]["trades"], 0)
        self.assertEqual(result["summary"]["blocked_by_target_quality"], 1)

    def test_build_report_and_write_report(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 100.8),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_file = os.path.join(tmpdir, "snapshots.jsonl")
            output_file = os.path.join(tmpdir, "llm_target_backtest.json")
            archive_dir = os.path.join(tmpdir, "archive")

            with open(snapshot_file, "w", encoding="utf-8") as f:
                for row in snapshots:
                    f.write(json.dumps(row) + "\n")

            backtest.SNAPSHOT_LOG_FILE = snapshot_file
            backtest.BACKTEST_OUTPUT_FILE = output_file
            backtest.BACKTEST_ARCHIVE_DIR = archive_dir
            backtest.BACKTEST_WINDOW_HOURS = 1000
            backtest.BACKTEST_ENTRY_WAIT_HOURS = 4
            backtest.BACKTEST_MAX_HOLD_HOURS = 24

            report = backtest.build_report()
            archive_file = backtest.write_report(report)

            self.assertTrue(os.path.exists(output_file))
            self.assertTrue(os.path.exists(archive_file))
            self.assertIn("with_target_quality", report["strategies"])
            self.assertEqual(report["bot_outputs"]["with_target_quality"]["trades"], 1)


if __name__ == "__main__":
    unittest.main()
