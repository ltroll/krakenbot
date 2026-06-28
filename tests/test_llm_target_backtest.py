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
        "round_trip_fee_pct": 0.0032,
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

    targets = (
        target_prices
        if target_prices is not None else
        [{"buy_price": 100.0, "sell_pct": 0.5}]
    )
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
        self.original_rotate_daily = backtest.SNAPSHOT_ROTATE_DAILY

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
        backtest.SNAPSHOT_ROTATE_DAILY = self.original_rotate_daily

    def test_with_target_quality_uses_quality_profit_target(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 101.1),
        ]

        result = backtest.simulate_strategy("with_target_quality", snapshots)

        self.assertEqual(result["summary"]["trades"], 1)
        self.assertEqual(result["summary"]["raw_candidates"], 1)
        self.assertEqual(result["summary"]["approved_candidates"], 1)
        self.assertEqual(result["summary"]["fill_rate_after_approval"], 1.0)
        trade = result["recent_trades"][0]
        self.assertEqual(trade["exit_reason"], "take_profit")
        self.assertAlmostEqual(trade["exit_price"], 101.02, places=2)
        self.assertAlmostEqual(trade["fee_bps"], 32.0, places=2)
        self.assertAlmostEqual(trade["gross_return_pct"], 1.02, places=2)
        self.assertAlmostEqual(trade["net_return_pct"], 0.7, places=2)

    def test_backtest_accepts_multi_asset_signal_payload(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 101.1),
        ]
        snapshots[0]["signal"]["payload"] = {
            "schema_version": "multi-asset-sentiment-v1",
            "single_asset_schema_version": "web-sentiment-v2",
            "processed_at": "2026-05-30T12:00:00+00:00",
            "assets": {
                "BTC": snapshots[0]["signal"]["payload"],
                "ETH": {
                    "processed_at": "2026-05-30T12:00:00+00:00",
                    "action_recommendation": "blocked",
                    "execution_signal": -0.5,
                    "confidence": 0.2,
                    "target_prices": [{"buy_price": 1.0, "sell_pct": 0.5}],
                },
            },
        }

        result = backtest.simulate_strategy("with_target_quality", snapshots)

        self.assertEqual(result["summary"]["trades"], 1)
        self.assertEqual(result["recent_trades"][0]["entry_price"], 100.0)

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
        self.assertEqual(result["summary"]["raw_candidates"], 1)
        self.assertEqual(result["summary"]["approved_candidates"], 0)
        self.assertIsNone(result["summary"]["fill_rate_after_approval"])
        self.assertEqual(result["summary"]["blocked_by_target_quality"], 1)

    def test_effective_fee_bps_prefers_maker_taker_fee_fields(self):
        snapshot = make_snapshot(
            "2026-05-30T12:00:00+00:00",
            101.0,
            strategy_overrides={
                "maker_fee_pct": 0.0025,
                "taker_fee_pct": 0.004,
                "round_trip_fee_pct": 0.0032,
            },
        )

        self.assertAlmostEqual(backtest.effective_fee_bps(snapshot), 65.0, places=2)

    def test_backtest_derives_missing_targets_from_quality_targets(self):
        snapshots = [
            make_snapshot(
                "2026-05-30T12:00:00+00:00",
                101.0,
                target_prices=[]
            ),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 101.1),
        ]

        result = backtest.simulate_strategy("with_target_quality", snapshots)

        summary = result["summary"]
        self.assertEqual(summary["raw_candidates"], 1)
        self.assertEqual(summary["quality_fallback_target_snapshots"], 1)
        self.assertEqual(summary["signal_target_snapshots"], 0)
        self.assertEqual(summary["trades"], 1)

    def test_target_diagnostics_counts_signal_and_quality_targets(self):
        snapshots = [
            make_snapshot(
                "2026-05-30T12:00:00+00:00",
                101.0,
                target_prices=[]
            ),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
        ]

        diagnostics = backtest.target_diagnostics(snapshots)

        self.assertEqual(diagnostics["snapshots"], 2)
        self.assertEqual(diagnostics["snapshots_with_signal"], 2)
        self.assertEqual(diagnostics["snapshots_with_signal_targets"], 1)
        self.assertEqual(diagnostics["snapshots_with_quality_targets"], 2)
        self.assertEqual(diagnostics["snapshots_with_quality_fallback_targets"], 1)

    def test_sentiment_block_records_shadow_target_quality(self):
        snapshots = [
            make_snapshot(
                "2026-05-30T12:00:00+00:00",
                101.0,
                action_recommendation="contrarian_watch"
            )
        ]

        result = backtest.simulate_strategy("with_target_quality", snapshots)

        summary = result["summary"]
        self.assertEqual(summary["raw_candidates"], 1)
        self.assertEqual(summary["approved_candidates"], 0)
        self.assertEqual(summary["blocked_by_sentiment"], 1)
        self.assertEqual(summary["shadow_target_quality_approved"], 1)
        self.assertEqual(summary["shadow_target_quality_rejected"], 0)
        decision = result["recent_decisions"][0]
        self.assertTrue(decision["shadow_target_quality"]["allowed"])

    def test_target_quality_only_ignores_sentiment_but_uses_quality(self):
        snapshots = [
            make_snapshot(
                "2026-05-30T12:00:00+00:00",
                101.0,
                action_recommendation="watch_only"
            ),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 101.1),
        ]

        result = backtest.simulate_strategy("target_quality_only", snapshots)

        self.assertEqual(result["summary"]["approved_candidates"], 1)
        self.assertEqual(result["summary"]["blocked_by_sentiment"], 0)
        self.assertEqual(result["summary"]["trades"], 1)

    def test_price_target_variant_uses_larger_profit_target(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T13:30:00+00:00", 101.2),
        ]

        default_result = backtest.simulate_strategy("price_target_only", snapshots)
        variant_result = backtest.simulate_strategy(
            "price_target_only_tp_0_8",
            snapshots
        )

        self.assertEqual(default_result["recent_trades"][0]["exit_time"], "2026-05-30T13:00:00+00:00")
        self.assertEqual(variant_result["recent_trades"][0]["exit_time"], "2026-05-30T13:30:00+00:00")
        self.assertAlmostEqual(
            variant_result["recent_trades"][0]["gross_return_pct"],
            1.12,
            places=2
        )
        self.assertAlmostEqual(
            variant_result["recent_trades"][0]["net_return_pct"],
            0.8,
            places=2
        )

    def test_profit_only_variant_holds_through_stop_loss(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 99.4),
        ]

        stop_result = backtest.simulate_strategy("price_target_only", snapshots)
        hold_result = backtest.simulate_strategy(
            "price_target_only_tp_1_2_hold",
            snapshots
        )

        self.assertEqual(stop_result["summary"]["stop_loss_count"], 1)
        self.assertEqual(hold_result["summary"]["stop_loss_count"], 0)
        self.assertEqual(hold_result["summary"]["trades"], 0)
        self.assertEqual(hold_result["summary"]["open_position_count"], 1)
        self.assertEqual(len(hold_result["open_positions"]), 1)
        self.assertLess(
            hold_result["summary"]["open_position_unrealized_net_pct"],
            0
        )

    def test_unfilled_approved_candidate_has_zero_fill_rate(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T17:00:00+00:00", 101.0),
        ]
        snapshots[1]["signal"]["payload"] = {}
        snapshots[1]["target_quality"]["payload"]["targets"] = []

        result = backtest.simulate_strategy("price_target_only", snapshots)

        summary = result["summary"]
        self.assertEqual(summary["approved_candidates"], 1)
        self.assertEqual(summary["not_filled"], 1)
        self.assertEqual(summary["trades"], 0)
        self.assertEqual(summary["fill_rate_after_approval"], 0.0)
        self.assertEqual(summary["terminal_rate_after_approval"], 1.0)

    def test_sentiment_discount_requires_deeper_entry(self):
        shallow = make_snapshot(
            "2026-05-30T12:00:00+00:00",
            101.0,
            action_recommendation="watch_only",
            target_prices=[{"buy_price": 100.9, "sell_pct": 0.5}]
        )
        deep = make_snapshot(
            "2026-05-30T12:30:00+00:00",
            101.0,
            action_recommendation="watch_only",
            target_prices=[{"buy_price": 100.0, "sell_pct": 0.5}]
        )
        fill = make_snapshot("2026-05-30T13:00:00+00:00", 100.0)
        exit_snapshot = make_snapshot("2026-05-30T13:30:00+00:00", 101.1)

        result = backtest.simulate_strategy(
            "sentiment_discount_with_quality",
            [shallow, deep, fill, exit_snapshot]
        )

        summary = result["summary"]
        self.assertEqual(summary["raw_candidates"], 2)
        self.assertEqual(summary["blocked_by_sentiment"], 1)
        self.assertEqual(summary["approved_candidates"], 1)
        self.assertEqual(summary["trades"], 1)

    def test_negative_best_trade_result_mentions_no_trade_outperformed(self):
        strategies = {
            "with_target_quality": {
                "summary": {
                    **backtest.empty_summary(),
                    "trades": 0,
                    "total_net_return_pct": None,
                }
            },
            "price_target_only": {
                "summary": {
                    **backtest.empty_summary(),
                    "trades": 1,
                    "win_rate": 0.0,
                    "total_net_return_pct": -0.5,
                }
            },
        }

        summary = backtest.top_summary(strategies)

        self.assertEqual(summary["best_strategy"], "price_target_only")
        self.assertIn("No-trade outperformed", summary["best_strategy_reason"])

    def test_negative_strategy_score_ranks_below_no_trade(self):
        losing_trade = {
            "trades": 1,
            "approved_candidates": 7,
            "raw_candidates": 7,
            "win_rate": 0.0,
            "total_net_return_pct": -1.15,
            "marked_to_market_net_return_pct": -1.15,
            "open_position_count": 0,
            "fill_rate_after_approval": 0.1429,
            "terminal_rate_after_approval": 0.8571,
        }
        no_trade = {
            "trades": 0,
            "approved_candidates": 0,
            "raw_candidates": 1441,
            "marked_to_market_net_return_pct": None,
            "open_position_count": 0,
        }

        self.assertLess(
            backtest.strategy_comparison_score(losing_trade),
            backtest.strategy_comparison_score(no_trade)
        )

    def test_unfilled_strategy_score_ranks_below_no_trade(self):
        unfilled = {
            "trades": 0,
            "approved_candidates": 1,
            "raw_candidates": 1200,
            "marked_to_market_net_return_pct": None,
            "open_position_count": 0,
            "fill_rate_after_approval": 0.0,
            "terminal_rate_after_approval": 1.0,
        }
        no_trade = {
            "trades": 0,
            "approved_candidates": 0,
            "raw_candidates": 1441,
            "marked_to_market_net_return_pct": None,
            "open_position_count": 0,
        }

        self.assertLess(
            backtest.strategy_comparison_score(unfilled),
            backtest.strategy_comparison_score(no_trade)
        )

    def test_production_eligibility_marks_probe_variants(self):
        eligible = backtest.production_eligibility(
            {
                "target_quality_enabled": True,
                "backtest_strategy_variant": "with_target_quality",
            },
            "with_target_quality"
        )
        price_probe = backtest.production_eligibility(
            {
                "probe_only": True,
                "target_quality_enabled": False,
            },
            "price_target_only_tp_0_8"
        )

        self.assertTrue(eligible["production_eligible"])
        self.assertIsNone(eligible["production_ineligible_reason"])
        self.assertFalse(price_probe["production_eligible"])
        self.assertEqual(
            price_probe["production_ineligible_reason"],
            "probe_only,price_target_only_variant,target_quality_disabled"
        )

    def test_strategy_comparison_includes_production_eligibility(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 101.1),
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            strategy_file = os.path.join(tmpdir, "probe.json")
            strategy_set_file = os.path.join(tmpdir, "strategies.txt")
            csv_file = os.path.join(tmpdir, "ranked.csv")
            with open(strategy_file, "w", encoding="utf-8") as f:
                json.dump({
                    "probe_only": True,
                    "target_quality_enabled": False,
                    "backtest_strategy_variant": "price_target_only_tp_0_8",
                }, f)
            with open(strategy_set_file, "w", encoding="utf-8") as f:
                f.write(f"probe,{strategy_file}\n")

            comparison = backtest.build_strategy_comparison_rows(
                snapshots,
                strategy_set_file
            )
            output_file = backtest.write_strategy_comparison_csv(
                comparison,
                csv_file,
                ranked=True
            )

            row = comparison["rows"][0]
            with open(output_file, encoding="utf-8") as f:
                header = f.readline().strip().split(",")

            self.assertFalse(row["production_eligible"])
            self.assertIn("probe_only", row["production_ineligible_reason"])
            self.assertIn("production_eligible", header)
            self.assertIn("production_ineligible_reason", header)

    def test_best_result_mentions_negative_open_exposure(self):
        strategies = {
            "target_quality_only_hold": {
                "summary": {
                    **backtest.empty_summary(),
                    "trades": 1,
                    "win_rate": 1.0,
                    "total_net_return_pct": 0.18,
                    "open_position_count": 1,
                    "open_position_unrealized_net_pct": -0.97,
                    "marked_to_market_net_return_pct": -0.79,
                }
            },
        }

        summary = backtest.top_summary(strategies)
        headline = summary["strategy_headlines"]["target_quality_only_hold"]

        self.assertEqual(summary["best_strategy"], "target_quality_only_hold")
        self.assertIn("Marked-to-market", summary["best_strategy_reason"])
        self.assertIn("Open exposure made no-trade better", summary["best_strategy_reason"])
        self.assertEqual(headline["marked_to_market_net_return_pct"], -0.79)

    def test_build_report_and_write_report(self):
        snapshots = [
            make_snapshot("2026-05-30T12:00:00+00:00", 101.0),
            make_snapshot("2026-05-30T12:30:00+00:00", 100.0),
            make_snapshot("2026-05-30T13:00:00+00:00", 101.1),
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
            self.assertIsNotNone(report["top_summary"]["best_strategy"])
            self.assertIn("strategy_headlines", report["top_summary"])
            self.assertIn("price_target_only_tp_0_8", report["strategies"])
            self.assertIn("price_target_only_tp_1_2", report["strategies"])
            self.assertIn("price_target_only_tp_1_2_hold", report["strategies"])
            self.assertIn("target_quality_only", report["strategies"])
            self.assertIn("target_quality_only_hold", report["strategies"])
            self.assertIn("sentiment_discount_with_quality", report["strategies"])
            self.assertIn(
                "sentiment_discount_with_quality_tp_1_5",
                report["strategies"]
            )
            self.assertIn(
                "sentiment_discount_with_quality_tp_1_5_hold",
                report["strategies"]
            )
            headline = report["top_summary"]["strategy_headlines"]["with_target_quality"]
            self.assertEqual(headline["not_filled"], 0)
            self.assertEqual(headline["terminal_rate_after_approval"], 1.0)
            self.assertEqual(report["simulation"]["fee_bps"], 32.0)
            self.assertEqual(len(report["snapshot_files"]), 1)
            self.assertIsNone(report["snapshot_diagnostics"]["empty_window_reason"])
            self.assertIn("written_outputs", report)

    def test_build_report_diagnoses_missing_snapshot_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            backtest.SNAPSHOT_LOG_FILE = os.path.join(tmpdir, "snapshots.jsonl")
            backtest.SNAPSHOT_ROTATE_DAILY = True
            backtest.BACKTEST_WINDOW_HOURS = 24

            report = backtest.build_report()

            diagnostics = report["snapshot_diagnostics"]
            self.assertEqual(report["snapshot_count"], 0)
            self.assertEqual(report["snapshot_files"], [])
            self.assertEqual(
                diagnostics["empty_window_reason"],
                "no snapshot files found"
            )
            self.assertGreaterEqual(len(diagnostics["expected_snapshot_files"]), 1)
            self.assertFalse(diagnostics["expected_file_metadata"][0]["exists"])

    def test_build_report_diagnoses_snapshots_outside_window(self):
        snapshots = [make_snapshot("2026-05-30T12:00:00+00:00", 101.0)]

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_file = os.path.join(tmpdir, "snapshots.jsonl")
            with open(snapshot_file, "w", encoding="utf-8") as f:
                for row in snapshots:
                    f.write(json.dumps(row) + "\n")

            backtest.SNAPSHOT_LOG_FILE = snapshot_file
            backtest.SNAPSHOT_ROTATE_DAILY = False
            backtest.BACKTEST_WINDOW_HOURS = 1

            report = backtest.build_report()

            diagnostics = report["snapshot_diagnostics"]
            self.assertEqual(report["snapshot_count"], 0)
            self.assertEqual(diagnostics["loaded_snapshot_count"], 1)
            self.assertEqual(diagnostics["filtered_out_by_window"], 1)
            self.assertEqual(
                diagnostics["empty_window_reason"],
                "all loaded snapshots were older than the report window"
            )

    def test_snapshot_source_files_prefers_rotated_daily_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = os.path.join(tmpdir, "snapshots.jsonl")
            may30 = os.path.join(tmpdir, "snapshots_20260530.jsonl")
            may31 = os.path.join(tmpdir, "snapshots_20260531.jsonl")
            june01 = os.path.join(tmpdir, "snapshots_20260601.jsonl")

            for path in (may30, may31, june01):
                with open(path, "w", encoding="utf-8") as f:
                    f.write("")

            backtest.SNAPSHOT_ROTATE_DAILY = True
            files = backtest.snapshot_source_files(
                base_path,
                backtest.parse_iso8601("2026-05-31T12:00:00+00:00"),
                backtest.parse_iso8601("2026-06-01T12:00:00+00:00"),
            )

            self.assertEqual(files, [os.path.abspath(may31), os.path.abspath(june01)])


if __name__ == "__main__":
    unittest.main()
