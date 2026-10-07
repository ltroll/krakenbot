import os
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import range_grid_control_plane as control_plane


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, ticker, hourly, daily):
        self.responses = [ticker, hourly, daily]

    def get(self, url, params=None, timeout=None):
        return FakeResponse(self.responses.pop(0))


class FakeSignalSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        return FakeResponse(self.payload)


def ohlc_payload(rows):
    return {"error": [], "result": {"XXBTZUSD": rows, "last": 0}}


class RangeGridControlPlaneTests(unittest.TestCase):
    def test_host_loopback_detection(self):
        self.assertTrue(control_plane.host_is_loopback("127.0.0.1"))
        self.assertTrue(control_plane.host_is_loopback("::1"))
        self.assertTrue(control_plane.host_is_loopback("localhost"))
        self.assertFalse(control_plane.host_is_loopback("0.0.0.0"))
        self.assertFalse(control_plane.host_is_loopback("192.168.1.20"))

    def test_ohlc_statistics_use_close_average_and_low_high(self):
        now = datetime(2026, 9, 19, 18, tzinfo=timezone.utc)
        rows = []
        for offset, close in enumerate((100.0, 110.0, 120.0)):
            timestamp = int((now - timedelta(hours=2 - offset)).timestamp())
            rows.append([timestamp, close - 1, close + 2, close - 3, close])

        candles = control_plane.parse_ohlc_candles(ohlc_payload(rows))
        summary = control_plane.summarize_candles(
            candles,
            now - timedelta(hours=3),
        )

        self.assertEqual(summary["average"], 110.0)
        self.assertEqual(summary["low"], 97.0)
        self.assertEqual(summary["high"], 122.0)
        self.assertEqual(summary["samples"], 3)

    def test_market_snapshot_builds_requested_periods(self):
        now = datetime(2026, 9, 19, 18, tzinfo=timezone.utc)
        ticker = {
            "error": [],
            "result": {"XXBTZUSD": {"c": ["81234.5", "1"]}},
        }
        hourly_rows = [
            [
                int((now - timedelta(hours=offset)).timestamp()),
                "80000", "82000", "79000", str(81000 + offset),
            ]
            for offset in range(24, -1, -1)
        ]
        daily_rows = [
            [
                int((now - timedelta(days=offset)).timestamp()),
                "70000", "83000", "68000", str(75000 + offset),
            ]
            for offset in range(210, -1, -1)
        ]
        market = control_plane.KrakenMarketData(
            session=FakeSession(
                ticker,
                ohlc_payload(hourly_rows),
                ohlc_payload(daily_rows),
            ),
            cache_seconds=60,
        )

        with patch.object(control_plane, "utc_now", return_value=now):
            snapshot = market.snapshot()

        self.assertEqual(snapshot["current_price"], 81234.5)
        self.assertEqual(set(snapshot["periods"]), {"24h", "7d", "50d", "200d"})
        self.assertFalse(snapshot["stale"])
        self.assertLessEqual(len(snapshot["history"]["daily"]), 200)

    def test_sentiment_snapshot_selects_asset_and_derives_weather(self):
        now = datetime(2026, 9, 19, 18, tzinfo=timezone.utc)
        payload = {
            "schema_version": "multi-asset-sentiment-v1",
            "processed_at": (now - timedelta(minutes=5)).isoformat(),
            "freshness": {
                "fresh_for_minutes": 10,
                "warn_after_minutes": 15,
                "stale_after_minutes": 30,
            },
            "assets": {
                "BTC": {
                    "asset_id": "BTC",
                    "asset_price": 80440,
                    "asset_sentiment": -0.12,
                    "execution_signal": 0.24,
                    "confidence": 0.72,
                    "signal_status": "fresh",
                    "bot_action_allowed": True,
                    "action_recommendation": "cautious_accumulation",
                    "action_policy": {"reason": "price is near support"},
                    "risk_context": {
                        "market_risk_score": 0.42,
                        "buy_aggression_score": 0.61,
                        "downside_risk_score": 0.38,
                        "inputs": {
                            "mean_reversion_opportunity": 0.0961,
                        },
                        "weather_report": {
                            "condition": "leveling_after_drop",
                            "alert_level": "watch",
                            "trade_permission": "bot_decides",
                            "bot_decision_authority": "bot",
                            "market_location": {
                                "current_price": 80440,
                                "range_position": 0.31,
                                "support_bands": [{
                                    "price": 80155.0,
                                    "type": "recent_low",
                                }],
                                "resistance_bands": [{
                                    "price": 80872.31034483,
                                    "type": "range_mean",
                                }],
                                "nearest_support": {
                                    "price": 80155.0,
                                    "type": "recent_low",
                                },
                                "nearest_resistance": {
                                    "price": 80872.31034483,
                                    "type": "range_mean",
                                },
                                "distance_to_nearest_support_pct": 0.4545,
                                "room_to_nearest_resistance_pct": 0.4363,
                            },
                            "market_opportunity": {
                                "cycle_phase": "early_rebound",
                                "entry_opportunity_score": 0.67,
                                "bot_hint": "accumulate in measured steps",
                            },
                            "bot_tuning": {
                                "position_size_multiplier": 0.8,
                                "target_profit_multiplier": 1.2,
                            },
                        },
                    },
                    "contributors": [
                        {
                            "source_type": "fear_greed",
                            "source_id": "fear_greed",
                            "observed_at": (
                                now - timedelta(minutes=5)
                            ).isoformat(),
                            "score": {
                                "btc_sentiment": 0.21,
                                "confidence": 0.77,
                            },
                        },
                        {
                            "source_type": "kraken_flow",
                            "source_id": "btc_kraken_flow",
                            "score": {"flow_pressure": -0.4087},
                        },
                    ],
                },
                "ETH": {"asset_id": "ETH", "execution_signal": -0.5},
            },
        }
        session = FakeSignalSession(payload)
        sentiment = control_plane.SentimentData(
            session=session,
            url="http://signal.test/multi_asset_signal.json",
            asset_id="BTC",
            cache_seconds=60,
        )

        with patch.object(control_plane, "utc_now", return_value=now):
            snapshot = sentiment.snapshot()
            cached_snapshot = sentiment.snapshot()

        self.assertTrue(snapshot["available"])
        self.assertFalse(snapshot["stale"])
        self.assertEqual(snapshot["freshness_state"], "fresh")
        self.assertEqual(snapshot["age_minutes"], 5.0)
        self.assertEqual(snapshot["signal"]["asset_id"], "BTC")
        self.assertEqual(snapshot["signal"]["fear_greed_index"], 71.0)
        self.assertTrue(snapshot["signal"]["fear_greed_index_inferred"])
        self.assertEqual(snapshot["signal"]["fear_greed_sentiment"], 0.21)
        self.assertEqual(snapshot["signal"]["flow_pressure"], -0.4087)
        self.assertEqual(
            snapshot["signal"]["mean_reversion_opportunity"],
            0.0961,
        )
        self.assertEqual(
            snapshot["risk"]["weather_condition"],
            "leveling_after_drop",
        )
        self.assertEqual(
            snapshot["risk"]["weather_entry_opportunity_score"],
            0.67,
        )
        self.assertEqual(
            snapshot["risk"]["suggested_take_profit_multiplier"],
            1.2,
        )
        self.assertEqual(
            snapshot["market_structure"]["support_price"],
            80155.0,
        )
        self.assertEqual(
            snapshot["market_structure"]["resistance_price"],
            80872.31034483,
        )
        self.assertEqual(
            snapshot["market_structure"]["support_bands"][0]["type"],
            "recent_low",
        )
        self.assertEqual(cached_snapshot["signal"]["asset_price"], 80440)
        self.assertEqual(session.calls, 1)

    def test_sentiment_snapshot_is_unavailable_without_url(self):
        snapshot = control_plane.SentimentData(url="").snapshot()

        self.assertFalse(snapshot["available"])
        self.assertTrue(snapshot["stale"])
        self.assertIn("LLM_SIGNAL_URL", snapshot["error"])

    def test_backtest_source_can_follow_anchor_router_location(self):
        with patch.dict(os.environ, {
            "RANGE_GRID_CONTROL_BACKTEST_URL": "",
            "RANGE_GRID_BACKTEST_URL": "",
            "RANGE_GRID_BACKTEST_OUTPUT_FILE": "",
            "RANGE_GRID_ANCHOR_ROUTER_FILE": (
                "http://backtest.test/bot/range_grid_anchor_winners.json"
            ),
        }):
            source = control_plane.configured_backtest_source()

        self.assertEqual(
            source,
            "http://backtest.test/bot/range_grid_backtest.json",
        )

    def test_backtest_snapshot_ranks_and_summarizes_latest_report(self):
        report = {
            "timestamp": "2026-09-19T18:00:00+00:00",
            "since": "2026-09-12T18:00:00+00:00",
            "snapshot_count": 168,
            "trade_event_count": 42,
            "replay": {
                "summary": {
                    "raw_candidates": 90,
                    "approved_candidates": 8,
                    "blocked_reason_counts": {"price_above_level": 22},
                    "level_stability_shadow": {
                        "enabled": True,
                        "shadow_only": True,
                        "raw_anchor_changes": 7,
                        "stable_anchor_changes": 2,
                        "approved_candidate_allowed": 6,
                        "approved_candidate_blocked": 2,
                    },
                },
            },
            "actual_live": {
                "buy_orders_placed": 5,
                "buy_orders_filled": 4,
                "sell_orders_filled": 3,
                "realized_estimated_net_pnl": 12.34,
            },
            "missed_opportunities": {
                "approved_but_not_placed": 3,
                "placement_rate_vs_approved": 0.625,
            },
            "watchlist": {
                "status": "attention",
                "items": [{
                    "severity": "warning",
                    "code": "placement_gap",
                    "message": "Three approved entries were not placed.",
                }],
            },
            "strategy_comparison": {
                "rows": [
                    {
                        "strategy_label": "runner_up",
                        "practical_score": 0.4,
                        "approved_candidates": 3,
                        "potential_avg_end_return_pct": 0.2,
                    },
                    {
                        "strategy_label": "winner",
                        "practical_score": 1.2,
                        "approved_candidates": 5,
                        "simulation_starting_cash_usd": 7000.0,
                        "simulation_starting_cash_source": (
                            "environment_override"
                        ),
                        "simulation_net_return_pct": 0.8,
                    },
                ],
                "entry_price_performance": [{
                    "strategy_label": "winner",
                    "strategy_file": "/tmp/winner.json",
                    "performance": {
                        "basis": "simulated_fills_including_open_mark_to_market",
                        "ranking_metric": "net_return_on_entry_notional_pct",
                        "target_bucket_pct": 0.005,
                        "bucket_size": 500.0,
                        "last_price": 81_000.0,
                        "filled_entries": 3,
                        "closed_positions": 2,
                        "open_positions": 1,
                        "best_band": {
                            "rank": 1,
                            "price_band_low": 80_000.0,
                            "price_band_high": 80_500.0,
                            "filled_entries": 2,
                            "net_return_on_entry_notional_pct": 2.0,
                        },
                        "bands": [{
                            "rank": 1,
                            "price_band_low": 80_000.0,
                            "price_band_high": 80_500.0,
                            "average_entry_price": 80_250.0,
                            "filled_entries": 2,
                            "closed_positions": 2,
                            "open_positions": 0,
                            "close_rate": 1.0,
                            "entry_notional_usd": 200.0,
                            "total_net_pnl_usd": 4.0,
                            "net_return_on_entry_notional_pct": 2.0,
                            "sources": {"range_low": 2},
                        }],
                    },
                }],
            },
        }
        session = FakeSignalSession(report)
        backtest = control_plane.BacktestData(
            session=session,
            source="http://backtest.test/bot/range_grid_backtest.json",
            cache_seconds=300,
        )

        snapshot = backtest.snapshot()
        cached_snapshot = backtest.snapshot()

        self.assertTrue(snapshot["available"])
        self.assertFalse(snapshot["stale"])
        self.assertEqual(snapshot["window_hours"], 168.0)
        self.assertEqual(snapshot["snapshot_count"], 168)
        self.assertEqual(snapshot["actual"]["buy_orders_filled"], 4)
        self.assertEqual(
            snapshot["level_stability_shadow"]["raw_anchor_changes"],
            7,
        )
        self.assertEqual(
            snapshot["replay"]["level_stability_shadow"][
                "approved_candidate_blocked"
            ],
            2,
        )
        self.assertEqual(snapshot["missed"]["placement_rate_vs_approved"], 0.625)
        self.assertEqual(snapshot["strategies"][0]["strategy_label"], "winner")
        self.assertEqual(
            snapshot["strategies"][0]["simulation_starting_cash_usd"],
            7000.0,
        )
        self.assertEqual(
            snapshot["strategies"][0]["simulation_starting_cash_source"],
            "environment_override",
        )
        self.assertEqual(snapshot["strategies"][1]["strategy_label"], "runner_up")
        self.assertEqual(
            snapshot["entry_price_performance"]["strategy_label"],
            "winner",
        )
        self.assertEqual(
            snapshot["entry_price_performance"]["best_band"][
                "price_band_low"
            ],
            80_000.0,
        )
        self.assertEqual(snapshot["watchlist"]["items"][0]["code"], "placement_gap")
        self.assertEqual(cached_snapshot["strategy_count"], 2)
        self.assertEqual(session.calls, 1)

    def test_backtest_snapshot_is_unavailable_without_source(self):
        snapshot = control_plane.BacktestData(source="").snapshot()

        self.assertFalse(snapshot["available"])
        self.assertTrue(snapshot["stale"])
        self.assertIn("not configured", snapshot["error"])

    def test_decision_history_normalizes_relevant_trade_log_events(self):
        records = [
            {
                "ts": "2026-10-04T12:00:00+00:00",
                "event": "BOT_START",
                "strategy_profile": "ignored.json",
            },
            {
                "ts": "2026-10-04T12:01:00+00:00",
                "event": "BUY_CANDIDATE_SKIPPED",
                "reason": "price_above_level",
                "level": 80100,
                "market_price": 80200,
                "buy_source": "range_low",
                "txid": "must-not-leak",
            },
            {
                "ts": "2026-10-04T12:02:00+00:00",
                "event": "TRADE_DECISION",
                "side": "buy",
                "price": 80000,
                "execution_signal": 0.42,
                "strategy_modes": ["low", "median"],
            },
            {
                "ts": "2026-10-04T12:03:00+00:00",
                "event": "BUY_ORDER_PLACED",
                "price": 80000,
                "volume": 0.00125,
                "buy_source": "range_low",
                "client_order_id": "must-not-leak-either",
            },
            {
                "ts": "2026-10-04T12:04:00+00:00",
                "event": "LOOP_ERROR",
                "message": "temporary failure",
            },
        ]
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".jsonl",
        ) as handle:
            for record in records:
                handle.write(json.dumps(record) + "\n")
            handle.write("not-json\n")
            handle.flush()

            history = control_plane.build_decision_history(
                handle.name,
                limit=4,
            )

        self.assertTrue(history["available"])
        self.assertEqual(history["returned_count"], 4)
        self.assertEqual(history["events"][0]["event"], "LOOP_ERROR")
        self.assertEqual(history["events"][1]["event"], "BUY_ORDER_PLACED")
        self.assertEqual(history["counts"], {
            "trade": 1,
            "decision": 1,
            "no_trade": 1,
            "error": 1,
        })
        self.assertEqual(history["malformed_lines"], 1)
        self.assertEqual(
            history["events"][2]["reason"],
            "candidate_passed_buy_checks",
        )
        serialized = json.dumps(history)
        self.assertNotIn("must-not-leak", serialized)
        self.assertNotIn("client_order_id", serialized)

    def test_decision_history_limit_is_validated_and_capped(self):
        self.assertEqual(control_plane.parse_decision_history_limit(None), 100)
        self.assertEqual(control_plane.parse_decision_history_limit(["25"]), 25)
        self.assertEqual(control_plane.parse_decision_history_limit("5000"), 500)
        with self.assertRaisesRegex(ValueError, "at least 1"):
            control_plane.parse_decision_history_limit("0")
        with self.assertRaisesRegex(ValueError, "integer"):
            control_plane.parse_decision_history_limit("many")

    def test_decision_history_continues_into_rotated_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            active_path = os.path.join(directory, "trade_log.jsonl")
            rotated_path = os.path.join(
                directory,
                "trade_log_20261004T120000Z.jsonl",
            )
            with open(rotated_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "ts": "2026-10-04T11:59:00+00:00",
                    "event": "TRADE_DECISION",
                    "side": "hold",
                    "reason": "price_above_level",
                }) + "\n")
            with open(active_path, "w", encoding="utf-8") as handle:
                handle.write(json.dumps({
                    "ts": "2026-10-04T12:01:00+00:00",
                    "event": "BUY_ORDER_PLACED",
                    "price": 80000,
                }) + "\n")

            history = control_plane.build_decision_history(
                active_path,
                limit=2,
            )

        self.assertEqual(history["returned_count"], 2)
        self.assertEqual(history["files_scanned"], 2)
        self.assertEqual(
            [event["event"] for event in history["events"]],
            ["BUY_ORDER_PLACED", "TRADE_DECISION"],
        )

    def test_decision_history_reports_missing_trade_log(self):
        history = control_plane.build_decision_history(
            "/path/that/does/not/exist/trade_log.jsonl",
            limit=25,
        )

        self.assertFalse(history["available"])
        self.assertEqual(history["events"], [])
        self.assertIn("not available", history["error"])

    def test_backtest_snapshot_rejects_report_for_another_asset(self):
        report = {
            "asset_id": "BTC",
            "timestamp": "2026-09-19T18:00:00+00:00",
            "since": "2026-09-18T18:00:00+00:00",
        }

        with patch.object(control_plane, "SIGNAL_ASSET_ID", "SOL"):
            snapshot = control_plane.build_backtest_snapshot(
                report,
                source="test.json",
                captured_at="2026-09-19T18:01:00+00:00",
            )

        self.assertFalse(snapshot["available"])
        self.assertIn("does not match", snapshot["error"])

    def test_strategy_control_lists_profiles_and_reports_pending_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            filename = "range_grid_strategy_selected.json"
            with open(
                os.path.join(directory, filename),
                "w",
                encoding="utf-8",
            ) as handle:
                json.dump({
                    "operating_mode": "range_only",
                    "paper_trading_enabled": False,
                    "grid_anchor": "low,median",
                    "range_window_hours": 24,
                    "max_grid_size": 4,
                    "profit_target_pct": 0.012,
                }, handle)
            with patch.object(
                control_plane,
                "CONFIGURED_STRATEGY_PROFILE",
                "range_grid_strategy_default.json",
            ):
                snapshot = control_plane.build_strategy_control_snapshot(
                    {"strategy_profile_override": filename},
                    {"strategy_profile": "range_grid_strategy_default.json"},
                    directory,
                )

        self.assertEqual(snapshot["selected_override"], filename)
        self.assertEqual(snapshot["desired_profile"], filename)
        self.assertTrue(snapshot["restart_pending"])
        self.assertEqual(snapshot["desired"]["grid_anchor"], "low,median")
        self.assertEqual(snapshot["options"][0]["filename"], filename)

    def test_strategy_control_treats_absolute_active_profile_as_same_file(self):
        filename = "range_grid_strategy_default.json"
        with patch.object(
            control_plane,
            "CONFIGURED_STRATEGY_PROFILE",
            filename,
        ):
            snapshot = control_plane.build_strategy_control_snapshot(
                {},
                {"strategy_profile": f"/opt/krakenbot/{filename}"},
                tempfile.gettempdir(),
            )

        self.assertFalse(snapshot["restart_pending"])

    def test_status_payload_reports_operator_cycle_usage(self):
        class MarketData:
            def snapshot(self):
                return {"current_price": 80000, "stale": False}

        with tempfile.TemporaryDirectory() as directory:
            control_path = os.path.join(directory, "control.json")
            state_path = os.path.join(directory, "state.json")
            status_path = os.path.join(directory, "status.json")
            today = datetime.now(timezone.utc).date().isoformat()
            control_plane.save_control_state(control_path, {
                "daily_round_trip_limit_enabled": True,
                "daily_round_trip_limit": 3,
                "order_size_override_enabled": True,
                "order_size_usd": 125,
            })
            with open(state_path, "w", encoding="utf-8") as handle:
                json.dump({
                    "open_buy_orders": {
                        "one": {"operator_round_trip_day": today},
                    },
                    "open_sell_orders": {
                        "two": {"operator_round_trip_day": today},
                    },
                }, handle)
            with open(status_path, "w", encoding="utf-8") as handle:
                json.dump({}, handle)

            with (
                patch.object(control_plane, "CONTROL_FILE", control_path),
                patch.object(control_plane, "STATE_FILE", state_path),
                patch.object(control_plane, "STATUS_FILE", status_path),
                patch.object(control_plane, "STRATEGY_DIRECTORY", directory),
            ):
                payload = control_plane.build_status_payload(MarketData())

        limits = payload["operator_limits"]
        self.assertTrue(limits["order_size_override_enabled"])
        self.assertEqual(limits["order_size_usd"], 125.0)
        self.assertEqual(limits["round_trips"]["active_count"], 2)
        self.assertEqual(limits["round_trips"]["remaining"], 1)

    def test_control_page_has_interactive_dated_chart_tooltip(self):
        html = control_plane.HTML_FILE.read_text(encoding="utf-8")

        self.assertIn('id="chartTooltip"', html)
        self.assertIn("pointermove", html)
        self.assertIn("candle.close", html)
        self.assertIn("timeZone:'UTC'", html)
        self.assertIn('id="floorEnabled"', html)
        self.assertIn('id="ceilingEnabled"', html)
        self.assertIn('id="strategySelect"', html)
        self.assertIn('id="applyStrategyButton"', html)
        self.assertIn('id="profitTargetEnabled"', html)
        self.assertIn('id="profitTargetRange"', html)
        self.assertIn('id="profitTargetNumber"', html)
        self.assertIn('id="profitCalculatorBuyPrice"', html)
        self.assertIn('id="profitCalculatorSellPrice"', html)
        self.assertIn('id="saveProfitTargetButton"', html)
        self.assertIn('id="orderSizeEnabled"', html)
        self.assertIn('id="orderSizeRange"', html)
        self.assertIn('id="orderSizeNumber"', html)
        self.assertIn('id="dailyRoundTripLimitEnabled"', html)
        self.assertIn('id="dailyRoundTripLimitRange"', html)
        self.assertIn('id="dailyRoundTripLimitNumber"', html)
        self.assertIn('id="dailyRoundTripActive"', html)
        self.assertIn('id="dailyRoundTripRemaining"', html)
        self.assertIn('id="saveExecutionControlsButton"', html)
        self.assertIn("function updateProfitTargetCalculator", html)
        self.assertIn("function renderExecutionControls", html)
        self.assertIn("profit_target_override_enabled", html)
        self.assertIn("net_profit_target_pct", html)
        self.assertIn("order_size_override_enabled", html)
        self.assertIn("daily_round_trip_limit_enabled", html)
        self.assertIn("0% means estimated break-even", html)
        self.assertIn("Old inventory does not block today", html)
        self.assertIn("function renderPermissions", html)
        self.assertIn("Don’t buy below", html)
        self.assertIn("Don’t buy above", html)
        self.assertIn("strategy still chooses levels", html)
        self.assertNotIn("Manual target mode", html)
        self.assertIn('data-tab="trading"', html)
        self.assertIn('data-tab="weather"', html)
        self.assertIn('data-tab="backtest"', html)
        self.assertIn('data-tab="decisions"', html)
        self.assertIn('id="weatherView"', html)
        self.assertIn('id="backtestView"', html)
        self.assertIn('id="decisionsView"', html)
        self.assertIn('id="decisionLimit"', html)
        self.assertIn('id="decisionOutcomeFilter"', html)
        self.assertIn('id="decisionHistoryTable"', html)
        self.assertIn('id="assetPairLabel"', html)
        self.assertIn('id="chartAssetLabel"', html)
        self.assertNotIn("Engine BTC price", html)
        self.assertIn('id="weatherCondition"', html)
        self.assertIn('id="supportLevels"', html)
        self.assertIn('id="resistanceLevels"', html)
        self.assertIn('id="levelStabilityStatus"', html)
        self.assertIn('id="levelStabilityFacts"', html)
        self.assertIn("effective_shadow_buy_ceiling", html)
        self.assertIn("automatic_buy_ceiling_reason", html)
        self.assertIn("stableSupport.status", html)
        self.assertIn("stableResistance.status", html)
        self.assertIn("support_invalidations", html)
        self.assertIn("automatic_buy_ceiling_fail_open_snapshots", html)
        self.assertIn("max_abs_raw_stable_divergence_pct", html)
        self.assertIn("Shadow mode does not alter orders", html)
        self.assertIn("function renderSentiment", html)
        self.assertIn("function renderStructureCard", html)
        self.assertNotIn("['Support / resistance'", html)
        self.assertIn("function renderBacktest", html)
        self.assertIn('id="backtestEntryPriceTable"', html)
        self.assertIn("Best simulated entry ranges", html)
        self.assertIn("entry_price_performance", html)
        self.assertIn("simulation_starting_cash_usd", html)
        self.assertIn("simulation_starting_cash_source", html)
        self.assertIn("marked to", html)
        self.assertIn("request('/api/backtest')", html)
        self.assertIn("/api/decisions?limit=", html)
        self.assertIn("function renderDecisionHistory", html)
        self.assertIn("request('/api/strategy',", html)
        self.assertIn("Ranked strategies", html)
        self.assertIn("Suggested bot tuning", html)


if __name__ == "__main__":
    unittest.main()
