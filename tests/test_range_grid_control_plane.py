import os
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
                        "simulation_net_return_pct": 0.8,
                    },
                ],
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
        self.assertEqual(snapshot["missed"]["placement_rate_vs_approved"], 0.625)
        self.assertEqual(snapshot["strategies"][0]["strategy_label"], "winner")
        self.assertEqual(snapshot["strategies"][1]["strategy_label"], "runner_up")
        self.assertEqual(snapshot["watchlist"]["items"][0]["code"], "placement_gap")
        self.assertEqual(cached_snapshot["strategy_count"], 2)
        self.assertEqual(session.calls, 1)

    def test_backtest_snapshot_is_unavailable_without_source(self):
        snapshot = control_plane.BacktestData(source="").snapshot()

        self.assertFalse(snapshot["available"])
        self.assertTrue(snapshot["stale"])
        self.assertIn("not configured", snapshot["error"])

    def test_control_page_has_interactive_dated_chart_tooltip(self):
        html = control_plane.HTML_FILE.read_text(encoding="utf-8")

        self.assertIn('id="chartTooltip"', html)
        self.assertIn("pointermove", html)
        self.assertIn("candle.close", html)
        self.assertIn("timeZone:'UTC'", html)
        self.assertIn('id="floorEnabled"', html)
        self.assertIn('id="ceilingEnabled"', html)
        self.assertIn("function renderPermissions", html)
        self.assertIn("Don’t buy below", html)
        self.assertIn("Don’t buy above", html)
        self.assertIn("strategy still chooses levels", html)
        self.assertNotIn("Manual target mode", html)
        self.assertIn('data-tab="trading"', html)
        self.assertIn('data-tab="weather"', html)
        self.assertIn('data-tab="backtest"', html)
        self.assertIn('id="weatherView"', html)
        self.assertIn('id="backtestView"', html)
        self.assertIn('id="weatherCondition"', html)
        self.assertIn("function renderSentiment", html)
        self.assertIn("function renderBacktest", html)
        self.assertIn("request('/api/backtest')", html)
        self.assertIn("Ranked strategies", html)
        self.assertIn("Suggested bot tuning", html)


if __name__ == "__main__":
    unittest.main()
