import json
import os
import tempfile
import unittest
from datetime import timedelta

import range_grid_dashboard as dashboard


class RangeGridDashboardTests(unittest.TestCase):
    def test_build_dashboard_renders_expected_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            now = dashboard.utc_now()
            status_ts = (now - timedelta(minutes=2)).isoformat()
            buy_ts = (now - timedelta(minutes=30)).isoformat()
            sell_ts = (now - timedelta(minutes=20)).isoformat()
            alert_ts = (now - timedelta(minutes=10)).isoformat()
            status_file = os.path.join(tmpdir, "status.json")
            state_file = os.path.join(tmpdir, "state.json")
            trade_log_file = os.path.join(tmpdir, "trade_log.jsonl")
            alert_log_file = os.path.join(tmpdir, "alerts.jsonl")
            output_file = os.path.join(tmpdir, "dashboard.html")

            with open(status_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "timestamp": status_ts,
                        "strategy_profile": "range_grid_strategy_recovery_range_only.json",
                        "operating_mode": "range_plus_llm",
                        "price": 76543.2,
                        "execution_signal": 0.061,
                        "action_recommendation": "bullish_allowed",
                        "signal_status": "fresh",
                        "runtime_block_reason": None,
                        "open_buy_count": 2,
                        "open_sell_count": 3,
                        "deployed_inventory_usd": 250.55,
                        "inventory_buckets_usd": {"llm_target": 120.0},
                        "strategy_modes": ["low", "high"],
                        "configured_strategy_modes": ["low", "high", "llm_target"],
                        "grid_anchor": "low,high",
                        "grid_levels": [76100.5, 75750.25, 75400.0],
                        "execution_quality": {
                            "approval_to_placement_rate": 0.8,
                            "placement_to_fill_rate": 0.5,
                            "fill_to_exit_rate": 0.75,
                        },
                        "range_fallback_active": False,
                        "realized_pnl_today": 1.25,
                        "sell_backlog_count": 3,
                        "sell_backlog_oldest_minutes": 42.0,
                        "weather_report_available": True,
                        "weather_condition": "breakout_tailwind",
                        "weather_alert_level": "watch",
                        "weather_trade_permission": "bot_decides",
                        "weather_bot_decision_authority": "bot",
                        "weather_emergency_bell": False,
                        "weather_opportunity_tags": ["breakout_tailwind"],
                        "weather_risk_warnings": ["source_health_degraded"],
                        "weather_position_size_multiplier": 0.32,
                        "weather_grid_aggression_multiplier": 0.68,
                        "weather_target_profit_multiplier": 1.08,
                        "weather_entry_discount_multiplier": 0.96,
                        "weather_leveling_state": "leveling",
                        "weather_leveling_score": 0.82,
                        "weather_market_range_zone": "middle_range",
                        "weather_market_range_position": 0.7402,
                        "weather_market_distance_to_recent_high_pct": 0.7853,
                        "weather_market_distance_from_recent_low_pct": 2.2885,
                        "weather_market_price_return_24h_pct": 1.5441,
                        "weather_market_price_return_4h_pct": -0.4585,
                    },
                    f,
                )

            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "stats": {
                            "approved_buy_candidates": 15,
                            "buy_orders_placed": 12,
                            "buy_orders_filled": 8,
                            "sell_orders_placed": 7,
                            "sell_orders_filled": 5,
                            "buy_order_rejections": 2,
                            "realized_gross_pnl": 1.75,
                            "realized_estimated_net_pnl": 1.22,
                        }
                    },
                    f,
                )

            with open(trade_log_file, "w", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": buy_ts,
                    "event": "TRADE_DECISION",
                    "side": "buy",
                    "buy_source": "range_high_band",
                    "message": ""
                }) + "\n")
                f.write(json.dumps({
                    "ts": buy_ts,
                    "event": "BUY_ORDER_PLACED",
                    "buy_source": "range_high_band",
                    "message": "BUY placed @ 76000"
                }) + "\n")
                f.write(json.dumps({
                    "ts": buy_ts,
                    "event": "BUY_ORDER_FILLED",
                    "buy_source": "range_high_band",
                    "message": "BUY filled @ 76000"
                }) + "\n")
                f.write(json.dumps({
                    "ts": sell_ts,
                    "event": "SELL_ORDER_FILLED",
                    "buy_source": "range_high_band",
                    "estimated_net_pnl": 0.42,
                    "message": "SELL filled"
                }) + "\n")

            with open(alert_log_file, "w", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": alert_ts,
                    "event": "ALERT",
                    "severity": "warning",
                    "alert_type": "order_tracker_error",
                    "message": "Order tracker update failed"
                }) + "\n")

            original_status = dashboard.STATUS_FILE
            original_state = dashboard.STATE_FILE
            original_trade = dashboard.TRADE_LOG_FILE
            original_alert = dashboard.ALERT_LOG_FILE
            original_lookback = dashboard.LOOKBACK_HOURS
            try:
                dashboard.STATUS_FILE = status_file
                dashboard.STATE_FILE = state_file
                dashboard.TRADE_LOG_FILE = trade_log_file
                dashboard.ALERT_LOG_FILE = alert_log_file
                dashboard.LOOKBACK_HOURS = 48
                result = dashboard.build_dashboard(output_file)
            finally:
                dashboard.STATUS_FILE = original_status
                dashboard.STATE_FILE = original_state
                dashboard.TRADE_LOG_FILE = original_trade
                dashboard.ALERT_LOG_FILE = original_alert
                dashboard.LOOKBACK_HOURS = original_lookback

            self.assertEqual(result["output_file"], os.path.abspath(output_file))
            self.assertTrue(os.path.exists(output_file))
            with open(output_file, encoding="utf-8") as f:
                html_text = f.read()
            self.assertIn("Range Grid Bot", html_text)
            self.assertIn("bullish_allowed", html_text)
            self.assertIn("76,543.20", html_text)
            self.assertIn("range_grid_strategy_recovery_range_only.json", html_text)
            self.assertIn("Order tracker update failed", html_text)
            self.assertIn("250.55", html_text)
            self.assertIn("Current Grid Levels", html_text)
            self.assertIn("Market Weather", html_text)
            self.assertIn("breakout_tailwind", html_text)
            self.assertIn("source_health_degraded", html_text)
            self.assertIn("middle_range", html_text)
            self.assertIn("0.8200", html_text)
            self.assertIn("Execution Quality", html_text)
            self.assertIn("Recent Approved Candidates", html_text)
            self.assertIn("Lifetime Approved Candidates", html_text)
            self.assertIn("80.0%", html_text)
            self.assertIn("76,100.50", html_text)
            self.assertIn("75,750.25", html_text)

    def test_build_dashboard_handles_missing_inputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = os.path.join(tmpdir, "dashboard.html")
            original_status = dashboard.STATUS_FILE
            original_state = dashboard.STATE_FILE
            original_trade = dashboard.TRADE_LOG_FILE
            original_alert = dashboard.ALERT_LOG_FILE
            try:
                dashboard.STATUS_FILE = os.path.join(tmpdir, "missing-status.json")
                dashboard.STATE_FILE = os.path.join(tmpdir, "missing-state.json")
                dashboard.TRADE_LOG_FILE = os.path.join(tmpdir, "missing-trade.jsonl")
                dashboard.ALERT_LOG_FILE = os.path.join(tmpdir, "missing-alerts.jsonl")
                result = dashboard.build_dashboard(output_file)
            finally:
                dashboard.STATUS_FILE = original_status
                dashboard.STATE_FILE = original_state
                dashboard.TRADE_LOG_FILE = original_trade
                dashboard.ALERT_LOG_FILE = original_alert
            self.assertEqual(result["output_file"], os.path.abspath(output_file))
            self.assertTrue(os.path.exists(output_file))
            with open(output_file, encoding="utf-8") as f:
                html_text = f.read()
            self.assertIn("No status snapshot found", html_text)


if __name__ == "__main__":
    unittest.main()
