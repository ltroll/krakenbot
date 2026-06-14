import json
import os
import tempfile
import unittest

import range_grid_dashboard as dashboard


class RangeGridDashboardTests(unittest.TestCase):
    def test_build_dashboard_renders_expected_content(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            status_file = os.path.join(tmpdir, "status.json")
            state_file = os.path.join(tmpdir, "state.json")
            trade_log_file = os.path.join(tmpdir, "trade_log.jsonl")
            alert_log_file = os.path.join(tmpdir, "alerts.jsonl")
            output_file = os.path.join(tmpdir, "dashboard.html")

            with open(status_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "timestamp": "2026-06-13T12:00:00+00:00",
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
                        "range_fallback_active": False,
                        "realized_pnl_today": 1.25,
                        "sell_backlog_count": 3,
                        "sell_backlog_oldest_minutes": 42.0,
                    },
                    f,
                )

            with open(state_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "stats": {
                            "buy_orders_placed": 12,
                            "buy_orders_filled": 8,
                            "sell_orders_placed": 7,
                            "sell_orders_filled": 5,
                            "realized_gross_pnl": 1.75,
                            "realized_estimated_net_pnl": 1.22,
                        }
                    },
                    f,
                )

            with open(trade_log_file, "w", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": "2026-06-13T11:30:00+00:00",
                    "event": "BUY_ORDER_PLACED",
                    "message": "BUY placed @ 76000"
                }) + "\n")
                f.write(json.dumps({
                    "ts": "2026-06-13T11:40:00+00:00",
                    "event": "SELL_ORDER_FILLED",
                    "estimated_net_pnl": 0.42,
                    "message": "SELL filled"
                }) + "\n")

            with open(alert_log_file, "w", encoding="utf-8") as f:
                f.write(json.dumps({
                    "ts": "2026-06-13T11:50:00+00:00",
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
            self.assertIn("Order tracker update failed", html_text)
            self.assertIn("250.55", html_text)

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
