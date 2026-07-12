from datetime import datetime, timedelta, timezone
import unittest

import range_grid_guardrails as guardrails


class RangeGridGuardrailsTests(unittest.TestCase):
    def test_validate_strategy_config_rejects_invalid_operating_mode(self):
        errors = guardrails.validate_strategy_config({
            "grid_anchor": "low,high",
            "operating_mode": "banana",
        })
        self.assertTrue(any("operating_mode" in error for error in errors))

    def test_validate_strategy_config_rejects_bad_bucket_cap(self):
        errors = guardrails.validate_strategy_config({
            "grid_anchor": "low,high",
            "operating_mode": "range_plus_llm",
            "max_inventory_usd_by_bucket": {"llm_target": -1},
        })
        self.assertTrue(any("max_inventory_usd_by_bucket.llm_target" in error for error in errors))

    def test_validate_strategy_config_rejects_bad_sell_policy_source_map(self):
        errors = guardrails.validate_strategy_config({
            "grid_anchor": "low,high",
            "operating_mode": "range_plus_llm",
            "aging_start_minutes_by_source": {"banana": 30},
        })
        self.assertTrue(any("aging_start_minutes_by_source.banana" in error for error in errors))

    def test_validate_strategy_config_allows_negative_execution_threshold(self):
        errors = guardrails.validate_strategy_config({
            "grid_anchor": "low,high",
            "operating_mode": "range_only",
            "execution_signal_threshold": -0.08,
        })
        self.assertFalse(errors)

    def test_validate_strategy_config_rejects_out_of_bounds_execution_threshold(self):
        errors = guardrails.validate_strategy_config({
            "grid_anchor": "low,high",
            "operating_mode": "range_only",
            "execution_signal_threshold": -1.5,
        })
        self.assertTrue(any("execution_signal_threshold" in error for error in errors))

    def test_validate_strategy_config_rejects_out_of_bounds_risk_context_threshold(self):
        errors = guardrails.validate_strategy_config({
            "grid_anchor": "low,high",
            "operating_mode": "range_only",
            "risk_context_high_band_min_breakout_score": 1.5,
            "risk_context_position_size_blend": -0.1,
            "high_anchor_backlog_old_order_weight": 1.5,
        })
        self.assertTrue(
            any("risk_context_high_band_min_breakout_score" in error for error in errors)
        )
        self.assertTrue(
            any("risk_context_position_size_blend" in error for error in errors)
        )
        self.assertTrue(
            any("high_anchor_backlog_old_order_weight" in error for error in errors)
        )

    def test_summarize_sell_backlog_counts_and_ages(self):
        now = datetime(2026, 6, 13, 12, 0, tzinfo=timezone.utc)
        summary = guardrails.summarize_sell_backlog(
            {
                "a": {"placed_at": (now - timedelta(minutes=30)).isoformat()},
                "b": {"placed_at": (now - timedelta(minutes=90)).isoformat()},
            },
            now,
        )
        self.assertEqual(summary["count"], 2)
        self.assertAlmostEqual(summary["oldest_age_minutes"], 90.0, places=2)

    def test_runtime_buy_block_reason_blocks_on_daily_loss(self):
        reason = guardrails.runtime_buy_block_reason(
            operating_mode="range_plus_llm",
            realized_pnl_today=-25.0,
            max_daily_loss_usd=20.0,
            sell_backlog_count=0,
            sell_backlog_limit=0,
            sell_backlog_oldest_minutes=0.0,
            sell_backlog_minutes_limit=0,
            consecutive_loop_errors=0,
            max_consecutive_loop_errors=10,
            consecutive_private_api_failures=0,
            max_consecutive_private_api_failures=10,
        )
        self.assertEqual(reason, "max_daily_loss_usd")

    def test_runtime_buy_block_reason_blocks_on_sell_backlog(self):
        reason = guardrails.runtime_buy_block_reason(
            operating_mode="range_plus_llm",
            realized_pnl_today=0.0,
            max_daily_loss_usd=0.0,
            sell_backlog_count=4,
            sell_backlog_limit=4,
            sell_backlog_oldest_minutes=10.0,
            sell_backlog_minutes_limit=0,
            consecutive_loop_errors=0,
            max_consecutive_loop_errors=10,
            consecutive_private_api_failures=0,
            max_consecutive_private_api_failures=10,
        )
        self.assertEqual(reason, "sell_backlog_count")

    def test_runtime_buy_block_reason_blocks_on_operating_mode(self):
        reason = guardrails.runtime_buy_block_reason(
            operating_mode="sell_only",
            realized_pnl_today=0.0,
            max_daily_loss_usd=0.0,
            sell_backlog_count=0,
            sell_backlog_limit=0,
            sell_backlog_oldest_minutes=0.0,
            sell_backlog_minutes_limit=0,
            consecutive_loop_errors=0,
            max_consecutive_loop_errors=10,
            consecutive_private_api_failures=0,
            max_consecutive_private_api_failures=10,
        )
        self.assertEqual(reason, "operating_mode_sell_only")


if __name__ == "__main__":
    unittest.main()
