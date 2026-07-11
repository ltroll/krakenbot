import json
import unittest

import llm_target_bot as bot
from risk_context import derive_risk_context


class LlmTargetBotWeatherPolicyTests(unittest.TestCase):
    def setUp(self):
        self.originals = {
            "USE_WEATHER_REPORT_POLICY": bot.USE_WEATHER_REPORT_POLICY,
            "WEATHER_REPORT_REQUIRED": bot.WEATHER_REPORT_REQUIRED,
            "WEATHER_EMERGENCY_BELL_BLOCK": bot.WEATHER_EMERGENCY_BELL_BLOCK,
            "WEATHER_POSITION_SIZE_ENABLED": bot.WEATHER_POSITION_SIZE_ENABLED,
            "WEATHER_TARGET_PROFIT_ENABLED": bot.WEATHER_TARGET_PROFIT_ENABLED,
            "WEATHER_MIN_POSITION_SIZE_MULTIPLIER": (
                bot.WEATHER_MIN_POSITION_SIZE_MULTIPLIER
            ),
            "WEATHER_MAX_POSITION_SIZE_MULTIPLIER": (
                bot.WEATHER_MAX_POSITION_SIZE_MULTIPLIER
            ),
        }

    def tearDown(self):
        for name, value in self.originals.items():
            setattr(bot, name, value)

    def weather_derived(self, **overrides):
        weather = {
            "mode": "weather_report",
            "bot_decision_authority": "bot",
            "trade_permission": "bot_decides",
            "condition": "breakout_tailwind",
            "alert_level": "watch",
            "emergency_bell": False,
            "bot_tuning": {
                "position_size_multiplier": 0.32,
                "target_profit_multiplier": 1.08,
            },
        }
        weather.update(overrides)
        return derive_risk_context({"weather_report": weather})

    def test_legacy_action_gate_remains_default(self):
        derived = self.weather_derived()

        self.assertTrue(bot.should_apply_legacy_action_gate(derived))

    def test_weather_bot_decides_bypasses_legacy_action_gate_when_enabled(self):
        bot.USE_WEATHER_REPORT_POLICY = True
        derived = self.weather_derived()

        self.assertFalse(bot.should_apply_legacy_action_gate(derived))

    def test_weather_policy_blocks_emergency_bell(self):
        bot.USE_WEATHER_REPORT_POLICY = True
        bot.WEATHER_EMERGENCY_BELL_BLOCK = True
        derived = self.weather_derived(
            condition="storm_warning",
            alert_level="danger",
            emergency_bell=True,
        )

        self.assertEqual(
            bot.weather_policy_gate_failure(derived),
            {"reason": "weather_emergency_bell"},
        )

    def test_weather_policy_can_require_weather_report(self):
        bot.USE_WEATHER_REPORT_POLICY = True
        bot.WEATHER_REPORT_REQUIRED = True
        derived = derive_risk_context({})

        self.assertEqual(
            bot.weather_policy_gate_failure(derived),
            {"reason": "weather_report_missing"},
        )

    def test_weather_position_multiplier_is_clamped(self):
        bot.USE_WEATHER_REPORT_POLICY = True
        bot.WEATHER_POSITION_SIZE_ENABLED = True
        bot.WEATHER_MIN_POSITION_SIZE_MULTIPLIER = 0.25
        bot.WEATHER_MAX_POSITION_SIZE_MULTIPLIER = 0.75
        derived = self.weather_derived()

        self.assertEqual(bot.weather_position_multiplier(derived), 0.32)

    def test_weather_target_profit_multiplier_uses_tuning(self):
        bot.USE_WEATHER_REPORT_POLICY = True
        bot.WEATHER_TARGET_PROFIT_ENABLED = True
        derived = self.weather_derived()

        self.assertEqual(bot.weather_target_profit_multiplier(derived), 1.08)

    def test_weather_strategy_profiles_are_valid_json(self):
        for path in (
            "llm_target_strategy_weather_dryrun.json",
            "llm_target_strategy_weather_tiny_live.json",
            "llm_target_strategy_weather_tiny_selective_live.json",
            "llm_target_strategy_weather_small_live.json",
        ):
            with open(path, encoding="utf-8") as f:
                payload = json.load(f)
            self.assertTrue(payload["use_weather_report_policy"])
            self.assertTrue(payload["weather_report_required"])
            self.assertTrue(payload["weather_emergency_bell_block"])
            self.assertEqual(payload["max_open_buy_orders"], 1)
            self.assertEqual(payload["max_open_sell_orders"], 1)
            if path != "llm_target_strategy_weather_dryrun.json":
                self.assertEqual(
                    payload["backtest_strategy_variant"],
                    "weather_target_quality_tp_0_8",
                )

    def test_weather_live_profiles_step_up_in_size(self):
        with open("llm_target_strategy_weather_tiny_live.json", encoding="utf-8") as f:
            tiny = json.load(f)
        with open("llm_target_strategy_weather_small_live.json", encoding="utf-8") as f:
            small = json.load(f)

        self.assertLess(tiny["max_trade_usd"], small["max_trade_usd"])
        self.assertLess(tiny["max_inventory_usd"], small["max_inventory_usd"])
        self.assertFalse(tiny["dry_run"])
        self.assertFalse(small["dry_run"])

    def test_weather_tiny_selective_profile_is_stricter_than_tiny_live(self):
        with open("llm_target_strategy_weather_tiny_live.json", encoding="utf-8") as f:
            tiny = json.load(f)
        with open(
            "llm_target_strategy_weather_tiny_selective_live.json",
            encoding="utf-8"
        ) as f:
            selective = json.load(f)

        self.assertEqual(tiny["max_trade_usd"], selective["max_trade_usd"])
        self.assertGreater(
            selective["target_quality_min_samples"],
            tiny["target_quality_min_samples"],
        )
        self.assertGreater(
            selective["target_quality_min_ev_pct"],
            tiny["target_quality_min_ev_pct"],
        )
        self.assertGreater(
            selective["target_quality_min_4h_fill_probability"],
            tiny["target_quality_min_4h_fill_probability"],
        )


if __name__ == "__main__":
    unittest.main()
