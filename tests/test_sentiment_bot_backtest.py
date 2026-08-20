import unittest
from datetime import datetime, timezone

import sentiment_bot_backtest as backtest


class SentimentBotBacktestSafetyTests(unittest.TestCase):
    def signal(self, **overrides):
        now = datetime.now(timezone.utc).isoformat()
        payload = {
            "processed_at": now,
            "signal_status": "fresh",
            "bot_action_allowed": True,
            "source_status": {
                "market_data": {"status": "fresh"},
                "price_regime": {"status": "fresh"},
                "kraken_flow": {"status": "fresh"},
            },
            "execution_signal": 1.0,
            "smoothed_signal": 1.0,
            "confidence": 0.2,
            "price_regime": {
                "realized_volatility_24h_pct": 0.04,
            },
        }
        payload.update(overrides)
        return payload

    def snapshot(self, signal):
        return {
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "ticker": {"last_price": 65000.0},
            "signal": {"payload": signal},
            "strategy_profile": {
                "payload": {
                    "confidence_threshold": 0.45,
                    "confidence_weighting": True,
                    "volatility_dampening": True,
                    "volatility_cutoff": 0.01,
                    "use_signal_status_gates": True,
                    "require_bot_action_allowed": True,
                }
            },
        }

    def test_backtest_enforces_confidence_threshold(self):
        snapshot = self.snapshot(self.signal())

        event = backtest.evaluate_snapshot(
            snapshot,
            "current",
            0.005,
            1000.0,
        )

        self.assertEqual(event["status"], "hold")
        self.assertEqual(event["reason"], "confidence_below_threshold")

    def test_backtest_uses_same_volatility_dampening(self):
        config = {
            "confidence_weighting": True,
            "volatility_dampening": True,
            "volatility_cutoff": 0.01,
        }

        weighted, _, _, _ = backtest.weighted_signal(self.signal(), config)

        self.assertAlmostEqual(weighted, 0.05)

    def test_backtest_missing_timestamp_fails_closed_without_crashing(self):
        signal = self.signal()
        signal.pop("processed_at")
        snapshot = self.snapshot(signal)

        event = backtest.evaluate_snapshot(
            snapshot,
            "current",
            0.005,
            1000.0,
        )

        self.assertEqual(event["status"], "hold")
        self.assertEqual(event["reason"], "signal_timestamp_missing")
        self.assertIsNone(event["signal_age_minutes"])

    def test_backtest_inventory_includes_pending_protection_and_buy_intents(self):
        snapshot = {
            "state": {
                "pending_profit_sells": [
                    {"buy_price": 65000.0, "volume": 0.004}
                ],
                "pending_order_intents": [
                    {
                        "side": "buy",
                        "price": 64000.0,
                        "volume": 0.002,
                    },
                    {
                        "side": "sell",
                        "price": 66000.0,
                        "volume": 0.001,
                    },
                ],
            }
        }

        inventory = backtest.current_inventory_usd(snapshot, 65500.0)

        self.assertAlmostEqual(inventory, 388.0)
        self.assertEqual(backtest.open_order_count(snapshot, "buy"), 1)
        self.assertEqual(backtest.open_order_count(snapshot, "sell"), 1)


if __name__ == "__main__":
    unittest.main()
