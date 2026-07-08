import unittest

from signal_normalizer import normalize_signal_payload, select_asset_signal


def make_multi_asset_signal():
    return {
        "schema_version": "multi-asset-sentiment-v1",
        "single_asset_schema_version": "web-sentiment-v2",
        "processed_at": "2026-06-12T14:40:14+00:00",
        "freshness": {
            "processed_at": "2026-06-12T14:40:14+00:00",
            "fresh_for_minutes": 10,
            "warn_after_minutes": 12,
            "stale_after_minutes": 20,
            "engine_health_stale_after_minutes": 10,
        },
        "assets": {
            "BTC": {
                "asset_id": "BTC",
                "asset": {"symbol": "BTC", "name": "Bitcoin"},
                "processed_at": "2026-06-12T14:40:13+00:00",
                "asset_price": 63682.0,
                "asset_sentiment": 0.288037,
                "execution_signal": -0.0364,
                "confidence": 0.494274,
                "direction_bias": 0.231436,
                "risk_multiplier": 1.018,
                "contributor_count": 25,
                "signal_status": "fresh",
                "bot_action_allowed": True,
                "action_recommendation": "watch_only",
                "action_policy": {"reason": "watch-only test"},
                "risk_context": {
                    "recommended_posture": "entry_allowed",
                    "market_risk_score": 0.2763,
                    "buy_aggression_score": 0.4872,
                    "position_size_multiplier": 0.3525,
                    "hard_safety_flags": [],
                },
                "source_status": {
                    "asset_price": {"status": "fresh"},
                    "asset_price_regime": {"status": "fresh"},
                },
                "asset_price_regime": {
                    "range_position": 0.917,
                    "price_high": 63806.0,
                    "price_low": 62312.0,
                    "price_return_24h_pct": 1.3577,
                },
                "market_structure": {
                    "support": 62312.0,
                    "resistance": 63806.0,
                    "upside_pct": 0.1947,
                    "downside_pct": 2.1513,
                    "risk_reward": 0.09,
                },
                "asset_price_record": {
                    "price_usd": 63682.0,
                    "source": "coingecko_simple_price",
                },
                "asset_pipeline": {
                    "asset_price_source_status": "fresh",
                    "asset_price_regime_source_status": "fresh",
                },
            },
            "ETH": {
                "asset_id": "ETH",
                "execution_signal": -0.0785,
                "confidence": 0.265846,
            },
        },
    }


class SignalNormalizerTests(unittest.TestCase):
    def test_select_asset_signal_uses_requested_asset(self):
        selected = select_asset_signal(make_multi_asset_signal(), asset_id="ETH")

        self.assertEqual(selected["asset_id"], "ETH")
        self.assertEqual(selected["processed_at"], "2026-06-12T14:40:14+00:00")

    def test_normalize_signal_payload_flattens_multi_asset_btc(self):
        normalized = normalize_signal_payload(
            make_multi_asset_signal(),
            asset_id="BTC"
        )

        self.assertEqual(normalized["asset_id"], "BTC")
        self.assertEqual(normalized["asset_symbol"], "BTC")
        self.assertEqual(normalized["btc_price"], 63682.0)
        self.assertEqual(normalized["execution_signal"], -0.0364)
        self.assertEqual(normalized["action_recommendation"], "watch_only")
        self.assertEqual(
            normalized["risk_context"]["recommended_posture"],
            "entry_allowed"
        )
        self.assertEqual(
            normalized["risk_context"]["buy_aggression_score"],
            0.4872
        )
        self.assertEqual(normalized["source_status"]["market_data"]["status"], "fresh")
        self.assertEqual(normalized["source_status"]["price_regime"]["status"], "fresh")
        self.assertEqual(normalized["price_regime"]["range_position_24h"], 0.917)
        self.assertEqual(normalized["price_regime"]["price_high_24h"], 63806.0)
        self.assertEqual(normalized["price_regime"]["return_24h_pct"], 1.3577)
        self.assertEqual(normalized["market_structure"]["support_price"], 62312.0)
        self.assertEqual(normalized["market_structure"]["resistance_price"], 63806.0)
        self.assertEqual(
            normalized["market_structure"]["upside_to_resistance_pct"],
            0.1947
        )
        self.assertEqual(
            normalized["market_structure"]["downside_to_support_pct"],
            2.1513
        )
        self.assertEqual(
            normalized["market_structure"]["risk_reward_to_structure"],
            0.09
        )
        self.assertEqual(normalized["freshness"]["stale_after_minutes"], 20)
        self.assertEqual(
            normalized["asset_price_record"]["source"],
            "coingecko_simple_price"
        )
        self.assertEqual(
            normalized["asset_pipeline"]["asset_price_source_status"],
            "fresh"
        )
        self.assertEqual(normalized["target_prices"], [])


if __name__ == "__main__":
    unittest.main()
