import unittest

from competition_shadow_bot import evaluate_competition_decision


def make_payload(**overrides):
    payload = {
        "schema_version": "competition-decision-v1",
        "processed_at": "2026-06-17T14:05:00+00:00",
        "status": "ok",
        "source_timestamp": "2026-06-17T14:04:12+00:00",
        "source_age_minutes": 0.8,
        "source_stale_after_minutes": 10,
        "competition": {
            "asset_id": "NEO",
            "symbol": "NEO",
            "kraken_pair": "NEOUSD",
            "mode": "shadow",
        },
        "decision": "shadow_candidate",
        "reason": "Competition market conditions pass configured shadow guardrails.",
        "filter_failures": {"config": [], "time_window": [], "market": []},
        "market": {
            "last_price": 9.95,
            "mid_price": 9.95,
            "spread_bps": 20.1,
            "top10_bid_depth_usd": 5200.45,
            "top10_ask_depth_usd": 4800.12,
            "trade_count": 34,
            "total_notional_usd": 42000.75,
            "aggression_score": 0.18,
        },
        "risk": {
            "shadow_only": True,
            "live_trading_enabled": False,
            "max_position_usd": 25.0,
            "max_daily_loss_usd": 25.0,
            "max_trades_per_day": 20,
            "max_spread_bps": 35.0,
            "min_top10_usd_depth": 1000.0,
        },
    }
    payload.update(overrides)
    return payload


class CompetitionShadowBotTests(unittest.TestCase):
    def test_records_tradeable_shadow_candidate(self):
        result = evaluate_competition_decision(make_payload())

        self.assertEqual(result["action"], "record_tradeable_shadow_candidate")
        self.assertEqual(result["reason"], "shadow_only_market_tradeable")
        self.assertEqual(result["summary"]["asset_id"], "NEO")
        self.assertEqual(result["summary"]["kraken_pair"], "NEOUSD")

    def test_status_not_ok_does_nothing(self):
        result = evaluate_competition_decision(
            make_payload(status="blocked", reason="collector unavailable")
        )

        self.assertEqual(result["action"], "do_nothing")
        self.assertEqual(result["reason"], "status_not_ok")

    def test_stale_source_does_nothing(self):
        result = evaluate_competition_decision(
            make_payload(source_age_minutes=10.1, source_stale_after_minutes=10)
        )

        self.assertEqual(result["action"], "do_nothing")
        self.assertEqual(result["reason"], "source_snapshot_stale")

    def test_shadow_block_records_reason(self):
        result = evaluate_competition_decision(
            make_payload(
                decision="blocked",
                reason="spread rises above configured limit",
            )
        )

        self.assertEqual(result["action"], "record_shadow_blocked")
        self.assertEqual(result["reason"], "spread rises above configured limit")

    def test_live_mode_is_not_supported(self):
        payload = make_payload(risk={"shadow_only": False, "live_trading_enabled": True})
        result = evaluate_competition_decision(payload)

        self.assertEqual(result["action"], "do_nothing")
        self.assertEqual(result["reason"], "live_mode_not_supported_by_shadow_bot")


if __name__ == "__main__":
    unittest.main()
