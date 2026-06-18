import unittest
from datetime import datetime, timedelta, timezone

from competition_backtest import (
    competition_allows_entry,
    replay_strategy,
    simulated_buy_allows_entry,
)


def make_snapshot(
    minutes,
    price,
    decision="shadow_candidate",
    status="ok",
    spread_bps=20,
):
    captured_at = datetime(2026, 6, 17, 14, 0, tzinfo=timezone.utc) + timedelta(
        minutes=minutes
    )
    return {
        "captured_at": captured_at.isoformat(),
        "snapshot_kind": "competition_backtest_input",
        "decision": {
            "ok": True,
            "payload": {
                "status": status,
                "source_age_minutes": 1,
                "source_stale_after_minutes": 10,
                "decision": decision,
                "reason": "test reason",
                "competition": {
                    "asset_id": "NEO",
                    "kraken_pair": "NEOUSD",
                },
                "market": {
                    "mid_price": price,
                    "last_price": price,
                },
                "risk": {
                    "shadow_only": True,
                    "max_position_usd": 25.0,
                },
            },
            "summary": {
                "status": status,
                "source_age_minutes": 1,
                "source_stale_after_minutes": 10,
                "decision": decision,
                "reason": "test reason",
                "asset_id": "NEO",
                "kraken_pair": "NEOUSD",
                "mid_price": price,
                "last_price": price,
                "spread_bps": spread_bps,
                "shadow_only": True,
                "max_position_usd": 25.0,
            },
        },
    }


class CompetitionBacktestTests(unittest.TestCase):
    def test_competition_allows_shadow_candidate_only(self):
        allowed, reason = competition_allows_entry(make_snapshot(0, 2.0))
        self.assertTrue(allowed)
        self.assertIsNone(reason)

        allowed, reason = competition_allows_entry(
            make_snapshot(0, 2.0, decision="blocked")
        )
        self.assertFalse(allowed)
        self.assertEqual(reason, "test reason")

    def test_simulated_buy_allows_any_fresh_ok_snapshot(self):
        allowed, reason = simulated_buy_allows_entry(
            make_snapshot(0, 2.0, decision="blocked")
        )
        self.assertTrue(allowed)
        self.assertIsNone(reason)

    def test_replay_strategy_closes_take_profit(self):
        snapshots = [
            make_snapshot(0, 2.00),
            make_snapshot(5, 2.03),
        ]

        result = replay_strategy(
            "competition_allowed",
            snapshots,
            competition_allows_entry,
            trade_usd=25,
            take_profit_pct=1.0,
            stop_loss_pct=1.0,
            max_hold_minutes=60,
            cooldown_minutes=0,
            fee_bps=0,
        )

        summary = result["summary"]
        self.assertEqual(summary["entries"], 1)
        self.assertEqual(summary["closed_trades"], 1)
        self.assertEqual(summary["take_profit_count"], 1)
        self.assertGreater(summary["net_pnl_usd"], 0)

    def test_replay_strategy_baseline_enters_when_competition_blocks(self):
        snapshots = [
            make_snapshot(0, 2.00, decision="blocked"),
            make_snapshot(5, 2.03, decision="blocked"),
        ]

        competition = replay_strategy(
            "competition_allowed",
            snapshots,
            competition_allows_entry,
            trade_usd=25,
            take_profit_pct=1.0,
            stop_loss_pct=1.0,
            max_hold_minutes=60,
            cooldown_minutes=0,
            fee_bps=0,
        )
        baseline = replay_strategy(
            "simulated_buy_allowed",
            snapshots,
            simulated_buy_allows_entry,
            trade_usd=25,
            take_profit_pct=1.0,
            stop_loss_pct=1.0,
            max_hold_minutes=60,
            cooldown_minutes=0,
            fee_bps=0,
        )

        self.assertEqual(competition["summary"]["entries"], 0)
        self.assertEqual(baseline["summary"]["entries"], 1)

    def test_taker_fill_pays_spread_while_maker_model_captures_it(self):
        snapshots = [make_snapshot(0, 2.0), make_snapshot(60, 2.0)]

        taker = replay_strategy(
            "taker",
            snapshots,
            competition_allows_entry,
            trade_usd=25,
            take_profit_pct=10,
            stop_loss_pct=10,
            max_hold_minutes=60,
            cooldown_minutes=0,
            fee_bps=0,
            fill_model="taker",
        )
        maker = replay_strategy(
            "maker",
            snapshots,
            competition_allows_entry,
            trade_usd=25,
            take_profit_pct=10,
            stop_loss_pct=10,
            max_hold_minutes=60,
            cooldown_minutes=0,
            fee_bps=0,
            fill_model="maker",
        )

        self.assertLess(taker["summary"]["net_pnl_usd"], 0)
        self.assertGreater(maker["summary"]["net_pnl_usd"], 0)

    def test_signal_reset_prevents_immediate_reentry(self):
        snapshots = [
            make_snapshot(0, 2.00),
            make_snapshot(5, 2.03),
            make_snapshot(6, 2.03),
            make_snapshot(7, 2.03, decision="blocked"),
            make_snapshot(8, 2.03),
        ]

        result = replay_strategy(
            "competition_allowed",
            snapshots,
            competition_allows_entry,
            trade_usd=25,
            take_profit_pct=1,
            stop_loss_pct=1,
            max_hold_minutes=60,
            cooldown_minutes=0,
            fee_bps=0,
            require_signal_reset=True,
        )

        self.assertEqual(result["summary"]["entries"], 2)
        self.assertEqual(
            result["summary"]["blocked_reasons"]["waiting_for_signal_reset"],
            1,
        )

    def test_open_mark_to_market_is_not_counted_as_closed(self):
        result = replay_strategy(
            "competition_allowed",
            [make_snapshot(0, 2.0), make_snapshot(5, 2.01)],
            competition_allows_entry,
            trade_usd=25,
            take_profit_pct=10,
            stop_loss_pct=10,
            max_hold_minutes=60,
            cooldown_minutes=0,
            fee_bps=40,
        )

        summary = result["summary"]
        self.assertEqual(summary["closed_trades"], 0)
        self.assertEqual(summary["open_trades"], 1)
        self.assertEqual(summary["mark_to_market_count"], 1)
        self.assertEqual(summary["realized_net_pnl_usd"], 0)
        self.assertEqual(summary["net_pnl_usd"], summary["unrealized_net_pnl_usd"])


if __name__ == "__main__":
    unittest.main()
