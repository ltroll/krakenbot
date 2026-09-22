import unittest

from recover_range_grid_filled_buys import (
    adopt_recovered_buys,
    recovered_buy_state,
)


class RecoverRangeGridFilledBuysTests(unittest.TestCase):
    def test_builds_exact_locked_recovery_state_from_confirmed_fill(self):
        order = {
            "status": "closed",
            "opentm": 1790082662.76496,
            "closetm": 1790083340.656698,
            "vol": "0.00235516",
            "vol_exec": "0.00235516",
            "cost": "202.32473",
            "fee": "0.60697",
            "price": "85906.9",
            "cl_ord_id": "rge64bb562f1513535",
            "descr": {"type": "buy", "pair": "XBTUSD"},
        }

        recovered = recovered_buy_state(
            "OVJLX5-GMJNW-KCJH6V",
            order,
            configured_pair="XXBTZUSD",
            buy_source="range_median",
            net_profit_target_pct=0.018,
            base_profit_target_pct=0.009,
        )

        self.assertEqual(recovered["volume"], 0.00235516)
        self.assertEqual(recovered["buy_cost"], 202.32473)
        self.assertEqual(recovered["buy_fee"], 0.60697)
        self.assertAlmostEqual(recovered["price"], 85906.9)
        self.assertEqual(recovered["locked_sell_profit_target_pct"], 0.018)
        self.assertEqual(recovered["fear_greed_profit_target_multiplier"], 2.0)
        self.assertTrue(recovered["operator_profit_target_override_applied"])

    def test_adoption_refuses_already_tracked_trade(self):
        order = {
            "txid": "BUY-1",
            "trade_id": "BUY-1",
            "price": 85000.0,
            "volume": 0.002,
            "buy_cost": 170.0,
            "buy_fee": 0.5,
            "locked_sell_profit_target_pct": 0.01,
        }
        state = {
            "open_buy_orders": {},
            "open_sell_orders": {
                "SELL-1": {"trade_id": "BUY-1"},
            },
            "processed_fills": {"buy": {}},
        }

        with self.assertRaisesRegex(ValueError, "already tracked"):
            adopt_recovered_buys(state, [order])

    def test_adoption_adds_fill_without_marking_it_processed(self):
        order = {
            "txid": "BUY-2",
            "trade_id": "BUY-2",
            "price": 84999.9,
            "volume": 0.002,
            "buy_cost": 169.9998,
            "buy_fee": 0.5,
            "locked_sell_profit_target_pct": 0.01,
        }
        state = {
            "open_buy_orders": {},
            "open_sell_orders": {},
            "processed_fills": {"buy": {}},
        }

        adopted = adopt_recovered_buys(state, [order])

        self.assertEqual(adopted[0]["txid"], "BUY-2")
        self.assertIn("84999.9", state["open_buy_orders"])
        self.assertNotIn("BUY-2", state["processed_fills"]["buy"])


if __name__ == "__main__":
    unittest.main()
