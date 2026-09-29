import ast
import os
import unittest


class RangeGridOperatorControlWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.bot_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "range_grid_bot.py",
        )
        with open(cls.bot_path, encoding="utf-8") as handle:
            cls.source = handle.read()
        cls.tree = ast.parse(cls.source)

    def test_bot_reads_operator_size_and_cycle_limit(self):
        call_names = {
            node.func.id
            for node in ast.walk(self.tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
        }

        self.assertIn("operator_buy_order_size_usd", call_names)
        self.assertIn("operator_round_trip_limit_status", call_names)
        self.assertIn(
            'skip_reason = "operator_daily_round_trip_limit"',
            self.source,
        )
        self.assertIn(
            '"operator_order_size_insufficient_available_usd"',
            self.source,
        )

    def test_cycle_metadata_follows_buy_into_sell_state(self):
        self.assertGreaterEqual(
            self.source.count('"operator_round_trip_day"'),
            2,
        )
        self.assertIn(
            '"operator_order_size_override_usd": order.get(',
            self.source,
        )
        self.assertIn(
            '"operator_daily_round_trip_limit_at_start": order.get(',
            self.source,
        )

    def test_operator_size_replaces_strategy_calculated_notional(self):
        self.assertIn(
            "trade_notional_usd = operator_order_size_override_usd",
            self.source,
        )
        self.assertIn(
            "volume = trade_notional_usd / level",
            self.source,
        )


if __name__ == "__main__":
    unittest.main()
