import ast
import os
import unittest


class RangeGridBotLoggingTests(unittest.TestCase):
    def _bot_tree(self):
        bot_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "range_grid_bot.py",
        )
        with open(bot_path, encoding="utf-8") as handle:
            return ast.parse(handle.read())

    def test_sentiment_risk_fields_are_not_passed_twice(self):
        tree = self._bot_tree()

        risk_function = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "sentiment_risk_log_fields"
        )
        risk_keys = {
            key.value
            for node in ast.walk(risk_function)
            if isinstance(node, ast.Dict)
            for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }

        collisions = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            expands_risk_fields = any(
                keyword.arg is None
                and isinstance(keyword.value, ast.Name)
                and keyword.value.id == "sentiment_risk_fields"
                for keyword in node.keywords
            )
            if not expands_risk_fields:
                continue
            explicit_keys = {
                keyword.arg for keyword in node.keywords if keyword.arg is not None
            }
            duplicate_keys = sorted(risk_keys & explicit_keys)
            if duplicate_keys:
                collisions.append((node.lineno, duplicate_keys))

        self.assertEqual(collisions, [])

    def test_runtime_identity_fields_are_not_passed_twice(self):
        tree = self._bot_tree()

        identity_function = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            and node.name == "runtime_identity"
        )
        identity_keys = {
            key.value
            for node in ast.walk(identity_function)
            if isinstance(node, ast.Dict)
            for key in node.keys
            if isinstance(key, ast.Constant) and isinstance(key.value, str)
        }

        collisions = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            expands_identity = any(
                keyword.arg is None
                and isinstance(keyword.value, ast.Name)
                and keyword.value.id == "instance_identity"
                for keyword in node.keywords
            )
            if not expands_identity:
                continue
            explicit_keys = {
                keyword.arg for keyword in node.keywords if keyword.arg is not None
            }
            duplicate_keys = sorted(identity_keys & explicit_keys)
            if duplicate_keys:
                collisions.append((node.lineno, duplicate_keys))

        self.assertEqual(collisions, [])

    def test_live_kraken_order_and_ticker_paths_use_configured_pair(self):
        tree = self._bot_tree()
        violations = []

        for function_name in ("get_price", "place_buy", "place_sell"):
            function = next(
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.FunctionDef)
                and node.name == function_name
            )
            for node in ast.walk(function):
                if (
                    isinstance(node, ast.Constant)
                    and isinstance(node.value, str)
                    and node.value == "XXBTZUSD"
                ):
                    violations.append((function_name, node.lineno))

        self.assertEqual(violations, [])

    def test_live_flow_control_receives_effective_route_config(self):
        tree = self._bot_tree()
        calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "flow_adjustment"
        ]

        self.assertTrue(
            any(
                len(call.args) == 3
                and isinstance(call.args[2], ast.Name)
                and call.args[2].id == "route_config"
                for call in calls
            )
        )


if __name__ == "__main__":
    unittest.main()
