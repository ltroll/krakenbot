import ast
import os
import unittest


class RangeGridBotLoggingTests(unittest.TestCase):
    def test_sentiment_risk_fields_are_not_passed_twice(self):
        bot_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "range_grid_bot.py",
        )
        with open(bot_path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())

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


if __name__ == "__main__":
    unittest.main()
