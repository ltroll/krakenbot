import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from range_grid_instance import (
    available_quote_cash,
    effective_inventory_cap,
    instance_runtime_path,
    instance_web_path,
    normalize_instance_id,
    price_from_record,
    validate_instance_configuration,
)
from range_grid_instance_check import build_preflight


class RangeGridInstanceTests(unittest.TestCase):
    def test_sol_live_profile_matches_paper_baseline_except_mode(self):
        repository = Path(__file__).resolve().parent.parent
        with (repository / "range_grid_strategy_sol_paper_baseline.json").open(
            encoding="utf-8"
        ) as paper_handle, (repository / "range_grid_strategy_sol_live_baseline.json").open(
            encoding="utf-8"
        ) as live_handle:
            paper = json.load(paper_handle)
            live = json.load(live_handle)

        self.assertTrue(paper.pop("paper_trading_enabled"))
        self.assertFalse(live.pop("paper_trading_enabled"))
        self.assertEqual(live, paper)

    def test_instance_paths_preserve_legacy_defaults_without_instance(self):
        self.assertEqual(instance_runtime_path("", "last_state.json"), "last_state.json")
        self.assertEqual(
            instance_web_path(
                "",
                "range_grid_activity.jsonl",
                legacy_path="/var/www/html/bot/range_grid_activity.jsonl",
            ),
            "/var/www/html/bot/range_grid_activity.jsonl",
        )

    def test_instance_paths_namespace_new_asset(self):
        self.assertEqual(
            instance_runtime_path("sol", "last_state.json"),
            "instances/sol/last_state.json",
        )
        self.assertEqual(
            instance_web_path(
                "sol",
                "range_grid_activity.jsonl",
                legacy_path="/var/www/html/bot/range_grid_activity.jsonl",
            ),
            "/var/www/html/bot/sol/range_grid_activity.jsonl",
        )

    def test_instance_id_rejects_unsafe_value(self):
        with self.assertRaises(ValueError):
            normalize_instance_id("../sol")

    def test_capital_allocation_and_cash_reserve_are_hard_limits(self):
        self.assertEqual(effective_inventory_cap(2500, 400), 400)
        self.assertEqual(effective_inventory_cap(250, 400), 250)
        self.assertEqual(available_quote_cash(1000, 150, 300), 550)

    def test_price_records_support_asset_neutral_and_legacy_fields(self):
        self.assertEqual(
            price_from_record({"asset_price_usd": 145.2}, "SOL"),
            145.2,
        )
        self.assertEqual(
            price_from_record({"sol_price_usd": 146.1}, "SOL"),
            146.1,
        )
        self.assertEqual(
            price_from_record({"btc_price_usd": 80_000}, "BTC"),
            80_000,
        )

    def test_sol_live_instance_requires_identity_strategy_and_confirmation(self):
        errors = validate_instance_configuration(
            instance_id="sol",
            asset_id="SOL",
            kraken_pair="SOLUSD",
            strategy={"asset_id": "SOL", "kraken_pair": "SOLUSD"},
            order_tracker_symbol="SOLUSD",
            paper_trading_enabled=False,
            live_enabled=True,
            live_confirmation="SOLUSD",
        )
        self.assertEqual(errors, [])

    def test_pair_validation_accepts_kraken_btc_aliases(self):
        errors = validate_instance_configuration(
            instance_id="btc-secondary",
            asset_id="BTC",
            kraken_pair="XXBTZUSD",
            strategy={"asset_id": "BTC", "kraken_pair": "XBTUSD"},
            order_tracker_symbol="BTCUSD",
            paper_trading_enabled=True,
        )

        self.assertEqual(errors, [])

    def test_non_btc_instance_fails_closed_on_btc_strategy_or_missing_gate(self):
        errors = validate_instance_configuration(
            instance_id="sol",
            asset_id="SOL",
            kraken_pair="SOLUSD",
            strategy={"asset_id": "BTC", "kraken_pair": "XXBTZUSD"},
            order_tracker_symbol="BTCUSD",
            paper_trading_enabled=False,
            live_enabled=False,
            live_confirmation="",
        )
        self.assertTrue(any("strategy asset_id=BTC" in error for error in errors))
        self.assertTrue(any("order tracker symbol" in error for error in errors))
        self.assertTrue(any("LIVE_ENABLED" in error for error in errors))
        self.assertTrue(any("LIVE_CONFIRMATION" in error for error in errors))

    def test_preflight_accepts_isolated_sol_paper_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            strategy_path = os.path.join(
                directory,
                "range_grid_strategy_sol_test.json",
            )
            env_path = os.path.join(directory, "sol.env")
            with open(strategy_path, "w", encoding="utf-8") as handle:
                json.dump({
                    "asset_id": "SOL",
                    "kraken_pair": "SOLUSD",
                    "paper_trading_enabled": True,
                    "grid_anchor": "low",
                    "order_tracker_symbol": "SOLUSD",
                }, handle)
            with open(env_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "RANGE_GRID_INSTANCE_ID=sol\n"
                    "SIGNAL_ASSET_ID=SOL\n"
                    "KRAKEN_PAIR=SOLUSD\n"
                    f"RANGE_GRID_STRATEGY_DIRECTORY={directory}\n"
                    "RANGE_GRID_STRATEGY_PROFILE="
                    "range_grid_strategy_sol_test.json\n"
                    "RANGE_GRID_CAPITAL_ALLOCATION_USD=500\n"
                    "RANGE_GRID_QUOTE_CASH_RESERVE_USD=100\n"
                    "KRAKEN_API_KEY=test-key\n"
                    "KRAKEN_API_SECRET=test-secret\n"
                )
            with patch.dict(os.environ, {}, clear=True):
                result = build_preflight(env_path)

        self.assertTrue(result["ok"], result["errors"])
        self.assertEqual(result["paths"]["state"], "instances/sol/last_state.json")

    def test_preflight_rejects_placeholder_token_on_lan(self):
        with tempfile.TemporaryDirectory() as directory:
            strategy_path = os.path.join(directory, "range_grid_strategy_sol.json")
            env_path = os.path.join(directory, "sol.env")
            with open(strategy_path, "w", encoding="utf-8") as handle:
                json.dump({
                    "asset_id": "SOL",
                    "kraken_pair": "SOLUSD",
                    "paper_trading_enabled": True,
                    "grid_anchor": "low",
                }, handle)
            with open(env_path, "w", encoding="utf-8") as handle:
                handle.write(
                    "RANGE_GRID_INSTANCE_ID=sol\n"
                    "SIGNAL_ASSET_ID=SOL\n"
                    "KRAKEN_PAIR=SOLUSD\n"
                    f"RANGE_GRID_STRATEGY_DIRECTORY={directory}\n"
                    "RANGE_GRID_STRATEGY_PROFILE=range_grid_strategy_sol.json\n"
                    "RANGE_GRID_CAPITAL_ALLOCATION_USD=500\n"
                    "KRAKEN_API_KEY=test-key\n"
                    "KRAKEN_API_SECRET=test-secret\n"
                    "RANGE_GRID_CONTROL_HOST=0.0.0.0\n"
                    "RANGE_GRID_CONTROL_TOKEN=replace-with-a-token\n"
                )
            with patch.dict(os.environ, {}, clear=True):
                result = build_preflight(env_path)

        self.assertFalse(result["ok"])
        self.assertTrue(any("CONTROL_TOKEN" in error for error in result["errors"]))


if __name__ == "__main__":
    unittest.main()
