import json
import os
import tempfile
import unittest

from range_grid_strategy_catalog import (
    normalize_strategy_filename,
    strategy_catalog,
    validate_strategy_profile_selection,
)


class RangeGridStrategyCatalogTests(unittest.TestCase):
    def test_catalog_exposes_valid_profile_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "range_grid_strategy_test_live.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({
                    "operating_mode": "range_only",
                    "paper_trading_enabled": False,
                    "grid_anchor": "low,median",
                    "range_window_hours": 24,
                    "max_grid_size": 4,
                    "profit_target_pct": 0.012,
                }, handle)

            entries = strategy_catalog(directory)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["filename"], path.rsplit(os.sep, 1)[-1])
        self.assertEqual(entries[0]["label"], "Test Live")
        self.assertTrue(entries[0]["valid"])
        self.assertFalse(entries[0]["paper_trading_enabled"])
        self.assertEqual(entries[0]["operating_mode"], "range_only")

    def test_selection_rejects_paths_missing_files_and_invalid_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            invalid = os.path.join(
                directory,
                "range_grid_strategy_invalid.json",
            )
            with open(invalid, "w", encoding="utf-8") as handle:
                json.dump({"operating_mode": "not-a-mode"}, handle)

            with self.assertRaises(ValueError):
                normalize_strategy_filename("../range_grid_strategy_bad.json")
            with self.assertRaisesRegex(ValueError, "not found"):
                validate_strategy_profile_selection(
                    "range_grid_strategy_missing.json",
                    directory,
                )
            with self.assertRaisesRegex(ValueError, "invalid"):
                validate_strategy_profile_selection(
                    "range_grid_strategy_invalid.json",
                    directory,
                )


if __name__ == "__main__":
    unittest.main()
