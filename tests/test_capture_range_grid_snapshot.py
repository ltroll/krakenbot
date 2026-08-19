import json
import os
import tempfile
import unittest

import capture_range_grid_snapshot as capture


class CaptureRangeGridSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.original_status_file = capture.STATUS_FILE

    def tearDown(self):
        capture.STATUS_FILE = self.original_status_file

    def test_status_snapshot_preserves_effective_strategy_contract(self):
        effective_strategy = {
            "base": {
                "effective_fingerprint": "sha256:base",
                "composition_mode": "deep_route_over_base",
            },
            "routes": {
                "low": {
                    "effective_fingerprint": "sha256:low",
                    "payload": {"position_size_pct": 0.05},
                }
            },
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            capture.STATUS_FILE = os.path.join(tmpdir, "status.json")
            with open(capture.STATUS_FILE, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "timestamp": "2026-08-19T12:00:00+00:00",
                        "strategy_profile": "production.json",
                        "base_strategy_fingerprint": "sha256:base",
                        "effective_strategy": effective_strategy,
                    },
                    handle,
                )

            result = capture.status_snapshot()

        self.assertTrue(result["ok"])
        self.assertEqual(
            result["summary"]["base_strategy_fingerprint"],
            "sha256:base",
        )
        self.assertEqual(
            result["summary"]["effective_strategy"],
            effective_strategy,
        )


if __name__ == "__main__":
    unittest.main()
