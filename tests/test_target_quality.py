import json
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from target_quality import (
    TARGET_QUALITY_MATCH_TOLERANCE_PCT,
    evaluate_quality_target,
    load_target_quality_snapshot,
    match_quality_target,
    unavailable_quality_decision,
)


def write_snapshot(payload):
    handle = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    try:
        json.dump(payload, handle)
        handle.flush()
        return handle.name
    finally:
        handle.close()


class TargetQualityTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 5, 30, 15, 0, tzinfo=timezone.utc)
        self.base_target = {
            "buy_price": 73159.0,
            "matched_sample_count": 42,
            "fill_probability": {
                "4h": {
                    "sample_count": 42,
                    "fill_probability": 0.47
                }
            },
            "best_profit_target_pct": 0.5,
            "best_expected_value_pct_per_signal": 0.0846,
            "recommendation": "watch"
        }
        self.paths = []

    def tearDown(self):
        for path in self.paths:
            if os.path.exists(path):
                os.unlink(path)

    def snapshot_path(self, **target_overrides):
        payload = {
            "status": "ok",
            "timestamp": self.now.isoformat(),
            "targets": [{**self.base_target, **target_overrides}]
        }
        path = write_snapshot(payload)
        self.paths.append(path)
        return path

    def test_quality_file_allows_good_target(self):
        path = self.snapshot_path()
        snapshot = load_target_quality_snapshot(path, 30, now=self.now)
        self.assertTrue(snapshot["available"])

        matched = match_quality_target(73159.0, snapshot["targets"])
        result = evaluate_quality_target(
            matched,
            min_samples=20,
            min_ev_pct=0.02,
            min_4h_fill_probability=0.35,
            allowed_recommendations={"buy_allowed", "watch"}
        )
        self.assertTrue(result["allowed"])
        self.assertEqual(result["reason"], "quality_allowed")

    def test_low_sample_count_blocks(self):
        path = self.snapshot_path(matched_sample_count=10)
        snapshot = load_target_quality_snapshot(path, 30, now=self.now)
        matched = match_quality_target(73159.0, snapshot["targets"])
        result = evaluate_quality_target(
            matched,
            min_samples=20,
            min_ev_pct=0.02,
            min_4h_fill_probability=0.35,
            allowed_recommendations={"buy_allowed", "watch"}
        )
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "quality_low_sample_count")

    def test_low_ev_blocks(self):
        path = self.snapshot_path(best_expected_value_pct_per_signal=0.01)
        snapshot = load_target_quality_snapshot(path, 30, now=self.now)
        matched = match_quality_target(73159.0, snapshot["targets"])
        result = evaluate_quality_target(
            matched,
            min_samples=20,
            min_ev_pct=0.02,
            min_4h_fill_probability=0.35,
            allowed_recommendations={"buy_allowed", "watch"}
        )
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "quality_low_expected_value")

    def test_low_4h_fill_probability_blocks(self):
        path = self.snapshot_path(
            fill_probability={"4h": {"sample_count": 42, "fill_probability": 0.2}}
        )
        snapshot = load_target_quality_snapshot(path, 30, now=self.now)
        matched = match_quality_target(73159.0, snapshot["targets"])
        result = evaluate_quality_target(
            matched,
            min_samples=20,
            min_ev_pct=0.02,
            min_4h_fill_probability=0.35,
            allowed_recommendations={"buy_allowed", "watch"}
        )
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "quality_low_4h_fill_probability")

    def test_stale_missing_file_behavior_fail_open(self):
        snapshot = load_target_quality_snapshot(
            "/tmp/does-not-exist-target-quality.json",
            30,
            now=self.now
        )
        decision = unavailable_quality_decision(snapshot, fail_closed=False)
        self.assertTrue(decision["allowed"])
        self.assertFalse(decision["blocked"])
        self.assertEqual(decision["reason"], "target_quality_missing")

    def test_stale_missing_file_behavior_fail_closed(self):
        path = self.snapshot_path()
        stale_now = self.now + timedelta(minutes=31)
        snapshot = load_target_quality_snapshot(path, 30, now=stale_now)
        decision = unavailable_quality_decision(snapshot, fail_closed=True)
        self.assertFalse(decision["allowed"])
        self.assertTrue(decision["blocked"])
        self.assertEqual(decision["reason"], "target_quality_stale")

    def test_nearest_buy_price_matching_within_tolerance(self):
        candidate = 73159.0
        within_tolerance = candidate * (1 + TARGET_QUALITY_MATCH_TOLERANCE_PCT / 2)
        path = self.snapshot_path(buy_price=within_tolerance)
        snapshot = load_target_quality_snapshot(path, 30, now=self.now)
        matched = match_quality_target(candidate, snapshot["targets"])
        self.assertIsNotNone(matched)
        self.assertAlmostEqual(float(matched["buy_price"]), within_tolerance)


if __name__ == "__main__":
    unittest.main()
