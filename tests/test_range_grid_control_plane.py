import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import range_grid_control_plane as control_plane


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, ticker, hourly, daily):
        self.responses = [ticker, hourly, daily]

    def get(self, url, params=None, timeout=None):
        return FakeResponse(self.responses.pop(0))


def ohlc_payload(rows):
    return {"error": [], "result": {"XXBTZUSD": rows, "last": 0}}


class RangeGridControlPlaneTests(unittest.TestCase):
    def test_host_loopback_detection(self):
        self.assertTrue(control_plane.host_is_loopback("127.0.0.1"))
        self.assertTrue(control_plane.host_is_loopback("::1"))
        self.assertTrue(control_plane.host_is_loopback("localhost"))
        self.assertFalse(control_plane.host_is_loopback("0.0.0.0"))
        self.assertFalse(control_plane.host_is_loopback("192.168.1.20"))

    def test_ohlc_statistics_use_close_average_and_low_high(self):
        now = datetime(2026, 9, 19, 18, tzinfo=timezone.utc)
        rows = []
        for offset, close in enumerate((100.0, 110.0, 120.0)):
            timestamp = int((now - timedelta(hours=2 - offset)).timestamp())
            rows.append([timestamp, close - 1, close + 2, close - 3, close])

        candles = control_plane.parse_ohlc_candles(ohlc_payload(rows))
        summary = control_plane.summarize_candles(
            candles,
            now - timedelta(hours=3),
        )

        self.assertEqual(summary["average"], 110.0)
        self.assertEqual(summary["low"], 97.0)
        self.assertEqual(summary["high"], 122.0)
        self.assertEqual(summary["samples"], 3)

    def test_market_snapshot_builds_requested_periods(self):
        now = datetime(2026, 9, 19, 18, tzinfo=timezone.utc)
        ticker = {
            "error": [],
            "result": {"XXBTZUSD": {"c": ["81234.5", "1"]}},
        }
        hourly_rows = [
            [
                int((now - timedelta(hours=offset)).timestamp()),
                "80000", "82000", "79000", str(81000 + offset),
            ]
            for offset in range(24, -1, -1)
        ]
        daily_rows = [
            [
                int((now - timedelta(days=offset)).timestamp()),
                "70000", "83000", "68000", str(75000 + offset),
            ]
            for offset in range(210, -1, -1)
        ]
        market = control_plane.KrakenMarketData(
            session=FakeSession(
                ticker,
                ohlc_payload(hourly_rows),
                ohlc_payload(daily_rows),
            ),
            cache_seconds=60,
        )

        with patch.object(control_plane, "utc_now", return_value=now):
            snapshot = market.snapshot()

        self.assertEqual(snapshot["current_price"], 81234.5)
        self.assertEqual(set(snapshot["periods"]), {"24h", "7d", "50d", "200d"})
        self.assertFalse(snapshot["stale"])
        self.assertLessEqual(len(snapshot["history"]["daily"]), 200)


if __name__ == "__main__":
    unittest.main()
