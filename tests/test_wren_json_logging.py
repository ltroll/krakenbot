import io
import json
import math
import sys
import unittest
import warnings
from unittest.mock import patch

import wren_json_logging as wjl
from wren_json_logging import (
    WJL_PREFIX,
    build_wjl_payload,
    emit_wjl,
    install_wjl_runtime,
    wjl_line,
)


class WrenJsonLoggingTests(unittest.TestCase):
    def test_line_uses_wjl_v1_prefix_and_valid_one_line_json(self):
        line = wjl_line(
            "BUY_ORDER_PLACED",
            message="BUY placed\nwithout a second wire line",
            timestamp="2026-09-22T18:42:10+00:00",
            fields={"price": 80000, "txid": "ABC123"},
            host_name="pibot2",
            process_id=123,
        )

        self.assertTrue(line.startswith(WJL_PREFIX))
        self.assertNotIn("\n", line)
        payload = json.loads(line[len(WJL_PREFIX):])
        self.assertEqual(payload["name"], "BUY_ORDER_PLACED")
        self.assertEqual(payload["severity"], "info")
        self.assertEqual(payload["product"], "krakenbot")
        self.assertEqual(payload["service"], "range_grid_bot")
        self.assertEqual(payload["host"]["name"], "pibot2")
        self.assertEqual(payload["process"]["pid"], 123)
        self.assertEqual(payload["event"]["action"], "buy_order_placed")
        self.assertEqual(payload["fields"]["price"], 80000)

    def test_error_event_gets_error_severity_and_non_finite_values_are_safe(self):
        payload = build_wjl_payload(
            "LOOP_ERROR",
            fields={"bad_number": math.inf, "nested": {"value": math.nan}},
        )

        self.assertEqual(payload["severity"], "error")
        self.assertIsNone(payload["fields"]["bad_number"])
        self.assertIsNone(payload["fields"]["nested"]["value"])

    def test_emit_wjl_writes_exactly_one_terminated_record(self):
        output = io.StringIO()

        emit_wjl(
            "BOT_START",
            message="Range grid bot starting",
            stream=output,
            host_name="pibot2",
            process_id=456,
        )

        emitted = output.getvalue()
        self.assertEqual(emitted.count("\n"), 1)
        self.assertTrue(emitted.startswith(WJL_PREFIX))
        self.assertTrue(emitted.endswith("\n"))

    def test_runtime_hooks_encode_warnings_and_uncaught_exceptions(self):
        original_exception_hook = sys.excepthook
        original_warning_hook = warnings.showwarning
        try:
            with patch.object(wjl, "emit_wjl") as mocked_emit:
                install_wjl_runtime()
                warnings.showwarning(
                    UserWarning("careful"),
                    UserWarning,
                    "bot.py",
                    42,
                )
                sys.excepthook(RuntimeError, RuntimeError("boom"), None)

            self.assertEqual(mocked_emit.call_args_list[0].args[0], "PYTHON_WARNING")
            self.assertEqual(
                mocked_emit.call_args_list[1].args[0],
                "UNHANDLED_EXCEPTION",
            )
            self.assertEqual(
                mocked_emit.call_args_list[1].kwargs["severity"],
                "critical",
            )
        finally:
            sys.excepthook = original_exception_hook
            warnings.showwarning = original_warning_hook


if __name__ == "__main__":
    unittest.main()
