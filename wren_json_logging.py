#!/usr/bin/env python3

"""Emit newline-delimited Wren JSON Logging version 1 records."""

import json
import math
import os
import socket
import sys
import traceback
import warnings
from datetime import date, datetime, timezone


WJL_PREFIX = "wjl_1:"
WJL_VERSION = 1
VALID_SEVERITIES = {
    "info",
    "warning",
    "error",
    "critical",
    "fatal",
}


def utc_now_iso():
    return datetime.now(timezone.utc).isoformat()


def _safe_value(value, depth=0):
    """Return a JSON-safe value while keeping each WJL record on one line."""
    if depth >= 7:
        return str(value)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {
            str(key): _safe_value(item, depth + 1)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_safe_value(item, depth + 1) for item in value]
    return str(value)


def normalize_severity(severity):
    if isinstance(severity, int) and 0 <= severity <= 10:
        return severity
    normalized = str(severity or "info").strip().lower()
    return normalized if normalized in VALID_SEVERITIES else "info"


def severity_for_event(name):
    event_name = str(name or "").upper()
    if any(token in event_name for token in ("FATAL", "CRITICAL")):
        return "critical"
    if any(
        token in event_name
        for token in ("ERROR", "FAILED", "FAILURE", "REJECTED")
    ):
        return "error"
    if any(
        token in event_name
        for token in (
            "WARNING",
            "WARN",
            "CANCEL",
            "EXPIRED",
            "RECONCILED",
            "LOCKED",
        )
    ):
        return "warning"
    return "info"


def build_wjl_payload(
    name,
    *,
    message="",
    severity=None,
    fields=None,
    timestamp=None,
    category="trading",
    product="krakenbot",
    service="range_grid_bot",
    tags=None,
    host_name=None,
    process_id=None,
):
    event_name = str(name or "BOT_MESSAGE").strip() or "BOT_MESSAGE"
    event_name = event_name[:256]
    event_message = str(message or event_name)[:65536]
    normalized_fields = _safe_value(fields or {})
    payload = {
        "name": event_name,
        "message": event_message,
        "timestamp": str(timestamp or utc_now_iso()),
        "severity": normalize_severity(
            severity if severity is not None else severity_for_event(event_name)
        ),
        "category": str(category or "trading")[:128],
        "product": str(product or "krakenbot")[:128],
        "service": str(service or "range_grid_bot")[:128],
        "host": {"name": str(host_name or socket.gethostname())},
        "process": {"pid": int(process_id or os.getpid())},
        "event": {"action": event_name.lower()},
        "tags": [
            str(tag)
            for tag in (tags or ["kraken", "range-grid", "trading-bot"])
        ][:32],
    }
    if normalized_fields:
        payload["fields"] = normalized_fields
    return payload


def wjl_line(name, **kwargs):
    payload = build_wjl_payload(name, **kwargs)
    return WJL_PREFIX + json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    )


def emit_wjl(name, *, stream=None, **kwargs):
    output = stream or sys.stdout
    output.write(wjl_line(name, **kwargs) + "\n")
    output.flush()


def install_wjl_runtime(
    *,
    category="trading",
    product="krakenbot",
    service="range_grid_bot",
    tags=None,
    capture_warnings=True,
):
    """Convert uncaught exceptions and warnings into valid WJL records."""

    common = {
        "category": category,
        "product": product,
        "service": service,
        "tags": tags,
    }

    def exception_hook(exception_type, exception, exception_traceback):
        if issubclass(exception_type, KeyboardInterrupt):
            name = "BOT_INTERRUPTED"
            severity = "info"
        else:
            name = "UNHANDLED_EXCEPTION"
            severity = "critical"
        stack_trace = "".join(
            traceback.format_exception(
                exception_type,
                exception,
                exception_traceback,
            )
        )[:65536]
        emit_wjl(
            name,
            message=str(exception) or exception_type.__name__,
            severity=severity,
            fields={
                "exception.type": exception_type.__name__,
                "exception.stack_trace": stack_trace,
            },
            **common,
        )

    sys.excepthook = exception_hook

    if capture_warnings:
        def warning_hook(message, category, filename, lineno, file=None, line=None):
            emit_wjl(
                "PYTHON_WARNING",
                message=str(message),
                severity="warning",
                fields={
                    "warning.category": category.__name__,
                    "code.filepath": filename,
                    "code.lineno": lineno,
                    "code.line": line,
                },
                **common,
            )

        warnings.showwarning = warning_hook
