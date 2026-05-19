#!/usr/bin/env python3
"""Small OLED status display for a Kraken bot host."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


WIDTH = 128
HEIGHT = 32
LINE_HEIGHT = 8
REFRESH_SECONDS = float(os.getenv("BOT_DISPLAY_REFRESH_SECONDS", "5"))
LOG_WINDOW_SECONDS = int(os.getenv("BOT_DISPLAY_LOG_WINDOW_SECONDS", "3600"))
LOG_TAIL_LINES = int(os.getenv("BOT_DISPLAY_LOG_TAIL_LINES", "300"))

ERROR_WORDS = ("ERROR", "FATAL", "EXCEPTION", "TRACEBACK")
WARNING_WORDS = ("WARN", "WARNING", "STALE", "SKIPPED")


def split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def load_env_file(path: Path) -> dict[str, str]:
    env = {}
    if not path.exists():
        return env

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip("\"'")
    return env


LOCAL_ENV = load_env_file(Path(".env"))


def getenv(name: str, default: str = "") -> str:
    return os.getenv(name) or LOCAL_ENV.get(name, default)


def hostname() -> str:
    return socket.gethostname().split(".")[0]


def local_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            return sock.getsockname()[0]
    except OSError:
        try:
            result = subprocess.run(
                ["hostname", "-I"],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            return result.stdout.split()[0] if result.stdout.split() else "ip unknown"
        except Exception:
            return "ip unknown"


def uptime_text() -> str:
    try:
        uptime_seconds = float(Path("/proc/uptime").read_text().split()[0])
    except Exception:
        return "uptime unknown"

    days, remainder = divmod(int(uptime_seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    if days:
        return f"up {days}d {hours}h"
    if hours:
        return f"up {hours}h {minutes}m"
    return f"up {minutes}m"


def load_text() -> str:
    try:
        one_minute, _, _ = os.getloadavg()
        return f"load {one_minute:.2f}"
    except OSError:
        return "load unknown"


def disk_text() -> str:
    usage = shutil.disk_usage("/")
    used_pct = usage.used / usage.total * 100
    return f"disk {used_pct:.0f}%"


def log_file_candidates() -> list[Path]:
    configured = split_csv(getenv("BOT_DISPLAY_LOG_FILES"))
    if configured:
        return [Path(path) for path in configured]

    env_names = (
        "STATS_TREND_TRADE_LOG_FILE",
        "RANGE_GRID_TRADE_LOG_FILE",
        "SENTIMENT_TRADE_LOG_FILE",
        "TRADE_LOG_FILE",
    )
    paths = [Path(value) for name in env_names if (value := getenv(name))]
    paths.extend(Path(".").glob("*trade_log*.jsonl"))
    paths.extend(Path(".").glob("trade_log.jsonl"))

    seen = set()
    unique_paths = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        unique_paths.append(path)
    return unique_paths


def tail_lines(path: Path, limit: int) -> Iterable[str]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            yield from deque(handle, maxlen=limit)
    except OSError:
        return


def parse_ts(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def read_recent_log_events(now: datetime) -> list[dict[str, object]]:
    records = []
    cutoff = now.timestamp() - LOG_WINDOW_SECONDS

    for path in log_file_candidates():
        for line in tail_lines(path, LOG_TAIL_LINES):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = parse_ts(record.get("ts"))
            if ts and ts.timestamp() >= cutoff:
                record["_path"] = str(path)
                records.append(record)

    records.sort(key=lambda item: parse_ts(item.get("ts")) or datetime.min.replace(tzinfo=timezone.utc))
    return records


def event_severity(record: dict[str, object]) -> str:
    text = " ".join(
        str(record.get(key, ""))
        for key in ("event", "message", "reason", "status", "error", "notes")
    ).upper()
    if any(word in text for word in ERROR_WORDS):
        return "error"
    if any(word in text for word in WARNING_WORDS):
        return "warning"
    return "good"


def service_state(service: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "is-active", service],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except Exception:
        return "unknown"
    return result.stdout.strip() or "unknown"


def health_status(records: list[dict[str, object]]) -> tuple[str, str]:
    services = split_csv(getenv("BOT_DISPLAY_SERVICES"))
    inactive_services = [
        f"{service}:{state}"
        for service in services
        if (state := service_state(service)) not in ("active", "unknown")
    ]
    if inactive_services:
        return "error", inactive_services[0]
    unknown_services = [
        service
        for service in services
        if service_state(service) == "unknown"
    ]
    if unknown_services:
        return "warning", f"{unknown_services[0]} unknown"

    for record in reversed(records):
        severity = event_severity(record)
        if severity == "error":
            return "error", str(record.get("event") or record.get("message") or "log error")

    for record in reversed(records):
        severity = event_severity(record)
        if severity == "warning":
            return "warning", str(record.get("event") or record.get("message") or "log warning")

    return "good", "no recent issues"


def last_event_text(records: list[dict[str, object]], now: datetime) -> str:
    if not records:
        return "last log none"

    latest = records[-1]
    ts = parse_ts(latest.get("ts"))
    if not ts:
        return "last log unknown"
    age_seconds = max(0, int(now.timestamp() - ts.timestamp()))
    if age_seconds < 60:
        age = f"{age_seconds}s"
    elif age_seconds < 3600:
        age = f"{age_seconds // 60}m"
    else:
        age = f"{age_seconds // 3600}h"
    return f"last {age} {latest.get('event', '')}".strip()


def bottom_metrics(records: list[dict[str, object]], now: datetime, reason: str) -> list[str]:
    errors = sum(1 for record in records if event_severity(record) == "error")
    warnings = sum(1 for record in records if event_severity(record) == "warning")
    metrics = [
        reason,
        local_ip(),
        uptime_text(),
        load_text(),
        disk_text(),
        f"err {errors} warn {warnings}",
        last_event_text(records, now),
    ]
    return [metric for metric in metrics if metric]


def text_width(font, text: str) -> float:
    if hasattr(font, "getlength"):
        return font.getlength(text)
    if hasattr(font, "getbbox"):
        left, _, right, _ = font.getbbox(text)
        return right - left
    return font.getsize(text)[0]


def fit_text(text: str, font, max_width: int) -> str:
    if not text:
        return ""
    if text_width(font, text) <= max_width:
        return text
    ellipsis = "..."
    allowed = max_width - text_width(font, ellipsis)
    clipped = ""
    for char in text:
        if text_width(font, clipped + char) > allowed:
            break
        clipped += char
    return clipped + ellipsis


def screen_lines(metric_index: int) -> list[str]:
    now = datetime.now(timezone.utc)
    records = read_recent_log_events(now)
    status, reason = health_status(records)
    metrics = bottom_metrics(records, now, reason)
    metric = metrics[metric_index % len(metrics)] if metrics else reason

    return [
        hostname(),
        "",
        f"status: {status}",
        metric,
    ]


def render_stdout(once: bool) -> None:
    metric_index = 0
    while True:
        print("\n".join(screen_lines(metric_index)))
        if once:
            return
        metric_index += 1
        time.sleep(REFRESH_SECONDS)


def render_oled() -> None:
    import Adafruit_SSD1306
    from PIL import Image, ImageDraw, ImageFont

    display = Adafruit_SSD1306.SSD1306_128_32(rst=None)
    display.begin()
    display.command(0x81)
    display.command(int(getenv("BOT_DISPLAY_BRIGHTNESS", "255")))
    display.clear()
    display.display()

    image = Image.new("1", (display.width, display.height))
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    metric_index = 0

    try:
        while True:
            draw.rectangle((0, 0, display.width, display.height), outline=0, fill=0)
            for row, text in enumerate(screen_lines(metric_index)):
                draw.text((0, row * LINE_HEIGHT), fit_text(text, font, display.width), font=font, fill=255)

            display.image(image)
            display.display()
            metric_index += 1
            time.sleep(REFRESH_SECONDS)
    finally:
        display.clear()
        display.display()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Display Kraken bot status on an SSD1306 OLED.")
    parser.add_argument("--stdout", action="store_true", help="print the screen text instead of using the OLED")
    parser.add_argument("--once", action="store_true", help="render one screen and exit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.stdout:
        render_stdout(args.once)
        return
    render_oled()


if __name__ == "__main__":
    main()
