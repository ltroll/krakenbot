#!/usr/bin/env python3
"""Small OLED status display for a Kraken bot host."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


WIDTH = 128
HEIGHT = 32
LABEL_Y = 0
VALUE_Y = 16
REFRESH_SECONDS = float(os.getenv("BOT_DISPLAY_REFRESH_SECONDS", "5"))
ERROR_WINDOW_SECONDS = int(os.getenv("BOT_DISPLAY_ERROR_WINDOW_SECONDS", "3600"))
LOG_TAIL_LINES = int(os.getenv("BOT_DISPLAY_LOG_TAIL_LINES", "300"))

ERROR_WORDS = ("ERROR", "FATAL", "EXCEPTION", "TRACEBACK")


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


def read_log_events() -> list[dict[str, object]]:
    records = []

    for path in log_file_candidates():
        for line in tail_lines(path, LOG_TAIL_LINES):
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = parse_ts(record.get("ts"))
            if ts:
                record["_path"] = str(path)
                records.append(record)

    records.sort(key=lambda item: parse_ts(item.get("ts")) or datetime.min.replace(tzinfo=timezone.utc))
    return records


def is_error_event(record: dict[str, object]) -> bool:
    text = " ".join(
        str(record.get(key, ""))
        for key in ("event", "message", "reason", "status", "error", "notes")
    ).upper()
    return any(word in text for word in ERROR_WORDS)


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


def service_status_text() -> str:
    services = split_csv(getenv("BOT_DISPLAY_SERVICES"))
    if not services:
        return "down"
    return "up" if all(service_state(service) == "active" for service in services) else "down"


def last_event_age_text(records: list[dict[str, object]], now: datetime) -> str:
    if not records:
        return "none"

    latest = records[-1]
    ts = parse_ts(latest.get("ts"))
    if not ts:
        return "unknown"
    age_minutes = max(0, int((now.timestamp() - ts.timestamp()) // 60))
    return f"{age_minutes} minutes"


def recent_error_count(records: list[dict[str, object]], now: datetime) -> int:
    cutoff = now.timestamp() - ERROR_WINDOW_SECONDS
    return sum(
        1
        for record in records
        if (ts := parse_ts(record.get("ts")))
        and ts.timestamp() >= cutoff
        and is_error_event(record)
    )


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


def load_font(image_font):
    font_size = int(getenv("BOT_DISPLAY_FONT_SIZE", "14"))
    font_paths = [
        getenv("BOT_DISPLAY_FONT"),
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation2/LiberationSans-Bold.ttf",
    ]
    for font_path in font_paths:
        if not font_path:
            continue
        try:
            return image_font.truetype(font_path, font_size)
        except OSError:
            continue
    return image_font.load_default()


def screen_lines(page_index: int) -> list[str]:
    now = datetime.now(timezone.utc)
    records = read_log_events()
    pages = [
        ("status:", service_status_text()),
        ("last event:", last_event_age_text(records, now)),
        ("errors:", str(recent_error_count(records, now))),
    ]
    label, value = pages[page_index % len(pages)]
    return [label, value]


def render_stdout(once: bool, page_index: int = 0) -> None:
    while True:
        print("\n".join(screen_lines(page_index)))
        if once:
            return
        page_index += 1
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
    font = load_font(ImageFont)
    page_index = 0

    try:
        while True:
            draw.rectangle((0, 0, display.width, display.height), outline=0, fill=0)
            label, value = screen_lines(page_index)
            draw.text((0, LABEL_Y), fit_text(label, font, display.width), font=font, fill=255)
            draw.text((0, VALUE_Y), fit_text(value, font, display.width), font=font, fill=255)

            display.image(image)
            display.display()
            page_index += 1
            time.sleep(REFRESH_SECONDS)
    finally:
        display.clear()
        display.display()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Display Kraken bot status on an SSD1306 OLED.")
    parser.add_argument("--stdout", action="store_true", help="print the screen text instead of using the OLED")
    parser.add_argument("--once", action="store_true", help="render one screen and exit")
    parser.add_argument("--page", type=int, default=0, help="starting page for stdout preview")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.stdout:
        render_stdout(args.once, args.page)
        return
    render_oled()


if __name__ == "__main__":
    main()
