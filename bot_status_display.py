#!/usr/bin/env python3
"""Small OLED status display for a Kraken bot host."""

from __future__ import annotations

import argparse
import os
import subprocess
import time
from datetime import timedelta
from pathlib import Path

import log_viewer

WIDTH = 128
HEIGHT = 32
LABEL_Y = 0
VALUE_Y = 16
REFRESH_SECONDS = float(os.getenv("BOT_DISPLAY_REFRESH_SECONDS", "5"))
ERROR_WINDOW_SECONDS = int(os.getenv("BOT_DISPLAY_ERROR_WINDOW_SECONDS", "3600"))


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


def load_logs_for_display() -> list[dict[str, object]]:
    logs = []
    for path in log_file_candidates():
        try:
            logs.extend(log_viewer.load_logs(log_file=str(path)))
        except FileNotFoundError:
            continue
    return logs


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


def health_summary() -> dict[str, object]:
    return log_viewer.summarize_health(
        load_logs_for_display(),
        error_window=timedelta(seconds=ERROR_WINDOW_SECONDS),
    )


def last_event_age_text(summary: dict[str, object]) -> str:
    age_minutes = summary.get("last_event_minutes")
    if age_minutes is None:
        return "none"
    return f"{age_minutes} minutes"


def recent_error_count_text(summary: dict[str, object]) -> str:
    return str(summary.get("errors", 0))


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
    summary = health_summary()
    pages = [
        ("status:", service_status_text()),
        ("last event:", last_event_age_text(summary)),
        ("errors:", recent_error_count_text(summary)),
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
