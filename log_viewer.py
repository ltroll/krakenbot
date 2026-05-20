#!/usr/bin/env python3
import argparse
import json
import time
from datetime import datetime, timedelta, timezone
import os
from collections import Counter

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if load_dotenv:
    load_dotenv(dotenv_path=ENV_FILE)

LOG_FILE = os.getenv("TRADE_LOG_FILE", "trade_log.jsonl")
ERROR_WORDS = ("ERROR", "FATAL", "EXCEPTION", "TRACEBACK")


# ---------------- COLORS ----------------

class Colors:
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"


def colorize(event, text):
    if "ERROR" in event or "FATAL" in event:
        return Colors.RED + text + Colors.RESET
    elif "TRADE" in event or "ORDER" in event:
        return Colors.GREEN + text + Colors.RESET
    elif "WARN" in event:
        return Colors.YELLOW + text + Colors.RESET
    else:
        return Colors.BLUE + text + Colors.RESET


# ---------------- UTIL ----------------

def get_log_file(log_file=None):
    return log_file or os.getenv("TRADE_LOG_FILE") or LOG_FILE


def parse_ts(ts):
    try:
        parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def format_log(entry):
    ts = entry.get("ts", "N/A")
    event = entry.get("event", "UNKNOWN")
    message = entry.get("message", "")

    extra = {
        k: v for k, v in entry.items()
        if k not in ["ts", "event", "message"]
    }

    extra_str = " ".join([f"{k}={v}" for k, v in extra.items()])

    base = f"{ts} | {event:<18} | {message} {extra_str}"
    return colorize(event, base)


def load_logs(log_file=None):
    active_log_file = get_log_file(log_file)

    if not os.path.exists(active_log_file):
        raise FileNotFoundError(f"Log file not found: {active_log_file}")

    logs = []

    with open(active_log_file, "r") as f:
        for line in f:
            try:
                logs.append(json.loads(line.strip()))
            except:
                continue

    return logs


# ---------------- FILTERS ----------------

def filter_by_time(logs, cutoff):
    return [
        entry for entry in logs
        if (ts := parse_ts(entry.get("ts"))) and ts >= cutoff
    ]


def filter_by_event(logs, event_name):
    return [
        entry for entry in logs
        if entry.get("event", "").lower() == event_name.lower()
    ]


def is_error_event(entry):
    text = " ".join(
        str(entry.get(key, ""))
        for key in ("event", "message", "reason", "status", "error", "notes")
    ).upper()
    return any(word in text for word in ERROR_WORDS)


def latest_log_timestamp(logs):
    timestamps = [
        parsed
        for entry in logs
        if (parsed := parse_ts(entry.get("ts")))
    ]
    return max(timestamps) if timestamps else None


def summarize_health(logs, now=None, error_window=timedelta(hours=1)):
    now = now or datetime.now(timezone.utc)
    cutoff = now - error_window
    latest_ts = latest_log_timestamp(logs)
    error_count = sum(
        1
        for entry in logs
        if (ts := parse_ts(entry.get("ts")))
        and ts >= cutoff
        and is_error_event(entry)
    )

    return {
        "total_logs": len(logs),
        "latest_ts": latest_ts,
        "last_event_minutes": (
            max(0, int((now - latest_ts).total_seconds() // 60))
            if latest_ts
            else None
        ),
        "errors": error_count,
        "error_window_seconds": int(error_window.total_seconds()),
    }


# ---------------- SUMMARY ----------------

def show_summary(logs):
    print("\n📊 LOG SUMMARY\n")

    total = len(logs)
    events = Counter([log.get("event", "UNKNOWN") for log in logs])

    trades = sum(v for k, v in events.items() if "TRADE" in k or "ORDER" in k)
    errors = sum(v for k, v in events.items() if "ERROR" in k or "FATAL" in k)

    print(f"Total logs:   {total}")
    print(f"Trades:       {trades}")
    print(f"Errors:       {errors}")

    print("\nTop events:")
    for event, count in events.most_common(10):
        print(f"{event:<20} {count}")

    print()


# ---------------- MODES ----------------

def run_tail(event_filter=None, log_file=None):
    active_log_file = get_log_file(log_file)
    print(f"🔴 Live tailing logs from {active_log_file}...\n")

    with open(active_log_file, "r") as f:
        f.seek(0, os.SEEK_END)

        while True:
            line = f.readline()

            if not line:
                time.sleep(0.5)
                continue

            try:
                entry = json.loads(line.strip())

                if event_filter and entry.get("event") != event_filter:
                    continue

                print(format_log(entry))

            except:
                continue


def run_time_filter(hours=None, days=None, event_filter=None, summary=False, log_file=None):
    logs = load_logs(log_file=log_file)

    now = datetime.now(timezone.utc)

    if hours:
        cutoff = now - timedelta(hours=hours)
        logs = filter_by_time(logs, cutoff)
    elif days:
        cutoff = now - timedelta(days=days)
        logs = filter_by_time(logs, cutoff)

    if event_filter:
        logs = filter_by_event(logs, event_filter)

    logs.sort(key=lambda x: x.get("ts", ""))

    if summary:
        show_summary(logs)
        return logs

    print(f"Showing {len(logs)} log entries\n")

    for entry in logs:
        print(format_log(entry))

    return logs


# ---------------- MAIN ----------------

def main():
    parser = argparse.ArgumentParser(description="Trading Bot Log Viewer")

    parser.add_argument("--tail", action="store_true", help="Live tail logs")
    parser.add_argument("--hours", type=int, help="Show last N hours")
    parser.add_argument("--days", type=int, help="Show last N days")
    parser.add_argument("--event", type=str, help="Filter by event name")
    parser.add_argument("--summary", action="store_true", help="Show summary only")

    args = parser.parse_args()

    try:
        if args.tail:
            run_tail(event_filter=args.event)
        else:
            run_time_filter(
                hours=args.hours,
                days=args.days,
                event_filter=args.event,
                summary=args.summary
            )
    except FileNotFoundError as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
