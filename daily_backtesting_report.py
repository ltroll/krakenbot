#!/usr/bin/env python3
"""Run daily bot backtests and collect the last 24 hours of bot logs."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional


DEFAULT_REPORT_DIR = "/var/www/html/bot/backtesting_reports"
DEFAULT_BACKTEST_DIR = "/home/ben/sentiment_engine"
DEFAULT_POLICY_OUTPUT = "/var/www/html/bot/bot_policy_backtest.json"
DEFAULT_REPLAY_OUTPUT = "/var/www/html/bot/bot_replay_backtest.json"
DEFAULT_HOURS = 24
ISO_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\S+)")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
KV_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)=([^ ]+)")


@dataclass
class BacktestRun:
    name: str
    command: list[str]
    cwd: Path
    returncode: Optional[int] = None
    stdout: str = ""
    stderr: str = ""
    error: Optional[str] = None


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_value(*names: str, default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def parse_args() -> argparse.Namespace:
    load_dotenv(Path(".env"))

    parser = argparse.ArgumentParser(
        description=(
            "Run policy/replay backtests, summarize their JSON output, and write "
            "one daily report with the last N hours of bot logs."
        )
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=float(env_value("BACKTEST_REPORT_HOURS", default=str(DEFAULT_HOURS))),
        help="Lookback window for backtests and logs. Default: 24.",
    )
    parser.add_argument(
        "--report-dir",
        default=env_value("BACKTEST_REPORT_DIR", default=DEFAULT_REPORT_DIR),
        help=f"Directory for daily report files. Default: {DEFAULT_REPORT_DIR}",
    )
    parser.add_argument(
        "--backtest-dir",
        default=env_value("BACKTEST_SCRIPT_DIR", default=DEFAULT_BACKTEST_DIR),
        help=f"Directory containing bot_policy_backtest.py and bot_replay_backtest.py. Default: {DEFAULT_BACKTEST_DIR}",
    )
    parser.add_argument(
        "--python",
        default=env_value("BACKTEST_PYTHON", default=sys.executable),
        help="Python executable for the backtest scripts.",
    )
    parser.add_argument(
        "--policy-script",
        default=env_value("BOT_POLICY_BACKTEST_SCRIPT", default="bot_policy_backtest.py"),
        help="Policy backtest script name or absolute path.",
    )
    parser.add_argument(
        "--replay-script",
        default=env_value("BOT_REPLAY_BACKTEST_SCRIPT", default="bot_replay_backtest.py"),
        help="Replay backtest script name or absolute path.",
    )
    parser.add_argument(
        "--policy-output",
        default=env_value("BOT_POLICY_BACKTEST_OUTPUT_FILE", default=DEFAULT_POLICY_OUTPUT),
        help=f"Policy backtest JSON output path. Default: {DEFAULT_POLICY_OUTPUT}",
    )
    parser.add_argument(
        "--policy-url",
        default=env_value("BOT_POLICY_BACKTEST_URL", default=""),
        help="Optional URL to read the policy backtest JSON from for the report.",
    )
    parser.add_argument(
        "--replay-output",
        default=env_value("BOT_REPLAY_BACKTEST_OUTPUT_FILE", default=DEFAULT_REPLAY_OUTPUT),
        help=f"Replay backtest JSON output path. Default: {DEFAULT_REPLAY_OUTPUT}",
    )
    parser.add_argument(
        "--replay-url",
        default=env_value("BOT_REPLAY_BACKTEST_URL", default=""),
        help="Optional URL to read the replay backtest JSON from for the report.",
    )
    parser.add_argument(
        "--log-file",
        default=env_value("SENTIMENT_TRADE_LOG_FILE", "TRADE_LOG_FILE", default="trade_log.jsonl"),
        help="Bot trading log file to include. Defaults to SENTIMENT_TRADE_LOG_FILE, TRADE_LOG_FILE, then trade_log.jsonl.",
    )
    parser.add_argument(
        "--take-profit-pct",
        type=float,
        default=float(env_value("BACKTEST_TAKE_PROFIT_PCT", default="0.5")),
    )
    parser.add_argument(
        "--stop-loss-pct",
        type=float,
        default=float(env_value("BACKTEST_STOP_LOSS_PCT", default="0.5")),
    )
    parser.add_argument(
        "--max-hold-hours",
        type=float,
        default=float(env_value("BACKTEST_MAX_HOLD_HOURS", default="24")),
    )
    parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=int(env_value("BACKTEST_COOLDOWN_MINUTES", default="60")),
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        default=float(env_value("BACKTEST_FEE_BPS", default="0")),
    )
    parser.add_argument(
        "--entry-wait-hours",
        type=float,
        default=float(env_value("BACKTEST_ENTRY_WAIT_HOURS", default="4")),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(env_value("BACKTEST_REPORT_TIMEOUT_SECONDS", default="900")),
    )
    parser.add_argument(
        "--no-raw-log",
        action="store_true",
        default=not env_bool("BACKTEST_REPORT_INCLUDE_RAW_LOG", True),
        help="Summarize log events without embedding raw log lines.",
    )
    parser.add_argument(
        "--skip-backtests",
        action="store_true",
        default=env_bool("BACKTEST_REPORT_SKIP_BACKTESTS", False),
        help="Only collect logs and summarize existing JSON backtest outputs.",
    )
    return parser.parse_args()


def resolve_script(backtest_dir: Path, script: str) -> Path:
    script_path = Path(script)
    if script_path.is_absolute():
        return script_path
    return backtest_dir / script_path


def run_command(name: str, command: list[str], cwd: Path, timeout: int) -> BacktestRun:
    run = BacktestRun(name=name, command=command, cwd=cwd)
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        run.returncode = completed.returncode
        run.stdout = completed.stdout.strip()
        run.stderr = completed.stderr.strip()
    except Exception as exc:  # Keep report generation alive if a producer fails.
        run.error = str(exc)
    return run


def build_backtest_runs(args: argparse.Namespace) -> list[BacktestRun]:
    backtest_dir = Path(args.backtest_dir)
    policy_script = resolve_script(backtest_dir, args.policy_script)
    replay_script = resolve_script(backtest_dir, args.replay_script)
    common = [
        "--hours",
        str(args.hours),
        "--take-profit-pct",
        str(args.take_profit_pct),
        "--stop-loss-pct",
        str(args.stop_loss_pct),
        "--max-hold-hours",
        str(args.max_hold_hours),
        "--cooldown-minutes",
        str(args.cooldown_minutes),
        "--fee-bps",
        str(args.fee_bps),
    ]
    commands = [
        (
            "policy",
            [
                args.python,
                str(policy_script),
                *common,
                str(Path(args.policy_output)),
            ],
        ),
        (
            "replay",
            [
                args.python,
                str(replay_script),
                "--entry-wait-hours",
                str(args.entry_wait_hours),
                *common,
                str(Path(args.replay_output)),
            ],
        ),
    ]
    return [
        run_command(name, command, backtest_dir, args.timeout_seconds)
        for name, command in commands
    ]


def parse_timestamp(value: Any) -> Optional[datetime]:
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_log_line(line: str) -> tuple[Optional[datetime], Optional[dict[str, Any]]]:
    stripped = clean_log_line(line)
    if not stripped:
        return None, None
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        match = ISO_PREFIX_RE.match(stripped)
        parsed_at = parse_timestamp(match.group(1)) if match else None
        payload = None
        parts = stripped.split("|", 2)
        if len(parts) >= 2:
            payload = {"ts": parts[0].strip(), "event": parts[1].strip()}
            if len(parts) == 3:
                payload.update({key: value for key, value in KV_RE.findall(parts[2])})
        return parsed_at, payload
    return parse_timestamp(payload.get("ts")), payload


def clean_log_line(line: str) -> str:
    return ANSI_RE.sub("", line).strip()


def collect_log_lines(log_file: Path, since: datetime) -> tuple[list[str], Counter[str], Counter[str], Counter[str]]:
    lines: list[str] = []
    event_counts: Counter[str] = Counter()
    decision_reasons: Counter[str] = Counter()
    trade_events: Counter[str] = Counter()

    if not log_file.exists():
        return lines, event_counts, decision_reasons, trade_events

    with log_file.open("r", encoding="utf-8", errors="replace") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            ts, payload = parse_log_line(line)
            if ts is None or ts < since:
                continue
            lines.append(clean_log_line(line))
            if not payload:
                continue
            event = str(payload.get("event") or "UNKNOWN")
            event_counts[event] += 1
            if event == "TRADE_DECISION":
                decision_reasons[str(payload.get("reason") or "unknown")] += 1
            if event in {
                "BUY_LIMIT_ORDER_PLACED",
                "SELL_LIMIT_ORDER_PLACED",
                "MARKET_BUY_PLACED",
                "BUYNOW",
                "BUY_FILLED",
                "SELL_FILLED",
            }:
                trade_events[event] += 1

    return lines, event_counts, decision_reasons, trade_events


def load_json_path(path: Path) -> Optional[dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_json_url(url: str, timeout: int) -> Optional[dict[str, Any]]:
    if not url:
        return None
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except (OSError, urllib.error.URLError, json.JSONDecodeError):
        return None


def load_json_source(url: str, path: Path, timeout: int) -> tuple[Optional[dict[str, Any]], str]:
    if url:
        payload = load_json_url(url, timeout)
        if payload:
            return payload, url
    return load_json_path(path), str(path)


def fmt_pct(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{value:.4f}%"
    return "n/a"


def fmt_count(value: Any) -> str:
    if isinstance(value, (int, float)):
        return str(value)
    return "n/a"


def strategy_rows(payload: Optional[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    if not payload:
        return []
    bot_outputs = payload.get("bot_outputs")
    if isinstance(bot_outputs, dict) and bot_outputs:
        return [(str(name), summary) for name, summary in bot_outputs.items() if isinstance(summary, dict)]
    strategies = payload.get("strategies")
    rows: list[tuple[str, dict[str, Any]]] = []
    if isinstance(strategies, dict):
        for name, strategy in strategies.items():
            if isinstance(strategy, dict) and isinstance(strategy.get("summary"), dict):
                rows.append((str(name), strategy["summary"]))
    return rows


def render_backtest_summary(title: str, source: str, payload: Optional[dict[str, Any]]) -> list[str]:
    lines = [f"### {title}", "", f"JSON: `{source}`"]
    if not payload:
        lines.extend(["", "Could not read or parse this backtest JSON.", ""])
        return lines

    lines.extend(
        [
            f"timestamp: `{payload.get('timestamp', 'n/a')}`",
            f"since: `{payload.get('since', 'n/a')}`",
            f"signals tested: `{payload.get('signal_count', payload.get('signals_tested', 'n/a'))}`",
            "",
        ]
    )

    rows = strategy_rows(payload)
    if not rows:
        lines.extend(["No strategy summaries found.", ""])
        return lines

    lines.extend(
        [
            "| strategy | trades | win rate | avg net | total net | max drawdown | take/stop/timeout | candidates | blocked | not filled |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for name, summary in rows:
        wins = summary.get("win_rate")
        win_rate = f"{wins * 100:.1f}%" if isinstance(wins, (int, float)) else "n/a"
        lines.append(
            "| "
            + " | ".join(
                [
                    name,
                    fmt_count(summary.get("trades")),
                    win_rate,
                    fmt_pct(summary.get("avg_net_return_pct")),
                    fmt_pct(summary.get("total_net_return_pct")),
                    fmt_pct(summary.get("max_drawdown_pct")),
                    f"{fmt_count(summary.get('take_profit_count'))}/{fmt_count(summary.get('stop_loss_count'))}/{fmt_count(summary.get('timeout_count'))}",
                    fmt_count(summary.get("candidate_signals")),
                    fmt_count(summary.get("blocked_by_sentiment")),
                    fmt_count(summary.get("not_filled")),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def render_command(run: BacktestRun) -> list[str]:
    status = "error" if run.error else str(run.returncode)
    lines = [
        f"### {run.name} command",
        "",
        f"cwd: `{run.cwd}`",
        f"exit: `{status}`",
        "",
        "```bash",
        " ".join(run.command),
        "```",
        "",
    ]
    if run.error:
        lines.extend(["error:", "", "```text", run.error, "```", ""])
    if run.stdout:
        lines.extend(["stdout:", "", "```text", truncate(run.stdout, 4000), "```", ""])
    if run.stderr:
        lines.extend(["stderr:", "", "```text", truncate(run.stderr, 4000), "```", ""])
    return lines


def truncate(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "\n... truncated ..."


def render_counter(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["none"]
    return [f"- {name}: {count}" for name, count in counter.most_common()]


def write_report(args: argparse.Namespace, runs: list[BacktestRun]) -> Path:
    now = datetime.now(timezone.utc)
    since = now - timedelta(hours=args.hours)
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"backtesting_report_{now.date().isoformat()}.md"

    log_file = Path(args.log_file)
    if not log_file.is_absolute():
        log_file = Path.cwd() / log_file

    log_lines, event_counts, decision_reasons, trade_events = collect_log_lines(log_file, since)
    policy_json, policy_source = load_json_source(
        args.policy_url,
        Path(args.policy_output),
        args.timeout_seconds,
    )
    replay_json, replay_source = load_json_source(
        args.replay_url,
        Path(args.replay_output),
        args.timeout_seconds,
    )

    report: list[str] = [
        f"# Bot Backtesting Report - {now.date().isoformat()}",
        "",
        f"generated_at_utc: `{now.isoformat()}`",
        f"window_start_utc: `{since.isoformat()}`",
        f"window_hours: `{args.hours:g}`",
        "",
        "## Backtest Summaries",
        "",
    ]
    report.extend(render_backtest_summary("Policy Backtest", policy_source, policy_json))
    report.extend(render_backtest_summary("Replay Backtest", replay_source, replay_json))

    report.extend(["## Backtest Commands", ""])
    for run in runs:
        report.extend(render_command(run))

    report.extend(
        [
            "## Bot Log Summary",
            "",
            f"log_file: `{log_file}`",
            f"lines_in_window: `{len(log_lines)}`",
            "",
            "### Events",
            "",
            *render_counter(event_counts),
            "",
            "### Trade Decisions",
            "",
            *render_counter(decision_reasons),
            "",
            "### Trade Events",
            "",
            *render_counter(trade_events),
            "",
        ]
    )

    if not args.no_raw_log:
        report.extend(["## Raw Bot Logs", "", "```jsonl"])
        report.extend(log_lines)
        report.extend(["```", ""])

    temp_path = report_path.with_suffix(".tmp")
    temp_path.write_text("\n".join(report), encoding="utf-8")
    temp_path.replace(report_path)
    return report_path


def main() -> int:
    args = parse_args()
    runs: list[BacktestRun] = []
    if not args.skip_backtests:
        runs = build_backtest_runs(args)
    report_path = write_report(args, runs)
    failed = [run for run in runs if run.error or run.returncode != 0]
    print(f"Wrote report: {report_path}")
    if failed:
        print(f"Backtest command failures: {', '.join(run.name for run in failed)}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
