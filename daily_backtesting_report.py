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
DEFAULT_FORWARD_OUTPUT = "/var/www/html/bot/signal_forward_backtest.json"
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


@dataclass
class LogSummary:
    lines: list[str]
    event_counts: Counter[str]
    decision_reasons: Counter[str]
    trade_events: Counter[str]
    signal_statuses: Counter[str]
    signal_ages: list[float]
    stale_records: list[dict[str, Any]]
    longest_stale_streak: int
    contrarian_candidates: Counter[str]


@dataclass
class ReportMetrics:
    date: str
    live_trade_events: int
    replay_sentiment_trades: Optional[float]
    replay_sentiment_return_pct: Optional[float]
    replay_price_only_return_pct: Optional[float]
    policy_sentiment_trades: Optional[float]
    policy_sentiment_return_pct: Optional[float]
    error_events: int


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
        "--forward-output",
        default=env_value(
            "SIGNAL_FORWARD_BACKTEST_OUTPUT_FILE",
            "BOT_SIGNAL_FORWARD_BACKTEST_OUTPUT_FILE",
            default=DEFAULT_FORWARD_OUTPUT,
        ),
        help=f"Signal forward-return backtest JSON output path. Default: {DEFAULT_FORWARD_OUTPUT}",
    )
    parser.add_argument(
        "--forward-url",
        default=env_value(
            "SIGNAL_FORWARD_BACKTEST_URL",
            "BOT_SIGNAL_FORWARD_BACKTEST_URL",
            default="",
        ),
        help="Optional URL to read the signal forward-return backtest JSON from for the report.",
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


def collect_log_lines(log_file: Path, since: datetime) -> LogSummary:
    lines: list[str] = []
    event_counts: Counter[str] = Counter()
    decision_reasons: Counter[str] = Counter()
    trade_events: Counter[str] = Counter()
    signal_statuses: Counter[str] = Counter()
    signal_ages: list[float] = []
    stale_records: list[dict[str, Any]] = []
    contrarian_candidates: Counter[str] = Counter()
    current_stale_streak = 0
    longest_stale_streak = 0

    if not log_file.exists():
        return LogSummary(
            lines,
            event_counts,
            decision_reasons,
            trade_events,
            signal_statuses,
            signal_ages,
            stale_records,
            longest_stale_streak,
            contrarian_candidates,
        )

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
            if event == "SIGNAL_UPDATE":
                raw_status = str(payload.get("signal_status") or "unknown")
                freshness_state = str(payload.get("signal_freshness_state") or "")
                status = freshness_state or raw_status
                signal_statuses[status] += 1
                age = number_value(payload.get("signal_age_minutes"))
                if age is not None:
                    signal_ages.append(age)
                if status not in ("fresh", "warn", "unknown"):
                    current_stale_streak += 1
                    longest_stale_streak = max(longest_stale_streak, current_stale_streak)
                    stale_records.append(
                        {
                            "ts": payload.get("ts"),
                            "status": status,
                            "raw_status": raw_status,
                            "age": age,
                            "processed_at": payload.get("processed_at"),
                        }
                    )
                else:
                    current_stale_streak = 0
                if str(payload.get("action_recommendation") or "") == "watch_only":
                    execution_signal = number_value(payload.get("execution_signal"))
                    liquidity_risk = number_value(payload.get("liquidity_risk"))
                    confidence = number_value(payload.get("confidence"))
                    range_position = number_value(payload.get("range_position_24h"))
                    contrarian_candidates["watch_only_signals"] += 1
                    if execution_signal is not None and execution_signal < 0:
                        contrarian_candidates["negative_watch_only"] += 1
                    if status == "fresh":
                        contrarian_candidates["fresh_watch_only"] += 1
                    if liquidity_risk is not None and liquidity_risk <= 0.45:
                        contrarian_candidates["liquidity_acceptable"] += 1
                    if confidence is not None and confidence >= 0.45:
                        contrarian_candidates["confidence_acceptable"] += 1
                    if range_position is not None and range_position <= 0.35:
                        contrarian_candidates["low_range_position"] += 1
                    if (
                        execution_signal is not None
                        and execution_signal < 0
                        and status == "fresh"
                        and (liquidity_risk is None or liquidity_risk <= 0.45)
                        and (confidence is None or confidence >= 0.45)
                        and (range_position is None or range_position <= 0.35)
                    ):
                        contrarian_candidates["contrarian_buy_watch_candidates"] += 1
            if event in {
                "BUY_LIMIT_ORDER_PLACED",
                "SELL_LIMIT_ORDER_PLACED",
                "MARKET_BUY_PLACED",
                "BUYNOW",
                "BUY_FILLED",
                "SELL_FILLED",
            }:
                trade_events[event] += 1

    return LogSummary(
        lines,
        event_counts,
        decision_reasons,
        trade_events,
        signal_statuses,
        signal_ages,
        stale_records,
        longest_stale_streak,
        contrarian_candidates,
    )


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
        return str(int(value)) if float(value).is_integer() else str(value)
    return "n/a"


def fmt_minutes(value: Any) -> str:
    if isinstance(value, (int, float)):
        return f"{value:.2f} min"
    return "n/a"


def number_value(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


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


def strategy_summary(payload: Optional[dict[str, Any]], *names: str) -> dict[str, Any]:
    rows = dict(strategy_rows(payload))
    for name in names:
        summary = rows.get(name)
        if isinstance(summary, dict):
            return summary
    return {}


def trade_event_total(trade_events: Counter[str]) -> int:
    return sum(
        trade_events.get(name, 0)
        for name in (
            "BUY_LIMIT_ORDER_PLACED",
            "SELL_LIMIT_ORDER_PLACED",
            "MARKET_BUY_PLACED",
            "BUYNOW",
            "BUY_FILLED",
            "SELL_FILLED",
        )
    )


def error_group_counts(event_counts: Counter[str], signal_statuses: Counter[str]) -> dict[str, int]:
    tracker = (
        event_counts.get("ORDER_TRACKER_CHECKIN_ERROR", 0)
        + event_counts.get("ORDER_TRACKER_ERROR", 0)
    )
    kraken = event_counts.get("KRAKEN_EXCEPTION", 0)
    sentiment = event_counts.get("SENTIMENT_ERROR", 0)
    market = event_counts.get("PRICE_ERROR", 0) + event_counts.get("ORDERBOOK_ERROR", 0)
    backtest = event_counts.get("BACKTEST_FETCH_ERROR", 0)
    known = {
        "ORDER_TRACKER_CHECKIN_ERROR",
        "ORDER_TRACKER_ERROR",
        "KRAKEN_EXCEPTION",
        "SENTIMENT_ERROR",
        "PRICE_ERROR",
        "ORDERBOOK_ERROR",
        "BACKTEST_FETCH_ERROR",
    }
    other = sum(
        count
        for event, count in event_counts.items()
        if (event.endswith("ERROR") or event.endswith("EXCEPTION")) and event not in known
    )
    stale = sum(
        count
        for status, count in signal_statuses.items()
        if status not in ("fresh", "unknown")
    )
    return {
        "tracker_errors": tracker,
        "kraken_exceptions": kraken,
        "sentiment_errors": sentiment,
        "market_data_errors": market,
        "backtest_fetch_errors": backtest,
        "other_errors": other,
        "signal_stale_events": stale,
    }


def current_metrics(
    date_label: str,
    policy_json: Optional[dict[str, Any]],
    replay_json: Optional[dict[str, Any]],
    log_summary: LogSummary,
) -> ReportMetrics:
    policy = strategy_summary(policy_json, "sentiment_long_policy", "sentiment_policy")
    replay = strategy_summary(replay_json, "with_sentiment_policy")
    price_only = strategy_summary(replay_json, "price_target_only")
    errors = error_group_counts(log_summary.event_counts, log_summary.signal_statuses)
    return ReportMetrics(
        date=date_label,
        live_trade_events=trade_event_total(log_summary.trade_events),
        replay_sentiment_trades=number_value(replay.get("trades")),
        replay_sentiment_return_pct=number_value(replay.get("total_net_return_pct")),
        replay_price_only_return_pct=number_value(price_only.get("total_net_return_pct")),
        policy_sentiment_trades=number_value(policy.get("trades")),
        policy_sentiment_return_pct=number_value(policy.get("total_net_return_pct")),
        error_events=sum(errors.values()),
    )


def render_health_summary(
    event_counts: Counter[str],
    signal_statuses: Counter[str],
) -> list[str]:
    errors = error_group_counts(event_counts, signal_statuses)
    network_errors = (
        errors["tracker_errors"]
        + errors["kraken_exceptions"]
        + errors["sentiment_errors"]
        + errors["market_data_errors"]
        + errors["backtest_fetch_errors"]
        + errors["other_errors"]
    )
    return [
        "## Health Summary",
        "",
        f"- Network/API errors: {network_errors}",
        f"- Kraken exceptions: {errors['kraken_exceptions']}",
        f"- Tracker errors: {errors['tracker_errors']}",
        f"- Sentiment fetch errors: {errors['sentiment_errors']}",
        f"- Market data errors: {errors['market_data_errors']}",
        f"- Backtest fetch errors: {errors['backtest_fetch_errors']}",
        f"- Other errors: {errors['other_errors']}",
        f"- Signal stale/non-fresh events: {errors['signal_stale_events']}",
        "",
    ]


def render_signal_freshness_summary(log_summary: LogSummary) -> list[str]:
    stale_count = len(log_summary.stale_records)
    if not log_summary.signal_ages and not stale_count:
        return [
            "## Signal Freshness",
            "",
            "No signal freshness data was available in the bot log window.",
            "",
        ]

    avg_age = (
        sum(log_summary.signal_ages) / len(log_summary.signal_ages)
        if log_summary.signal_ages
        else None
    )
    max_age = max(log_summary.signal_ages) if log_summary.signal_ages else None
    first_stale = log_summary.stale_records[0] if stale_count else {}
    last_stale = log_summary.stale_records[-1] if stale_count else {}

    lines = [
        "## Signal Freshness",
        "",
        f"- Signal updates with age data: {len(log_summary.signal_ages)}",
        f"- Stale/non-fresh signal updates: {stale_count}",
        f"- Avg signal age: {fmt_minutes(avg_age)}",
        f"- Max signal age: {fmt_minutes(max_age)}",
        f"- Longest stale streak: {log_summary.longest_stale_streak}",
    ]
    if stale_count:
        lines.extend(
            [
                f"- First stale event: `{first_stale.get('ts', 'n/a')}` status=`{first_stale.get('status', 'n/a')}` raw_status=`{first_stale.get('raw_status', 'n/a')}` age={fmt_minutes(first_stale.get('age'))}",
                f"- Last stale event: `{last_stale.get('ts', 'n/a')}` status=`{last_stale.get('status', 'n/a')}` raw_status=`{last_stale.get('raw_status', 'n/a')}` age={fmt_minutes(last_stale.get('age'))}",
            ]
        )
    lines.append("")
    return lines


def render_verdict(metrics: ReportMetrics) -> list[str]:
    notes: list[str] = []
    trading_verdict = "PASS"
    health_verdict = "PASS"
    tuning_verdict = "PASS"
    live = metrics.live_trade_events
    replay_trades = metrics.replay_sentiment_trades
    price_return = metrics.replay_price_only_return_pct
    policy_return = metrics.policy_sentiment_return_pct

    if metrics.error_events:
        health_verdict = "WARN"
        notes.append(f"{metrics.error_events} health/error events were logged.")

    if metrics.replay_sentiment_trades is None and metrics.policy_sentiment_trades is None:
        trading_verdict = "WARN"
        notes.append("Backtest summaries were unavailable or could not be parsed.")

    if replay_trades is not None:
        if live == 0 and replay_trades == 0:
            if isinstance(price_return, (int, float)) and price_return < 0:
                notes.append("Live bot avoided a losing price-target replay day.")
            else:
                notes.append("Live bot matched replay: no sentiment-policy trades expected.")
        elif live == 0 and replay_trades > 0:
            trading_verdict = "WARN"
            notes.append(
                "Replay expected sentiment-policy trades, but live bot logged no trade events."
            )
        elif live > 0 and replay_trades == 0:
            trading_verdict = "WARN"
            notes.append(
                "Live bot logged trade events while replay expected no sentiment-policy trades."
            )
        else:
            notes.append("Live bot and replay both showed trading activity.")

    if isinstance(policy_return, (int, float)) and policy_return < 0:
        tuning_verdict = "WATCH"
        notes.append("Policy backtest trade outcome was negative.")
    if isinstance(price_return, (int, float)) and price_return < 0:
        tuning_verdict = "WATCH"
        notes.append("Price-target replay baseline was negative.")
    if (
        isinstance(price_return, (int, float))
        and price_return > 0
        and replay_trades == 0
        and live == 0
    ):
        tuning_verdict = "WATCH"
        notes.append("No-trade policy had positive price-target opportunity cost.")

    if not notes:
        notes.append("No obvious mismatch or health issue detected.")

    overall = "WARN" if "WARN" in (trading_verdict, health_verdict) else tuning_verdict
    return [
        "## Verdict",
        "",
        f"Overall Verdict: **{overall}**",
        f"Trading Verdict: **{trading_verdict}**",
        f"Health Verdict: **{health_verdict}**",
        f"Tuning Verdict: **{tuning_verdict}**",
        "",
        *[f"- {note}" for note in notes],
        "",
    ]


def parse_float_text(value: str) -> Optional[float]:
    value = value.strip()
    if value in ("", "n/a"):
        return None
    value = value.rstrip("%")
    try:
        return float(value)
    except ValueError:
        return None


def parse_int_text(value: str) -> int:
    parsed = parse_float_text(value)
    return int(parsed) if parsed is not None else 0


def parse_strategy_table_row(text: str, strategy: str) -> dict[str, Any]:
    for line in text.splitlines():
        if not line.startswith(f"| {strategy} |"):
            continue
        cells = [cell.strip() for cell in line.strip("|").split("|")]
        if len(cells) < 6:
            return {}
        return {
            "trades": parse_float_text(cells[1]),
            "total_net_return_pct": parse_float_text(cells[4]),
        }
    return {}


def parse_event_count(text: str, event_name: str) -> int:
    match = re.search(rf"^- {re.escape(event_name)}: (\d+)$", text, re.MULTILINE)
    return int(match.group(1)) if match else 0


def parse_old_report(path: Path) -> Optional[ReportMetrics]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None

    title = re.search(r"^# Bot Backtesting Report - ([0-9-]+)", text, re.MULTILINE)
    if not title:
        return None

    replay = parse_strategy_table_row(text, "with_sentiment_policy")
    price_only = parse_strategy_table_row(text, "price_target_only")
    policy = parse_strategy_table_row(text, "sentiment_long_policy")
    live_trade_events = sum(
        parse_event_count(text, event)
        for event in (
            "BUY_LIMIT_ORDER_PLACED",
            "SELL_LIMIT_ORDER_PLACED",
            "MARKET_BUY_PLACED",
            "BUYNOW",
            "BUY_FILLED",
            "SELL_FILLED",
        )
    )
    error_events = sum(
        parse_event_count(text, event)
        for event in (
            "ORDER_TRACKER_CHECKIN_ERROR",
            "ORDER_TRACKER_ERROR",
            "KRAKEN_EXCEPTION",
            "SENTIMENT_ERROR",
            "PRICE_ERROR",
            "ORDERBOOK_ERROR",
            "BACKTEST_FETCH_ERROR",
        )
    )

    return ReportMetrics(
        date=title.group(1),
        live_trade_events=live_trade_events,
        replay_sentiment_trades=number_value(replay.get("trades")),
        replay_sentiment_return_pct=number_value(replay.get("total_net_return_pct")),
        replay_price_only_return_pct=number_value(price_only.get("total_net_return_pct")),
        policy_sentiment_trades=number_value(policy.get("trades")),
        policy_sentiment_return_pct=number_value(policy.get("total_net_return_pct")),
        error_events=error_events,
    )


def render_rolling_summary(
    report_dir: Path,
    report_path: Path,
    current: ReportMetrics,
    days: int = 7,
) -> list[str]:
    rows: dict[str, ReportMetrics] = {}
    for path in report_dir.glob("backtesting_report_*.md"):
        if path == report_path:
            continue
        parsed = parse_old_report(path)
        if parsed:
            rows[parsed.date] = parsed
    rows[current.date] = current

    ordered = [rows[key] for key in sorted(rows)[-days:]]
    if not ordered:
        return []

    lines = [
        "## Rolling Summary",
        "",
        f"Last {min(days, len(ordered))} report days:",
        "",
        "| date | live trade events | replay sentiment trades | replay sentiment return | price-only return | policy return | errors |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in ordered:
        lines.append(
            "| "
            + " | ".join(
                [
                    row.date,
                    str(row.live_trade_events),
                    fmt_count(row.replay_sentiment_trades),
                    fmt_pct(row.replay_sentiment_return_pct),
                    fmt_pct(row.replay_price_only_return_pct),
                    fmt_pct(row.policy_sentiment_return_pct),
                    str(row.error_events),
                ]
            )
            + " |"
        )
    lines.append("")
    return lines


def render_live_vs_backtest(metrics: ReportMetrics) -> list[str]:
    lines = [
        "## Live Vs Backtest",
        "",
        f"- Live trade events: {metrics.live_trade_events}",
        f"- Replay sentiment-policy trades: {fmt_count(metrics.replay_sentiment_trades)}",
        f"- Policy backtest sentiment trades: {fmt_count(metrics.policy_sentiment_trades)}",
        f"- Replay price-target-only return: {fmt_pct(metrics.replay_price_only_return_pct)}",
        "",
        "### Safety And Opportunity",
        "",
    ]

    price_return = metrics.replay_price_only_return_pct
    policy_return = metrics.policy_sentiment_return_pct
    replay_trades = metrics.replay_sentiment_trades

    if replay_trades is None:
        lines.append("- Safety result: replay sentiment-policy trade count was unavailable.")
    elif metrics.live_trade_events == 0 and replay_trades == 0:
        lines.append("- Safety result: live bot matched replay by staying out of sentiment-policy trades.")
    elif metrics.live_trade_events == 0 and isinstance(replay_trades, (int, float)) and replay_trades > 0:
        lines.append("- Safety result: live bot skipped trades that replay expected.")
    elif metrics.live_trade_events > 0 and replay_trades == 0:
        lines.append("- Safety result: live bot traded while replay expected no sentiment-policy trades.")
    else:
        lines.append("- Safety result: live and replay both showed trading activity.")

    if isinstance(policy_return, (int, float)) and policy_return < 0 and metrics.live_trade_events == 0:
        lines.append(f"- Avoided policy loss: live bot avoided a policy backtest outcome of {fmt_pct(policy_return)}.")
    if isinstance(price_return, (int, float)) and price_return > 0 and metrics.live_trade_events == 0:
        lines.append(f"- Opportunity cost: price-target replay was positive at {fmt_pct(price_return)} while live stayed flat.")
    elif isinstance(price_return, (int, float)) and price_return < 0 and metrics.live_trade_events == 0:
        lines.append(f"- Avoided baseline loss: price-target replay was negative at {fmt_pct(price_return)}.")

    lines.append("")
    return lines


def render_contrarian_candidate_summary(log_summary: LogSummary) -> list[str]:
    candidates = log_summary.contrarian_candidates
    lines = [
        "## Contrarian Candidate Tracking",
        "",
        "This is a watch-list diagnostic only; it does not imply the bot should trade these setups.",
        "",
        f"- Watch-only signal updates: {candidates.get('watch_only_signals', 0)}",
        f"- Negative watch-only signal updates: {candidates.get('negative_watch_only', 0)}",
        f"- Fresh watch-only signal updates: {candidates.get('fresh_watch_only', 0)}",
        f"- Liquidity acceptable watch-only updates: {candidates.get('liquidity_acceptable', 0)}",
        f"- Confidence acceptable watch-only updates: {candidates.get('confidence_acceptable', 0)}",
        f"- Low range-position watch-only updates: {candidates.get('low_range_position', 0)}",
        f"- Contrarian buy watch candidates: {candidates.get('contrarian_buy_watch_candidates', 0)}",
        "",
    ]
    return lines


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


def bucket_label(name: str) -> str:
    return {
        "abs_signal_lt_0.02": "abs(signal) < 0.02",
        "abs_signal_0.02_to_0.05": "0.02 <= abs(signal) < 0.05",
        "abs_signal_0.05_to_0.10": "0.05 <= abs(signal) < 0.10",
        "abs_signal_gte_0.10": "abs(signal) >= 0.10",
    }.get(name, name)


def is_strong_bucket(name: str) -> bool:
    return name in {"abs_signal_0.05_to_0.10", "abs_signal_gte_0.10"}


def pct_delta_text(value: float) -> str:
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.4f}%"


def render_forward_signal_analysis(source: str, payload: Optional[dict[str, Any]]) -> list[str]:
    lines = ["## Signal Forward Analysis", "", f"JSON: `{source}`"]
    if not payload:
        lines.extend(["", "No signal forward-return JSON was available.", ""])
        return lines

    results = payload.get("results")
    if not isinstance(results, dict) or not results:
        lines.extend(["", "No forward-return horizon results found.", ""])
        return lines

    timestamp = parse_timestamp(payload.get("timestamp"))
    since = parse_timestamp(payload.get("since"))
    forward_window_hours = None
    if timestamp and since:
        forward_window_hours = (timestamp - since).total_seconds() / 3600

    lines.extend(
        [
            f"timestamp: `{payload.get('timestamp', 'n/a')}`",
            f"since: `{payload.get('since', 'n/a')}`",
            f"forward window hours: `{forward_window_hours:.1f}`" if forward_window_hours is not None else "forward window hours: `n/a`",
            f"signals tested: `{payload.get('signal_count', 'n/a')}`",
            "",
        ]
    )

    warnings: list[str] = []
    contrarian_rows: list[tuple[str, str, int, float, Optional[float]]] = []
    thin_rows: list[tuple[str, str, str, int]] = []

    for horizon, horizon_payload in sorted(
        results.items(),
        key=lambda item: float(item[0]) if str(item[0]).replace(".", "", 1).isdigit() else 0.0,
    ):
        if not isinstance(horizon_payload, dict):
            continue
        signed_buckets = horizon_payload.get("signed_buckets")
        if not isinstance(signed_buckets, dict):
            continue
        for bucket_name, signed in signed_buckets.items():
            if not isinstance(signed, dict):
                continue
            negative = signed.get("negative")
            if isinstance(negative, dict):
                count = negative.get("count")
                avg_return = negative.get("avg_forward_return_pct")
                accuracy = negative.get("directional_accuracy")
                if (
                    is_strong_bucket(bucket_name)
                    and isinstance(count, int)
                    and count >= 30
                    and isinstance(avg_return, (int, float))
                    and avg_return > 0
                ):
                    contrarian_rows.append(
                        (
                            str(horizon),
                            bucket_label(bucket_name),
                            count,
                            float(avg_return),
                            float(accuracy) if isinstance(accuracy, (int, float)) else None,
                        )
                    )
            for side in ("positive", "negative"):
                side_payload = signed.get(side)
                if not isinstance(side_payload, dict):
                    continue
                count = side_payload.get("count")
                if is_strong_bucket(bucket_name) and isinstance(count, int) and count < 30:
                    thin_rows.append((str(horizon), bucket_label(bucket_name), side, count))

    if contrarian_rows:
        warnings.append(
            "Strong negative signals are currently behaving like contrarian/bounce markers, not bearish trade triggers."
        )
    if thin_rows:
        warnings.append(
            "Some strong-signal buckets have thin sample sizes; avoid tuning production gates from those buckets alone."
        )
    if not warnings:
        warnings.append("No strong signal inversion or sample-size issue detected.")

    lines.extend(["### Findings", ""])
    lines.extend(f"- {warning}" for warning in warnings)
    lines.append("")

    lines.extend(
        [
            "### Horizon Summary",
            "",
            "| horizon | samples | avg forward return | directional accuracy | avg abs signal | avg confidence |",
            "| --- | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for horizon, horizon_payload in sorted(
        results.items(),
        key=lambda item: float(item[0]) if str(item[0]).replace(".", "", 1).isdigit() else 0.0,
    ):
        if not isinstance(horizon_payload, dict):
            continue
        summary = horizon_payload.get("summary")
        if not isinstance(summary, dict):
            continue
        accuracy = summary.get("directional_accuracy")
        accuracy_text = f"{accuracy * 100:.1f}%" if isinstance(accuracy, (int, float)) else "n/a"
        lines.append(
            "| "
            + " | ".join(
                [
                    f"{horizon}h",
                    fmt_count(summary.get("count")),
                    fmt_pct(summary.get("avg_forward_return_pct")),
                    accuracy_text,
                    fmt_count(summary.get("avg_abs_signal")),
                    fmt_count(summary.get("avg_confidence")),
                ]
            )
            + " |"
        )
    lines.append("")

    if contrarian_rows:
        lines.extend(
            [
                "### Contrarian Warnings",
                "",
                "| horizon | bucket | negative samples | avg forward return after negative signal | directional accuracy |",
                "| --- | --- | ---: | ---: | ---: |",
            ]
        )
        for horizon, bucket, count, avg_return, accuracy in contrarian_rows:
            accuracy_text = f"{accuracy * 100:.1f}%" if accuracy is not None else "n/a"
            lines.append(
                f"| {horizon}h | {bucket} | {count} | {pct_delta_text(avg_return)} | {accuracy_text} |"
            )
        lines.append("")

    if thin_rows:
        lines.extend(
            [
                "### Thin Sample Warnings",
                "",
                "| horizon | bucket | side | samples |",
                "| --- | --- | --- | ---: |",
            ]
        )
        for horizon, bucket, side, count in thin_rows:
            lines.append(f"| {horizon}h | {bucket} | {side} | {count} |")
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

    log_summary = collect_log_lines(log_file, since)
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
    forward_json, forward_source = load_json_source(
        args.forward_url,
        Path(args.forward_output),
        args.timeout_seconds,
    )
    metrics = current_metrics(
        now.date().isoformat(),
        policy_json,
        replay_json,
        log_summary,
    )

    report: list[str] = [
        f"# Bot Backtesting Report - {now.date().isoformat()}",
        "",
        f"generated_at_utc: `{now.isoformat()}`",
        f"window_start_utc: `{since.isoformat()}`",
        f"window_hours: `{args.hours:g}`",
        "",
    ]
    report.extend(render_verdict(metrics))
    report.extend(
        render_health_summary(
            log_summary.event_counts,
            log_summary.signal_statuses,
        )
    )
    report.extend(render_signal_freshness_summary(log_summary))
    report.extend(render_rolling_summary(report_dir, report_path, metrics))
    report.extend(render_live_vs_backtest(metrics))
    report.extend(render_contrarian_candidate_summary(log_summary))
    report.extend(
        [
        "## Backtest Summaries",
        "",
        ]
    )
    report.extend(render_backtest_summary("Policy Backtest", policy_source, policy_json))
    report.extend(render_backtest_summary("Replay Backtest", replay_source, replay_json))
    report.extend(render_forward_signal_analysis(forward_source, forward_json))

    report.extend(["## Backtest Commands", ""])
    for run in runs:
        report.extend(render_command(run))

    report.extend(
        [
            "## Bot Log Summary",
            "",
            f"log_file: `{log_file}`",
            f"lines_in_window: `{len(log_summary.lines)}`",
            "",
            "### Events",
            "",
            *render_counter(log_summary.event_counts),
            "",
            "### Trade Decisions",
            "",
            *render_counter(log_summary.decision_reasons),
            "",
            "### Trade Events",
            "",
            *render_counter(log_summary.trade_events),
            "",
        ]
    )

    if not args.no_raw_log:
        report.extend(["## Raw Bot Logs", "", "```jsonl"])
        report.extend(log_summary.lines)
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
