#!/usr/bin/env python3

from __future__ import annotations

import html
import json
import os
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile

from dotenv import load_dotenv

load_dotenv()


STATUS_FILE = os.getenv("RANGE_GRID_STATUS_FILE", "range_grid_status.json")
ALERT_LOG_FILE = os.getenv("RANGE_GRID_ALERT_LOG_FILE", "range_grid_alerts.jsonl")
STATE_FILE = os.getenv("RANGE_GRID_STATE_FILE", "last_state.json")
TRADE_LOG_FILE = os.getenv("RANGE_GRID_TRADE_LOG_FILE", "trade_log.jsonl")
ACTIVITY_LOG_FILE = os.getenv("RANGE_GRID_ACTIVITY_LOG_FILE", "range_grid_activity.jsonl")
OUTPUT_FILE = os.getenv(
    "RANGE_GRID_DASHBOARD_OUTPUT",
    "/var/www/html/bot/range_grid_dashboard.html",
)
LOOKBACK_HOURS = float(os.getenv("RANGE_GRID_DASHBOARD_LOOKBACK_HOURS", "24"))
RECENT_EVENT_LIMIT = int(os.getenv("RANGE_GRID_DASHBOARD_RECENT_EVENT_LIMIT", "30"))
MAX_LOG_SCAN_LINES = int(os.getenv("RANGE_GRID_DASHBOARD_MAX_LOG_SCAN_LINES", "5000"))
HEALTH_STALE_MINUTES = float(os.getenv("RANGE_GRID_DASHBOARD_HEALTH_STALE_MINUTES", "5"))
ACTIVITY_LOG_ROTATE_DAILY = os.getenv(
    "RANGE_GRID_ACTIVITY_LOG_ROTATE_DAILY", "true"
).lower() not in {"0", "false", "no", "off"}
DASHBOARD_ROUND_TRIP_FEE_PCT = float(os.getenv(
    "RANGE_GRID_DASHBOARD_ROUND_TRIP_FEE_PCT",
    os.getenv("ROUND_TRIP_FEE_PCT", "0.0065"),
))


def parse_iso8601(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def utc_now():
    return datetime.now(timezone.utc)


def safe_read_json(path):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None
    except Exception:
        return None


def safe_float(value, default=None):
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def read_jsonl_tail(path, limit):
    records = deque(maxlen=limit)
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except Exception:
                    continue
    except FileNotFoundError:
        return []
    except Exception:
        return []
    return list(records)


def dated_jsonl_path(path, day):
    source = Path(path)
    name = source.name
    if name.endswith(".jsonl"):
        base = name[:-len(".jsonl")]
        suffix = ".jsonl"
    else:
        base = source.stem
        suffix = source.suffix
    return source.with_name(f"{base}_{day.strftime('%Y%m%d')}{suffix}")


def size_rotated_jsonl_paths(path):
    source = Path(path)
    name = source.name
    if name.endswith(".jsonl"):
        base = name[:-len(".jsonl")]
        suffix = ".jsonl"
    else:
        base = source.stem
        suffix = source.suffix or ".jsonl"

    paths = [
        candidate
        for candidate in source.parent.glob(f"{base}_[0-9][0-9][0-9][0-9]*{suffix}")
        if candidate.is_file()
    ]
    paths.sort(key=lambda candidate: candidate.stat().st_mtime)
    return paths


def read_activity_records(now=None):
    now = now or utc_now()
    if not ACTIVITY_LOG_ROTATE_DAILY:
        records = []
        for path in [*size_rotated_jsonl_paths(ACTIVITY_LOG_FILE), Path(ACTIVITY_LOG_FILE)]:
            records.extend(read_jsonl_tail(path, MAX_LOG_SCAN_LINES))
        records.sort(key=lambda record: record.get("ts") or "")
        return records[-MAX_LOG_SCAN_LINES:]

    days = max(1, int((LOOKBACK_HOURS + 23) // 24) + 1)
    records = []
    seen = set()
    for offset in range(days):
        day = (now - timedelta(days=offset)).date()
        for path in (dated_jsonl_path(ACTIVITY_LOG_FILE, day), Path(ACTIVITY_LOG_FILE)):
            path_key = str(path)
            if path_key in seen:
                continue
            seen.add(path_key)
            records.extend(read_jsonl_tail(path, MAX_LOG_SCAN_LINES))
    records.sort(key=lambda record: record.get("ts") or "")
    return records[-MAX_LOG_SCAN_LINES:]


def fmt_number(value, digits=2, fallback="--"):
    if value is None:
        return fallback
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return fallback


def fmt_int(value, fallback="--"):
    if value is None:
        return fallback
    try:
        return f"{int(value):,}"
    except Exception:
        return fallback


def fmt_pct(value, digits=2, fallback="--"):
    if value is None:
        return fallback
    try:
        return f"{float(value):.{digits}f}%"
    except Exception:
        return fallback


def fmt_bool(value):
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "--"


def fmt_list(value):
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) or "--"
    if value:
        return str(value)
    return "--"


def alert_detail_text(alert):
    parts = [str(alert.get("message", "--"))]
    reason = alert.get("reason")
    if reason:
        parts.append(f"reason={reason}")
    if alert.get("sell_backlog_count") is not None:
        parts.append(f"sell_backlog={alert.get('sell_backlog_count')}")
    if alert.get("sell_backlog_oldest_minutes") is not None:
        parts.append(
            f"oldest_sell={fmt_number(alert.get('sell_backlog_oldest_minutes'), 1)}m"
        )
    if alert.get("realized_pnl_today") is not None:
        parts.append(f"pnl_today=${fmt_number(alert.get('realized_pnl_today'), 2)}")
    return " | ".join(parts)


def age_minutes(value, now=None):
    now = now or utc_now()
    ts = parse_iso8601(value) if isinstance(value, str) else value
    if ts is None:
        return None
    return max(0.0, (now - ts).total_seconds() / 60.0)


def human_age(value, now=None):
    minutes = age_minutes(value, now)
    if minutes is None:
        return "--"
    if minutes < 1:
        return "<1m"
    if minutes < 60:
        return f"{int(minutes)}m"
    hours = minutes / 60.0
    if hours < 24:
        return f"{hours:.1f}h"
    return f"{hours / 24.0:.1f}d"


def classify_health(status, alerts, now=None):
    now = now or utc_now()
    if not status:
        return ("unknown", "No status snapshot found")

    status_age = age_minutes(status.get("timestamp"), now)
    if status_age is None or status_age > HEALTH_STALE_MINUTES:
        return ("stale", f"Status snapshot is stale ({human_age(status.get('timestamp'), now)} old)")

    runtime_block_reason = status.get("runtime_block_reason")
    if runtime_block_reason:
        return ("guarded", f"Buys blocked by guardrail: {runtime_block_reason}")

    recent_critical = [
        alert for alert in alerts
        if (alert.get("severity") == "critical")
        and (age_minutes(alert.get("ts"), now) or 10**9) <= 60
    ]
    if recent_critical:
        return ("degraded", recent_critical[0].get("message") or "Recent critical alert")

    return ("healthy", "Bot heartbeat and recent signals look healthy")


def compute_recent_metrics(trade_records, now=None):
    now = now or utc_now()
    lookback_cutoff = now - timedelta(hours=LOOKBACK_HOURS)
    summary = {
        "approved_candidates": 0,
        "buys_placed": 0,
        "buys_filled": 0,
        "sells_placed": 0,
        "sells_filled": 0,
        "buy_rejections": 0,
        "order_rejected": 0,
        "loop_errors": 0,
        "alerts": 0,
        "reconciliations": 0,
        "realized_net_pnl": 0.0,
        "realized_gross_pnl": 0.0,
        "buy_notional_usd": 0.0,
        "filled_buy_notional_usd": 0.0,
        "sell_notional_usd": 0.0,
        "sell_extension_shadow_decisions": 0,
        "sell_extension_shadow_additional_gross_pnl": 0.0,
        "activity_summary_count": 0,
        "latest_activity_summary": None,
        "actions": Counter(),
        "approved_by_source": Counter(),
        "placed_by_source": Counter(),
        "filled_by_source": Counter(),
        "exited_by_source": Counter(),
    }
    recent = []

    for record in trade_records:
        ts = parse_iso8601(record.get("ts"))
        if ts is None or ts < lookback_cutoff:
            continue
        event = record.get("event")
        if event == "TRADE_DECISION" and record.get("side") == "buy":
            summary["approved_candidates"] += 1
            summary["approved_by_source"][record.get("buy_source") or "unknown"] += 1
        elif event == "BUY_ORDER_PLACED":
            summary["buys_placed"] += 1
            summary["placed_by_source"][record.get("buy_source") or "unknown"] += 1
            summary["buy_notional_usd"] += safe_float(
                record.get("trade_notional_usd"),
                (safe_float(record.get("price"), 0.0) * safe_float(record.get("volume"), 0.0)),
            ) or 0.0
        elif event == "BUY_ORDER_FILLED":
            summary["buys_filled"] += 1
            summary["filled_by_source"][record.get("buy_source") or "unknown"] += 1
            summary["filled_buy_notional_usd"] += safe_float(
                record.get("trade_notional_usd"),
                (safe_float(record.get("price"), 0.0) * safe_float(record.get("volume"), 0.0)),
            ) or 0.0
        elif event == "SELL_ORDER_PLACED":
            summary["sells_placed"] += 1
            summary["sell_notional_usd"] += safe_float(
                record.get("trade_notional_usd"),
                (safe_float(record.get("price"), 0.0) * safe_float(record.get("volume"), 0.0)),
            ) or 0.0
        elif event == "SELL_ORDER_FILLED":
            summary["sells_filled"] += 1
            summary["exited_by_source"][record.get("buy_source") or "unknown"] += 1
            summary["realized_net_pnl"] += safe_float(record.get("estimated_net_pnl"), 0.0) or 0.0
            summary["realized_gross_pnl"] += safe_float(record.get("gross_pnl"), 0.0) or 0.0
        elif event == "SELL_EXTENSION_SHADOW_DECISION":
            summary["sell_extension_shadow_decisions"] += 1
            summary["sell_extension_shadow_additional_gross_pnl"] += safe_float(
                record.get("additional_gross_pnl"), 0.0
            ) or 0.0
        elif event == "ORDER_REJECTED":
            summary["order_rejected"] += 1
            if record.get("side") == "buy":
                summary["buy_rejections"] += 1
        elif event == "LOOP_ERROR":
            summary["loop_errors"] += 1
        elif event == "ALERT":
            summary["alerts"] += 1
        elif event in ("BTC_RECONCILED", "STARTUP_RECONCILE_COMPLETE"):
            summary["reconciliations"] += 1
        elif event == "ACTIVITY_SUMMARY":
            summary["activity_summary_count"] += 1
            summary["latest_activity_summary"] = record

        if event:
            summary["actions"][event] += 1
            recent.append(record)

    return summary, recent[-RECENT_EVENT_LIMIT:]


def compute_open_sell_pnl(
    state,
    current_price=None,
    now=None,
    round_trip_fee_pct=None,
):
    now = now or utc_now()
    fee_pct = safe_float(round_trip_fee_pct, DASHBOARD_ROUND_TRIP_FEE_PCT)
    fee_pct = max(0.0, fee_pct or 0.0)
    open_sells = (state or {}).get("open_sell_orders") or {}
    summary = {
        "open_sell_count": len(open_sells),
        "round_trip_fee_pct": fee_pct,
        "open_sell_volume": 0.0,
        "open_sell_buy_notional_usd": 0.0,
        "open_sell_target_notional_usd": 0.0,
        "open_sell_target_gross_pnl": 0.0,
        "open_sell_target_estimated_net_pnl": 0.0,
        "open_sell_current_gross_pnl": None,
        "open_sell_current_estimated_net_pnl": None,
        "open_sell_avg_age_minutes": None,
        "open_sell_oldest_age_minutes": None,
    }
    total_age = 0.0
    age_count = 0
    oldest_age = None
    current_gross = 0.0
    current_estimated_net = 0.0
    has_current_price = current_price is not None

    for order in open_sells.values():
        volume = safe_float(order.get("volume"), 0.0) or 0.0
        buy_price = safe_float(order.get("buy_price"), safe_float(order.get("level"), 0.0)) or 0.0
        sell_price = safe_float(order.get("sell_price"), 0.0) or 0.0
        if volume <= 0 or buy_price <= 0 or sell_price <= 0:
            continue

        buy_notional = volume * buy_price
        target_notional = volume * sell_price
        target_gross = target_notional - buy_notional
        estimated_fee = buy_notional * fee_pct

        summary["open_sell_volume"] += volume
        summary["open_sell_buy_notional_usd"] += buy_notional
        summary["open_sell_target_notional_usd"] += target_notional
        summary["open_sell_target_gross_pnl"] += target_gross
        summary["open_sell_target_estimated_net_pnl"] += target_gross - estimated_fee

        if has_current_price:
            mark_gross = volume * (current_price - buy_price)
            current_gross += mark_gross
            current_estimated_net += mark_gross - estimated_fee

        age = age_minutes(order.get("placed_at"), now)
        if age is not None:
            total_age += age
            age_count += 1
            oldest_age = age if oldest_age is None else max(oldest_age, age)

    if has_current_price:
        summary["open_sell_current_gross_pnl"] = current_gross
        summary["open_sell_current_estimated_net_pnl"] = current_estimated_net
    if age_count:
        summary["open_sell_avg_age_minutes"] = total_age / age_count
        summary["open_sell_oldest_age_minutes"] = oldest_age
    return summary


def render_source_counter(counter):
    if not counter:
        return "--"
    return ", ".join(
        f"{name} ({count})"
        for name, count in counter.most_common()
    )


def execution_metric_rows(status, recent_summary):
    lifetime_stats = ((status or {}).get("stats") or {})
    lifetime_quality = ((status or {}).get("execution_quality") or {})

    return key_value_rows([
        ("Recent Approved Candidates", recent_summary.get("approved_candidates", 0)),
        ("Recent Approval -> Placement", fmt_pct(
            (
                (recent_summary["buys_placed"] / recent_summary["approved_candidates"]) * 100
                if recent_summary.get("approved_candidates")
                else None
            ),
            1,
        )),
        ("Recent Placement -> Fill", fmt_pct(
            (
                (recent_summary["buys_filled"] / recent_summary["buys_placed"]) * 100
                if recent_summary.get("buys_placed")
                else None
            ),
            1,
        )),
        ("Recent Fill -> Exit", fmt_pct(
            (
                (recent_summary["sells_filled"] / recent_summary["buys_filled"]) * 100
                if recent_summary.get("buys_filled")
                else None
            ),
            1,
        )),
        ("Recent Buy Rejections", recent_summary.get("buy_rejections", 0)),
        ("Recent Approved Sources", render_source_counter(recent_summary.get("approved_by_source"))),
        ("Recent Placed Sources", render_source_counter(recent_summary.get("placed_by_source"))),
        ("Recent Filled Sources", render_source_counter(recent_summary.get("filled_by_source"))),
        ("Recent Exited Sources", render_source_counter(recent_summary.get("exited_by_source"))),
        ("Lifetime Approved Candidates", lifetime_stats.get("approved_buy_candidates", "--")),
        ("Lifetime Approval -> Placement", fmt_pct(
            (
                float(lifetime_quality.get("approval_to_placement_rate")) * 100
                if lifetime_quality.get("approval_to_placement_rate") is not None
                else None
            ),
            1,
        )),
        ("Lifetime Placement -> Fill", fmt_pct(
            (
                float(lifetime_quality.get("placement_to_fill_rate")) * 100
                if lifetime_quality.get("placement_to_fill_rate") is not None
                else None
            ),
            1,
        )),
        ("Lifetime Fill -> Exit", fmt_pct(
            (
                float(lifetime_quality.get("fill_to_exit_rate")) * 100
                if lifetime_quality.get("fill_to_exit_rate") is not None
                else None
            ),
            1,
        )),
    ])


def compute_alert_metrics(alert_records, now=None):
    now = now or utc_now()
    lookback_cutoff = now - timedelta(hours=LOOKBACK_HOURS)
    recent_alerts = []
    severities = Counter()
    types = Counter()

    for alert in alert_records:
        ts = parse_iso8601(alert.get("ts"))
        if ts is None or ts < lookback_cutoff:
            continue
        recent_alerts.append(alert)
        severities[alert.get("severity") or "unknown"] += 1
        types[alert.get("alert_type") or "unknown"] += 1

    return {
        "count": len(recent_alerts),
        "severities": severities,
        "types": types,
        "recent": recent_alerts[-10:],
    }


def stat_card(label, value, tone="neutral", subtext=None):
    sub = f'<div class="sub">{html.escape(str(subtext))}</div>' if subtext else ""
    return (
        f'<section class="card {tone}">'
        f'<div class="label">{html.escape(label)}</div>'
        f'<div class="value">{html.escape(str(value))}</div>'
        f"{sub}</section>"
    )


def key_value_rows(mapping):
    rows = []
    for key, value in mapping:
        rows.append(
            "<tr>"
            f"<th>{html.escape(str(key))}</th>"
            f"<td>{html.escape(str(value))}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def weather_metric_rows(status):
    status = status or {}
    return key_value_rows([
        ("Report Available", fmt_bool(status.get("weather_report_available"))),
        ("Condition", status.get("weather_condition") or "--"),
        ("Alert Level", status.get("weather_alert_level") or "--"),
        ("Emergency Bell", fmt_bool(status.get("weather_emergency_bell"))),
        ("Trade Permission", status.get("weather_trade_permission") or "--"),
        ("Decision Authority", status.get("weather_bot_decision_authority") or "--"),
        ("Opportunity Tags", fmt_list(status.get("weather_opportunity_tags"))),
        ("Risk Warnings", fmt_list(status.get("weather_risk_warnings"))),
        ("Position Size Multiplier", fmt_number(
            status.get("weather_position_size_multiplier"), 4
        )),
        ("Grid Aggression Multiplier", fmt_number(
            status.get("weather_grid_aggression_multiplier"), 4
        )),
        ("Target Profit Multiplier", fmt_number(
            status.get("weather_target_profit_multiplier"), 4
        )),
        ("Entry Discount Multiplier", fmt_number(
            status.get("weather_entry_discount_multiplier"), 4
        )),
        ("Leveling State", status.get("weather_leveling_state") or "--"),
        ("Leveling Score", fmt_number(status.get("weather_leveling_score"), 4)),
        ("Market Zone", status.get("weather_market_range_zone") or "--"),
        ("Market Range Position", fmt_number(
            status.get("weather_market_range_position"), 4
        )),
        ("Distance To High", fmt_pct(
            status.get("weather_market_distance_to_recent_high_pct"), 4
        )),
        ("Distance From Low", fmt_pct(
            status.get("weather_market_distance_from_recent_low_pct"), 4
        )),
        ("24h Return", fmt_pct(
            status.get("weather_market_price_return_24h_pct"), 4
        )),
        ("4h Return", fmt_pct(
            status.get("weather_market_price_return_4h_pct"), 4
        )),
    ])


def render_grid_levels_table(levels):
    if not isinstance(levels, list) or not levels:
        return '<tr><td colspan="2">No current grid levels</td></tr>'

    rows = []
    for idx, level in enumerate(levels, start=1):
        rows.append(
            "<tr>"
            f"<th>Level {idx}</th>"
            f"<td>${html.escape(fmt_number(level, 2))}</td>"
            "</tr>"
        )
    return "\n".join(rows)


def compact_strategy_label(value):
    if not value:
        return "--"
    return Path(str(value)).name.removesuffix(".json")


def compact_grid_levels(status):
    status = status or {}
    levels = status.get("grid_levels") or []
    chips = []
    for level in levels[:6]:
        numeric = safe_float(level)
        if numeric is not None:
            chips.append(f'<span class="chip">${html.escape(fmt_number(numeric, 2))}</span>')
    if not chips:
        chips.append('<span class="chip muted-chip">No actionable levels</span>')
    return "".join(chips)


def compact_order_rows(state, side, now=None, limit=5):
    now = now or utc_now()
    key = "open_buy_orders" if side == "buy" else "open_sell_orders"
    raw_orders = (state or {}).get(key) or {}
    if isinstance(raw_orders, dict):
        orders = list(raw_orders.values())
    elif isinstance(raw_orders, list):
        orders = raw_orders
    else:
        orders = []

    def order_price(order):
        if side == "buy":
            return safe_float(order.get("price"), safe_float(order.get("level"), 0.0)) or 0.0
        return safe_float(order.get("sell_price"), 0.0) or 0.0

    orders.sort(key=order_price, reverse=(side == "buy"))
    rows = []
    for order in orders[:limit]:
        price = order_price(order)
        volume = safe_float(order.get("volume"), 0.0) or 0.0
        notional = price * volume
        source = str(order.get("buy_source") or "unknown").replace("range_", "")
        age = human_age(order.get("placed_at"), now)
        detail = ""
        if side == "sell":
            buy_price = safe_float(
                order.get("buy_price"),
                safe_float(order.get("level"), 0.0),
            ) or 0.0
            target_gross = volume * max(0.0, price - buy_price)
            detail = f"+${fmt_number(target_gross, 2)} gross"
        else:
            anchor = safe_float(order.get("entry_anchor_level"))
            if anchor is not None and abs(anchor - price) > 0.005:
                detail = f"anchor ${fmt_number(anchor, 2)}"
            elif order.get("near_touch_applied"):
                detail = "near-touch"
            else:
                detail = order.get("entry_placement_mode") or "resting"
        rows.append(
            "<tr>"
            f"<td><strong>{html.escape(source)}</strong><span>{html.escape(str(detail))}</span></td>"
            f"<td>${html.escape(fmt_number(price, 2))}</td>"
            f"<td>${html.escape(fmt_number(notional, 2))}</td>"
            f"<td>{html.escape(age)}</td>"
            "</tr>"
        )
    if not rows:
        return f'<tr><td colspan="4">No open {html.escape(side)} orders</td></tr>'
    if len(orders) > limit:
        rows.append(
            f'<tr class="more-row"><td colspan="4">+{len(orders) - limit} more {html.escape(side)} orders</td></tr>'
        )
    return "".join(rows)


def pnl_tone(value):
    if value is None:
        return "neutral"
    return "positive" if value >= 0 else "negative"


def pnl_metric_rows(recent_summary, open_pnl, state):
    state_summary = ((state or {}).get("stats") or {})
    latest_activity = recent_summary.get("latest_activity_summary") or {}
    avg_buy_size = (
        recent_summary.get("buy_notional_usd", 0.0) / recent_summary.get("buys_placed", 0)
        if recent_summary.get("buys_placed")
        else None
    )
    return key_value_rows([
        (
            f"Realized Net PnL ({LOOKBACK_HOURS:g}h)",
            f"${fmt_number(recent_summary.get('realized_net_pnl'), 4)}",
        ),
        (
            f"Realized Gross PnL ({LOOKBACK_HOURS:g}h)",
            f"${fmt_number(recent_summary.get('realized_gross_pnl'), 4)}",
        ),
        (
            f"Buy Notional Placed ({LOOKBACK_HOURS:g}h)",
            f"${fmt_number(recent_summary.get('buy_notional_usd'), 2)}",
        ),
        (
            f"Avg Buy Size ({LOOKBACK_HOURS:g}h)",
            f"${fmt_number(avg_buy_size, 2)}",
        ),
        (
            "Open Sell Target Net PnL",
            f"${fmt_number(open_pnl.get('open_sell_target_estimated_net_pnl'), 4)}",
        ),
        (
            "Open Sell Target Gross PnL",
            f"${fmt_number(open_pnl.get('open_sell_target_gross_pnl'), 4)}",
        ),
        (
            "Open Sell Mark-to-Market Net",
            f"${fmt_number(open_pnl.get('open_sell_current_estimated_net_pnl'), 4)}",
        ),
        (
            "Open Sell Buy Notional",
            f"${fmt_number(open_pnl.get('open_sell_buy_notional_usd'), 2)}",
        ),
        ("Open Sell Count", fmt_int(open_pnl.get("open_sell_count"))),
        (
            "Open Sell Avg Age",
            f"{fmt_number(open_pnl.get('open_sell_avg_age_minutes'), 1)}m",
        ),
        (
            "Open Sell Oldest Age",
            f"{fmt_number(open_pnl.get('open_sell_oldest_age_minutes'), 1)}m",
        ),
        (
            "Shadow Extension Upside",
            f"${fmt_number(recent_summary.get('sell_extension_shadow_additional_gross_pnl'), 4)}"
            f" across {fmt_int(recent_summary.get('sell_extension_shadow_decisions'))} ideas",
        ),
        (
            "Latest Activity Realized Today",
            f"${fmt_number(latest_activity.get('realized_pnl_today'), 4)}",
        ),
        (
            "Lifetime Realized Gross PnL",
            f"${fmt_number(state_summary.get('realized_gross_pnl'), 8)}",
        ),
        (
            "Lifetime Realized Net PnL",
            f"${fmt_number(state_summary.get('realized_estimated_net_pnl'), 8)}",
        ),
        (
            "Dashboard Fee Assumption",
            fmt_pct(
                safe_float(
                    open_pnl.get("round_trip_fee_pct"),
                    DASHBOARD_ROUND_TRIP_FEE_PCT,
                ) * 100,
                4,
            ),
        ),
    ])


def render_dashboard(status, state, recent_summary, recent_events, alert_summary, open_pnl, now=None):
    now = now or utc_now()
    status = status or {}
    health_state, health_message = classify_health(status, alert_summary["recent"], now)
    latest_activity = recent_summary.get("latest_activity_summary") or {}

    def current_value(key, default=None):
        value = status.get(key)
        if value is None:
            value = latest_activity.get(key)
        return default if value is None else value

    inventory_buckets = status.get("inventory_buckets_usd") or {}
    grid_levels = status.get("grid_levels") or []
    status_timestamp = status.get("timestamp")
    last_alert_ts = alert_summary["recent"][-1]["ts"] if alert_summary["recent"] else None
    top_actions = ", ".join(
        f"{name} ({count})"
        for name, count in recent_summary["actions"].most_common(5)
    ) or "--"

    cards = "\n".join([
        stat_card("Health", health_state.upper(), tone=health_state, subtext=health_message),
        stat_card("Mode", status.get("operating_mode", "--")),
        stat_card("Price", f"${fmt_number(status.get('price'), 2)}"),
        stat_card(
            "Weather",
            status.get("weather_condition") or "--",
            subtext=status.get("weather_alert_level") or None,
        ),
        stat_card(
            "Leveling",
            status.get("weather_leveling_state") or "--",
            subtext=fmt_number(status.get("weather_leveling_score"), 4),
        ),
        stat_card("Signal", fmt_number(status.get("execution_signal"), 4)),
        stat_card("Action", status.get("action_recommendation", "--")),
        stat_card("Runtime Block", status.get("runtime_block_reason", "none")),
        stat_card("Open Buys", fmt_int(status.get("open_buy_count"))),
        stat_card("Open Sells", fmt_int(status.get("open_sell_count"))),
        stat_card(
            "Inventory",
            f"${fmt_number(status.get('deployed_inventory_usd'), 2)}",
        ),
        stat_card(
            f"{int(LOOKBACK_HOURS)}h Net PnL",
            f"${fmt_number(recent_summary['realized_net_pnl'], 4)}",
            tone=pnl_tone(recent_summary["realized_net_pnl"]),
            subtext="filled sells only",
        ),
        stat_card(
            "Open Target Net",
            f"${fmt_number(open_pnl.get('open_sell_target_estimated_net_pnl'), 4)}",
            tone=pnl_tone(open_pnl.get("open_sell_target_estimated_net_pnl")),
            subtext="if current sells fill",
        ),
        stat_card(
            "Open Mark Net",
            f"${fmt_number(open_pnl.get('open_sell_current_estimated_net_pnl'), 4)}",
            tone=pnl_tone(open_pnl.get("open_sell_current_estimated_net_pnl")),
            subtext="at current price",
        ),
        stat_card("Alerts", fmt_int(alert_summary["count"]), subtext=f"last {LOOKBACK_HOURS:g}h"),
        stat_card("Snapshot Age", human_age(status_timestamp, now)),
    ])

    weather_rows = weather_metric_rows(status)
    runtime_rows = key_value_rows([
        ("Timestamp", status_timestamp or "--"),
        ("Strategy File", status.get("strategy_profile", "--")),
        ("Strategy Modes", ", ".join(status.get("strategy_modes") or []) or "--"),
        ("Configured Modes", ", ".join(status.get("configured_strategy_modes") or []) or "--"),
        ("Grid Anchor", status.get("grid_anchor", "--")),
        ("Signal Status", status.get("signal_status", "--")),
        ("Range Fallback Active", status.get("range_fallback_active", False)),
        ("Realized PnL Today", f"${fmt_number(status.get('realized_pnl_today'), 4)}"),
        ("Sell Backlog Count", fmt_int(status.get("sell_backlog_count"))),
        ("Oldest Sell Age", f"{fmt_number(status.get('sell_backlog_oldest_minutes'), 2)}m"),
        ("Last Alert Age", human_age(last_alert_ts, now)),
    ])
    pnl_rows = pnl_metric_rows(recent_summary, open_pnl, state)

    kpi_rows = key_value_rows([
        ("Approved Candidates", recent_summary["approved_candidates"]),
        ("Buys Placed", recent_summary["buys_placed"]),
        ("Buys Filled", recent_summary["buys_filled"]),
        ("Sells Placed", recent_summary["sells_placed"]),
        ("Sells Filled", recent_summary["sells_filled"]),
        ("Buy Rejections", recent_summary["buy_rejections"]),
        ("Order Rejections", recent_summary["order_rejected"]),
        ("Loop Errors", recent_summary["loop_errors"]),
        ("Reconciliations", recent_summary["reconciliations"]),
        ("Top Recent Events", top_actions),
    ])
    execution_rows = execution_metric_rows(status, recent_summary)

    bucket_rows = key_value_rows([
        (bucket, f"${fmt_number(value, 2)}")
        for bucket, value in sorted(inventory_buckets.items())
    ]) or '<tr><th>Inventory Buckets</th><td>--</td></tr>'
    grid_level_rows = render_grid_levels_table(grid_levels)

    alert_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(alert.get('ts', '--')))}</td>"
        f"<td>{html.escape(str(alert.get('severity', '--')))}</td>"
        f"<td>{html.escape(str(alert.get('alert_type', '--')))}</td>"
        f"<td>{html.escape(alert_detail_text(alert))}</td>"
        "</tr>"
        for alert in reversed(alert_summary["recent"])
    ) or '<tr><td colspan="4">No recent alerts</td></tr>'

    event_rows = "\n".join(
        "<tr>"
        f"<td>{html.escape(str(event.get('ts', '--')))}</td>"
        f"<td>{html.escape(str(event.get('event', '--')))}</td>"
        f"<td>{html.escape(str(event.get('message', '') or event.get('reason', '--')))}</td>"
        "</tr>"
        for event in reversed(recent_events)
    ) or '<tr><td colspan="3">No recent events</td></tr>'

    state_summary = ((state or {}).get("stats") or {})
    strategy_label = compact_strategy_label(current_value("strategy_profile"))
    active_modes = ", ".join(current_value("strategy_modes", []) or []) or "--"
    run_mode = "PAPER" if current_value("paper_trading_enabled", False) else "LIVE"
    runtime_block = current_value("runtime_block_reason")
    process_state = runtime_block or current_value("action_recommendation", "evaluating")
    open_buy_count = len(((state or {}).get("open_buy_orders") or {}))
    open_sell_count = len(((state or {}).get("open_sell_orders") or {}))
    if not open_buy_count:
        open_buy_count = int(safe_float(current_value("open_buy_count"), 0) or 0)
    if not open_sell_count:
        open_sell_count = int(safe_float(current_value("open_sell_count"), 0) or 0)
    buy_order_rows = compact_order_rows(state, "buy", now)
    sell_order_rows = compact_order_rows(state, "sell", now)
    grid_chips = compact_grid_levels(status)
    lifetime_net = state_summary.get("realized_estimated_net_pnl")
    realized_today = current_value("realized_pnl_today")
    open_target_net = open_pnl.get("open_sell_target_estimated_net_pnl")
    open_mark_net = open_pnl.get("open_sell_current_estimated_net_pnl")

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="60">
  <title>Range Grid Bot Dashboard</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: rgba(255,255,255,0.78);
      --ink: #1f2a2a;
      --muted: #5f6b67;
      --line: rgba(31,42,42,0.12);
      --healthy: #0d7a5f;
      --guarded: #9a6700;
      --degraded: #b42318;
      --stale: #6b7280;
      --positive: #0b6b4b;
      --negative: #b42318;
      --shadow: 0 18px 40px rgba(29, 39, 39, 0.08);
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: Georgia, "Times New Roman", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(255,255,255,0.7), transparent 36%),
        linear-gradient(160deg, #efe6d2 0%, #f8f5ef 55%, #e4ece9 100%);
    }}
    .wrap {{
      max-width: 1480px;
      margin: 0 auto;
      padding: 14px 18px 32px;
    }}
    .topbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 12px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(1.45rem, 2.4vw, 2.2rem);
      letter-spacing: -0.03em;
      line-height: 1;
    }}
    .freshness {{
      color: var(--muted);
      font-size: 0.82rem;
      text-align: right;
    }}
    .health-pill {{
      display: inline-block;
      margin-left: 8px;
      padding: 5px 9px;
      border-radius: 999px;
      color: white;
      background: var(--healthy);
      font-size: 0.72rem;
      font-weight: 700;
      letter-spacing: 0.08em;
    }}
    .health-pill.guarded {{ background: var(--guarded); }}
    .health-pill.degraded {{ background: var(--degraded); }}
    .health-pill.stale, .health-pill.unknown {{ background: var(--stale); }}
    .process-grid {{
      display: grid;
      grid-template-columns: 1.05fr 1.65fr 1.5fr 1fr 1.35fr;
      gap: 10px;
      margin-bottom: 10px;
    }}
    .process-card {{
      min-width: 0;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 15px;
      padding: 11px 13px;
      box-shadow: var(--shadow);
    }}
    .process-label {{
      color: var(--muted);
      font-size: 0.68rem;
      font-weight: 700;
      letter-spacing: 0.09em;
      text-transform: uppercase;
      margin-bottom: 6px;
    }}
    .process-value {{
      font-size: 1.35rem;
      font-weight: 700;
      line-height: 1.05;
    }}
    .strategy-value {{
      font-size: 0.94rem;
      line-height: 1.2;
      overflow-wrap: anywhere;
    }}
    .process-sub {{
      margin-top: 6px;
      color: var(--muted);
      font-size: 0.78rem;
      line-height: 1.3;
    }}
    .range-line, .profit-line {{
      display: flex;
      justify-content: space-between;
      gap: 8px;
      font-size: 0.78rem;
      line-height: 1.5;
    }}
    .profit-line strong {{ font-variant-numeric: tabular-nums; }}
    .chips {{
      display: flex;
      gap: 5px;
      flex-wrap: wrap;
      margin-top: 7px;
    }}
    .chip {{
      display: inline-block;
      padding: 3px 6px;
      border-radius: 999px;
      background: rgba(13,122,95,0.10);
      color: var(--positive);
      font-size: 0.7rem;
      font-weight: 700;
    }}
    .muted-chip {{ background: rgba(95,107,103,0.10); color: var(--muted); }}
    .order-board {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      margin-bottom: 12px;
    }}
    .order-panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 15px;
      padding: 9px 12px;
      box-shadow: var(--shadow);
    }}
    .order-title {{
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      margin-bottom: 5px;
    }}
    .order-title h2 {{ margin: 0; font-size: 0.95rem; }}
    .order-count {{ color: var(--muted); font-size: 0.76rem; }}
    .compact-table {{ font-size: 0.76rem; }}
    .compact-table th, .compact-table td {{ padding: 4px 5px 4px 0; }}
    .compact-table th {{ width: auto; font-size: 0.65rem; text-transform: uppercase; letter-spacing: 0.06em; }}
    .compact-table td span {{ display: block; color: var(--muted); font-size: 0.66rem; }}
    .more-row td {{ text-align: center; color: var(--muted); }}
    details.diagnostics {{ margin-top: 10px; }}
    details.diagnostics > summary {{
      cursor: pointer;
      color: var(--muted);
      font-size: 0.86rem;
      font-weight: 700;
      padding: 8px 2px;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 14px;
      margin-bottom: 24px;
    }}
    .card {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 14px 16px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .card.healthy {{ border-top: 4px solid var(--healthy); }}
    .card.guarded {{ border-top: 4px solid var(--guarded); }}
    .card.degraded {{ border-top: 4px solid var(--degraded); }}
    .card.stale {{ border-top: 4px solid var(--stale); }}
    .card.positive {{ border-top: 4px solid var(--positive); }}
    .card.negative {{ border-top: 4px solid var(--negative); }}
    .card.danger {{ border-top: 4px solid var(--negative); }}
    .card.watch {{ border-top: 4px solid var(--guarded); }}
    .card.caution {{ border-top: 4px solid var(--guarded); }}
    .label {{
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 8px;
    }}
    .value {{
      font-size: 1.4rem;
      font-weight: 700;
    }}
    .sub {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 0.92rem;
      line-height: 1.35;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1.2fr 1fr;
      gap: 18px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: 18px;
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }}
    .panel h2 {{
      margin: 0 0 14px;
      font-size: 1.15rem;
      letter-spacing: -0.02em;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }}
    th, td {{
      text-align: left;
      padding: 10px 0;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
      width: 38%;
      padding-right: 16px;
    }}
    .full {{
      margin-top: 18px;
    }}
    .two {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 18px;
      margin-top: 18px;
    }}
    .foot {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 0.88rem;
    }}
    @media (max-width: 900px) {{
      .process-grid {{ grid-template-columns: 1fr 1fr; }}
      .order-board, .grid, .two {{ grid-template-columns: 1fr; }}
      .topbar {{ align-items: flex-start; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <header class="topbar">
      <h1>Range Grid Bot</h1>
      <div class="freshness">
        Updated {html.escape(human_age(status_timestamp, now))} ago
        <span class="health-pill {html.escape(health_state)}">{html.escape(health_state.upper())}</span>
      </div>
    </header>

    <section class="process-grid" aria-label="Current Process">
      <article class="process-card">
        <div class="process-label">BTC / USD</div>
        <div class="process-value">${fmt_number(current_value('price'), 2)}</div>
        <div class="process-sub">{html.escape(str(current_value('weather_condition', '--')))} · {html.escape(str(current_value('weather_market_range_zone', '--')))}</div>
      </article>
      <article class="process-card">
        <div class="process-label">Current Strategy · {html.escape(run_mode)}</div>
        <div class="process-value strategy-value">{html.escape(strategy_label)}</div>
        <div class="process-sub">{html.escape(active_modes)} · {html.escape(str(process_state))}</div>
      </article>
      <article class="process-card">
        <div class="process-label">Current Grids</div>
        <div class="range-line"><span>Low</span><strong>${fmt_number(current_value('range_low'), 2)}</strong></div>
        <div class="range-line"><span>Median</span><strong>${fmt_number(current_value('range_median'), 2)}</strong></div>
        <div class="range-line"><span>High</span><strong>${fmt_number(current_value('range_high'), 2)}</strong></div>
        <div class="chips">{grid_chips}</div>
      </article>
      <article class="process-card">
        <div class="process-label">Open Orders</div>
        <div class="process-value">{open_buy_count} buy · {open_sell_count} sell</div>
        <div class="process-sub">${fmt_number(current_value('deployed_inventory_usd'), 2)} deployed<br>{fmt_int(current_value('sell_backlog_count', open_sell_count))} in sell backlog</div>
      </article>
      <article class="process-card">
        <div class="process-label">Profit</div>
        <div class="profit-line"><span>Today</span><strong>${fmt_number(realized_today, 2)}</strong></div>
        <div class="profit-line"><span>{LOOKBACK_HOURS:g}h realized</span><strong>${fmt_number(recent_summary.get('realized_net_pnl'), 2)}</strong></div>
        <div class="profit-line"><span>Open at target</span><strong>${fmt_number(open_target_net, 2)}</strong></div>
        <div class="profit-line"><span>Open mark</span><strong>${fmt_number(open_mark_net, 2)}</strong></div>
        <div class="profit-line"><span>Lifetime net</span><strong>${fmt_number(lifetime_net, 2)}</strong></div>
      </article>
    </section>

    <section class="order-board" aria-label="Open Order Details">
      <div class="order-panel">
        <div class="order-title"><h2>Open Buy Orders</h2><span class="order-count">{open_buy_count} total</span></div>
        <table class="compact-table">
          <thead><tr><th>Source</th><th>Buy price</th><th>Notional</th><th>Age</th></tr></thead>
          <tbody>{buy_order_rows}</tbody>
        </table>
      </div>
      <div class="order-panel">
        <div class="order-title"><h2>Open Sell Orders</h2><span class="order-count">{open_sell_count} total</span></div>
        <table class="compact-table">
          <thead><tr><th>Source</th><th>Sell price</th><th>Notional</th><th>Age</th></tr></thead>
          <tbody>{sell_order_rows}</tbody>
        </table>
      </div>
    </section>

    <details class="diagnostics">
      <summary>Detailed diagnostics, weather, execution quality, alerts, and events</summary>

    <section class="cards">
      {cards}
    </section>

    <section class="grid">
      <div class="panel">
        <h2>Runtime</h2>
        <table>{runtime_rows}</table>
      </div>
      <div class="panel">
        <h2>Recent KPI Window ({LOOKBACK_HOURS:g}h)</h2>
        <table>{kpi_rows}</table>
      </div>
      <div class="panel">
        <h2>PnL Accounting</h2>
        <table>{pnl_rows}</table>
      </div>
    </section>

    <section class="two">
      <div class="panel">
        <h2>Market Weather</h2>
        <table>{weather_rows}</table>
      </div>
      <div class="panel">
        <h2>Current Grid Levels</h2>
        <table>{grid_level_rows}</table>
      </div>
      <div class="panel">
        <h2>Execution Quality</h2>
        <table>{execution_rows}</table>
      </div>
      <div class="panel">
        <h2>Inventory Buckets</h2>
        <table>{bucket_rows}</table>
      </div>
      <div class="panel">
        <h2>Lifetime State Stats</h2>
        <table>{key_value_rows([
          ("Buy Orders Placed", state_summary.get("buy_orders_placed", "--")),
          ("Buy Orders Filled", state_summary.get("buy_orders_filled", "--")),
          ("Sell Orders Placed", state_summary.get("sell_orders_placed", "--")),
          ("Sell Orders Filled", state_summary.get("sell_orders_filled", "--")),
          ("Realized Gross PnL", f"${fmt_number(state_summary.get('realized_gross_pnl'), 8)}"),
          ("Realized Net PnL", f"${fmt_number(state_summary.get('realized_estimated_net_pnl'), 8)}"),
        ])}</table>
      </div>
    </section>

    <section class="two full">
      <div class="panel">
        <h2>Recent Alerts</h2>
        <table>
          <thead><tr><th>Time</th><th>Severity</th><th>Type</th><th>Message</th></tr></thead>
          <tbody>{alert_rows}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Recent Events</h2>
        <table>
          <thead><tr><th>Time</th><th>Event</th><th>Detail</th></tr></thead>
          <tbody>{event_rows}</tbody>
        </table>
      </div>
    </section>
    </details>

    <div class="foot">
      Sources: {html.escape(os.path.abspath(STATUS_FILE))}, {html.escape(os.path.abspath(ALERT_LOG_FILE))}, {html.escape(os.path.abspath(STATE_FILE))}, {html.escape(os.path.abspath(ACTIVITY_LOG_FILE))}, {html.escape(os.path.abspath(TRADE_LOG_FILE))}.
    </div>
  </div>
</body>
</html>
"""


def atomic_write(path, content):
    path = Path(path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path.parent, 0o755)
    except Exception:
        pass
    with NamedTemporaryFile("w", delete=False, dir=str(path.parent), encoding="utf-8") as tmp:
        tmp.write(content)
        temp_name = tmp.name
    os.replace(temp_name, path)
    os.chmod(path, 0o644)


def build_dashboard(output_file=OUTPUT_FILE):
    now = utc_now()
    status = safe_read_json(STATUS_FILE)
    state = safe_read_json(STATE_FILE) or {}
    trade_records = read_jsonl_tail(TRADE_LOG_FILE, MAX_LOG_SCAN_LINES)
    activity_records = read_activity_records(now)
    alert_records = read_jsonl_tail(ALERT_LOG_FILE, MAX_LOG_SCAN_LINES)
    metrics_records = activity_records or trade_records
    recent_summary, recent_events = compute_recent_metrics(metrics_records, now)
    alert_summary = compute_alert_metrics(alert_records, now)
    current_price = safe_float((status or {}).get("price"))
    open_pnl = compute_open_sell_pnl(
        state,
        current_price=current_price,
        now=now,
        round_trip_fee_pct=(status or {}).get("round_trip_fee_pct"),
    )
    html_content = render_dashboard(
        status,
        state,
        recent_summary,
        recent_events,
        alert_summary,
        open_pnl,
        now,
    )
    atomic_write(output_file, html_content)
    return {
        "output_file": os.path.abspath(output_file),
        "status_file": os.path.abspath(STATUS_FILE),
        "alert_log_file": os.path.abspath(ALERT_LOG_FILE),
        "state_file": os.path.abspath(STATE_FILE),
        "trade_log_file": os.path.abspath(TRADE_LOG_FILE),
        "activity_log_file": os.path.abspath(ACTIVITY_LOG_FILE),
        "activity_log_records": len(activity_records),
        "generated_at": now.isoformat(),
        "recent_alert_count": alert_summary["count"],
        "recent_event_count": len(recent_events),
    }


def main():
    result = build_dashboard()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
