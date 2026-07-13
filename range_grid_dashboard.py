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
OUTPUT_FILE = os.getenv(
    "RANGE_GRID_DASHBOARD_OUTPUT",
    "/var/www/html/bot/range_grid_dashboard.html",
)
LOOKBACK_HOURS = float(os.getenv("RANGE_GRID_DASHBOARD_LOOKBACK_HOURS", "24"))
RECENT_EVENT_LIMIT = int(os.getenv("RANGE_GRID_DASHBOARD_RECENT_EVENT_LIMIT", "30"))
MAX_LOG_SCAN_LINES = int(os.getenv("RANGE_GRID_DASHBOARD_MAX_LOG_SCAN_LINES", "5000"))
HEALTH_STALE_MINUTES = float(os.getenv("RANGE_GRID_DASHBOARD_HEALTH_STALE_MINUTES", "5"))


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
        elif event == "BUY_ORDER_FILLED":
            summary["buys_filled"] += 1
            summary["filled_by_source"][record.get("buy_source") or "unknown"] += 1
        elif event == "SELL_ORDER_PLACED":
            summary["sells_placed"] += 1
        elif event == "SELL_ORDER_FILLED":
            summary["sells_filled"] += 1
            summary["exited_by_source"][record.get("buy_source") or "unknown"] += 1
            try:
                summary["realized_net_pnl"] += float(record.get("estimated_net_pnl") or 0.0)
            except Exception:
                pass
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

        if event:
            summary["actions"][event] += 1
            recent.append(record)

    return summary, recent[-RECENT_EVENT_LIMIT:]


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


def render_dashboard(status, state, recent_summary, recent_events, alert_summary, now=None):
    now = now or utc_now()
    health_state, health_message = classify_health(status, alert_summary["recent"], now)
    inventory_buckets = (status or {}).get("inventory_buckets_usd") or {}
    grid_levels = (status or {}).get("grid_levels") or []
    status_timestamp = (status or {}).get("timestamp")
    last_alert_ts = alert_summary["recent"][-1]["ts"] if alert_summary["recent"] else None
    top_actions = ", ".join(
        f"{name} ({count})"
        for name, count in recent_summary["actions"].most_common(5)
    ) or "--"

    cards = "\n".join([
        stat_card("Health", health_state.upper(), tone=health_state, subtext=health_message),
        stat_card("Mode", (status or {}).get("operating_mode", "--")),
        stat_card("Price", f"${fmt_number((status or {}).get('price'), 2)}"),
        stat_card(
            "Weather",
            (status or {}).get("weather_condition") or "--",
            subtext=(status or {}).get("weather_alert_level") or None,
        ),
        stat_card(
            "Leveling",
            (status or {}).get("weather_leveling_state") or "--",
            subtext=fmt_number((status or {}).get("weather_leveling_score"), 4),
        ),
        stat_card("Signal", fmt_number((status or {}).get("execution_signal"), 4)),
        stat_card("Action", (status or {}).get("action_recommendation", "--")),
        stat_card("Runtime Block", (status or {}).get("runtime_block_reason", "none")),
        stat_card("Open Buys", fmt_int((status or {}).get("open_buy_count"))),
        stat_card("Open Sells", fmt_int((status or {}).get("open_sell_count"))),
        stat_card(
            "Inventory",
            f"${fmt_number((status or {}).get('deployed_inventory_usd'), 2)}",
        ),
        stat_card(
            f"{int(LOOKBACK_HOURS)}h Net PnL",
            f"${fmt_number(recent_summary['realized_net_pnl'], 4)}",
            tone="positive" if recent_summary["realized_net_pnl"] >= 0 else "negative",
        ),
        stat_card("Alerts", fmt_int(alert_summary["count"]), subtext=f"last {LOOKBACK_HOURS:g}h"),
        stat_card("Snapshot Age", human_age(status_timestamp, now)),
    ])

    weather_rows = weather_metric_rows(status)
    runtime_rows = key_value_rows([
        ("Timestamp", status_timestamp or "--"),
        ("Strategy File", (status or {}).get("strategy_profile", "--")),
        ("Strategy Modes", ", ".join((status or {}).get("strategy_modes") or []) or "--"),
        ("Configured Modes", ", ".join((status or {}).get("configured_strategy_modes") or []) or "--"),
        ("Grid Anchor", (status or {}).get("grid_anchor", "--")),
        ("Signal Status", (status or {}).get("signal_status", "--")),
        ("Range Fallback Active", (status or {}).get("range_fallback_active", False)),
        ("Realized PnL Today", f"${fmt_number((status or {}).get('realized_pnl_today'), 4)}"),
        ("Sell Backlog Count", fmt_int((status or {}).get("sell_backlog_count"))),
        ("Oldest Sell Age", f"{fmt_number((status or {}).get('sell_backlog_oldest_minutes'), 2)}m"),
        ("Last Alert Age", human_age(last_alert_ts, now)),
    ])

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
        f"<td>{html.escape(str(alert.get('message', '--')))}</td>"
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
      max-width: 1280px;
      margin: 0 auto;
      padding: 28px 20px 44px;
    }}
    .hero {{
      display: grid;
      gap: 12px;
      margin-bottom: 24px;
    }}
    h1 {{
      margin: 0;
      font-size: clamp(2rem, 4vw, 3.4rem);
      letter-spacing: -0.04em;
      line-height: 0.92;
    }}
    .lede {{
      color: var(--muted);
      font-size: 1rem;
      max-width: 72ch;
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
      .grid, .two {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <header class="hero">
      <h1>Range Grid Bot<br>Operations Dashboard</h1>
      <div class="lede">
        Static operational snapshot generated at {html.escape(now.isoformat())}. This page summarizes runtime health, recent trading activity, alerting, and inventory posture from the bot's local status, state, and logs.
      </div>
    </header>

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

    <div class="foot">
      Sources: {html.escape(os.path.abspath(STATUS_FILE))}, {html.escape(os.path.abspath(ALERT_LOG_FILE))}, {html.escape(os.path.abspath(STATE_FILE))}, {html.escape(os.path.abspath(TRADE_LOG_FILE))}.
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
    alert_records = read_jsonl_tail(ALERT_LOG_FILE, MAX_LOG_SCAN_LINES)
    recent_summary, recent_events = compute_recent_metrics(trade_records, now)
    alert_summary = compute_alert_metrics(alert_records, now)
    html_content = render_dashboard(
        status,
        state,
        recent_summary,
        recent_events,
        alert_summary,
        now,
    )
    atomic_write(output_file, html_content)
    return {
        "output_file": os.path.abspath(output_file),
        "status_file": os.path.abspath(STATUS_FILE),
        "alert_log_file": os.path.abspath(ALERT_LOG_FILE),
        "state_file": os.path.abspath(STATE_FILE),
        "trade_log_file": os.path.abspath(TRADE_LOG_FILE),
        "generated_at": now.isoformat(),
        "recent_alert_count": alert_summary["count"],
        "recent_event_count": len(recent_events),
    }


def main():
    result = build_dashboard()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
