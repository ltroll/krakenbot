#!/usr/bin/env python3

import json
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from target_quality import (
    evaluate_quality_target,
    match_quality_target,
    normalize_profit_target_pct,
    parse_iso8601,
    unavailable_quality_decision,
)


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

SNAPSHOT_LOG_FILE = os.getenv(
    "LLM_TARGET_BACKTEST_SNAPSHOT_FILE",
    "llm_target_backtest_snapshot_log.jsonl"
)
BACKTEST_OUTPUT_FILE = os.getenv(
    "LLM_TARGET_BACKTEST_OUTPUT_FILE",
    "/var/www/html/bot/llm_target_backtest.json"
)
BACKTEST_ARCHIVE_DIR = os.getenv(
    "LLM_TARGET_BACKTEST_ARCHIVE_DIR",
    "/var/www/html/bot"
)
BACKTEST_WINDOW_HOURS = float(os.getenv("LLM_TARGET_BACKTEST_WINDOW_HOURS", "24"))
BACKTEST_ENTRY_WAIT_HOURS = float(
    os.getenv("LLM_TARGET_BACKTEST_ENTRY_WAIT_HOURS", "4")
)
BACKTEST_MAX_HOLD_HOURS = float(
    os.getenv("LLM_TARGET_BACKTEST_MAX_HOLD_HOURS", "24")
)
BACKTEST_STOP_LOSS_PCT = float(os.getenv("LLM_TARGET_BACKTEST_STOP_LOSS_PCT", "0.5"))
BACKTEST_COOLDOWN_MINUTES = float(
    os.getenv("LLM_TARGET_BACKTEST_COOLDOWN_MINUTES", "60")
)
BACKTEST_FEE_BPS = float(os.getenv("LLM_TARGET_BACKTEST_FEE_BPS", "0.0"))
BACKTEST_RECENT_LIMIT = int(os.getenv("LLM_TARGET_BACKTEST_RECENT_LIMIT", "25"))
SNAPSHOT_ROTATE_DAILY = os.getenv(
    "LLM_TARGET_BACKTEST_ROTATE_DAILY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")


def now_utc():
    return datetime.now(timezone.utc)


def numeric_or_none(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def load_jsonl(path):
    if not os.path.exists(path):
        return []

    rows = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            try:
                rows.append(json.loads(text))
            except Exception:
                continue
    return rows


def rotated_snapshot_path(base_path, dt):
    root, ext = os.path.splitext(base_path)
    suffix = dt.strftime("%Y%m%d")
    return f"{root}_{suffix}{ext or '.jsonl'}"


def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def snapshot_source_files(base_path, since_dt, until_dt):
    rotated_files = []
    if SNAPSHOT_ROTATE_DAILY:
        for day in daterange(since_dt.date(), until_dt.date()):
            rotated_path = rotated_snapshot_path(
                base_path,
                datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            )
            if os.path.exists(rotated_path):
                rotated_files.append(os.path.abspath(rotated_path))

    if rotated_files:
        return rotated_files

    if os.path.exists(base_path):
        return [os.path.abspath(base_path)]

    return []


def expected_snapshot_source_files(base_path, since_dt, until_dt):
    if SNAPSHOT_ROTATE_DAILY:
        return [
            os.path.abspath(rotated_snapshot_path(
                base_path,
                datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc)
            ))
            for day in daterange(since_dt.date(), until_dt.date())
        ]

    return [os.path.abspath(base_path)]


def snapshot_file_metadata(paths):
    metadata = []
    for path in paths:
        exists = os.path.exists(path)
        row_count = None
        if exists:
            try:
                with open(path, encoding="utf-8") as f:
                    row_count = sum(1 for line in f if line.strip())
            except Exception:
                row_count = None

        metadata.append({
            "path": path,
            "exists": exists,
            "size_bytes": os.path.getsize(path) if exists else None,
            "row_count": row_count,
        })

    return metadata


def strategy_config(snapshot):
    profile = snapshot.get("strategy_profile", {})
    payload = profile.get("payload", {})
    return payload if isinstance(payload, dict) else {}


def strategy_value(snapshot, key, default):
    value = strategy_config(snapshot).get(key, default)
    return default if value is None else value


def extract_price(snapshot):
    ticker = snapshot.get("ticker", {})
    last_price = numeric_or_none(ticker.get("last_price"))
    if last_price is not None:
        return last_price

    signal_payload = (snapshot.get("signal") or {}).get("payload") or {}
    signal_price = numeric_or_none(signal_payload.get("btc_price"))
    if signal_price is not None:
        return signal_price

    target_quality_payload = (snapshot.get("target_quality") or {}).get("payload") or {}
    quality_price = numeric_or_none(target_quality_payload.get("current_price"))
    if quality_price is not None:
        return quality_price

    return None


def signal_payload(snapshot):
    payload = (snapshot.get("signal") or {}).get("payload")
    return payload if isinstance(payload, dict) else {}


def quality_payload(snapshot):
    payload = (snapshot.get("target_quality") or {}).get("payload")
    return payload if isinstance(payload, dict) else {}


def snapshot_timestamp(snapshot):
    return parse_iso8601(snapshot.get("captured_at"))


def select_target_candidate(snapshot, current_price, last_sell_price=None):
    signal = signal_payload(snapshot)
    targets = signal.get("target_prices", [])
    if not isinstance(targets, list):
        return None

    max_premium_pct = float(strategy_value(snapshot, "target_limit_max_premium_pct", 0.0005))
    prevent_buy_above_last_sell = str(
        strategy_value(snapshot, "prevent_buy_above_last_sell", True)
    ).strip().lower() in ("1", "true", "yes", "on")
    buy_after_sell_discount_pct = float(
        strategy_value(snapshot, "buy_after_sell_discount_pct", 0.001)
    )

    max_price = current_price * (1 + max_premium_pct)
    max_rebuy_price = None
    if prevent_buy_above_last_sell and last_sell_price is not None:
        max_rebuy_price = last_sell_price * (1 - buy_after_sell_discount_pct)

    valid = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        buy_price = numeric_or_none(target.get("buy_price"))
        if buy_price is None or buy_price <= 0:
            continue
        if buy_price > max_price:
            continue
        if max_rebuy_price is not None and buy_price >= max_rebuy_price:
            continue
        valid.append({
            "buy_price": buy_price,
            "signal_sell_pct": target.get("sell_pct")
        })

    if not valid:
        return None

    return max(valid, key=lambda item: item["buy_price"])


def quality_decision(snapshot, candidate, strategy_name):
    config = strategy_config(snapshot)

    if strategy_name == "price_target_only":
        return {
            "allowed": True,
            "reason": "price_target_only",
            "policy": {
                "recommendation": "price_target_only",
                "reason": "Sentiment and target quality ignored for baseline."
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    signal = signal_payload(snapshot)
    action_recommendation = signal.get("action_recommendation")
    action_policy = signal.get("action_policy", {})
    if not isinstance(action_policy, dict):
        action_policy = {}

    if action_recommendation != "bullish_allowed":
        return {
            "allowed": False,
            "reason": "blocked",
            "policy": {
                "recommendation": action_recommendation,
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": None
        }

    if strategy_name == "sentiment_policy_only":
        return {
            "allowed": True,
            "reason": "sentiment_policy_only",
            "policy": {
                "recommendation": "bullish_allowed",
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    target_quality_enabled = str(
        config.get("target_quality_enabled", True)
    ).strip().lower() in ("1", "true", "yes", "on")
    target_quality_fail_closed = str(
        config.get("target_quality_fail_closed", False)
    ).strip().lower() in ("1", "true", "yes", "on")
    min_samples = int(config.get("target_quality_min_samples", 20))
    min_ev_pct = float(config.get("target_quality_min_ev_pct", 0.02))
    min_fill_probability = float(
        config.get("target_quality_min_4h_fill_probability", 0.35)
    )
    allowed_recommendations = {
        value.strip()
        for value in str(
            config.get("target_quality_allowed_recommendations", "buy_allowed,watch")
        ).split(",")
        if value.strip()
    }

    if not target_quality_enabled:
        return {
            "allowed": True,
            "reason": "target_quality_disabled",
            "policy": {
                "recommendation": "bullish_allowed",
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    quality = quality_payload(snapshot)
    quality_status_ok = (
        (snapshot.get("target_quality") or {}).get("ok")
        and isinstance(quality, dict)
        and quality.get("status") == "ok"
    )
    if not quality_status_ok:
        unavailable = unavailable_quality_decision(
            {"available": False, "reason": (snapshot.get("target_quality") or {}).get("error") or "target_quality_unavailable"},
            fail_closed=target_quality_fail_closed
        )
        return {
            "allowed": unavailable["allowed"],
            "reason": unavailable["reason"],
            "policy": {
                "recommendation": "bullish_allowed",
                "reason": action_policy.get("reason") or signal.get("reason")
            },
            "profit_target_pct": normalize_profit_target_pct(
                candidate.get("signal_sell_pct")
            )
        }

    matched = match_quality_target(candidate["buy_price"], quality.get("targets", []))
    evaluation = evaluate_quality_target(
        matched,
        min_samples=min_samples,
        min_ev_pct=min_ev_pct,
        min_4h_fill_probability=min_fill_probability,
        allowed_recommendations=allowed_recommendations
    )
    profit_target_pct = normalize_profit_target_pct(
        evaluation.get("best_profit_target_pct")
    )
    if profit_target_pct is None:
        profit_target_pct = normalize_profit_target_pct(candidate.get("signal_sell_pct"))

    return {
        "allowed": evaluation["allowed"],
        "reason": evaluation["reason"],
        "policy": {
            "recommendation": evaluation.get("recommendation"),
            "reason": evaluation["reason"],
            "matched_sample_count": evaluation.get("matched_sample_count"),
            "fill_probability_4h": evaluation.get("fill_probability_4h"),
            "best_expected_value_pct_per_signal": evaluation.get(
                "best_expected_value_pct_per_signal"
            ),
            "best_profit_target_pct": evaluation.get("best_profit_target_pct")
        },
        "profit_target_pct": profit_target_pct
    }


def exit_prices(entry_price, target_profit_pct):
    tp_price = entry_price * (1 + target_profit_pct)
    sl_price = entry_price * (1 - (BACKTEST_STOP_LOSS_PCT / 100.0))
    return tp_price, sl_price


def compute_trade_stats(entry_price, exit_price, high_water, low_water):
    gross_return_pct = ((exit_price - entry_price) / entry_price) * 100.0
    fee_pct = BACKTEST_FEE_BPS / 100.0
    net_return_pct = gross_return_pct - fee_pct
    max_runup_pct = ((high_water - entry_price) / entry_price) * 100.0
    max_drawdown_pct = ((low_water - entry_price) / entry_price) * 100.0
    return gross_return_pct, net_return_pct, max_runup_pct, max_drawdown_pct


def empty_summary():
    return {
        "trades": 0,
        "win_rate": None,
        "avg_net_return_pct": None,
        "total_net_return_pct": None,
        "avg_hold_minutes": None,
        "take_profit_count": 0,
        "stop_loss_count": 0,
        "timeout_count": 0,
        "max_drawdown_pct": None,
        "max_runup_pct": None,
        "raw_candidates": 0,
        "approved_candidates": 0,
        "candidate_signals": 0,
        "blocked_by_sentiment": 0,
        "blocked_by_target_quality": 0,
        "missing_signal": 0,
        "missing_price": 0,
        "no_target": 0,
        "not_filled": 0,
        "skipped_during_position": 0
    }


def finalize_summary(summary, trades):
    summary["trades"] = len(trades)
    approved_candidates = summary["approved_candidates"]
    if approved_candidates > 0:
        summary["fill_rate_after_approval"] = round(
            (len(trades) + summary["not_filled"]) / approved_candidates,
            4
        )
    else:
        summary["fill_rate_after_approval"] = None

    if not trades:
        return summary

    winning = [trade for trade in trades if trade["net_return_pct"] > 0]
    summary["win_rate"] = round(len(winning) / len(trades), 4)
    summary["avg_net_return_pct"] = round(
        sum(trade["net_return_pct"] for trade in trades) / len(trades),
        6
    )
    summary["total_net_return_pct"] = round(
        sum(trade["net_return_pct"] for trade in trades),
        6
    )
    summary["avg_hold_minutes"] = round(
        sum(trade["hold_minutes"] for trade in trades) / len(trades),
        2
    )
    summary["max_drawdown_pct"] = round(
        min(trade["max_drawdown_pct"] for trade in trades),
        6
    )
    summary["max_runup_pct"] = round(
        max(trade["max_runup_pct"] for trade in trades),
        6
    )
    return summary


def simulate_strategy(strategy_name, snapshots):
    summary = empty_summary()
    recent_decisions = []
    trades = []
    position = None
    pending_entry = None
    cooldown_until = None
    last_sell_price = None

    for snapshot in snapshots:
        timestamp = snapshot_timestamp(snapshot)
        price = extract_price(snapshot)
        signal = signal_payload(snapshot)

        if timestamp is None:
            continue

        if pending_entry is not None:
            if price is not None and price <= pending_entry["buy_target"]["buy_price"]:
                position = {
                    **pending_entry,
                    "filled_at": timestamp,
                    "entry_price": pending_entry["buy_target"]["buy_price"],
                    "high_water": pending_entry["buy_target"]["buy_price"],
                    "low_water": pending_entry["buy_target"]["buy_price"],
                    "exit_deadline": timestamp + timedelta(hours=BACKTEST_MAX_HOLD_HOURS)
                }
                pending_entry = None
            elif timestamp > pending_entry["fill_deadline"]:
                summary["not_filled"] += 1
                pending_entry = None

        if position is not None and price is not None:
            position["high_water"] = max(position["high_water"], price)
            position["low_water"] = min(position["low_water"], price)
            tp_price, sl_price = exit_prices(
                position["entry_price"],
                position["target_profit_pct"]
            )
            exit_reason = None
            exit_price = None

            if price >= tp_price:
                exit_reason = "take_profit"
                exit_price = tp_price
                summary["take_profit_count"] += 1
            elif price <= sl_price:
                exit_reason = "stop_loss"
                exit_price = sl_price
                summary["stop_loss_count"] += 1
            elif timestamp >= position["exit_deadline"]:
                exit_reason = "timeout"
                exit_price = price
                summary["timeout_count"] += 1

            if exit_reason is not None:
                hold_minutes = (
                    timestamp - position["filled_at"]
                ).total_seconds() / 60.0
                gross_return_pct, net_return_pct, max_runup_pct, max_drawdown_pct = (
                    compute_trade_stats(
                        position["entry_price"],
                        exit_price,
                        position["high_water"],
                        position["low_water"]
                    )
                )
                trade = {
                    "strategy": strategy_name,
                    "decision_time": position["decision_time"].isoformat(),
                    "signal_timestamp": position["signal_timestamp"],
                    "decision_price": position["decision_price"],
                    "buy_target": position["buy_target"],
                    "filled_at": position["filled_at"].isoformat(),
                    "entry_price": position["entry_price"],
                    "exit_time": timestamp.isoformat(),
                    "exit_price": round(exit_price, 2),
                    "exit_reason": exit_reason,
                    "gross_return_pct": round(gross_return_pct, 6),
                    "net_return_pct": round(net_return_pct, 6),
                    "max_runup_pct": round(max_runup_pct, 6),
                    "max_drawdown_pct": round(max_drawdown_pct, 6),
                    "hold_minutes": round(hold_minutes, 2),
                    "execution_signal": position["execution_signal"],
                    "confidence": position["confidence"],
                    "contributor_count": position["contributor_count"],
                    "policy": position["policy"],
                }
                trades.append(trade)
                last_sell_price = exit_price
                cooldown_until = timestamp + timedelta(minutes=BACKTEST_COOLDOWN_MINUTES)
                position = None

        if position is not None or pending_entry is not None:
            summary["skipped_during_position"] += 1
            continue

        if cooldown_until is not None and timestamp < cooldown_until:
            summary["skipped_during_position"] += 1
            continue

        if price is None:
            summary["missing_price"] += 1
            continue

        if not signal:
            summary["missing_signal"] += 1
            continue

        candidate = select_target_candidate(snapshot, price, last_sell_price)
        if candidate is None:
            summary["no_target"] += 1
            continue

        summary["raw_candidates"] += 1
        decision = quality_decision(snapshot, candidate, strategy_name)
        policy = decision["policy"]
        if strategy_name != "price_target_only" and not decision["allowed"]:
            if decision["reason"] == "blocked":
                summary["blocked_by_sentiment"] += 1
            else:
                summary["blocked_by_target_quality"] += 1
            recent_decisions.append({
                "strategy": strategy_name,
                "decision_time": timestamp.isoformat(),
                "signal_timestamp": signal.get("processed_at"),
                "decision_price": price,
                "buy_target": candidate,
                "policy": policy
            })
            continue

        summary["approved_candidates"] += 1
        summary["candidate_signals"] += 1
        profit_target_pct = decision["profit_target_pct"]
        if profit_target_pct is None:
            profit_target_pct = float(
                strategy_value(snapshot, "target_profit_pct", 0.005)
            )

        pending_entry = {
            "strategy": strategy_name,
            "decision_time": timestamp,
            "signal_timestamp": signal.get("processed_at"),
            "decision_price": price,
            "buy_target": candidate,
            "policy": policy,
            "fill_deadline": timestamp + timedelta(hours=BACKTEST_ENTRY_WAIT_HOURS),
            "target_profit_pct": profit_target_pct,
            "execution_signal": numeric_or_none(signal.get("execution_signal")),
            "confidence": numeric_or_none(signal.get("confidence")),
            "contributor_count": signal.get("contributor_count"),
        }
        recent_decisions.append({
            "strategy": strategy_name,
            "decision_time": timestamp.isoformat(),
            "signal_timestamp": signal.get("processed_at"),
            "decision_price": price,
            "buy_target": candidate,
            "policy": policy
        })

    recent_decisions = recent_decisions[-BACKTEST_RECENT_LIMIT:]
    recent_trades = trades[-BACKTEST_RECENT_LIMIT:]
    return {
        "summary": finalize_summary(summary, trades),
        "recent_decisions": recent_decisions,
        "recent_trades": recent_trades
    }


def top_summary(strategies):
    summary_rows = []
    for name, payload in strategies.items():
        summary = payload["summary"]
        total_net = summary.get("total_net_return_pct")
        trades = summary.get("trades", 0)
        if trades > 0 and total_net is not None:
            score = (1, total_net, summary.get("win_rate") or 0.0)
        else:
            score = (0, float("-inf"), float("-inf"))
        summary_rows.append((score, name, summary))

    best_score, best_name, best_summary = max(summary_rows, key=lambda item: item[0])
    best_strategy = None
    best_strategy_reason = "No strategy produced any completed trades in this window."
    if best_score[0] > 0:
        best_strategy = best_name
        best_strategy_reason = (
            f"Best completed-trade result by total net return over the window: "
            f"{best_summary['total_net_return_pct']}% across {best_summary['trades']} trades."
        )

    return {
        "best_strategy": best_strategy,
        "best_strategy_reason": best_strategy_reason,
        "strategy_headlines": {
            name: {
                "raw_candidates": payload["summary"].get("raw_candidates"),
                "approved_candidates": payload["summary"].get("approved_candidates"),
                "trades": payload["summary"].get("trades"),
                "win_rate": payload["summary"].get("win_rate"),
                "total_net_return_pct": payload["summary"].get("total_net_return_pct"),
                "fill_rate_after_approval": payload["summary"].get(
                    "fill_rate_after_approval"
                ),
            }
            for name, payload in strategies.items()
        }
    }


def build_report():
    now = now_utc()
    since_dt = now - timedelta(hours=BACKTEST_WINDOW_HOURS)
    expected_snapshot_files = expected_snapshot_source_files(
        SNAPSHOT_LOG_FILE,
        since_dt,
        now
    )
    snapshot_files = snapshot_source_files(SNAPSHOT_LOG_FILE, since_dt, now)
    all_snapshots = []
    for path in snapshot_files:
        all_snapshots.extend(load_jsonl(path))
    snapshots = [
        snapshot
        for snapshot in all_snapshots
        if (snapshot_timestamp(snapshot) or datetime.min.replace(tzinfo=timezone.utc)) >= since_dt
    ]
    snapshots.sort(key=lambda snapshot: snapshot_timestamp(snapshot) or datetime.min.replace(tzinfo=timezone.utc))
    filtered_out_by_window = len(all_snapshots) - len(snapshots)

    strategies = {
        "with_target_quality": simulate_strategy("with_target_quality", snapshots),
        "sentiment_policy_only": simulate_strategy("sentiment_policy_only", snapshots),
        "price_target_only": simulate_strategy("price_target_only", snapshots),
    }

    return {
        "timestamp": now.isoformat(),
        "snapshot_file": os.path.abspath(SNAPSHOT_LOG_FILE),
        "snapshot_files": snapshot_files,
        "snapshot_diagnostics": {
            "rotate_daily": SNAPSHOT_ROTATE_DAILY,
            "expected_snapshot_files": expected_snapshot_files,
            "found_snapshot_files": snapshot_files,
            "expected_file_metadata": snapshot_file_metadata(expected_snapshot_files),
            "loaded_snapshot_count": len(all_snapshots),
            "filtered_out_by_window": filtered_out_by_window,
            "empty_window_reason": (
                "no snapshot files found"
                if not snapshot_files else
                "snapshot files had no rows"
                if not all_snapshots else
                "all loaded snapshots were older than the report window"
                if not snapshots else
                None
            ),
        },
        "since": since_dt.isoformat(),
        "snapshot_count": len(snapshots),
        "simulation": {
            "side": "long_only",
            "clock": "snapshot timestamps",
            "entry": "limit buy at highest eligible llm target below current price",
            "entry_wait_hours": BACKTEST_ENTRY_WAIT_HOURS,
            "stop_loss_pct": BACKTEST_STOP_LOSS_PCT,
            "max_hold_hours": BACKTEST_MAX_HOLD_HOURS,
            "cooldown_minutes": BACKTEST_COOLDOWN_MINUTES,
            "fee_bps": BACKTEST_FEE_BPS
        },
        "top_summary": top_summary(strategies),
        "strategies": strategies,
        "bot_outputs": {
            name: payload["summary"]
            for name, payload in strategies.items()
        }
    }


def write_report(report):
    output_dir = os.path.dirname(BACKTEST_OUTPUT_FILE)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(BACKTEST_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    os.makedirs(BACKTEST_ARCHIVE_DIR, exist_ok=True)
    archive_file = os.path.join(
        BACKTEST_ARCHIVE_DIR,
        f"llm_target_backtest_{now_utc().strftime('%Y%m%d')}.json"
    )
    with open(archive_file, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    return archive_file


def main():
    report = build_report()
    archive_file = write_report(report)
    print(json.dumps({
        "timestamp": report["timestamp"],
        "output_file": BACKTEST_OUTPUT_FILE,
        "archive_file": archive_file,
        "snapshot_count": report["snapshot_count"]
    }))


if __name__ == "__main__":
    main()
