#!/usr/bin/env python3

import argparse
import json
import os
from collections import Counter
from datetime import datetime, timedelta, timezone

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

DEFAULT_SNAPSHOT_LOG_FILE = "competition_backtest_snapshot_log.jsonl"
DEFAULT_CONFIG_FILE = "competition_bot_config.json"
DEFAULT_OUTPUT_FILE = "competition_backtest.json"


def parse_bool(value, default=False):
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("1", "true", "yes", "on")


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="Replay competition decision snapshots for simulated P&L."
    )
    parser.add_argument(
        "--config-file",
        default=os.getenv("COMPETITION_CONFIG_FILE", DEFAULT_CONFIG_FILE),
        help="Competition config JSON file.",
    )
    parser.add_argument(
        "--snapshot-file",
        help="Base JSONL snapshot file.",
    )
    parser.add_argument(
        "--output-file",
        help="JSON output path.",
    )
    parser.add_argument(
        "--window-hours",
        type=float,
        help="Lookback window to replay.",
    )
    parser.add_argument(
        "--trade-usd",
        type=float,
        help="Fallback simulated trade notional.",
    )
    parser.add_argument(
        "--take-profit-pct",
        type=float,
        help="Take-profit percent, e.g. 0.8 for 0.8%%.",
    )
    parser.add_argument(
        "--stop-loss-pct",
        type=float,
        help="Stop-loss percent, e.g. 0.8 for 0.8%%.",
    )
    parser.add_argument(
        "--max-hold-minutes",
        type=float,
        help="Maximum simulated hold time before timeout exit.",
    )
    parser.add_argument(
        "--cooldown-minutes",
        type=float,
        help="Cooldown after an exit before another entry.",
    )
    parser.add_argument(
        "--fee-bps",
        type=float,
        help="Round-trip fee in basis points.",
    )
    parser.add_argument(
        "--fill-model",
        choices=("mid", "taker", "maker"),
        help="Execution model: mid, taker (ask in/bid out), or maker (bid in/ask out).",
    )
    parser.add_argument(
        "--maker-fee-bps",
        type=float,
        help="Round-trip maker fee in basis points; defaults to --fee-bps.",
    )
    parser.add_argument(
        "--taker-fee-bps",
        type=float,
        help="Round-trip taker fee in basis points; defaults to --fee-bps.",
    )
    parser.add_argument(
        "--require-signal-reset",
        help="After an exit, require a blocked snapshot before re-entry.",
    )
    parser.add_argument(
        "--maker-order-timeout-minutes",
        type=float,
        help="Minutes before an unfilled post-only entry is cancelled.",
    )
    parser.add_argument(
        "--maker-cancel-on-signal-block",
        help="Cancel a pending maker entry when its entry signal becomes blocked.",
    )
    parser.add_argument(
        "--min-aggression-score",
        type=float,
        help="Minimum directional aggression score required for entry.",
    )
    parser.add_argument(
        "--min-trade-count",
        type=int,
        help="Minimum recent trade count required for directional entry.",
    )
    parser.add_argument(
        "--min-total-notional-usd",
        type=float,
        help="Minimum recent trade notional required for directional entry.",
    )
    parser.add_argument(
        "--recent-limit",
        type=int,
        help="Number of recent simulated trades to include.",
    )
    parser.add_argument(
        "--rotate-daily",
        default=os.getenv("COMPETITION_BACKTEST_ROTATE_DAILY", "true"),
        help="Whether to read date-suffixed JSONL files.",
    )
    return parser.parse_args()


def load_config(path):
    if not path or not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def env_config_value(env_name, config, key, default=None):
    value = os.getenv(env_name)
    if value is not None:
        return value
    return config.get(key, default)


def runtime_from_args(args):
    config = load_config(args.config_file)

    def value(arg_value, env_name, key, default):
        if arg_value is not None:
            return arg_value
        return env_config_value(env_name, config, key, default)

    return argparse.Namespace(**{
        "config_file": args.config_file,
        "snapshot_file": value(
            args.snapshot_file,
            "COMPETITION_BACKTEST_SNAPSHOT_FILE",
            "backtest_snapshot_file",
            DEFAULT_SNAPSHOT_LOG_FILE,
        ),
        "output_file": value(
            args.output_file,
            "COMPETITION_BACKTEST_OUTPUT_FILE",
            "backtest_output_file",
            DEFAULT_OUTPUT_FILE,
        ),
        "window_hours": float(value(
            args.window_hours,
            "COMPETITION_BACKTEST_WINDOW_HOURS",
            "backtest_window_hours",
            24,
        )),
        "trade_usd": float(value(
            args.trade_usd,
            "COMPETITION_BACKTEST_TRADE_USD",
            "backtest_trade_usd",
            25,
        )),
        "take_profit_pct": float(value(
            args.take_profit_pct,
            "COMPETITION_BACKTEST_TAKE_PROFIT_PCT",
            "backtest_take_profit_pct",
            0.8,
        )),
        "stop_loss_pct": float(value(
            args.stop_loss_pct,
            "COMPETITION_BACKTEST_STOP_LOSS_PCT",
            "backtest_stop_loss_pct",
            0.8,
        )),
        "max_hold_minutes": float(value(
            args.max_hold_minutes,
            "COMPETITION_BACKTEST_MAX_HOLD_MINUTES",
            "backtest_max_hold_minutes",
            60,
        )),
        "cooldown_minutes": float(value(
            args.cooldown_minutes,
            "COMPETITION_BACKTEST_COOLDOWN_MINUTES",
            "backtest_cooldown_minutes",
            0,
        )),
        "fee_bps": float(value(
            args.fee_bps,
            "COMPETITION_BACKTEST_FEE_BPS",
            "backtest_fee_bps",
            40,
        )),
        "fill_model": str(value(
            args.fill_model,
            "COMPETITION_BACKTEST_FILL_MODEL",
            "backtest_fill_model",
            "mid",
        )).strip().lower(),
        "maker_fee_bps": float(value(
            args.maker_fee_bps,
            "COMPETITION_BACKTEST_MAKER_FEE_BPS",
            "backtest_maker_fee_bps",
            value(
                args.fee_bps,
                "COMPETITION_BACKTEST_FEE_BPS",
                "backtest_fee_bps",
                40,
            ),
        )),
        "taker_fee_bps": float(value(
            args.taker_fee_bps,
            "COMPETITION_BACKTEST_TAKER_FEE_BPS",
            "backtest_taker_fee_bps",
            value(
                args.fee_bps,
                "COMPETITION_BACKTEST_FEE_BPS",
                "backtest_fee_bps",
                40,
            ),
        )),
        "require_signal_reset": parse_bool(value(
            args.require_signal_reset,
            "COMPETITION_BACKTEST_REQUIRE_SIGNAL_RESET",
            "backtest_require_signal_reset",
            False,
        )),
        "maker_order_timeout_minutes": float(value(
            args.maker_order_timeout_minutes,
            "COMPETITION_BACKTEST_MAKER_ORDER_TIMEOUT_MINUTES",
            "backtest_maker_order_timeout_minutes",
            5,
        )),
        "maker_cancel_on_signal_block": parse_bool(value(
            args.maker_cancel_on_signal_block,
            "COMPETITION_BACKTEST_MAKER_CANCEL_ON_SIGNAL_BLOCK",
            "backtest_maker_cancel_on_signal_block",
            True,
        )),
        "min_aggression_score": float(value(
            args.min_aggression_score,
            "COMPETITION_BACKTEST_MIN_AGGRESSION_SCORE",
            "backtest_min_aggression_score",
            0.15,
        )),
        "min_trade_count": int(value(
            args.min_trade_count,
            "COMPETITION_BACKTEST_MIN_TRADE_COUNT",
            "backtest_min_trade_count",
            5,
        )),
        "min_total_notional_usd": float(value(
            args.min_total_notional_usd,
            "COMPETITION_BACKTEST_MIN_TOTAL_NOTIONAL_USD",
            "backtest_min_total_notional_usd",
            1000,
        )),
        "recent_limit": int(value(
            args.recent_limit,
            "COMPETITION_BACKTEST_RECENT_LIMIT",
            "backtest_recent_limit",
            25,
        )),
        "rotate_daily": parse_bool(args.rotate_daily, default=True),
    })


def now_utc():
    return datetime.now(timezone.utc)


def parse_iso8601(value):
    if not value:
        return None
    try:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        ts = datetime.fromisoformat(text)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    except Exception:
        return None


def safe_float(value):
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
    return f"{root}_{dt.strftime('%Y%m%d')}{ext or '.jsonl'}"


def daterange(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def snapshot_source_files(base_path, since_dt, until_dt, rotate_daily=True):
    if rotate_daily:
        files = []
        for day in daterange(since_dt.date(), until_dt.date()):
            path = rotated_snapshot_path(
                base_path,
                datetime.combine(day, datetime.min.time(), tzinfo=timezone.utc),
            )
            if os.path.exists(path):
                files.append(os.path.abspath(path))
        if files:
            return files

    if os.path.exists(base_path):
        return [os.path.abspath(base_path)]

    return []


def snapshot_timestamp(snapshot):
    return parse_iso8601(snapshot.get("captured_at"))


def decision_payload(snapshot):
    payload = (snapshot.get("decision") or {}).get("payload")
    return payload if isinstance(payload, dict) else {}


def decision_summary(snapshot):
    summary = (snapshot.get("decision") or {}).get("summary")
    if isinstance(summary, dict) and summary:
        return summary

    payload = decision_payload(snapshot)
    competition = payload.get("competition") or {}
    market = payload.get("market") or {}
    risk = payload.get("risk") or {}
    return {
        "status": payload.get("status"),
        "decision": payload.get("decision"),
        "reason": payload.get("reason"),
        "source_age_minutes": payload.get("source_age_minutes"),
        "source_stale_after_minutes": payload.get("source_stale_after_minutes"),
        "asset_id": competition.get("asset_id"),
        "kraken_pair": competition.get("kraken_pair"),
        "last_price": market.get("last_price"),
        "mid_price": market.get("mid_price"),
        "spread_bps": market.get("spread_bps"),
        "trade_count": market.get("trade_count"),
        "total_notional_usd": market.get("total_notional_usd"),
        "aggression_score": market.get("aggression_score"),
        "shadow_only": risk.get("shadow_only"),
        "live_trading_enabled": risk.get("live_trading_enabled"),
        "max_position_usd": risk.get("max_position_usd"),
        "filter_failures": payload.get("filter_failures"),
    }


def snapshot_price(snapshot):
    summary = decision_summary(snapshot)
    return (
        safe_float(summary.get("mid_price"))
        or safe_float(summary.get("last_price"))
    )


def snapshot_last_price(snapshot):
    summary = decision_summary(snapshot)
    return safe_float(summary.get("last_price")) or safe_float(summary.get("mid_price"))


def snapshot_execution_price(snapshot, side, fill_model):
    price = snapshot_price(snapshot)
    if price is None or fill_model == "mid":
        return price

    spread_bps = safe_float(decision_summary(snapshot).get("spread_bps"))
    if spread_bps is None or spread_bps < 0:
        return None

    half_spread = spread_bps / 20000.0
    if fill_model == "taker":
        multiplier = 1.0 + half_spread if side == "buy" else 1.0 - half_spread
    elif fill_model == "maker":
        multiplier = 1.0 - half_spread if side == "buy" else 1.0 + half_spread
    else:
        raise ValueError(f"Unsupported fill model: {fill_model}")
    return price * multiplier


def snapshot_is_fresh_ok(snapshot):
    summary = decision_summary(snapshot)
    if summary.get("status") != "ok":
        return False, "status_not_ok"

    age = safe_float(summary.get("source_age_minutes"))
    stale_after = safe_float(summary.get("source_stale_after_minutes"))
    if age is None or stale_after is None:
        return False, "missing_source_freshness"
    if age > stale_after:
        return False, "source_snapshot_stale"

    return True, None


def competition_allows_entry(snapshot):
    fresh, reason = snapshot_is_fresh_ok(snapshot)
    if not fresh:
        return False, reason

    summary = decision_summary(snapshot)
    if summary.get("shadow_only") is not True:
        return False, "not_shadow_only"
    if summary.get("decision") != "shadow_candidate":
        return False, summary.get("reason") or "decision_not_shadow_candidate"

    return True, None


def competition_directional_allows_entry(
    snapshot,
    *,
    min_aggression_score,
    min_trade_count,
    min_total_notional_usd,
):
    allowed, reason = competition_allows_entry(snapshot)
    if not allowed:
        return False, reason

    summary = decision_summary(snapshot)
    aggression_score = safe_float(summary.get("aggression_score"))
    trade_count = safe_float(summary.get("trade_count"))
    total_notional_usd = safe_float(summary.get("total_notional_usd"))

    if aggression_score is None:
        return False, "directional_aggression_missing"
    if aggression_score < min_aggression_score:
        return False, "directional_aggression_below_min"
    if trade_count is None:
        return False, "directional_trade_count_missing"
    if trade_count < min_trade_count:
        return False, "directional_trade_count_below_min"
    if total_notional_usd is None:
        return False, "directional_notional_missing"
    if total_notional_usd < min_total_notional_usd:
        return False, "directional_notional_below_min"

    return True, None


def simulated_buy_allows_entry(snapshot):
    fresh, reason = snapshot_is_fresh_ok(snapshot)
    if not fresh:
        return False, reason
    return True, None


def entry_notional_usd(snapshot, fallback):
    max_position = safe_float(decision_summary(snapshot).get("max_position_usd"))
    if max_position is not None and max_position > 0:
        return min(fallback, max_position)
    return fallback


def return_pct(entry_price, exit_price):
    return ((exit_price - entry_price) / entry_price) * 100.0


def net_pnl_usd(notional, gross_return_pct, fee_bps):
    return notional * ((gross_return_pct / 100.0) - (fee_bps / 10000.0))


def empty_strategy_summary(name):
    return {
        "strategy": name,
        "snapshots": 0,
        "entries": 0,
        "closed_trades": 0,
        "open_trades": 0,
        "wins": 0,
        "losses": 0,
        "take_profit_count": 0,
        "stop_loss_count": 0,
        "timeout_count": 0,
        "mark_to_market_count": 0,
        "maker_orders_placed": 0,
        "maker_orders_filled": 0,
        "maker_orders_cancelled": 0,
        "maker_orders_expired": 0,
        "maker_orders_open": 0,
        "realized_gross_pnl_usd": 0.0,
        "realized_net_pnl_usd": 0.0,
        "unrealized_gross_pnl_usd": 0.0,
        "unrealized_net_pnl_usd": 0.0,
        "gross_pnl_usd": 0.0,
        "net_pnl_usd": 0.0,
        "avg_net_return_pct": None,
        "win_rate": None,
        "blocked_reasons": {},
    }


def close_trade(
    position,
    snapshot,
    exit_reason,
    fee_bps,
    fill_model,
    *,
    exit_price_override=None,
):
    exited_at = snapshot.get("captured_at")
    exit_price = exit_price_override
    if exit_price is None:
        exit_price = snapshot_execution_price(snapshot, "sell", fill_model)
    if exit_price is None:
        return None

    gross_return = return_pct(position["entry_price"], exit_price)
    gross_pnl = position["notional_usd"] * (gross_return / 100.0)
    net_pnl = net_pnl_usd(position["notional_usd"], gross_return, fee_bps)
    net_return = (net_pnl / position["notional_usd"]) * 100.0

    return {
        "strategy": position["strategy"],
        "entry_at": position["entry_at"],
        "exit_at": exited_at,
        "exit_reason": exit_reason,
        "entry_price": round(position["entry_price"], 8),
        "exit_price": round(exit_price, 8),
        "notional_usd": round(position["notional_usd"], 8),
        "gross_return_pct": round(gross_return, 6),
        "net_return_pct": round(net_return, 6),
        "gross_pnl_usd": round(gross_pnl, 8),
        "net_pnl_usd": round(net_pnl, 8),
        "entry_decision": position["entry_decision"],
        "entry_reason": position["entry_reason"],
        "entry_aggression_score": position.get("entry_aggression_score"),
        "entry_trade_count": position.get("entry_trade_count"),
        "entry_total_notional_usd": position.get("entry_total_notional_usd"),
        "entry_order_placed_at": position.get("entry_order_placed_at"),
        "fee_bps": round(fee_bps, 6),
        "position_status": "open" if exit_reason == "mark_to_market" else "closed",
    }


def update_summary_for_trade(summary, trade):
    summary["closed_trades"] += 1
    summary["gross_pnl_usd"] += trade["gross_pnl_usd"]
    summary["net_pnl_usd"] += trade["net_pnl_usd"]
    summary["realized_gross_pnl_usd"] += trade["gross_pnl_usd"]
    summary["realized_net_pnl_usd"] += trade["net_pnl_usd"]
    if trade["net_pnl_usd"] >= 0:
        summary["wins"] += 1
    else:
        summary["losses"] += 1

    reason_key = f"{trade['exit_reason']}_count"
    if reason_key in summary:
        summary[reason_key] += 1


def update_summary_for_open_trade(summary, trade):
    summary["open_trades"] = 1
    summary["mark_to_market_count"] += 1
    summary["gross_pnl_usd"] += trade["gross_pnl_usd"]
    summary["net_pnl_usd"] += trade["net_pnl_usd"]
    summary["unrealized_gross_pnl_usd"] += trade["gross_pnl_usd"]
    summary["unrealized_net_pnl_usd"] += trade["net_pnl_usd"]


def finalize_summary(summary, trades):
    money_fields = (
        "gross_pnl_usd",
        "net_pnl_usd",
        "realized_gross_pnl_usd",
        "realized_net_pnl_usd",
        "unrealized_gross_pnl_usd",
        "unrealized_net_pnl_usd",
    )
    for field in money_fields:
        summary[field] = round(summary[field], 8)
    if summary["closed_trades"]:
        summary["win_rate"] = round(summary["wins"] / summary["closed_trades"], 6)
        closed = [trade for trade in trades if trade["position_status"] == "closed"]
        avg = sum(trade["net_return_pct"] for trade in closed) / summary["closed_trades"]
        summary["avg_net_return_pct"] = round(avg, 6)
    summary["blocked_reasons"] = dict(Counter(summary["blocked_reasons"]).most_common())
    return summary


def replay_strategy(
    name,
    snapshots,
    allow_entry,
    *,
    trade_usd,
    take_profit_pct,
    stop_loss_pct,
    max_hold_minutes,
    cooldown_minutes,
    fee_bps,
    fill_model="mid",
    require_signal_reset=False,
    maker_order_timeout_minutes=5,
    maker_cancel_on_signal_block=True,
    maker_fee_bps=None,
    taker_fee_bps=None,
):
    summary = empty_strategy_summary(name)
    trades = []
    position = None
    pending_entry = None
    cooldown_until = None
    signal_armed = True
    blocked_reasons = Counter()
    maker_fee_bps = fee_bps if maker_fee_bps is None else maker_fee_bps
    taker_fee_bps = fee_bps if taker_fee_bps is None else taker_fee_bps

    for snapshot in snapshots:
        captured_at = snapshot_timestamp(snapshot)
        reference_price = snapshot_price(snapshot)
        if captured_at is None or reference_price is None or reference_price <= 0:
            continue

        summary["snapshots"] += 1

        if pending_entry is not None:
            allowed, reason = allow_entry(snapshot)
            age_minutes = (
                captured_at - pending_entry["placed_dt"]
            ).total_seconds() / 60.0
            if maker_cancel_on_signal_block and not allowed:
                summary["maker_orders_cancelled"] += 1
                blocked_reasons[reason or "maker_entry_signal_blocked"] += 1
                pending_entry = None
                signal_armed = True
                continue
            if age_minutes >= maker_order_timeout_minutes:
                summary["maker_orders_expired"] += 1
                pending_entry = None
                continue

            touch_price = snapshot_last_price(snapshot)
            if (
                captured_at > pending_entry["placed_dt"]
                and touch_price is not None
                and touch_price <= pending_entry["limit_price"]
            ):
                position = {
                    **pending_entry,
                    "entry_at": snapshot.get("captured_at"),
                    "entry_dt": captured_at,
                    "entry_price": pending_entry["limit_price"],
                    "entry_order_placed_at": pending_entry["placed_at"],
                }
                pending_entry = None
                summary["entries"] += 1
                summary["maker_orders_filled"] += 1
            else:
                continue

        if position is not None:
            held_minutes = (captured_at - position["entry_dt"]).total_seconds() / 60.0
            exit_price = snapshot_execution_price(snapshot, "sell", fill_model)
            if fill_model == "maker":
                exit_price = snapshot_execution_price(snapshot, "sell", "taker")
            if exit_price is None:
                continue
            gross_return = return_pct(position["entry_price"], exit_price)
            exit_reason = None
            exit_price_override = None
            trade_fee_bps = fee_bps
            if fill_model == "maker":
                target_price = position["entry_price"] * (1.0 + take_profit_pct / 100.0)
                stop_price = position["entry_price"] * (1.0 - abs(stop_loss_pct) / 100.0)
                touch_price = snapshot_last_price(snapshot)
                if touch_price is not None and touch_price >= target_price:
                    exit_reason = "take_profit"
                    exit_price_override = target_price
                    trade_fee_bps = maker_fee_bps
                elif touch_price is not None and touch_price <= stop_price:
                    exit_reason = "stop_loss"
                    trade_fee_bps = (maker_fee_bps + taker_fee_bps) / 2.0
                elif held_minutes >= max_hold_minutes:
                    exit_reason = "timeout"
                    trade_fee_bps = (maker_fee_bps + taker_fee_bps) / 2.0
            elif gross_return >= take_profit_pct:
                exit_reason = "take_profit"
            elif gross_return <= -abs(stop_loss_pct):
                exit_reason = "stop_loss"
            elif held_minutes >= max_hold_minutes:
                exit_reason = "timeout"

            if exit_reason:
                trade = close_trade(
                    position,
                    snapshot,
                    exit_reason,
                    trade_fee_bps,
                    "taker" if fill_model == "maker" else fill_model,
                    exit_price_override=exit_price_override,
                )
                if trade:
                    update_summary_for_trade(summary, trade)
                    trades.append(trade)
                    cooldown_until = captured_at + timedelta(minutes=cooldown_minutes)
                    if require_signal_reset:
                        signal_armed = False
                position = None
                continue

        if position is not None:
            continue
        if cooldown_until is not None and captured_at < cooldown_until:
            continue

        allowed, reason = allow_entry(snapshot)
        if not allowed:
            blocked_reasons[reason or "blocked"] += 1
            signal_armed = True
            continue
        if require_signal_reset and not signal_armed:
            blocked_reasons["waiting_for_signal_reset"] += 1
            continue

        summary_info = decision_summary(snapshot)
        notional = entry_notional_usd(snapshot, trade_usd)
        entry_price = snapshot_execution_price(snapshot, "buy", fill_model)
        if entry_price is None or entry_price <= 0:
            blocked_reasons["missing_spread_for_execution_model"] += 1
            continue
        new_position = {
            "strategy": name,
            "notional_usd": notional,
            "entry_decision": summary_info.get("decision"),
            "entry_reason": summary_info.get("reason"),
            "entry_aggression_score": safe_float(summary_info.get("aggression_score")),
            "entry_trade_count": safe_float(summary_info.get("trade_count")),
            "entry_total_notional_usd": safe_float(
                summary_info.get("total_notional_usd")
            ),
        }
        if fill_model == "maker":
            pending_entry = {
                **new_position,
                "placed_at": snapshot.get("captured_at"),
                "placed_dt": captured_at,
                "limit_price": entry_price,
            }
            summary["maker_orders_placed"] += 1
        else:
            position = {
                **new_position,
                "entry_at": snapshot.get("captured_at"),
                "entry_dt": captured_at,
                "entry_price": entry_price,
            }
            summary["entries"] += 1

    if position is not None:
        final_fee_bps = fee_bps
        final_model = fill_model
        if fill_model == "maker":
            final_fee_bps = (maker_fee_bps + taker_fee_bps) / 2.0
            final_model = "taker"
        trade = close_trade(
            position,
            snapshots[-1],
            "mark_to_market",
            final_fee_bps,
            final_model,
        )
        if trade:
            update_summary_for_open_trade(summary, trade)
            trades.append(trade)
    if pending_entry is not None:
        summary["maker_orders_open"] = 1

    summary["blocked_reasons"] = blocked_reasons
    return {
        "summary": finalize_summary(summary, trades),
        "trades": trades,
    }


def filter_snapshots(snapshots, since_dt, until_dt):
    filtered = []
    for snapshot in snapshots:
        ts = snapshot_timestamp(snapshot)
        if ts is None or ts < since_dt or ts > until_dt:
            continue
        filtered.append(snapshot)
    return sorted(filtered, key=snapshot_timestamp)


def load_snapshots(base_path, since_dt, until_dt, rotate_daily=True):
    files = snapshot_source_files(base_path, since_dt, until_dt, rotate_daily=rotate_daily)
    rows = []
    for path in files:
        rows.extend(load_jsonl(path))
    return filter_snapshots(rows, since_dt, until_dt), files


def build_backtest(args):
    until_dt = now_utc()
    since_dt = until_dt - timedelta(hours=args.window_hours)
    rotate_daily = parse_bool(args.rotate_daily, default=True)
    snapshots, source_files = load_snapshots(
        args.snapshot_file,
        since_dt,
        until_dt,
        rotate_daily=rotate_daily,
    )
    if args.fill_model not in ("mid", "taker", "maker"):
        raise ValueError("fill_model must be one of: mid, taker, maker")
    selected_fee_bps = {
        "mid": args.fee_bps,
        "taker": args.taker_fee_bps,
        "maker": args.maker_fee_bps,
    }[args.fill_model]

    def directional_allows_entry(snapshot):
        return competition_directional_allows_entry(
            snapshot,
            min_aggression_score=args.min_aggression_score,
            min_trade_count=args.min_trade_count,
            min_total_notional_usd=args.min_total_notional_usd,
        )

    scenarios = {
        "competition_allowed": replay_strategy(
            "competition_allowed",
            snapshots,
            competition_allows_entry,
            trade_usd=args.trade_usd,
            take_profit_pct=args.take_profit_pct,
            stop_loss_pct=args.stop_loss_pct,
            max_hold_minutes=args.max_hold_minutes,
            cooldown_minutes=args.cooldown_minutes,
            fee_bps=selected_fee_bps,
            fill_model=args.fill_model,
            require_signal_reset=args.require_signal_reset,
            maker_order_timeout_minutes=args.maker_order_timeout_minutes,
            maker_cancel_on_signal_block=args.maker_cancel_on_signal_block,
            maker_fee_bps=args.maker_fee_bps,
            taker_fee_bps=args.taker_fee_bps,
        ),
        "competition_directional": replay_strategy(
            "competition_directional",
            snapshots,
            directional_allows_entry,
            trade_usd=args.trade_usd,
            take_profit_pct=args.take_profit_pct,
            stop_loss_pct=args.stop_loss_pct,
            max_hold_minutes=args.max_hold_minutes,
            cooldown_minutes=args.cooldown_minutes,
            fee_bps=selected_fee_bps,
            fill_model=args.fill_model,
            require_signal_reset=args.require_signal_reset,
            maker_order_timeout_minutes=args.maker_order_timeout_minutes,
            maker_cancel_on_signal_block=args.maker_cancel_on_signal_block,
            maker_fee_bps=args.maker_fee_bps,
            taker_fee_bps=args.taker_fee_bps,
        ),
        "simulated_buy_allowed": replay_strategy(
            "simulated_buy_allowed",
            snapshots,
            simulated_buy_allows_entry,
            trade_usd=args.trade_usd,
            take_profit_pct=args.take_profit_pct,
            stop_loss_pct=args.stop_loss_pct,
            max_hold_minutes=args.max_hold_minutes,
            cooldown_minutes=args.cooldown_minutes,
            fee_bps=selected_fee_bps,
            fill_model=args.fill_model,
            require_signal_reset=False,
            maker_order_timeout_minutes=args.maker_order_timeout_minutes,
            maker_cancel_on_signal_block=args.maker_cancel_on_signal_block,
            maker_fee_bps=args.maker_fee_bps,
            taker_fee_bps=args.taker_fee_bps,
        ),
    }

    recent_limit = max(args.recent_limit, 0)
    for result in scenarios.values():
        result["recent_trades"] = result["trades"][-recent_limit:] if recent_limit else []
        del result["trades"]

    return {
        "timestamp": until_dt.isoformat(),
        "since": since_dt.isoformat(),
        "window_hours": args.window_hours,
        "snapshot_file_base": os.path.abspath(args.snapshot_file),
        "source_files": source_files,
        "snapshot_count": len(snapshots),
        "simulation": {
            "trade_usd": args.trade_usd,
            "take_profit_pct": args.take_profit_pct,
            "stop_loss_pct": args.stop_loss_pct,
            "max_hold_minutes": args.max_hold_minutes,
            "cooldown_minutes": args.cooldown_minutes,
            "fee_bps": selected_fee_bps,
            "maker_fee_bps": args.maker_fee_bps,
            "taker_fee_bps": args.taker_fee_bps,
            "fill_model": args.fill_model,
            "fill_model_detail": {
                "mid": "mid_or_last_price",
                "taker": "buy_at_inferred_ask_sell_at_inferred_bid",
                "maker": "post_only_bid_requires_later_trade_touch; maker_target; taker_stop_timeout",
            }[args.fill_model],
            "competition_require_signal_reset": args.require_signal_reset,
            "maker_order_timeout_minutes": args.maker_order_timeout_minutes,
            "maker_cancel_on_signal_block": args.maker_cancel_on_signal_block,
            "directional_min_aggression_score": args.min_aggression_score,
            "directional_min_trade_count": args.min_trade_count,
            "directional_min_total_notional_usd": args.min_total_notional_usd,
        },
        "strategies": scenarios,
    }


def write_json(path, payload):
    output_dir = os.path.dirname(path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)


def main():
    args = parse_cli_args()
    runtime = runtime_from_args(args)
    payload = build_backtest(runtime)
    write_json(runtime.output_file, payload)
    print(json.dumps({
        "output_file": os.path.abspath(runtime.output_file),
        "snapshot_count": payload["snapshot_count"],
        "competition_allowed": payload["strategies"]["competition_allowed"]["summary"],
        "competition_directional": payload["strategies"]["competition_directional"]["summary"],
        "simulated_buy_allowed": payload["strategies"]["simulated_buy_allowed"]["summary"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
