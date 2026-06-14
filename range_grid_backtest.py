#!/usr/bin/env python3

import json
import os
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

SNAPSHOT_LOG_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_SNAPSHOT_FILE",
    "range_grid_backtest_snapshot_log.jsonl"
)
SNAPSHOT_ROTATE_DAILY = os.getenv(
    "RANGE_GRID_BACKTEST_ROTATE_DAILY",
    "true"
).strip().lower() in ("1", "true", "yes", "on")
TRADE_LOG_FILE = (
    os.getenv("RANGE_GRID_TRADE_LOG_FILE")
    or os.getenv("TRADE_LOG_FILE")
    or "trade_log.jsonl"
)
BACKTEST_OUTPUT_FILE = os.getenv(
    "RANGE_GRID_BACKTEST_OUTPUT_FILE",
    "/var/www/html/bot/range_grid_backtest.json"
)
BACKTEST_ARCHIVE_DIR = os.getenv(
    "RANGE_GRID_BACKTEST_ARCHIVE_DIR",
    "/var/www/html/bot/range_grid_backtest"
)
BACKTEST_WINDOW_HOURS = float(os.getenv("RANGE_GRID_BACKTEST_WINDOW_HOURS", "24"))
BACKTEST_RECENT_LIMIT = int(os.getenv("RANGE_GRID_BACKTEST_RECENT_LIMIT", "25"))


def now_utc():
    return datetime.now(timezone.utc)


def parse_iso8601(value):
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
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


def snapshot_timestamp(snapshot):
    return parse_iso8601(snapshot.get("captured_at"))


def signal_payload(snapshot):
    payload = (snapshot.get("signal") or {}).get("payload")
    return payload if isinstance(payload, dict) else {}


def state_payload(snapshot):
    payload = snapshot.get("state") or {}
    return payload if isinstance(payload, dict) else {}


def state_summary(snapshot):
    summary = state_payload(snapshot).get("summary")
    return summary if isinstance(summary, dict) else {}


def strategy_payload(snapshot):
    payload = (snapshot.get("strategy_profile") or {}).get("payload")
    return payload if isinstance(payload, dict) else {}


def strategy_context(snapshot):
    context = snapshot.get("strategy_context") or {}
    return context if isinstance(context, dict) else {}


def positive_or_default(value, default=None):
    numeric = safe_float(value)
    if numeric is None:
        return default
    return numeric


def source_status_allows_trading(signal_status, source_status, require_fresh_signal, min_signal_status):
    if not require_fresh_signal:
        return True, None

    if signal_status and signal_status != min_signal_status:
        return False, f"signal_status_{signal_status}"

    if not isinstance(source_status, dict):
        return True, None

    for source_name, status_info in source_status.items():
        if not isinstance(status_info, dict):
            continue
        status_value = status_info.get("status")
        if status_value in ("fresh", "not_configured", None):
            continue
        return False, f"source_status_{source_name}_{status_value}"

    return True, None


def sentiment_buy_permissions(action_recommendation):
    normalized = (action_recommendation or "neutral").strip().lower()
    llm_buys_allowed = normalized == "bullish_allowed"
    range_buys_allowed = normalized in (
        "bullish_allowed",
        "neutral",
        "watch_only",
    )
    return {
        "llm_buys_allowed": llm_buys_allowed,
        "range_buys_allowed": range_buys_allowed,
        "any_buys_allowed": llm_buys_allowed or range_buys_allowed,
    }


def parse_strategy_modes(raw_value):
    if not raw_value:
        return []

    normalized_modes = []
    alias_map = {
        "llm": "llm_target",
        "sentiment": "llm_target",
    }
    valid_modes = {"low", "mean", "median", "high", "llm_target"}
    disabled_values = {"false", "none", "off", "disabled", "no"}

    for mode in str(raw_value).split(","):
        normalized_mode = mode.strip().lower()
        if not normalized_mode:
            continue
        if normalized_mode in disabled_values:
            continue
        normalized_mode = alias_map.get(normalized_mode, normalized_mode)
        if normalized_mode in valid_modes and normalized_mode not in normalized_modes:
            normalized_modes.append(normalized_mode)
    return normalized_modes


def compute_grid(anchor, entry_step_pct, max_grid_size):
    return sorted(
        [
            anchor * (1 - (entry_step_pct * (i + 1)))
            for i in range(max_grid_size)
        ],
        reverse=True
    )


def compute_high_anchor_grid(high, price, entry_step_pct):
    lower_bound = high * (1 - entry_step_pct)
    if lower_bound <= price <= high:
        return [price]
    return []


def flow_adjustment(config, flow_pressure, buy_source):
    if flow_pressure is None:
        return {"size_multiplier": 1.0, "block_buy": False, "reason": None}

    flow_defensive_size_multiplier = safe_float(
        config.get("flow_defensive_size_multiplier")
    ) or 0.75
    flow_block_llm_only_below = safe_float(
        config.get("flow_block_llm_only_below")
    ) or -0.5
    flow_block_threshold = safe_float(config.get("flow_block_threshold")) or -0.4
    flow_defensive_threshold = safe_float(config.get("flow_defensive_threshold")) or -0.2
    flow_block_high_only = bool(config.get("flow_block_high_only", True))

    if flow_pressure <= flow_block_llm_only_below and buy_source == "llm_target":
        return {
            "size_multiplier": flow_defensive_size_multiplier,
            "block_buy": False,
            "reason": "flow_llm_only"
        }

    if flow_pressure <= flow_block_threshold:
        if flow_block_high_only:
            return {
                "size_multiplier": flow_defensive_size_multiplier,
                "block_buy": buy_source == "range_high_band",
                "reason": "flow_block_high"
            }
        return {
            "size_multiplier": flow_defensive_size_multiplier,
            "block_buy": True,
            "reason": "flow_block_all"
        }

    if flow_pressure <= flow_defensive_threshold:
        return {
            "size_multiplier": flow_defensive_size_multiplier,
            "block_buy": buy_source == "range_high_band",
            "reason": "flow_defensive"
        }

    return {"size_multiplier": 1.0, "block_buy": False, "reason": None}


def find_llm_target(signal, price, llm_target_proximity_pct):
    targets = signal.get("target_prices")
    if not isinstance(targets, list):
        return None

    eligible = []
    for target in targets:
        if not isinstance(target, dict):
            continue
        buy_price = safe_float(target.get("buy_price"))
        if buy_price is None or buy_price <= 0:
            continue
        if buy_price > price * (1 + llm_target_proximity_pct):
            continue
        eligible.append({
            "buy_price": buy_price,
            "sell_pct": safe_float(target.get("sell_pct"))
        })

    if not eligible:
        return None

    return max(eligible, key=lambda item: item["buy_price"])


def derive_range_values(snapshot):
    signal = signal_payload(snapshot)
    price_regime = signal.get("price_regime", {})
    if not isinstance(price_regime, dict):
        price_regime = {}
    state_info = state_summary(snapshot)

    low = positive_or_default(price_regime.get("price_low_24h"), positive_or_default(state_info.get("range_low")))
    high = positive_or_default(price_regime.get("price_high_24h"), positive_or_default(state_info.get("range_high")))
    mean = positive_or_default(price_regime.get("price_mean_24h"), positive_or_default(state_info.get("range_mean")))
    median = positive_or_default(price_regime.get("price_median_24h"), positive_or_default(state_info.get("range_median")))
    return low, high, mean, median


def build_candidates(snapshot, price):
    signal = signal_payload(snapshot)
    config = strategy_payload(snapshot)
    context = strategy_context(snapshot)
    state_info = state_summary(snapshot)
    action_recommendation = signal.get("action_recommendation") or "neutral"
    signal_status = signal.get("signal_status")
    source_status = signal.get("source_status", {})
    source_guard_allows_trading = signal.get("bot_action_allowed")
    if source_guard_allows_trading is None:
        source_guard_allows_trading = True
    require_fresh_signal = bool(config.get("require_fresh_signal", True))
    min_signal_status = config.get("min_signal_status", "fresh")
    freshness_allows_trading, freshness_block_reason = source_status_allows_trading(
        signal_status,
        source_status,
        require_fresh_signal,
        min_signal_status
    )
    buy_permissions = sentiment_buy_permissions(action_recommendation)
    llm_buys_allowed = buy_permissions["llm_buys_allowed"]
    range_buys_allowed = buy_permissions["range_buys_allowed"]
    base_any_buys_allowed = buy_permissions["any_buys_allowed"]

    low, high, mean, median = derive_range_values(snapshot)
    strategy_modes = context.get("strategy_modes") or parse_strategy_modes(context.get("grid_anchor"))
    range_modes_enabled = any(mode != "llm_target" for mode in strategy_modes)
    allow_range_fallback_without_sentiment = bool(
        config.get("allow_range_fallback_without_sentiment", True)
    )
    llm_signal_gates_allow = (
        freshness_allows_trading and bool(source_guard_allows_trading)
    )
    range_fallback_active = (
        allow_range_fallback_without_sentiment
        and range_modes_enabled
        and not llm_signal_gates_allow
    )
    range_signal_gates_allow = (
        llm_signal_gates_allow or range_fallback_active
    )
    any_buys_allowed = (
        (llm_buys_allowed and llm_signal_gates_allow)
        or (range_buys_allowed and range_signal_gates_allow)
    )
    llm_target_proximity_pct = safe_float(config.get("llm_target_proximity_pct")) or safe_float(config.get("entry_step_pct")) or 0.0
    llm_target = find_llm_target(signal, price, llm_target_proximity_pct)
    mean_reversion_min_opportunity = safe_float(config.get("mean_reversion_min_opportunity")) or 0.0
    mean_reversion_opportunity = safe_float(signal.get("mean_reversion_opportunity"))
    if mean_reversion_opportunity is None:
        mean_reversion_opportunity = 0.0

    result = {
        "freshness_allows_trading": freshness_allows_trading,
        "freshness_block_reason": freshness_block_reason,
        "source_guard_allows_trading": bool(source_guard_allows_trading),
        "llm_buys_allowed": llm_buys_allowed,
        "range_buys_allowed": range_buys_allowed,
        "any_buys_allowed": any_buys_allowed,
        "range_fallback_active": range_fallback_active,
        "action_recommendation": action_recommendation,
        "low": low,
        "high": high,
        "mean": mean,
        "median": median,
        "strategy_modes": strategy_modes,
        "llm_target": llm_target,
        "llm_buy_allowed": False,
        "raw_candidates": [],
        "hold_reason": None,
        "unknown_balance_required": False,
    }

    llm_buy_allowed = (
        "llm_target" in strategy_modes
        and low and high
        and llm_target is not None
        and llm_signal_gates_allow
        and llm_buys_allowed
        and mean_reversion_opportunity >= mean_reversion_min_opportunity
    )
    result["llm_buy_allowed"] = llm_buy_allowed

    if not (
        range_signal_gates_allow
        and strategy_modes
        and low and high
        and base_any_buys_allowed
    ):
        result["hold_reason"] = (
            "buy_modes_disabled"
            if not strategy_modes
            else (
                None
                if any_buys_allowed
                else f"action_recommendation_{action_recommendation}"
            )
            or (
                None
                if range_fallback_active
                else freshness_block_reason
            )
            or (
                None
                if source_guard_allows_trading
                else "source_guard_blocked"
            )
            or "signal_below_threshold_or_range_unavailable"
        )
        return result

    entry_step_pct = safe_float(config.get("entry_step_pct")) or 0.0
    max_grid_size = int(config.get("max_grid_size", 4))
    sentiment_disable_high_anchor_below = safe_float(
        config.get("sentiment_disable_high_anchor_below")
    )
    btc_sentiment = safe_float(signal.get("btc_sentiment"))
    allow_high_anchor = True
    if (
        sentiment_disable_high_anchor_below is not None
        and btc_sentiment is not None
        and btc_sentiment < sentiment_disable_high_anchor_below
    ):
        allow_high_anchor = False

    candidate_levels = []
    if llm_buy_allowed:
        candidate_levels = [{
            "level": llm_target["buy_price"],
            "sell_pct_override": llm_target["sell_pct"],
            "buy_source": "llm_target"
        }]
    else:
        for strategy_mode in strategy_modes:
            if strategy_mode == "mean" and mean is not None:
                grid = compute_grid(mean, entry_step_pct, max_grid_size)
                sell_pct_override = None
                buy_source = "range_mean"
            elif strategy_mode == "median" and median is not None:
                grid = compute_grid(median, entry_step_pct, max_grid_size)
                sell_pct_override = None
                buy_source = "range_median"
            elif strategy_mode == "high":
                if not allow_high_anchor:
                    continue
                grid = compute_high_anchor_grid(high, price, entry_step_pct)
                sell_pct_override = safe_float(config.get("high_anchor_profit_target_pct"))
                buy_source = "range_high_band"
            else:
                grid = compute_grid(low, entry_step_pct, max_grid_size)
                sell_pct_override = None
                buy_source = "range_low"

            for level in grid:
                candidate_levels.append({
                    "level": level,
                    "sell_pct_override": sell_pct_override,
                    "buy_source": buy_source
                })

    deduped_candidates = []
    seen_levels = set()
    for candidate in candidate_levels:
        rounded_level = round(candidate["level"], 2)
        if rounded_level in seen_levels:
            continue
        seen_levels.add(rounded_level)
        deduped_candidates.append(candidate)

    result["raw_candidates"] = deduped_candidates
    if not deduped_candidates:
        result["hold_reason"] = "no_candidates"
    return result


def evaluate_candidate(snapshot, candidate, price):
    config = strategy_payload(snapshot)
    state_info = state_summary(snapshot)
    state_data = state_payload(snapshot)
    signal = signal_payload(snapshot)
    buy_permissions = sentiment_buy_permissions(
        signal.get("action_recommendation") or "neutral"
    )
    llm_buys_allowed = buy_permissions["llm_buys_allowed"]
    range_buys_allowed = buy_permissions["range_buys_allowed"]
    allow_range_fallback_without_sentiment = bool(
        config.get("allow_range_fallback_without_sentiment", True)
    )
    signal_status = signal.get("signal_status")
    source_status = signal.get("source_status", {})
    source_guard_allows_trading = signal.get("bot_action_allowed")
    if source_guard_allows_trading is None:
        source_guard_allows_trading = True
    require_fresh_signal = bool(config.get("require_fresh_signal", True))
    min_signal_status = config.get("min_signal_status", "fresh")
    freshness_allows_trading, freshness_block_reason = source_status_allows_trading(
        signal_status,
        source_status,
        require_fresh_signal,
        min_signal_status
    )
    llm_signal_gates_allow = (
        freshness_allows_trading and bool(source_guard_allows_trading)
    )
    range_fallback_active = (
        allow_range_fallback_without_sentiment
        and any(mode != "llm_target" for mode in (strategy_context(snapshot).get("strategy_modes") or []))
        and not llm_signal_gates_allow
    )
    range_signal_gates_allow = llm_signal_gates_allow or range_fallback_active

    open_buy_orders = state_data.get("open_buy_orders") or []
    open_sell_orders = state_data.get("open_sell_orders") or []
    open_buy_levels = {str(order.get("level")) for order in open_buy_orders if order.get("level") is not None}
    open_sell_levels = {str(order.get("level")) for order in open_sell_orders if order.get("level") is not None}
    buy_source = candidate["buy_source"]
    level = safe_float(candidate["level"]) or 0.0
    key = str(candidate["level"])

    last_sell_price = positive_or_default(state_info.get("last_sell_price"))
    prevent_buy_above_last_sell = bool(config.get("prevent_buy_above_last_sell", True))
    buy_after_sell_discount_pct = safe_float(config.get("buy_after_sell_discount_pct")) or 0.001
    mean_reversion_opportunity = safe_float(signal.get("mean_reversion_opportunity")) or 0.0
    mean_reversion_min_opportunity = safe_float(config.get("mean_reversion_min_opportunity")) or 0.0
    flow_pressure = safe_float(signal.get("flow_pressure"))
    llm_buy_cooldown_minutes_after_sell = int(config.get("llm_buy_cooldown_minutes_after_sell", 30))
    high_anchor_buy_cooldown_minutes = int(config.get("high_anchor_buy_cooldown_minutes", 15))
    max_open_high_anchor_orders = int(config.get("max_open_high_anchor_orders", 3))
    max_open_sell_orders = int(config.get("max_open_sell_orders", 999999))
    max_inventory_usd = safe_float(config.get("max_inventory_usd")) or float("inf")
    deployed_inventory_usd = safe_float(state_info.get("deployed_inventory_usd")) or 0.0

    if key in open_buy_levels:
        return False, "open_buy_order"
    if key in open_sell_levels:
        return False, "open_sell_order"
    if (
        prevent_buy_above_last_sell
        and last_sell_price is not None
        and level > (last_sell_price * (1 - buy_after_sell_discount_pct))
    ):
        return False, "above_last_sell_discount"
    if mean_reversion_opportunity < mean_reversion_min_opportunity:
        return False, "mean_reversion_opportunity_below_min"
    if buy_source != "llm_target" and price > level:
        return False, "price_above_level"
    if int(state_info.get("open_sell_count") or 0) >= max_open_sell_orders:
        return False, "max_open_sell_orders"
    if deployed_inventory_usd >= max_inventory_usd:
        return False, "max_inventory_usd"
    if buy_source == "llm_target" and not llm_signal_gates_allow:
        return False, freshness_block_reason or "source_guard_blocked"
    if buy_source != "llm_target" and not range_signal_gates_allow:
        return False, freshness_block_reason or "source_guard_blocked"
    if buy_source == "llm_target" and not llm_buys_allowed:
        return False, "sentiment_action_not_bullish_allowed"
    if buy_source != "llm_target" and not range_buys_allowed:
        return False, "sentiment_action_not_range_permitted"

    flow_control = flow_adjustment(config, flow_pressure, buy_source)
    if flow_control["block_buy"]:
        return False, flow_control["reason"]

    now = snapshot_timestamp(snapshot)
    last_llm_sell_at = parse_iso8601(state_info.get("last_llm_sell_at"))
    if (
        buy_source == "llm_target"
        and last_llm_sell_at is not None
        and now is not None
        and (now - last_llm_sell_at).total_seconds() / 60 < llm_buy_cooldown_minutes_after_sell
    ):
        return False, "llm_sell_cooldown"

    last_high_anchor_buy_at = parse_iso8601(state_info.get("last_high_anchor_buy_at"))
    if (
        buy_source == "range_high_band"
        and last_high_anchor_buy_at is not None
        and now is not None
        and (now - last_high_anchor_buy_at).total_seconds() / 60 < high_anchor_buy_cooldown_minutes
    ):
        return False, "high_anchor_cooldown"

    high_anchor_order_count = sum(
        1
        for order in open_buy_orders + open_sell_orders
        if order.get("buy_source") == "range_high_band"
    )
    if (
        buy_source == "range_high_band"
        and high_anchor_order_count >= max_open_high_anchor_orders
    ):
        return False, "max_open_high_anchor_orders"

    return True, None


def empty_replay_summary():
    return {
        "snapshots": 0,
        "valid_price_snapshots": 0,
        "missing_signal": 0,
        "missing_price": 0,
        "hold_snapshots": 0,
        "raw_candidates": 0,
        "approved_candidates": 0,
        "unknown_balance_candidates": 0,
        "hold_reason_counts": {},
        "blocked_reason_counts": {},
        "candidate_counts_by_source": {},
        "approved_counts_by_source": {},
    }


def replay_from_snapshots(snapshots):
    summary = empty_replay_summary()
    recent = []
    hold_reason_counts = Counter()
    blocked_reason_counts = Counter()
    candidate_counts_by_source = Counter()
    approved_counts_by_source = Counter()

    for snapshot in snapshots:
        summary["snapshots"] += 1
        price = safe_float((snapshot.get("ticker") or {}).get("last_price"))
        if price is None:
            signal = signal_payload(snapshot)
            price = safe_float(signal.get("btc_price"))
        if price is None:
            summary["missing_price"] += 1
            continue
        summary["valid_price_snapshots"] += 1

        signal = signal_payload(snapshot)
        if not signal:
            summary["missing_signal"] += 1
            continue

        built = build_candidates(snapshot, price)
        if built["hold_reason"] is not None:
            summary["hold_snapshots"] += 1
            hold_reason_counts[built["hold_reason"]] += 1
            recent.append({
                "captured_at": snapshot.get("captured_at"),
                "price": price,
                "action_recommendation": built["action_recommendation"],
                "hold_reason": built["hold_reason"],
                "raw_candidate_count": len(built["raw_candidates"]),
            })
            continue

        for candidate in built["raw_candidates"]:
            summary["raw_candidates"] += 1
            candidate_counts_by_source[candidate["buy_source"]] += 1
            approved, reason = evaluate_candidate(snapshot, candidate, price)
            if approved:
                summary["approved_candidates"] += 1
                approved_counts_by_source[candidate["buy_source"]] += 1
                recent.append({
                    "captured_at": snapshot.get("captured_at"),
                    "price": price,
                    "buy_source": candidate["buy_source"],
                    "level": round(candidate["level"], 2),
                    "status": "approved_gate_only",
                    "reason": None,
                })
            else:
                blocked_reason_counts[reason] += 1
                recent.append({
                    "captured_at": snapshot.get("captured_at"),
                    "price": price,
                    "buy_source": candidate["buy_source"],
                    "level": round(candidate["level"], 2),
                    "status": "blocked_gate_only",
                    "reason": reason,
                })

    summary["hold_reason_counts"] = dict(hold_reason_counts.most_common())
    summary["blocked_reason_counts"] = dict(blocked_reason_counts.most_common())
    summary["candidate_counts_by_source"] = dict(candidate_counts_by_source.most_common())
    summary["approved_counts_by_source"] = dict(approved_counts_by_source.most_common())

    return {
        "summary": summary,
        "recent_replay_events": recent[-BACKTEST_RECENT_LIMIT:],
        "replay_scope": "gate_only_no_private_balance_or_fill_simulation",
    }


def summarize_actual_trades(events):
    summary = {
        "events": len(events),
        "buy_orders_placed": 0,
        "buy_orders_filled": 0,
        "sell_orders_placed": 0,
        "sell_orders_filled": 0,
        "order_rejected": 0,
        "sell_order_repriced": 0,
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0,
        "buy_orders_placed_by_source": {},
        "buy_filled_by_source": {},
        "sell_filled_by_source": {},
        "rejected_by_side": {},
        "recent_fills": [],
        "recent_buy_orders": [],
    }
    buy_orders_placed_by_source = Counter()
    buy_filled_by_source = Counter()
    sell_filled_by_source = Counter()
    rejected_by_side = Counter()
    hold_minutes = []

    for event in events:
        name = event.get("event")
        if name == "BUY_ORDER_PLACED":
            summary["buy_orders_placed"] += 1
            buy_orders_placed_by_source[event.get("buy_source") or "unknown"] += 1
            summary["recent_buy_orders"].append({
                "ts": event.get("ts"),
                "buy_source": event.get("buy_source"),
                "price": event.get("price"),
                "volume": event.get("volume"),
            })
        elif name == "BUY_ORDER_FILLED":
            summary["buy_orders_filled"] += 1
            buy_filled_by_source[event.get("buy_source") or "unknown"] += 1
        elif name == "SELL_ORDER_PLACED":
            summary["sell_orders_placed"] += 1
        elif name == "SELL_ORDER_FILLED":
            summary["sell_orders_filled"] += 1
            sell_filled_by_source[event.get("buy_source") or "unknown"] += 1
            summary["realized_gross_pnl"] += safe_float(event.get("gross_pnl")) or 0.0
            summary["realized_estimated_net_pnl"] += safe_float(event.get("estimated_net_pnl")) or 0.0
            if safe_float(event.get("hold_minutes")) is not None:
                hold_minutes.append(float(event["hold_minutes"]))
            summary["recent_fills"].append({
                "ts": event.get("ts"),
                "side": "sell",
                "buy_source": event.get("buy_source"),
                "level": event.get("level"),
                "gross_pnl": event.get("gross_pnl"),
                "estimated_net_pnl": event.get("estimated_net_pnl"),
                "hold_minutes": event.get("hold_minutes"),
            })
        elif name == "ORDER_REJECTED":
            summary["order_rejected"] += 1
            rejected_by_side[event.get("side") or "unknown"] += 1
        elif name == "SELL_ORDER_REPRICED":
            summary["sell_order_repriced"] += 1

    summary["buy_orders_placed_by_source"] = dict(
        buy_orders_placed_by_source.most_common()
    )
    summary["buy_filled_by_source"] = dict(buy_filled_by_source.most_common())
    summary["sell_filled_by_source"] = dict(sell_filled_by_source.most_common())
    summary["rejected_by_side"] = dict(rejected_by_side.most_common())
    summary["realized_gross_pnl"] = round(summary["realized_gross_pnl"], 8)
    summary["realized_estimated_net_pnl"] = round(summary["realized_estimated_net_pnl"], 8)
    summary["average_hold_minutes"] = round(statistics.mean(hold_minutes), 2) if hold_minutes else None
    summary["recent_fills"] = summary["recent_fills"][-BACKTEST_RECENT_LIMIT:]
    summary["recent_buy_orders"] = summary["recent_buy_orders"][-BACKTEST_RECENT_LIMIT:]
    return summary


def summarize_missed_approved_opportunities(replay, actual):
    approved_by_source = replay["summary"].get("approved_counts_by_source", {})
    live_buys_by_source = actual.get("buy_orders_placed_by_source", {})

    missing_by_source = {}
    for source, approved_count in approved_by_source.items():
        live_count = int(live_buys_by_source.get(source, 0) or 0)
        missing_count = max(0, int(approved_count or 0) - live_count)
        if missing_count:
            missing_by_source[source] = missing_count

    total_missing = sum(missing_by_source.values())
    total_approved = int(replay["summary"].get("approved_candidates", 0) or 0)
    placement_rate = (
        round(actual.get("buy_orders_placed", 0) / total_approved, 4)
        if total_approved > 0
        else None
    )

    return {
        "approved_candidates": total_approved,
        "actual_buy_orders_placed": actual.get("buy_orders_placed", 0),
        "approved_but_not_placed": total_missing,
        "approved_but_not_placed_by_source": missing_by_source,
        "placement_rate_vs_approved": placement_rate,
        "notes": [
            "This is a gate-level comparison only.",
            "A missed approved opportunity means replay approved a buy candidate but no corresponding live BUY_ORDER_PLACED was seen in the reporting window.",
            "This does not yet model exchange fills, private-balance constraints, or one-to-one candidate-to-order matching."
        ],
    }


def build_report():
    now = now_utc()
    since_dt = now - timedelta(hours=BACKTEST_WINDOW_HOURS)
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

    all_events = load_jsonl(TRADE_LOG_FILE)
    events = [
        event
        for event in all_events
        if (parse_iso8601(event.get("ts")) or datetime.min.replace(tzinfo=timezone.utc)) >= since_dt
    ]

    replay = replay_from_snapshots(snapshots)
    actual = summarize_actual_trades(events)
    missed = summarize_missed_approved_opportunities(replay, actual)

    return {
        "timestamp": now.isoformat(),
        "snapshot_file_base": os.path.abspath(SNAPSHOT_LOG_FILE),
        "snapshot_files": snapshot_files,
        "trade_log_file": os.path.abspath(TRADE_LOG_FILE),
        "since": since_dt.isoformat(),
        "snapshot_count": len(snapshots),
        "trade_event_count": len(events),
        "replay": replay,
        "actual_live": actual,
        "missed_opportunities": missed,
        "top_summary": {
            "replay_scope": replay["replay_scope"],
            "approved_candidates": replay["summary"]["approved_candidates"],
            "raw_candidates": replay["summary"]["raw_candidates"],
            "actual_buy_orders_placed": actual["buy_orders_placed"],
            "approved_but_not_placed": missed["approved_but_not_placed"],
            "placement_rate_vs_approved": missed["placement_rate_vs_approved"],
            "actual_buy_orders_filled": actual["buy_orders_filled"],
            "actual_sell_orders_filled": actual["sell_orders_filled"],
            "actual_realized_estimated_net_pnl": actual["realized_estimated_net_pnl"],
            "notes": [
                "Replay is gate-level only and does not model private-balance checks or exchange fill mechanics.",
                "Use this to understand opportunity flow, blocking reasons, and live-vs-opportunity comparisons."
            ],
        },
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
        f"range_grid_backtest_{now_utc().strftime('%Y%m%d')}.json"
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
        "snapshot_count": report["snapshot_count"],
        "trade_event_count": report["trade_event_count"],
    }))


if __name__ == "__main__":
    main()
