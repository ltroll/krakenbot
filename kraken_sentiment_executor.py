#!/usr/bin/env python3

# =====================================================
# KRAKEN SENTIMENT EXECUTOR
# =====================================================

import base64
import csv
import hashlib
import hmac
import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

from fee_config import effective_round_trip_fee_pct
from risk_context import derive_risk_context
ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

# ----------------------
# CONFIG
# ----------------------


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="Run the Kraken sentiment executor service."
    )
    parser.add_argument(
        "--strategy-profile",
        help=(
            "Strategy profile name or JSON file path. Overrides "
            "SENTIMENT_STRATEGY_PROFILE/STRATEGY_PROFILE."
        )
    )
    parser.add_argument(
        "--bot-policy-backtest-url",
        help="Backtest policy JSON URL. Overrides BOT_POLICY_BACKTEST_URL."
    )
    parser.add_argument(
        "--bot-replay-backtest-url",
        help=(
            "Full replay backtest JSON URL. Overrides BOT_REPLAY_BACKTEST_URL "
            "and is preferred over BOT_POLICY_BACKTEST_URL when set."
        )
    )
    parser.add_argument(
        "--run-backtest",
        action="store_true",
        help=(
            "Fetch the configured backtest JSON, print a summary, and exit "
            "without starting the live trading loop."
        )
    )
    parser.add_argument(
        "--buynow",
        action="store_true",
        help=(
            "Place one post-only test buy inside the current order book, "
            "record it as an open buy, and exit."
        )
    )
    parser.add_argument(
        "--usd",
        "--USD",
        "--backtest-usd",
        dest="backtest_usd",
        type=float,
        help=(
            "USD amount for --run-backtest emulation or --buynow sizing."
        )
    )
    parser.add_argument(
        "--buynow-usd",
        type=float,
        help="USD amount for --buynow. Overrides --usd."
    )
    parser.add_argument(
        "--backtest-min-trades",
        type=int,
        help="Minimum policy trades required by the backtest health gate."
    )

    backtest_gate = parser.add_mutually_exclusive_group()
    backtest_gate.add_argument(
        "--backtest-health-gate",
        dest="backtest_health_gate",
        action="store_true",
        help="Enable the backtest health gate for this run."
    )
    backtest_gate.add_argument(
        "--no-backtest-health-gate",
        dest="backtest_health_gate",
        action="store_false",
        help="Disable the backtest health gate for this run."
    )
    parser.set_defaults(backtest_health_gate=None)

    fail_mode = parser.add_mutually_exclusive_group()
    fail_mode.add_argument(
        "--backtest-fail-closed",
        dest="backtest_fail_closed",
        action="store_true",
        help="Block buys if backtest data is missing or invalid."
    )
    fail_mode.add_argument(
        "--backtest-fail-open",
        dest="backtest_fail_closed",
        action="store_false",
        help="Allow buys if backtest data is missing or invalid."
    )
    parser.set_defaults(backtest_fail_closed=None)

    baseline = parser.add_mutually_exclusive_group()
    baseline.add_argument(
        "--backtest-require-policy-beats-baseline",
        dest="backtest_require_policy_beats_baseline",
        action="store_true",
        help="Require policy metrics to be at least as good as baseline."
    )
    baseline.add_argument(
        "--no-backtest-require-policy-beats-baseline",
        dest="backtest_require_policy_beats_baseline",
        action="store_false",
        help="Do not compare policy metrics to baseline for this run."
    )
    parser.set_defaults(backtest_require_policy_beats_baseline=None)

    args = parser.parse_args()
    if args.run_backtest and args.buynow:
        parser.error("--run-backtest and --buynow cannot be used together")

    return args


CLI_ARGS = parse_cli_args()

CONFIG_FILE = (
    os.getenv("SENTIMENT_CONFIG_FILE")
    or os.getenv("BOT_CONFIG_FILE")
    or "sentiment_bot_config.json"
)
STRATEGY_PROFILE = (
    CLI_ARGS.strategy_profile
    or os.getenv("SENTIMENT_STRATEGY_PROFILE")
    or os.getenv("STRATEGY_PROFILE")
    or "sentiment_strategy_default.json"
)
STATE_FILE = (
    os.getenv("SENTIMENT_STATE_FILE")
    or os.getenv("BOT_STATE_FILE")
    or "sentiment_state.json"
)
LOG_FILE = (
    os.getenv("SENTIMENT_TRADE_LOG_FILE")
    or os.getenv("TRADE_LOG_FILE")
    or "sentiment_trade_log.jsonl"
)
TRADE_ACTIVITY_FILE = (
    os.getenv("SENTIMENT_TRADE_ACTIVITY_FILE")
    or os.getenv("TRADE_ACTIVITY_FILE")
    or "sentiment_trade_activity.jsonl"
)
DECISION_CSV_FILE = os.getenv(
    "SENTIMENT_DECISION_CSV_FILE",
    "sentiment_decisions.csv"
)

KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
KRAKEN_ORDERBOOK_URL = os.getenv("KRAKEN_ORDERBOOK_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
SIGNAL_ASSET_ID = (
    os.getenv("SENTIMENT_ASSET_ID")
    or os.getenv("SIGNAL_ASSET_ID")
    or os.getenv("ASSET_ID")
)
SIGNAL_FILE = os.getenv("SIGNAL_FILE")
BOT_POLICY_BACKTEST_URL = (
    CLI_ARGS.bot_policy_backtest_url
    or os.getenv("BOT_POLICY_BACKTEST_URL")
)
BOT_REPLAY_BACKTEST_URL = (
    CLI_ARGS.bot_replay_backtest_url
    or os.getenv("BOT_REPLAY_BACKTEST_URL")
)
RUN_BACKTEST = (
    CLI_ARGS.run_backtest
    or os.getenv("SENTIMENT_RUN_BACKTEST", "").strip().lower()
    in ("1", "true", "yes", "on")
    or os.getenv("RUN_BACKTEST", "").strip().lower()
    in ("1", "true", "yes", "on")
)
BACKTEST_USD = CLI_ARGS.backtest_usd
if BACKTEST_USD is None and os.getenv("BACKTEST_USD"):
    BACKTEST_USD = float(os.getenv("BACKTEST_USD"))
BUYNOW_USD = CLI_ARGS.buynow_usd or CLI_ARGS.backtest_usd
if BUYNOW_USD is None and os.getenv("BUYNOW_USD"):
    BUYNOW_USD = float(os.getenv("BUYNOW_USD"))
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")


def infer_asset_id_from_pair(pair):
    pair = (pair or "").upper()
    if "ETH" in pair or "XETH" in pair:
        return "ETH"
    if "SOL" in pair:
        return "SOL"
    if "XBT" in pair or "BTC" in pair:
        return "BTC"
    return "BTC"


SELECTED_SIGNAL_ASSET_ID = (
    SIGNAL_ASSET_ID or infer_asset_id_from_pair(KRAKEN_PAIR)
).upper()


def key_fingerprint(value):
    if not value:
        return "missing"

    digest = hashlib.sha256(value.encode()).hexdigest()[:12]
    return f"sha256:{digest}"


def load_json_file(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def strategy_profile_is_file():
    if not STRATEGY_PROFILE:
        return False

    expanded_path = os.path.expanduser(STRATEGY_PROFILE)
    return (
        os.path.exists(expanded_path)
        or STRATEGY_PROFILE.endswith(".json")
        or os.sep in STRATEGY_PROFILE
    )


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}

    return load_json_file(CONFIG_FILE)


def select_strategy_profile(config_data):
    profiles = config_data.get("strategy_profiles")
    if profiles is None:
        return config_data

    if not isinstance(profiles, dict):
        raise RuntimeError(f"{CONFIG_FILE} strategy_profiles must be an object")

    profile = profiles.get(STRATEGY_PROFILE)
    if profile is None:
        available = ", ".join(sorted(profiles)) or "<none>"
        raise RuntimeError(
            f"Strategy profile '{STRATEGY_PROFILE}' not found in "
            f"{CONFIG_FILE}. Available profiles: {available}"
        )

    return profile


def load_strategy_config():
    if strategy_profile_is_file():
        path = os.path.expanduser(STRATEGY_PROFILE)
        if not os.path.exists(path):
            raise RuntimeError(f"Strategy profile file not found: {path}")

        return load_json_file(path)

    return select_strategy_profile(load_config())


strategy_config = load_strategy_config()


def profile_bool(name, default):
    value = strategy_config.get(name, default)
    if value is None:
        return default

    return parse_bool(value)


def parse_bool(value):
    if isinstance(value, bool):
        return value

    return str(value).strip().lower() in ("1", "true", "yes", "on")


def env_profile_bool(env_name, profile_name, default, cli_value=None):
    if cli_value is not None:
        return cli_value

    value = os.getenv(env_name)
    if value is not None:
        return parse_bool(value)

    return profile_bool(profile_name, default)


def env_profile_int(env_name, profile_name, default, cli_value=None):
    if cli_value is not None:
        return cli_value

    value = os.getenv(env_name)
    if value is not None:
        return int(value)

    return profile_int(profile_name, default)


def env_bool(env_name, default, cli_value=None):
    if cli_value is not None:
        return cli_value

    value = os.getenv(env_name)
    if value is not None:
        return parse_bool(value)

    return default


def env_int(env_name, default, cli_value=None):
    if cli_value is not None:
        return cli_value

    value = os.getenv(env_name)
    if value is not None:
        return int(value)

    return default


def env_profile_float(env_name, profile_name, default):
    value = os.getenv(env_name)
    if value is not None:
        return float(value)

    return profile_float(profile_name, default)


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


def profile_str(name, default=""):
    value = strategy_config.get(name, default)
    return default if value is None else str(value)


REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))
REQUEST_RETRY_ATTEMPTS = int(os.getenv("REQUEST_RETRY_ATTEMPTS", "2"))
REQUEST_RETRY_BACKOFF_SECONDS = float(
    os.getenv("REQUEST_RETRY_BACKOFF_SECONDS", "1.5")
)
KRAKEN_NONCE_RETRIES = int(os.getenv("KRAKEN_NONCE_RETRIES", "2"))
KRAKEN_LOCKOUT_COOLDOWN_SECONDS = int(
    os.getenv("KRAKEN_LOCKOUT_COOLDOWN_SECONDS", "300")
)
ORDER_TRACKER_URL = (
    os.getenv("ORDER_TRACKER_URL")
    or os.getenv("EXTERNAL_ORDER_TRACKER_URL")
)
ORDER_TRACKER_USER_AGENT = os.getenv("ORDER_TRACKER_USER_AGENT")
ORDER_TRACKER_SYMBOL = (
    os.getenv("ORDER_TRACKER_SYMBOL")
    or profile_str("order_tracker_symbol", KRAKEN_PAIR)
)
ORDER_TRACKER_TIMEOUT = env_profile_float(
    "ORDER_TRACKER_TIMEOUT_SECONDS",
    "order_tracker_timeout_seconds",
    5
)
ORDER_TRACKER_CHECKIN_TIMEOUT = env_profile_float(
    "ORDER_TRACKER_CHECKIN_TIMEOUT_SECONDS",
    "order_tracker_checkin_timeout_seconds",
    min(ORDER_TRACKER_TIMEOUT, 5)
)
PRICE_CHECK_INTERVAL_SECONDS = profile_int("price_check_interval_seconds", 60)
MIN_TRADE_USD = profile_float("min_trade_usd", 30)
CONF_THRESHOLD = profile_float("confidence_threshold", 0.45)
CONFIDENCE_WEIGHTING = profile_bool("confidence_weighting", True)
DRY_RUN = profile_bool("dry_run", False)
SHADOW_TRADES_ENABLED = env_profile_bool(
    "SENTIMENT_SHADOW_TRADES_ENABLED",
    "shadow_trades_enabled",
    False
)
EXECUTION_BUFFER_PCT = profile_float("execution_buffer_pct", 0.0025)
REBALANCE_COOLDOWN_MINUTES = profile_float("rebalance_cooldown_minutes", 15)
COOLDOWN_OVERRIDE_SIGNAL_ABS = profile_float("cooldown_override_signal_abs", 0.20)
ENTRY_PACING_ENABLED = env_profile_bool(
    "ENTRY_PACING_ENABLED",
    "entry_pacing_enabled",
    True
)
ENTRY_PACING_WINDOW_MINUTES = env_profile_float(
    "ENTRY_PACING_WINDOW_MINUTES",
    "entry_pacing_window_minutes",
    120
)
ENTRY_PACING_MAX_BUYS = env_profile_int(
    "ENTRY_PACING_MAX_BUYS",
    "entry_pacing_max_buys",
    2
)
ENTRY_PACING_MIN_PRICE_MOVE_PCT = env_profile_float(
    "ENTRY_PACING_MIN_PRICE_MOVE_PCT",
    "entry_pacing_min_price_move_pct",
    0.005
)
SENTIMENT_BUY_THRESHOLD = profile_float("sentiment_buy_threshold", 0.03)
POSITION_SIZE_PCT = profile_float("position_size_pct", 0.10)
MAX_TRADE_USD = profile_float("max_trade_usd", 0)
TARGET_PROFIT_PCT = profile_float("target_profit_pct", 0.006)
ROUND_TRIP_FEE_PCT = effective_round_trip_fee_pct(strategy_config, 0.0032)
DYNAMIC_PROFIT_TARGETS = profile_bool("dynamic_profit_targets", False)
MIN_TARGET_PROFIT_PCT = profile_float("min_target_profit_pct", TARGET_PROFIT_PCT)
BASE_TARGET_PROFIT_PCT = profile_float("base_target_profit_pct", TARGET_PROFIT_PCT)
MAX_TARGET_PROFIT_PCT = profile_float("max_target_profit_pct", TARGET_PROFIT_PCT)
DYNAMIC_PROFIT_LOW_VOLATILITY_PCT = profile_float(
    "dynamic_profit_low_volatility_pct",
    0.025
)
DYNAMIC_PROFIT_HIGH_VOLATILITY_PCT = profile_float(
    "dynamic_profit_high_volatility_pct",
    0.06
)
MAX_OPEN_SELL_ORDERS = profile_int("max_open_sell_orders", 1)
MAX_INVENTORY_USD = profile_float("max_inventory_usd", 250)
PREVENT_BUY_ABOVE_LAST_SELL = profile_bool("prevent_buy_above_last_sell", True)
BUY_AFTER_SELL_DISCOUNT_PCT = profile_float("buy_after_sell_discount_pct", 0.0)
HIGH_PRICE_BUY_BLOCK_PCT = profile_float("high_price_buy_block_pct", 0.0005)
USE_SIGNAL_STATUS_GATES = profile_bool("use_signal_status_gates", True)
REQUIRE_BOT_ACTION_ALLOWED = profile_bool("require_bot_action_allowed", True)
MAX_SIGNAL_AGE_MINUTES = profile_float("max_signal_age_minutes", 30)
CRITICAL_SOURCE_STATUSES = [
    source.strip()
    for source in profile_str(
        "critical_source_statuses",
        "market_data,price_regime,kraken_flow"
    ).split(",")
    if source.strip()
]
USE_RISK_MULTIPLIER = profile_bool("use_risk_multiplier", True)
MIN_RISK_MULTIPLIER = profile_float("min_risk_multiplier", 0.25)
MAX_RISK_MULTIPLIER = profile_float("max_risk_multiplier", 1.25)
ENABLE_TARGET_LIMIT_BUYS = profile_bool("enable_target_limit_buys", True)
MAX_OPEN_BUY_ORDERS = profile_int("max_open_buy_orders", 2)
MAX_TARGET_LIMIT_ORDERS_PER_CYCLE = profile_int(
    "max_target_limit_orders_per_cycle",
    2
)
TARGET_LIMIT_MAX_PREMIUM_PCT = profile_float(
    "target_limit_max_premium_pct",
    0.0005
)
MEAN_REVERSION_BUY_THRESHOLD = profile_float(
    "mean_reversion_buy_threshold",
    0.35
)
MEAN_REVERSION_RANGE_POSITION_MAX = profile_float(
    "mean_reversion_range_position_max",
    0.15
)
MEAN_REVERSION_FLOW_PRESSURE_MIN = profile_float(
    "mean_reversion_flow_pressure_min",
    0.0
)
USE_RISK_CONTEXT_POLICY = env_profile_bool(
    "USE_RISK_CONTEXT_POLICY",
    "use_risk_context_policy",
    True
)
RISK_CONTEXT_HARD_SAFETY_BLOCK = env_profile_bool(
    "RISK_CONTEXT_HARD_SAFETY_BLOCK",
    "risk_context_hard_safety_block",
    False
)
RISK_CONTEXT_MIN_BUY_SCORE = env_profile_float(
    "RISK_CONTEXT_MIN_BUY_SCORE",
    "risk_context_min_buy_score",
    0.50
)
RISK_CONTEXT_POSITION_SIZE_ENABLED = env_profile_bool(
    "RISK_CONTEXT_POSITION_SIZE_ENABLED",
    "risk_context_position_size_enabled",
    True
)
RISK_CONTEXT_TARGET_PROFIT_ENABLED = env_profile_bool(
    "RISK_CONTEXT_TARGET_PROFIT_ENABLED",
    "risk_context_target_profit_enabled",
    True
)
RISK_CONTEXT_MAX_POSITION_SIZE_MULTIPLIER = env_profile_float(
    "RISK_CONTEXT_MAX_POSITION_SIZE_MULTIPLIER",
    "risk_context_max_position_size_multiplier",
    1.25
)
HIGH_ENTRY_QUALITY_ENABLED = env_profile_bool(
    "HIGH_ENTRY_QUALITY_ENABLED",
    "high_entry_quality_enabled",
    True
)
HIGH_ENTRY_RANGE_POSITION_MIN = env_profile_float(
    "HIGH_ENTRY_RANGE_POSITION_MIN",
    "high_entry_range_position_min",
    0.90
)
HIGH_ENTRY_DISTANCE_TO_HIGH_PCT = env_profile_float(
    "HIGH_ENTRY_DISTANCE_TO_HIGH_PCT",
    "high_entry_distance_to_high_pct",
    0.75
)
HIGH_ENTRY_MAX_MARKET_RISK_SCORE = env_profile_float(
    "HIGH_ENTRY_MAX_MARKET_RISK_SCORE",
    "high_entry_max_market_risk_score",
    0.35
)
HIGH_ENTRY_MIN_CONFIRMATION_SCORE = env_profile_float(
    "HIGH_ENTRY_MIN_CONFIRMATION_SCORE",
    "high_entry_min_confirmation_score",
    0.65
)
HIGH_ENTRY_REQUIRE_POSITIVE_4H_RETURN = env_profile_bool(
    "HIGH_ENTRY_REQUIRE_POSITIVE_4H_RETURN",
    "high_entry_require_positive_4h_return",
    True
)
HIGH_ENTRY_SIZE_MULTIPLIER = env_profile_float(
    "HIGH_ENTRY_SIZE_MULTIPLIER",
    "high_entry_size_multiplier",
    0.50
)
USE_BACKTEST_HEALTH_GATE = env_bool(
    "USE_BACKTEST_HEALTH_GATE",
    False,
    CLI_ARGS.backtest_health_gate
)
BACKTEST_FAIL_CLOSED = env_bool(
    "BACKTEST_FAIL_CLOSED",
    True,
    CLI_ARGS.backtest_fail_closed
)
BACKTEST_MIN_TRADES = env_int(
    "BACKTEST_MIN_TRADES",
    5,
    CLI_ARGS.backtest_min_trades
)
BACKTEST_REQUIRE_POLICY_BEATS_BASELINE = env_bool(
    "BACKTEST_REQUIRE_POLICY_BEATS_BASELINE",
    True,
    CLI_ARGS.backtest_require_policy_beats_baseline
)

PAIR_INFO_CACHE = None


def request_with_retries(
    method,
    url,
    *,
    attempts=None,
    backoff_seconds=None,
    retry_statuses=None,
    **kwargs
):
    attempts = max(1, int(attempts or REQUEST_RETRY_ATTEMPTS))
    backoff_seconds = (
        REQUEST_RETRY_BACKOFF_SECONDS
        if backoff_seconds is None
        else float(backoff_seconds)
    )
    retry_statuses = retry_statuses or {429, 500, 502, 503, 504}
    last_error = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.request(method, url, **kwargs)
            if response.status_code in retry_statuses and attempt < attempts:
                last_error = requests.HTTPError(
                    f"{response.status_code} Server Error: {response.text[:200]}"
                )
            else:
                return response
        except requests.RequestException as e:
            last_error = e
            if attempt >= attempts:
                raise

        time.sleep(backoff_seconds * attempt)

    if last_error:
        raise last_error

    raise RuntimeError(f"{method} {url} failed without a response")

DECISION_CSV_FIELDS = [
    "ts",
    "cycle_id",
    "event",
    "side",
    "reason",
    "price",
    "execution_signal",
    "smoothed_signal",
    "weighted_signal",
    "confidence",
    "volume",
    "trade_value",
    "base_balance",
    "quote_balance",
    "portfolio_value",
    "dry_run",
    "order_txid",
    "order_status",
    "fill_volume",
    "fill_cost",
    "fill_fee",
    "fill_price",
    "result",
    "elapsed_minutes",
    "cooldown_minutes",
    "cooldown_override_signal_abs",
    "sentiment_buy_threshold",
    "target_profit_pct",
    "round_trip_fee_pct",
    "gross_target_pct",
    "last_sell_price",
    "max_rebuy_price",
    "open_sell_count",
    "deployed_inventory_usd",
    "max_inventory_usd",
    "risk_multiplier",
    "effective_risk_multiplier",
    "signal_status",
    "bot_action_allowed",
    "action_recommendation",
    "action_policy_reason",
    "contributor_count",
    "signal_age_minutes",
    "source_status_result",
    "risk_context_available",
    "risk_context_stale",
    "risk_context_recommended_posture",
    "risk_context_market_risk_score",
    "risk_context_buy_aggression_score",
    "risk_context_downside_risk_score",
    "risk_context_bottoming_score",
    "risk_context_rebound_score",
    "risk_context_breakout_score",
    "risk_context_hard_safety_flags",
    "weather_report_available",
    "weather_trade_permission",
    "weather_bot_decision_authority",
    "weather_condition",
    "weather_alert_level",
    "weather_emergency_bell",
    "weather_opportunity_tags",
    "weather_risk_warnings",
    "weather_market_current_price",
    "weather_market_range_high",
    "weather_market_range_low",
    "weather_market_range_position",
    "weather_market_range_zone",
    "weather_market_distance_to_recent_high_pct",
    "weather_market_distance_from_recent_low_pct",
    "weather_market_price_return_24h_pct",
    "weather_market_price_return_4h_pct",
    "risk_adjusted_buy_score",
    "risk_adjusted_market_score",
    "risk_adjusted_posture",
    "risk_adjusted_reason",
    "suggested_position_size_multiplier",
    "suggested_grid_aggression_multiplier",
    "suggested_entry_discount_multiplier",
    "suggested_take_profit_multiplier",
    "risk_context_source_processed_at",
    "risk_context_age_minutes",
    "mean_reversion_opportunity",
    "range_position_24h",
    "flow_pressure",
    "target_buy_price",
    "target_allocation_pct",
    "open_buy_count",
    "max_open_buy_orders",
    "price_high",
    "high_price_buy_block_pct",
    "max_high_buy_price",
    "high_entry_near_high",
    "high_entry_quality_reason",
    "high_entry_size_multiplier",
    "backtest_kind",
    "backtest_policy_trades",
    "backtest_policy_win_rate",
    "backtest_policy_avg_net_return_pct",
    "backtest_policy_max_drawdown_pct",
    "backtest_baseline_win_rate",
    "backtest_baseline_avg_net_return_pct",
    "backtest_baseline_max_drawdown_pct"
]

# ----------------------
# LOGGING
# ----------------------


def console(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def log_event(event, message="", **kwargs):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "message": message
    }
    record.update(kwargs)

    try:
        log_dir = os.path.dirname(LOG_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
    except Exception as e:
        console(f"LOG_WRITE_ERROR: {e}")


def log_trade_activity(event, mode="real", message="", **kwargs):
    if not TRADE_ACTIVITY_FILE:
        return

    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "mode": mode,
        "message": message,
        "bot": "sentiment_executor",
        "pair": KRAKEN_PAIR,
        "signal_asset_id": SELECTED_SIGNAL_ASSET_ID,
        "dry_run": DRY_RUN
    }
    record.update(kwargs)

    try:
        log_dir = os.path.dirname(TRADE_ACTIVITY_FILE)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        with open(TRADE_ACTIVITY_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
            f.flush()
    except Exception as e:
        console(f"TRADE_ACTIVITY_WRITE_ERROR: {e}")


def log_and_console(event, message="", **kwargs):
    log_event(event, message=message, **kwargs)

    if message:
        console(f"{event}: {message}")
    else:
        console(event)


def positive_float(value):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None

    return parsed if parsed > 0 else None


def tracker_value(value, fallback=None):
    return positive_float(value) or positive_float(fallback)


def notify_order_tracker(
    trade_id,
    side,
    price,
    quantity,
    order_id=None,
    fee=None,
    timestamp=None,
    notes=None,
    status=None
):
    if not ORDER_TRACKER_URL or not ORDER_TRACKER_USER_AGENT:
        return

    price = positive_float(price)
    quantity = positive_float(quantity)

    if not trade_id or side not in ("buy", "sell") or price is None or quantity is None:
        log_event(
            "ORDER_TRACKER_SKIPPED",
            reason="missing_required_fields",
            trade_id=trade_id,
            side=side,
            price=price,
            quantity=quantity,
            order_id=order_id
        )
        return

    payload = {
        "trade_id": str(trade_id),
        "side": side,
        "price": str(price),
        "quantity": str(quantity)
    }
    optional_fields = {
        "symbol": ORDER_TRACKER_SYMBOL,
        "order_id": order_id,
        "fee": positive_float(fee),
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        "notes": notes,
        "status": status
    }
    payload.update(
        {
            key: str(value)
            for key, value in optional_fields.items()
            if value is not None and value != ""
        }
    )

    try:
        response = requests.post(
            ORDER_TRACKER_URL,
            data=payload,
            headers={"User-Agent": ORDER_TRACKER_USER_AGENT},
            timeout=ORDER_TRACKER_TIMEOUT
        )
        response.raise_for_status()
        log_event(
            "ORDER_TRACKER_UPDATED",
            trade_id=trade_id,
            side=side,
            order_id=order_id,
            status_code=response.status_code
        )
    except Exception as e:
        log_event(
            "ORDER_TRACKER_ERROR",
            message=str(e),
            trade_id=trade_id,
            side=side,
            order_id=order_id
        )


def short_error_summary(error):
    return str(error).replace("\n", " ")[:200]


def send_checkin(status="ok", loop_count=None, message="loop_complete"):
    if not ORDER_TRACKER_URL or not ORDER_TRACKER_USER_AGENT:
        return

    payload = {
        "action": "checkin",
        "status": status,
        "message": message
    }
    if loop_count is not None:
        payload["loop_count"] = str(loop_count)

    try:
        response = request_with_retries(
            "POST",
            ORDER_TRACKER_URL,
            data=payload,
            headers={"User-Agent": ORDER_TRACKER_USER_AGENT},
            timeout=ORDER_TRACKER_CHECKIN_TIMEOUT
        )
        response.raise_for_status()
    except Exception as e:
        log_event("ORDER_TRACKER_CHECKIN_ERROR", message=str(e), status=status)


# ----------------------
# STATE
# ----------------------


def load_state():
    default = {
        "signal_memory": [0, 0],
        "last_cycle": None,
        "stats": {
            "cycles": 0,
            "trades_executed": 0,
            "dry_run_trades": 0,
            "skips": 0,
            "errors": 0,
            "buy_orders_placed": 0,
            "buy_orders_filled": 0,
            "sell_orders_placed": 0,
            "sell_orders_filled": 0
        },
        "last_nonce": 0,
        "last_trade_at": None,
        "trade_history": [],
        "open_buy_orders": {},
        "open_sell_orders": {},
        "last_sell_price": None,
        "last_sell_at": None,
        "kraken_private_paused_until": None
    }

    if not os.path.exists(STATE_FILE):
        return default

    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)

    for key, default_value in default.items():
        state.setdefault(key, default_value)

    state.setdefault("stats", {})
    for key, default_value in default["stats"].items():
        state["stats"].setdefault(key, default_value)

    if not isinstance(state.get("signal_memory"), list):
        state["signal_memory"] = [0, 0]
    state["signal_memory"] = (state["signal_memory"] + [0, 0])[:2]
    if not isinstance(state.get("trade_history"), list):
        state["trade_history"] = []
    if not isinstance(state.get("open_buy_orders"), dict):
        state["open_buy_orders"] = {}
    if not isinstance(state.get("open_sell_orders"), dict):
        state["open_sell_orders"] = {}

    return state


def save_state(state):
    state_dir = os.path.dirname(STATE_FILE)
    if state_dir:
        os.makedirs(state_dir, exist_ok=True)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


state = load_state()


def parse_iso8601(value):
    if not value:
        return None

    try:
        if isinstance(value, str) and value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def append_decision_csv(event, **kwargs):
    row = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event
    }
    row.update(kwargs)

    csv_dir = os.path.dirname(DECISION_CSV_FILE)
    if csv_dir:
        os.makedirs(csv_dir, exist_ok=True)

    write_header = (
        not os.path.exists(DECISION_CSV_FILE)
        or os.path.getsize(DECISION_CSV_FILE) == 0
    )

    csv_row = {
        field: row.get(field, "")
        for field in DECISION_CSV_FIELDS
    }

    if isinstance(csv_row.get("result"), (dict, list)):
        csv_row["result"] = json.dumps(csv_row["result"])

    try:
        with open(DECISION_CSV_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=DECISION_CSV_FIELDS)

            if write_header:
                writer.writeheader()

            writer.writerow(csv_row)
    except Exception as e:
        log_event("CSV_WRITE_ERROR", message=str(e), file=DECISION_CSV_FILE)


def record_trade_history(entry):
    state.setdefault("trade_history", [])
    state["trade_history"].append(entry)
    state["trade_history"] = state["trade_history"][-250:]
    save_state(state)

# ----------------------
# SAFE REQUEST HELPERS
# ----------------------


def require_runtime_config():
    required = {
        "KRAKEN_API_KEY": KRAKEN_API_KEY,
        "KRAKEN_API_SECRET": KRAKEN_API_SECRET,
        "KRAKEN_API_URL": KRAKEN_API_URL
    }
    missing = [name for name, value in required.items() if not value]

    if missing:
        raise RuntimeError(f"Missing environment variables: {missing}")

    if not LLM_SIGNAL_URL and not SIGNAL_FILE:
        raise RuntimeError("Missing LLM_SIGNAL_URL or SIGNAL_FILE in .env")


def next_nonce():
    wall_nonce = int(time.time() * 1000)
    last_nonce = int(state.get("last_nonce", 0))
    nonce = max(wall_nonce, last_nonce + 1)
    state["last_nonce"] = nonce
    save_state(state)
    return str(nonce)


def kraken_private_pause_seconds():
    paused_until = parse_iso8601(state.get("kraken_private_paused_until"))
    if paused_until is None:
        return 0

    remaining = (paused_until - datetime.now(timezone.utc)).total_seconds()
    if remaining <= 0:
        state["kraken_private_paused_until"] = None
        save_state(state)
        return 0

    return remaining


def pause_kraken_private_api(reason):
    paused_until = datetime.fromtimestamp(
        time.time() + KRAKEN_LOCKOUT_COOLDOWN_SECONDS,
        timezone.utc
    )
    state["kraken_private_paused_until"] = paused_until.isoformat()
    save_state(state)
    log_event(
        "KRAKEN_PRIVATE_PAUSED",
        message=reason,
        paused_until=state["kraken_private_paused_until"],
        cooldown_seconds=KRAKEN_LOCKOUT_COOLDOWN_SECONDS
    )


def kraken_private_terminal_error(message):
    return any(
        marker in message
        for marker in (
            "Temporary lockout",
            "Invalid key",
            "Permission denied"
        )
    )


def kraken_signature(endpoint, data):
    postdata = "&".join([f"{k}={v}" for k, v in data.items()])
    encoded = (str(data["nonce"]) + postdata).encode()
    message = endpoint.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(
        base64.b64decode(KRAKEN_API_SECRET),
        message,
        hashlib.sha512
    )
    return base64.b64encode(mac.digest()).decode()


def kraken_private(endpoint, data):
    url = KRAKEN_API_URL.rstrip("/") + endpoint
    payload = dict(data)
    payload["nonce"] = next_nonce()

    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": kraken_signature(endpoint, payload)
    }

    r = requests.post(
        url,
        headers=headers,
        data=payload,
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()

    result = r.json()
    if result.get("error"):
        raise RuntimeError(result["error"])

    return result


def safe_kraken_private(label, endpoint, data=None):
    pause_seconds = kraken_private_pause_seconds()
    if pause_seconds > 0:
        log_event(
            "KRAKEN_PRIVATE_SKIPPED",
            operation=label,
            reason="private_api_paused",
            pause_seconds=round(pause_seconds, 2)
        )
        return None

    attempts = max(1, KRAKEN_NONCE_RETRIES + 1)

    for attempt in range(1, attempts + 1):
        try:
            return kraken_private(endpoint, data or {})
        except Exception as e:
            message = str(e)
            log_event(
                "KRAKEN_EXCEPTION",
                operation=label,
                message=message,
                attempt=attempt,
                kraken_api_url=KRAKEN_API_URL,
                kraken_key_fingerprint=key_fingerprint(KRAKEN_API_KEY)
            )

            if kraken_private_terminal_error(message):
                pause_kraken_private_api(message)
                return None

            if "Invalid nonce" not in message or attempt >= attempts:
                return None

            state["last_nonce"] = max(
                int(state.get("last_nonce", 0)),
                int(time.time() * 1000)
            ) + 1000
            save_state(state)
            time.sleep(1)

    return None


# ----------------------
# MARKET DATA
# ----------------------


def get_price():
    try:
        if KRAKEN_TICKER_URL:
            r = request_with_retries(
                "GET",
                KRAKEN_TICKER_URL,
                timeout=REQUEST_TIMEOUT
            )
            r.raise_for_status()
            data = r.json()

            if data.get("error"):
                raise RuntimeError(data["error"])

            ticker = next(iter(data["result"].values()), None)
            if ticker:
                return float(ticker["c"][0])

        url = KRAKEN_API_URL.rstrip("/") + "/0/public/Ticker"
        r = request_with_retries(
            "GET",
            url,
            params={"pair": KRAKEN_PAIR},
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        data = r.json()

        if data.get("error"):
            raise RuntimeError(data["error"])

        ticker = next(iter(data["result"].values()), None)
        if not ticker:
            raise RuntimeError(f"Ticker data not found for {KRAKEN_PAIR}")

        return float(ticker["c"][0])
    except Exception as e:
        log_event("PRICE_ERROR", message=str(e))
        return None


def get_orderbook():
    try:
        if KRAKEN_ORDERBOOK_URL:
            r = request_with_retries(
                "GET",
                KRAKEN_ORDERBOOK_URL,
                timeout=REQUEST_TIMEOUT
            )
        else:
            url = KRAKEN_API_URL.rstrip("/") + "/0/public/Depth"
            r = request_with_retries(
                "GET",
                url,
                params={"pair": KRAKEN_PAIR, "count": 5},
                timeout=REQUEST_TIMEOUT
            )
        r.raise_for_status()
        data = r.json()

        if data.get("error"):
            raise RuntimeError(data["error"])

        book = next(iter(data.get("result", {}).values()), None)
        if not book:
            raise RuntimeError(f"Order book data not found for {KRAKEN_PAIR}")

        bid = book.get("bids", [])[0]
        ask = book.get("asks", [])[0]
        return {
            "bid": float(bid[0]),
            "bid_volume": float(bid[1]),
            "ask": float(ask[0]),
            "ask_volume": float(ask[1])
        }
    except Exception as e:
        log_event("ORDERBOOK_ERROR", message=str(e))
        return None


def get_pair_info():
    global PAIR_INFO_CACHE

    if PAIR_INFO_CACHE:
        return PAIR_INFO_CACHE

    url = KRAKEN_API_URL.rstrip("/") + "/0/public/AssetPairs"
    r = request_with_retries(
        "GET",
        url,
        params={"pair": KRAKEN_PAIR},
        timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    data = r.json()

    if data.get("error"):
        raise RuntimeError(data["error"])

    pair_info = next(iter(data["result"].values()), None)
    if not pair_info:
        raise RuntimeError(f"Pair metadata not found for {KRAKEN_PAIR}")

    PAIR_INFO_CACHE = pair_info
    return PAIR_INFO_CACHE


# ----------------------
# BALANCES
# ----------------------


def get_balances():
    result = safe_kraken_private("BALANCE", "/0/private/Balance")
    if not result:
        return None

    pair_info = get_pair_info()
    base_asset = pair_info["base"]
    quote_asset = pair_info["quote"]

    base_balance = float(result["result"].get(base_asset, 0))
    quote_balance = float(result["result"].get(quote_asset, 0))

    return base_balance, quote_balance


def round_volume(volume):
    pair_info = get_pair_info()
    lot_decimals = int(pair_info.get("lot_decimals", 8))
    factor = 10 ** lot_decimals
    return math.floor(volume * factor) / factor


def round_price(price):
    pair_info = get_pair_info()
    pair_decimals = int(pair_info.get("pair_decimals", 1))
    return round(price, pair_decimals)


def get_min_order_volume():
    pair_info = get_pair_info()
    return float(pair_info.get("ordermin") or 0)


def query_order(txid):
    result = safe_kraken_private(
        "QUERY_ORDER",
        "/0/private/QueryOrders",
        {"txid": txid}
    )

    if not result:
        return None

    return result.get("result", {}).get(txid)


def query_orders(txids):
    txids = [txid for txid in txids if txid]
    if not txids:
        return {}

    result = safe_kraken_private(
        "QUERY_ORDERS",
        "/0/private/QueryOrders",
        {"txid": ",".join(txids)}
    )

    if not result:
        return {}

    return result.get("result", {})


def order_fill_summary(order):
    if not order:
        return {}

    return {
        "order_status": order.get("status"),
        "fill_volume": order.get("vol_exec"),
        "fill_cost": order.get("cost"),
        "fill_fee": order.get("fee"),
        "fill_price": order.get("price")
    }


# ----------------------
# SIGNAL
# ----------------------


def load_signal():
    try:
        if LLM_SIGNAL_URL:
            r = request_with_retries(
                "GET",
                LLM_SIGNAL_URL,
                timeout=REQUEST_TIMEOUT
            )
            r.raise_for_status()
            signal = r.json()
        else:
            with open(SIGNAL_FILE, encoding="utf-8") as f:
                signal = json.load(f)

        return signal
    except Exception as e:
        log_event("SENTIMENT_ERROR", message=str(e))
        return None


def load_backtest():
    backtest_url = BOT_REPLAY_BACKTEST_URL or BOT_POLICY_BACKTEST_URL
    if not backtest_url:
        return None, "missing_backtest_url"

    try:
        r = request_with_retries(
            "GET",
            backtest_url,
            timeout=REQUEST_TIMEOUT
        )
        r.raise_for_status()
        return r.json(), None
    except Exception as e:
        log_event("BACKTEST_FETCH_ERROR", message=str(e))
        return None, "backtest_unavailable"


def backtest_summary_pair(backtest):
    bot_outputs = backtest.get("bot_outputs")
    strategies = backtest.get("strategies")

    if isinstance(bot_outputs, dict):
        policy = bot_outputs.get("with_sentiment_policy")
        baseline = bot_outputs.get("price_target_only")
        if isinstance(policy, dict) and isinstance(baseline, dict):
            return policy, baseline, "replay"

        policy = bot_outputs.get("sentiment_long_policy")
        baseline = bot_outputs.get("baseline_positive_signal")
        if isinstance(policy, dict) and isinstance(baseline, dict):
            return policy, baseline, "policy"

    if isinstance(strategies, dict):
        policy = strategies.get("with_sentiment_policy")
        baseline = strategies.get("price_target_only")
        if isinstance(policy, dict) and isinstance(baseline, dict):
            return (
                policy.get("summary", {}),
                baseline.get("summary", {}),
                "replay"
            )

        policy = strategies.get("sentiment_policy")
        baseline = strategies.get("positive_signal_baseline")
        if isinstance(policy, dict) and isinstance(baseline, dict):
            return (
                policy.get("summary", {}),
                baseline.get("summary", {}),
                "policy"
            )

    return None, None, None


def evaluate_backtest_health(backtest):
    policy, baseline, backtest_kind = backtest_summary_pair(backtest)
    if not isinstance(policy, dict) or not isinstance(baseline, dict):
        return {"reason": "backtest_missing_strategy_outputs"}

    details = {
        "backtest_kind": backtest_kind,
        "backtest_policy_trades": policy.get("trades"),
        "backtest_policy_win_rate": policy.get("win_rate"),
        "backtest_policy_avg_net_return_pct": policy.get("avg_net_return_pct"),
        "backtest_policy_max_drawdown_pct": policy.get("max_drawdown_pct"),
        "backtest_baseline_win_rate": baseline.get("win_rate"),
        "backtest_baseline_avg_net_return_pct": baseline.get(
            "avg_net_return_pct"
        ),
        "backtest_baseline_max_drawdown_pct": baseline.get("max_drawdown_pct")
    }

    policy_trades = int(policy.get("trades") or 0)
    if policy_trades < BACKTEST_MIN_TRADES:
        details["reason"] = "backtest_insufficient_trades"
        return details

    if policy.get("win_rate") is None or policy.get("avg_net_return_pct") is None:
        details["reason"] = "backtest_missing_policy_metrics"
        return details

    if BACKTEST_REQUIRE_POLICY_BEATS_BASELINE:
        if (
            baseline.get("win_rate") is None
            or baseline.get("avg_net_return_pct") is None
        ):
            details["reason"] = "backtest_missing_baseline_metrics"
            return details

        if policy["win_rate"] < baseline["win_rate"]:
            details["reason"] = "backtest_policy_win_rate_below_baseline"
            return details

        if policy["avg_net_return_pct"] < baseline["avg_net_return_pct"]:
            details["reason"] = "backtest_policy_return_below_baseline"
            return details

        policy_drawdown = policy.get("max_drawdown_pct")
        baseline_drawdown = baseline.get("max_drawdown_pct")
        if (
            backtest_kind == "replay"
            and policy_drawdown is not None
            and baseline_drawdown is not None
            and policy_drawdown < baseline_drawdown
        ):
            details["reason"] = "backtest_policy_drawdown_below_baseline"
            return details

    return None


def backtest_health_failure():
    if not USE_BACKTEST_HEALTH_GATE:
        return None

    backtest, error = load_backtest()
    if backtest is None:
        if BACKTEST_FAIL_CLOSED:
            return {"reason": error or "backtest_unavailable"}
        return None

    failure = evaluate_backtest_health(backtest)
    if failure is not None and BACKTEST_FAIL_CLOSED:
        return failure

    return None


def pct_text(value):
    if value is None:
        return "n/a"

    try:
        return f"{float(value):.4f}%"
    except (TypeError, ValueError):
        return "n/a"


def win_rate_text(value):
    if value is None:
        return "n/a"

    try:
        return f"{float(value) * 100:.1f}%"
    except (TypeError, ValueError):
        return "n/a"


def number_text(value):
    if value is None:
        return "n/a"

    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return "n/a"

    if parsed.is_integer():
        return str(int(parsed))
    return f"{parsed:.2f}"


def money_text(value):
    if value is None:
        return "n/a"

    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "n/a"


def metric_total_return_pct(metrics):
    total_return = metrics.get("total_net_return_pct")
    if total_return is not None:
        return total_return

    avg_return = metrics.get("avg_net_return_pct")
    trades = metrics.get("trades")
    if avg_return is None or trades is None:
        return None

    try:
        return float(avg_return) * float(trades)
    except (TypeError, ValueError):
        return None


def pct_to_money(pct_value, usd_amount):
    if pct_value is None or usd_amount is None:
        return None

    try:
        return float(usd_amount) * float(pct_value) / 100
    except (TypeError, ValueError):
        return None


def backtest_strategy_names(backtest_kind):
    if backtest_kind == "replay":
        return "with_sentiment_policy", "price_target_only"

    return "sentiment_policy", "positive_signal_baseline"


def strategy_recent_trades(backtest, strategy_name):
    strategies = backtest.get("strategies", {})
    if not isinstance(strategies, dict):
        return []

    strategy = strategies.get(strategy_name, {})
    if not isinstance(strategy, dict):
        return []

    trades = strategy.get("recent_trades", [])
    return trades if isinstance(trades, list) else []


def print_emulated_trade(trade, usd_amount):
    net_return = trade.get("net_return_pct")
    pnl = pct_to_money(net_return, usd_amount)
    entry_price = trade.get("entry_price")
    exit_price = trade.get("exit_price")
    volume = None

    try:
        if entry_price:
            volume = float(usd_amount) / float(entry_price)
    except (TypeError, ValueError, ZeroDivisionError):
        volume = None

    proceeds = None if pnl is None else float(usd_amount) + pnl
    parts = [
        trade.get("decision_time", "n/a"),
        f"buy={money_text(usd_amount)}",
        f"entry={entry_price if entry_price is not None else 'n/a'}",
        f"exit={exit_price if exit_price is not None else 'n/a'}",
        f"reason={trade.get('exit_reason', 'n/a')}",
        f"net={pct_text(net_return)}",
        f"pnl={money_text(pnl)}",
        f"proceeds={money_text(proceeds)}",
    ]
    if volume is not None:
        parts.insert(3, f"btc={volume:.8f}")

    print("  " + " | ".join(parts))


def print_backtest_metric_block(label, metrics, usd_amount=None):
    print(f"{label}:")
    print(f"  trades: {number_text(metrics.get('trades'))}")
    print(f"  win rate: {win_rate_text(metrics.get('win_rate'))}")
    print(f"  avg net return: {pct_text(metrics.get('avg_net_return_pct'))}")
    print(f"  total net return: {pct_text(metrics.get('total_net_return_pct'))}")
    print(f"  max drawdown: {pct_text(metrics.get('max_drawdown_pct'))}")
    print(f"  max runup: {pct_text(metrics.get('max_runup_pct'))}")
    print(f"  avg hold: {number_text(metrics.get('avg_hold_minutes'))} min")
    print(f"  take profit / stop / timeout: "
          f"{number_text(metrics.get('take_profit_count'))} / "
          f"{number_text(metrics.get('stop_loss_count'))} / "
          f"{number_text(metrics.get('timeout_count'))}")

    if usd_amount is not None:
        total_return = metric_total_return_pct(metrics)
        avg_return = metrics.get("avg_net_return_pct")
        max_drawdown = metrics.get("max_drawdown_pct")
        print(f"  emulated allocation: {money_text(usd_amount)} per trade")
        print(f"  estimated avg PnL/trade: "
              f"{money_text(pct_to_money(avg_return, usd_amount))}")
        print(f"  estimated total PnL: "
              f"{money_text(pct_to_money(total_return, usd_amount))}")
        print(f"  estimated max drawdown: "
              f"{money_text(pct_to_money(max_drawdown, usd_amount))}")

    optional_counts = [
        ("candidate signals", "candidate_signals"),
        ("blocked by sentiment", "blocked_by_sentiment"),
        ("not filled", "not_filled"),
        ("skipped during position", "skipped_during_position"),
        ("no target", "no_target"),
    ]
    present = [
        f"{name}: {number_text(metrics.get(key))}"
        for name, key in optional_counts
        if key in metrics
    ]
    if present:
        print("  " + " | ".join(present))


def run_backtest_report():
    backtest_url = BOT_REPLAY_BACKTEST_URL or BOT_POLICY_BACKTEST_URL
    print("Backtest report")
    print(f"URL: {backtest_url or 'unset'}")
    print(f"strategy_profile: {STRATEGY_PROFILE}")
    print(f"min_trades: {BACKTEST_MIN_TRADES}")
    if BACKTEST_USD is not None:
        print(f"usd_per_trade: {money_text(BACKTEST_USD)}")
    print(
        "require_policy_beats_baseline: "
        f"{BACKTEST_REQUIRE_POLICY_BEATS_BASELINE}"
    )
    print("")

    backtest, error = load_backtest()
    if backtest is None:
        print(f"Result: FAIL ({error or 'backtest_unavailable'})")
        return 1

    policy, baseline, backtest_kind = backtest_summary_pair(backtest)
    if not isinstance(policy, dict) or not isinstance(baseline, dict):
        print("Result: FAIL (backtest_missing_strategy_outputs)")
        return 1

    print(f"timestamp: {backtest.get('timestamp', 'n/a')}")
    if backtest.get("since"):
        print(f"since: {backtest.get('since')}")
    if backtest.get("signal_count") is not None:
        print(f"signals tested: {number_text(backtest.get('signal_count'))}")

    simulation = backtest.get("simulation", {})
    if isinstance(simulation, dict):
        print(
            "simulation: "
            f"{simulation.get('side', 'n/a')} | "
            f"entry={simulation.get('entry', 'n/a')} | "
            f"tp={simulation.get('take_profit_pct', 'n/a')} | "
            f"sl={simulation.get('stop_loss_pct', 'n/a')} | "
            f"max_hold={simulation.get('max_hold_hours', 'n/a')}h"
        )
    print("")

    policy_name, baseline_name = backtest_strategy_names(backtest_kind)
    print_backtest_metric_block(policy_name, policy, BACKTEST_USD)
    print("")
    print_backtest_metric_block(baseline_name, baseline, BACKTEST_USD)
    print("")

    failure = evaluate_backtest_health(backtest)
    if failure is None:
        print("Verdict: PASS")
        print("Finding: sentiment policy is healthy enough under current gates.")
    else:
        print(f"Verdict: FAIL ({failure.get('reason')})")
        print("Finding: live buys would be blocked by the backtest gate.")

    if policy.get("trades") is not None and baseline.get("trades") is not None:
        trade_delta = int(policy.get("trades") or 0) - int(
            baseline.get("trades") or 0
        )
        print(f"Trade count delta vs baseline: {trade_delta}")

    if (
        policy.get("avg_net_return_pct") is not None
        and baseline.get("avg_net_return_pct") is not None
    ):
        return_delta = (
            float(policy.get("avg_net_return_pct"))
            - float(baseline.get("avg_net_return_pct"))
        )
        print(f"Avg return delta vs baseline: {pct_text(return_delta)}")
        if BACKTEST_USD is not None:
            print(
                "Avg PnL delta/trade vs baseline: "
                f"{money_text(pct_to_money(return_delta, BACKTEST_USD))}"
            )

    if (
        policy.get("max_drawdown_pct") is not None
        and baseline.get("max_drawdown_pct") is not None
    ):
        drawdown_delta = (
            float(policy.get("max_drawdown_pct"))
            - float(baseline.get("max_drawdown_pct"))
        )
        print(f"Drawdown delta vs baseline: {pct_text(drawdown_delta)}")

    recent_trades = strategy_recent_trades(backtest, policy_name)
    if recent_trades:
        print("")
        print("Recent sentiment-policy trades:")
        for trade in recent_trades[-3:]:
            if BACKTEST_USD is not None:
                print_emulated_trade(trade, BACKTEST_USD)
            else:
                print(
                    "  "
                    f"{trade.get('decision_time', 'n/a')} | "
                    f"entry={trade.get('entry_price', 'n/a')} | "
                    f"exit={trade.get('exit_price', 'n/a')} | "
                    f"reason={trade.get('exit_reason', 'n/a')} | "
                    f"net={pct_text(trade.get('net_return_pct'))}"
                )

    return 0 if failure is None else 2


def price_tick():
    pair_info = get_pair_info()
    pair_decimals = int(pair_info.get("pair_decimals", 1))
    return 10 ** (-pair_decimals)


def buynow_price(book):
    tick = price_tick()
    bid = book["bid"]
    ask = book["ask"]

    if ask > bid + (2 * tick):
        return round_price(bid + tick)

    return round_price(bid)


def run_buynow():
    require_runtime_config()

    usd_amount = BUYNOW_USD or MIN_TRADE_USD
    if usd_amount <= 0:
        console("BUYNOW_ERROR: usd amount must be positive")
        return 1

    book = get_orderbook()
    if not book:
        console("BUYNOW_ERROR: unable to fetch order book")
        return 1

    buy_price = buynow_price(book)
    volume = round_volume(usd_amount / buy_price)
    min_volume = get_min_order_volume()
    if min_volume and volume < min_volume:
        min_usd = min_volume * buy_price
        console(
            "BUYNOW_ERROR: "
            f"{money_text(usd_amount)} is below minimum order size; "
            f"need about {money_text(min_usd)}"
        )
        return 1

    cycle_id = datetime.now(timezone.utc).isoformat()
    target_profit_pct = TARGET_PROFIT_PCT
    log_and_console(
        "BUYNOW",
        message=f"post-only buy {volume} @ {buy_price}",
        cycle_id=cycle_id,
        usd_amount=usd_amount,
        bid=book["bid"],
        ask=book["ask"],
        price=buy_price,
        volume=volume,
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT
    )
    result = place_limit_buy(
        buy_price,
        volume,
        cycle_id,
        target_profit_pct=target_profit_pct,
        post_only=True
    )
    if result:
        process_open_buy_orders(cycle_id)
        console(
            "BUYNOW_SUBMITTED: "
            "submitted and checked once for an immediate fill"
        )
        return 0

    console("BUYNOW_ERROR: order was not submitted")
    return 1


def select_asset_signal(signal):
    assets = signal.get("assets")
    if not isinstance(assets, dict):
        return signal

    asset_id = SELECTED_SIGNAL_ASSET_ID
    selected = assets.get(asset_id)
    if isinstance(selected, dict):
        result = dict(selected)
        result.setdefault("processed_at", signal.get("processed_at"))
        result.setdefault("freshness", signal.get("freshness"))
        result.setdefault("schema_version", signal.get("single_asset_schema_version"))
        return result

    available = ",".join(sorted(str(key) for key in assets.keys()))
    log_event(
        "SIGNAL_ASSET_MISSING",
        requested_asset_id=asset_id,
        available_asset_ids=available
    )
    return {}


def normalize_price_regime(price_regime):
    normalized = dict(price_regime)
    aliases = {
        "range_position": "range_position_24h",
        "realized_volatility_pct": "realized_volatility_24h_pct",
        "price_high": "price_high_24h",
        "price_low": "price_low_24h",
        "price_mean": "price_mean_24h",
        "price_median": "price_median_24h",
        "price_return_24h_pct": "return_24h_pct",
    }
    for source, target in aliases.items():
        if target not in normalized and source in normalized:
            normalized[target] = normalized[source]
    return normalized


def normalize_source_status(source_status):
    normalized = dict(source_status)
    if "asset_price" in normalized and "market_data" not in normalized:
        normalized["market_data"] = normalized["asset_price"]
    if "asset_price_regime" in normalized and "price_regime" not in normalized:
        normalized["price_regime"] = normalized["asset_price_regime"]
    return normalized


def normalize_signal(signal):
    if not isinstance(signal, dict):
        return {
            "execution_signal": float(signal),
            "confidence": 1.0,
            "price_regime": {}
        }

    signal = select_asset_signal(signal)

    price_regime = signal.get("price_regime")
    if not isinstance(price_regime, dict):
        price_regime = signal.get("asset_price_regime")
    if not isinstance(price_regime, dict):
        price_regime = {}
    price_regime = normalize_price_regime(price_regime)

    kraken_flow = signal.get("kraken_flow")
    if not isinstance(kraken_flow, dict):
        kraken_flow = {}

    source_status = signal.get("source_status")
    if not isinstance(source_status, dict):
        source_status = {}
    source_status = normalize_source_status(source_status)

    action_policy = signal.get("action_policy")
    if not isinstance(action_policy, dict):
        action_policy = {}

    risk_context = signal.get("risk_context")
    if not isinstance(risk_context, dict):
        risk_context = {}

    active_strategy = signal.get("active_strategy")
    if not isinstance(active_strategy, dict):
        active_strategy = {}

    target_prices = signal.get("target_prices")
    if not isinstance(target_prices, list):
        target_prices = []

    return {
        "asset_id": signal.get("asset_id"),
        "asset_symbol": signal.get("asset", {}).get("symbol")
        if isinstance(signal.get("asset"), dict)
        else signal.get("asset_id"),
        "asset_name": signal.get("asset", {}).get("name")
        if isinstance(signal.get("asset"), dict)
        else None,
        "asset_price": signal.get("asset_price"),
        "asset_sentiment": signal.get("asset_sentiment"),
        "liquidity_risk": signal.get("liquidity_risk"),
        "btc_relative_strength": signal.get("btc_relative_strength"),
        "eth_relative_strength": signal.get("eth_relative_strength"),
        "market_interpretation": signal.get("market_interpretation"),
        "execution_signal": float(signal.get("execution_signal", 0)),
        "confidence": float(signal.get("confidence", 0)),
        "btc_sentiment": signal.get("btc_sentiment", signal.get("asset_sentiment")),
        "regulatory_risk": signal.get("regulatory_risk"),
        "macro_tightening_bias": signal.get("macro_tightening_bias"),
        "direction_bias": signal.get("direction_bias"),
        "risk_multiplier": signal.get("risk_multiplier"),
        "smoothed_risk_multiplier": signal.get("smoothed_risk_multiplier"),
        "mean_reversion_opportunity": signal.get("mean_reversion_opportunity"),
        "flow_pressure": signal.get("flow_pressure"),
        "raw_btc_sentiment": signal.get(
            "raw_btc_sentiment",
            signal.get("asset_sentiment")
        ),
        "raw_confidence": signal.get("raw_confidence"),
        "raw_direction_bias": signal.get("raw_direction_bias"),
        "fear_greed_index": signal.get("fear_greed_index"),
        "signal_status": signal.get("signal_status"),
        "bot_action_allowed": signal.get("bot_action_allowed"),
        "action_recommendation": signal.get("action_recommendation"),
        "action_policy": action_policy,
        "risk_context": risk_context,
        "active_strategy": active_strategy,
        "contributor_count": signal.get("contributor_count"),
        "reason": signal.get("reason"),
        "processed_at": signal.get("processed_at"),
        "freshness": signal.get("freshness")
        if isinstance(signal.get("freshness"), dict)
        else {},
        "price_regime": price_regime,
        "kraken_flow": kraken_flow,
        "source_status": source_status,
        "target_prices": target_prices
    }


def smooth_signal(current_signal):
    prev1, prev2 = state["signal_memory"]
    smoothed = (
        0.6 * current_signal
        + 0.3 * prev1
        + 0.1 * prev2
    )

    state["signal_memory"] = [current_signal, prev1]
    save_state(state)

    return smoothed


# ----------------------
# ORDER EXECUTION
# ----------------------


def fill_values(order, fallback_volume=None, fallback_price=None):
    if not order:
        return {
            "volume": fallback_volume,
            "price": fallback_price,
            "cost": None,
            "fee": None
        }

    fill_volume = float(order.get("vol_exec") or order.get("vol") or 0)
    fill_cost = float(order.get("cost") or 0)
    fill_fee = float(order.get("fee") or 0)
    fill_price = float(order.get("price") or 0)

    if fill_price <= 0 and fill_cost > 0 and fill_volume > 0:
        fill_price = fill_cost / fill_volume

    return {
        "volume": fill_volume or fallback_volume,
        "price": fill_price or fallback_price,
        "cost": fill_cost or None,
        "fee": fill_fee or None
    }


def sell_target_price(buy_price, target_profit_pct=None):
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )
    return buy_price * (1 + profit_pct + ROUND_TRIP_FEE_PCT)


def log_shadow_buy_plan(
    cycle_id,
    *,
    reason,
    ordertype,
    price,
    volume,
    trade_value,
    target_profit_pct=None,
    fill_mode=None,
    **kwargs
):
    if not SHADOW_TRADES_ENABLED:
        return

    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )
    projected_sell_price = sell_target_price(price, profit_pct) if price else None
    log_trade_activity(
        "SHADOW_BUY_PLANNED",
        mode="shadow",
        cycle_id=cycle_id,
        reason=reason,
        side="buy",
        ordertype=ordertype,
        fill_mode=fill_mode,
        price=price,
        volume=volume,
        trade_value=trade_value,
        target_profit_pct=profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT,
        projected_sell_price=(
            round_price(projected_sell_price)
            if projected_sell_price is not None
            else None
        ),
        **kwargs
    )


def place_market_buy(volume, cycle_id):
    if DRY_RUN:
        state["stats"]["dry_run_trades"] += 1
        state["stats"]["buy_orders_placed"] += 1
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": True,
                "side": "buy",
                "volume": volume
            }
        )
        log_and_console(
            "DRY_RUN_BUY",
            message=f"buy {volume}",
            cycle_id=cycle_id,
            side="buy",
            volume=volume
        )
        log_trade_activity(
            "BUY_ORDER_DRY_RUN",
            cycle_id=cycle_id,
            side="buy",
            ordertype="market",
            volume=volume
        )
        return {"dry_run": True}

    result = safe_kraken_private(
        "BUY",
        "/0/private/AddOrder",
        {
            "pair": KRAKEN_PAIR,
            "type": "buy",
            "ordertype": "market",
            "volume": volume
        }
    )

    if not result:
        log_trade_activity(
            "BUY_ORDER_REJECTED",
            cycle_id=cycle_id,
            side="buy",
            ordertype="market",
            reason="api_no_result",
            volume=volume
        )
        return result

    if result:
        txids = result.get("result", {}).get("txid", [])
        txid = txids[0] if txids else None
        fill = order_fill_summary(query_order(txid)) if txid else {}
        state["stats"]["trades_executed"] += 1
        state["stats"]["buy_orders_placed"] += 1
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": False,
                "side": "buy",
                "volume": volume,
                "txid": txid,
                **fill
            }
        )
        log_and_console(
            "BUY_ORDER_PLACED",
            message=f"buy {volume}",
            cycle_id=cycle_id,
            side="buy",
            volume=volume,
            txid=txid,
            **fill,
            result=result.get("result")
        )
        log_trade_activity(
            "BUY_ORDER_PLACED",
            cycle_id=cycle_id,
            side="buy",
            ordertype="market",
            volume=volume,
            txid=txid,
            **fill
        )
        result["fill"] = fill

    return result


def place_limit_buy(price, volume, cycle_id, target_profit_pct=None, post_only=False):
    buy_price = round_price(price)
    buy_volume = round_volume(volume)
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )

    if DRY_RUN:
        state["stats"]["dry_run_trades"] += 1
        state["stats"]["buy_orders_placed"] += 1
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": True,
                "side": "buy",
                "ordertype": "limit",
                "volume": buy_volume,
                "price": buy_price,
                "post_only": post_only,
                "target_profit_pct": profit_pct,
                "round_trip_fee_pct": ROUND_TRIP_FEE_PCT
            }
        )
        log_and_console(
            "DRY_RUN_LIMIT_BUY",
            message=f"buy {buy_volume} @ {buy_price}",
            cycle_id=cycle_id,
            side="buy",
            volume=buy_volume,
            price=buy_price,
            post_only=post_only,
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT
        )
        log_trade_activity(
            "BUY_LIMIT_ORDER_DRY_RUN",
            cycle_id=cycle_id,
            side="buy",
            ordertype="limit",
            volume=buy_volume,
            price=buy_price,
            post_only=post_only,
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT
        )
        return {"dry_run": True}

    order_payload = {
        "pair": KRAKEN_PAIR,
        "type": "buy",
        "ordertype": "limit",
        "price": str(buy_price),
        "volume": str(buy_volume)
    }
    if post_only:
        order_payload["oflags"] = "post"

    result = safe_kraken_private(
        "LIMIT_BUY",
        "/0/private/AddOrder",
        order_payload
    )

    if not result:
        log_trade_activity(
            "BUY_ORDER_REJECTED",
            cycle_id=cycle_id,
            side="buy",
            ordertype="limit",
            reason="api_no_result",
            price=buy_price,
            volume=buy_volume,
            post_only=post_only,
            target_profit_pct=profit_pct
        )
        return result

    if result:
        txids = result.get("result", {}).get("txid", [])
        txid = txids[0] if txids else None
        if not txid:
            log_event(
                "ORDER_REJECTED",
                cycle_id=cycle_id,
                side="buy",
                reason="missing_txid",
                result=result.get("result")
            )
            log_trade_activity(
                "BUY_ORDER_REJECTED",
                cycle_id=cycle_id,
                side="buy",
                ordertype="limit",
                reason="missing_txid",
                price=buy_price,
                volume=buy_volume,
                result=result.get("result")
            )
            return result

        state["stats"]["trades_executed"] += 1
        state["stats"]["buy_orders_placed"] += 1
        state["last_trade_at"] = datetime.now(timezone.utc).isoformat()
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": buy_volume,
            "price": buy_price,
            "post_only": post_only,
            "target_profit_pct": profit_pct,
            "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
            "placed_at": cycle_id,
            "trade_id": txid
        }
        save_state(state)
        record_trade_history(
            {
                "ts": state["last_trade_at"],
                "cycle_id": cycle_id,
                "dry_run": False,
                "side": "buy",
                "ordertype": "limit",
                "volume": buy_volume,
                "price": buy_price,
                "post_only": post_only,
                "target_profit_pct": profit_pct,
                "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
                "txid": txid
            }
        )
        log_and_console(
            "BUY_LIMIT_ORDER_PLACED",
            message=f"buy {buy_volume} @ {buy_price}",
            cycle_id=cycle_id,
            side="buy",
            volume=buy_volume,
            price=buy_price,
            post_only=post_only,
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT,
            txid=txid,
            result=result.get("result")
        )
        log_trade_activity(
            "BUY_LIMIT_ORDER_PLACED",
            cycle_id=cycle_id,
            side="buy",
            ordertype="limit",
            volume=buy_volume,
            price=buy_price,
            post_only=post_only,
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT,
            txid=txid,
            trade_id=txid,
            result=result.get("result")
        )
        notify_order_tracker(
            trade_id=txid,
            side="buy",
            price=buy_price,
            quantity=buy_volume,
            order_id=txid,
            timestamp=cycle_id,
            notes="limit_buy_submitted"
        )

    return result


def place_limit_sell(
    price,
    volume,
    cycle_id,
    buy_txid=None,
    buy_price=None,
    target_profit_pct=None
):
    sell_price = round_price(price)
    sell_volume = round_volume(volume)

    if DRY_RUN:
        log_and_console(
            "DRY_RUN_SELL",
            message=f"sell {sell_volume} @ {sell_price}",
            cycle_id=cycle_id,
            side="sell",
            volume=sell_volume,
            price=sell_price,
            buy_txid=buy_txid,
            buy_price=buy_price,
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=(
                target_profit_pct + ROUND_TRIP_FEE_PCT
                if target_profit_pct is not None
                else None
            )
        )
        log_trade_activity(
            "SELL_ORDER_DRY_RUN",
            cycle_id=cycle_id,
            side="sell",
            ordertype="limit",
            volume=sell_volume,
            price=sell_price,
            buy_price=buy_price,
            buy_txid=buy_txid,
            trade_id=buy_txid,
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=(
                target_profit_pct + ROUND_TRIP_FEE_PCT
                if target_profit_pct is not None
                else None
            )
        )
        return {"dry_run": True}

    result = safe_kraken_private(
        "SELL",
        "/0/private/AddOrder",
        {
            "pair": KRAKEN_PAIR,
            "type": "sell",
            "ordertype": "limit",
            "price": str(sell_price),
            "volume": str(sell_volume)
        }
    )

    if not result:
        log_trade_activity(
            "SELL_ORDER_REJECTED",
            cycle_id=cycle_id,
            side="sell",
            ordertype="limit",
            reason="api_no_result",
            price=sell_price,
            volume=sell_volume,
            buy_price=buy_price,
            buy_txid=buy_txid,
            target_profit_pct=target_profit_pct
        )
        return result

    if result:
        txids = result.get("result", {}).get("txid", [])
        txid = txids[0] if txids else None
        if not txid:
            log_event(
                "ORDER_REJECTED",
                cycle_id=cycle_id,
                side="sell",
                reason="missing_txid",
                result=result.get("result")
            )
            log_trade_activity(
                "SELL_ORDER_REJECTED",
                cycle_id=cycle_id,
                side="sell",
                ordertype="limit",
                reason="missing_txid",
                price=sell_price,
                volume=sell_volume,
                buy_price=buy_price,
                buy_txid=buy_txid,
                result=result.get("result")
            )
            return result

        state["stats"]["sell_orders_placed"] += 1
        state["open_sell_orders"][txid] = {
            "txid": txid,
            "volume": sell_volume,
            "buy_price": buy_price,
            "sell_price": sell_price,
            "buy_txid": buy_txid,
            "target_profit_pct": target_profit_pct,
            "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
            "placed_at": cycle_id,
            "trade_id": buy_txid or txid
        }
        save_state(state)
        log_and_console(
            "SELL_ORDER_PLACED",
            message=f"sell {sell_volume} @ {sell_price}",
            cycle_id=cycle_id,
            txid=txid,
            volume=sell_volume,
            price=sell_price,
            buy_price=buy_price,
            buy_txid=buy_txid,
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=(
                target_profit_pct + ROUND_TRIP_FEE_PCT
                if target_profit_pct is not None
                else None
            )
        )
        log_trade_activity(
            "SELL_ORDER_PLACED",
            cycle_id=cycle_id,
            side="sell",
            ordertype="limit",
            txid=txid,
            trade_id=buy_txid or txid,
            volume=sell_volume,
            price=sell_price,
            buy_price=buy_price,
            buy_txid=buy_txid,
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=(
                target_profit_pct + ROUND_TRIP_FEE_PCT
                if target_profit_pct is not None
                else None
            ),
            result=result.get("result")
        )
        notify_order_tracker(
            trade_id=buy_txid or txid,
            side="sell",
            price=sell_price,
            quantity=sell_volume,
            order_id=txid,
            timestamp=cycle_id
        )

    return result


# ----------------------
# EXECUTOR
# ----------------------


def skip_cycle(reason, cycle_id, **kwargs):
    state["stats"]["skips"] += 1
    save_state(state)
    console(f"HOLD: {reason}")
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="hold",
        reason=reason,
        **kwargs
    )
    append_decision_csv(
        "hold",
        cycle_id=cycle_id,
        side="hold",
        reason=reason,
        dry_run=DRY_RUN,
        **kwargs
    )


def cooldown_active(now, weighted_signal):
    last_trade_at = parse_iso8601(state.get("last_trade_at"))
    if REBALANCE_COOLDOWN_MINUTES <= 0 or last_trade_at is None:
        return None

    elapsed_minutes = (now - last_trade_at).total_seconds() / 60
    if elapsed_minutes >= REBALANCE_COOLDOWN_MINUTES:
        return None

    if abs(weighted_signal) >= COOLDOWN_OVERRIDE_SIGNAL_ABS:
        return None

    return {
        "elapsed_minutes": elapsed_minutes,
        "cooldown_minutes": REBALANCE_COOLDOWN_MINUTES,
        "cooldown_override_signal_abs": COOLDOWN_OVERRIDE_SIGNAL_ABS
    }


def trade_entry_price(entry):
    for key in ("price", "fill_price", "buy_price"):
        value = numeric_or_none(entry.get(key))
        if value is not None and value > 0:
            return value
    return None


def recent_buy_entries(now):
    if not ENTRY_PACING_ENABLED or ENTRY_PACING_WINDOW_MINUTES <= 0:
        return []

    cutoff_seconds = ENTRY_PACING_WINDOW_MINUTES * 60
    entries = []
    for entry in state.get("trade_history", []):
        if entry.get("side") != "buy":
            continue

        entry_time = parse_iso8601(entry.get("ts"))
        if entry_time is None:
            continue

        age_seconds = (now - entry_time).total_seconds()
        if age_seconds < 0 or age_seconds > cutoff_seconds:
            continue

        entries.append({
            "ts": entry.get("ts"),
            "price": trade_entry_price(entry),
            "txid": entry.get("txid")
        })

    return entries


def entry_pacing_block(now, price):
    if not ENTRY_PACING_ENABLED:
        return None

    recent_entries = recent_buy_entries(now)
    if ENTRY_PACING_MAX_BUYS > 0 and len(recent_entries) >= ENTRY_PACING_MAX_BUYS:
        return {
            "reason": "entry_pacing_max_buys",
            "entry_pacing_window_minutes": ENTRY_PACING_WINDOW_MINUTES,
            "entry_pacing_max_buys": ENTRY_PACING_MAX_BUYS,
            "recent_buy_count": len(recent_entries)
        }

    current_price = numeric_or_none(price)
    if current_price is None or ENTRY_PACING_MIN_PRICE_MOVE_PCT <= 0:
        return None

    comparable_prices = [
        entry["price"]
        for entry in recent_entries
        if entry.get("price") is not None and entry.get("price") > 0
    ]
    if not comparable_prices:
        return None

    nearest_price = min(
        comparable_prices,
        key=lambda entry_price: abs(current_price - entry_price)
    )
    distance_pct = abs(current_price - nearest_price) / nearest_price
    if distance_pct < ENTRY_PACING_MIN_PRICE_MOVE_PCT:
        return {
            "reason": "entry_pacing_price_cluster",
            "entry_pacing_window_minutes": ENTRY_PACING_WINDOW_MINUTES,
            "entry_pacing_min_price_move_pct": ENTRY_PACING_MIN_PRICE_MOVE_PCT,
            "recent_buy_count": len(recent_entries),
            "nearest_recent_buy_price": nearest_price,
            "entry_price_distance_pct": distance_pct
        }

    return None


def numeric_or_none(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def clamp(value, lower, upper):
    return max(lower, min(upper, value))


def signal_age_minutes(sentiment, now):
    freshness = sentiment.get("freshness")
    freshness_processed_at = (
        freshness.get("processed_at")
        if isinstance(freshness, dict)
        else None
    )
    processed_at = parse_iso8601(sentiment.get("processed_at") or freshness_processed_at)
    if processed_at is None:
        return None

    return (now - processed_at).total_seconds() / 60


def freshness_contract(sentiment):
    freshness = sentiment.get("freshness")
    return freshness if isinstance(freshness, dict) else {}


def freshness_minutes(sentiment, key):
    value = freshness_contract(sentiment).get(key)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def signal_freshness_state(sentiment, now):
    age_minutes = signal_age_minutes(sentiment, now)
    if age_minutes is None:
        return "unknown"

    stale_after = freshness_minutes(sentiment, "stale_after_minutes")
    warn_after = freshness_minutes(sentiment, "warn_after_minutes")
    fresh_for = freshness_minutes(sentiment, "fresh_for_minutes")

    if stale_after is not None and age_minutes > stale_after:
        return "stale"
    if warn_after is not None and age_minutes > warn_after:
        return "warn"
    if fresh_for is not None and age_minutes <= fresh_for:
        return "fresh"
    return "fresh"


def derived_risk_context(sentiment, now):
    freshness_state = signal_freshness_state(sentiment, now)
    return derive_risk_context(
        sentiment.get("risk_context"),
        fallback_processed_at=sentiment.get("processed_at"),
        stale=freshness_state == "stale",
        now=now
    )


def risk_context_trade_pause(risk_view):
    if risk_view.get("weather_emergency_bell"):
        return True, "weather_emergency_bell"

    if (
        RISK_CONTEXT_HARD_SAFETY_BLOCK
        and not risk_view.get("weather_report_available")
        and risk_view.get("risk_context_hard_safety_flags")
    ):
        return True, "risk_context_hard_safety_flags"

    if risk_view.get("risk_context_stale"):
        return False, "risk_context_stale"

    return False, None


def effective_market_location_range_position(risk_view, range_position):
    weather_range_position = risk_view.get("weather_market_range_position")
    if weather_range_position is not None:
        return weather_range_position
    return range_position


def market_location_near_high(risk_view, range_position):
    if not HIGH_ENTRY_QUALITY_ENABLED:
        return False

    effective_range_position = effective_market_location_range_position(
        risk_view,
        range_position
    )
    distance_to_high = risk_view.get("weather_market_distance_to_recent_high_pct")
    range_zone = str(risk_view.get("weather_market_range_zone") or "").lower()

    if (
        effective_range_position is not None
        and effective_range_position >= HIGH_ENTRY_RANGE_POSITION_MIN
    ):
        return True
    if (
        distance_to_high is not None
        and distance_to_high <= HIGH_ENTRY_DISTANCE_TO_HIGH_PCT
    ):
        return True
    return range_zone in (
        "upper_range",
        "near_high",
        "range_high",
        "breakout_zone",
    )


def high_entry_quality_check(risk_view, range_position):
    if not market_location_near_high(risk_view, range_position):
        return {
            "near_high": False,
            "allowed": True,
            "reason": None,
            "size_multiplier": 1.0,
        }

    if risk_view.get("weather_emergency_bell"):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "weather_emergency_bell",
            "size_multiplier": 0.0,
        }

    alert_level = str(risk_view.get("weather_alert_level") or "normal").lower()
    if alert_level in ("danger", "storm", "risk_off"):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_alert_level",
            "size_multiplier": 0.0,
        }

    market_risk = risk_view.get("risk_context_market_risk_score")
    if (
        market_risk is not None
        and market_risk > HIGH_ENTRY_MAX_MARKET_RISK_SCORE
    ):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_market_risk_high",
            "size_multiplier": 0.0,
        }

    condition = str(risk_view.get("weather_condition") or "").lower()
    opportunity_tags = {
        str(tag).lower()
        for tag in (risk_view.get("weather_opportunity_tags") or [])
    }
    confirmation_score = max(
        risk_view.get("risk_context_breakout_score") or 0.0,
        risk_view.get("risk_context_rebound_score") or 0.0,
        risk_view.get("risk_context_buy_aggression_score") or 0.0,
    )
    breakout_weather = (
        condition == "breakout_tailwind"
        or "breakout_tailwind" in opportunity_tags
    )
    if (
        not breakout_weather
        and confirmation_score < HIGH_ENTRY_MIN_CONFIRMATION_SCORE
    ):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_confirmation_low",
            "size_multiplier": 0.0,
        }

    price_return_4h = risk_view.get("weather_market_price_return_4h_pct")
    if (
        HIGH_ENTRY_REQUIRE_POSITIVE_4H_RETURN
        and price_return_4h is not None
        and price_return_4h <= 0
    ):
        return {
            "near_high": True,
            "allowed": False,
            "reason": "high_entry_4h_momentum_negative",
            "size_multiplier": 0.0,
        }

    return {
        "near_high": True,
        "allowed": True,
        "reason": "high_entry_breakout_confirmed",
        "size_multiplier": clamp(HIGH_ENTRY_SIZE_MULTIPLIER, 0.0, 1.0),
    }


def weather_report_bot_decides(sentiment):
    risk_context = sentiment.get("risk_context")
    if not isinstance(risk_context, dict):
        return False
    weather = risk_context.get("weather_report")
    if not isinstance(weather, dict):
        return False
    return (
        weather.get("bot_decision_authority") == "bot"
        or weather.get("trade_permission") == "bot_decides"
    )


def signal_gate_failure(sentiment, now):
    if not USE_SIGNAL_STATUS_GATES:
        return None

    signal_status = sentiment.get("signal_status")
    age_minutes = signal_age_minutes(sentiment, now)
    freshness_state = signal_freshness_state(sentiment, now)
    contract = freshness_contract(sentiment)
    if (
        signal_status
        and signal_status != "fresh"
        and not contract
        and not weather_report_bot_decides(sentiment)
    ):
        return {
            "reason": "signal_not_fresh",
            "signal_status": signal_status,
            "bot_action_allowed": sentiment.get("bot_action_allowed"),
            "source_status_result": signal_status
        }
    if freshness_state == "stale":
        return {
            "reason": "signal_too_old",
            "signal_status": signal_status,
            "bot_action_allowed": sentiment.get("bot_action_allowed"),
            "signal_age_minutes": age_minutes,
            "source_status_result": "freshness_contract:stale"
        }

    if (
        REQUIRE_BOT_ACTION_ALLOWED
        and sentiment.get("bot_action_allowed") is False
        and not weather_report_bot_decides(sentiment)
    ):
        return {
            "reason": "bot_action_not_allowed",
            "signal_status": signal_status,
            "bot_action_allowed": sentiment.get("bot_action_allowed"),
            "source_status_result": sentiment.get("reason")
        }

    if (
        not contract
        and
        MAX_SIGNAL_AGE_MINUTES > 0
        and age_minutes is not None
        and age_minutes > MAX_SIGNAL_AGE_MINUTES
    ):
        return {
            "reason": "signal_too_old",
            "signal_status": signal_status,
            "bot_action_allowed": sentiment.get("bot_action_allowed"),
            "signal_age_minutes": age_minutes
        }

    source_status = sentiment.get("source_status", {})
    for source in CRITICAL_SOURCE_STATUSES:
        status = source_status.get(source, {})
        if not isinstance(status, dict):
            continue
        if (
            status.get("status") not in (None, "fresh", "not_configured")
            and not contract
        ):
            return {
                "reason": "critical_source_not_fresh",
                "signal_status": signal_status,
                "bot_action_allowed": sentiment.get("bot_action_allowed"),
                "source_status_result": f"{source}:{status.get('status')}"
            }

    return None


def effective_risk_multiplier(sentiment):
    if not USE_RISK_MULTIPLIER:
        return 1.0

    multiplier = numeric_or_none(sentiment.get("smoothed_risk_multiplier"))
    if multiplier is None:
        multiplier = numeric_or_none(sentiment.get("risk_multiplier"))
    if multiplier is None:
        return 1.0

    return clamp(multiplier, MIN_RISK_MULTIPLIER, MAX_RISK_MULTIPLIER)


def mean_reversion_setup_allowed(sentiment):
    price_regime = sentiment.get("price_regime", {})
    kraken_flow = sentiment.get("kraken_flow", {})

    opportunity = numeric_or_none(sentiment.get("mean_reversion_opportunity"))
    if opportunity is None:
        opportunity = numeric_or_none(
            price_regime.get("mean_reversion_opportunity")
        )

    range_position = numeric_or_none(price_regime.get("range_position_24h"))

    flow_pressure = numeric_or_none(sentiment.get("flow_pressure"))
    if flow_pressure is None:
        flow_pressure = numeric_or_none(kraken_flow.get("aggression_score"))

    if opportunity is None or range_position is None or flow_pressure is None:
        return False

    return (
        opportunity >= MEAN_REVERSION_BUY_THRESHOLD
        and range_position <= MEAN_REVERSION_RANGE_POSITION_MAX
        and flow_pressure >= MEAN_REVERSION_FLOW_PRESSURE_MIN
    )


def dynamic_target_profit_pct(sentiment, weighted_signal):
    if not DYNAMIC_PROFIT_TARGETS:
        return TARGET_PROFIT_PCT

    price_regime = sentiment.get("price_regime", {})
    kraken_flow = sentiment.get("kraken_flow", {})
    target = BASE_TARGET_PROFIT_PCT

    range_position = numeric_or_none(price_regime.get("range_position_24h"))
    if range_position is not None:
        if range_position <= 0.10:
            target += 0.002
        elif range_position <= 0.20:
            target += 0.001
        elif range_position >= 0.70:
            target -= 0.002

    opportunity = numeric_or_none(sentiment.get("mean_reversion_opportunity"))
    if opportunity is not None:
        if opportunity >= 0.55:
            target += 0.002
        elif opportunity >= MEAN_REVERSION_BUY_THRESHOLD:
            target += 0.001

    flow_pressure = numeric_or_none(sentiment.get("flow_pressure"))
    if flow_pressure is None:
        flow_pressure = numeric_or_none(kraken_flow.get("aggression_score"))
    if flow_pressure is not None:
        if flow_pressure >= 0.40:
            target += 0.0015
        elif flow_pressure <= 0:
            target -= 0.0015

    if weighted_signal >= SENTIMENT_BUY_THRESHOLD + 0.04:
        target += 0.002
    elif weighted_signal < 0:
        target -= 0.001

    volatility = numeric_or_none(
        price_regime.get("realized_volatility_24h_pct")
    )
    if volatility is not None:
        if volatility >= DYNAMIC_PROFIT_HIGH_VOLATILITY_PCT:
            target += 0.0015
        elif volatility <= DYNAMIC_PROFIT_LOW_VOLATILITY_PCT:
            target -= 0.001

    return clamp(target, MIN_TARGET_PROFIT_PCT, MAX_TARGET_PROFIT_PCT)


def target_limit_orders(
    sentiment,
    current_price,
    total_trade_value,
    max_buy_price=None
):
    if not ENABLE_TARGET_LIMIT_BUYS:
        return []

    targets = []
    for target in sentiment.get("target_prices", []):
        if not isinstance(target, dict):
            continue

        buy_price = numeric_or_none(target.get("buy_price"))
        if buy_price is None or buy_price <= 0:
            continue

        max_price = current_price * (1 + TARGET_LIMIT_MAX_PREMIUM_PCT)
        if buy_price > max_price:
            continue
        if max_buy_price is not None and buy_price >= max_buy_price:
            continue

        allocation = numeric_or_none(target.get("sell_pct"))
        if allocation is None:
            allocation = numeric_or_none(target.get("allocation_pct"))
        if allocation is None or allocation <= 0:
            allocation = 1.0

        targets.append(
            {
                "buy_price": buy_price,
                "allocation": allocation
            }
        )

    if not targets:
        return []

    targets = targets[:MAX_TARGET_LIMIT_ORDERS_PER_CYCLE]
    allocation_sum = sum(target["allocation"] for target in targets)
    if allocation_sum <= 0:
        return []

    return [
        {
            "buy_price": target["buy_price"],
            "trade_value": total_trade_value * target["allocation"] / allocation_sum,
            "allocation_pct": target["allocation"] / allocation_sum
        }
        for target in targets
    ]


def current_inventory_usd(current_price):
    open_buy_notional = sum(
        (order.get("price") or current_price) * (order.get("volume") or 0)
        for order in state["open_buy_orders"].values()
    )
    open_sell_notional = sum(
        (order.get("buy_price") or current_price) * (order.get("volume") or 0)
        for order in state["open_sell_orders"].values()
    )
    return open_buy_notional + open_sell_notional


def place_profit_sell_for_buy(
    cycle_id,
    buy_txid,
    buy_price,
    volume,
    target_profit_pct=None
):
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )
    target_price = sell_target_price(buy_price, profit_pct)
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="sell",
        reason="profit_target_after_buy_fill",
        buy_txid=buy_txid,
        buy_price=buy_price,
        price=round_price(target_price),
        volume=round_volume(volume),
        target_profit_pct=profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT
    )
    append_decision_csv(
        "trade_decision",
        cycle_id=cycle_id,
        side="sell",
        reason="profit_target_after_buy_fill",
        price=round_price(target_price),
        volume=round_volume(volume),
        dry_run=DRY_RUN,
        order_txid=buy_txid,
        target_profit_pct=profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=profit_pct + ROUND_TRIP_FEE_PCT
    )
    return place_limit_sell(
        target_price,
        volume,
        cycle_id,
        buy_txid=buy_txid,
        buy_price=buy_price,
        target_profit_pct=profit_pct
    )


def process_open_buy_orders(cycle_id):
    statuses = query_orders(state["open_buy_orders"].keys())

    for txid, order in list(state["open_buy_orders"].items()):
        status = statuses.get(txid)
        if status is None:
            continue

        order_status = status.get("status")
        if order_status == "closed":
            fill = fill_values(
                status,
                fallback_volume=order.get("volume"),
                fallback_price=order.get("price")
            )
            state["stats"]["buy_orders_filled"] += 1
            del state["open_buy_orders"][txid]
            save_state(state)
            log_and_console(
                "BUY_ORDER_FILLED",
                message=f"buy filled @ {round_price(fill['price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=fill["volume"],
                price=fill["price"],
                cost=fill["cost"],
                fee=fill["fee"]
            )
            log_trade_activity(
                "BUY_ORDER_FILLED",
                cycle_id=cycle_id,
                side="buy",
                txid=txid,
                trade_id=order.get("trade_id") or txid,
                volume=fill["volume"],
                price=fill["price"],
                cost=fill["cost"],
                fee=fill["fee"],
                target_profit_pct=order.get("target_profit_pct"),
                round_trip_fee_pct=order.get("round_trip_fee_pct")
            )
            place_profit_sell_for_buy(
                cycle_id,
                txid,
                fill["price"],
                fill["volume"],
                order.get("target_profit_pct")
            )
        elif order_status in ("canceled", "expired"):
            del state["open_buy_orders"][txid]
            save_state(state)
            log_and_console(
                "ORDER_" + order_status.upper(),
                message=f"buy order {order_status}",
                cycle_id=cycle_id,
                txid=txid,
                side="buy"
            )
            log_trade_activity(
                "BUY_ORDER_" + order_status.upper(),
                cycle_id=cycle_id,
                side="buy",
                txid=txid,
                trade_id=order.get("trade_id") or txid,
                price=order.get("price"),
                volume=order.get("volume"),
                order_status=order_status
            )
            notify_order_tracker(
                trade_id=order.get("trade_id") or txid,
                side="buy",
                price=order.get("price"),
                quantity=order.get("volume"),
                order_id=txid,
                timestamp=cycle_id,
                notes=f"order_status={order_status}",
                status=order_status
            )


def process_open_sell_orders(cycle_id):
    statuses = query_orders(state["open_sell_orders"].keys())

    for txid, order in list(state["open_sell_orders"].items()):
        status = statuses.get(txid)
        if status is None:
            continue

        order_status = status.get("status")
        if order_status == "closed":
            fill = fill_values(
                status,
                fallback_volume=order.get("volume"),
                fallback_price=order.get("sell_price")
            )
            state["stats"]["sell_orders_filled"] += 1
            state["last_sell_price"] = fill["price"] or order.get("sell_price")
            state["last_sell_at"] = cycle_id
            del state["open_sell_orders"][txid]
            save_state(state)
            log_and_console(
                "SELL_ORDER_FILLED",
                message=f"sell filled @ {round_price(state['last_sell_price'])}",
                cycle_id=cycle_id,
                txid=txid,
                volume=fill["volume"],
                price=state["last_sell_price"],
                cost=fill["cost"],
                fee=fill["fee"],
                buy_price=order.get("buy_price")
            )
            log_trade_activity(
                "SELL_ORDER_FILLED",
                cycle_id=cycle_id,
                side="sell",
                txid=txid,
                trade_id=order.get("trade_id") or order.get("buy_txid") or txid,
                volume=fill["volume"],
                price=state["last_sell_price"],
                cost=fill["cost"],
                fee=fill["fee"],
                buy_price=order.get("buy_price"),
                buy_txid=order.get("buy_txid"),
                target_profit_pct=order.get("target_profit_pct"),
                round_trip_fee_pct=order.get("round_trip_fee_pct")
            )
        elif order_status in ("canceled", "expired"):
            del state["open_sell_orders"][txid]
            save_state(state)
            log_and_console(
                "ORDER_" + order_status.upper(),
                message=f"sell order {order_status}",
                cycle_id=cycle_id,
                txid=txid,
                side="sell"
            )
            log_trade_activity(
                "SELL_ORDER_" + order_status.upper(),
                cycle_id=cycle_id,
                side="sell",
                txid=txid,
                trade_id=order.get("trade_id") or order.get("buy_txid") or txid,
                price=order.get("sell_price"),
                volume=order.get("volume"),
                buy_price=order.get("buy_price"),
                buy_txid=order.get("buy_txid"),
                order_status=order_status
            )
            notify_order_tracker(
                trade_id=order.get("trade_id") or order.get("buy_txid") or txid,
                side="sell",
                price=order.get("sell_price"),
                quantity=order.get("volume"),
                order_id=txid,
                timestamp=cycle_id,
                notes=f"order_status={order_status}",
                status=order_status
            )


def maybe_handle_submitted_buy(
    result,
    cycle_id,
    volume,
    price,
    target_profit_pct=None
):
    profit_pct = (
        TARGET_PROFIT_PCT
        if target_profit_pct is None
        else target_profit_pct
    )

    if DRY_RUN:
        state["stats"]["buy_orders_filled"] += 1
        save_state(state)
        log_trade_activity(
            "BUY_ORDER_FILLED",
            cycle_id=cycle_id,
            side="buy",
            txid="dry_run_buy",
            trade_id="dry_run_buy",
            volume=volume,
            price=price,
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT
        )
        place_profit_sell_for_buy(
            cycle_id,
            "dry_run_buy",
            price,
            volume,
            profit_pct
        )
        return

    if not result:
        return

    result_payload = result.get("result", {})
    txids = result_payload.get("txid", [])
    txid = txids[0] if txids else None
    fill = result.get("fill", {})

    fill_volume = float(fill.get("fill_volume") or 0)
    fill_price = float(fill.get("fill_price") or 0)
    order_status = fill.get("order_status")

    if txid:
        notify_order_tracker(
            trade_id=txid,
            side="buy",
            price=tracker_value(fill_price, price),
            quantity=tracker_value(fill_volume, volume),
            order_id=txid,
            fee=fill.get("fill_fee"),
            timestamp=cycle_id,
            notes=(
                None
                if order_status == "closed"
                else f"order_status={order_status or 'submitted'}"
            )
        )

    if txid and order_status == "closed" and fill_volume > 0 and fill_price > 0:
        state["stats"]["buy_orders_filled"] += 1
        save_state(state)
        log_trade_activity(
            "BUY_ORDER_FILLED",
            cycle_id=cycle_id,
            side="buy",
            txid=txid,
            trade_id=txid,
            volume=fill_volume,
            price=fill_price,
            fee=fill.get("fill_fee"),
            cost=fill.get("fill_cost"),
            target_profit_pct=profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT
        )
        place_profit_sell_for_buy(
            cycle_id,
            txid,
            fill_price,
            fill_volume,
            profit_pct
        )
        return

    if txid:
        state["open_buy_orders"][txid] = {
            "txid": txid,
            "volume": volume,
            "price": price,
            "target_profit_pct": profit_pct,
            "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
            "placed_at": cycle_id,
            "trade_id": txid
        }
        save_state(state)


def run_cycle():
    now = datetime.now(timezone.utc)
    cycle_id = now.isoformat()
    state["last_cycle"] = cycle_id
    state["stats"]["cycles"] += 1
    save_state(state)

    price = get_price()
    if price is None:
        skip_cycle("missing_price", cycle_id)
        return

    process_open_buy_orders(cycle_id)
    process_open_sell_orders(cycle_id)

    signal = load_signal()
    if signal is None:
        skip_cycle("missing_signal", cycle_id, price=price)
        return

    sentiment = normalize_signal(signal)
    raw_signal = sentiment["execution_signal"]
    confidence = sentiment["confidence"]
    smoothed_signal = smooth_signal(raw_signal)
    weighted_signal = (
        smoothed_signal * confidence
        if CONFIDENCE_WEIGHTING
        else smoothed_signal
    )
    age_minutes = signal_age_minutes(sentiment, now)
    risk_multiplier = numeric_or_none(sentiment.get("risk_multiplier"))
    effective_multiplier = effective_risk_multiplier(sentiment)
    price_regime = sentiment.get("price_regime", {})
    range_position = numeric_or_none(price_regime.get("range_position_24h"))
    mean_reversion_opportunity = numeric_or_none(
        sentiment.get("mean_reversion_opportunity")
    )
    flow_pressure = numeric_or_none(sentiment.get("flow_pressure"))
    target_profit_pct = dynamic_target_profit_pct(sentiment, weighted_signal)
    action_policy = sentiment.get("action_policy", {})
    action_policy_reason = action_policy.get("reason") or sentiment.get("reason")
    action_recommendation = sentiment.get("action_recommendation")
    risk_view = derived_risk_context(sentiment, now)
    risk_context_usable = (
        USE_RISK_CONTEXT_POLICY
        and risk_view.get("risk_context_available")
        and not risk_view.get("risk_context_stale")
    )
    if risk_context_usable and RISK_CONTEXT_TARGET_PROFIT_ENABLED:
        target_profit_pct = clamp(
            target_profit_pct
            * risk_view.get("suggested_take_profit_multiplier", 1.0),
            MIN_TARGET_PROFIT_PCT,
            MAX_TARGET_PROFIT_PCT
        )

    log_event(
        "SIGNAL_UPDATE",
        cycle_id=cycle_id,
        signal_asset_id=sentiment.get("asset_id"),
        signal_asset_symbol=sentiment.get("asset_symbol"),
        asset_price=sentiment.get("asset_price"),
        price=price,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        asset_sentiment=sentiment.get("asset_sentiment"),
        liquidity_risk=sentiment.get("liquidity_risk"),
        btc_relative_strength=sentiment.get("btc_relative_strength"),
        eth_relative_strength=sentiment.get("eth_relative_strength"),
        btc_sentiment=sentiment.get("btc_sentiment"),
        raw_btc_sentiment=sentiment.get("raw_btc_sentiment"),
        raw_confidence=sentiment.get("raw_confidence"),
        direction_bias=sentiment.get("direction_bias"),
        raw_direction_bias=sentiment.get("raw_direction_bias"),
        fear_greed_index=sentiment.get("fear_greed_index"),
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        signal_status=sentiment.get("signal_status"),
        signal_freshness_state=signal_freshness_state(sentiment, now),
        freshness_fresh_for_minutes=freshness_minutes(sentiment, "fresh_for_minutes"),
        freshness_warn_after_minutes=freshness_minutes(sentiment, "warn_after_minutes"),
        freshness_stale_after_minutes=freshness_minutes(sentiment, "stale_after_minutes"),
        bot_action_allowed=sentiment.get("bot_action_allowed"),
        action_recommendation=action_recommendation,
        action_policy_reason=action_policy_reason,
        contributor_count=sentiment.get("contributor_count"),
        signal_age_minutes=age_minutes,
        risk_context=sentiment.get("risk_context"),
        risk_context_available=risk_view.get("risk_context_available"),
        risk_context_stale=risk_view.get("risk_context_stale"),
        risk_context_recommended_posture=risk_view.get("risk_context_recommended_posture"),
        risk_context_market_risk_score=risk_view.get("risk_context_market_risk_score"),
        risk_context_buy_aggression_score=risk_view.get("risk_context_buy_aggression_score"),
        risk_context_downside_risk_score=risk_view.get("risk_context_downside_risk_score"),
        risk_context_bottoming_score=risk_view.get("risk_context_bottoming_score"),
        risk_context_rebound_score=risk_view.get("risk_context_rebound_score"),
        risk_context_breakout_score=risk_view.get("risk_context_breakout_score"),
        risk_context_hard_safety_flags=risk_view.get("risk_context_hard_safety_flags"),
        weather_report_available=risk_view.get("weather_report_available"),
        weather_trade_permission=risk_view.get("weather_trade_permission"),
        weather_bot_decision_authority=risk_view.get("weather_bot_decision_authority"),
        weather_condition=risk_view.get("weather_condition"),
        weather_alert_level=risk_view.get("weather_alert_level"),
        weather_emergency_bell=risk_view.get("weather_emergency_bell"),
        weather_opportunity_tags=risk_view.get("weather_opportunity_tags"),
        weather_risk_warnings=risk_view.get("weather_risk_warnings"),
        weather_market_current_price=risk_view.get("weather_market_current_price"),
        weather_market_range_high=risk_view.get("weather_market_range_high"),
        weather_market_range_low=risk_view.get("weather_market_range_low"),
        weather_market_range_position=risk_view.get("weather_market_range_position"),
        weather_market_range_zone=risk_view.get("weather_market_range_zone"),
        weather_market_distance_to_recent_high_pct=(
            risk_view.get("weather_market_distance_to_recent_high_pct")
        ),
        weather_market_distance_from_recent_low_pct=(
            risk_view.get("weather_market_distance_from_recent_low_pct")
        ),
        weather_market_price_return_24h_pct=(
            risk_view.get("weather_market_price_return_24h_pct")
        ),
        weather_market_price_return_4h_pct=(
            risk_view.get("weather_market_price_return_4h_pct")
        ),
        risk_adjusted_buy_score=risk_view.get("risk_adjusted_buy_score"),
        risk_adjusted_market_score=risk_view.get("risk_adjusted_market_score"),
        risk_adjusted_posture=risk_view.get("risk_adjusted_posture"),
        risk_adjusted_reason=risk_view.get("risk_adjusted_reason"),
        suggested_position_size_multiplier=risk_view.get("suggested_position_size_multiplier"),
        suggested_grid_aggression_multiplier=risk_view.get("suggested_grid_aggression_multiplier"),
        suggested_entry_discount_multiplier=risk_view.get("suggested_entry_discount_multiplier"),
        suggested_take_profit_multiplier=risk_view.get("suggested_take_profit_multiplier"),
        risk_context_source_processed_at=risk_view.get("risk_context_source_processed_at"),
        risk_context_age_minutes=risk_view.get("risk_context_age_minutes"),
        mean_reversion_opportunity=mean_reversion_opportunity,
        range_position_24h=range_position,
        flow_pressure=flow_pressure,
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        processed_at=sentiment.get("processed_at")
    )
    console(f"Price: {price} | Signal: {raw_signal} | Confidence: {confidence}")

    gate_failure = signal_gate_failure(sentiment, now)
    if gate_failure is not None:
        reason = gate_failure.pop("reason")
        skip_cycle(
            reason,
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            **gate_failure
        )
        return

    if risk_context_usable:
        should_pause, risk_block_reason = risk_context_trade_pause(risk_view)
        if should_pause:
            skip_cycle(
                risk_block_reason,
                cycle_id,
                price=price,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                action_recommendation=action_recommendation,
                action_policy_reason=action_policy_reason,
                contributor_count=sentiment.get("contributor_count"),
                bot_action_allowed=sentiment.get("bot_action_allowed"),
                **risk_view
            )
            return

    backtest_failure = backtest_health_failure()
    if backtest_failure is not None:
        reason = backtest_failure.pop("reason")
        skip_cycle(
            reason,
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            action_recommendation=action_recommendation,
            action_policy_reason=action_policy_reason,
            contributor_count=sentiment.get("contributor_count"),
            **risk_view,
            **backtest_failure
        )
        return

    target_limit_candidates = target_limit_orders(sentiment, price, 1)
    allow_target_limit_buy = (
        mean_reversion_setup_allowed(sentiment)
        and bool(target_limit_candidates)
    )
    allow_market_buy = not allow_target_limit_buy

    if allow_market_buy and weighted_signal < SENTIMENT_BUY_THRESHOLD:
        skip_cycle(
            "local_signal_below_buy_threshold",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD,
            action_recommendation=action_recommendation,
            action_policy_reason=action_policy_reason,
            contributor_count=sentiment.get("contributor_count"),
            bot_action_allowed=sentiment.get("bot_action_allowed"),
            **risk_view
        )
        return

    high_entry = high_entry_quality_check(risk_view, range_position)
    if allow_market_buy and high_entry["near_high"] and not high_entry["allowed"]:
        skip_cycle(
            high_entry["reason"],
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            action_recommendation=action_recommendation,
            action_policy_reason=action_policy_reason,
            contributor_count=sentiment.get("contributor_count"),
            bot_action_allowed=sentiment.get("bot_action_allowed"),
            high_entry_near_high=True,
            high_entry_quality_reason=high_entry["reason"],
            high_entry_size_multiplier=high_entry["size_multiplier"],
            **risk_view
        )
        return

    price_high = numeric_or_none(price_regime.get("price_high_24h"))
    if price_high is None:
        price_high = risk_view.get("weather_market_range_high")
    if allow_market_buy and price_high is not None and HIGH_PRICE_BUY_BLOCK_PCT >= 0:
        max_high_buy_price = price_high * (1 - HIGH_PRICE_BUY_BLOCK_PCT)
        if price >= max_high_buy_price:
            if not (high_entry["near_high"] and high_entry["allowed"]):
                skip_cycle(
                    "price_near_regime_high",
                    cycle_id,
                    price=price,
                    execution_signal=raw_signal,
                    smoothed_signal=smoothed_signal,
                    weighted_signal=weighted_signal,
                    confidence=confidence,
                    price_high=price_high,
                    high_price_buy_block_pct=HIGH_PRICE_BUY_BLOCK_PCT,
                    max_high_buy_price=max_high_buy_price,
                    high_entry_near_high=high_entry["near_high"],
                    high_entry_quality_reason=high_entry["reason"],
                    **risk_view
                )
                return

    cooldown = cooldown_active(now, weighted_signal)
    if cooldown is not None:
        skip_cycle(
            "cooldown",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            **cooldown
        )
        return

    entry_pacing = entry_pacing_block(now, price)
    if entry_pacing is not None:
        skip_cycle(
            entry_pacing.pop("reason"),
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            **entry_pacing
        )
        return

    last_sell_price = state.get("last_sell_price")
    max_rebuy_price = None
    if PREVENT_BUY_ABOVE_LAST_SELL and last_sell_price is not None:
        max_rebuy_price = float(last_sell_price) * (
            1 - BUY_AFTER_SELL_DISCOUNT_PCT
        )
        if price >= max_rebuy_price:
            if not (
                allow_target_limit_buy
                and target_limit_orders(sentiment, price, 1, max_rebuy_price)
            ):
                skip_cycle(
                    "price_above_last_sell",
                    cycle_id,
                    price=price,
                    execution_signal=raw_signal,
                    smoothed_signal=smoothed_signal,
                    weighted_signal=weighted_signal,
                    confidence=confidence,
                    last_sell_price=last_sell_price,
                    max_rebuy_price=max_rebuy_price
                )
                return

    if len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS:
        skip_cycle(
            "max_open_buy_orders",
            cycle_id,
            price=price,
            open_buy_count=len(state["open_buy_orders"]),
            max_open_buy_orders=MAX_OPEN_BUY_ORDERS
        )
        return

    if len(state["open_sell_orders"]) >= MAX_OPEN_SELL_ORDERS:
        skip_cycle(
            "max_open_sell_orders",
            cycle_id,
            price=price,
            open_sell_count=len(state["open_sell_orders"])
        )
        return

    if current_inventory_usd(price) >= MAX_INVENTORY_USD:
        skip_cycle(
            "max_inventory_usd",
            cycle_id,
            price=price,
            deployed_inventory_usd=current_inventory_usd(price),
            max_inventory_usd=MAX_INVENTORY_USD
        )
        return

    balances = get_balances()
    if balances is None:
        skip_cycle(
            "balance_fetch_failed",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            confidence=confidence
        )
        return

    base_balance, quote_balance = balances
    trade_value = quote_balance * POSITION_SIZE_PCT
    if MAX_TRADE_USD > 0:
        trade_value = min(trade_value, MAX_TRADE_USD)
    trade_value *= effective_multiplier
    if risk_context_usable and RISK_CONTEXT_POSITION_SIZE_ENABLED:
        trade_value *= clamp(
            risk_view.get("suggested_position_size_multiplier", 1.0),
            0.0,
            RISK_CONTEXT_MAX_POSITION_SIZE_MULTIPLIER
        )
    if allow_market_buy and high_entry["near_high"] and high_entry["allowed"]:
        trade_value *= high_entry["size_multiplier"]
    trade_value *= max(0, 1 - EXECUTION_BUFFER_PCT)

    projected_inventory = current_inventory_usd(price) + trade_value
    if projected_inventory > MAX_INVENTORY_USD:
        trade_value = max(0, MAX_INVENTORY_USD - current_inventory_usd(price))

    if trade_value < MIN_TRADE_USD:
        skip_cycle(
            "small_trade",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            confidence=confidence,
            trade_value=trade_value,
            min_trade_usd=MIN_TRADE_USD,
            risk_multiplier=risk_multiplier,
            effective_risk_multiplier=effective_multiplier
        )
        return

    min_volume = get_min_order_volume()
    if allow_target_limit_buy:
        orders = target_limit_orders(
            sentiment,
            price,
            trade_value,
            max_rebuy_price
        )
        placed_orders = 0

        for order in orders:
            if len(state["open_buy_orders"]) >= MAX_OPEN_BUY_ORDERS:
                break

            target_price = order["buy_price"]
            target_trade_value = order["trade_value"]
            volume = round_volume(target_trade_value / target_price)
            if volume <= 0 or volume < min_volume:
                log_event(
                    "TRADE_DECISION",
                    cycle_id=cycle_id,
                    side="hold",
                    reason="target_below_min_volume",
                    price=price,
                    target_buy_price=target_price,
                    trade_value=target_trade_value,
                    volume=volume,
                    min_volume=min_volume
                )
                continue

            log_event(
                "TRADE_DECISION",
                cycle_id=cycle_id,
                side="buy",
                reason="mean_reversion_target_limit_buy",
                volume=volume,
                price=target_price,
                target_buy_price=target_price,
                target_allocation_pct=order["allocation_pct"],
                trade_value=target_trade_value,
                base_balance=base_balance,
                quote_balance=quote_balance,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                action_recommendation=action_recommendation,
                action_policy_reason=action_policy_reason,
                contributor_count=sentiment.get("contributor_count"),
                target_profit_pct=target_profit_pct,
                round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
                gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
                risk_multiplier=risk_multiplier,
                effective_risk_multiplier=effective_multiplier,
                mean_reversion_opportunity=mean_reversion_opportunity,
                range_position_24h=range_position,
                flow_pressure=flow_pressure,
                **risk_view
            )
            append_decision_csv(
                "trade_decision",
                cycle_id=cycle_id,
                side="buy",
                reason="mean_reversion_target_limit_buy",
                volume=volume,
                price=target_price,
                target_buy_price=target_price,
                target_allocation_pct=order["allocation_pct"],
                trade_value=target_trade_value,
                base_balance=base_balance,
                quote_balance=quote_balance,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                action_recommendation=action_recommendation,
                action_policy_reason=action_policy_reason,
                contributor_count=sentiment.get("contributor_count"),
                dry_run=DRY_RUN,
                target_profit_pct=target_profit_pct,
                round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
                gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
                risk_multiplier=risk_multiplier,
                effective_risk_multiplier=effective_multiplier,
                mean_reversion_opportunity=mean_reversion_opportunity,
                range_position_24h=range_position,
                flow_pressure=flow_pressure,
                **risk_view
            )

            log_shadow_buy_plan(
                cycle_id,
                reason="mean_reversion_target_limit_buy",
                ordertype="limit",
                fill_mode="limit",
                price=target_price,
                volume=volume,
                trade_value=target_trade_value,
                target_profit_pct=target_profit_pct,
                target_buy_price=target_price,
                target_allocation_pct=order["allocation_pct"],
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                action_recommendation=action_recommendation,
                action_policy_reason=action_policy_reason,
                risk_multiplier=risk_multiplier,
                effective_risk_multiplier=effective_multiplier,
                mean_reversion_opportunity=mean_reversion_opportunity,
                range_position_24h=range_position,
                flow_pressure=flow_pressure,
                **risk_view
            )

            result = place_limit_buy(
                target_price,
                volume,
                cycle_id,
                target_profit_pct
            )
            result_payload = (
                result.get("result") if isinstance(result, dict) else None
            )
            txids = (
                result_payload.get("txid", [])
                if isinstance(result_payload, dict)
                else []
            )
            append_decision_csv(
                "trade_executed" if result else "trade_rejected",
                cycle_id=cycle_id,
                side="buy",
                reason=(
                    "dry_run"
                    if DRY_RUN
                    else ("limit_submitted" if result else "rejected")
                ),
                volume=volume,
                price=target_price,
                target_buy_price=target_price,
                target_allocation_pct=order["allocation_pct"],
                trade_value=target_trade_value,
                base_balance=base_balance,
                quote_balance=quote_balance,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence,
                action_recommendation=action_recommendation,
                action_policy_reason=action_policy_reason,
                contributor_count=sentiment.get("contributor_count"),
                dry_run=DRY_RUN,
                order_txid=txids[0] if txids else "",
                target_profit_pct=target_profit_pct,
                round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
                gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
                result=result_payload or result
            )

            if result:
                placed_orders += 1

        if placed_orders <= 0:
            skip_cycle(
                "no_target_limit_orders_placed",
                cycle_id,
                price=price,
                execution_signal=raw_signal,
                smoothed_signal=smoothed_signal,
                weighted_signal=weighted_signal,
                confidence=confidence
            )
            return

        log_event(
            "CYCLE_SUMMARY",
            cycle_id=cycle_id,
            price=price,
            execution_signal=raw_signal,
            smoothed_signal=smoothed_signal,
            weighted_signal=weighted_signal,
            confidence=confidence,
            action_recommendation=action_recommendation,
            side="buy",
            reason="mean_reversion_target_limit_buy",
            trade_value=trade_value,
            base_balance=base_balance,
            quote_balance=quote_balance,
            open_buy_count=len(state["open_buy_orders"]),
            open_sell_count=len(state["open_sell_orders"]),
            deployed_inventory_usd=current_inventory_usd(price),
            last_sell_price=state.get("last_sell_price"),
            target_profit_pct=target_profit_pct,
            round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
            gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
            dry_run=DRY_RUN
        )
        return

    volume = round_volume(trade_value / price)
    if volume <= 0 or volume < min_volume:
        skip_cycle(
            "below_min_volume",
            cycle_id,
            price=price,
            trade_value=trade_value,
            volume=volume,
            min_volume=min_volume
        )
        return

    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="buy",
        reason="sentiment_buy",
        volume=volume,
        price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        action_recommendation=action_recommendation,
        action_policy_reason=action_policy_reason,
        contributor_count=sentiment.get("contributor_count"),
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        high_entry_near_high=high_entry["near_high"],
        high_entry_quality_reason=high_entry["reason"],
        high_entry_size_multiplier=high_entry["size_multiplier"],
        **risk_view
    )
    append_decision_csv(
        "trade_decision",
        cycle_id=cycle_id,
        side="buy",
        reason="sentiment_buy",
        volume=volume,
        price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        action_recommendation=action_recommendation,
        action_policy_reason=action_policy_reason,
        contributor_count=sentiment.get("contributor_count"),
        dry_run=DRY_RUN,
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        high_entry_near_high=high_entry["near_high"],
        high_entry_quality_reason=high_entry["reason"],
        high_entry_size_multiplier=high_entry["size_multiplier"],
        **risk_view
    )

    log_shadow_buy_plan(
        cycle_id,
        reason="sentiment_buy",
        ordertype="market",
        fill_mode="market",
        price=price,
        volume=volume,
        trade_value=trade_value,
        target_profit_pct=target_profit_pct,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        action_recommendation=action_recommendation,
        action_policy_reason=action_policy_reason,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        high_entry_near_high=high_entry["near_high"],
        high_entry_quality_reason=high_entry["reason"],
        high_entry_size_multiplier=high_entry["size_multiplier"],
        **risk_view
    )

    result = place_market_buy(volume, cycle_id)
    result_payload = result.get("result") if isinstance(result, dict) else None
    fill = result.get("fill", {}) if isinstance(result, dict) else {}
    txids = result_payload.get("txid", []) if isinstance(result_payload, dict) else []
    append_decision_csv(
        "trade_executed" if result else "trade_rejected",
        cycle_id=cycle_id,
        side="buy",
        reason="dry_run" if DRY_RUN else ("submitted" if result else "rejected"),
        volume=volume,
        price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        action_recommendation=action_recommendation,
        action_policy_reason=action_policy_reason,
        contributor_count=sentiment.get("contributor_count"),
        dry_run=DRY_RUN,
        order_txid=txids[0] if txids else "",
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        high_entry_near_high=high_entry["near_high"],
        high_entry_quality_reason=high_entry["reason"],
        high_entry_size_multiplier=high_entry["size_multiplier"],
        **risk_view,
        **fill,
        result=result_payload or result
    )
    maybe_handle_submitted_buy(
        result,
        cycle_id,
        volume,
        price,
        target_profit_pct
    )

    log_event(
        "CYCLE_SUMMARY",
        cycle_id=cycle_id,
        price=price,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        action_recommendation=action_recommendation,
        side="buy",
        volume=volume,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        open_buy_count=len(state["open_buy_orders"]),
        open_sell_count=len(state["open_sell_orders"]),
        deployed_inventory_usd=current_inventory_usd(price),
        last_sell_price=state.get("last_sell_price"),
        target_profit_pct=target_profit_pct,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        gross_target_pct=target_profit_pct + ROUND_TRIP_FEE_PCT,
        risk_multiplier=risk_multiplier,
        effective_risk_multiplier=effective_multiplier,
        high_entry_near_high=high_entry["near_high"],
        high_entry_quality_reason=high_entry["reason"],
        high_entry_size_multiplier=high_entry["size_multiplier"],
        dry_run=DRY_RUN
    )


# ----------------------
# MAIN LOOP
# ----------------------


def main():
    if RUN_BACKTEST:
        return run_backtest_report()

    if CLI_ARGS.buynow:
        return run_buynow()

    require_runtime_config()

    log_and_console(
        "BOT_START",
        message="Sentiment executor starting",
        config_file=CONFIG_FILE,
        strategy_profile=STRATEGY_PROFILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        trade_activity_file=TRADE_ACTIVITY_FILE,
        shadow_trades_enabled=SHADOW_TRADES_ENABLED,
        pair=KRAKEN_PAIR,
        signal_asset_id=SELECTED_SIGNAL_ASSET_ID,
        signal_url=LLM_SIGNAL_URL,
        kraken_api_url=KRAKEN_API_URL,
        kraken_key_fingerprint=key_fingerprint(KRAKEN_API_KEY),
        request_timeout_seconds=REQUEST_TIMEOUT,
        request_retry_attempts=REQUEST_RETRY_ATTEMPTS,
        request_retry_backoff_seconds=REQUEST_RETRY_BACKOFF_SECONDS,
        order_tracker_timeout_seconds=ORDER_TRACKER_TIMEOUT,
        order_tracker_checkin_timeout_seconds=ORDER_TRACKER_CHECKIN_TIMEOUT,
        dry_run=DRY_RUN,
        min_trade_usd=MIN_TRADE_USD,
        confidence_threshold=CONF_THRESHOLD,
        confidence_weighting=CONFIDENCE_WEIGHTING,
        execution_buffer_pct=EXECUTION_BUFFER_PCT,
        rebalance_cooldown_minutes=REBALANCE_COOLDOWN_MINUTES,
        cooldown_override_signal_abs=COOLDOWN_OVERRIDE_SIGNAL_ABS,
        entry_pacing_enabled=ENTRY_PACING_ENABLED,
        entry_pacing_window_minutes=ENTRY_PACING_WINDOW_MINUTES,
        entry_pacing_max_buys=ENTRY_PACING_MAX_BUYS,
        entry_pacing_min_price_move_pct=ENTRY_PACING_MIN_PRICE_MOVE_PCT,
        sentiment_buy_threshold=SENTIMENT_BUY_THRESHOLD,
        position_size_pct=POSITION_SIZE_PCT,
        max_trade_usd=MAX_TRADE_USD,
        target_profit_pct=TARGET_PROFIT_PCT,
        round_trip_fee_pct=ROUND_TRIP_FEE_PCT,
        dynamic_profit_targets=DYNAMIC_PROFIT_TARGETS,
        min_target_profit_pct=MIN_TARGET_PROFIT_PCT,
        base_target_profit_pct=BASE_TARGET_PROFIT_PCT,
        max_target_profit_pct=MAX_TARGET_PROFIT_PCT,
        dynamic_profit_low_volatility_pct=DYNAMIC_PROFIT_LOW_VOLATILITY_PCT,
        dynamic_profit_high_volatility_pct=DYNAMIC_PROFIT_HIGH_VOLATILITY_PCT,
        max_open_sell_orders=MAX_OPEN_SELL_ORDERS,
        max_open_buy_orders=MAX_OPEN_BUY_ORDERS,
        max_inventory_usd=MAX_INVENTORY_USD,
        prevent_buy_above_last_sell=PREVENT_BUY_ABOVE_LAST_SELL,
        buy_after_sell_discount_pct=BUY_AFTER_SELL_DISCOUNT_PCT,
        high_price_buy_block_pct=HIGH_PRICE_BUY_BLOCK_PCT,
        use_signal_status_gates=USE_SIGNAL_STATUS_GATES,
        require_bot_action_allowed=REQUIRE_BOT_ACTION_ALLOWED,
        max_signal_age_minutes=MAX_SIGNAL_AGE_MINUTES,
        use_risk_multiplier=USE_RISK_MULTIPLIER,
        min_risk_multiplier=MIN_RISK_MULTIPLIER,
        max_risk_multiplier=MAX_RISK_MULTIPLIER,
        enable_target_limit_buys=ENABLE_TARGET_LIMIT_BUYS,
        max_target_limit_orders_per_cycle=MAX_TARGET_LIMIT_ORDERS_PER_CYCLE,
        mean_reversion_buy_threshold=MEAN_REVERSION_BUY_THRESHOLD,
        mean_reversion_range_position_max=MEAN_REVERSION_RANGE_POSITION_MAX,
        mean_reversion_flow_pressure_min=MEAN_REVERSION_FLOW_PRESSURE_MIN,
        use_risk_context_policy=USE_RISK_CONTEXT_POLICY,
        risk_context_hard_safety_block=RISK_CONTEXT_HARD_SAFETY_BLOCK,
        risk_context_min_buy_score=RISK_CONTEXT_MIN_BUY_SCORE,
        risk_context_position_size_enabled=RISK_CONTEXT_POSITION_SIZE_ENABLED,
        risk_context_target_profit_enabled=RISK_CONTEXT_TARGET_PROFIT_ENABLED,
        risk_context_max_position_size_multiplier=(
            RISK_CONTEXT_MAX_POSITION_SIZE_MULTIPLIER
        ),
        use_backtest_health_gate=USE_BACKTEST_HEALTH_GATE,
        backtest_fail_closed=BACKTEST_FAIL_CLOSED,
        backtest_min_trades=BACKTEST_MIN_TRADES,
        backtest_require_policy_beats_baseline=(
            BACKTEST_REQUIRE_POLICY_BEATS_BASELINE
        ),
        bot_policy_backtest_url=BOT_POLICY_BACKTEST_URL,
        bot_replay_backtest_url=BOT_REPLAY_BACKTEST_URL,
        decision_csv_file=DECISION_CSV_FILE,
        price_check_interval_seconds=PRICE_CHECK_INTERVAL_SECONDS
    )
    console(
        "Backtest gate: "
        f"{'enabled' if USE_BACKTEST_HEALTH_GATE else 'disabled'} | "
        f"fail_closed={BACKTEST_FAIL_CLOSED} | "
        f"min_trades={BACKTEST_MIN_TRADES} | "
        f"url={BOT_REPLAY_BACKTEST_URL or BOT_POLICY_BACKTEST_URL or 'unset'}"
    )

    while True:
        try:
            run_cycle()
            send_checkin(
                loop_count=state.get("stats", {}).get("cycles"),
                message="loop_complete"
            )
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            log_and_console("BOT_STOP", message="Sentiment executor stopped")
            break
        except Exception as e:
            state["stats"]["errors"] += 1
            save_state(state)
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            send_checkin(
                status="error",
                loop_count=state.get("stats", {}).get("cycles"),
                message=short_error_summary(e)
            )
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
