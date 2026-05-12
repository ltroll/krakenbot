#!/usr/bin/env python3

# =====================================================
# RANGE GRID SENTIMENT BOT (RESTORED STABLE VERSION)
# =====================================================

import json
import os
import statistics
import time
from datetime import datetime, timedelta, timezone

import krakenex
import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

CONFIG_FILE = os.getenv("BOT_CONFIG_FILE", "range_grid_config.json")
STRATEGY_PROFILE = os.getenv("STRATEGY_PROFILE", "default")
STATE_FILE = os.getenv("BOT_STATE_FILE", "last_state.json")
LOG_FILE = os.getenv("TRADE_LOG_FILE", "trade_log.jsonl")

KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")
PRICE_LOG_URL = os.getenv("PRICE_LOG_URL")


def parse_strategy_modes(raw_value):
    if not raw_value:
        return ["low"]

    modes = [
        mode.strip().lower()
        for mode in raw_value.split(",")
        if mode.strip()
    ]
    return modes or ["low"]

def load_config():
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


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


def profile_int(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else int(value)


def profile_float(name, default):
    value = strategy_config.get(name, default)
    return default if value is None else float(value)


config = load_config()
strategy_config = select_strategy_profile(config)

range_window_hours = profile_int("range_window_hours", 24)
max_grid_size = profile_int("max_grid_size", 4)
profit_target_pct = profile_float("profit_target_pct", 0.01)
entry_step_pct = profile_float("entry_step_pct", profit_target_pct / 2)
round_trip_fee_pct = profile_float("round_trip_fee_pct", 0.0032)
position_size_pct = profile_float("position_size_pct", 0.10)
execution_signal_threshold = profile_float("execution_signal_threshold", 0.0)
llm_target_proximity_pct = profile_float(
    "llm_target_proximity_pct",
    entry_step_pct
)
price_check_interval_seconds = profile_int("price_check_interval_seconds", 120)
range_refresh_interval_minutes = profile_int("range_refresh_interval_minutes", 60)
max_open_sell_orders = profile_int("max_open_sell_orders", 999999)
max_inventory_usd = profile_float("max_inventory_usd", 1e18)
aging_start_minutes = profile_int("aging_start_minutes", 999999)
aging_step_minutes = profile_int("aging_step_minutes", 60)
aging_profit_reduction_pct = profile_float("aging_profit_reduction_pct", 0.0)
min_profit_target_pct = profile_float(
    "min_profit_target_pct",
    profit_target_pct
)
high_anchor_buy_cooldown_minutes = profile_int(
    "high_anchor_buy_cooldown_minutes",
    15
)
max_open_high_anchor_orders = profile_int("max_open_high_anchor_orders", 3)
high_anchor_profit_target_pct = profile_float(
    "high_anchor_profit_target_pct",
    profit_target_pct
)
sentiment_defensive_threshold = profile_float(
    "sentiment_defensive_threshold",
    max(0.03, execution_signal_threshold)
)
sentiment_risk_on_threshold = profile_float("sentiment_risk_on_threshold", 0.12)
sentiment_defensive_size_multiplier = profile_float(
    "sentiment_defensive_size_multiplier",
    0.65
)
sentiment_risk_on_size_multiplier = profile_float(
    "sentiment_risk_on_size_multiplier",
    1.2
)
sentiment_defensive_inventory_multiplier = profile_float(
    "sentiment_defensive_inventory_multiplier",
    0.7
)
sentiment_risk_on_inventory_multiplier = profile_float(
    "sentiment_risk_on_inventory_multiplier",
    1.2
)
sentiment_defensive_open_sell_multiplier = profile_float(
    "sentiment_defensive_open_sell_multiplier",
    0.75
)
sentiment_risk_on_open_sell_multiplier = profile_float(
    "sentiment_risk_on_open_sell_multiplier",
    1.25
)
sentiment_disable_high_anchor_below = profile_float(
    "sentiment_disable_high_anchor_below",
    0.05
)
sentiment_defensive_extra_aging_reduction_pct = profile_float(
    "sentiment_defensive_extra_aging_reduction_pct",
    0.001
)
grid_anchor = strategy_config.get("grid_anchor", "low").strip().lower()
strategy_modes = parse_strategy_modes(grid_anchor)

# ----------------------
# KRAKEN INIT
# ----------------------

api = krakenex.API()
api.uri = KRAKEN_API_URL
api.key = os.getenv("KRAKEN_API_KEY")
api.secret = os.getenv("KRAKEN_API_SECRET")

pair_info = api.query_public("AssetPairs")["result"]["XXBTZUSD"]

PRICE_DECIMALS = pair_info["pair_decimals"]
VOLUME_DECIMALS = pair_info["lot_decimals"]

# ----------------------
# LOGGING
# ----------------------


def log_event(event, **kwargs):
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "event": event,
        "message": kwargs.pop("message", "")
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
        print(
            f"[{datetime.now(timezone.utc).isoformat()}] "
            f"LOG_WRITE_ERROR: {e}"
        )


def console(msg):
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def log_and_console(event, message="", **kwargs):
    log_event(event, message=message, **kwargs)

    if message:
        console(f"{event}: {message}")
    else:
        console(event)


# ----------------------
# SAFE KRAKEN WRAPPER
# ----------------------

def kraken_call(label, fn, *args, **kwargs):
    try:
        resp = fn(*args, **kwargs)
    except Exception as e:
        log_event("KRAKEN_EXCEPTION", operation=label, message=str(e))
        return None

    if not isinstance(resp, dict):
        log_event("KRAKEN_BAD_RESPONSE", operation=label)
        return None

    if resp.get("error"):
        log_event("KRAKEN_API_ERROR", operation=label, error=resp["error"])
        return resp

    return resp


# ----------------------
# STATE
# ----------------------

def load_state():
    default = {
        "open_buy_orders": {},
        "open_sell_orders": {},
        "range_low": None,
        "range_high": None,
        "range_mean": None,
        "range_median": None,
        "last_range_refresh": None,
        "last_high_anchor_buy_at": None,
        "stats": {
            "buy_orders_placed": 0,
            "buy_orders_filled": 0,
            "sell_orders_placed": 0,
            "sell_orders_filled": 0,
            "realized_gross_pnl": 0.0,
            "realized_estimated_net_pnl": 0.0
        }
    }

    if not os.path.exists(STATE_FILE):
        return default

    with open(STATE_FILE, encoding="utf-8") as f:
        state = json.load(f)

    for key in default:
        if key not in state:
            state[key] = default[key]

    return state


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def normalize_state(state):
    normalized_buy_orders = {}
    normalized_sell_orders = {}
    state.setdefault("stats", {})

    for key, default_value in {
        "buy_orders_placed": 0,
        "buy_orders_filled": 0,
        "sell_orders_placed": 0,
        "sell_orders_filled": 0,
        "realized_gross_pnl": 0.0,
        "realized_estimated_net_pnl": 0.0
    }.items():
        state["stats"].setdefault(key, default_value)

    for level, order in state["open_buy_orders"].items():
        if isinstance(order, dict):
            normalized_buy_orders[level] = {
                "txid": order.get("txid"),
                "volume": order.get("volume"),
                "price": order.get("price", float(level)),
                "placed_at": order.get("placed_at"),
                "sell_pct_override": order.get("sell_pct_override"),
                "buy_source": order.get("buy_source")
            }
        else:
            normalized_buy_orders[level] = {
                "txid": None,
                "volume": None,
                "price": float(level),
                "placed_at": None,
                "sell_pct_override": None,
                "buy_source": None
            }

    for txid, order in state["open_sell_orders"].items():
        if isinstance(order, dict) and "level" in order:
            normalized_sell_orders[txid] = {
                "level": order.get("level"),
                "volume": order.get("volume"),
                "buy_price": order.get("buy_price"),
                "sell_price": order.get("sell_price"),
                "placed_at": order.get("placed_at"),
                "sell_pct_override": order.get("sell_pct_override"),
                "buy_source": order.get("buy_source")
            }
        else:
            normalized_sell_orders[txid] = {
                "level": None,
                "volume": order.get("volume"),
                "buy_price": None,
                "sell_price": None,
                "placed_at": None,
                "sell_pct_override": None,
                "buy_source": None
            }

    state["open_buy_orders"] = normalized_buy_orders
    state["open_sell_orders"] = normalized_sell_orders
    return state


def parse_iso8601(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


state = normalize_state(load_state())


# ----------------------
# PRICE
# ----------------------

def get_price():
    try:
        if KRAKEN_TICKER_URL:
            r = requests.get(KRAKEN_TICKER_URL, timeout=5)
            data = r.json()

            if "result" in data:
                pair = list(data["result"].keys())[0]
                return float(data["result"][pair]["c"][0])

        r = api.query_public("Ticker", {"pair": "XXBTZUSD"})
        return float(r["result"]["XXBTZUSD"]["c"][0])
    except Exception as e:
        log_event("PRICE_ERROR", message=str(e))
        return None


# ----------------------
# SENTIMENT
# ----------------------

def get_sentiment():
    try:
        r = requests.get(LLM_SIGNAL_URL, timeout=5)
        data = r.json()

        if isinstance(data, dict):
            target_prices = data.get("target_prices", [])
            if not isinstance(target_prices, list):
                target_prices = []

            return {
                "execution_signal": data.get("execution_signal", 0),
                "target_prices": target_prices,
                "btc_sentiment": data.get("btc_sentiment"),
                "regulatory_risk": data.get("regulatory_risk"),
                "macro_tightening_bias": data.get("macro_tightening_bias"),
                "confidence": data.get("confidence"),
                "direction_bias": data.get("direction_bias"),
                "raw_btc_sentiment": data.get("raw_btc_sentiment"),
                "raw_regulatory_risk": data.get("raw_regulatory_risk"),
                "raw_macro_tightening_bias": data.get("raw_macro_tightening_bias"),
                "raw_confidence": data.get("raw_confidence"),
                "raw_direction_bias": data.get("raw_direction_bias"),
                "btc_price": data.get("btc_price"),
                "fear_greed_index": data.get("fear_greed_index"),
                "processed_at": data.get("processed_at"),
                "price_regime": (
                    data.get("price_regime")
                    if isinstance(data.get("price_regime"), dict)
                    else {}
                )
            }

        return {
            "execution_signal": float(data),
            "target_prices": [],
            "price_regime": {}
        }
    except Exception as e:
        log_event("SENTIMENT_ERROR", message=str(e))
        return None


# ----------------------
# RANGE REFRESH
# ----------------------

def refresh_range():
    try:
        r = requests.get(PRICE_LOG_URL, timeout=10)

        records = []
        for line in r.text.splitlines():
            if not line.strip():
                continue

            try:
                records.append(json.loads(line))
            except Exception:
                continue

        cutoff = datetime.now(timezone.utc) - timedelta(hours=range_window_hours)
        prices = []

        for record in records:
            try:
                ts = datetime.fromisoformat(record["timestamp"])

                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)

                if ts >= cutoff:
                    prices.append(float(record["btc_price_usd"]))
            except Exception:
                continue

        if prices:
            state["range_low"] = min(prices)
            state["range_high"] = max(prices)
            state["range_mean"] = sum(prices) / len(prices)
            state["range_median"] = statistics.median(prices)
            state["last_range_refresh"] = datetime.now(timezone.utc).isoformat()

            save_state(state)

            log_and_console(
                "RANGE_REFRESH",
                message=(
                    f"low={state['range_low']} "
                    f"high={state['range_high']} "
                    f"mean={round(state['range_mean'], 2)} "
                    f"median={round(state['range_median'], 2)}"
                ),
                range_low=state["range_low"],
                range_high=state["range_high"],
                range_mean=state["range_mean"],
                range_median=state["range_median"],
                samples=len(prices)
            )
        else:
            log_event(
                "RANGE_REFRESH_SKIPPED",
                message="No prices available in configured range window"
            )
    except Exception as e:
        log_event("RANGE_REFRESH_ERROR", message=str(e))


# ----------------------
# GRID
# ----------------------

def compute_grid(low, high, mean):
    return sorted(
        [
            mean * (1 - (entry_step_pct * (i + 1)))
            for i in range(max_grid_size)
        ],
        reverse=True
    )


def compute_high_anchor_grid(high, price):
    lower_bound = high * (1 - entry_step_pct)

    if lower_bound <= price <= high:
        return [price]

    return []


def high_anchor_open_order_count():
    open_buy_count = sum(
        1
        for order in state["open_buy_orders"].values()
        if order.get("buy_source") == "range_high_band"
    )
    open_sell_count = sum(
        1
        for order in state["open_sell_orders"].values()
        if order.get("buy_source") == "range_high_band"
    )
    return open_buy_count + open_sell_count


def high_anchor_cooldown_remaining_minutes(now):
    last_buy_at = parse_iso8601(state.get("last_high_anchor_buy_at"))

    if last_buy_at is None:
        return 0

    elapsed_minutes = (now - last_buy_at).total_seconds() / 60
    return max(0, high_anchor_buy_cooldown_minutes - elapsed_minutes)


# ----------------------
# ORDER HELPERS
# ----------------------

def place_buy(price, volume):
    return api.query_private("AddOrder", {
        "pair": "XXBTZUSD",
        "type": "buy",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS))
    })


def place_sell(price, volume):
    return api.query_private("AddOrder", {
        "pair": "XXBTZUSD",
        "type": "sell",
        "ordertype": "limit",
        "price": str(round(price, PRICE_DECIMALS)),
        "volume": str(round(volume, VOLUME_DECIMALS))
    })


def cancel_order(txid):
    return api.query_private("CancelOrder", {"txid": txid})


def order_filled(txid):
    try:
        r = api.query_private("QueryOrders", {"txid": txid})

        if r.get("error"):
            return False

        return r["result"][txid]["status"] == "closed"
    except Exception:
        return False


def get_order_status(txid):
    try:
        r = api.query_private("QueryOrders", {"txid": txid})

        if r.get("error"):
            log_event(
                "ORDER_STATUS_ERROR",
                txid=txid,
                error=r["error"]
            )
            return None

        return r["result"][txid]["status"]
    except Exception as e:
        log_event("ORDER_STATUS_ERROR", txid=txid, message=str(e))
        return None


def compute_sell_target_price(buy_price, profit_target_override=None):
    profit_target = (
        profit_target_pct
        if profit_target_override is None
        else profit_target_override
    )
    return buy_price * (1 + profit_target + round_trip_fee_pct)


def normalize_llm_sell_pct(raw_sell_pct):
    if raw_sell_pct is None:
        return None

    try:
        return float(raw_sell_pct) / 100.0
    except Exception:
        return None


def select_llm_target(target_prices, current_price):
    valid_targets = []

    for target in target_prices:
        if not isinstance(target, dict):
            continue

        buy_price = target.get("buy_price")
        sell_pct = normalize_llm_sell_pct(target.get("sell_pct"))

        if buy_price is None or sell_pct is None:
            continue

        try:
            buy_price = float(buy_price)
        except Exception:
            continue

        distance_pct = abs(current_price - buy_price) / buy_price
        if distance_pct > llm_target_proximity_pct:
            continue

        valid_targets.append(
            {
                "buy_price": buy_price,
                "sell_pct": sell_pct,
                "distance_pct": distance_pct
            }
        )

    if not valid_targets:
        return None

    return min(valid_targets, key=lambda target: target["distance_pct"])


def numeric_or_default(value, default):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def compute_adjusted_profit_target(age_minutes, base_profit_target=None):
    starting_profit_target = (
        profit_target_pct
        if base_profit_target is None
        else base_profit_target
    )

    if (
        age_minutes is None
        or aging_profit_reduction_pct <= 0
        or age_minutes < aging_start_minutes
    ):
        return starting_profit_target

    reduction_steps = (
        int((age_minutes - aging_start_minutes) // max(1, aging_step_minutes))
        + 1
    )
    adjusted_profit = (
        starting_profit_target - reduction_steps * aging_profit_reduction_pct
    )
    return max(min_profit_target_pct, adjusted_profit)


def sentiment_regime(execution_signal):
    if execution_signal < execution_signal_threshold:
        return {
            "name": "paused",
            "position_size_multiplier": 0.0,
            "inventory_multiplier": 0.0,
            "open_sell_multiplier": 0.0,
            "allow_high_anchor": False,
            "extra_aging_reduction_pct": sentiment_defensive_extra_aging_reduction_pct
        }

    if execution_signal < sentiment_defensive_threshold:
        return {
            "name": "defensive",
            "position_size_multiplier": sentiment_defensive_size_multiplier,
            "inventory_multiplier": sentiment_defensive_inventory_multiplier,
            "open_sell_multiplier": sentiment_defensive_open_sell_multiplier,
            "allow_high_anchor": (
                execution_signal >= sentiment_disable_high_anchor_below
            ),
            "extra_aging_reduction_pct": sentiment_defensive_extra_aging_reduction_pct
        }

    if execution_signal >= sentiment_risk_on_threshold:
        return {
            "name": "risk_on",
            "position_size_multiplier": sentiment_risk_on_size_multiplier,
            "inventory_multiplier": sentiment_risk_on_inventory_multiplier,
            "open_sell_multiplier": sentiment_risk_on_open_sell_multiplier,
            "allow_high_anchor": True,
            "extra_aging_reduction_pct": 0.0
        }

    return {
        "name": "neutral",
        "position_size_multiplier": 1.0,
        "inventory_multiplier": 1.0,
        "open_sell_multiplier": 1.0,
        "allow_high_anchor": True,
        "extra_aging_reduction_pct": 0.0
    }


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


# ----------------------
# MAIN LOOP
# ----------------------

def main():
    log_and_console(
        "BOT_START",
        message="Range Grid Average bot starting",
        config_file=CONFIG_FILE,
        strategy_profile=STRATEGY_PROFILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        grid_anchor=grid_anchor,
        strategy_modes=strategy_modes,
        range_window_hours=range_window_hours,
        max_grid_size=max_grid_size,
        profit_target_pct=profit_target_pct,
        entry_step_pct=entry_step_pct,
        round_trip_fee_pct=round_trip_fee_pct,
        position_size_pct=position_size_pct,
        execution_signal_threshold=execution_signal_threshold,
        llm_target_proximity_pct=llm_target_proximity_pct,
        price_check_interval_seconds=price_check_interval_seconds,
        range_refresh_interval_minutes=range_refresh_interval_minutes,
        max_open_sell_orders=max_open_sell_orders,
        max_inventory_usd=max_inventory_usd,
        aging_start_minutes=aging_start_minutes,
        aging_step_minutes=aging_step_minutes,
        aging_profit_reduction_pct=aging_profit_reduction_pct,
        min_profit_target_pct=min_profit_target_pct,
        high_anchor_buy_cooldown_minutes=high_anchor_buy_cooldown_minutes,
        max_open_high_anchor_orders=max_open_high_anchor_orders,
        high_anchor_profit_target_pct=high_anchor_profit_target_pct,
        sentiment_defensive_threshold=sentiment_defensive_threshold,
        sentiment_risk_on_threshold=sentiment_risk_on_threshold,
        sentiment_defensive_size_multiplier=(
            sentiment_defensive_size_multiplier
        ),
        sentiment_risk_on_size_multiplier=sentiment_risk_on_size_multiplier,
        sentiment_defensive_inventory_multiplier=(
            sentiment_defensive_inventory_multiplier
        ),
        sentiment_risk_on_inventory_multiplier=(
            sentiment_risk_on_inventory_multiplier
        ),
        sentiment_defensive_open_sell_multiplier=(
            sentiment_defensive_open_sell_multiplier
        ),
        sentiment_risk_on_open_sell_multiplier=(
            sentiment_risk_on_open_sell_multiplier
        ),
        sentiment_disable_high_anchor_below=(
            sentiment_disable_high_anchor_below
        ),
        sentiment_defensive_extra_aging_reduction_pct=(
            sentiment_defensive_extra_aging_reduction_pct
        )
    )

    while True:
        try:
            now = datetime.now(timezone.utc)
            cycle_id = now.isoformat()
            actions = []

            price = get_price()
            sentiment_payload = get_sentiment()

            if price is None or sentiment_payload is None:
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    execution_signal=None,
                    reason="missing_price_or_signal",
                    cycle_id=cycle_id
                )
                time.sleep(price_check_interval_seconds)
                continue

            execution_signal = sentiment_payload["execution_signal"]
            target_prices = sentiment_payload.get("target_prices", [])
            llm_target = select_llm_target(target_prices, price)
            price_regime = sentiment_payload.get("price_regime", {})
            regime = sentiment_regime(execution_signal)
            effective_position_size_pct = (
                position_size_pct * regime["position_size_multiplier"]
            )
            effective_max_inventory_usd = (
                max_inventory_usd * regime["inventory_multiplier"]
            )
            effective_max_open_sell_orders = max(
                1,
                int(round(
                    max_open_sell_orders * regime["open_sell_multiplier"]
                ))
            )

            log_event(
                "SIGNAL_UPDATE",
                execution_signal=execution_signal,
                price=price,
                btc_sentiment=sentiment_payload.get("btc_sentiment"),
                confidence=sentiment_payload.get("confidence"),
                raw_btc_sentiment=sentiment_payload.get("raw_btc_sentiment"),
                raw_confidence=sentiment_payload.get("raw_confidence"),
                direction_bias=sentiment_payload.get("direction_bias"),
                raw_direction_bias=sentiment_payload.get("raw_direction_bias"),
                fear_greed_index=sentiment_payload.get("fear_greed_index"),
                llm_target_count=len(target_prices),
                active_llm_target=(
                    None if llm_target is None
                    else round(llm_target["buy_price"], PRICE_DECIMALS)
                ),
                price_regime_range_position=price_regime.get("range_position_24h"),
                price_regime_volatility_pct=price_regime.get(
                    "realized_volatility_24h_pct"
                ),
                sentiment_regime=regime["name"],
                effective_position_size_pct=effective_position_size_pct,
                effective_max_inventory_usd=effective_max_inventory_usd,
                effective_max_open_sell_orders=effective_max_open_sell_orders,
                high_anchor_enabled=regime["allow_high_anchor"]
            )
            console(f"Price: {price} | Signal: {execution_signal}")

            if (
                state["last_range_refresh"] is None
                or (
                    now - datetime.fromisoformat(state["last_range_refresh"])
                ).total_seconds() > (range_refresh_interval_minutes * 60)
            ):
                refresh_range()

            low = state["range_low"]
            high = state["range_high"]
            mean = state["range_mean"]
            median = state["range_median"]

            low = numeric_or_default(
                price_regime.get("price_low_24h"),
                low
            )
            high = numeric_or_default(
                price_regime.get("price_high_24h"),
                high
            )
            mean = numeric_or_default(
                price_regime.get("price_mean_24h"),
                mean
            )
            median = numeric_or_default(
                price_regime.get("price_median_24h"),
                median
            )

            # SELL EXIT CHECK
            for txid, order in list(state["open_sell_orders"].items()):
                status = get_order_status(txid)

                if status is None:
                    continue

                if status == "closed":
                    buy_price = order.get("buy_price")
                    sell_price = order.get("sell_price")
                    volume = order.get("volume", 0)
                    gross_pnl = None
                    estimated_net_pnl = None
                    hold_minutes = None

                    if (
                        buy_price is not None
                        and sell_price is not None
                        and volume is not None
                    ):
                        gross_pnl = volume * (sell_price - buy_price)
                        estimated_net_pnl = gross_pnl - (
                            volume * buy_price * round_trip_fee_pct
                        )

                    placed_at = parse_iso8601(order.get("placed_at"))
                    if placed_at is not None:
                        hold_minutes = (
                            now - placed_at
                        ).total_seconds() / 60

                    sell_level = order.get("level")
                    del state["open_sell_orders"][txid]
                    state["stats"]["sell_orders_filled"] += 1
                    if gross_pnl is not None:
                        state["stats"]["realized_gross_pnl"] += gross_pnl
                    if estimated_net_pnl is not None:
                        state["stats"]["realized_estimated_net_pnl"] += (
                            estimated_net_pnl
                        )
                    save_state(state)
                    actions.append("sell_filled")

                    log_and_console(
                        "SELL_ORDER_FILLED",
                        message=f"SELL filled for level {sell_level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        level=sell_level,
                        volume=volume,
                        buy_price=buy_price,
                        sell_price=sell_price,
                        buy_source=order.get("buy_source"),
                        gross_pnl=gross_pnl,
                        estimated_net_pnl=estimated_net_pnl,
                        hold_minutes=hold_minutes
                    )
                    continue

                if status == "open":
                    buy_price = order.get("buy_price")
                    current_sell_price = order.get("sell_price")
                    sell_pct_override = order.get("sell_pct_override")
                    placed_at = parse_iso8601(order.get("placed_at"))
                    age_minutes = None
                    if placed_at is not None:
                        age_minutes = (
                            now - placed_at
                        ).total_seconds() / 60

                    adjusted_profit_target = compute_adjusted_profit_target(
                        age_minutes,
                        sell_pct_override
                    )
                    adjusted_profit_target = max(
                        min_profit_target_pct,
                        adjusted_profit_target
                        - regime["extra_aging_reduction_pct"]
                    )

                    if buy_price is None or current_sell_price is None:
                        continue

                    adjusted_sell_price = compute_sell_target_price(
                        buy_price,
                        adjusted_profit_target
                    )

                    if round(adjusted_sell_price, PRICE_DECIMALS) >= round(
                        current_sell_price,
                        PRICE_DECIMALS
                    ):
                        continue

                    cancel_resp = kraken_call(
                        "CANCEL_SELL",
                        cancel_order,
                        txid
                    )

                    if not cancel_resp or cancel_resp.get("error"):
                        actions.append("sell_reprice_cancel_failed")
                        log_event(
                            "SELL_REPRICE_SKIPPED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_price=buy_price,
                            current_sell_price=current_sell_price,
                            adjusted_sell_price=adjusted_sell_price,
                            age_minutes=age_minutes,
                            adjusted_profit_target_pct=adjusted_profit_target,
                            reason="cancel_failed"
                        )
                        continue

                    replace_resp = kraken_call(
                        "REPRICE_SELL",
                        place_sell,
                        adjusted_sell_price,
                        order["volume"]
                    )

                    if not replace_resp or replace_resp.get("error"):
                        actions.append("sell_reprice_replace_failed")
                        log_event(
                            "SELL_REPRICE_SKIPPED",
                            cycle_id=cycle_id,
                            txid=txid,
                            level=order.get("level"),
                            buy_price=buy_price,
                            current_sell_price=current_sell_price,
                            adjusted_sell_price=adjusted_sell_price,
                            age_minutes=age_minutes,
                            adjusted_profit_target_pct=adjusted_profit_target,
                            reason="replace_failed"
                        )
                        continue

                    new_txid = replace_resp["result"]["txid"][0]
                    state["open_sell_orders"][new_txid] = {
                        "level": order.get("level"),
                        "volume": order["volume"],
                        "buy_price": buy_price,
                        "sell_price": adjusted_sell_price,
                        "placed_at": order.get("placed_at"),
                        "sell_pct_override": sell_pct_override,
                        "buy_source": order.get("buy_source")
                    }
                    del state["open_sell_orders"][txid]
                    save_state(state)
                    actions.append("sell_repriced")

                    log_and_console(
                        "SELL_ORDER_REPRICED",
                        message=(
                            f"SELL repriced from "
                            f"{round(current_sell_price, PRICE_DECIMALS)} to "
                            f"{round(adjusted_sell_price, PRICE_DECIMALS)}"
                        ),
                        cycle_id=cycle_id,
                        old_txid=txid,
                        txid=new_txid,
                        level=order.get("level"),
                        volume=order.get("volume"),
                        buy_price=buy_price,
                        old_sell_price=current_sell_price,
                        sell_price=adjusted_sell_price,
                        age_minutes=age_minutes,
                        adjusted_profit_target_pct=adjusted_profit_target,
                        sell_pct_override=sell_pct_override,
                        buy_source=order.get("buy_source")
                    )
                    continue

                if status in ("canceled", "expired"):
                    sell_level = order.get("level")
                    del state["open_sell_orders"][txid]
                    save_state(state)
                    actions.append(f"sell_{status}")

                    log_and_console(
                        "ORDER_" + status.upper(),
                        message=f"SELL order {status} for level {sell_level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        side="sell",
                        level=sell_level,
                        volume=order.get("volume"),
                        buy_price=order.get("buy_price"),
                        sell_price=order.get("sell_price")
                    )

            # SELL CHECK
            for level, order in list(state["open_buy_orders"].items()):
                txid = order["txid"]
                status = get_order_status(txid)

                if status is None:
                    continue

                if status in ("canceled", "expired"):
                    del state["open_buy_orders"][level]
                    save_state(state)
                    actions.append(f"buy_{status}")

                    log_and_console(
                        "ORDER_" + status.upper(),
                        message=f"BUY order {status} for level {level}",
                        cycle_id=cycle_id,
                        txid=txid,
                        side="buy",
                        level=level,
                        volume=order.get("volume"),
                        price=order.get("price", float(level))
                    )
                    continue

                if status != "closed":
                    continue

                buy_price = float(level)
                sell_pct_override = order.get("sell_pct_override")
                buy_source = order.get("buy_source")
                sell_price = compute_sell_target_price(
                    buy_price,
                    sell_pct_override
                )
                placed_at = parse_iso8601(order.get("placed_at"))
                hold_minutes = None
                if placed_at is not None:
                    hold_minutes = (
                        now - placed_at
                    ).total_seconds() / 60

                state["stats"]["buy_orders_filled"] += 1
                actions.append("buy_filled")

                log_and_console(
                    "BUY_ORDER_FILLED",
                    message=f"BUY filled @ {round(buy_price, PRICE_DECIMALS)}",
                    cycle_id=cycle_id,
                    txid=txid,
                    level=level,
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(buy_price, PRICE_DECIMALS),
                    hold_minutes=hold_minutes,
                    sell_pct_override=sell_pct_override
                )

                log_event(
                    "TRADE_DECISION",
                    cycle_id=cycle_id,
                    side="sell",
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(sell_price, PRICE_DECIMALS),
                    buy_price=round(buy_price, PRICE_DECIMALS),
                    sell_pct_override=sell_pct_override,
                    buy_source=buy_source
                )

                sell_resp = kraken_call(
                    "SELL",
                    place_sell,
                    sell_price,
                    order["volume"]
                )

                if not sell_resp or sell_resp.get("error"):
                    actions.append("sell_rejected")
                    log_event(
                        "ORDER_REJECTED",
                        cycle_id=cycle_id,
                        side="sell",
                        level=level,
                        volume=round(order["volume"], VOLUME_DECIMALS),
                        buy_price=round(buy_price, PRICE_DECIMALS),
                        sell_price=round(sell_price, PRICE_DECIMALS),
                        sell_pct_override=sell_pct_override,
                        buy_source=buy_source,
                        error=(
                            None if not sell_resp
                            else sell_resp.get("error")
                        )
                    )
                    continue

                txid = sell_resp["result"]["txid"][0]
                state["open_sell_orders"][txid] = {
                    "level": level,
                    "volume": order["volume"],
                    "buy_price": buy_price,
                    "sell_price": sell_price,
                    "placed_at": cycle_id,
                    "sell_pct_override": sell_pct_override,
                    "buy_source": buy_source
                }
                del state["open_buy_orders"][level]
                state["stats"]["sell_orders_placed"] += 1
                save_state(state)
                actions.append("sell_placed")

                log_and_console(
                    "SELL_ORDER_PLACED",
                    message=f"SELL placed @ {round(sell_price, PRICE_DECIMALS)}",
                    cycle_id=cycle_id,
                    txid=txid,
                    volume=round(order["volume"], VOLUME_DECIMALS),
                    price=round(sell_price, PRICE_DECIMALS),
                    buy_price=round(buy_price, PRICE_DECIMALS),
                    sell_pct_override=sell_pct_override,
                    buy_source=buy_source
                )

            # BUY CANDIDATES
            if low and high and execution_signal >= execution_signal_threshold:
                candidate_levels = []
                if llm_target is not None:
                    candidate_levels = [
                        {
                            "level": llm_target["buy_price"],
                            "sell_pct_override": llm_target["sell_pct"],
                            "buy_source": "llm_target"
                        }
                    ]
                else:
                    for strategy_mode in strategy_modes:
                        if strategy_mode == "mean":
                            grid = compute_grid(low, high, mean)
                            sell_pct_override = None
                            buy_source = "range_mean"
                        elif strategy_mode == "median" and median is not None:
                            grid = compute_grid(low, high, median)
                            sell_pct_override = None
                            buy_source = "range_median"
                        elif strategy_mode == "high":
                            if not regime["allow_high_anchor"]:
                                continue
                            grid = compute_high_anchor_grid(high, price)
                            sell_pct_override = high_anchor_profit_target_pct
                            buy_source = "range_high_band"
                        else:
                            grid = compute_grid(low, high, low)
                            sell_pct_override = None
                            buy_source = "range_low"

                        for level in grid:
                            candidate_levels.append(
                                {
                                    "level": level,
                                    "sell_pct_override": sell_pct_override,
                                    "buy_source": buy_source
                                }
                            )

                deduped_candidates = []
                seen_levels = set()
                for candidate in candidate_levels:
                    rounded_level = round(candidate["level"], PRICE_DECIMALS)
                    if rounded_level in seen_levels:
                        continue
                    seen_levels.add(rounded_level)
                    deduped_candidates.append(candidate)

                bal = kraken_call("BALANCE", api.query_private, "Balance")

                if not bal:
                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="hold",
                        price=price,
                        execution_signal=execution_signal,
                        reason="balance_fetch_failed"
                    )
                    actions.append("hold_balance_fetch_failed")
                    time.sleep(price_check_interval_seconds)
                    continue

                usd = float(bal["result"].get("ZUSD", 0))
                reserved_sell_levels = {
                    sell_order.get("level")
                    for sell_order in state["open_sell_orders"].values()
                }
                deployed_inventory_usd = current_inventory_usd(price)
                high_anchor_order_count = high_anchor_open_order_count()
                high_anchor_cooldown_remaining = (
                    high_anchor_cooldown_remaining_minutes(now)
                )

                for candidate in deduped_candidates:
                    level = candidate["level"]
                    active_sell_pct_override = candidate["sell_pct_override"]
                    buy_source = candidate["buy_source"]
                    key = str(level)
                    skip_reason = None

                    if key in state["open_buy_orders"]:
                        skip_reason = "open_buy_order"
                    elif key in reserved_sell_levels:
                        skip_reason = "open_sell_order"
                    elif buy_source != "llm_target" and price > level:
                        skip_reason = "price_above_level"
                    elif (
                        len(state["open_sell_orders"])
                        >= effective_max_open_sell_orders
                    ):
                        skip_reason = "max_open_sell_orders"
                    elif deployed_inventory_usd >= effective_max_inventory_usd:
                        skip_reason = "max_inventory_usd"
                    elif (
                        buy_source == "range_high_band"
                        and high_anchor_cooldown_remaining > 0
                    ):
                        skip_reason = "high_anchor_cooldown"
                    elif (
                        buy_source == "range_high_band"
                        and high_anchor_order_count >= max_open_high_anchor_orders
                    ):
                        skip_reason = "max_open_high_anchor_orders"

                    volume = (usd * effective_position_size_pct) / level
                    projected_inventory_usd = deployed_inventory_usd + (
                        level * volume
                    )

                    if (
                        skip_reason is None
                        and projected_inventory_usd > effective_max_inventory_usd
                    ):
                        skip_reason = "max_inventory_usd"

                    if skip_reason is None and volume < 0.00005:
                        skip_reason = "below_min_volume"

                    if skip_reason is not None:
                        log_event(
                            "GRID_LEVEL_EVAL",
                            cycle_id=cycle_id,
                            level=round(level, PRICE_DECIMALS),
                            market_price=price,
                            execution_signal=execution_signal,
                            usd_balance=usd,
                            deployed_inventory_usd=deployed_inventory_usd,
                            high_anchor_order_count=high_anchor_order_count,
                            high_anchor_cooldown_remaining_minutes=round(
                                high_anchor_cooldown_remaining,
                                2
                            ),
                            sentiment_regime=regime["name"],
                            effective_position_size_pct=(
                                effective_position_size_pct
                            ),
                            effective_max_inventory_usd=(
                                effective_max_inventory_usd
                            ),
                            effective_max_open_sell_orders=(
                                effective_max_open_sell_orders
                            ),
                            buy_source=buy_source,
                            reason=skip_reason
                        )
                        continue

                    log_event(
                        "TRADE_DECISION",
                        cycle_id=cycle_id,
                        side="buy",
                        volume=round(volume, VOLUME_DECIMALS),
                        price=round(level, PRICE_DECIMALS),
                        execution_signal=execution_signal,
                        range_low=low,
                        range_high=high,
                        range_mean=mean,
                        range_median=median,
                        sentiment_regime=regime["name"],
                        effective_position_size_pct=effective_position_size_pct,
                        buy_source=buy_source,
                        high_anchor_order_count=high_anchor_order_count,
                        sell_pct_override=active_sell_pct_override
                    )

                    buy_resp = kraken_call(
                        "BUY",
                        place_buy,
                        level,
                        volume
                    )

                    if not buy_resp or buy_resp.get("error"):
                        actions.append("buy_rejected")
                        log_event(
                            "ORDER_REJECTED",
                            cycle_id=cycle_id,
                            side="buy",
                            level=round(level, PRICE_DECIMALS),
                            volume=round(volume, VOLUME_DECIMALS),
                            execution_signal=execution_signal,
                            buy_source=buy_source,
                            sell_pct_override=active_sell_pct_override,
                            error=(
                                None if not buy_resp
                                else buy_resp.get("error")
                            )
                        )
                        continue

                    txid = buy_resp["result"]["txid"][0]
                    state["open_buy_orders"][key] = {
                        "txid": txid,
                        "volume": volume,
                        "price": level,
                        "placed_at": cycle_id,
                        "sell_pct_override": active_sell_pct_override,
                        "buy_source": buy_source
                    }
                    if buy_source == "range_high_band":
                        state["last_high_anchor_buy_at"] = cycle_id
                        high_anchor_order_count += 1
                        high_anchor_cooldown_remaining = (
                            high_anchor_buy_cooldown_minutes
                        )
                    state["stats"]["buy_orders_placed"] += 1
                    save_state(state)
                    actions.append("buy_placed")
                    deployed_inventory_usd = projected_inventory_usd

                    log_and_console(
                        "BUY_ORDER_PLACED",
                        message=f"BUY placed @ {round(level, PRICE_DECIMALS)}",
                        cycle_id=cycle_id,
                        txid=txid,
                        volume=round(volume, VOLUME_DECIMALS),
                        price=round(level, PRICE_DECIMALS),
                        buy_source=buy_source,
                        sell_pct_override=active_sell_pct_override
                    )
            else:
                log_event(
                    "TRADE_DECISION",
                    side="hold",
                    price=price,
                    execution_signal=execution_signal,
                    threshold=execution_signal_threshold,
                    range_low=low,
                    range_high=high,
                    range_mean=mean,
                    range_median=median,
                    sentiment_regime=regime["name"],
                    reason="signal_below_threshold_or_range_unavailable",
                    cycle_id=cycle_id,
                    price_regime_range_position=price_regime.get(
                        "range_position_24h"
                    ),
                    price_regime_volatility_pct=price_regime.get(
                        "realized_volatility_24h_pct"
                    )
                )
                actions.append("hold")

            log_event(
                "CYCLE_SUMMARY",
                cycle_id=cycle_id,
                price=price,
                execution_signal=execution_signal,
                threshold=execution_signal_threshold,
                range_low=low,
                range_high=high,
                range_mean=mean,
                range_median=median,
                price_regime_timestamp=price_regime.get("timestamp"),
                price_regime_range_position=price_regime.get(
                    "range_position_24h"
                ),
                price_regime_volatility_pct=price_regime.get(
                    "realized_volatility_24h_pct"
                ),
                price_regime_mean_reversion_buy_target=price_regime.get(
                    "mean_reversion_buy_target"
                ),
                price_regime_median_reversion_buy_target=price_regime.get(
                    "median_reversion_buy_target"
                ),
                sentiment_regime=regime["name"],
                effective_position_size_pct=effective_position_size_pct,
                effective_max_inventory_usd=effective_max_inventory_usd,
                effective_max_open_sell_orders=effective_max_open_sell_orders,
                high_anchor_enabled=regime["allow_high_anchor"],
                grid_anchor=grid_anchor,
                buy_source=(
                    "llm_target"
                    if low and high and execution_signal >= execution_signal_threshold and llm_target is not None
                    else ",".join(strategy_modes)
                ),
                strategy_modes=strategy_modes,
                grid_levels=(
                    [
                        round(candidate["level"], PRICE_DECIMALS)
                        for candidate in deduped_candidates
                    ]
                    if low and high and execution_signal >= execution_signal_threshold
                    else []
                ),
                high_anchor_order_count=high_anchor_open_order_count(),
                high_anchor_cooldown_remaining_minutes=round(
                    high_anchor_cooldown_remaining_minutes(now),
                    2
                ),
                deployed_inventory_usd=round(
                    current_inventory_usd(price), 8
                ),
                open_buy_count=len(state["open_buy_orders"]),
                open_sell_count=len(state["open_sell_orders"]),
                open_buy_volume=sum(
                    order.get("volume", 0) or 0
                    for order in state["open_buy_orders"].values()
                ),
                open_sell_volume=sum(
                    order.get("volume", 0) or 0
                    for order in state["open_sell_orders"].values()
                ),
                buy_orders_placed=state["stats"]["buy_orders_placed"],
                buy_orders_filled=state["stats"]["buy_orders_filled"],
                sell_orders_placed=state["stats"]["sell_orders_placed"],
                sell_orders_filled=state["stats"]["sell_orders_filled"],
                realized_gross_pnl=round(
                    state["stats"]["realized_gross_pnl"], 8
                ),
                realized_estimated_net_pnl=round(
                    state["stats"]["realized_estimated_net_pnl"], 8
                ),
                actions=actions or ["no_action"]
            )

            time.sleep(price_check_interval_seconds)
        except Exception as e:
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            time.sleep(price_check_interval_seconds)


if __name__ == "__main__":
    main()
