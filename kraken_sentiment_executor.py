#!/usr/bin/env python3

# =====================================================
# KRAKEN SENTIMENT EXECUTOR
# =====================================================

import base64
import hashlib
import hmac
import json
import math
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

# ----------------------
# CONFIG
# ----------------------

CONFIG_FILE = os.getenv("BOT_CONFIG_FILE", "sentiment_bot_config.json")
STATE_FILE = os.getenv("BOT_STATE_FILE", "sentiment_state.json")
LOG_FILE = os.getenv("TRADE_LOG_FILE", "sentiment_trade_log.jsonl")

KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
SIGNAL_FILE = os.getenv("SIGNAL_FILE")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")


def load_config():
    if not os.path.exists(CONFIG_FILE):
        return {}

    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


config = load_config()


def env_bool(name, default):
    value = os.getenv(name)
    if value is None:
        return default

    return value.strip().lower() in ("1", "true", "yes", "on")


def env_float(name, default):
    value = os.getenv(name)
    return default if value is None else float(value)


def env_int(name, default):
    value = os.getenv(name)
    return default if value is None else int(value)


REQUEST_TIMEOUT = env_int(
    "REQUEST_TIMEOUT_SECONDS",
    config.get("request_timeout_seconds", 10)
)
KRAKEN_NONCE_RETRIES = env_int(
    "KRAKEN_NONCE_RETRIES",
    config.get("kraken_nonce_retries", 2)
)
PRICE_CHECK_INTERVAL_SECONDS = env_int(
    "PRICE_CHECK_INTERVAL_SECONDS",
    config.get("price_check_interval_seconds", 60)
)
BASE_ALLOCATION = env_float(
    "BASE_BTC_ALLOCATION",
    config.get("base_btc_allocation", 0.50)
)
MAX_STEP = env_float(
    "MAX_ADJUSTMENT_PER_CYCLE",
    config.get("max_adjustment_per_cycle", 0.10)
)
MIN_TRADE_USD = env_float(
    "MIN_TRADE_USD",
    config.get("min_trade_usd", 30)
)
CONF_THRESHOLD = env_float(
    "CONFIDENCE_THRESHOLD",
    config.get("confidence_threshold", 0.45)
)
CONFIDENCE_WEIGHTING = env_bool(
    "CONFIDENCE_WEIGHTING",
    config.get("confidence_weighting", True)
)
DRY_RUN = env_bool("DRY_RUN", config.get("dry_run", False))
MIN_ALLOC = env_float(
    "MIN_TOTAL_ALLOCATION",
    config.get("min_total_allocation", 0.05)
)
MAX_ALLOC = env_float(
    "MAX_TOTAL_ALLOCATION",
    config.get("max_total_allocation", 0.95)
)
MIN_ALLOC_DELTA = env_float(
    "MIN_ALLOCATION_CHANGE",
    config.get("min_allocation_change", 0.02)
)
EXECUTION_BUFFER_PCT = env_float(
    "EXECUTION_BUFFER_PCT",
    config.get("execution_buffer_pct", 0.0025)
)

PAIR_INFO_CACHE = None

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


def log_and_console(event, message="", **kwargs):
    log_event(event, message=message, **kwargs)

    if message:
        console(f"{event}: {message}")
    else:
        console(event)


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
            "errors": 0
        },
        "last_nonce": 0
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

    return state


def save_state(state):
    state_dir = os.path.dirname(STATE_FILE)
    if state_dir:
        os.makedirs(state_dir, exist_ok=True)

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


state = load_state()

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
                attempt=attempt
            )

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
            r = requests.get(KRAKEN_TICKER_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()

            if data.get("error"):
                raise RuntimeError(data["error"])

            ticker = next(iter(data["result"].values()), None)
            if ticker:
                return float(ticker["c"][0])

        url = KRAKEN_API_URL.rstrip("/") + "/0/public/Ticker"
        r = requests.get(
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


def get_pair_info():
    global PAIR_INFO_CACHE

    if PAIR_INFO_CACHE:
        return PAIR_INFO_CACHE

    url = KRAKEN_API_URL.rstrip("/") + "/0/public/AssetPairs"
    r = requests.get(
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


def get_min_order_volume():
    pair_info = get_pair_info()
    return float(pair_info.get("ordermin") or 0)


# ----------------------
# SIGNAL
# ----------------------


def load_signal():
    try:
        if LLM_SIGNAL_URL:
            r = requests.get(LLM_SIGNAL_URL, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            signal = r.json()
        else:
            with open(SIGNAL_FILE, encoding="utf-8") as f:
                signal = json.load(f)

        return signal
    except Exception as e:
        log_event("SENTIMENT_ERROR", message=str(e))
        return None


def normalize_signal(signal):
    if not isinstance(signal, dict):
        return {
            "execution_signal": float(signal),
            "confidence": 1.0
        }

    return {
        "execution_signal": float(signal.get("execution_signal", 0)),
        "confidence": float(signal.get("confidence", 0)),
        "btc_sentiment": signal.get("btc_sentiment"),
        "regulatory_risk": signal.get("regulatory_risk"),
        "macro_tightening_bias": signal.get("macro_tightening_bias"),
        "direction_bias": signal.get("direction_bias"),
        "raw_btc_sentiment": signal.get("raw_btc_sentiment"),
        "raw_confidence": signal.get("raw_confidence"),
        "raw_direction_bias": signal.get("raw_direction_bias"),
        "fear_greed_index": signal.get("fear_greed_index"),
        "processed_at": signal.get("processed_at")
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


def place_market(side, volume, cycle_id):
    if DRY_RUN:
        state["stats"]["dry_run_trades"] += 1
        save_state(state)
        log_and_console(
            "DRY_RUN",
            message=f"{side} {volume}",
            cycle_id=cycle_id,
            side=side,
            volume=volume
        )
        return {"dry_run": True}

    result = safe_kraken_private(
        "ADD_ORDER",
        "/0/private/AddOrder",
        {
            "pair": KRAKEN_PAIR,
            "type": side,
            "ordertype": "market",
            "volume": volume
        }
    )

    if result:
        state["stats"]["trades_executed"] += 1
        save_state(state)
        log_and_console(
            "TRADE_EXECUTED",
            message=f"{side} {volume}",
            cycle_id=cycle_id,
            side=side,
            volume=volume,
            result=result.get("result")
        )

    return result


# ----------------------
# EXECUTOR
# ----------------------


def skip_cycle(reason, cycle_id, **kwargs):
    state["stats"]["skips"] += 1
    save_state(state)
    log_event(
        "TRADE_DECISION",
        cycle_id=cycle_id,
        side="hold",
        reason=reason,
        **kwargs
    )


def run_cycle():
    now = datetime.now(timezone.utc)
    cycle_id = now.isoformat()
    state["last_cycle"] = cycle_id
    state["stats"]["cycles"] += 1
    save_state(state)

    signal = load_signal()
    if signal is None:
        skip_cycle("missing_signal", cycle_id)
        return

    sentiment = normalize_signal(signal)
    raw_signal = sentiment["execution_signal"]
    confidence = sentiment["confidence"]

    if confidence < CONF_THRESHOLD:
        skip_cycle(
            "low_confidence",
            cycle_id,
            execution_signal=raw_signal,
            confidence=confidence,
            confidence_threshold=CONF_THRESHOLD
        )
        return

    smoothed_signal = smooth_signal(raw_signal)
    weighted_signal = (
        smoothed_signal * confidence
        if CONFIDENCE_WEIGHTING
        else smoothed_signal
    )

    price = get_price()
    if price is None:
        skip_cycle(
            "missing_price",
            cycle_id,
            execution_signal=raw_signal,
            confidence=confidence
        )
        return

    log_event(
        "SIGNAL_UPDATE",
        cycle_id=cycle_id,
        price=price,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        btc_sentiment=sentiment.get("btc_sentiment"),
        raw_btc_sentiment=sentiment.get("raw_btc_sentiment"),
        raw_confidence=sentiment.get("raw_confidence"),
        direction_bias=sentiment.get("direction_bias"),
        raw_direction_bias=sentiment.get("raw_direction_bias"),
        fear_greed_index=sentiment.get("fear_greed_index"),
        processed_at=sentiment.get("processed_at")
    )
    console(f"Price: {price} | Signal: {raw_signal} | Confidence: {confidence}")

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
    portfolio_value = base_balance * price + quote_balance

    if portfolio_value <= 0:
        skip_cycle(
            "empty_portfolio",
            cycle_id,
            price=price,
            base_balance=base_balance,
            quote_balance=quote_balance,
            portfolio_value=portfolio_value
        )
        return

    allocation_current = (base_balance * price) / portfolio_value
    allocation_target = BASE_ALLOCATION + weighted_signal
    allocation_target = max(MIN_ALLOC, allocation_target)
    allocation_target = min(MAX_ALLOC, allocation_target)

    raw_delta = allocation_target - allocation_current
    delta = max(-MAX_STEP, min(MAX_STEP, raw_delta))

    if abs(delta) < MIN_ALLOC_DELTA:
        skip_cycle(
            "small_delta",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            confidence=confidence,
            allocation_current=allocation_current,
            allocation_target=allocation_target,
            raw_delta=raw_delta,
            delta=delta
        )
        return

    trade_value = abs(delta) * portfolio_value
    trade_value *= max(0, 1 - EXECUTION_BUFFER_PCT)

    if trade_value < MIN_TRADE_USD:
        skip_cycle(
            "small_trade",
            cycle_id,
            price=price,
            execution_signal=raw_signal,
            confidence=confidence,
            trade_value=trade_value,
            min_trade_usd=MIN_TRADE_USD
        )
        return

    side = "buy" if delta > 0 else "sell"
    volume = round_volume(trade_value / price)
    min_volume = get_min_order_volume()

    if volume <= 0:
        skip_cycle(
            "zero_volume",
            cycle_id,
            price=price,
            trade_value=trade_value,
            volume=volume
        )
        return

    if volume < min_volume:
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
        side=side,
        volume=volume,
        price=price,
        trade_value=trade_value,
        base_balance=base_balance,
        quote_balance=quote_balance,
        portfolio_value=portfolio_value,
        allocation_current=allocation_current,
        allocation_target=allocation_target,
        raw_delta=raw_delta,
        delta=delta,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence
    )

    place_market(side, volume, cycle_id)

    log_event(
        "CYCLE_SUMMARY",
        cycle_id=cycle_id,
        price=price,
        execution_signal=raw_signal,
        smoothed_signal=smoothed_signal,
        weighted_signal=weighted_signal,
        confidence=confidence,
        side=side,
        volume=volume,
        trade_value=trade_value,
        allocation_current=allocation_current,
        allocation_target=allocation_target,
        base_balance=base_balance,
        quote_balance=quote_balance,
        portfolio_value=portfolio_value,
        dry_run=DRY_RUN
    )


# ----------------------
# MAIN LOOP
# ----------------------


def main():
    require_runtime_config()

    log_and_console(
        "BOT_START",
        message="Sentiment executor starting",
        config_file=CONFIG_FILE,
        state_file=STATE_FILE,
        log_file=LOG_FILE,
        pair=KRAKEN_PAIR,
        dry_run=DRY_RUN,
        base_btc_allocation=BASE_ALLOCATION,
        max_adjustment_per_cycle=MAX_STEP,
        min_trade_usd=MIN_TRADE_USD,
        confidence_threshold=CONF_THRESHOLD,
        confidence_weighting=CONFIDENCE_WEIGHTING,
        min_total_allocation=MIN_ALLOC,
        max_total_allocation=MAX_ALLOC,
        min_allocation_change=MIN_ALLOC_DELTA,
        execution_buffer_pct=EXECUTION_BUFFER_PCT,
        price_check_interval_seconds=PRICE_CHECK_INTERVAL_SECONDS
    )

    while True:
        try:
            run_cycle()
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            log_and_console("BOT_STOP", message="Sentiment executor stopped")
            break
        except Exception as e:
            state["stats"]["errors"] += 1
            save_state(state)
            log_event("LOOP_ERROR", message=str(e))
            console(f"Loop error: {e}")
            time.sleep(PRICE_CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
