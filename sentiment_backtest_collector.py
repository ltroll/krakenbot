#!/usr/bin/env python3
"""Collect lightweight sentiment-bot snapshots for offline backtesting."""

import argparse
import json
import os
import socket
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
from urllib.parse import urlencode
from urllib.request import Request
from urllib.request import urlopen

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(*_args, **_kwargs):
        return False
from signal_normalizer import normalize_signal_payload


ENV_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
load_dotenv(dotenv_path=ENV_FILE, override=True)

CONFIG_FILE = os.getenv("SENTIMENT_CONFIG_FILE", "sentiment_bot_config.json")
STRATEGY_PROFILE = os.getenv(
    "SENTIMENT_STRATEGY_PROFILE",
    "sentiment_strategy_default.json",
)
STATE_FILE = os.getenv("SENTIMENT_STATE_FILE", "sentiment_state.json")
SNAPSHOT_LOG_FILE = os.getenv(
    "SENTIMENT_BACKTEST_SNAPSHOT_FILE",
    "sentiment_backtest_snapshot_log.jsonl",
)
SNAPSHOT_ROTATE_DAILY = os.getenv(
    "SENTIMENT_BACKTEST_ROTATE_DAILY",
    "true",
).strip().lower() in ("1", "true", "yes", "on")
SNAPSHOT_RETENTION_DAYS = int(os.getenv(
    "SENTIMENT_BACKTEST_SNAPSHOT_RETENTION_DAYS",
    "14",
))

KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
KRAKEN_TICKER_URL = os.getenv("KRAKEN_TICKER_URL")
KRAKEN_ORDERBOOK_URL = os.getenv("KRAKEN_ORDERBOOK_URL")
LLM_SIGNAL_URL = os.getenv("LLM_SIGNAL_URL")
SIGNAL_FILE = os.getenv("SIGNAL_FILE")
SIGNAL_ASSET_ID = os.getenv("SIGNAL_ASSET_ID") or os.getenv("ASSET_ID")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))


def now_utc():
    return datetime.now(timezone.utc)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Collect one sentiment-bot backtest snapshot and exit.",
    )
    parser.add_argument(
        "--snapshot-file",
        default=SNAPSHOT_LOG_FILE,
        help="JSONL file to append. Default: SENTIMENT_BACKTEST_SNAPSHOT_FILE.",
    )
    parser.add_argument(
        "--no-rotate-daily",
        action="store_true",
        help="Append directly to --snapshot-file instead of adding YYYYMMDD.",
    )
    parser.add_argument(
        "--skip-orderbook",
        action="store_true",
        help="Skip order book fetch to minimize collector load.",
    )
    parser.add_argument(
        "--stdout",
        action="store_true",
        help="Print the snapshot JSON instead of appending to a file.",
    )
    return parser.parse_args(argv)


def is_url(value):
    if not value:
        return False
    return urlparse(str(value)).scheme in ("http", "https")


def read_json_source(source, timeout=REQUEST_TIMEOUT):
    if not source:
        return {"ok": False, "error": "missing_source", "payload": None}
    try:
        if is_url(source):
            with urlopen(source, timeout=timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        else:
            with open(os.path.expanduser(source), encoding="utf-8") as f:
                payload = json.load(f)
        return {"ok": True, "error": None, "payload": payload}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "payload": None}


def fetch_public_json(url, params=None, timeout=REQUEST_TIMEOUT):
    try:
        target_url = url
        if params:
            query = urlencode(params)
            separator = "&" if "?" in target_url else "?"
            target_url = f"{target_url}{separator}{query}"
        request = Request(target_url, headers={"User-Agent": "sentiment-backtest-collector/1.0"})
        with urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
        if isinstance(payload, dict) and payload.get("error"):
            return {"ok": False, "error": str(payload["error"]), "payload": payload}
        return {"ok": True, "error": None, "payload": payload}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "payload": None}


def safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def infer_asset_id_from_pair(pair):
    pair = (pair or "").upper()
    if "ETH" in pair or "XETH" in pair:
        return "ETH"
    if "SOL" in pair:
        return "SOL"
    if "XBT" in pair or "BTC" in pair:
        return "BTC"
    return "BTC"


def selected_asset_id():
    return (SIGNAL_ASSET_ID or infer_asset_id_from_pair(KRAKEN_PAIR)).upper()


def strategy_profile_is_file():
    expanded = os.path.expanduser(STRATEGY_PROFILE or "")
    return (
        bool(STRATEGY_PROFILE)
        and (
            os.path.exists(expanded)
            or STRATEGY_PROFILE.endswith(".json")
            or os.sep in STRATEGY_PROFILE
        )
    )


def load_strategy_config():
    if strategy_profile_is_file():
        result = read_json_source(STRATEGY_PROFILE)
        return result if result["ok"] else result

    config = read_json_source(CONFIG_FILE)
    if not config["ok"] or not isinstance(config["payload"], dict):
        return config
    profiles = config["payload"].get("strategy_profiles")
    if profiles is None:
        return config
    profile = profiles.get(STRATEGY_PROFILE)
    if profile is None:
        return {
            "ok": False,
            "error": f"strategy_profile_not_found:{STRATEGY_PROFILE}",
            "payload": None,
        }
    return {"ok": True, "error": None, "payload": profile}


def ticker_snapshot():
    if KRAKEN_TICKER_URL:
        result = fetch_public_json(KRAKEN_TICKER_URL)
    else:
        result = fetch_public_json(
            KRAKEN_API_URL.rstrip("/") + "/0/public/Ticker",
            params={"pair": KRAKEN_PAIR},
        )
    last_price = None
    if result["ok"] and isinstance(result["payload"], dict):
        ticker = next(iter(result["payload"].get("result", {}).values()), None)
        try:
            last_price = float(ticker["c"][0]) if ticker else None
        except Exception:
            last_price = None
    return {
        "ok": result["ok"],
        "error": result["error"],
        "last_price": last_price,
        "source": KRAKEN_TICKER_URL or f"{KRAKEN_API_URL.rstrip()}/0/public/Ticker",
    }


def orderbook_snapshot():
    if KRAKEN_ORDERBOOK_URL:
        result = fetch_public_json(KRAKEN_ORDERBOOK_URL)
    else:
        result = fetch_public_json(
            KRAKEN_API_URL.rstrip("/") + "/0/public/Depth",
            params={"pair": KRAKEN_PAIR, "count": 5},
        )
    book = None
    if result["ok"] and isinstance(result["payload"], dict):
        raw_book = next(iter(result["payload"].get("result", {}).values()), None)
        if raw_book:
            bids = raw_book.get("bids") or []
            asks = raw_book.get("asks") or []
            book = {
                "bid": safe_float(bids[0][0]) if bids else None,
                "bid_volume": safe_float(bids[0][1]) if bids else None,
                "ask": safe_float(asks[0][0]) if asks else None,
                "ask_volume": safe_float(asks[0][1]) if asks else None,
            }
    return {
        "ok": result["ok"],
        "error": result["error"],
        "book": book,
        "source": KRAKEN_ORDERBOOK_URL or f"{KRAKEN_API_URL.rstrip()}/0/public/Depth",
    }


def signal_snapshot():
    source = SIGNAL_FILE or LLM_SIGNAL_URL
    result = read_json_source(source)
    normalized = None
    if result["ok"] and isinstance(result["payload"], dict):
        try:
            normalized = normalize_signal_payload(
                result["payload"],
                asset_id=selected_asset_id(),
            )
        except Exception as exc:
            result = {
                "ok": False,
                "error": f"normalize_failed:{exc}",
                "payload": result["payload"],
            }
    return {
        "ok": result["ok"],
        "error": result["error"],
        "source": source,
        "selected_asset_id": selected_asset_id(),
        "payload": normalized,
    }


def normalize_order_map(order_map):
    orders = []
    if not isinstance(order_map, dict):
        return orders
    for txid, order in order_map.items():
        if not isinstance(order, dict):
            continue
        orders.append({
            "txid": txid,
            "trade_id": order.get("trade_id") or order.get("buy_txid") or txid,
            "volume": safe_float(order.get("volume")),
            "price": safe_float(order.get("price")),
            "buy_price": safe_float(order.get("buy_price")),
            "sell_price": safe_float(order.get("sell_price")),
            "target_profit_pct": safe_float(order.get("target_profit_pct")),
            "round_trip_fee_pct": safe_float(order.get("round_trip_fee_pct")),
            "placed_at": order.get("placed_at"),
            "buy_txid": order.get("buy_txid"),
        })
    return orders


def state_snapshot():
    result = read_json_source(STATE_FILE)
    if not result["ok"] or not isinstance(result["payload"], dict):
        return {
            "ok": result["ok"],
            "error": result["error"],
            "path": os.path.abspath(STATE_FILE),
            "summary": None,
            "open_buy_orders": [],
            "open_sell_orders": [],
        }
    raw = result["payload"]
    open_buy_orders = normalize_order_map(raw.get("open_buy_orders"))
    open_sell_orders = normalize_order_map(raw.get("open_sell_orders"))
    return {
        "ok": True,
        "error": None,
        "path": os.path.abspath(STATE_FILE),
        "summary": {
            "last_cycle": raw.get("last_cycle"),
            "last_trade_at": raw.get("last_trade_at"),
            "last_sell_price": safe_float(raw.get("last_sell_price")),
            "last_sell_at": raw.get("last_sell_at"),
            "open_buy_count": len(open_buy_orders),
            "open_sell_count": len(open_sell_orders),
            "stats": raw.get("stats"),
        },
        "open_buy_orders": open_buy_orders,
        "open_sell_orders": open_sell_orders,
    }


def rotated_snapshot_path(base_path, dt):
    if not SNAPSHOT_ROTATE_DAILY:
        return base_path
    root, ext = os.path.splitext(base_path)
    return f"{root}_{dt.strftime('%Y%m%d')}{ext or '.jsonl'}"


def cleanup_old_snapshots(base_path, retention_days):
    if retention_days <= 0 or not SNAPSHOT_ROTATE_DAILY:
        return
    cutoff = now_utc().date() - timedelta(days=retention_days)
    root, ext = os.path.splitext(base_path)
    directory = os.path.dirname(os.path.abspath(base_path)) or "."
    prefix = os.path.basename(root) + "_"
    suffix = ext or ".jsonl"
    try:
        for name in os.listdir(directory):
            if not name.startswith(prefix) or not name.endswith(suffix):
                continue
            date_text = name[len(prefix):-len(suffix)]
            try:
                file_date = datetime.strptime(date_text, "%Y%m%d").date()
            except ValueError:
                continue
            if file_date < cutoff:
                os.unlink(os.path.join(directory, name))
    except Exception:
        return


def write_snapshot(snapshot, base_path):
    path = rotated_snapshot_path(base_path, now_utc())
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(snapshot, sort_keys=True) + "\n")
    cleanup_old_snapshots(base_path, SNAPSHOT_RETENTION_DAYS)
    return path


def build_snapshot(args):
    captured_at = now_utc().isoformat()
    strategy = load_strategy_config()
    ticker = ticker_snapshot()
    signal = signal_snapshot()
    return {
        "schema_version": "sentiment-bot-backtest-snapshot-v1",
        "captured_at": captured_at,
        "hostname": socket.gethostname(),
        "pair": KRAKEN_PAIR,
        "asset_id": selected_asset_id(),
        "ticker": ticker,
        "orderbook": (
            {"ok": False, "error": "skipped", "book": None}
            if args.skip_orderbook
            else orderbook_snapshot()
        ),
        "signal": signal,
        "strategy_profile": {
            "name": STRATEGY_PROFILE,
            "config_file": CONFIG_FILE,
            "ok": strategy["ok"],
            "error": strategy["error"],
            "payload": strategy["payload"] if strategy["ok"] else None,
        },
        "state": state_snapshot(),
        "collector": {
            "snapshot_file": args.snapshot_file,
            "rotate_daily": not args.no_rotate_daily and SNAPSHOT_ROTATE_DAILY,
            "private_api_used": False,
        },
    }


def main(argv=None):
    args = parse_args(argv)
    global SNAPSHOT_ROTATE_DAILY
    if args.no_rotate_daily:
        SNAPSHOT_ROTATE_DAILY = False

    snapshot = build_snapshot(args)
    if args.stdout:
        print(json.dumps(snapshot, indent=2, sort_keys=True))
        return 0

    path = write_snapshot(snapshot, args.snapshot_file)
    print(f"Wrote snapshot: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
