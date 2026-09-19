#!/usr/bin/env python3

"""HTTP control plane for the range-grid bot.

The service deliberately never calls private Kraken endpoints. It writes a small
operator-control file which the trading process consumes on its next cycle.
"""

import hmac
import ipaddress
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv

from range_grid_control import (
    ControlStateError,
    control_status,
    load_control_state_fail_safe,
    merge_control_update,
    save_control_state,
)


load_dotenv()

CONTROL_FILE = os.getenv(
    "RANGE_GRID_CONTROL_FILE",
    "range_grid_control_state.json",
)
CONTROL_AUDIT_FILE = os.getenv(
    "RANGE_GRID_CONTROL_AUDIT_FILE",
    "range_grid_control_audit.jsonl",
)
STATUS_FILE = os.getenv("RANGE_GRID_STATUS_FILE", "range_grid_status.json")
STATE_FILE = os.getenv("RANGE_GRID_STATE_FILE", "last_state.json")
CONTROL_HOST = os.getenv("RANGE_GRID_CONTROL_HOST", "127.0.0.1")
CONTROL_PORT = int(os.getenv("RANGE_GRID_CONTROL_PORT", "8787"))
CONTROL_TOKEN = os.getenv("RANGE_GRID_CONTROL_TOKEN", "").strip()
CONTROL_MARKET_CACHE_SECONDS = max(
    15,
    int(os.getenv("RANGE_GRID_CONTROL_MARKET_CACHE_SECONDS", "60")),
)
REQUEST_TIMEOUT_SECONDS = max(
    2,
    int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10")),
)
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com").rstrip("/")
KRAKEN_TICKER_URL = os.getenv(
    "KRAKEN_TICKER_URL",
    f"{KRAKEN_API_URL}/0/public/Ticker?pair={KRAKEN_PAIR}",
)
KRAKEN_OHLC_ENDPOINT = os.getenv(
    "RANGE_GRID_CONTROL_OHLC_URL",
    f"{KRAKEN_API_URL}/0/public/OHLC",
)
HTML_FILE = Path(__file__).with_name("range_grid_control_plane.html")


def utc_now():
    return datetime.now(timezone.utc)


def utc_now_iso():
    return utc_now().isoformat()


def read_json_object(path):
    try:
        with open(os.path.expanduser(path), encoding="utf-8") as handle:
            payload = json.load(handle)
        return payload if isinstance(payload, dict) else {}
    except (OSError, ValueError, TypeError):
        return {}


def _result_payload(payload):
    if not isinstance(payload, dict) or payload.get("error"):
        raise RuntimeError(f"Kraken response error: {payload.get('error') if isinstance(payload, dict) else 'invalid response'}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise RuntimeError("Kraken response did not contain a result object")
    return result


def _pair_result(result):
    for key, value in result.items():
        if key != "last":
            return value
    raise RuntimeError("Kraken response did not contain pair data")


def parse_ohlc_candles(payload):
    rows = _pair_result(_result_payload(payload))
    candles = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, list) or len(row) < 5:
            continue
        try:
            candles.append({
                "timestamp": datetime.fromtimestamp(
                    int(float(row[0])),
                    tz=timezone.utc,
                ).isoformat(),
                "epoch": int(float(row[0])),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
            })
        except (TypeError, ValueError, OSError):
            continue
    candles.sort(key=lambda candle: candle["epoch"])
    return candles


def summarize_candles(candles, since):
    cutoff = int(since.timestamp())
    selected = [candle for candle in candles if candle["epoch"] >= cutoff]
    if not selected:
        return None
    return {
        "average": round(
            sum(candle["close"] for candle in selected) / len(selected),
            2,
        ),
        "low": round(min(candle["low"] for candle in selected), 2),
        "high": round(max(candle["high"] for candle in selected), 2),
        "samples": len(selected),
        "starts_at": selected[0]["timestamp"],
        "ends_at": selected[-1]["timestamp"],
    }


class KrakenMarketData:
    def __init__(self, session=None, cache_seconds=CONTROL_MARKET_CACHE_SECONDS):
        self.session = session or requests.Session()
        self.cache_seconds = cache_seconds
        self._lock = threading.Lock()
        self._cached_at = 0.0
        self._cached = None

    def _get_json(self, url, params=None):
        response = self.session.get(
            url,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()

    def _current_price(self):
        ticker = _pair_result(_result_payload(self._get_json(KRAKEN_TICKER_URL)))
        return float(ticker["c"][0])

    def _ohlc(self, interval):
        return parse_ohlc_candles(self._get_json(
            KRAKEN_OHLC_ENDPOINT,
            params={"pair": KRAKEN_PAIR, "interval": interval},
        ))

    def snapshot(self, force=False):
        with self._lock:
            now_monotonic = time.monotonic()
            if (
                not force
                and self._cached is not None
                and now_monotonic - self._cached_at < self.cache_seconds
            ):
                return self._cached

            captured_at = utc_now()
            try:
                current_price = self._current_price()
                hourly = self._ohlc(60)
                daily = self._ohlc(1440)
                periods = {
                    "24h": summarize_candles(
                        hourly,
                        captured_at - timedelta(hours=24),
                    ),
                    "7d": summarize_candles(
                        daily,
                        captured_at - timedelta(days=7),
                    ),
                    "50d": summarize_candles(
                        daily,
                        captured_at - timedelta(days=50),
                    ),
                    "200d": summarize_candles(
                        daily,
                        captured_at - timedelta(days=200),
                    ),
                }
                snapshot = {
                    "captured_at": captured_at.isoformat(),
                    "current_price": round(current_price, 2),
                    "periods": periods,
                    "history": {
                        "hourly": hourly[-168:],
                        "daily": daily[-200:],
                    },
                    "source": "Kraken public market data",
                    "stale": False,
                    "error": None,
                }
                self._cached = snapshot
                self._cached_at = now_monotonic
                return snapshot
            except Exception as exc:
                if self._cached is not None:
                    stale = dict(self._cached)
                    stale.update({"stale": True, "error": str(exc)})
                    return stale
                return {
                    "captured_at": captured_at.isoformat(),
                    "current_price": None,
                    "periods": {},
                    "history": {"hourly": [], "daily": []},
                    "source": "Kraken public market data",
                    "stale": True,
                    "error": str(exc),
                }


def host_is_loopback(host):
    normalized = str(host or "").strip().lower()
    if normalized in {"localhost", "ip6-localhost"}:
        return True
    try:
        return ipaddress.ip_address(normalized).is_loopback
    except ValueError:
        return False


def append_audit_event(event):
    path = os.path.abspath(os.path.expanduser(CONTROL_AUDIT_FILE))
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    payload = {"timestamp": utc_now_iso(), **event}
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, separators=(",", ":")) + "\n")


def build_status_payload(market_data):
    control = load_control_state_fail_safe(CONTROL_FILE)
    bot_status = read_json_object(STATUS_FILE)
    bot_state = read_json_object(STATE_FILE)
    open_buy_orders = list((bot_state.get("open_buy_orders") or {}).values())
    open_sell_orders = list((bot_state.get("open_sell_orders") or {}).values())
    return {
        "generated_at": utc_now_iso(),
        "control": control_status(control),
        "market": market_data.snapshot(),
        "bot": bot_status,
        "orders": {
            "open_buy_count": len(open_buy_orders),
            "open_sell_count": len(open_sell_orders),
            "open_buys": [{
                "price": order.get("price"),
                "volume": order.get("volume"),
                "buy_source": order.get("buy_source"),
                "grid_slot": order.get("grid_slot"),
                "placed_at": order.get("placed_at"),
            } for order in open_buy_orders],
            "open_sells": [{
                "buy_price": order.get("buy_price"),
                "sell_price": order.get("sell_price"),
                "volume": order.get("volume"),
                "buy_source": order.get("buy_source"),
                "grid_slot": order.get("grid_slot"),
                "placed_at": order.get("placed_at"),
            } for order in open_sell_orders],
        },
    }


class ControlPlaneServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, *, token, market_data):
        super().__init__(address, handler)
        self.control_token = token
        self.market_data = market_data
        self.control_write_lock = threading.Lock()


class ControlPlaneHandler(BaseHTTPRequestHandler):
    server_version = "RangeGridControl/1.0"
    max_request_bytes = 64 * 1024

    def log_message(self, format_string, *args):
        print(
            f"{self.address_string()} - "
            f"{format_string % args}",
            flush=True,
        )

    def _security_headers(self):
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; connect-src 'self'; "
            "img-src 'self' data:",
        )
        self.send_header("Cache-Control", "no-store")

    def _send_json(self, status, payload):
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._security_headers()
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self):
        try:
            body = HTML_FILE.read_bytes()
        except OSError as exc:
            self._send_json(500, {"error": f"control page unavailable: {exc}"})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self._security_headers()
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self):
        expected = self.server.control_token
        if not expected:
            return True
        authorization = self.headers.get("Authorization", "")
        supplied = ""
        if authorization.lower().startswith("bearer "):
            supplied = authorization[7:].strip()
        if not supplied:
            supplied = self.headers.get("X-Control-Token", "").strip()
        return bool(supplied) and hmac.compare_digest(supplied, expected)

    def _require_auth(self):
        if self._authorized():
            return True
        self._send_json(401, {"error": "authentication required"})
        return False

    def _read_json(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ControlStateError("invalid Content-Length") from exc
        if length <= 0 or length > self.max_request_bytes:
            raise ControlStateError("request body is empty or too large")
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ControlStateError("request body must be valid JSON") from exc
        if not isinstance(payload, dict):
            raise ControlStateError("request body must be a JSON object")
        return payload

    def do_GET(self):
        path = urlparse(self.path).path
        if path in {"/", "/index.html"}:
            self._send_html()
            return
        if path == "/health":
            self._send_json(200, {"status": "ok", "timestamp": utc_now_iso()})
            return
        if path == "/api/status":
            if not self._require_auth():
                return
            self._send_json(
                200,
                build_status_payload(self.server.market_data),
            )
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        path = urlparse(self.path).path
        if path not in {"/api/control", "/api/hold"}:
            self._send_json(404, {"error": "not found"})
            return
        if not self._require_auth():
            return
        try:
            payload = self._read_json()
            if path == "/api/hold":
                payload = {
                    "buying_paused": payload.get("buying_paused", True),
                    "expected_revision": payload.get("expected_revision"),
                }

            expected_revision = payload.pop("expected_revision", None)
            with self.server.control_write_lock:
                current = load_control_state_fail_safe(CONTROL_FILE)
                if current.get("load_error"):
                    current["revision"] = 0
                if (
                    expected_revision is not None
                    and int(expected_revision) != int(current.get("revision", 0))
                ):
                    self._send_json(409, {
                        "error": "control state changed; refresh and try again",
                        "control": control_status(current),
                    })
                    return
                updated = merge_control_update(
                    current,
                    payload,
                    updated_by=self.client_address[0],
                )
                save_control_state(CONTROL_FILE, updated)
                append_audit_event({
                    "event": "CONTROL_UPDATED",
                    "remote_address": self.client_address[0],
                    "revision": updated["revision"],
                    "buying_paused": updated["buying_paused"],
                    "cancel_open_buys_on_hold": updated[
                        "cancel_open_buys_on_hold"
                    ],
                    "manual_targets_enabled": updated[
                        "manual_targets_enabled"
                    ],
                    "buy_targets": updated["buy_targets"],
                    "active_target_count": len([
                        target
                        for target in updated["buy_targets"]
                        if target["enabled"]
                    ]),
                })
            self._send_json(200, {"control": control_status(updated)})
        except (ControlStateError, TypeError, ValueError) as exc:
            self._send_json(400, {"error": str(exc)})
        except Exception as exc:
            self._send_json(500, {"error": str(exc)})


def main():
    if not host_is_loopback(CONTROL_HOST) and not CONTROL_TOKEN:
        raise RuntimeError(
            "RANGE_GRID_CONTROL_TOKEN is required when the control plane "
            "binds to a non-loopback address"
        )
    server = ControlPlaneServer(
        (CONTROL_HOST, CONTROL_PORT),
        ControlPlaneHandler,
        token=CONTROL_TOKEN,
        market_data=KrakenMarketData(),
    )
    print(json.dumps({
        "status": "starting",
        "url": f"http://{CONTROL_HOST}:{CONTROL_PORT}/",
        "control_file": os.path.abspath(CONTROL_FILE),
        "status_file": os.path.abspath(STATUS_FILE),
        "authentication_required": bool(CONTROL_TOKEN),
    }, indent=2), flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
