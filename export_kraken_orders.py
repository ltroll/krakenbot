#!/usr/bin/env python3

import argparse
import base64
import csv
import hashlib
import hmac
import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv


DEFAULT_OUTPUT = "kraken_orders.csv"
CLOSED_ORDERS_PAGE_SIZE = 50
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))


CSV_FIELDS = [
    "txid",
    "source",
    "status",
    "opentm",
    "opentm_iso",
    "closetm",
    "closetm_iso",
    "starttm",
    "starttm_iso",
    "expiretm",
    "expiretm_iso",
    "descr_pair",
    "descr_type",
    "descr_ordertype",
    "descr_price",
    "descr_price2",
    "descr_leverage",
    "descr_order",
    "vol",
    "vol_exec",
    "cost",
    "fee",
    "price",
    "stopprice",
    "limitprice",
    "misc",
    "oflags",
    "userref",
    "refid",
    "reason",
    "trades",
    "raw_json",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download Kraken open and closed spot orders to a CSV file."
    )
    parser.add_argument(
        "-o",
        "--output",
        default=DEFAULT_OUTPUT,
        help=f"CSV output path. Defaults to {DEFAULT_OUTPUT}.",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to a dotenv file with KRAKEN_API_KEY and KRAKEN_API_SECRET.",
    )
    parser.add_argument(
        "--api-url",
        default=None,
        help="Override KRAKEN_API_URL. Defaults to env value or https://api.kraken.com.",
    )
    parser.add_argument(
        "--include-trades",
        action="store_true",
        help="Ask Kraken to include trade IDs attached to each order.",
    )
    parser.add_argument(
        "--closed-only",
        action="store_true",
        help="Export only closed orders.",
    )
    parser.add_argument(
        "--open-only",
        action="store_true",
        help="Export only open orders.",
    )
    return parser.parse_args()


def unix_to_iso(value):
    if value in (None, "", 0, "0"):
        return ""
    try:
        return datetime.fromtimestamp(float(value), timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


class KrakenClient:
    def __init__(self, api_url=None):
        self.api_url = (api_url or os.getenv(
            "KRAKEN_API_URL",
            "https://api.kraken.com",
        )).rstrip("/")
        self.api_key = require_env("KRAKEN_API_KEY")
        self.api_secret = require_env("KRAKEN_API_SECRET")
        self.last_nonce = 0

    def next_nonce(self):
        wall_nonce = int(time.time() * 1000)
        self.last_nonce = max(wall_nonce, self.last_nonce + 1)
        return str(self.last_nonce)

    def signature(self, endpoint, data):
        postdata = urlencode(data)
        encoded = (str(data["nonce"]) + postdata).encode()
        message = endpoint.encode() + hashlib.sha256(encoded).digest()
        mac = hmac.new(
            base64.b64decode(self.api_secret),
            message,
            hashlib.sha512,
        )
        return base64.b64encode(mac.digest()).decode()

    def private(self, endpoint, data=None):
        path = f"/0/private/{endpoint}"
        payload = dict(data or {})
        payload["nonce"] = self.next_nonce()
        headers = {
            "API-Key": self.api_key,
            "API-Sign": self.signature(path, payload),
        }
        response = requests.post(
            self.api_url + path,
            headers=headers,
            data=payload,
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(
                f"Unexpected response from Kraken {endpoint}: {payload!r}"
            )

        errors = payload.get("error") or []
        if errors:
            raise RuntimeError(f"Kraken {endpoint} error: {errors}")

        return payload.get("result", {})


def query_private(api, endpoint, data=None):
    result = api.private(endpoint, data or {})
    if not isinstance(result, dict):
        raise RuntimeError(f"Unexpected result from Kraken {endpoint}: {result!r}")
    errors = result.get("error") or []
    if errors:
        raise RuntimeError(f"Kraken {endpoint} error: {errors}")
    return result


def get_open_orders(api, include_trades=False):
    data = {"trades": "true"} if include_trades else {}
    result = query_private(api, "OpenOrders", data)
    return result.get("open", {}) or {}


def get_closed_orders(api, include_trades=False):
    all_orders = {}
    offset = 0
    total = None

    while True:
        data = {"ofs": offset}
        if include_trades:
            data["trades"] = "true"

        result = query_private(api, "ClosedOrders", data)
        orders = result.get("closed", {}) or {}
        count = int(result.get("count", 0) or 0)
        total = count if total is None else total

        if not orders:
            break

        all_orders.update(orders)
        offset += CLOSED_ORDERS_PAGE_SIZE

        print(
            f"Downloaded {len(all_orders)} of {total or '?'} closed orders...",
            file=sys.stderr,
        )

        if total is not None and len(all_orders) >= total:
            break

        # Keep private API calls polite and reduce the chance of rate-limit errors.
        time.sleep(1)

    return all_orders


def csv_row(txid, source, order):
    descr = order.get("descr") or {}
    row = {
        "txid": txid,
        "source": source,
        "status": order.get("status", ""),
        "opentm": order.get("opentm", ""),
        "opentm_iso": unix_to_iso(order.get("opentm")),
        "closetm": order.get("closetm", ""),
        "closetm_iso": unix_to_iso(order.get("closetm")),
        "starttm": order.get("starttm", ""),
        "starttm_iso": unix_to_iso(order.get("starttm")),
        "expiretm": order.get("expiretm", ""),
        "expiretm_iso": unix_to_iso(order.get("expiretm")),
        "descr_pair": descr.get("pair", ""),
        "descr_type": descr.get("type", ""),
        "descr_ordertype": descr.get("ordertype", ""),
        "descr_price": descr.get("price", ""),
        "descr_price2": descr.get("price2", ""),
        "descr_leverage": descr.get("leverage", ""),
        "descr_order": descr.get("order", ""),
        "vol": order.get("vol", ""),
        "vol_exec": order.get("vol_exec", ""),
        "cost": order.get("cost", ""),
        "fee": order.get("fee", ""),
        "price": order.get("price", ""),
        "stopprice": order.get("stopprice", ""),
        "limitprice": order.get("limitprice", ""),
        "misc": order.get("misc", ""),
        "oflags": order.get("oflags", ""),
        "userref": order.get("userref", ""),
        "refid": order.get("refid", ""),
        "reason": order.get("reason", ""),
        "trades": json.dumps(order.get("trades", []), sort_keys=True),
        "raw_json": json.dumps(order, sort_keys=True),
    }
    return row


def write_csv(path, open_orders, closed_orders):
    rows = []
    rows.extend(
        csv_row(txid, "open", order)
        for txid, order in sorted(open_orders.items())
    )
    rows.extend(
        csv_row(txid, "closed", order)
        for txid, order in sorted(
            closed_orders.items(),
            key=lambda item: float(item[1].get("opentm") or 0),
            reverse=True,
        )
    )

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)


def main():
    args = parse_args()
    if args.open_only and args.closed_only:
        raise RuntimeError("Use either --open-only or --closed-only, not both.")

    load_dotenv(dotenv_path=args.env_file, override=True)
    api = KrakenClient(args.api_url)

    open_orders = {}
    closed_orders = {}

    if not args.closed_only:
        open_orders = get_open_orders(api, args.include_trades)
        print(f"Downloaded {len(open_orders)} open orders.", file=sys.stderr)

    if not args.open_only:
        closed_orders = get_closed_orders(api, args.include_trades)

    total = write_csv(args.output, open_orders, closed_orders)
    print(
        f"Wrote {total} orders to {args.output} "
        f"({len(open_orders)} open, {len(closed_orders)} closed)."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
