#!/usr/bin/env python3

import base64
import hashlib
import hmac
import json
import os
import sys
import time
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv

load_dotenv()

KRAKEN_API_URL = os.getenv("KRAKEN_API_URL", "https://api.kraken.com")
KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_PAIR = os.getenv("KRAKEN_PAIR", "XXBTZUSD")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))


def print_step(label, ok, detail):
    status = "OK" if ok else "FAIL"
    print(f"[{status}] {label}: {detail}")


def public_get(path, params=None):
    response = requests.get(
        KRAKEN_API_URL.rstrip("/") + path,
        params=params or {},
        timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(payload["error"])
    return payload


def next_nonce():
    return str(int(time.time() * 1000))


def kraken_signature(endpoint, postdata, nonce):
    encoded = (str(nonce) + postdata).encode()
    message = endpoint.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(
        base64.b64decode(KRAKEN_API_SECRET),
        message,
        hashlib.sha512
    )
    return base64.b64encode(mac.digest()).decode()


def private_post(endpoint, data=None):
    if not KRAKEN_API_KEY or not KRAKEN_API_SECRET:
        raise RuntimeError("Missing KRAKEN_API_KEY or KRAKEN_API_SECRET")

    payload = dict(data or {})
    payload["nonce"] = next_nonce()
    postdata = urlencode(payload)
    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": kraken_signature(endpoint, postdata, payload["nonce"]),
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(
        KRAKEN_API_URL.rstrip("/") + endpoint,
        headers=headers,
        data=postdata,
        timeout=REQUEST_TIMEOUT
    )
    response.raise_for_status()
    result = response.json()
    if result.get("error"):
        raise RuntimeError(result["error"])
    return result


def main():
    failures = 0

    try:
        system_status = public_get("/0/public/SystemStatus")
        status = system_status["result"].get("status", "unknown")
        print_step("Public SystemStatus", True, status)
    except Exception as e:
        failures += 1
        print_step("Public SystemStatus", False, str(e))

    try:
        ticker = public_get("/0/public/Ticker", {"pair": KRAKEN_PAIR})
        first = next(iter(ticker["result"].values()))
        price = first["c"][0]
        print_step("Public Ticker", True, f"{KRAKEN_PAIR} last={price}")
    except Exception as e:
        failures += 1
        print_step("Public Ticker", False, str(e))

    try:
        pair_info = public_get("/0/public/AssetPairs", {"pair": KRAKEN_PAIR})
        first = next(iter(pair_info["result"].values()))
        print_step(
            "Public AssetPairs",
            True,
            f"pair_decimals={first.get('pair_decimals')} lot_decimals={first.get('lot_decimals')}"
        )
    except Exception as e:
        failures += 1
        print_step("Public AssetPairs", False, str(e))

    if KRAKEN_API_KEY and KRAKEN_API_SECRET:
        try:
            balance = private_post("/0/private/Balance")
            assets = sorted(balance.get("result", {}).keys())
            preview = ", ".join(assets[:5]) if assets else "<none>"
            print_step("Private Balance", True, f"assets={preview}")
        except Exception as e:
            failures += 1
            print_step("Private Balance", False, str(e))

        open_order_txid = None
        open_order_txids = []
        try:
            open_orders = private_post("/0/private/OpenOrders")
            open_order_map = open_orders.get("result", {}).get("open", {})
            count = len(open_order_map)
            open_order_txid = next(iter(open_order_map.keys()), None)
            open_order_txids = list(open_order_map.keys())
            print_step("Private OpenOrders", True, f"open_orders={count}")
        except Exception as e:
            failures += 1
            print_step("Private OpenOrders", False, str(e))

        if open_order_txid:
            try:
                order_info = private_post(
                    "/0/private/QueryOrders",
                    {"txid": open_order_txid}
                )
                found = open_order_txid in order_info.get("result", {})
                detail = (
                    f"txid={open_order_txid} found={found}"
                )
                print_step("Private QueryOrders", True, detail)
            except Exception as e:
                failures += 1
                print_step("Private QueryOrders", False, str(e))
        else:
            print_step(
                "Private QueryOrders",
                True,
                "skipped because no open orders were returned"
            )

        if len(open_order_txids) >= 2:
            batch_txids = open_order_txids[:2]
            try:
                order_info = private_post(
                    "/0/private/QueryOrders",
                    {"txid": ",".join(batch_txids)}
                )
                found = sum(
                    1 for txid in batch_txids
                    if txid in order_info.get("result", {})
                )
                detail = f"txids={len(batch_txids)} found={found}"
                print_step("Private QueryOrders Batch", True, detail)
            except Exception as e:
                failures += 1
                print_step("Private QueryOrders Batch", False, str(e))
        else:
            print_step(
                "Private QueryOrders Batch",
                True,
                "skipped because fewer than two open orders were returned"
            )
    else:
        print_step("Private API", False, "missing credentials")
        failures += 1

    summary = {"failures": failures, "pair": KRAKEN_PAIR}
    print(json.dumps(summary))
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
