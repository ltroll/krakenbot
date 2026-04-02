#!/usr/bin/env python3

import os
import krakenex
from dotenv import load_dotenv

load_dotenv()

PAIR = "XBTUSD"


def sell_all_btc():

    api_key = os.getenv("KRAKEN_API_KEY")
    api_secret = os.getenv("KRAKEN_API_SECRET")
    api_url = os.getenv("KRAKEN_API_URL")

    if not api_key or not api_secret:
        raise Exception("Missing KRAKEN_API_KEY or KRAKEN_API_SECRET")

    api = krakenex.API()
    api.key = api_key
    api.secret = api_secret

    if api_url:
        api.uri = api_url

    # Get balances
    balance = api.query_private("Balance")

    if balance["error"]:
        raise Exception(balance["error"])

    btc_balance = float(balance["result"].get("XXBT", 0))

    print(f"BTC Balance: {btc_balance}")

    if btc_balance <= 0:
        print("No BTC to sell.")
        return

    confirm = input("Type SELL to liquidate all BTC: ")

    if confirm != "SELL":
        print("Aborted.")
        return

    print("Submitting market sell order...")

    order = api.query_private(
        "AddOrder",
        {
            "pair": PAIR,
            "type": "sell",
            "ordertype": "market",
            "volume": btc_balance
        }
    )

    print("Order response:")
    print(order)


if __name__ == "__main__":
    sell_all_btc()
