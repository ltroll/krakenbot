#!/usr/bin/env python3

import os
import time
import base64
import hashlib
import hmac
import urllib.parse
import requests
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

API_KEY = os.getenv("KRAKEN_API_KEY")
API_SECRET = os.getenv("KRAKEN_API_SECRET")
API_URL = os.getenv("KRAKEN_API_URL")
TICKER_URL = os.getenv("KRAKEN_TICKER_URL")


def kraken_signature(urlpath, data, secret):
    """
    Create Kraken API signature
    """
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()

    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    mac = hmac.new(
        base64.b64decode(secret),
        message,
        hashlib.sha512
    )

    return base64.b64encode(mac.digest()).decode()


def get_balances():
    """
    Fetch account balances from Kraken
    """
    endpoint = "/0/private/Balance"
    url = API_URL + endpoint

    nonce = str(int(time.time() * 1000))

    data = {
        "nonce": nonce
    }

    headers = {
        "API-Key": API_KEY,
        "API-Sign": kraken_signature(endpoint, data, API_SECRET)
    }

    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()

    result = response.json()

    if result["error"]:
        raise Exception(result["error"])

    return result["result"]


def get_btc_price():
    """
    Fetch BTC/USD price
    """
    response = requests.get(TICKER_URL)
    response.raise_for_status()

    data = response.json()

    price = float(data["result"]["XXBTZUSD"]["c"][0])

    return price


def main():
    balances = get_balances()

    usd_balance = float(balances.get("ZUSD", 0))
    btc_balance = float(balances.get("XXBT", 0))

    btc_price = get_btc_price()

    btc_value_usd = btc_balance * btc_price
    total_value = usd_balance + btc_value_usd

    print("\n📊 Kraken Account Value Summary")
    print("-----------------------------")
    print(f"USD Balance: ${usd_balance:,.2f}")
    print(f"BTC Balance: {btc_balance:.6f} BTC")
    print(f"BTC Price:   ${btc_price:,.2f}")
    print(f"BTC Value:   ${btc_value_usd:,.2f}")
    print("-----------------------------")
    print(f"💰 Total Value: ${total_value:,.2f}\n")


if __name__ == "__main__":
    main()
