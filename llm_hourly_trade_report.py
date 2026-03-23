#!/usr/bin/env python3

import os
import time
import requests
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import hashlib
import hmac
import base64
import urllib.parse

from llama3 import ask_ollama_stream
from discord_notify import send_discord_message

load_dotenv()


KRAKEN_API_KEY = os.getenv("KRAKEN_API_KEY")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET")
KRAKEN_API_URL = os.getenv("KRAKEN_API_URL")


###########################################
# Kraken API Auth Helper
###########################################
def kraken_request(uri_path, data):
    url = KRAKEN_API_URL + uri_path

    nonce = str(int(time.time() * 1000))
    data["nonce"] = nonce

    postdata = urllib.parse.urlencode(data)
    encoded = (nonce + postdata).encode()

    message = uri_path.encode() + hashlib.sha256(encoded).digest()

    signature = hmac.new(
        base64.b64decode(KRAKEN_API_SECRET),
        message,
        hashlib.sha512
    )

    sigdigest = base64.b64encode(signature.digest())

    headers = {
        "API-Key": KRAKEN_API_KEY,
        "API-Sign": sigdigest.decode()
    }

    response = requests.post(url, headers=headers, data=data)

    return response.json()


###########################################
# Fetch Trades (Last Hour)
###########################################
def get_recent_trades():
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)

    result = kraken_request("/0/private/TradesHistory", {
        "ofs": 0
    })

    if "error" in result and result["error"]:
        raise Exception(result["error"])

    trades = result["result"]["trades"]

    recent_trades = []

    for txid, trade in trades.items():
        trade_time = datetime.fromtimestamp(trade["time"], timezone.utc)

        if trade_time >= one_hour_ago:
            trade["txid"] = txid
            recent_trades.append(trade)

    return recent_trades


###########################################
# Build LLM Prompt
###########################################
def build_prompt(trades):

    summary_lines = []

    total_volume = 0
    buys = 0
    sells = 0

    for t in trades:
        pair = t["pair"]
        side = t["type"]
        price = float(t["price"])
        volume = float(t["vol"])

        summary_lines.append(
            f"{side.upper()} {pair} @ {price} volume={volume}"
        )

        total_volume += volume

        if side == "buy":
            buys += 1
        else:
            sells += 1

    trade_summary = "\n".join(summary_lines)

    prompt = f"""
You are a crypto trading performance analyst.

Analyze the following Kraken trades from the last hour.

Trades:

{trade_summary}

Provide:

- performance evaluation
- risk observations
- trade execution quality
- suggestions if improvements exist

Keep response concise and actionable.
"""

    return prompt


###########################################
# Main Workflow
###########################################
def run_report():

    trades = get_recent_trades()

    if not trades:
        send_discord_message(
            message_type="trading",
            message="Hourly Kraken Report:\nNothing to report."
        )
        return

    prompt = build_prompt(trades)

    llm_report = ask_ollama_stream(prompt)

    message = f"""
📊 Hourly Kraken Trade Report

Trades executed: {len(trades)}

LLM Analysis:

{llm_report}
"""

    send_discord_message(
        message_type="trading",
        message=message
    )


###########################################
# Entry
###########################################
if __name__ == "__main__":
    run_report()
