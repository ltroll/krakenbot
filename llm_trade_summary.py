"""
trade_llm_summary.py

Purpose:
Generate a structured LLM summary of a trade event and forward it to Discord.

Dependencies:
- llama3.py must expose: ask_ollama_stream(prompt: str, model="llama3") -> str
- discord_notify.py must expose:
  send_discord_message(message_type="general", message="...")
"""

from llama3 import ask_ollama_stream
from discord_notify import send_discord_message


# -------------------------------------------------
# Prompt Builder
# -------------------------------------------------


def build_trade_prompt(
    side,
    price,
    volume,
    trade_value,
    fee,
    portfolio_btc,
    portfolio_usd,
    current_allocation,
    signal,
    adjusted_target,
    trigger_reason,
    prev_alloc=None,
    new_alloc=None,
    vol_scaled=None,
    trend_filter=None,
):
    """
    Build structured prompt for LLM trade summarization
    """

    return f"""
You are an automated portfolio risk reporting assistant.

Summarize this crypto trading bot execution for Discord.

Rules:
- Maximum 4 sentences
- No emojis
- No speculation
- No trading advice
- Explain WHY the trade happened
- Mention allocation movement if available
- Mention volatility scaling if relevant
- Mention trend filter impact if active

Trade Data

Action: {side}
Price: {price}
Volume BTC: {volume}
Trade Value USD: {trade_value}
Fee USD: {fee}

Portfolio Snapshot

BTC Holdings: {portfolio_btc}
USD Holdings: {portfolio_usd}
BTC Allocation Before Trade: {current_allocation}

Signal Context

Composite Signal Strength: {signal}
Adjusted Target Allocation: {adjusted_target}

Allocation Movement

Previous Allocation: {prev_alloc}
New Allocation: {new_alloc}

Risk Controls

Volatility Scaling Applied: {vol_scaled}
Trend Filter Active: {trend_filter}

Trigger Reason: {trigger_reason}

Output Format:

TRADE SUMMARY:
<summary here>
"""


# -------------------------------------------------
# Public Function
# -------------------------------------------------


def send_trade_summary(
    side,
    price,
    volume,
    trade_value,
    fee,
    portfolio_btc,
    portfolio_usd,
    current_allocation,
    signal,
    adjusted_target,
    trigger_reason,
    prev_alloc=None,
    new_alloc=None,
    vol_scaled=None,
    trend_filter=None,
):
    """
    Generate summary via LLM and send to Discord
    """

    prompt = build_trade_prompt(
        side,
        price,
        volume,
        trade_value,
        fee,
        portfolio_btc,
        portfolio_usd,
        current_allocation,
        signal,
        adjusted_target,
        trigger_reason,
        prev_alloc,
        new_alloc,
        vol_scaled,
        trend_filter,
    )

    summary = ask_ollama_stream(prompt)

    send_discord_message(
        message_type="trade",
        message=summary
    )

    return summary

