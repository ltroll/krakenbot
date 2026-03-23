import streamlit as st
import sqlite3
import pandas as pd
import time
from datetime import datetime, timedelta

# --- Custom CSS for dark mode ---
st.markdown(
    """
    <style>
    /* Page background */
    .stApp {
        background-color: #0E1117;
        color: #F0F0F0;
    }

    /* Metric values */
    .stMetricValue {
        color: #F0F0F0 !important;
    }

    /* Headers */
    h1, h2, h3, h4, h5, h6 {
        color: #F0F0F0;
    }

    /* Warning text */
    .stWarning {
        background-color: #2E2E2E !important;
        color: #FFC107 !important;
    }

    /* Line charts */
    .element-container svg {
        background-color: #0E1117 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- Database connection ---
conn = sqlite3.connect("trading.db", check_same_thread=False)

st.title("Trading Bot Dashboard - Dev")

# --- Placeholders for dynamic updates ---
portfolio_metric = st.empty()
btc_metric = st.empty()
usd_metric = st.empty()
realized_metric = st.empty()
last_updated_text = st.empty()
warning_text = st.empty()

profit_chart = st.empty()
unrealized_chart = st.empty()
portfolio_chart = st.empty()

# --- Auto-refresh loop ---
REFRESH_INTERVAL = 60  # seconds
STALE_THRESHOLD_MINUTES = 5  # warn if no new trade in this many minutes

while True:
    df = pd.read_sql("SELECT * FROM trades ORDER BY timestamp ASC", conn)

    if df.empty:
        st.warning("No trade data available yet.")
    else:
        latest = df.iloc[-1]
        portfolio_value = latest["usd_balance"] + latest["btc_balance"] * latest["price"]

        # --- Update metrics ---
        portfolio_metric.metric("Portfolio Value", f"${portfolio_value:,.2f}")
        btc_metric.metric("BTC Balance", f"{latest['btc_balance']:.6f} BTC")
        usd_metric.metric("USD Balance", f"${latest['usd_balance']:.2f}")

        realized = df["realized_pnl"].sum()
        realized_metric.metric("Realized Profit", f"${realized:.2f}")

        # --- Update charts ---
        df["portfolio_value"] = df["usd_balance"] + df["btc_balance"] * df["price"]

        profit_chart.line_chart(df[["realized_pnl"]].cumsum().rename(columns={"realized_pnl": "Cumulative PnL"}))
        unrealized_chart.line_chart(df["unrealized_pnl"].rename("Unrealized PnL"))
        portfolio_chart.line_chart(df["portfolio_value"].rename("Portfolio Value"))

        # --- Last updated timestamp ---
        now = datetime.now()
        last_updated_text.text(f"Last updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")

        # --- Check for stale trades ---
        last_trade_time = pd.to_datetime(latest["timestamp"])
        if now - last_trade_time > timedelta(minutes=STALE_THRESHOLD_MINUTES):
            warning_text.warning("⚠️ No new trades in the last 5 minutes!")
        else:
            warning_text.empty()

    time.sleep(REFRESH_INTERVAL)
