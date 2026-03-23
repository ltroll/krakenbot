#!/usr/bin/env python3
import os
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
import krakenex
from dotenv import load_dotenv
import statistics
import time
from pnl_tracker import PnLTracker
from llm_trade_summary import send_trade_summary

load_dotenv()

CONFIG_FILE = "bot_config.json"
SENTIMENT_FILE = "llm_signal.json"
STATE_FILE = "last_state.json"

logging.basicConfig(
    filename="trade_log.jsonl",
    level=logging.INFO,
    format="%(message)s"
)

def utc_ts():
    return datetime.now(timezone.utc).isoformat()


class KrakenTrader:

    def __init__(self):

        self.config = self.load_json(CONFIG_FILE)
        self.validate_env()

        self.pnl = PnLTracker()

        self.api = krakenex.API()
        self.api.key = os.getenv("KRAKEN_API_KEY")
        self.api.secret = os.getenv("KRAKEN_API_SECRET")
        self.api.uri = os.getenv("KRAKEN_API_URL")

        self.ticker_url = os.getenv("KRAKEN_TICKER_URL")

        self.last_state = self.load_state()

    def log_event(self, event, message="", **kwargs):
        log_entry = {
            "ts": utc_ts(),
            "event": event,
            "message": message,
            **kwargs
        }
        logging.info(json.dumps(log_entry))
        print(event, message)

    def save_state(self):
        with open(STATE_FILE, "w") as f:
            json.dump(self.last_state, f)

    def kraken_request(self, endpoint, data=None):

        if data is None:
            data = {}

        self.log_event("KRAKEN_REQUEST", endpoint=endpoint, payload=data)

        response = self.api.query_private(endpoint, data)

        self.log_event("KRAKEN_RESPONSE", endpoint=endpoint, response=response)

        if response.get("error"):
            self.log_event("KRAKEN_ERROR", endpoint=endpoint, error=response["error"])

        return response

    def validate_env(self):

        required = [
            "KRAKEN_API_KEY",
            "KRAKEN_API_SECRET",
            "KRAKEN_API_URL",
            "KRAKEN_TICKER_URL"
        ]

        missing = [v for v in required if not os.getenv(v)]

        if missing:
            raise Exception(f"Missing environment variables: {missing}")

    def load_json(self, path):

        with open(path) as f:
            return json.load(f)

    def load_state(self):

        try:
            with open(STATE_FILE) as f:
                return json.load(f)

        except:

            return {
                "last_signal": None,
                "last_trade_time": None,
                "entry_price": None
            }

    def get_btc_price(self):

        r = requests.get(self.ticker_url).json()

        pair = list(r["result"].keys())[0]

        return float(r["result"][pair]["c"][0])

    def get_current_position(self):

        balance = self.kraken_request("Balance")

        return float(balance["result"].get("XXBT", 0))

    def load_sentiment(self):

        sentiment = requests.get(os.getenv("LLM_SIGNAL_URL")).json()

        if "smoothed_risk_multiplier" not in sentiment:

            sentiment["smoothed_risk_multiplier"] = sentiment.get(
                "risk_multiplier", 1.0
            )

        return sentiment

    def compute_signal(self, s):

        composite = (
            s["btc_sentiment"]
            + s["macro_tightening_bias"]
            - s["regulatory_risk"]
        )

        composite = max(min(composite, 1), -1)

        exposure = (composite + 1) / 2

        exposure *= s.get("smoothed_risk_multiplier", 1.0)

        return exposure

    def should_trade(self, signal):

        last_signal = self.last_state.get("last_signal")

        if last_signal is None:
            return True

        diff = abs(signal - last_signal)

        if diff < self.config.get("signal_change_threshold", 0.05):

            self.log_event(
                "TRADE_SKIPPED",
                reason="signal_threshold",
                diff=diff
            )

            return False

        last_trade_time = self.last_state.get("last_trade_time")

        if last_trade_time:

            elapsed = datetime.now(timezone.utc) - datetime.fromisoformat(last_trade_time)

            if elapsed < timedelta(
                minutes=self.config.get("max_trade_frequency_minutes", 3)
            ):

                self.log_event("TRADE_SKIPPED", reason="cooldown")

                return False

        return True

    def get_portfolio(self, price):

        balance = self.kraken_request("Balance")["result"]

        btc = float(balance.get("XXBT", 0))

        usd = float(balance.get("ZUSD", 0))

        btc_value = btc * price

        total_value = btc_value + usd

        return {
            "btc": btc,
            "usd": usd,
            "btc_value": btc_value,
            "total_value": total_value
        }

    def enforce_profit_logic(self, price):

        entry_price = self.last_state.get("entry_price")

        if entry_price is None:
            return None

        pnl_pct = (price - entry_price) / entry_price

        if pnl_pct >= self.config.get("profit_target_pct", 0.007):

            self.log_event(
                "EXIT_SIGNAL",
                reason="profit_target",
                pnl_pct=pnl_pct
            )

            return 0

        if pnl_pct <= -self.config.get("stop_loss_pct", 0.004):

            self.log_event(
                "EXIT_SIGNAL",
                reason="stop_loss",
                pnl_pct=pnl_pct
            )

            return 0

        return None

    def run(self):

        price = self.get_btc_price()

        sentiment = self.load_sentiment()

        signal = self.compute_signal(sentiment)

        portfolio = self.get_portfolio(price)

        override_signal = self.enforce_profit_logic(price)

        # --- Forced exit immediately if profit target or stop loss ---
        if override_signal == 0 and portfolio["btc"] > 0:

            self.log_event("FORCED_EXIT", reason="profit_or_stop")

            side = "sell"
            volume = portfolio["btc"]

            resp = self.kraken_request(
                "AddOrder",
                {
                    "pair": self.config["pair"],
                    "type": side,
                    "ordertype": "market",
                    "volume": volume
                }
            )

            fee = volume * price * self.config.get("taker_fee", 0.0026)

            realized_pnl = self.pnl.record_trade(
                side,
                price,
                volume,
                fee,
                portfolio["btc"],
                portfolio["usd"],
                self.last_state.get("entry_price"),
                price
            )

            self.log_event(
                "TRADE_EXECUTED",
                side=side,
                price=price,
                volume=volume,
                fee=fee,
                realized_pnl=realized_pnl
            )

            self.last_state["entry_price"] = None
            self.last_state["last_trade_time"] = utc_ts()
            self.last_state["last_signal"] = 0
            self.save_state()
            return

        # Update signal if override exists but not full exit
        if override_signal is not None:
            signal = override_signal

        if not self.should_trade(signal):
            return

        total_value = portfolio["total_value"]
        btc_value = portfolio["btc_value"]

        if total_value == 0:
            self.log_event("ERROR", message="Total portfolio value is zero")
            return

        current_allocation = btc_value / total_value

        adjustment = self.config.get("adjustment_factor", 0.35)

        adjusted_target = current_allocation + (
            (signal - current_allocation) * adjustment
        )

        band = self.config.get("rebalance_band", 0.07)

        if adjusted_target - band <= current_allocation <= adjusted_target + band:

            self.log_event("NO_TRADE", reason="rebalance_band")
            return

        trade_value = abs((adjusted_target * total_value) - btc_value)

        max_trade_pct = self.config.get("max_trade_pct", 0.10)
        max_trade_value = total_value * max_trade_pct
        trade_value = min(trade_value, max_trade_value)

        if trade_value < self.config.get("min_trade_size_usd", 40):
            self.log_event(
                "NO_TRADE",
                reason="min_size",
                trade_value=trade_value
            )
            return

        side = "buy" if (adjusted_target * total_value) > btc_value else "sell"

        # --- Prevent chasing high prices ---
        entry_price = self.last_state.get("entry_price")
        if side == "buy" and entry_price:
            if price > entry_price * 1.004:
                self.log_event(
                    "BUY_SKIPPED",
                    reason="above_entry_price",
                    entry_price=entry_price,
                    price=price
                )
                return

        volume = round(trade_value / price, 6)

        self.log_event(
            "TRADE_DECISION",
            side=side,
            volume=volume,
            price=price
        )

        resp = self.kraken_request(
            "AddOrder",
            {
                "pair": self.config["pair"],
                "type": side,
                "ordertype": "market",
                "volume": volume
            }
        )

        if resp.get("error"):
            return

        fee = volume * price * self.config.get("taker_fee", 0.0026)

        realized_pnl = self.pnl.record_trade(
            side,
            price,
            volume,
            fee,
            portfolio["btc"],
            portfolio["usd"],
            self.last_state.get("entry_price"),
            price
        )

        self.log_event(
            "TRADE_EXECUTED",
            side=side,
            price=price,
            volume=volume,
            fee=fee,
            realized_pnl=realized_pnl
        )

        # --- Weighted average entry tracking ---
        if side == "buy":
            prev_entry = self.last_state.get("entry_price")
            if prev_entry is None:
                self.last_state["entry_price"] = price
            else:
                total_btc = portfolio["btc"] + volume
                self.last_state["entry_price"] = (
                    (prev_entry * portfolio["btc"]) + (price * volume)
                ) / total_btc
        else:
            self.last_state["entry_price"] = None

        self.last_state["last_trade_time"] = utc_ts()
        self.last_state["last_signal"] = signal
        self.save_state()


if __name__ == "__main__":

    trader = KrakenTrader()

    trader.log_event(
        "BOT_START",
        message="Kraken Sentiment Trader Starting"
    )

    price = trader.get_btc_price()

    trader.log_event("PRICE", price=price)

    trader.run()
