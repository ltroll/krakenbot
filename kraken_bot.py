#!/usr/bin/env python3
import os
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
import krakenex
from dotenv import load_dotenv
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

    def validate_env(self):

        required = [
            "KRAKEN_API_KEY",
            "KRAKEN_API_SECRET",
            "KRAKEN_API_URL",
            "KRAKEN_TICKER_URL",
            "LLM_SIGNAL_URL"
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

    def kraken_request(self, endpoint, data=None):

        if data is None:
            data = {}

        response = self.api.query_private(endpoint, data)

        if response.get("error"):
            self.log_event(
                "KRAKEN_ERROR",
                endpoint=endpoint,
                error=response["error"]
            )

        return response

    def get_btc_price(self):

        r = requests.get(self.ticker_url).json()

        pair = list(r["result"].keys())[0]

        return float(r["result"][pair]["c"][0])

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

    def load_sentiment(self):

        sentiment = requests.get(
            os.getenv("LLM_SIGNAL_URL"),
            timeout=5
        ).json()

        if "execution_signal" not in sentiment:

            self.log_event(
                "WARNING",
                message="execution_signal missing — fallback active"
            )

            composite = (
                sentiment.get("btc_sentiment", 0)
                + sentiment.get("macro_tightening_bias", 0)
                - sentiment.get("regulatory_risk", 0)
            )

            sentiment["execution_signal"] = max(min(composite, 1), -1)

        sentiment.setdefault("smoothed_risk_multiplier", 1.0)
        sentiment.setdefault("confidence", 0.5)

        return sentiment

    def compute_target_allocation(self, sentiment):

        execution_signal = sentiment["execution_signal"]

        execution_signal = max(min(execution_signal, 1), -1)

        allocation = (execution_signal + 1) / 2

        allocation *= sentiment["smoothed_risk_multiplier"]

        allocation = max(min(allocation, 1), 0)

        return allocation

    def should_trade(self, signal):

        last_signal = self.last_state.get("last_signal")

        if last_signal is None:
            return True

        diff = abs(signal - last_signal)

        if diff < self.config.get("signal_change_threshold", 0.025):

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

    def enforce_profit_logic(self, price):

        entry_price = self.last_state.get("entry_price")

        if entry_price is None:
            return None

        pnl_pct = (price - entry_price) / entry_price

        min_move_required = self.config.get("min_stop_move_pct", 0.006)

        if abs(pnl_pct) < min_move_required:
            return None

        if pnl_pct >= self.config.get("profit_target_pct", 0.007):

            self.log_event("EXIT_SIGNAL", reason="profit_target")

            return 0

        if pnl_pct <= -self.config.get("stop_loss_pct", 0.004):

            self.log_event("EXIT_SIGNAL", reason="stop_loss")

            return 0

        return None

    def run(self):

        price = self.get_btc_price()

        sentiment = self.load_sentiment()

        signal = self.compute_target_allocation(sentiment)

        self.log_event(
            "SIGNAL_UPDATE",
            execution_signal=sentiment["execution_signal"],
            allocation_target=signal,
            confidence=sentiment["confidence"]
        )

        portfolio = self.get_portfolio(price)

        override_signal = self.enforce_profit_logic(price)

        if override_signal == 0 and portfolio["btc"] > 0:

            self.log_event("FORCED_EXIT", reason="profit_or_stop")

            volume = portfolio["btc"]

            resp = self.kraken_request(
                "AddOrder",
                {
                    "pair": self.config["pair"],
                    "type": "sell",
                    "ordertype": "market",
                    "volume": volume
                }
            )

            self.last_state["entry_price"] = None
            self.last_state["last_signal"] = 0
            self.last_state["last_trade_time"] = utc_ts()

            self.save_state()

            return

        if override_signal is not None:
            signal = override_signal

        if not self.should_trade(signal):
            return

        total_value = portfolio["total_value"]
        btc_value = portfolio["btc_value"]

        if total_value == 0:

            self.log_event("ERROR", message="portfolio empty")

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

        trade_value = min(
            trade_value,
            total_value * self.config.get("max_trade_pct", 0.10)
        )

        if trade_value < self.config.get("min_trade_size_usd", 40):

            self.log_event("NO_TRADE", reason="min_trade_size")

            return

        side = "buy" if signal > current_allocation else "sell"

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

        if side == "buy":

            prev_entry = self.last_state.get("entry_price")

            if prev_entry is None:

                self.last_state["entry_price"] = price

            else:

                total_btc = portfolio["btc"] + volume

                self.last_state["entry_price"] = (
                    (prev_entry * portfolio["btc"])
                    + (price * volume)
                ) / total_btc

        else:

            self.last_state["entry_price"] = None

        self.last_state["last_trade_time"] = utc_ts()
        self.last_state["last_signal"] = signal

        self.save_state()


if __name__ == "__main__":

    trader = KrakenTrader()

    trader.log_event("BOT_START", message="Kraken Sentiment Trader Starting")

    trader.run()
