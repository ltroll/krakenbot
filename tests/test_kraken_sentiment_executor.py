import copy
import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch


_IMPORT_TMPDIR = tempfile.TemporaryDirectory()
_IMPORT_ENV = {
    "SENTIMENT_STATE_FILE": os.path.join(_IMPORT_TMPDIR.name, "state.json"),
    "SENTIMENT_STATE_BACKUP_FILE": os.path.join(
        _IMPORT_TMPDIR.name,
        "state.json.bak",
    ),
    "SENTIMENT_STRATEGY_PROFILE": os.path.abspath(
        "sentiment_strategy_default.json"
    ),
    "SENTIMENT_TRADE_LOG_FILE": os.path.join(_IMPORT_TMPDIR.name, "trades.jsonl"),
    "SENTIMENT_TRADE_ACTIVITY_FILE": os.path.join(
        _IMPORT_TMPDIR.name,
        "activity.jsonl",
    ),
    "SENTIMENT_DECISION_CSV_FILE": os.path.join(
        _IMPORT_TMPDIR.name,
        "decisions.csv",
    ),
}
_ORIGINAL_ENV = {name: os.environ.get(name) for name in _IMPORT_ENV}
os.environ.update(_IMPORT_ENV)
try:
    import kraken_sentiment_executor as bot
finally:
    for _name, _value in _ORIGINAL_ENV.items():
        if _value is None:
            os.environ.pop(_name, None)
        else:
            os.environ[_name] = _value


class KrakenSentimentExecutorSafetyTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.originals = {
            "STATE_FILE": bot.STATE_FILE,
            "STATE_BACKUP_FILE": bot.STATE_BACKUP_FILE,
            "LOG_FILE": bot.LOG_FILE,
            "TRADE_ACTIVITY_FILE": bot.TRADE_ACTIVITY_FILE,
            "DECISION_CSV_FILE": bot.DECISION_CSV_FILE,
            "DRY_RUN": bot.DRY_RUN,
            "PAIR_INFO_CACHE": bot.PAIR_INFO_CACHE,
            "CONF_THRESHOLD": bot.CONF_THRESHOLD,
            "VOLATILITY_DAMPENING": bot.VOLATILITY_DAMPENING,
            "VOLATILITY_CUTOFF": bot.VOLATILITY_CUTOFF,
            "state": bot.state,
        }
        bot.STATE_FILE = os.path.join(self.tmpdir.name, "state.json")
        bot.STATE_BACKUP_FILE = f"{bot.STATE_FILE}.bak"
        bot.LOG_FILE = os.path.join(self.tmpdir.name, "trades.jsonl")
        bot.TRADE_ACTIVITY_FILE = os.path.join(self.tmpdir.name, "activity.jsonl")
        bot.DECISION_CSV_FILE = os.path.join(self.tmpdir.name, "decisions.csv")
        bot.DRY_RUN = False
        bot.PAIR_INFO_CACHE = {
            "lot_decimals": 8,
            "pair_decimals": 1,
            "ordermin": "0.0001",
        }
        bot.state = bot.load_state()

    def tearDown(self):
        for name, value in self.originals.items():
            setattr(bot, name, value)
        self.tmpdir.cleanup()

    def test_atomic_state_recovers_last_valid_backup(self):
        bot.save_state({"marker": 1})
        bot.save_state({"marker": 2})
        with open(bot.STATE_FILE, "w", encoding="utf-8") as handle:
            handle.write("{")

        recovered = bot.load_state()

        self.assertEqual(recovered["marker"], 1)

    def test_canceled_partial_buy_queues_executed_volume_for_sell(self):
        bot.state["open_buy_orders"]["BUY-1"] = {
            "txid": "BUY-1",
            "volume": 0.01,
            "price": 65000.0,
            "target_profit_pct": 0.008,
            "round_trip_fee_pct": 0.0065,
        }
        status = {
            "status": "canceled",
            "vol": "0.010",
            "vol_exec": "0.004",
            "cost": "260.00",
            "fee": "0.67",
            "price": "65000.0",
        }

        with (
            patch.object(bot, "query_orders", return_value={"BUY-1": status}),
            patch.object(bot, "retry_pending_profit_sells"),
            patch.object(bot, "log_and_console"),
            patch.object(bot, "log_trade_activity"),
            patch.object(bot, "notify_order_tracker"),
        ):
            bot.process_open_buy_orders("cycle-1")

        self.assertNotIn("BUY-1", bot.state["open_buy_orders"])
        pending = bot.state["pending_profit_sells"]["BUY-1"]
        self.assertAlmostEqual(pending["volume"], 0.004)
        self.assertAlmostEqual(pending["buy_price"], 65000.0)
        self.assertEqual(pending["source_order_status"], "canceled")

    def test_failed_profit_sell_remains_durable_and_counted(self):
        bot.state["pending_profit_sells"]["BUY-2"] = {
            "buy_txid": "BUY-2",
            "buy_price": 65000.0,
            "volume": 0.01,
            "target_profit_pct": 0.008,
        }

        with patch.object(bot, "place_profit_sell_for_buy", return_value=None):
            bot.retry_pending_profit_sells("cycle-2")

        self.assertIn("BUY-2", bot.state["pending_profit_sells"])
        self.assertAlmostEqual(bot.current_inventory_usd(66000.0), 650.0)

    def test_accepted_profit_sell_atomically_replaces_pending_inventory(self):
        bot.state["pending_profit_sells"]["BUY-2B"] = {
            "buy_txid": "BUY-2B",
            "buy_price": 65000.0,
            "volume": 0.01,
            "target_profit_pct": 0.008,
        }

        with (
            patch.object(
                bot,
                "safe_kraken_private",
                return_value={"result": {"txid": ["SELL-2B"]}},
            ),
            patch.object(bot, "log_and_console"),
            patch.object(bot, "log_trade_activity"),
            patch.object(bot, "notify_order_tracker"),
        ):
            bot.retry_pending_profit_sells("cycle-2b")

        self.assertNotIn("BUY-2B", bot.state["pending_profit_sells"])
        self.assertFalse(bot.state["pending_order_intents"])
        self.assertEqual(
            bot.state["open_sell_orders"]["SELL-2B"]["buy_txid"],
            "BUY-2B",
        )

    def test_canceled_partial_sell_requeues_only_remaining_volume(self):
        bot.state["open_sell_orders"]["SELL-1"] = {
            "txid": "SELL-1",
            "volume": 0.01,
            "sell_price": 66000.0,
            "buy_price": 65000.0,
            "buy_txid": "BUY-3",
            "target_profit_pct": 0.008,
        }
        status = {
            "status": "canceled",
            "vol": "0.010",
            "vol_exec": "0.004",
            "cost": "264.00",
            "fee": "0.68",
            "price": "66000.0",
        }

        with (
            patch.object(bot, "query_orders", return_value={"SELL-1": status}),
            patch.object(bot, "retry_pending_profit_sells"),
            patch.object(bot, "log_and_console"),
            patch.object(bot, "log_trade_activity"),
            patch.object(bot, "notify_order_tracker"),
        ):
            bot.process_open_sell_orders("cycle-3")

        self.assertNotIn("SELL-1", bot.state["open_sell_orders"])
        pending = bot.state["pending_profit_sells"]["BUY-3"]
        self.assertAlmostEqual(pending["volume"], 0.006)
        self.assertAlmostEqual(bot.state["last_sell_price"], 66000.0)

    def test_ambiguous_buy_intent_blocks_duplicate_submission(self):
        with (
            patch.object(bot, "safe_kraken_private", return_value=None) as private,
            patch.object(bot, "log_trade_activity"),
        ):
            first = bot.place_limit_buy(65000.0, 0.001, "cycle-4")
            second = bot.place_limit_buy(65000.0, 0.001, "cycle-4")

        self.assertIsNone(first)
        self.assertIsNone(second)
        self.assertEqual(private.call_count, 1)
        self.assertEqual(len(bot.state["pending_order_intents"]), 1)
        client_order_id = next(iter(bot.state["pending_order_intents"]))
        submitted_payload = private.call_args.args[2]
        self.assertEqual(submitted_payload["cl_ord_id"], client_order_id)

    def test_reconciliation_adopts_order_by_client_id(self):
        client_order_id = "ks0000b123456789ab"
        bot.state["pending_order_intents"][client_order_id] = {
            "side": "buy",
            "ordertype": "limit",
            "price": 65000.0,
            "volume": 0.001,
            "cycle_id": "cycle-5",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "intent_key": None,
            "order_state": {
                "volume": 0.001,
                "price": 65000.0,
                "target_profit_pct": 0.008,
            },
        }

        with patch.object(
            bot,
            "recover_order_by_client_id",
            return_value=("BUY-5", {"status": "open"}, True),
        ):
            bot.reconcile_pending_order_intents()

        self.assertNotIn(client_order_id, bot.state["pending_order_intents"])
        self.assertEqual(
            bot.state["open_buy_orders"]["BUY-5"]["client_order_id"],
            client_order_id,
        )

    def test_signal_contract_missing_fields_fail_closed(self):
        now = datetime.now(timezone.utc)
        valid = {
            "processed_at": now.isoformat(),
            "signal_status": "fresh",
            "bot_action_allowed": True,
            "freshness": {},
            "source_status": {
                name: {"status": "fresh"}
                for name in bot.CRITICAL_SOURCE_STATUSES
            },
        }

        missing_timestamp = copy.deepcopy(valid)
        missing_timestamp.pop("processed_at")
        missing_permission = copy.deepcopy(valid)
        missing_permission.pop("bot_action_allowed")
        missing_sources = copy.deepcopy(valid)
        missing_sources["source_status"] = {}

        self.assertEqual(
            bot.signal_gate_failure(missing_timestamp, now)["reason"],
            "signal_timestamp_missing",
        )
        self.assertEqual(
            bot.signal_gate_failure(missing_permission, now)["reason"],
            "bot_action_not_allowed",
        )
        self.assertEqual(
            bot.signal_gate_failure(missing_sources, now)["reason"],
            "critical_source_status_missing",
        )

    def test_confidence_and_volatility_controls_are_enforced(self):
        bot.CONF_THRESHOLD = 0.45
        bot.VOLATILITY_DAMPENING = True
        bot.VOLATILITY_CUTOFF = 0.01

        failure = bot.confidence_gate_failure(0.2)
        weighted, multiplier, volatility = bot.apply_volatility_dampening(
            0.12,
            {"price_regime": {"realized_volatility_24h_pct": 0.04}},
        )

        self.assertEqual(failure["reason"], "confidence_below_threshold")
        self.assertAlmostEqual(multiplier, 0.25)
        self.assertAlmostEqual(weighted, 0.03)
        self.assertAlmostEqual(volatility, 0.04)


if __name__ == "__main__":
    unittest.main()
