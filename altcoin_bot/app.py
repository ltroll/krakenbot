from __future__ import annotations

import argparse
import fcntl
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional

import requests

from .audit import atomic_json, operational_logger
from .config import RuntimeConfig, Strategy
from .engine import Engine
from .exchange import ExchangeError, KrakenReadOnly
from .models import ContractError, ScoutDecision
from .store import Store


class Application:
    def __init__(self, config: RuntimeConfig):
        self.config = config
        self.strategy = Strategy.load(config.strategy_file)
        self.store = Store(config.database_file)
        self.engine = Engine(self.store, self.strategy, config.mode, config.event_file)
        self.exchange = KrakenReadOnly(config.kraken_api_url, config.request_timeout_seconds,
                                       config.kraken_api_key, config.kraken_api_secret)
        self.log = operational_logger(config.operational_log_file)
        self.source_status: Dict[str, Any] = {}
        self.balances: Dict[str, Any] = {}
        self.exchange_orders: Dict[str, Any] = {}
        self.failures = []

    def fetch_json(self, url: str) -> Dict[str, Any]:
        response = requests.get(url, timeout=self.config.request_timeout_seconds)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ContractError("remote JSON must be an object")
        return payload

    def reconcile_before_action(self) -> bool:
        """Read-only only. No AddOrder/CancelOrder code exists in this application."""
        mismatch = self.engine.reconcile_local()
        if not self.config.read_only_reconciliation:
            self.balances = {"status": "disabled"}
            self.exchange_orders = {"status": "disabled"}
            return mismatch
        snapshot = self.exchange.reconcile()
        self.balances = snapshot["balances"]
        self.exchange_orders = snapshot["open_orders"]
        # Any exchange order is foreign because this foundation has never submitted one.
        # Expose it, but do not claim/cancel it or treat it as local state.
        exchange_txids = set(self.exchange_orders)
        exchange_client_ids = {
            str(order.get("cl_ord_id") or order.get("userref") or "")
            for order in self.exchange_orders.values() if isinstance(order, dict)
        }
        for local in self.store.active_orders():
            exchange_id = local.get("exchange_order_id")
            if exchange_id and exchange_id not in exchange_txids:
                mismatch = True
            if local["status"] == "unknown" and not exchange_id:
                # A timeout remains unknown unless read-only reconciliation finds its deterministic ID.
                if local["client_order_id"] not in exchange_client_ids:
                    mismatch = True
        return mismatch

    def run_cycle(self) -> None:
        now = datetime.now(timezone.utc)
        self.failures = []
        kill_switch = self.config.kill_switch_file.exists()
        try:
            payload = self.fetch_json(self.config.decision_url)
            decision = ScoutDecision.parse(payload)
            age_seconds = (now - decision.generated_at).total_seconds()
            self.source_status["asset_scout_decision"] = {
                "status": "ok", "generated_at": decision.generated_at.isoformat(),
                "age_seconds": age_seconds, "fresh": 0 <= age_seconds <=
                float(self.strategy.raw["maximum_signal_age_minutes"]) * 60,
            }
            for name, url in self.config.informational_urls.items():
                try:
                    info = self.fetch_json(url)
                    self.source_status[name] = {"status": "available", "advisory_only": True,
                                                "schema_version": info.get("schema_version"),
                                                "generated_at": info.get("generated_at") or info.get("processed_at")}
                except Exception as exc:
                    self.source_status[name] = {"status": "unavailable", "advisory_only": True,
                                                "error": type(exc).__name__}
            mismatch = self.reconcile_before_action()
            asset_config = self.strategy.raw["assets"].get(decision.asset, {})
            pair = asset_config.get("kraken_pair", decision.asset + "USD")
            rules = self.exchange.pair_rules(pair)
            ticker = self.exchange.ticker(pair)
            errors = int(self.store.metadata("consecutive_errors", "0"))
            self.engine.process_decision(decision, rules, now, kill_switch, mismatch, errors,
                                         ticker["ask"])
            # Dry-run action occurs only after a fresh reconciliation checkpoint.
            self.engine.advance_dry_run(ticker["bid"], ticker["ask"], now)
            self.store.set_metadata("consecutive_errors", "0")
            self.store.set_metadata("last_success_at", now.isoformat())
            self.log.info("cycle complete mode=%s decision_id=%s kill_switch=%s",
                          self.config.mode, decision.decision_id, kill_switch)
        except Exception as exc:
            errors = int(self.store.metadata("consecutive_errors", "0")) + 1
            self.store.set_metadata("consecutive_errors", str(errors))
            self.failures.append({"type": type(exc).__name__, "message": str(exc)})
            self.engine.emit("cycle_failed", "error", error_type=type(exc).__name__,
                             message=str(exc), consecutive_errors=errors)
            self.log.exception("cycle failed closed")
            if "asset_scout_decision" not in self.source_status:
                self.source_status["asset_scout_decision"] = {"status": "invalid_or_unavailable", "fresh": False}
        finally:
            self.write_status(kill_switch)

    def write_status(self, kill_switch: bool) -> None:
        active_orders = self.store.active_orders()
        positions = self.store.open_positions()
        today = datetime.now(timezone.utc).date().isoformat()
        closed_sol = self.store.closed_positions("SOL")
        cumulative = Decimal("0")
        peak = Decimal("0")
        max_drawdown = Decimal("0")
        for trade in closed_sol:
            cumulative += Decimal(trade["realized_pnl"])
            peak = max(peak, cumulative)
            max_drawdown = max(max_drawdown, peak - cumulative)
        source_fresh = bool(self.source_status.get("asset_scout_decision", {}).get("fresh"))
        readiness = {
            "ready_for_live": False,
            "closed_sol_paper_trades": len(closed_sol),
            "minimum_closed_trades": 10,
            "preferred_closed_trades": 20,
            "cumulative_realized_net_pnl": str(cumulative),
            "maximum_realized_drawdown": str(max_drawdown),
            "criteria": {
                "minimum_trade_sample": len(closed_sol) >= 10,
                "positive_net_returns": cumulative > 0,
                "multiple_regimes_verified": False,
                "stable_freshness_and_health": source_fresh and not self.failures,
            },
            "note": "Live remains unavailable and regime coverage requires external review.",
        }
        status = {
            "schema_version": "altcoin-bot-status-v1",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "mode": self.config.mode,
            "live_submission_available": False,
            "safety": {"kill_switch_active": kill_switch,
                       "live_enabled_setting_ignored": self.config.live_enabled,
                       "read_only_reconciliation": self.config.read_only_reconciliation,
                       "consecutive_errors": int(self.store.metadata("consecutive_errors", "0"))},
            "source_freshness": self.source_status,
            "balances": self.balances,
            "exchange_open_orders": self.exchange_orders,
            "active_orders": active_orders,
            "positions": positions,
            "limits": self.strategy.limits,
            "daily": {"utc_date": today, "filled_trades": self.store.filled_entries_today(today),
                      "realized_net_pnl": str(self.store.realized_pnl_today(today))},
            "production_readiness": readiness,
            "failures": self.failures,
            "recent_events": self.store.recent_events(20),
        }
        atomic_json(self.config.status_file, status)


def main() -> None:
    parser = argparse.ArgumentParser(description="Kraken altcoin observer/dry-run bot")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--interval-seconds", type=int)
    args = parser.parse_args()
    app = Application(RuntimeConfig.from_env(args.env_file))
    app.config.lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = app.config.lock_file.open("w")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        raise SystemExit("another altcoin bot instance holds the lock") from exc
    interval = args.interval_seconds or int(app.strategy.raw.get("poll_interval_seconds", 60))
    while True:
        app.run_cycle()
        if args.once:
            break
        time.sleep(max(1, interval))


if __name__ == "__main__":
    main()
