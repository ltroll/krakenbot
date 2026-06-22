from __future__ import annotations

import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv


def _bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class RuntimeConfig:
    mode: str
    decision_url: str
    informational_urls: Dict[str, str]
    strategy_file: Path
    database_file: Path
    status_file: Path
    event_file: Path
    operational_log_file: Path
    kill_switch_file: Path
    request_timeout_seconds: int
    kraken_api_url: str
    kraken_api_key: str
    kraken_api_secret: str
    live_enabled: bool
    live_confirmation: str
    read_only_reconciliation: bool
    lock_file: Path

    @classmethod
    def from_env(cls, env_file: str = ".env") -> "RuntimeConfig":
        load_dotenv(env_file, override=False)
        mode = os.getenv("ALTCOIN_BOT_MODE", "observer").strip().lower()
        if mode not in {"observer", "dry_run"}:
            raise ValueError("ALTCOIN_BOT_MODE must be observer or dry_run; live is not implemented")
        return cls(
            mode=mode,
            decision_url=os.environ["ALTCOIN_SCOUT_DECISION_URL"],
            informational_urls={
                "multi_asset_signal": os.environ["ALTCOIN_MULTI_ASSET_SIGNAL_URL"],
                "rolling_backtest": os.environ["ALTCOIN_ROLLING_BACKTEST_URL"],
                "paper_trading": os.environ["ALTCOIN_PAPER_TRADING_URL"],
            },
            strategy_file=Path(os.getenv("ALTCOIN_STRATEGY_FILE", "altcoin_strategy_default.json")),
            database_file=Path(os.getenv("ALTCOIN_DATABASE_FILE", "altcoin_bot.sqlite3")),
            status_file=Path(os.getenv("ALTCOIN_STATUS_FILE", "/var/www/html/bot/altcoin_bot_status.json")),
            event_file=Path(os.getenv("ALTCOIN_EVENT_FILE", "altcoin_order_events.jsonl")),
            operational_log_file=Path(os.getenv("ALTCOIN_OPERATIONAL_LOG_FILE", "altcoin_bot.log")),
            kill_switch_file=Path(os.getenv("ALTCOIN_KILL_SWITCH_FILE", "altcoin_bot.kill")),
            request_timeout_seconds=int(os.getenv("ALTCOIN_REQUEST_TIMEOUT_SECONDS", "10")),
            kraken_api_url=os.getenv("KRAKEN_API_URL", "https://api.kraken.com"),
            kraken_api_key=os.getenv("KRAKEN_API_KEY", ""),
            kraken_api_secret=os.getenv("KRAKEN_API_SECRET", ""),
            live_enabled=_bool(os.getenv("ALTCOIN_LIVE_ENABLED"), False),
            live_confirmation=os.getenv("ALTCOIN_LIVE_CONFIRMATION", ""),
            read_only_reconciliation=_bool(os.getenv("ALTCOIN_READ_ONLY_RECONCILIATION"), False),
            lock_file=Path(os.getenv("ALTCOIN_LOCK_FILE", "altcoin_bot.lock")),
        )


@dataclass(frozen=True)
class Strategy:
    raw: Dict[str, Any]

    @classmethod
    def load(cls, path: Path) -> "Strategy":
        with path.open(encoding="utf-8") as handle:
            raw = json.load(handle)
        required = {"assets", "take_profit_pct", "stop_loss_pct", "fees_bps", "limits"}
        missing = sorted(required - set(raw))
        if missing:
            raise ValueError(f"strategy missing fields: {', '.join(missing)}")
        return cls(raw)

    def d(self, key: str) -> Decimal:
        return Decimal(str(self.raw[key]))

    @property
    def limits(self) -> Dict[str, Any]:
        return self.raw["limits"]

    def asset_enabled(self, asset: str) -> bool:
        return bool(self.raw["assets"].get(asset, {}).get("enabled", False))
