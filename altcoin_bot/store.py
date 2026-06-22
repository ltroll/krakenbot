from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS decisions (
  decision_id TEXT PRIMARY KEY,
  asset TEXT NOT NULL,
  generated_at TEXT NOT NULL,
  received_at TEXT NOT NULL,
  outcome TEXT NOT NULL,
  failures_json TEXT NOT NULL,
  client_order_id TEXT
);
CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  client_order_id TEXT NOT NULL UNIQUE,
  exchange_order_id TEXT UNIQUE,
  decision_id TEXT NOT NULL,
  asset TEXT NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('entry','take_profit','stop','forced_exit')),
  status TEXT NOT NULL,
  price TEXT NOT NULL,
  quantity TEXT NOT NULL,
  filled_quantity TEXT NOT NULL DEFAULT '0',
  expires_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY(decision_id) REFERENCES decisions(decision_id)
);
CREATE TABLE IF NOT EXISTS positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  decision_id TEXT NOT NULL UNIQUE,
  asset TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('open','closed')),
  entry_price TEXT NOT NULL,
  quantity TEXT NOT NULL,
  remaining_quantity TEXT NOT NULL,
  opened_at TEXT NOT NULL,
  closed_at TEXT,
  exit_price TEXT,
  exit_reason TEXT,
  realized_pnl TEXT NOT NULL DEFAULT '0',
  FOREIGN KEY(decision_id) REFERENCES decisions(decision_id)
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  created_at TEXT NOT NULL,
  event_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  decision_id TEXT,
  details_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS events_created_idx ON events(created_at);
CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript(SCHEMA)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(str(self.path), timeout=10)
        db.row_factory = sqlite3.Row
        try:
            yield db
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    def decision_exists(self, decision_id: str) -> bool:
        with self.connect() as db:
            return db.execute("SELECT 1 FROM decisions WHERE decision_id=?", (decision_id,)).fetchone() is not None

    def metadata(self, key: str, default: str = "") -> str:
        with self.connect() as db:
            row = db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            return row["value"] if row else default

    def set_metadata(self, key: str, value: str) -> None:
        with self.connect() as db:
            db.execute("INSERT INTO metadata(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                       (key, value))

    def record_decision(self, decision_id: str, asset: str, generated_at: str,
                        outcome: str, failures: Iterable[str], client_order_id: Optional[str]) -> bool:
        with self.connect() as db:
            cursor = db.execute(
                "INSERT OR IGNORE INTO decisions VALUES (?,?,?,?,?,?,?)",
                (decision_id, asset, generated_at, iso_now(), outcome,
                 json.dumps(list(failures), separators=(",", ":")), client_order_id),
            )
            return cursor.rowcount == 1

    def create_order(self, client_order_id: str, decision_id: str, asset: str,
                     kind: str, price: Decimal, quantity: Decimal, status: str = "pending") -> None:
        now = iso_now()
        with self.connect() as db:
            db.execute(
                "INSERT INTO orders(client_order_id,decision_id,asset,kind,status,price,quantity,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?)",
                (client_order_id, decision_id, asset, kind, status, str(price), str(quantity), now, now),
            )

    def accept_candidate(self, decision_id: str, asset: str, generated_at: str,
                         client_order_id: str, price: Decimal, quantity: Decimal,
                         expires_at: str) -> bool:
        """Atomically claim a decision and create its one entry order."""
        now = iso_now()
        with self.connect() as db:
            inserted = db.execute(
                "INSERT OR IGNORE INTO decisions VALUES (?,?,?,?,?,?,?)",
                (decision_id, asset, generated_at, now, "accepted", "[]", client_order_id),
            ).rowcount
            if not inserted:
                return False
            db.execute(
                "INSERT INTO orders(client_order_id,decision_id,asset,kind,status,price,quantity,expires_at,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (client_order_id, decision_id, asset, "entry", "pending", str(price), str(quantity), expires_at, now, now),
            )
            return True

    def update_order(self, client_order_id: str, status: str, filled_quantity: Decimal,
                     exchange_order_id: Optional[str] = None) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE orders SET status=?,filled_quantity=?,exchange_order_id=COALESCE(?,exchange_order_id),updated_at=? "
                "WHERE client_order_id=?",
                (status, str(filled_quantity), exchange_order_id, iso_now(), client_order_id),
            )

    def cancel_active_orders(self, decision_id: str, kinds: Iterable[str]) -> None:
        values = tuple(kinds)
        if not values:
            return
        placeholders = ",".join("?" for _ in values)
        with self.connect() as db:
            db.execute(
                f"UPDATE orders SET status='cancelled',updated_at=? WHERE decision_id=? "
                f"AND kind IN ({placeholders}) AND status IN ('pending','partial','unknown')",
                (iso_now(), decision_id, *values),
            )

    def upsert_take_profit(self, client_order_id: str, decision_id: str, asset: str,
                           price: Decimal, quantity: Decimal) -> None:
        now = iso_now()
        with self.connect() as db:
            db.execute(
                "INSERT INTO orders(client_order_id,decision_id,asset,kind,status,price,quantity,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(client_order_id) DO UPDATE SET "
                "price=excluded.price,quantity=excluded.quantity,updated_at=excluded.updated_at",
                (client_order_id, decision_id, asset, "take_profit", "pending", str(price), str(quantity), now, now),
            )

    def create_or_grow_position(self, decision_id: str, asset: str, entry_price: Decimal,
                                fill_quantity: Decimal, opened_at: str) -> None:
        with self.connect() as db:
            row = db.execute("SELECT * FROM positions WHERE decision_id=?", (decision_id,)).fetchone()
            if row:
                old_qty = Decimal(row["quantity"])
                old_price = Decimal(row["entry_price"])
                new_qty = old_qty + fill_quantity
                average = ((old_price * old_qty) + (entry_price * fill_quantity)) / new_qty
                db.execute(
                    "UPDATE positions SET entry_price=?,quantity=?,remaining_quantity=? WHERE decision_id=?",
                    (str(average), str(new_qty), str(new_qty), decision_id),
                )
            else:
                db.execute(
                    "INSERT INTO positions(decision_id,asset,status,entry_price,quantity,remaining_quantity,opened_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (decision_id, asset, "open", str(entry_price), str(fill_quantity), str(fill_quantity), opened_at),
                )

    def close_position(self, decision_id: str, exit_price: Decimal, reason: str,
                       pnl: Decimal, closed_at: str) -> None:
        with self.connect() as db:
            db.execute(
                "UPDATE positions SET status='closed',remaining_quantity='0',closed_at=?,exit_price=?,exit_reason=?,realized_pnl=? "
                "WHERE decision_id=? AND status='open'",
                (closed_at, str(exit_price), reason, str(pnl), decision_id),
            )

    def open_positions(self) -> List[Dict[str, Any]]:
        with self.connect() as db:
            return [dict(row) for row in db.execute("SELECT * FROM positions WHERE status='open' ORDER BY id")]

    def closed_positions(self, asset: Optional[str] = None) -> List[Dict[str, Any]]:
        query = "SELECT * FROM positions WHERE status='closed'"
        params = ()
        if asset:
            query += " AND asset=?"
            params = (asset,)
        query += " ORDER BY closed_at"
        with self.connect() as db:
            return [dict(row) for row in db.execute(query, params)]

    def active_orders(self) -> List[Dict[str, Any]]:
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                "SELECT * FROM orders WHERE status IN ('pending','partial','unknown') ORDER BY id")]

    def filled_entries_today(self, day_prefix: str) -> int:
        with self.connect() as db:
            row = db.execute(
                "SELECT COUNT(*) AS n FROM positions WHERE opened_at LIKE ?", (day_prefix + "%",)
            ).fetchone()
            return int(row["n"])

    def realized_pnl_today(self, day_prefix: str) -> Decimal:
        with self.connect() as db:
            rows = db.execute(
                "SELECT realized_pnl FROM positions WHERE status='closed' AND closed_at LIKE ?", (day_prefix + "%",)
            )
            return sum((Decimal(row["realized_pnl"]) for row in rows), Decimal("0"))

    def last_closed_at(self, asset: str) -> Optional[str]:
        with self.connect() as db:
            row = db.execute(
                "SELECT closed_at FROM positions WHERE asset=? AND status='closed' ORDER BY closed_at DESC LIMIT 1",
                (asset,),
            ).fetchone()
            return row["closed_at"] if row else None

    def event(self, event_type: str, severity: str = "info", decision_id: Optional[str] = None,
              **details: Any) -> Dict[str, Any]:
        record = {"created_at": iso_now(), "event_type": event_type, "severity": severity,
                  "decision_id": decision_id, "details": details}
        with self.connect() as db:
            db.execute("INSERT INTO events(created_at,event_type,severity,decision_id,details_json) VALUES(?,?,?,?,?)",
                       (record["created_at"], event_type, severity, decision_id,
                        json.dumps(details, separators=(",", ":"), default=str)))
        return record

    def recent_events(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self.connect() as db:
            rows = db.execute("SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [{"created_at": r["created_at"], "event_type": r["event_type"],
                 "severity": r["severity"], "decision_id": r["decision_id"],
                 "details": json.loads(r["details_json"])} for r in reversed(rows)]
