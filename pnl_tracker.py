import sqlite3
from datetime import datetime

class PnLTracker:

    def __init__(self, db="trading.db"):
        self.conn = sqlite3.connect(db)
        self.create_table()

    def create_table(self):

        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            side TEXT,
            price REAL,
            volume REAL,
            fee REAL,
            realized_pnl REAL,
            unrealized_pnl REAL,
            btc_balance REAL,
            usd_balance REAL,
            avg_entry_price REAL
        )
        """)

        self.conn.commit()

    def record_trade(
        self,
        side,
        price,
        volume,
        fee,
        btc_balance,
        usd_balance,
        avg_entry_price,
        current_price
    ):

        realized = 0
        unrealized = 0

        if avg_entry_price is not None and btc_balance > 0:

            if side == "sell":
                realized = (price - avg_entry_price) * volume - fee

            unrealized = (current_price - avg_entry_price) * btc_balance

        self.conn.execute(
            """
            INSERT INTO trades
            (timestamp, side, price, volume, fee, realized_pnl,
             unrealized_pnl, btc_balance, usd_balance, avg_entry_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.utcnow().isoformat(),
                side,
                price,
                volume,
                fee,
                realized,
                unrealized,
                btc_balance,
                usd_balance,
                avg_entry_price
            )
        )

        self.conn.commit()
