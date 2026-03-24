from __future__ import annotations
"""SQLite 데이터 저장소.

베이시스, 시세, 펀딩레이트, 주문, PnL 등을 시계열로 저장.
"""


import sqlite3
import time
import logging
from pathlib import Path
from dataclasses import asdict
from typing import Any

logger = logging.getLogger("arbitrage.storage")


CREATE_TABLES_SQL = """
-- 시세 데이터 (perp)
CREATE TABLE IF NOT EXISTS perp_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    mark_price REAL NOT NULL,
    index_price REAL NOT NULL,
    funding_rate REAL,
    predicted_funding REAL,
    open_interest REAL,
    volume_24h REAL,
    basis_bps REAL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_perp_ts ON perp_prices(ticker, ts);

-- 시세 데이터 (futures / 키움)
CREATE TABLE IF NOT EXISTS futures_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    contract_month TEXT NOT NULL,
    price REAL NOT NULL,
    bid REAL,
    ask REAL,
    volume INTEGER,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_futures_ts ON futures_prices(symbol, ts);

-- 베이시스 스프레드
CREATE TABLE IF NOT EXISTS basis_spread (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product TEXT NOT NULL,          -- wti / brent
    perp_price REAL NOT NULL,
    futures_price REAL NOT NULL,
    basis REAL NOT NULL,            -- perp - futures
    basis_bps REAL NOT NULL,
    funding_rate REAL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_basis_ts ON basis_spread(product, ts);

-- 펀딩레이트 히스토리
CREATE TABLE IF NOT EXISTS funding_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    funding_rate REAL NOT NULL,
    premium REAL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_funding_ts ON funding_history(ticker, ts);

-- 주문 기록
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id TEXT,
    product TEXT NOT NULL,
    leg TEXT NOT NULL,               -- perp / futures
    side TEXT NOT NULL,              -- buy / sell
    size REAL NOT NULL,
    price REAL,
    filled_price REAL,
    filled_size REAL,
    status TEXT NOT NULL,            -- pending / filled / cancelled / error
    is_paper INTEGER DEFAULT 1,
    ts REAL NOT NULL
);

-- 포지션 기록
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product TEXT NOT NULL,
    perp_size REAL DEFAULT 0,
    perp_entry REAL DEFAULT 0,
    futures_size REAL DEFAULT 0,
    futures_entry REAL DEFAULT 0,
    unrealized_pnl REAL DEFAULT 0,
    realized_pnl REAL DEFAULT 0,
    funding_pnl REAL DEFAULT 0,
    status TEXT DEFAULT 'open',      -- open / closed
    opened_at REAL NOT NULL,
    closed_at REAL
);

-- 일일 PnL
CREATE TABLE IF NOT EXISTS daily_pnl (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    product TEXT NOT NULL,
    trading_pnl REAL DEFAULT 0,
    funding_pnl REAL DEFAULT 0,
    fees REAL DEFAULT 0,
    net_pnl REAL DEFAULT 0,
    num_trades INTEGER DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_pnl ON daily_pnl(date, product);
"""


class Storage:
    """SQLite 저장소 관리."""

    def __init__(self, db_path: str = "data/arbitrage.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    def connect(self):
        """DB 연결 및 테이블 생성."""
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(CREATE_TABLES_SQL)
        self._conn.commit()
        logger.info(f"Database connected: {self.db_path}")

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self.connect()
        return self._conn  # type: ignore

    # ── 시세 저장 ──

    def save_perp_price(
        self,
        ticker: str,
        mark_price: float,
        index_price: float,
        funding_rate: float = 0,
        predicted_funding: float = 0,
        open_interest: float = 0,
        volume_24h: float = 0,
        ts: float | None = None,
    ):
        """퍼페추얼 시세 저장."""
        ts = ts or time.time()
        basis_bps = (mark_price - index_price) / index_price * 10_000 if index_price else 0
        self.conn.execute(
            """INSERT INTO perp_prices
               (ticker, mark_price, index_price, funding_rate, predicted_funding,
                open_interest, volume_24h, basis_bps, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (ticker, mark_price, index_price, funding_rate, predicted_funding,
             open_interest, volume_24h, basis_bps, ts),
        )
        self.conn.commit()

    def save_futures_price(
        self,
        symbol: str,
        contract_month: str,
        price: float,
        bid: float = 0,
        ask: float = 0,
        volume: int = 0,
        ts: float | None = None,
    ):
        """월물 선물 시세 저장."""
        ts = ts or time.time()
        self.conn.execute(
            """INSERT INTO futures_prices
               (symbol, contract_month, price, bid, ask, volume, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (symbol, contract_month, price, bid, ask, volume, ts),
        )
        self.conn.commit()

    def save_basis(
        self,
        product: str,
        perp_price: float,
        futures_price: float,
        funding_rate: float = 0,
        ts: float | None = None,
    ):
        """베이시스 스프레드 저장."""
        ts = ts or time.time()
        basis = perp_price - futures_price
        basis_bps = basis / futures_price * 10_000 if futures_price else 0
        self.conn.execute(
            """INSERT INTO basis_spread
               (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts),
        )
        self.conn.commit()

    def save_funding(self, ticker: str, funding_rate: float, premium: float = 0, ts: float | None = None):
        """펀딩레이트 저장."""
        ts = ts or time.time()
        self.conn.execute(
            "INSERT INTO funding_history (ticker, funding_rate, premium, ts) VALUES (?, ?, ?, ?)",
            (ticker, funding_rate, premium, ts),
        )
        self.conn.commit()

    # ── 주문 저장 ──

    def save_order(
        self,
        product: str,
        leg: str,
        side: str,
        size: float,
        price: float | None = None,
        filled_price: float | None = None,
        filled_size: float | None = None,
        order_id: str = "",
        status: str = "pending",
        is_paper: bool = True,
        ts: float | None = None,
    ) -> int:
        """주문 기록 저장. 반환: row id."""
        ts = ts or time.time()
        cursor = self.conn.execute(
            """INSERT INTO orders
               (order_id, product, leg, side, size, price, filled_price,
                filled_size, status, is_paper, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (order_id, product, leg, side, size, price, filled_price,
             filled_size, status, 1 if is_paper else 0, ts),
        )
        self.conn.commit()
        return cursor.lastrowid  # type: ignore

    # ── 조회 ──

    def get_recent_basis(self, product: str, hours: float = 24) -> list[dict]:
        """최근 N시간 베이시스 데이터."""
        since = time.time() - hours * 3600
        rows = self.conn.execute(
            """SELECT * FROM basis_spread
               WHERE product = ? AND ts >= ?
               ORDER BY ts ASC""",
            (product, since),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_recent_perp_prices(self, ticker: str, hours: float = 24) -> list[dict]:
        """최근 N시간 퍼프 시세."""
        since = time.time() - hours * 3600
        rows = self.conn.execute(
            """SELECT * FROM perp_prices
               WHERE ticker = ? AND ts >= ?
               ORDER BY ts ASC""",
            (ticker, since),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_basis_stats(self, product: str, hours: float = 24) -> dict:
        """베이시스 통계 (평균, 표준편차, 최대, 최소)."""
        since = time.time() - hours * 3600
        row = self.conn.execute(
            """SELECT
                AVG(basis_bps) as mean,
                -- SQLite에는 STDDEV가 없으므로 수동 계산
                COUNT(*) as cnt,
                MIN(basis_bps) as min_val,
                MAX(basis_bps) as max_val,
                SUM(basis_bps) as sum_val,
                SUM(basis_bps * basis_bps) as sum_sq
               FROM basis_spread
               WHERE product = ? AND ts >= ?""",
            (product, since),
        ).fetchone()

        if not row or row["cnt"] < 2:
            return {"mean": 0, "std": 0, "min": 0, "max": 0, "count": 0}

        mean = row["mean"]
        cnt = row["cnt"]
        variance = (row["sum_sq"] / cnt) - (mean ** 2)
        std = max(0, variance) ** 0.5

        return {
            "mean": mean,
            "std": std,
            "min": row["min_val"],
            "max": row["max_val"],
            "count": cnt,
        }

    def get_cumulative_funding(self, ticker: str, hours: float = 24) -> float:
        """누적 펀딩레이트."""
        since = time.time() - hours * 3600
        row = self.conn.execute(
            "SELECT SUM(funding_rate) as total FROM funding_history WHERE ticker = ? AND ts >= ?",
            (ticker, since),
        ).fetchone()
        return row["total"] if row and row["total"] else 0.0

    # ── 포지션 관리 (Paper Trading Engine용) ──

    def save_position(
        self,
        product: str,
        perp_size: float,
        perp_entry: float,
        futures_size: float,
        futures_entry: float,
        ts: float | None = None,
    ) -> int:
        """포지션 오픈 기록. 반환: row id."""
        ts = ts or time.time()
        cursor = self.conn.execute(
            """INSERT INTO positions
               (product, perp_size, perp_entry, futures_size, futures_entry,
                status, opened_at)
               VALUES (?, ?, ?, ?, ?, 'open', ?)""",
            (product, perp_size, perp_entry, futures_size, futures_entry, ts),
        )
        self.conn.commit()
        return cursor.lastrowid  # type: ignore

    def close_position(
        self,
        product: str,
        realized_pnl: float = 0,
        funding_pnl: float = 0,
    ):
        """가장 최근 오픈 포지션을 클로즈."""
        ts = time.time()
        self.conn.execute(
            """UPDATE positions
               SET status = 'closed', realized_pnl = ?, funding_pnl = ?, closed_at = ?
               WHERE id = (
                   SELECT id FROM positions
                   WHERE product = ? AND status = 'open'
                   ORDER BY id DESC LIMIT 1
               )""",
            (realized_pnl, funding_pnl, ts, product),
        )
        self.conn.commit()

    def get_open_positions(self) -> list[dict]:
        """오픈 포지션 목록."""
        rows = self.conn.execute(
            "SELECT * FROM positions WHERE status = 'open' ORDER BY opened_at DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def update_daily_pnl(
        self,
        product: str,
        trading_pnl: float = 0,
        funding_pnl: float = 0,
        fees: float = 0,
        dt: str | None = None,
    ):
        """일일 PnL 업데이트 (UPSERT)."""
        from datetime import date as date_cls
        dt = dt or date_cls.today().isoformat()
        net = trading_pnl + funding_pnl - fees

        self.conn.execute(
            """INSERT INTO daily_pnl (date, product, trading_pnl, funding_pnl, fees, net_pnl, num_trades)
               VALUES (?, ?, ?, ?, ?, ?, 1)
               ON CONFLICT(date, product) DO UPDATE SET
                 trading_pnl = trading_pnl + excluded.trading_pnl,
                 funding_pnl = funding_pnl + excluded.funding_pnl,
                 fees = fees + excluded.fees,
                 net_pnl = net_pnl + excluded.net_pnl,
                 num_trades = num_trades + 1""",
            (dt, product, trading_pnl, funding_pnl, fees, net),
        )
        self.conn.commit()

    def get_daily_pnl_summary(self, days: int = 7) -> list[dict]:
        """최근 N일 일일 PnL 요약."""
        rows = self.conn.execute(
            """SELECT date, SUM(trading_pnl) as trading_pnl,
                      SUM(funding_pnl) as funding_pnl,
                      SUM(fees) as fees, SUM(net_pnl) as net_pnl,
                      SUM(num_trades) as num_trades
               FROM daily_pnl
               GROUP BY date
               ORDER BY date DESC
               LIMIT ?""",
            (days,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_trade_history(self, limit: int = 50) -> list[dict]:
        """최근 주문 이력."""
        rows = self.conn.execute(
            "SELECT * FROM orders WHERE is_paper = 1 ORDER BY ts DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]
