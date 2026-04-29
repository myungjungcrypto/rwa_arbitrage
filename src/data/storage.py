from __future__ import annotations
"""SQLite 데이터 저장소.

베이시스, 시세, 펀딩레이트, 주문, PnL 등을 시계열로 저장.

스키마 버전 (v2, 2026-04-28): pair_id 추가 + leg_prices 통합 테이블.
기존 컬럼은 유지하고 추가만(additive migration), legacy row는 connect() 시
자동 backfill. 자세한 내용은 `MIGRATION_V2_*` 상수 참고.
"""


import os
import sqlite3
import time
import shutil
import logging
from pathlib import Path
from dataclasses import asdict
from typing import Any

logger = logging.getLogger("arbitrage.storage")


# ──────────────────────────────────────────────
# Schema version
# ──────────────────────────────────────────────

SCHEMA_VERSION = 3

# legacy product → pair_id 매핑 (Phase B backward compat)
LEGACY_PRODUCT_PAIR_MAP = {
    "wti": "wti_cme_hl",
    "brent": "brent_cme_hl",
}

# legacy product → 어느 leg 어느 거래소인지 (orders 테이블 backfill용)
LEGACY_LEG_EXCHANGE_MAP = {
    "perp": "hyperliquid",
    "futures": "kis",
}


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


# ──────────────────────────────────────────────
# Migration v2 — additive only
# ──────────────────────────────────────────────

MIGRATION_V2_SCHEMA_META = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at REAL NOT NULL
);
"""

MIGRATION_V2_PAIRS = """
CREATE TABLE IF NOT EXISTS pairs (
    pair_id TEXT PRIMARY KEY,
    leg_a_exchange TEXT NOT NULL,
    leg_a_symbol TEXT NOT NULL,
    leg_a_role TEXT NOT NULL,
    leg_b_exchange TEXT NOT NULL,
    leg_b_symbol TEXT NOT NULL,
    leg_b_role TEXT NOT NULL,
    strategy TEXT NOT NULL DEFAULT 'basis_convergence',
    gate TEXT NOT NULL DEFAULT 'cme_hours',
    created_at REAL NOT NULL
);
"""

MIGRATION_V2_LEG_PRICES = """
CREATE TABLE IF NOT EXISTS leg_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    leg TEXT NOT NULL,                  -- 'a' | 'b'
    exchange TEXT NOT NULL,
    symbol TEXT NOT NULL,
    mid_price REAL NOT NULL,
    bid REAL,
    ask REAL,
    bid_qty REAL,
    ask_qty REAL,
    index_price REAL,
    funding_rate REAL,
    funding_interval_hours REAL,
    contract_month TEXT,
    volume_24h REAL,
    ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_leg_prices_pair_ts ON leg_prices(pair_id, leg, ts);
"""

# (table, column, type_with_default)
MIGRATION_V2_ALTER_COLUMNS = [
    ("basis_spread", "pair_id", "TEXT"),
    ("basis_spread", "leg_a_price", "REAL"),
    ("basis_spread", "leg_b_price", "REAL"),
    ("perp_prices", "exchange", "TEXT DEFAULT 'hyperliquid'"),
    ("futures_prices", "exchange", "TEXT DEFAULT 'kis'"),
    ("orders", "pair_id", "TEXT"),
    ("orders", "exchange", "TEXT"),
    ("positions", "pair_id", "TEXT"),
    ("daily_pnl", "pair_id", "TEXT"),
]

# legacy 단일 페어 시드 (wti 운영 데이터를 새 스키마와 연결)
MIGRATION_V2_SEED_LEGACY_PAIR = """
INSERT OR IGNORE INTO pairs
    (pair_id, leg_a_exchange, leg_a_symbol, leg_a_role,
     leg_b_exchange, leg_b_symbol, leg_b_role, strategy, gate, created_at)
VALUES
    ('wti_cme_hl', 'hyperliquid', 'xyz:CL', 'perp',
     'kis', 'MCLM26', 'dated_futures', 'basis_convergence', 'cme_hours',
     strftime('%s','now'));
"""

MIGRATION_V2_BACKFILL = [
    """UPDATE basis_spread
          SET pair_id='wti_cme_hl',
              leg_a_price=perp_price,
              leg_b_price=futures_price
        WHERE pair_id IS NULL AND product='wti'""",
    """UPDATE orders
          SET pair_id='wti_cme_hl',
              exchange = CASE leg WHEN 'perp' THEN 'hyperliquid' ELSE 'kis' END
        WHERE pair_id IS NULL AND product='wti'""",
    """UPDATE positions
          SET pair_id='wti_cme_hl'
        WHERE pair_id IS NULL AND product='wti'""",
    """UPDATE daily_pnl
          SET pair_id='wti_cme_hl'
        WHERE pair_id IS NULL AND product='wti'""",
]


# ──────────────────────────────────────────────
# Migration v3 — engine_state snapshot 테이블 (대시보드용)
# ──────────────────────────────────────────────
#
# 봇이 30초마다 EngineState dataclass + 최근 basis 통계를 dump.
# Streamlit 대시보드가 이 테이블을 읽어 live counter 표시.
# additive only — 백업 불필요 (v2에서 이미 백업됨).

MIGRATION_V3_ENGINE_STATE = """
CREATE TABLE IF NOT EXISTS engine_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    ts REAL NOT NULL,
    total_signals INTEGER DEFAULT 0,
    total_entries INTEGER DEFAULT 0,
    total_exits INTEGER DEFAULT 0,
    rejected_by_risk INTEGER DEFAULT 0,
    failed_orders INTEGER DEFAULT 0,
    open_positions INTEGER DEFAULT 0,
    closed_trades INTEGER DEFAULT 0,
    cumulative_pnl_usd REAL DEFAULT 0,
    entry_signals_generated INTEGER DEFAULT 0,
    entry_exec_filter_skip INTEGER DEFAULT 0,
    entry_warmup_skip INTEGER DEFAULT 0,
    entry_min_abs_skip INTEGER DEFAULT 0,
    basis_mean_bps REAL,
    basis_std_bps REAL,
    basis_min_bps REAL,
    basis_max_bps REAL,
    basis_n INTEGER
);
CREATE INDEX IF NOT EXISTS idx_engine_state_pair_ts ON engine_state(pair_id, ts);
"""


class Storage:
    """SQLite 저장소 관리."""

    def __init__(self, db_path: str = "data/arbitrage.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._conn: sqlite3.Connection | None = None

    def connect(self):
        """DB 연결 + 테이블 생성 + 자동 마이그레이션."""
        self._conn = sqlite3.connect(self.db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(CREATE_TABLES_SQL)
        self._conn.commit()
        self._auto_migrate()
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

    # ── 마이그레이션 ──

    def _get_schema_version(self) -> int:
        """schema_meta가 없거나 비어있으면 1 (legacy)."""
        try:
            row = self.conn.execute(
                "SELECT value FROM schema_meta WHERE key='version'"
            ).fetchone()
            if row:
                return int(row["value"])
        except sqlite3.OperationalError:
            pass  # 테이블 없음
        return 1

    def _column_exists(self, table: str, column: str) -> bool:
        rows = self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        return any(r["name"] == column for r in rows)

    def _has_data(self) -> bool:
        """기존 운영 데이터(basis_spread) 유무 — 마이그레이션 백업 결정용."""
        try:
            row = self.conn.execute(
                "SELECT COUNT(*) AS n FROM basis_spread"
            ).fetchone()
            return bool(row and row["n"] > 0)
        except sqlite3.OperationalError:
            return False

    def _backup_once(self, version: int) -> None:
        """v{N} 마이그레이션 직전 1회 백업. 백업 파일 이미 있으면 skip."""
        backup_path = f"{self.db_path}.pre-v{version}.bak"
        if Path(backup_path).exists():
            logger.info(f"Backup already exists: {backup_path} (skip)")
            return
        if not Path(self.db_path).exists():
            return
        # WAL 등 dirty 상태 안전한 백업 — sqlite3.Connection.backup() 사용
        backup_conn = sqlite3.connect(backup_path)
        try:
            self.conn.backup(backup_conn)
            logger.warning(f"DB backup created: {backup_path}")
        finally:
            backup_conn.close()

    def _auto_migrate(self) -> None:
        """현재 버전이 SCHEMA_VERSION 미만이면 단계별 마이그레이션 실행.

        백업 정책: v1 → v2는 ALTER TABLE 다수라 위험 → 백업.
        v2 → v3는 CREATE TABLE only (additive) → 백업 불필요.
        """
        # schema_meta 테이블은 항상 생성 (v1 DB도 OK)
        self.conn.executescript(MIGRATION_V2_SCHEMA_META)
        self.conn.commit()

        current = self._get_schema_version()
        if current >= SCHEMA_VERSION:
            return

        had_data = self._has_data()
        if had_data:
            logger.warning(f"Migrating DB from v{current} to v{SCHEMA_VERSION}")
            # v1에서 시작할 때만 백업 (v1→v2 단계에 ALTER TABLE 다수 포함)
            if current < 2:
                self._backup_once(2)

        # v1 → v2
        if current < 2:
            self._migrate_to_v2()

        # v2 → v3 (additive only — engine_state 추가, 백업 불필요)
        if current < 3:
            self._migrate_to_v3()

        # 버전 기록
        self.conn.execute(
            "INSERT OR REPLACE INTO schema_meta (key, value, updated_at) VALUES (?, ?, ?)",
            ("version", str(SCHEMA_VERSION), time.time()),
        )
        self.conn.commit()
        if had_data:
            logger.warning(f"DB migrated to schema v{SCHEMA_VERSION}")

    def _migrate_to_v2(self) -> None:
        """v1 → v2: 컬럼 추가 + pairs/leg_prices 신규 + legacy backfill (additive)."""
        cur = self.conn.cursor()
        cur.executescript(MIGRATION_V2_PAIRS)
        cur.executescript(MIGRATION_V2_LEG_PRICES)

        for table, col, type_def in MIGRATION_V2_ALTER_COLUMNS:
            if not self._column_exists(table, col):
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {type_def}")

        cur.executescript(MIGRATION_V2_SEED_LEGACY_PAIR)
        for stmt in MIGRATION_V2_BACKFILL:
            cur.execute(stmt)
        self.conn.commit()

    def _migrate_to_v3(self) -> None:
        """v2 → v3: engine_state 스냅샷 테이블 신규 (additive only)."""
        self.conn.executescript(MIGRATION_V3_ENGINE_STATE)
        self.conn.commit()

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
        """베이시스 스프레드 저장. v2 신규 컬럼(pair_id/leg_a_price/leg_b_price)도 함께 채움."""
        ts = ts or time.time()
        basis = perp_price - futures_price
        basis_bps = basis / futures_price * 10_000 if futures_price else 0
        pair_id = LEGACY_PRODUCT_PAIR_MAP.get(product)
        self.conn.execute(
            """INSERT INTO basis_spread
               (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts,
                pair_id, leg_a_price, leg_b_price)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts,
             pair_id, perp_price, futures_price),
        )
        self.conn.commit()

    def save_basis_by_pair(
        self,
        pair_id: str,
        leg_a_price: float,
        leg_b_price: float,
        funding_rate: float = 0,
        product: str | None = None,
        ts: float | None = None,
    ):
        """v2 forward-path: pair_id 기반 basis 저장.

        legacy `product` 필드도 채움 (조회 호환성). product 미지정 시 pair_id에서
        역산 (`wti_cme_hl` → `wti`).
        """
        ts = ts or time.time()
        basis = leg_a_price - leg_b_price
        basis_bps = basis / leg_b_price * 10_000 if leg_b_price else 0
        if product is None:
            product = pair_id.split("_", 1)[0]   # "wti_cme_hl" → "wti"
        self.conn.execute(
            """INSERT INTO basis_spread
               (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts,
                pair_id, leg_a_price, leg_b_price)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (product, leg_a_price, leg_b_price, basis, basis_bps, funding_rate, ts,
             pair_id, leg_a_price, leg_b_price),
        )
        self.conn.commit()

    def save_leg_quote(
        self,
        pair_id: str,
        leg: str,
        exchange: str,
        symbol: str,
        mid_price: float,
        bid: float = 0.0,
        ask: float = 0.0,
        bid_qty: float = 0.0,
        ask_qty: float = 0.0,
        index_price: float = 0.0,
        funding_rate: float = 0.0,
        funding_interval_hours: float = 0.0,
        contract_month: str = "",
        volume_24h: float = 0.0,
        ts: float | None = None,
    ):
        """v2 forward-path: leg_prices 통합 테이블에 호가 1건 저장.

        Phase C 콜렉터가 사용. leg는 'a' | 'b'.
        """
        ts = ts or time.time()
        self.conn.execute(
            """INSERT INTO leg_prices
               (pair_id, leg, exchange, symbol, mid_price, bid, ask, bid_qty, ask_qty,
                index_price, funding_rate, funding_interval_hours, contract_month,
                volume_24h, ts)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (pair_id, leg, exchange, symbol, mid_price, bid, ask, bid_qty, ask_qty,
             index_price, funding_rate, funding_interval_hours, contract_month,
             volume_24h, ts),
        )
        self.conn.commit()

    def upsert_pair(
        self,
        pair_id: str,
        leg_a_exchange: str,
        leg_a_symbol: str,
        leg_a_role: str,
        leg_b_exchange: str,
        leg_b_symbol: str,
        leg_b_role: str,
        strategy: str = "basis_convergence",
        gate: str = "cme_hours",
    ) -> None:
        """pairs 테이블에 페어 정의 등록 (idempotent)."""
        self.conn.execute(
            """INSERT INTO pairs
                 (pair_id, leg_a_exchange, leg_a_symbol, leg_a_role,
                  leg_b_exchange, leg_b_symbol, leg_b_role, strategy, gate, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT(pair_id) DO UPDATE SET
                   leg_a_exchange=excluded.leg_a_exchange,
                   leg_a_symbol=excluded.leg_a_symbol,
                   leg_a_role=excluded.leg_a_role,
                   leg_b_exchange=excluded.leg_b_exchange,
                   leg_b_symbol=excluded.leg_b_symbol,
                   leg_b_role=excluded.leg_b_role,
                   strategy=excluded.strategy,
                   gate=excluded.gate""",
            (pair_id, leg_a_exchange, leg_a_symbol, leg_a_role,
             leg_b_exchange, leg_b_symbol, leg_b_role, strategy, gate, time.time()),
        )
        self.conn.commit()

    def get_pairs(self) -> list[dict]:
        """등록된 페어 목록."""
        rows = self.conn.execute(
            "SELECT * FROM pairs ORDER BY pair_id"
        ).fetchall()
        return [dict(r) for r in rows]

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
        pair_id: str | None = None,
        exchange: str | None = None,
    ) -> int:
        """주문 기록 저장. v2 컬럼(pair_id/exchange) 함께 채움. 반환: row id."""
        ts = ts or time.time()
        if pair_id is None:
            pair_id = LEGACY_PRODUCT_PAIR_MAP.get(product)
        if exchange is None:
            exchange = LEGACY_LEG_EXCHANGE_MAP.get(leg)
        cursor = self.conn.execute(
            """INSERT INTO orders
               (order_id, product, leg, side, size, price, filled_price,
                filled_size, status, is_paper, ts, pair_id, exchange)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (order_id, product, leg, side, size, price, filled_price,
             filled_size, status, 1 if is_paper else 0, ts, pair_id, exchange),
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

    def get_all_basis(self, product: str) -> list[dict]:
        """전체 베이시스 데이터 로드 (백테스트용)."""
        rows = self.conn.execute(
            """SELECT * FROM basis_spread
               WHERE product = ?
               ORDER BY ts ASC""",
            (product,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_basis_range(self, product: str, start_ts: float, end_ts: float) -> list[dict]:
        """특정 시간 범위의 베이시스 데이터 로드."""
        rows = self.conn.execute(
            """SELECT * FROM basis_spread
               WHERE product = ? AND ts >= ? AND ts <= ?
               ORDER BY ts ASC""",
            (product, start_ts, end_ts),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_spread_stats(self, product: str, hours: float = 24) -> dict:
        """futures bid/ask 스프레드 통계 (백테스트 스프레드 추정용)."""
        since = time.time() - hours * 3600
        # futures_prices에서 bid/ask 스프레드 계산
        row = self.conn.execute(
            """SELECT
                AVG((ask - bid) / ((ask + bid) / 2) * 10000) as avg_spread_bps,
                MIN((ask - bid) / ((ask + bid) / 2) * 10000) as min_spread_bps,
                MAX((ask - bid) / ((ask + bid) / 2) * 10000) as max_spread_bps,
                COUNT(*) as n
               FROM futures_prices
               WHERE symbol = ? AND ts >= ? AND bid > 0 AND ask > 0""",
            (product.upper() if product == "mcl" else
             {"wti": "MCL", "brent": "BZ"}.get(product, product), since),
        ).fetchone()
        if row:
            return dict(row)
        return {"avg_spread_bps": 0, "min_spread_bps": 0, "max_spread_bps": 0, "n": 0}

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

    def get_basis_history(self, product: str, hours: float = 24) -> list[float]:
        """basis_bps 리스트 반환 (시간순, 오래된→최신).

        SignalGenerator 부트스트랩용.
        deque maxlen이 자동으로 잘라주므로 전체 DB 데이터를 로드하여
        window를 최대한 채운다.
        """
        rows = self.conn.execute(
            "SELECT basis_bps FROM basis_spread WHERE product = ? ORDER BY ts ASC",
            (product,),
        ).fetchall()
        return [row["basis_bps"] for row in rows]

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
        pair_id: str | None = None,
    ) -> int:
        """포지션 오픈 기록. v2 pair_id 함께 채움. 반환: row id."""
        ts = ts or time.time()
        if pair_id is None:
            pair_id = LEGACY_PRODUCT_PAIR_MAP.get(product)
        cursor = self.conn.execute(
            """INSERT INTO positions
               (product, perp_size, perp_entry, futures_size, futures_entry,
                status, opened_at, pair_id)
               VALUES (?, ?, ?, ?, ?, 'open', ?, ?)""",
            (product, perp_size, perp_entry, futures_size, futures_entry, ts, pair_id),
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
        pair_id: str | None = None,
    ):
        """일일 PnL 업데이트 (UPSERT). v2 pair_id 함께 채움."""
        from datetime import date as date_cls
        dt = dt or date_cls.today().isoformat()
        net = trading_pnl + funding_pnl - fees
        if pair_id is None:
            pair_id = LEGACY_PRODUCT_PAIR_MAP.get(product)

        self.conn.execute(
            """INSERT INTO daily_pnl
                 (date, product, trading_pnl, funding_pnl, fees, net_pnl, num_trades, pair_id)
                 VALUES (?, ?, ?, ?, ?, ?, 1, ?)
                 ON CONFLICT(date, product) DO UPDATE SET
                   trading_pnl = trading_pnl + excluded.trading_pnl,
                   funding_pnl = funding_pnl + excluded.funding_pnl,
                   fees = fees + excluded.fees,
                   net_pnl = net_pnl + excluded.net_pnl,
                   num_trades = num_trades + 1,
                   pair_id = COALESCE(daily_pnl.pair_id, excluded.pair_id)""",
            (dt, product, trading_pnl, funding_pnl, fees, net, pair_id),
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

    # ── Engine state snapshot (Phase M1, 대시보드용) ──

    def save_engine_state(
        self,
        pair_id: str,
        state: dict,
        basis_stats: dict | None = None,
        ts: float | None = None,
    ) -> int:
        """EngineState dataclass + 최근 basis 통계를 1 row로 INSERT.

        engine.state_snapshot_loop이 30초마다 호출. 대시보드는 이 row를 polling.

        Args:
            pair_id: e.g. 'wti_cme_hl'
            state: EngineState 필드 dict (asdict(engine._state))
            basis_stats: {'mean': .., 'std': .., 'min': .., 'max': .., 'count': ..} 옵션
        """
        ts = ts or time.time()
        bs = basis_stats or {}
        cursor = self.conn.execute(
            """INSERT INTO engine_state
                 (pair_id, ts,
                  total_signals, total_entries, total_exits,
                  rejected_by_risk, failed_orders,
                  open_positions, closed_trades, cumulative_pnl_usd,
                  entry_signals_generated, entry_exec_filter_skip,
                  entry_warmup_skip, entry_min_abs_skip,
                  basis_mean_bps, basis_std_bps, basis_min_bps, basis_max_bps,
                  basis_n)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                pair_id, ts,
                state.get("total_signals", 0),
                state.get("total_entries", 0),
                state.get("total_exits", 0),
                state.get("rejected_by_risk", 0),
                state.get("failed_orders", 0),
                state.get("open_positions", 0),
                state.get("closed_trades", 0),
                state.get("cumulative_pnl_usd", 0.0),
                state.get("entry_signals_generated", 0),
                state.get("entry_exec_filter_skip", 0),
                state.get("entry_warmup_skip", 0),
                state.get("entry_min_abs_skip", 0),
                bs.get("mean"),
                bs.get("std"),
                bs.get("min"),
                bs.get("max"),
                bs.get("count"),
            ),
        )
        self.conn.commit()
        return cursor.lastrowid  # type: ignore

    def get_latest_engine_state(self, pair_id: str) -> dict | None:
        """가장 최근 engine_state 1건. 없으면 None."""
        row = self.conn.execute(
            "SELECT * FROM engine_state WHERE pair_id = ? ORDER BY ts DESC LIMIT 1",
            (pair_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_engine_state_history(self, pair_id: str, hours: float = 24) -> list[dict]:
        """최근 N시간 engine_state 시계열 (대시보드 funnel/trend 차트용)."""
        since = time.time() - hours * 3600
        rows = self.conn.execute(
            """SELECT * FROM engine_state
                 WHERE pair_id = ? AND ts >= ?
                 ORDER BY ts ASC""",
            (pair_id, since),
        ).fetchall()
        return [dict(r) for r in rows]

    def cleanup_engine_state_older_than(self, days: float = 30) -> int:
        """오래된 snapshot 정리 (운영 도구). 반환: 삭제 row 수."""
        cutoff = time.time() - days * 86400
        cur = self.conn.execute(
            "DELETE FROM engine_state WHERE ts < ?", (cutoff,)
        )
        self.conn.commit()
        return cur.rowcount
