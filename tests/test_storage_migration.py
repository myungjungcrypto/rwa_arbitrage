"""Phase B 회귀 — DB schema v1 → v2 자동 마이그레이션.

검증:
  1. fresh DB → v2로 시작
  2. legacy v1 데이터 있는 DB → 자동 v2로 업그레이드 + 백필
  3. v2 DB 재연결 → 추가 변경 없음 (idempotent)
  4. 신규 메서드(save_basis_by_pair, save_leg_quote, upsert_pair)
  5. 기존 메서드(save_basis 등) 후방 호환 (legacy 호출 그대로 작동)
  6. 백업 파일 1회만 생성
"""

from __future__ import annotations

import os
import sqlite3
import time
import tempfile
from pathlib import Path

import pytest

from src.data.storage import (
    CREATE_TABLES_SQL,
    LEGACY_PRODUCT_PAIR_MAP,
    SCHEMA_VERSION,
    Storage,
)


@pytest.fixture
def fresh_db_path(tmp_path: Path) -> str:
    return str(tmp_path / "test.db")


@pytest.fixture
def legacy_v1_db_path(tmp_path: Path) -> str:
    """v1 스키마 DB 만들고 legacy 데이터 일부 주입."""
    db_path = str(tmp_path / "legacy.db")
    con = sqlite3.connect(db_path)
    con.executescript(CREATE_TABLES_SQL)
    # legacy 데이터
    con.execute(
        """INSERT INTO basis_spread
             (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts)
             VALUES ('wti', 80.10, 80.00, 0.10, 12.5, 0.0001, ?)""",
        (time.time() - 3600,),
    )
    con.execute(
        """INSERT INTO basis_spread
             (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts)
             VALUES ('wti', 80.20, 80.05, 0.15, 18.7, 0.00012, ?)""",
        (time.time() - 1800,),
    )
    con.execute(
        """INSERT INTO orders
             (order_id, product, leg, side, size, price, filled_price,
              filled_size, status, is_paper, ts)
             VALUES ('A1', 'wti', 'perp', 'sell', 1, 80.10, 80.10, 1, 'filled', 1, ?)""",
        (time.time() - 3600,),
    )
    con.execute(
        """INSERT INTO orders
             (order_id, product, leg, side, size, price, filled_price,
              filled_size, status, is_paper, ts)
             VALUES ('A2', 'wti', 'futures', 'buy', 1, 80.00, 80.00, 1, 'filled', 1, ?)""",
        (time.time() - 3600,),
    )
    con.execute(
        """INSERT INTO positions
             (product, perp_size, perp_entry, futures_size, futures_entry,
              status, opened_at)
             VALUES ('wti', -1, 80.10, 1, 80.00, 'closed', ?)""",
        (time.time() - 3600,),
    )
    con.execute(
        """INSERT INTO daily_pnl
             (date, product, trading_pnl, funding_pnl, fees, net_pnl, num_trades)
             VALUES ('2026-04-25', 'wti', 100.0, 0.5, 13.4, 87.1, 5)""",
    )
    con.commit()
    con.close()
    return db_path


# ──────────────────────────────────────────────
# Fresh DB
# ──────────────────────────────────────────────


def test_fresh_db_starts_at_target_version(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    assert s._get_schema_version() == SCHEMA_VERSION
    s.close()


def test_fresh_db_has_v2_tables(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    con = s.conn
    # v2 신규 테이블 모두 존재
    for tbl in ("schema_meta", "pairs", "leg_prices"):
        n = con.execute(
            f"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tbl}'"
        ).fetchone()[0]
        assert n == 1, f"{tbl} 테이블 없음"
    s.close()


def test_fresh_db_seeds_legacy_pair(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    rows = s.get_pairs()
    pair_ids = [r["pair_id"] for r in rows]
    assert "wti_cme_hl" in pair_ids
    s.close()


def test_fresh_db_no_backup_created(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    # fresh DB(데이터 없음)에서는 백업 생성 안 함
    backup_path = f"{fresh_db_path}.pre-v{SCHEMA_VERSION}.bak"
    assert not Path(backup_path).exists()
    s.close()


# ──────────────────────────────────────────────
# Legacy v1 → v2 migration
# ──────────────────────────────────────────────


def test_legacy_v1_db_migrates_on_connect(legacy_v1_db_path):
    s = Storage(legacy_v1_db_path)
    s.connect()
    assert s._get_schema_version() == SCHEMA_VERSION
    s.close()


def test_legacy_data_backfilled_with_pair_id(legacy_v1_db_path):
    s = Storage(legacy_v1_db_path)
    s.connect()
    con = s.conn

    # basis_spread: pair_id + leg_a/leg_b_price 채워짐
    rows = con.execute(
        "SELECT product, pair_id, leg_a_price, leg_b_price, perp_price, futures_price "
        "FROM basis_spread"
    ).fetchall()
    assert len(rows) == 2
    for r in rows:
        assert r["product"] == "wti"
        assert r["pair_id"] == "wti_cme_hl"
        assert r["leg_a_price"] == r["perp_price"]
        assert r["leg_b_price"] == r["futures_price"]

    # orders: pair_id + exchange 채워짐
    rows = con.execute("SELECT leg, pair_id, exchange FROM orders").fetchall()
    assert len(rows) == 2
    for r in rows:
        assert r["pair_id"] == "wti_cme_hl"
        if r["leg"] == "perp":
            assert r["exchange"] == "hyperliquid"
        else:
            assert r["exchange"] == "kis"

    # positions: pair_id 채워짐
    rows = con.execute("SELECT pair_id FROM positions").fetchall()
    assert all(r["pair_id"] == "wti_cme_hl" for r in rows)

    # daily_pnl: pair_id 채워짐
    rows = con.execute("SELECT pair_id FROM daily_pnl").fetchall()
    assert all(r["pair_id"] == "wti_cme_hl" for r in rows)

    s.close()


def test_legacy_db_creates_backup(legacy_v1_db_path):
    s = Storage(legacy_v1_db_path)
    s.connect()
    backup_path = f"{legacy_v1_db_path}.pre-v2.bak"
    assert Path(backup_path).exists()
    # 백업이 실제 v1 데이터 보유
    bcon = sqlite3.connect(backup_path)
    n = bcon.execute("SELECT COUNT(*) FROM basis_spread").fetchone()[0]
    assert n == 2
    bcon.close()
    s.close()


def test_legacy_data_preserved_after_migration(legacy_v1_db_path):
    s = Storage(legacy_v1_db_path)
    s.connect()
    con = s.conn
    # 행 수 보존
    assert con.execute("SELECT COUNT(*) FROM basis_spread").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 2
    assert con.execute("SELECT COUNT(*) FROM positions").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM daily_pnl").fetchone()[0] == 1
    # 기존 컬럼 값 보존
    row = con.execute(
        "SELECT perp_price, futures_price FROM basis_spread ORDER BY ts ASC"
    ).fetchone()
    assert row["perp_price"] == 80.10
    assert row["futures_price"] == 80.00
    s.close()


# ──────────────────────────────────────────────
# Idempotency
# ──────────────────────────────────────────────


def test_migration_idempotent_second_connect_no_change(legacy_v1_db_path):
    s = Storage(legacy_v1_db_path)
    s.connect()
    rows1 = s.conn.execute("SELECT * FROM basis_spread").fetchall()
    s.close()

    # 두 번째 연결 — 이미 v2이므로 추가 변경 없음
    s2 = Storage(legacy_v1_db_path)
    s2.connect()
    assert s2._get_schema_version() == SCHEMA_VERSION
    rows2 = s2.conn.execute("SELECT * FROM basis_spread").fetchall()
    assert len(rows1) == len(rows2)
    s2.close()


def test_migration_backup_not_overwritten_on_second_run(legacy_v1_db_path):
    s = Storage(legacy_v1_db_path)
    s.connect()
    backup_path = f"{legacy_v1_db_path}.pre-v2.bak"
    backup_mtime_1 = os.path.getmtime(backup_path)
    s.close()

    # 다시 connect — 백업 mtime 변경 없음 (이미 존재하므로 skip)
    s2 = Storage(legacy_v1_db_path)
    s2.connect()
    backup_mtime_2 = os.path.getmtime(backup_path)
    assert backup_mtime_1 == backup_mtime_2
    s2.close()


# ──────────────────────────────────────────────
# Backward-compat: legacy save_basis 등이 v2 컬럼도 채움
# ──────────────────────────────────────────────


def test_save_basis_legacy_call_fills_v2_columns(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_basis(product="wti", perp_price=80.10, futures_price=80.00,
                 funding_rate=0.0001)
    row = s.conn.execute("SELECT * FROM basis_spread").fetchone()
    assert row["pair_id"] == "wti_cme_hl"
    assert row["leg_a_price"] == 80.10
    assert row["leg_b_price"] == 80.00
    s.close()


def test_save_order_legacy_call_fills_v2_columns(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_order(product="wti", leg="perp", side="sell", size=1.0,
                 price=80.10, filled_price=80.10, filled_size=1.0,
                 status="filled")
    s.save_order(product="wti", leg="futures", side="buy", size=1.0,
                 price=80.00, filled_price=80.00, filled_size=1.0,
                 status="filled")
    rows = s.conn.execute("SELECT leg, pair_id, exchange FROM orders").fetchall()
    assert {r["leg"]: r["exchange"] for r in rows} == {
        "perp": "hyperliquid", "futures": "kis"
    }
    assert all(r["pair_id"] == "wti_cme_hl" for r in rows)
    s.close()


def test_save_position_legacy_call_fills_pair_id(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_position(product="wti", perp_size=-1, perp_entry=80.10,
                    futures_size=1, futures_entry=80.00)
    row = s.conn.execute("SELECT pair_id FROM positions").fetchone()
    assert row["pair_id"] == "wti_cme_hl"
    s.close()


def test_update_daily_pnl_legacy_call_fills_pair_id(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.update_daily_pnl(product="wti", trading_pnl=100, funding_pnl=0,
                       fees=13, dt="2026-04-28")
    row = s.conn.execute("SELECT pair_id FROM daily_pnl").fetchone()
    assert row["pair_id"] == "wti_cme_hl"
    s.close()


# ──────────────────────────────────────────────
# Forward-path: 신규 메서드
# ──────────────────────────────────────────────


def test_save_basis_by_pair(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_basis_by_pair(pair_id="wti_hl_binance",
                          leg_a_price=80.20, leg_b_price=80.05)
    row = s.conn.execute(
        "SELECT * FROM basis_spread WHERE pair_id='wti_hl_binance'"
    ).fetchone()
    assert row["pair_id"] == "wti_hl_binance"
    assert row["leg_a_price"] == 80.20
    assert row["leg_b_price"] == 80.05
    assert row["product"] == "wti"   # pair_id의 첫 토큰
    s.close()


def test_save_leg_quote(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_leg_quote(pair_id="wti_hl_binance", leg="b",
                     exchange="binance", symbol="CLUSDT",
                     mid_price=80.05, bid=80.04, ask=80.06,
                     funding_rate=0.0001, funding_interval_hours=4.0)
    row = s.conn.execute(
        "SELECT * FROM leg_prices WHERE pair_id='wti_hl_binance' AND leg='b'"
    ).fetchone()
    assert row["exchange"] == "binance"
    assert row["symbol"] == "CLUSDT"
    assert row["mid_price"] == 80.05
    assert row["funding_interval_hours"] == 4.0
    s.close()


def test_upsert_pair_idempotent(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.upsert_pair(pair_id="wti_hl_binance",
                  leg_a_exchange="hyperliquid", leg_a_symbol="xyz:CL",
                  leg_a_role="perp",
                  leg_b_exchange="binance", leg_b_symbol="CLUSDT",
                  leg_b_role="perp")
    # 다시 호출 — 중복 안 만듦
    s.upsert_pair(pair_id="wti_hl_binance",
                  leg_a_exchange="hyperliquid", leg_a_symbol="xyz:CL",
                  leg_a_role="perp",
                  leg_b_exchange="binance", leg_b_symbol="CLUSDT",
                  leg_b_role="perp")
    rows = s.conn.execute(
        "SELECT * FROM pairs WHERE pair_id='wti_hl_binance'"
    ).fetchall()
    assert len(rows) == 1
    s.close()


# ──────────────────────────────────────────────
# Sanity: 기존 read 메서드들이 여전히 동작
# ──────────────────────────────────────────────


def test_legacy_read_methods_still_work(legacy_v1_db_path):
    s = Storage(legacy_v1_db_path)
    s.connect()
    assert len(s.get_basis_history("wti", hours=24)) > 0
    stats = s.get_basis_stats("wti", hours=24)
    assert stats["count"] > 0
    s.close()
