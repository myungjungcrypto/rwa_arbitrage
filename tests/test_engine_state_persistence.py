"""Phase M1 회귀 — schema v3 + engine_state 스냅샷 저장.

검증:
  1. fresh DB → v3 시작
  2. v2 DB → v3로 자동 업그레이드 (engine_state 테이블만 추가, 백업 없음)
  3. v3 idempotent (재연결 시 추가 변경 없음)
  4. save_engine_state / get_latest / get_history 정상 동작
  5. 정리 (cleanup_older_than) 정상 동작
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
    MIGRATION_V2_ALTER_COLUMNS,
    MIGRATION_V2_LEG_PRICES,
    MIGRATION_V2_PAIRS,
    MIGRATION_V2_SCHEMA_META,
    MIGRATION_V2_SEED_LEGACY_PAIR,
    SCHEMA_VERSION,
    Storage,
)


@pytest.fixture
def fresh_db_path(tmp_path: Path) -> str:
    return str(tmp_path / "fresh.db")


@pytest.fixture
def v2_db_path(tmp_path: Path) -> str:
    """v2 단계까지만 마이그레이션된 DB. v3로 자동 업그레이드되는지 검증용."""
    db_path = str(tmp_path / "v2.db")
    con = sqlite3.connect(db_path)
    con.executescript(CREATE_TABLES_SQL)
    con.executescript(MIGRATION_V2_SCHEMA_META)
    con.executescript(MIGRATION_V2_PAIRS)
    con.executescript(MIGRATION_V2_LEG_PRICES)
    cur = con.cursor()
    for table, col, type_def in MIGRATION_V2_ALTER_COLUMNS:
        existing = cur.execute(f"PRAGMA table_info({table})").fetchall()
        if not any(r[1] == col for r in existing):
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {type_def}")
    con.executescript(MIGRATION_V2_SEED_LEGACY_PAIR)
    con.execute(
        "INSERT OR REPLACE INTO schema_meta (key, value, updated_at) VALUES ('version','2',?)",
        (time.time(),),
    )
    # 데이터 1줄 — 백업 트리거 위해 (사실 v3는 백업 안 함; 하지만 _has_data 조건은 만족)
    con.execute(
        """INSERT INTO basis_spread
             (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts,
              pair_id, leg_a_price, leg_b_price)
             VALUES ('wti', 80, 80, 0, 0, 0, ?, 'wti_cme_hl', 80, 80)""",
        (time.time(),),
    )
    con.commit()
    con.close()
    return db_path


# ──────────────────────────────────────────────
# Schema version
# ──────────────────────────────────────────────


def test_fresh_db_lands_on_v3(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    assert SCHEMA_VERSION == 3
    assert s._get_schema_version() == 3
    s.close()


def test_v2_db_upgrades_to_v3(v2_db_path):
    # connect 전: v2
    con = sqlite3.connect(v2_db_path)
    row = con.execute("SELECT value FROM schema_meta WHERE key='version'").fetchone()
    assert row[0] == "2"
    con.close()

    # connect 후: v3
    s = Storage(v2_db_path)
    s.connect()
    assert s._get_schema_version() == 3
    s.close()


def test_v3_creates_engine_state_table(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    n = s.conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='engine_state'"
    ).fetchone()[0]
    assert n == 1
    # 인덱스도 함께
    idx = s.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_engine_state_pair_ts'"
    ).fetchone()
    assert idx is not None
    s.close()


def test_v3_idempotent_no_change_on_reconnect(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_engine_state("wti_cme_hl", {"total_signals": 1})
    n_before = s.conn.execute("SELECT COUNT(*) FROM engine_state").fetchone()[0]
    s.close()

    s2 = Storage(fresh_db_path)
    s2.connect()
    n_after = s2.conn.execute("SELECT COUNT(*) FROM engine_state").fetchone()[0]
    assert n_after == n_before
    assert s2._get_schema_version() == 3
    s2.close()


# ──────────────────────────────────────────────
# save_engine_state
# ──────────────────────────────────────────────


def test_save_engine_state_inserts_row(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    state = {
        "total_signals": 1234,
        "total_entries": 5,
        "total_exits": 5,
        "rejected_by_risk": 0,
        "failed_orders": 0,
        "open_positions": 0,
        "closed_trades": 5,
        "cumulative_pnl_usd": -19.78,
        "entry_signals_generated": 48,
        "entry_exec_filter_skip": 47,
        "entry_warmup_skip": 0,
        "entry_min_abs_skip": 0,
    }
    basis_stats = {"mean": -2.0, "std": 3.0, "min": -15.5, "max": 19.9, "count": 5829}
    rid = s.save_engine_state("wti_cme_hl", state, basis_stats)
    assert rid > 0
    row = s.conn.execute(
        "SELECT * FROM engine_state WHERE id=?", (rid,)
    ).fetchone()
    assert row["pair_id"] == "wti_cme_hl"
    assert row["total_signals"] == 1234
    assert row["entry_exec_filter_skip"] == 47
    assert row["cumulative_pnl_usd"] == -19.78
    assert row["basis_mean_bps"] == -2.0
    assert row["basis_std_bps"] == 3.0
    assert row["basis_n"] == 5829
    s.close()


def test_save_engine_state_partial_state_uses_defaults(fresh_db_path):
    """state dict이 일부 키만 갖고 있어도 기본값(0)으로 채움."""
    s = Storage(fresh_db_path)
    s.connect()
    rid = s.save_engine_state("wti_cme_hl", {"total_signals": 100})
    row = s.conn.execute("SELECT * FROM engine_state WHERE id=?", (rid,)).fetchone()
    assert row["total_signals"] == 100
    assert row["total_entries"] == 0
    assert row["cumulative_pnl_usd"] == 0
    assert row["basis_mean_bps"] is None
    s.close()


def test_save_engine_state_no_basis_stats(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    rid = s.save_engine_state("wti_cme_hl", {"total_signals": 1}, basis_stats=None)
    row = s.conn.execute("SELECT * FROM engine_state WHERE id=?", (rid,)).fetchone()
    assert row["basis_mean_bps"] is None
    assert row["basis_std_bps"] is None
    assert row["basis_n"] is None
    s.close()


# ──────────────────────────────────────────────
# get_latest_engine_state / get_engine_state_history
# ──────────────────────────────────────────────


def test_get_latest_returns_most_recent(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    base = time.time()
    s.save_engine_state("wti_cme_hl", {"total_signals": 1}, ts=base - 10)
    s.save_engine_state("wti_cme_hl", {"total_signals": 2}, ts=base - 5)
    s.save_engine_state("wti_cme_hl", {"total_signals": 3}, ts=base)
    latest = s.get_latest_engine_state("wti_cme_hl")
    assert latest is not None
    assert latest["total_signals"] == 3
    s.close()


def test_get_latest_returns_none_when_empty(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    assert s.get_latest_engine_state("wti_cme_hl") is None
    s.close()


def test_get_latest_isolates_by_pair_id(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_engine_state("wti_cme_hl", {"total_signals": 100})
    s.save_engine_state("wti_hl_binance", {"total_signals": 200})
    a = s.get_latest_engine_state("wti_cme_hl")
    b = s.get_latest_engine_state("wti_hl_binance")
    assert a["total_signals"] == 100
    assert b["total_signals"] == 200
    s.close()


def test_history_filters_by_time_window(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    base = time.time()
    s.save_engine_state("wti_cme_hl", {"total_signals": 1}, ts=base - 7200)   # 2h 전
    s.save_engine_state("wti_cme_hl", {"total_signals": 2}, ts=base - 1800)   # 30m 전
    s.save_engine_state("wti_cme_hl", {"total_signals": 3}, ts=base - 600)    # 10m 전
    rows = s.get_engine_state_history("wti_cme_hl", hours=1.0)
    # 1시간 윈도우 → 30m, 10m 전만 포함
    assert len(rows) == 2
    assert rows[0]["total_signals"] == 2  # 시간순 (오래된→최신)
    assert rows[1]["total_signals"] == 3
    s.close()


def test_history_isolates_by_pair_id(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    s.save_engine_state("wti_cme_hl", {"total_signals": 1})
    s.save_engine_state("wti_hl_binance", {"total_signals": 2})
    s.save_engine_state("wti_cme_hl", {"total_signals": 3})
    a = s.get_engine_state_history("wti_cme_hl", hours=24)
    b = s.get_engine_state_history("wti_hl_binance", hours=24)
    assert len(a) == 2
    assert len(b) == 1
    s.close()


# ──────────────────────────────────────────────
# Cleanup
# ──────────────────────────────────────────────


def test_cleanup_removes_old_rows(fresh_db_path):
    s = Storage(fresh_db_path)
    s.connect()
    base = time.time()
    s.save_engine_state("wti_cme_hl", {"total_signals": 1}, ts=base - 31 * 86400)
    s.save_engine_state("wti_cme_hl", {"total_signals": 2}, ts=base - 5 * 86400)
    s.save_engine_state("wti_cme_hl", {"total_signals": 3}, ts=base)
    deleted = s.cleanup_engine_state_older_than(days=30)
    assert deleted == 1
    remaining = s.conn.execute("SELECT COUNT(*) FROM engine_state").fetchone()[0]
    assert remaining == 2
    s.close()


# ──────────────────────────────────────────────
# v2 데이터 보존 (regression)
# ──────────────────────────────────────────────


def test_v2_to_v3_preserves_existing_data(v2_db_path):
    """v2에서 v3로 업그레이드 시 기존 row 보존."""
    con = sqlite3.connect(v2_db_path)
    n_before = con.execute("SELECT COUNT(*) FROM basis_spread").fetchone()[0]
    con.close()

    s = Storage(v2_db_path)
    s.connect()
    n_after = s.conn.execute("SELECT COUNT(*) FROM basis_spread").fetchone()[0]
    assert n_after == n_before
    s.close()


def test_v2_to_v3_no_extra_backup_created(v2_db_path):
    """v3 업그레이드 시 추가 백업 생성 안 함 (additive only)."""
    s = Storage(v2_db_path)
    s.connect()
    backup_v3 = f"{v2_db_path}.pre-v3.bak"
    assert not Path(backup_v3).exists()
    s.close()
