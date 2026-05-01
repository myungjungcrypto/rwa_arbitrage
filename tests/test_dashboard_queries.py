"""Phase M2 회귀 — dashboard.queries 함수들이 read-only DB로 정확한 DataFrame 반환.

Streamlit UI 테스트는 안 함 (통합 부담). queries 레이어만 검증.
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytest

from dashboard import queries
from src.data.storage import Storage


# ──────────────────────────────────────────────
# Fixtures: v3 DB with sample data
# ──────────────────────────────────────────────


@pytest.fixture
def db_path(tmp_path: Path) -> str:
    """v3 DB + 샘플 데이터 (basis, positions, daily_pnl, engine_state)."""
    p = str(tmp_path / "test.db")
    s = Storage(p)
    s.connect()
    base = time.time()

    # basis_spread 5분 데이터
    for i in range(60):
        ts = base - (60 - i) * 60       # 매 1분
        s.save_basis(
            product="wti",
            perp_price=80.0 + i * 0.001,
            futures_price=80.0,
            funding_rate=0.0,
            ts=ts,
        )

    # closed positions 4건 (다양한 entry_bp)
    s.conn.execute(
        """INSERT INTO positions
             (product, pair_id, perp_size, perp_entry, futures_size, futures_entry,
              realized_pnl, funding_pnl, status, opened_at, closed_at)
             VALUES
             ('wti','wti_cme_hl', -200, 80.50, 200, 80.00, +50.0, 0.0, 'closed', ?, ?),
             ('wti','wti_cme_hl', +200, 80.05, -200, 80.10, -10.0, 0.0, 'closed', ?, ?),
             ('wti','wti_cme_hl', -200, 80.20, 200, 80.00, -15.0, 0.0, 'closed', ?, ?),
             ('wti','wti_cme_hl', -200, 80.04, 200, 80.00, -20.0, 0.0, 'closed', ?, ?)""",
        (
            base - 3600 * 5, base - 3600 * 5 + 60,
            base - 3600 * 4, base - 3600 * 4 + 30,
            base - 3600 * 3, base - 3600 * 3 + 7200,
            base - 3600 * 2, base - 3600 * 2 + 60,
        ),
    )

    # open position 1건
    s.conn.execute(
        """INSERT INTO positions
             (product, pair_id, perp_size, perp_entry, futures_size, futures_entry,
              unrealized_pnl, status, opened_at)
             VALUES ('wti','wti_cme_hl', -200, 80.30, 200, 80.00, 25.0, 'open', ?)""",
        (base - 600,),
    )

    # daily_pnl 3일
    for d in (-2, -1, 0):
        from datetime import date, timedelta
        dt = (date.today() + timedelta(days=d)).isoformat()
        s.update_daily_pnl(product="wti", trading_pnl=10.0 + d, fees=2.5, dt=dt)

    # engine_state 3건 (시간순)
    for i, ts in enumerate([base - 90, base - 60, base - 30]):
        s.save_engine_state(
            "wti_cme_hl",
            {
                "total_signals": (i + 1) * 1000,
                "total_entries": i + 1,
                "total_exits": i + 1,
                "open_positions": 0,
                "closed_trades": i + 1,
                "cumulative_pnl_usd": -10.0 * (i + 1),
                "entry_signals_generated": (i + 1) * 5,
                "entry_exec_filter_skip": (i + 1) * 4,
                "entry_warmup_skip": 0,
                "entry_min_abs_skip": 0,
                "rejected_by_risk": 0, "failed_orders": 0,
            },
            basis_stats={"mean": -2.0, "std": 3.0, "min": -10.0, "max": 12.0, "count": 3000 * (i + 1)},
            ts=ts,
        )

    s.conn.commit()
    s.close()
    return p


# ──────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────


def test_open_connection_is_readonly(db_path):
    con = queries.open_connection(db_path)
    # write 시도하면 OperationalError
    with pytest.raises(sqlite3.OperationalError):
        con.execute("INSERT INTO basis_spread (product, perp_price, futures_price, basis, basis_bps, funding_rate, ts) VALUES ('x',1,1,0,0,0,0)")
    con.close()


# ──────────────────────────────────────────────
# Engine state
# ──────────────────────────────────────────────


def test_engine_state_latest_returns_most_recent(db_path):
    con = queries.open_connection(db_path)
    state = queries.load_engine_state_latest(con, "wti_cme_hl")
    assert state is not None
    assert state["total_signals"] == 3000   # 마지막 i=2 → (2+1)*1000
    con.close()


def test_engine_state_latest_unknown_pair_returns_none(db_path):
    con = queries.open_connection(db_path)
    assert queries.load_engine_state_latest(con, "nonexistent") is None
    con.close()


def test_engine_state_history_time_ordered(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_engine_state_history(con, "wti_cme_hl", hours=1)
    assert len(df) == 3
    # ts 오름차순
    assert (df["ts"].diff().dropna() >= 0).all()
    assert "ts_dt" in df.columns
    con.close()


def test_state_freshness_seconds(db_path):
    con = queries.open_connection(db_path)
    state = queries.load_engine_state_latest(con, "wti_cme_hl")
    fresh = queries.state_freshness_seconds(state)
    assert 25 < fresh < 60   # ts = base - 30 정도
    con.close()


def test_state_freshness_returns_none_when_no_state():
    assert queries.state_freshness_seconds(None) is None


def test_list_pairs_with_state(db_path):
    con = queries.open_connection(db_path)
    pairs = queries.list_pairs_with_state(con)
    assert "wti_cme_hl" in pairs
    con.close()


def test_list_registered_pairs_includes_seed(db_path):
    con = queries.open_connection(db_path)
    pairs = queries.list_registered_pairs(con)
    pair_ids = {p["pair_id"] for p in pairs}
    assert "wti_cme_hl" in pair_ids
    con.close()


# ──────────────────────────────────────────────
# Daily PnL
# ──────────────────────────────────────────────


def test_daily_pnl_filters_by_pair_id(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_daily_pnl(con, "wti_cme_hl", days=30)
    assert not df.empty
    assert "cumulative" in df.columns
    # 정렬은 시간 오름차순 (cumulative 누적용)
    assert (df["date"].diff().dropna() >= pd.Timedelta(0)).all()
    con.close()


def test_daily_pnl_no_filter_returns_all(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_daily_pnl(con, None, days=30)
    assert not df.empty
    con.close()


# ──────────────────────────────────────────────
# Trades
# ──────────────────────────────────────────────


def test_closed_trades_decorated_columns(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_closed_trades(con, "wti_cme_hl", limit=10)
    assert len(df) == 4
    for c in ("opened_dt", "closed_dt", "hold_hours", "entry_spread_bps",
              "direction", "net_pnl", "win"):
        assert c in df.columns


def test_closed_trades_direction_inferred_from_entry_spread(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_closed_trades(con, "wti_cme_hl", limit=10)
    # row 1: perp_entry > futures_entry → long_basis
    # row 2: perp_entry < futures_entry → short_basis
    long_rows = df[df["direction"] == "long_basis"]
    short_rows = df[df["direction"] == "short_basis"]
    assert all(long_rows["entry_spread_bps"] > 0)
    assert all(short_rows["entry_spread_bps"] < 0)


def test_closed_trades_win_flag(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_closed_trades(con, "wti_cme_hl", limit=10)
    # 1건 이긴 케이스 (realized=+50), 3건 진 케이스
    assert (df["win"]).sum() == 1
    assert (~df["win"]).sum() == 3


def test_open_positions(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_open_positions(con)
    assert len(df) == 1
    assert df.iloc[0]["unrealized_pnl"] == 25.0
    assert "direction" in df.columns


# ──────────────────────────────────────────────
# Basis series
# ──────────────────────────────────────────────


def test_basis_series_window_filter(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_basis_series(con, "wti_cme_hl", hours=1)
    # 60개 1분 간격 데이터 중 1시간 윈도우 → 약 60개
    assert 50 <= len(df) <= 65
    assert "ts_dt" in df.columns


# ──────────────────────────────────────────────
# Analytics
# ──────────────────────────────────────────────


def test_compute_entry_bp_buckets(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_closed_trades(con, "wti_cme_hl", limit=10)
    bucket_df = queries.compute_entry_bp_buckets(df)
    assert not bucket_df.empty
    # win_rate 컬럼 존재
    for c in ("bucket", "n", "wins", "win_rate", "avg_pnl", "total_pnl"):
        assert c in bucket_df.columns
    con.close()


def test_compute_entry_bp_buckets_empty():
    df = pd.DataFrame(columns=["entry_spread_bps", "win", "net_pnl", "id"])
    bucket_df = queries.compute_entry_bp_buckets(df)
    assert bucket_df.empty


def test_compute_hold_time_buckets(db_path):
    con = queries.open_connection(db_path)
    df = queries.load_closed_trades(con, "wti_cme_hl", limit=10)
    hold_df = queries.compute_hold_time_buckets(df)
    assert not hold_df.empty
    for c in ("bucket", "n", "avg_pnl", "total_pnl"):
        assert c in hold_df.columns
    con.close()


def test_compute_entry_funnel(db_path):
    con = queries.open_connection(db_path)
    state = queries.load_engine_state_latest(con, "wti_cme_hl")
    funnel = queries.compute_entry_funnel(state)
    assert funnel["total_signals"] == 3000
    assert funnel["entry_signals_generated"] == 15
    assert funnel["entry_exec_filter_skip"] == 12
    assert funnel["total_entries"] == 3
    con.close()


def test_compute_entry_funnel_no_state():
    funnel = queries.compute_entry_funnel(None)
    assert funnel["total_signals"] == 0
    assert funnel["total_entries"] == 0


# ──────────────────────────────────────────────
# All-time stats (DB-derived, 봇 재시작 영향 없음)
# ──────────────────────────────────────────────


def test_alltime_stats_pair_id(db_path):
    con = queries.open_connection(db_path)
    s = queries.load_alltime_stats(con, "wti_cme_hl")
    assert s["closed_n"] == 4    # fixture에 4건 closed
    assert s["open_n"] == 1
    # 합계: +50 -10 -15 -20 = +5
    assert s["closed_realized"] == 5.0
    assert s["closed_funding"] == 0.0
    assert s["closed_net"] == 5.0
    assert s["open_unrealized"] == 25.0
    con.close()


def test_alltime_stats_no_pair_returns_aggregate(db_path):
    con = queries.open_connection(db_path)
    s = queries.load_alltime_stats(con, None)
    assert s["closed_n"] == 4
    assert s["open_n"] == 1
    con.close()


def test_alltime_stats_unknown_pair_returns_zeros(db_path):
    con = queries.open_connection(db_path)
    s = queries.load_alltime_stats(con, "nonexistent_pair")
    assert s["closed_n"] == 0
    assert s["open_n"] == 0
    assert s["closed_net"] == 0
    con.close()


# ──────────────────────────────────────────────
# since_date filter — 2026-04-21 cutoff (zombie cleanup 이전 제외)
# ──────────────────────────────────────────────


@pytest.fixture
def db_with_pre_cutoff_data(tmp_path: Path) -> str:
    """4-21 이전 zombie 거래 + 4-21 이후 정상 거래 + basis 데이터."""
    p = str(tmp_path / "cutoff.db")
    s = Storage(p)
    s.connect()
    base = time.time()

    # 4-20 zombie cleanup 거래 (closed_at = 2026-04-20)
    pre_ts = datetime(2026, 4, 20, 12, 0).timestamp()
    s.conn.execute(
        """INSERT INTO positions (product, pair_id,
              perp_size, perp_entry, futures_size, futures_entry,
              realized_pnl, funding_pnl, status, opened_at, closed_at)
            VALUES ('wti','wti_cme_hl', -200, 80.0, 200, 80.0,
                    -2500.0, 0.0, 'closed', ?, ?)""",
        (pre_ts - 86400 * 5, pre_ts),
    )

    # 4-21 이후 정상 거래
    post_ts = datetime(2026, 4, 21, 17, 0).timestamp()
    s.conn.execute(
        """INSERT INTO positions (product, pair_id,
              perp_size, perp_entry, futures_size, futures_entry,
              realized_pnl, funding_pnl, status, opened_at, closed_at)
            VALUES ('wti','wti_cme_hl', -200, 80.20, 200, 80.0,
                    +50.0, 0.0, 'closed', ?, ?)""",
        (post_ts, post_ts + 60),
    )

    # daily_pnl
    s.conn.execute(
        "INSERT INTO daily_pnl (date, product, trading_pnl, funding_pnl, fees, net_pnl, num_trades) "
        "VALUES ('2026-04-20', 'wti', -2500.0, 0, 0, -2500.0, 1)"
    )
    s.conn.execute(
        "INSERT INTO daily_pnl (date, product, trading_pnl, funding_pnl, fees, net_pnl, num_trades) "
        "VALUES ('2026-04-21', 'wti', 50.0, 0, 13.0, 37.0, 1)"
    )

    # basis_spread (cutoff 전후)
    s.save_basis(product="wti", perp_price=80, futures_price=80,
                 funding_rate=0, ts=pre_ts)
    s.save_basis(product="wti", perp_price=80.1, futures_price=80,
                 funding_rate=0, ts=post_ts)
    s.save_basis(product="wti", perp_price=80.2, futures_price=80,
                 funding_rate=0, ts=time.time() - 600)

    s.conn.commit()
    s.close()
    return p


def test_alltime_stats_filters_zombie_data_with_since_date(db_with_pre_cutoff_data):
    con = queries.open_connection(db_with_pre_cutoff_data)

    # cutoff 없으면 zombie -2500 포함
    s_all = queries.load_alltime_stats(con, "wti_cme_hl", since_date=None)
    assert s_all["closed_n"] == 2
    assert s_all["closed_net"] == -2450.0

    # cutoff 적용 시 정상 +50만
    s_cut = queries.load_alltime_stats(con, "wti_cme_hl",
                                        since_date="2026-04-21")
    assert s_cut["closed_n"] == 1
    assert s_cut["closed_net"] == 50.0
    con.close()


def test_closed_trades_filters_with_since_date(db_with_pre_cutoff_data):
    con = queries.open_connection(db_with_pre_cutoff_data)
    df_all = queries.load_closed_trades(con, "wti_cme_hl", limit=10, since_date=None)
    df_cut = queries.load_closed_trades(con, "wti_cme_hl", limit=10,
                                          since_date="2026-04-21")
    assert len(df_all) == 2
    assert len(df_cut) == 1
    assert df_cut.iloc[0]["realized_pnl"] == 50.0
    con.close()


def test_daily_pnl_filters_with_since_date(db_with_pre_cutoff_data):
    con = queries.open_connection(db_with_pre_cutoff_data)
    df_all = queries.load_daily_pnl(con, "wti_cme_hl", days=30, since_date=None)
    df_cut = queries.load_daily_pnl(con, "wti_cme_hl", days=30,
                                      since_date="2026-04-21")
    # cutoff 없으면 4-20, 4-21 둘 다
    assert len(df_all) == 2
    # cutoff 적용 시 4-21만
    assert len(df_cut) == 1
    assert df_cut["net"].sum() == 37.0
    con.close()


def test_basis_series_max_window_or_cutoff(db_with_pre_cutoff_data):
    """hours 윈도우와 cutoff 둘 중 더 가까운(=많이 잘리는) 것 적용."""
    con = queries.open_connection(db_with_pre_cutoff_data)
    # hours가 매우 큼 (1년) → cutoff가 binding
    df_cut = queries.load_basis_series(con, "wti_cme_hl", hours=8760,
                                          since_date="2026-04-21")
    # 2026-04-20 row 제외, 2026-04-21 + 최근 row 포함
    assert all(df_cut["ts"] >= datetime(2026, 4, 21).timestamp())
    con.close()


def test_default_since_date_constant():
    assert queries.DEFAULT_SINCE_DATE.isoformat() == "2026-04-21"
