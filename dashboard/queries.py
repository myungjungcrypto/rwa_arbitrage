"""대시보드용 SQL → pandas DataFrame.

`scripts/analyze_paper.py`의 SQL 로직을 pandas로 wrap. 모든 함수는
read-only — DB 변경 없음. Streamlit 캐시는 호출자가 처리.

Phase M2.1.
"""

from __future__ import annotations

import sqlite3
from typing import Optional

import pandas as pd


DEFAULT_DB_PATH = "data/arbitrage.db"


# ──────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────


def open_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Read-only connection (mode=ro). 봇이 동시에 쓰는 동안 안전하게 read."""
    # SQLite는 ro 모드는 URI로만 지원
    uri = f"file:{db_path}?mode=ro"
    con = sqlite3.connect(uri, uri=True, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


# ──────────────────────────────────────────────
# Engine state (live counters)
# ──────────────────────────────────────────────


def load_engine_state_latest(con: sqlite3.Connection, pair_id: str) -> Optional[dict]:
    """가장 최근 engine_state 1건. 없으면 None."""
    row = con.execute(
        "SELECT * FROM engine_state WHERE pair_id = ? ORDER BY ts DESC LIMIT 1",
        (pair_id,),
    ).fetchone()
    return dict(row) if row else None


def load_engine_state_history(
    con: sqlite3.Connection, pair_id: str, hours: float = 24
) -> pd.DataFrame:
    """최근 N시간 engine_state 시계열."""
    df = pd.read_sql_query(
        """SELECT * FROM engine_state
             WHERE pair_id = ?
               AND ts >= strftime('%s','now') - ? * 3600
             ORDER BY ts ASC""",
        con, params=(pair_id, hours),
    )
    if not df.empty:
        df["ts_dt"] = pd.to_datetime(df["ts"], unit="s")
    return df


def list_pairs_with_state(con: sqlite3.Connection) -> list[str]:
    """engine_state에 적재된 적 있는 pair_id 목록."""
    rows = con.execute(
        "SELECT DISTINCT pair_id FROM engine_state ORDER BY pair_id"
    ).fetchall()
    return [r["pair_id"] for r in rows]


def list_registered_pairs(con: sqlite3.Connection) -> list[dict]:
    """pairs 테이블에 등록된 페어 메타."""
    try:
        rows = con.execute(
            "SELECT * FROM pairs ORDER BY pair_id"
        ).fetchall()
        return [dict(r) for r in rows]
    except sqlite3.OperationalError:
        return []


# ──────────────────────────────────────────────
# Daily PnL
# ──────────────────────────────────────────────


def load_daily_pnl(
    con: sqlite3.Connection, pair_id: Optional[str] = None, days: int = 30
) -> pd.DataFrame:
    """일별 PnL — pair_id 지정 시 필터, 미지정 시 전체 product 합산.

    Schema: legacy daily_pnl(date, product, trading/funding/fees/net_pnl, num_trades)
    + v2 추가 컬럼 pair_id.
    """
    if pair_id:
        sql = """SELECT date,
                        SUM(trading_pnl) AS trading,
                        SUM(funding_pnl) AS funding,
                        SUM(fees) AS fees,
                        SUM(net_pnl) AS net,
                        SUM(num_trades) AS n
                   FROM daily_pnl
                  WHERE (pair_id = ? OR (pair_id IS NULL AND product = ?))
                  GROUP BY date
                  ORDER BY date DESC
                  LIMIT ?"""
        # legacy 'wti' product backfill 호환: pair_id 또는 product 매칭
        legacy_product = pair_id.split("_", 1)[0]
        df = pd.read_sql_query(sql, con, params=(pair_id, legacy_product, days))
    else:
        df = pd.read_sql_query(
            """SELECT date,
                      SUM(trading_pnl) AS trading,
                      SUM(funding_pnl) AS funding,
                      SUM(fees) AS fees,
                      SUM(net_pnl) AS net,
                      SUM(num_trades) AS n
                 FROM daily_pnl
                 GROUP BY date
                 ORDER BY date DESC
                 LIMIT ?""",
            con, params=(days,),
        )
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        df["cumulative"] = df["net"].cumsum()
    return df


# ──────────────────────────────────────────────
# Trades
# ──────────────────────────────────────────────


def load_closed_trades(
    con: sqlite3.Connection, pair_id: Optional[str] = None, limit: int = 200
) -> pd.DataFrame:
    """완료된 거래 목록. analyze_paper.load_closed_trades 동치."""
    if pair_id:
        legacy_product = pair_id.split("_", 1)[0]
        sql = """SELECT id, product, pair_id,
                        perp_size, perp_entry, futures_size, futures_entry,
                        realized_pnl, funding_pnl,
                        opened_at, closed_at
                   FROM positions
                  WHERE status='closed'
                    AND (pair_id = ? OR (pair_id IS NULL AND product = ?))
                  ORDER BY closed_at DESC
                  LIMIT ?"""
        df = pd.read_sql_query(sql, con, params=(pair_id, legacy_product, limit))
    else:
        df = pd.read_sql_query(
            """SELECT id, product, pair_id,
                      perp_size, perp_entry, futures_size, futures_entry,
                      realized_pnl, funding_pnl,
                      opened_at, closed_at
                 FROM positions
                WHERE status='closed'
                ORDER BY closed_at DESC
                LIMIT ?""",
            con, params=(limit,),
        )
    if df.empty:
        return df

    df["opened_dt"] = pd.to_datetime(df["opened_at"], unit="s")
    df["closed_dt"] = pd.to_datetime(df["closed_at"], unit="s")
    df["hold_hours"] = (df["closed_at"] - df["opened_at"]) / 3600.0
    df["entry_spread_bps"] = (df["perp_entry"] - df["futures_entry"]) / df["futures_entry"] * 10_000
    df["direction"] = df["entry_spread_bps"].apply(
        lambda x: "long_basis" if x > 0 else "short_basis"
    )
    df["realized_pnl"] = df["realized_pnl"].fillna(0.0)
    df["funding_pnl"] = df["funding_pnl"].fillna(0.0)
    df["net_pnl"] = df["realized_pnl"] + df["funding_pnl"]
    df["win"] = df["net_pnl"] > 0
    return df


def load_open_positions(con: sqlite3.Connection) -> pd.DataFrame:
    """현재 오픈 포지션."""
    df = pd.read_sql_query(
        """SELECT id, product, pair_id,
                  perp_size, perp_entry, futures_size, futures_entry,
                  unrealized_pnl, opened_at
             FROM positions
            WHERE status='open'
            ORDER BY opened_at DESC""",
        con,
    )
    if df.empty:
        return df
    df["opened_dt"] = pd.to_datetime(df["opened_at"], unit="s")
    df["entry_spread_bps"] = (df["perp_entry"] - df["futures_entry"]) / df["futures_entry"] * 10_000
    df["direction"] = df["entry_spread_bps"].apply(
        lambda x: "long_basis" if x > 0 else "short_basis"
    )
    return df


# ──────────────────────────────────────────────
# Basis time series
# ──────────────────────────────────────────────


def load_basis_series(
    con: sqlite3.Connection, pair_id: Optional[str] = None, hours: float = 24
) -> pd.DataFrame:
    """basis_spread 최근 N시간 (basis chart용)."""
    if pair_id:
        legacy_product = pair_id.split("_", 1)[0]
        df = pd.read_sql_query(
            """SELECT ts, perp_price, futures_price, basis_bps, funding_rate
                 FROM basis_spread
                WHERE (pair_id = ? OR (pair_id IS NULL AND product = ?))
                  AND ts >= strftime('%s','now') - ? * 3600
                ORDER BY ts ASC""",
            con, params=(pair_id, legacy_product, hours),
        )
    else:
        df = pd.read_sql_query(
            """SELECT ts, perp_price, futures_price, basis_bps, funding_rate
                 FROM basis_spread
                WHERE ts >= strftime('%s','now') - ? * 3600
                ORDER BY ts ASC""",
            con, params=(hours,),
        )
    if not df.empty:
        df["ts_dt"] = pd.to_datetime(df["ts"], unit="s")
    return df


# ──────────────────────────────────────────────
# Analytics
# ──────────────────────────────────────────────


def compute_entry_bp_buckets(closed_df: pd.DataFrame) -> pd.DataFrame:
    """진입 spread 절대값 버킷별 WR + PnL."""
    if closed_df.empty:
        return pd.DataFrame(columns=["bucket", "n", "wins", "win_rate", "avg_pnl", "total_pnl"])

    def bucket(b):
        a = abs(b)
        if a < 10:
            return "< 10bp"
        if a < 20:
            return "10-20bp"
        if a < 50:
            return "20-50bp"
        return "≥ 50bp"

    df = closed_df.copy()
    df["bucket"] = df["entry_spread_bps"].apply(bucket)
    out = (
        df.groupby("bucket")
          .agg(
              n=("id", "count"),
              wins=("win", "sum"),
              avg_pnl=("net_pnl", "mean"),
              total_pnl=("net_pnl", "sum"),
          )
          .reset_index()
    )
    out["win_rate"] = out["wins"] / out["n"]
    bucket_order = ["< 10bp", "10-20bp", "20-50bp", "≥ 50bp"]
    out["bucket"] = pd.Categorical(out["bucket"], categories=bucket_order, ordered=True)
    return out.sort_values("bucket").reset_index(drop=True)


def compute_hold_time_buckets(closed_df: pd.DataFrame) -> pd.DataFrame:
    """보유 시간 버킷별 통계."""
    if closed_df.empty:
        return pd.DataFrame(columns=["bucket", "n", "avg_pnl", "total_pnl"])

    def bucket(h):
        if h < 1 / 60:
            return "< 1m"
        if h < 1:
            return "< 1h"
        if h < 4:
            return "1-4h"
        if h < 24:
            return "4-24h"
        if h < 48:
            return "1-2d"
        return "≥ 2d"

    df = closed_df.copy()
    df["bucket"] = df["hold_hours"].apply(bucket)
    out = (
        df.groupby("bucket")
          .agg(
              n=("id", "count"),
              avg_pnl=("net_pnl", "mean"),
              total_pnl=("net_pnl", "sum"),
          )
          .reset_index()
    )
    bucket_order = ["< 1m", "< 1h", "1-4h", "4-24h", "1-2d", "≥ 2d"]
    out["bucket"] = pd.Categorical(out["bucket"], categories=bucket_order, ordered=True)
    return out.sort_values("bucket").reset_index(drop=True)


def compute_entry_funnel(state_latest: Optional[dict]) -> dict:
    """signals → entry_signals_generated → exec/warmup/min_abs_skip → entries.

    대시보드 funnel 시각화용.
    """
    if not state_latest:
        return {
            "total_signals": 0,
            "entry_signals_generated": 0,
            "entry_exec_filter_skip": 0,
            "entry_warmup_skip": 0,
            "entry_min_abs_skip": 0,
            "total_entries": 0,
        }
    return {
        "total_signals": state_latest.get("total_signals", 0),
        "entry_signals_generated": state_latest.get("entry_signals_generated", 0),
        "entry_exec_filter_skip": state_latest.get("entry_exec_filter_skip", 0),
        "entry_warmup_skip": state_latest.get("entry_warmup_skip", 0),
        "entry_min_abs_skip": state_latest.get("entry_min_abs_skip", 0),
        "total_entries": state_latest.get("total_entries", 0),
    }


def state_freshness_seconds(state_latest: Optional[dict]) -> Optional[float]:
    """latest engine_state row 시각이 현재로부터 몇 초 전인지. None이면 데이터 없음."""
    if not state_latest:
        return None
    import time as _t
    return _t.time() - state_latest["ts"]


def load_alltime_stats(con: sqlite3.Connection, pair_id: Optional[str] = None) -> dict:
    """positions 테이블에서 누적 통계.

    engine_state 카운터는 봇 프로세스 메모리 기준(재시작 시 리셋) →
    "전체 기간" 표기는 이 DB 기반 stats 사용.
    """
    if pair_id:
        legacy_product = pair_id.split("_", 1)[0]
        where = "AND (pair_id = ? OR (pair_id IS NULL AND product = ?))"
        params: tuple = (pair_id, legacy_product)
    else:
        where = ""
        params = ()
    closed_row = con.execute(
        f"""SELECT COUNT(*) AS n,
                   COALESCE(SUM(realized_pnl), 0) AS realized,
                   COALESCE(SUM(funding_pnl), 0) AS funding
              FROM positions
             WHERE status='closed' {where}""",
        params,
    ).fetchone()
    open_row = con.execute(
        f"""SELECT COUNT(*) AS n,
                   COALESCE(SUM(unrealized_pnl), 0) AS unrealized
              FROM positions
             WHERE status='open' {where}""",
        params,
    ).fetchone()
    return {
        "closed_n": closed_row["n"],
        "closed_realized": closed_row["realized"],
        "closed_funding": closed_row["funding"],
        "closed_net": closed_row["realized"] + closed_row["funding"],
        "open_n": open_row["n"],
        "open_unrealized": open_row["unrealized"],
    }
