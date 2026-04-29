"""Streamlit 페이퍼 트레이딩 대시보드.

실행:
    streamlit run dashboard/app.py --server.address=127.0.0.1 --server.port=8501

원격 EC2에서 운영 시 SSH 터널:
    ssh -L 8501:localhost:8501 ec2
    # 브라우저 → http://localhost:8501

PM2 등록:
    pm2 start --name rwa-arb-dashboard --interpreter none -- \\
       streamlit run dashboard/app.py --server.address=127.0.0.1 \\
                                        --server.port=8501 --server.headless=true
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pandas as pd
import streamlit as st

# 패키지 import 가능하도록 repo root 추가
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dashboard import charts, queries     # noqa: E402

DEFAULT_DB_PATH = os.environ.get("RWA_DB_PATH", "data/arbitrage.db")
DEFAULT_REFRESH_S = 10


# ──────────────────────────────────────────────
# Cached query wrappers
# ──────────────────────────────────────────────

@st.cache_resource
def _con(db_path: str):
    return queries.open_connection(db_path)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _state_latest(db_path: str, pair_id: str):
    con = _con(db_path)
    return queries.load_engine_state_latest(con, pair_id)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _state_history(db_path: str, pair_id: str, hours: float):
    con = _con(db_path)
    return queries.load_engine_state_history(con, pair_id, hours=hours)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _daily_pnl(db_path: str, pair_id: str | None, days: int):
    con = _con(db_path)
    return queries.load_daily_pnl(con, pair_id, days=days)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _closed_trades(db_path: str, pair_id: str | None, limit: int):
    con = _con(db_path)
    return queries.load_closed_trades(con, pair_id, limit=limit)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _open_positions(db_path: str):
    con = _con(db_path)
    return queries.load_open_positions(con)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _basis_series(db_path: str, pair_id: str | None, hours: float):
    con = _con(db_path)
    return queries.load_basis_series(con, pair_id, hours=hours)


@st.cache_data(ttl=60)
def _pairs_with_state(db_path: str):
    con = _con(db_path)
    return queries.list_pairs_with_state(con)


@st.cache_data(ttl=60)
def _registered_pairs(db_path: str):
    con = _con(db_path)
    return queries.list_registered_pairs(con)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _alltime_stats(db_path: str, pair_id: str | None):
    con = _con(db_path)
    return queries.load_alltime_stats(con, pair_id)


# ──────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────

st.set_page_config(
    page_title="rwa_arb Dashboard",
    page_icon="📊",
    layout="wide",
)


def main():
    st.title("📊 rwa_arb 페이퍼 트레이딩 대시보드")

    # ── Sidebar ──
    with st.sidebar:
        st.header("설정")
        db_path = st.text_input("DB 경로", value=DEFAULT_DB_PATH)

        if not Path(db_path).exists():
            st.error(f"DB 없음: {db_path}")
            st.stop()

        # 페어 목록: engine_state가 적재된 페어 + pairs 테이블 등록 페어 합집합
        pairs_state = set(_pairs_with_state(db_path))
        pairs_reg = {p["pair_id"] for p in _registered_pairs(db_path)}
        all_pairs = sorted(pairs_state | pairs_reg) or ["wti_cme_hl"]
        pair_id = st.selectbox("페어", all_pairs, index=0)

        time_range = st.selectbox(
            "시간 범위 (basis chart, state history)",
            ["1h", "6h", "24h", "3d", "7d"],
            index=2,
        )
        hours_map = {"1h": 1, "6h": 6, "24h": 24, "3d": 72, "7d": 168}
        hours = hours_map[time_range]

        days_pnl = st.slider("Daily PnL: 최근 N일", min_value=7, max_value=90, value=30)
        n_trades = st.slider("Trade history: 최근 N건", min_value=20, max_value=500, value=100)

        auto_refresh = st.checkbox("자동 새로고침 (10초)", value=True)
        if auto_refresh:
            try:
                from streamlit_autorefresh import st_autorefresh
                st_autorefresh(interval=DEFAULT_REFRESH_S * 1000, key="auto_refresh")
            except ImportError:
                st.warning("`pip install streamlit-autorefresh` 권장")

        st.divider()
        st.caption("read-only · DB 변경 없음")
        st.caption(f"refresh interval: {DEFAULT_REFRESH_S}s")

    # ── Live state header ──
    state = _state_latest(db_path, pair_id)
    fresh = queries.state_freshness_seconds(state)

    cols = st.columns([2, 1, 1, 1, 1])
    with cols[0]:
        if fresh is None:
            st.error("📛 No state snapshot yet — 봇이 안 돌고 있거나 첫 30초 내")
        elif fresh < 60:
            st.success(f"🟢 Healthy · last update {fresh:.0f}s ago")
        elif fresh < 300:
            st.warning(f"🟡 Stale · last update {fresh:.0f}s ago")
        else:
            st.error(f"🔴 Bot dead? · last update {fresh/60:.1f}m ago")

    # 메트릭은 DB 기반 (engine_state 카운터는 봇 프로세스 재시작 시 리셋되므로
    # "전체 기간" 표기는 positions 테이블에서 직접 집계)
    stats = _alltime_stats(db_path, pair_id)
    cols[1].metric("Open positions", stats["open_n"])
    cols[2].metric("Closed trades", stats["closed_n"])
    cols[3].metric("Cumulative PnL", f"${stats['closed_net']:+.2f}")

    if state:
        cols[4].metric("Total signals (session)", state["total_signals"])
        st.caption(
            f"Pair: **{state['pair_id']}** · "
            f"Session entry signals: {state['entry_signals_generated']} · "
            f"Skips → exec: {state['entry_exec_filter_skip']}, "
            f"warmup: {state['entry_warmup_skip']}, "
            f"min_abs: {state['entry_min_abs_skip']} · "
            f"Risk reject: {state['rejected_by_risk']} · "
            f"Order fail: {state['failed_orders']}"
        )
        if state["basis_mean_bps"] is not None:
            st.caption(
                f"basis (recent {state['basis_n']} pts): "
                f"mean **{state['basis_mean_bps']:+.1f}bp** · "
                f"std {state['basis_std_bps']:.1f}bp · "
                f"range [{state['basis_min_bps']:+.1f}, {state['basis_max_bps']:+.1f}]bp"
            )
    else:
        cols[4].metric("Total signals (session)", "—")

    st.divider()

    # ── Basis chart ──
    st.subheader("📈 Basis chart")
    basis_df = _basis_series(db_path, pair_id, hours)
    closed_for_chart = _closed_trades(db_path, pair_id, limit=500)
    open_for_chart = _open_positions(db_path)
    if not basis_df.empty:
        fig = charts.basis_chart(
            basis_df,
            closed_df=closed_for_chart if not closed_for_chart.empty else None,
            open_df=open_for_chart if not open_for_chart.empty else None,
        )
        st.plotly_chart(fig, use_container_width=True)
        st.caption(
            f"{len(basis_df):,} basis points · "
            f"{basis_df['ts_dt'].min()} → {basis_df['ts_dt'].max()}"
        )
    else:
        st.info(f"최근 {hours}h basis 데이터 없음")

    st.divider()

    # ── Open positions ──
    st.subheader("🔓 오픈 포지션")
    open_df = _open_positions(db_path)
    if open_df.empty:
        st.info("현재 오픈 포지션 없음")
    else:
        view = open_df[[
            "id", "pair_id", "opened_dt", "direction",
            "entry_spread_bps", "perp_entry", "futures_entry",
            "perp_size", "futures_size", "unrealized_pnl",
        ]].rename(columns={
            "opened_dt": "opened",
            "entry_spread_bps": "entry_bp",
            "unrealized_pnl": "unrealized",
        })
        st.dataframe(view, use_container_width=True, hide_index=True)

    st.divider()

    # ── Trade history ──
    st.subheader(f"📜 Trade history (최근 {n_trades}건)")
    closed_df = _closed_trades(db_path, pair_id, limit=n_trades)
    if closed_df.empty:
        st.info("완료된 거래 없음")
    else:
        view = closed_df.copy()
        view["fees_est"] = (
            view["perp_size"].abs() * view["perp_entry"] * 0.00009 * 2
            + view["futures_size"].abs() * 2.50 * 2
        )
        cols_view = [
            "id", "opened_dt", "closed_dt", "hold_hours", "direction",
            "entry_spread_bps", "perp_entry", "futures_entry",
            "realized_pnl", "funding_pnl", "fees_est", "net_pnl", "win",
        ]
        view = view[cols_view].rename(columns={
            "opened_dt": "opened",
            "closed_dt": "closed",
            "hold_hours": "hold_h",
            "entry_spread_bps": "entry_bp",
            "perp_entry": "perp_in",
            "futures_entry": "fut_in",
            "realized_pnl": "realized",
            "funding_pnl": "funding",
            "fees_est": "fees(est)",
            "net_pnl": "net",
        })
        st.dataframe(
            view, use_container_width=True, hide_index=True,
            column_config={
                "hold_h": st.column_config.NumberColumn("hold_h", format="%.1f"),
                "entry_bp": st.column_config.NumberColumn("entry_bp", format="%+.1f"),
                "perp_in": st.column_config.NumberColumn("perp_in", format="%.2f"),
                "fut_in": st.column_config.NumberColumn("fut_in", format="%.2f"),
                "realized": st.column_config.NumberColumn("realized", format="$%+.2f"),
                "funding": st.column_config.NumberColumn("funding", format="$%+.2f"),
                "fees(est)": st.column_config.NumberColumn("fees(est)", format="$%.2f"),
                "net": st.column_config.NumberColumn("net", format="$%+.2f"),
            },
        )

        # 합계
        st.caption(
            f"총 {len(view)}건 · "
            f"net 합계 ${view['net'].sum():+.2f} · "
            f"승 {(view['win']).sum()}건, 패 {(~view['win']).sum()}건 · "
            f"승률 {(view['win']).mean():.0%}"
        )

    st.divider()

    # ── Daily PnL ──
    col_left, col_right = st.columns([2, 1])
    with col_left:
        st.subheader(f"💰 Daily PnL (최근 {days_pnl}일)")
        daily_df = _daily_pnl(db_path, pair_id, days_pnl)
        if daily_df.empty:
            st.info("데이터 없음")
        else:
            st.plotly_chart(charts.daily_pnl_bar(daily_df), use_container_width=True)
            st.plotly_chart(charts.cumulative_pnl_line(daily_df), use_container_width=True)
            view_d = daily_df[["date", "n", "trading", "funding", "fees", "net", "cumulative"]].copy()
            view_d["date"] = view_d["date"].dt.strftime("%Y-%m-%d")
            st.dataframe(
                view_d, use_container_width=True, hide_index=True,
                column_config={
                    "trading": st.column_config.NumberColumn("trading", format="$%+.2f"),
                    "funding": st.column_config.NumberColumn("funding", format="$%+.2f"),
                    "fees": st.column_config.NumberColumn("fees", format="$%.2f"),
                    "net": st.column_config.NumberColumn("net", format="$%+.2f"),
                    "cumulative": st.column_config.NumberColumn("cumulative", format="$%+.2f"),
                },
            )

    with col_right:
        st.subheader("🚪 Entry funnel")
        if state:
            funnel = queries.compute_entry_funnel(state)
            st.plotly_chart(charts.entry_funnel_bar(funnel), use_container_width=True)
        else:
            st.info("엔진 state 없음")

    st.divider()

    # ── Win/Loss buckets ──
    st.subheader("🎯 Entry spread bucket × WR")
    if not closed_df.empty:
        bucket_df = queries.compute_entry_bp_buckets(closed_df)
        st.plotly_chart(charts.entry_bp_bucket_bar(bucket_df), use_container_width=True)
        view_b = bucket_df.copy()
        view_b["bucket"] = view_b["bucket"].astype(str)
        view_b["win_rate"] = (view_b["win_rate"] * 100).round(0)   # 0-1 → 0-100
        st.dataframe(
            view_b, use_container_width=True, hide_index=True,
            column_config={
                "win_rate": st.column_config.NumberColumn("win_rate", format="%.0f%%"),
                "avg_pnl": st.column_config.NumberColumn("avg_pnl", format="$%+.2f"),
                "total_pnl": st.column_config.NumberColumn("total_pnl", format="$%+.2f"),
            },
        )

        st.subheader("⏱️ Hold time bucket × Avg PnL")
        hold_df = queries.compute_hold_time_buckets(closed_df)
        view_h = hold_df.copy()
        view_h["bucket"] = view_h["bucket"].astype(str)
        st.dataframe(
            view_h, use_container_width=True, hide_index=True,
            column_config={
                "avg_pnl": st.column_config.NumberColumn("avg_pnl", format="$%+.2f"),
                "total_pnl": st.column_config.NumberColumn("total_pnl", format="$%+.2f"),
            },
        )

        st.subheader("📍 Entry spread vs Net PnL")
        st.plotly_chart(charts.trade_pnl_scatter(closed_df), use_container_width=True)
    else:
        st.info("완료된 거래 없음 — bucket 분석 생략")


if __name__ == "__main__":
    main()
