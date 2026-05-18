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
import sqlite3
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
def _daily_pnl(db_path: str, pair_id: str | None, days: int, since_iso: str | None):
    con = _con(db_path)
    return queries.load_daily_pnl(con, pair_id, days=days, since_date=since_iso)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _closed_trades(db_path: str, pair_id: str | None, limit: int, since_iso: str | None):
    con = _con(db_path)
    return queries.load_closed_trades(con, pair_id, limit=limit, since_date=since_iso)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _open_positions(db_path: str):
    con = _con(db_path)
    return queries.load_open_positions(con)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _basis_series(db_path: str, pair_id: str | None, hours: float, since_iso: str | None):
    con = _con(db_path)
    return queries.load_basis_series(con, pair_id, hours=hours, since_date=since_iso)


@st.cache_data(ttl=60)
def _pairs_with_state(db_path: str):
    con = _con(db_path)
    return queries.list_pairs_with_state(con)


@st.cache_data(ttl=60)
def _registered_pairs(db_path: str):
    con = _con(db_path)
    return queries.list_registered_pairs(con)


@st.cache_data(ttl=DEFAULT_REFRESH_S)
def _alltime_stats(db_path: str, pair_id: str | None, since_iso: str | None):
    con = _con(db_path)
    return queries.load_alltime_stats(con, pair_id, since_date=since_iso)


# ──────────────────────────────────────────────
# UI
# ──────────────────────────────────────────────

st.set_page_config(
    page_title="rwa_arb Dashboard",
    page_icon="📊",
    layout="wide",
)


def main():
    # mode 표시는 설정 파일에서 직접 (engine_state에는 없음)
    mode = queries.load_bot_mode()
    if mode == "LIVE":
        st.title("🔴 rwa_arb 대시보드 — LIVE")
    elif mode == "PAPER":
        st.title("📊 rwa_arb 대시보드 — PAPER")
    else:
        st.title(f"❓ rwa_arb 대시보드 — mode={mode}")

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

        st.divider()
        # 의미있는 데이터 cutoff — zombie cleanup(2026-04-20) 이후
        from datetime import date as _date
        use_cutoff = st.checkbox(
            "Cutoff 적용 (zombie 청산 이후만)",
            value=True,
            help="2026-04-20 close_zombies.py 일회성 -$20K 이벤트 + 그 이전의 "
                 "롤오버 버그 영향 데이터를 분석에서 제외",
        )
        if use_cutoff:
            cutoff_date = st.date_input(
                "Since",
                value=queries.DEFAULT_SINCE_DATE,
                min_value=_date(2026, 1, 1),
                max_value=_date.today(),
            )
            since_iso: str | None = cutoff_date.isoformat()
        else:
            since_iso = None
        st.divider()

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
    stats = _alltime_stats(db_path, pair_id, since_iso)
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

    # ── Per-leg quote freshness (LIVE 운영 가시성) ──
    # LIVE 모드에서는 양 leg 모두 실시간 흘러야 함. 어느 한쪽이 stale이면
    # 봇은 backend 워치독이 60s 후 자동 flatten 하지만, UI에서는 그 전에 보임.
    with sqlite3.connect(db_path) as _con:
        _con.row_factory = sqlite3.Row
        legf = queries.leg_quote_freshness(_con, pair_id)

    fcols = st.columns([1, 1, 4])
    for i, (leg, label) in enumerate([("a", "leg_a (perp)"), ("b", "leg_b (futures)")]):
        info = legf.get(leg)
        with fcols[i]:
            if info is None:
                st.metric(f"{label} quote", "no data")
            else:
                age = info["age_s"]
                if age < 5:
                    st.metric(f"{label} quote", f"🟢 {age:.0f}s ago")
                elif age < 60:
                    st.metric(f"{label} quote", f"🟡 {age:.0f}s ago")
                else:
                    st.metric(f"{label} quote", f"🔴 {age/60:.1f}m ago")
    fcols[2].caption(
        "LIVE 모드: 양 leg 모두 🟢 < 5s 정상. "
        "🔴 > 60s면 backend WS 워치독이 자동 flatten 발동 + Telegram 알림."
    )

    # ── Account balances (거래소별 잔고) ──
    # 봇이 2분마다 각 어댑터 .get_account_value() polling → DB 저장.
    # paper-only 어댑터(lighter/binance/bybit/okx scaffold)는 NotImplementedError로 skip되어 표시 X.
    with sqlite3.connect(db_path) as _con:
        _con.row_factory = sqlite3.Row
        balances = queries.load_latest_balances(_con)

    st.subheader("💰 거래소 잔고")
    if not balances:
        st.info("아직 잔고 데이터 없음 — 봇 부팅 후 첫 polling(2분 이내) 대기 또는 LIVE 모드 인증 키 확인")
    else:
        bcols = st.columns(min(len(balances), 5) or 1)
        for i, b in enumerate(balances):
            with bcols[i % len(bcols)]:
                age_min = b["age_s"] / 60
                age_str = f"{b['age_s']:.0f}s" if b["age_s"] < 120 else f"{age_min:.1f}m"
                ok = b.get("note", "ok") == "ok" and b["value"] > 0
                emoji = "🟢" if ok else ("🟡" if b["value"] == 0 else "🔴")
                label = f"{emoji} {b['exchange'].upper()} ({b['currency']})"
                st.metric(label, f"{b['value']:,.2f}", delta=None)
                if b.get("note") and b["note"] != "ok":
                    st.caption(f"⚠️ {b['note'][:80]}")
                else:
                    st.caption(f"updated {age_str} ago")

    st.divider()

    # ── Basis chart ──
    st.subheader("📈 Basis chart")
    basis_df = _basis_series(db_path, pair_id, hours, since_iso)
    closed_for_chart = _closed_trades(db_path, pair_id, limit=500, since_iso=since_iso)
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
    # ── 🧪 Entry diagnostics (LIVE 테스트용, latency/slippage 진단) ──
    # 나중에 LIVE 안정화되면 제거. signal vs exec vs latency 한눈에.
    st.subheader("🧪 Entry diagnostics (signal vs exec, latency)")
    with sqlite3.connect(db_path) as _con:
        _con.row_factory = sqlite3.Row
        diag_df = queries.load_entry_diagnostics(_con, pair_id, limit=20)
    if diag_df.empty:
        st.info(
            "v5 schema 마이그레이션 이전 거래 또는 신규 entry 없음. "
            "다음 LIVE entry 후 표시됨."
        )
    else:
        # 정리해서 표시 — id, time, signal/exec/slip/latency, prices, pnl
        display_df = diag_df.copy()
        display_df["time"] = display_df["opened_dt"].dt.strftime("%m-%d %H:%M:%S")
        display_df = display_df[[
            "id", "time", "signal_basis_bps", "exec_basis_bps", "slip_bps",
            "max_latency_ms", "entry_latency_ms_a", "entry_latency_ms_b",
            "perp_entry", "futures_entry", "realized_pnl", "status",
        ]].rename(columns={
            "signal_basis_bps": "signal_bp",
            "exec_basis_bps": "exec_bp",
            "slip_bps": "slip_bp",
            "max_latency_ms": "max_ms",
            "entry_latency_ms_a": "leg_a_ms",
            "entry_latency_ms_b": "leg_b_ms",
            "perp_entry": "perp",
            "futures_entry": "fut",
            "realized_pnl": "pnl",
        })
        st.dataframe(
            display_df,
            hide_index=True,
            column_config={
                "signal_bp": st.column_config.NumberColumn(format="%+.1f"),
                "exec_bp":   st.column_config.NumberColumn(format="%+.1f"),
                "slip_bp":   st.column_config.NumberColumn(
                    format="%+.1f",
                    help="signal_bp - exec_bp (latency가 잡아먹은 edge)",
                ),
                "max_ms":    st.column_config.NumberColumn(format="%.0f"),
                "leg_a_ms":  st.column_config.NumberColumn(format="%.0f"),
                "leg_b_ms":  st.column_config.NumberColumn(format="%.0f"),
                "perp":      st.column_config.NumberColumn(format="%.3f"),
                "fut":       st.column_config.NumberColumn(format="%.3f"),
                "pnl":       st.column_config.NumberColumn(format="%+.2f"),
            },
        )
        # 요약 통계 (NULL 제외)
        valid = diag_df.dropna(subset=["signal_basis_bps", "exec_basis_bps"])
        if not valid.empty:
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("avg signal", f"{valid['signal_basis_bps'].mean():+.1f}bp")
            c2.metric("avg exec",   f"{valid['exec_basis_bps'].mean():+.1f}bp")
            c3.metric(
                "avg slip", f"{valid['slip_bps'].mean():+.1f}bp",
                help="signal 대비 fill 시점까지 잠식된 spread (latency 영향)",
            )
            v_lat = valid.dropna(subset=["max_latency_ms"])
            if not v_lat.empty:
                c4.metric("avg max latency", f"{v_lat['max_latency_ms'].mean():.0f}ms")
        st.caption(
            "🧪 테스트용 — slip_bp = signal_bp - exec_bp (latency가 잡아먹은 spike profit). "
            "낮을수록 좋음 (PAPER 기준 1-2bp, LIVE 목표 ≤ 5bp)."
        )

    st.divider()

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
    closed_df = _closed_trades(db_path, pair_id, limit=n_trades, since_iso=since_iso)
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
        daily_df = _daily_pnl(db_path, pair_id, days_pnl, since_iso)
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
