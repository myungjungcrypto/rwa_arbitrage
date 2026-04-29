"""Plotly figure 생성기.

Phase M2.2. queries.py가 반환한 DataFrame을 입력으로 받아 Figure 반환.
모든 함수는 순수 (DB 접근 없음).
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def basis_chart(
    basis_df: pd.DataFrame,
    closed_df: Optional[pd.DataFrame] = None,
    open_df: Optional[pd.DataFrame] = None,
    entry_threshold_bps: float = 20.0,
    min_abs_entry_bps: float = 10.0,
) -> go.Figure:
    """basis_bps 시계열 + 임계값 라인 + 진입/청산 마커.

    rolling mean ± 3σ band를 1h window로 overlay.
    """
    fig = go.Figure()

    if basis_df is None or basis_df.empty:
        fig.update_layout(title="No basis data")
        return fig

    df = basis_df.copy()
    # 1h rolling stats
    if "ts_dt" not in df.columns:
        df["ts_dt"] = pd.to_datetime(df["ts"], unit="s")
    df = df.set_index("ts_dt").sort_index()
    rolling = df["basis_bps"].rolling("1h")
    df["mean"] = rolling.mean()
    df["std"] = rolling.std()
    df["upper"] = df["mean"] + 3 * df["std"]
    df["lower"] = df["mean"] - 3 * df["std"]

    # ±3σ band
    fig.add_trace(go.Scatter(
        x=df.index, y=df["upper"],
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=df.index, y=df["lower"],
        line=dict(width=0), fill="tonexty",
        fillcolor="rgba(120,120,255,0.12)",
        name="±3σ (1h)", hoverinfo="skip",
    ))

    # basis_bps 라인
    fig.add_trace(go.Scatter(
        x=df.index, y=df["basis_bps"], mode="lines",
        name="basis_bps", line=dict(color="#2563eb", width=1.5),
    ))

    # entry threshold lines
    fig.add_hline(y=entry_threshold_bps, line_dash="dash",
                   line_color="rgba(220,40,40,0.6)",
                   annotation_text=f"+{entry_threshold_bps:.0f}bp threshold",
                   annotation_position="top right")
    fig.add_hline(y=-entry_threshold_bps, line_dash="dash",
                   line_color="rgba(220,40,40,0.6)",
                   annotation_text=f"-{entry_threshold_bps:.0f}bp threshold",
                   annotation_position="bottom right")
    fig.add_hline(y=min_abs_entry_bps, line_dash="dot",
                   line_color="rgba(180,180,40,0.5)",
                   annotation_text=f"min_abs ±{min_abs_entry_bps:.0f}bp",
                   annotation_position="top left")
    fig.add_hline(y=-min_abs_entry_bps, line_dash="dot",
                   line_color="rgba(180,180,40,0.5)",
                   annotation_position="bottom left")

    # 진입/청산 마커 — closed trades
    if closed_df is not None and not closed_df.empty:
        in_window = closed_df[
            (closed_df["opened_at"] >= df["ts"].min()) &
            (closed_df["opened_at"] <= df["ts"].max())
        ]
        if not in_window.empty:
            wins = in_window[in_window["win"]]
            losses = in_window[~in_window["win"]]
            if not wins.empty:
                fig.add_trace(go.Scatter(
                    x=wins["opened_dt"], y=wins["entry_spread_bps"],
                    mode="markers", name="Entry (Win)",
                    marker=dict(color="#16a34a", size=9, symbol="triangle-up",
                                 line=dict(width=1, color="white")),
                    hovertemplate="Entry W: %{y:.1f}bp<br>%{x}<extra></extra>",
                ))
            if not losses.empty:
                fig.add_trace(go.Scatter(
                    x=losses["opened_dt"], y=losses["entry_spread_bps"],
                    mode="markers", name="Entry (Loss)",
                    marker=dict(color="#dc2626", size=9, symbol="triangle-up",
                                 line=dict(width=1, color="white")),
                    hovertemplate="Entry L: %{y:.1f}bp<br>%{x}<extra></extra>",
                ))

    # 오픈 포지션 마커
    if open_df is not None and not open_df.empty:
        in_window = open_df[open_df["opened_at"] >= df["ts"].min()]
        if not in_window.empty:
            fig.add_trace(go.Scatter(
                x=in_window["opened_dt"], y=in_window["entry_spread_bps"],
                mode="markers", name="Open",
                marker=dict(color="#eab308", size=11, symbol="circle-open",
                             line=dict(width=2)),
            ))

    fig.update_layout(
        height=420,
        margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title=None,
        yaxis_title="basis (bp)",
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return fig


def daily_pnl_bar(daily_df: pd.DataFrame) -> go.Figure:
    """일별 net PnL bar (양수=초록, 음수=빨강)."""
    fig = go.Figure()
    if daily_df is None or daily_df.empty:
        fig.update_layout(title="No daily PnL data")
        return fig
    colors = ["#16a34a" if v >= 0 else "#dc2626" for v in daily_df["net"]]
    fig.add_trace(go.Bar(
        x=daily_df["date"], y=daily_df["net"],
        marker_color=colors, name="Daily Net",
        hovertemplate="%{x|%Y-%m-%d}<br>Net: $%{y:+.2f}<br>"
                      "Trades: %{customdata[0]}<extra></extra>",
        customdata=daily_df[["n"]],
    ))
    fig.add_hline(y=0, line_color="rgba(0,0,0,0.3)")
    fig.update_layout(
        height=320, margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title=None, yaxis_title="Net PnL ($)",
    )
    return fig


def cumulative_pnl_line(daily_df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    if daily_df is None or daily_df.empty or "cumulative" not in daily_df:
        fig.update_layout(title="No cumulative PnL data")
        return fig
    fig.add_trace(go.Scatter(
        x=daily_df["date"], y=daily_df["cumulative"],
        mode="lines+markers", line=dict(color="#2563eb", width=2),
        name="Cumulative",
    ))
    fig.add_hline(y=0, line_color="rgba(0,0,0,0.2)")
    fig.update_layout(
        height=300, margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title=None, yaxis_title="Cumulative PnL ($)",
    )
    return fig


def entry_funnel_bar(funnel: dict) -> go.Figure:
    """signals → entry_signals → entries 깔때기 (skip 분해 포함)."""
    rows = [
        ("Total signals", funnel["total_signals"]),
        ("Entry signals", funnel["entry_signals_generated"]),
        ("Skip: exec_filter", -funnel["entry_exec_filter_skip"]),
        ("Skip: warmup", -funnel["entry_warmup_skip"]),
        ("Skip: min_abs", -funnel["entry_min_abs_skip"]),
        ("Entries", funnel["total_entries"]),
    ]
    df = pd.DataFrame(rows, columns=["stage", "count"])
    fig = go.Figure()
    colors = ["#94a3b8", "#3b82f6", "#dc2626", "#dc2626", "#dc2626", "#16a34a"]
    fig.add_trace(go.Bar(
        x=df["stage"], y=df["count"].abs(),
        marker_color=colors,
        text=df["count"].abs(),
        textposition="outside",
    ))
    fig.update_layout(
        height=280, margin=dict(l=20, r=20, t=20, b=40),
        showlegend=False, yaxis_title="count",
    )
    return fig


def entry_bp_bucket_bar(bucket_df: pd.DataFrame) -> go.Figure:
    if bucket_df is None or bucket_df.empty:
        fig = go.Figure()
        fig.update_layout(title="No closed trades")
        return fig
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=bucket_df["bucket"].astype(str), y=bucket_df["total_pnl"],
        name="Total PnL",
        marker_color=[
            "#16a34a" if v >= 0 else "#dc2626" for v in bucket_df["total_pnl"]
        ],
        text=[
            f"n={n}<br>WR {wr:.0%}"
            for n, wr in zip(bucket_df["n"], bucket_df["win_rate"])
        ],
        textposition="outside",
        hovertemplate="%{x}<br>Total PnL: $%{y:.2f}<extra></extra>",
    ))
    fig.add_hline(y=0, line_color="rgba(0,0,0,0.3)")
    fig.update_layout(
        height=300, margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title="|entry spread|", yaxis_title="Total PnL ($)",
        showlegend=False,
    )
    return fig


def trade_pnl_scatter(closed_df: pd.DataFrame) -> go.Figure:
    """진입 spread vs net PnL scatter (W/L 색상)."""
    fig = go.Figure()
    if closed_df is None or closed_df.empty:
        fig.update_layout(title="No closed trades")
        return fig
    wins = closed_df[closed_df["win"]]
    losses = closed_df[~closed_df["win"]]
    if not wins.empty:
        fig.add_trace(go.Scatter(
            x=wins["entry_spread_bps"], y=wins["net_pnl"],
            mode="markers", name="Win",
            marker=dict(color="#16a34a", size=8, opacity=0.7),
            hovertemplate="entry: %{x:.1f}bp<br>net: $%{y:+.2f}<extra></extra>",
        ))
    if not losses.empty:
        fig.add_trace(go.Scatter(
            x=losses["entry_spread_bps"], y=losses["net_pnl"],
            mode="markers", name="Loss",
            marker=dict(color="#dc2626", size=8, opacity=0.7),
            hovertemplate="entry: %{x:.1f}bp<br>net: $%{y:+.2f}<extra></extra>",
        ))
    fig.add_hline(y=0, line_color="rgba(0,0,0,0.3)")
    fig.add_vline(x=0, line_color="rgba(0,0,0,0.2)", line_dash="dot")
    fig.update_layout(
        height=320, margin=dict(l=20, r=20, t=20, b=20),
        xaxis_title="Entry spread (bp)", yaxis_title="Net PnL ($)",
    )
    return fig
