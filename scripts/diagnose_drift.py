from __future__ import annotations
"""Basis drift 진단 스크립트.

perp vs futures(KIS) 가격 시계열을 로딩, resample/forward-fill 조인 후
basis_bps 계산 → 지속 괴리 시점과 규모를 출력한다.

매월 5~10 영업일 roll window 음영을 포함한 3-subplot PNG 생성.

Usage:
    python scripts/diagnose_drift.py
    python scripts/diagnose_drift.py --db data/arbitrage_snapshot.db --out data/drift.png
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path


def load_series(db_path: str):
    """perp_prices / futures_prices / positions 로딩."""
    import pandas as pd

    con = sqlite3.connect(db_path)
    try:
        perp = pd.read_sql_query(
            """SELECT ts, ticker, mark_price, index_price, funding_rate
               FROM perp_prices
               WHERE ticker LIKE '%CL'
               ORDER BY ts""",
            con,
        )
        fut = pd.read_sql_query(
            """SELECT ts, symbol, contract_month, price, bid, ask
               FROM futures_prices
               ORDER BY ts""",
            con,
        )
        pos = pd.read_sql_query(
            """SELECT id, product, perp_entry, futures_entry,
                      opened_at, closed_at, status, realized_pnl, funding_pnl
               FROM positions
               ORDER BY opened_at""",
            con,
        )
    finally:
        con.close()

    for df, col in [(perp, "ts"), (fut, "ts"), (pos, "opened_at")]:
        df["dt"] = pd.to_datetime(df[col], unit="s", utc=True)
    return perp, fut, pos


def compute_basis(perp, fut, resample: str = "5min"):
    """resample + forward-fill join, basis_bps 컬럼 추가."""
    import pandas as pd

    p = perp.set_index("dt")[["mark_price", "index_price"]].resample(resample).last().ffill()
    f = fut.set_index("dt")[["price", "contract_month"]].resample(resample).last().ffill()
    df = p.join(f, how="inner").dropna()
    df["basis_bps"] = (df["mark_price"] - df["price"]) / df["price"] * 10_000
    df["index_basis_bps"] = (df["mark_price"] - df["index_price"]) / df["index_price"] * 10_000
    return df


def find_divergence_start(series, threshold_bps: float = 50.0, min_duration_minutes: int = 60):
    """basis_bps 절대값이 threshold를 넘은 후 min_duration 이상 지속된 최초 시점."""
    import pandas as pd

    s = series["basis_bps"]
    violated = s.abs() >= threshold_bps
    if not violated.any():
        return None

    resolution_minutes = (s.index[1] - s.index[0]).total_seconds() / 60
    required_samples = max(1, int(min_duration_minutes / resolution_minutes))

    rolling_violation = violated.rolling(required_samples).sum()
    first_sustained = rolling_violation[rolling_violation >= required_samples].index
    if len(first_sustained) == 0:
        return None
    first_idx = first_sustained[0]
    return first_idx - pd.Timedelta(minutes=min_duration_minutes)


def console_summary(df, positions, divergence_start):
    print("\n" + "=" * 80)
    print("  Basis Drift Diagnostic")
    print("=" * 80)
    print(f"  Period:    {df.index[0]}  →  {df.index[-1]}")
    print(f"  Samples:   {len(df):,}")
    print()

    if divergence_start is not None:
        post = df.loc[divergence_start:]
        pre = df.loc[:divergence_start]
        print(f"  First sustained drift (|basis|≥50bp, ≥1h): {divergence_start}")
        print(f"    Pre-drift   mean basis_bps = {pre['basis_bps'].mean():+8.1f}  (n={len(pre):,})")
        print(f"    Post-drift  mean basis_bps = {post['basis_bps'].mean():+8.1f}  (n={len(post):,})")
    else:
        print("  No sustained drift (|basis|≥50bp for ≥1h) detected.")

    print()
    print("  contract_month 별 basis_bps mean:")
    for sym, grp in df.groupby("contract_month"):
        print(f"    {sym:>10}: mean={grp['basis_bps'].mean():+8.1f}  "
              f"min={grp['basis_bps'].min():+8.1f}  max={grp['basis_bps'].max():+8.1f}  "
              f"n={len(grp):,}")

    open_pos = positions[positions["status"] == "open"]
    closed_pos = positions[positions["status"] == "closed"]
    print()
    print(f"  Positions:  open={len(open_pos)}  closed={len(closed_pos)}")
    if len(open_pos):
        print(f"    Earliest open: {open_pos['dt'].min()}")
        print(f"    Latest open:   {open_pos['dt'].max()}")


def plot(df, positions, divergence_start, out_png: str):
    import matplotlib.dates as mdates
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    import pandas as pd

    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)

    ax1 = axes[0]
    ax1.plot(df.index, df["mark_price"], label="perp mark", color="tab:blue", linewidth=1)
    ax1.plot(df.index, df["price"], label="KIS futures", color="tab:orange", linewidth=1)
    ax1.plot(df.index, df["index_price"], label="perp index", color="tab:green",
             linewidth=0.8, alpha=0.7)
    ax1.set_ylabel("Price ($)")
    ax1.legend(loc="upper left")
    ax1.set_title("WTI: perp mark vs KIS futures vs perp index")
    ax1.grid(True, alpha=0.3)

    ax2 = axes[1]
    ax2.plot(df.index, df["basis_bps"], label="perp - KIS futures", color="tab:red", linewidth=1)
    ax2.plot(df.index, df["index_basis_bps"], label="perp - perp index", color="gray",
             linewidth=0.8, alpha=0.6)
    ax2.axhline(0, color="black", linewidth=0.5)
    ax2.axhline(50, color="gray", linestyle="--", linewidth=0.5)
    ax2.axhline(-50, color="gray", linestyle="--", linewidth=0.5)
    ax2.set_ylabel("basis (bp)")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3)

    start_date = df.index[0].normalize()
    end_date = df.index[-1].normalize()
    months = pd.date_range(start_date, end_date + pd.DateOffset(months=1), freq="MS")
    for m in months:
        roll_start, roll_end = _roll_window(m)
        if roll_end < df.index[0] or roll_start > df.index[-1]:
            continue
        for a in (ax1, ax2):
            a.axvspan(roll_start, roll_end, color="yellow", alpha=0.15)

    if divergence_start is not None:
        for a in (ax1, ax2):
            a.axvline(divergence_start, color="red", linestyle="--", linewidth=1,
                      label=f"drift start {divergence_start:%m-%d %H:%M}")

    ax3 = axes[2]
    open_pos = positions[positions["status"] == "open"]
    closed_pos = positions[positions["status"] == "closed"]
    if len(closed_pos):
        ax3.scatter(closed_pos["dt"], closed_pos["perp_entry"], color="tab:blue",
                    s=15, label=f"closed (n={len(closed_pos)})", alpha=0.6)
    if len(open_pos):
        ax3.scatter(open_pos["dt"], open_pos["perp_entry"], color="red",
                    s=40, marker="^", label=f"open (n={len(open_pos)})", zorder=5)
    ax3.plot(df.index, df["mark_price"], color="tab:blue", linewidth=0.5, alpha=0.4)
    ax3.set_ylabel("Entry price ($)")
    ax3.set_xlabel("Time (UTC)")
    ax3.legend(loc="upper left")
    ax3.grid(True, alpha=0.3)

    roll_patch = mpatches.Patch(color="yellow", alpha=0.3, label="CME roll window (BD 5-10)")
    ax2.legend(handles=[*ax2.get_legend().legendHandles, roll_patch], loc="upper left")

    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()
    fig.tight_layout()

    Path(out_png).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_png, dpi=120, bbox_inches="tight")
    print(f"\n  Plot saved: {out_png}")


def _roll_window(month_start):
    """주어진 월(MS)의 5~10 영업일 UTC 범위 반환."""
    import pandas as pd

    days = pd.bdate_range(month_start, month_start + pd.DateOffset(days=20))
    if len(days) < 10:
        return month_start, month_start
    start = days[4]
    end = days[9] + pd.Timedelta(days=1)
    return (start.tz_localize("UTC") if start.tzinfo is None else start,
            end.tz_localize("UTC") if end.tzinfo is None else end)


def main():
    parser = argparse.ArgumentParser(description="Basis drift diagnostic")
    parser.add_argument("--db", default="data/arbitrage_snapshot.db")
    parser.add_argument("--out", default="data/drift.png")
    parser.add_argument("--resample", default="5min")
    parser.add_argument("--threshold-bps", type=float, default=50.0)
    parser.add_argument("--min-duration-min", type=int, default=60)
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    try:
        import pandas as pd  # noqa
        import matplotlib  # noqa
    except ImportError as e:
        print(f"Missing dependency: {e}. Install with: pip install pandas matplotlib",
              file=sys.stderr)
        sys.exit(1)

    perp, fut, pos = load_series(args.db)
    if perp.empty or fut.empty:
        print("No data to diagnose (empty perp or futures table).", file=sys.stderr)
        sys.exit(1)

    df = compute_basis(perp, fut, resample=args.resample)
    divergence_start = find_divergence_start(
        df, threshold_bps=args.threshold_bps, min_duration_minutes=args.min_duration_min
    )

    console_summary(df, pos, divergence_start)
    plot(df, pos, divergence_start, args.out)


if __name__ == "__main__":
    main()
