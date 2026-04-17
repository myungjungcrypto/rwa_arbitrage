from __future__ import annotations
"""페이퍼 트레이딩 결과 상세 분석.

positions 테이블의 closed/open trades를 기준으로:
  - 각 거래의 entry/exit basis, hold, PnL 분해
  - 승/패 패턴 (entry basis, direction, hold, funding 분포)
  - 일별/주별 summary + M7 전후 비교
  - 오픈 포지션의 현재 unrealized PnL

Usage:
    python scripts/analyze_paper.py
    python scripts/analyze_paper.py --db data/arbitrage_snapshot.db
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from statistics import mean, median, stdev


def fmt_ts(t: float | None) -> str:
    if not t:
        return "-"
    return datetime.fromtimestamp(t).strftime("%Y-%m-%d %H:%M")


def load_closed_trades(con: sqlite3.Connection) -> list[dict]:
    rows = con.execute(
        """SELECT id, product, perp_size, perp_entry, futures_size, futures_entry,
                  realized_pnl, funding_pnl, opened_at, closed_at
           FROM positions
           WHERE status='closed'
           ORDER BY opened_at"""
    ).fetchall()
    trades = []
    for r in rows:
        d = dict(r)
        d["hold_hours"] = (d["closed_at"] - d["opened_at"]) / 3600 if d["closed_at"] else 0
        entry_spread_bps = (d["perp_entry"] - d["futures_entry"]) / d["futures_entry"] * 10_000
        d["entry_spread_bps"] = entry_spread_bps
        d["direction"] = "long_basis" if entry_spread_bps > 0 else "short_basis"
        d["net_pnl"] = (d["realized_pnl"] or 0) + (d["funding_pnl"] or 0)
        trades.append(d)
    return trades


def load_open_trades(con: sqlite3.Connection) -> list[dict]:
    rows = con.execute(
        """SELECT id, product, perp_size, perp_entry, futures_size, futures_entry,
                  opened_at
           FROM positions
           WHERE status='open'
           ORDER BY opened_at"""
    ).fetchall()
    return [dict(r) for r in rows]


def latest_prices(con: sqlite3.Connection, product: str) -> tuple[float | None, float | None]:
    """최근 perp mark price, futures price."""
    perp = con.execute(
        """SELECT mark_price FROM perp_prices
           WHERE ticker LIKE '%CL' OR ticker LIKE '%BZ'
           ORDER BY ts DESC LIMIT 1"""
    ).fetchone()
    fut = con.execute(
        """SELECT price FROM futures_prices
           ORDER BY ts DESC LIMIT 1"""
    ).fetchone()
    return (perp["mark_price"] if perp else None, fut["price"] if fut else None)


def print_header(title: str):
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


def summarize(trades: list[dict], label: str):
    if not trades:
        print(f"\n[{label}] (no trades)")
        return
    realized = sum(t["realized_pnl"] or 0 for t in trades)
    funding = sum(t["funding_pnl"] or 0 for t in trades)
    net = realized + funding
    wins = [t for t in trades if t["net_pnl"] > 0]
    holds = [t["hold_hours"] for t in trades]
    entries = [t["entry_spread_bps"] for t in trades]

    print(f"\n[{label}]  n={len(trades)}")
    print(f"  PnL: realized=${realized:+.2f}  funding=${funding:+.2f}  net=${net:+.2f}")
    print(f"  Win rate: {len(wins)}/{len(trades)} ({100*len(wins)/len(trades):.0f}%)")
    print(f"  Hold (h):  mean={mean(holds):.1f}  median={median(holds):.1f}  "
          f"max={max(holds):.1f}")
    print(f"  Entry spread (bp):  mean={mean(entries):+.1f}  "
          f"range=[{min(entries):+.1f}, {max(entries):+.1f}]")
    longs = [t for t in trades if t["direction"] == "long_basis"]
    shorts = [t for t in trades if t["direction"] == "short_basis"]
    print(f"  Direction:  long_basis={len(longs)}  short_basis={len(shorts)}")


def bucket_by_pnl(trades: list[dict]):
    """승자 vs 패자 entry spread 분포 비교."""
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    if wins and losses:
        w_entry = [abs(t["entry_spread_bps"]) for t in wins]
        l_entry = [abs(t["entry_spread_bps"]) for t in losses]
        w_hold = [t["hold_hours"] for t in wins]
        l_hold = [t["hold_hours"] for t in losses]
        print(f"\n--- Win vs Loss 분포 ---")
        print(f"  |entry spread|  (bp):  win_avg={mean(w_entry):.1f}  loss_avg={mean(l_entry):.1f}")
        print(f"  hold hours:            win_avg={mean(w_hold):.1f}  loss_avg={mean(l_hold):.1f}")
        w_net = [t["net_pnl"] for t in wins]
        l_net = [t["net_pnl"] for t in losses]
        print(f"  net PnL ($):           win_avg=${mean(w_net):+.2f}  loss_avg=${mean(l_net):+.2f}")


def print_all_trades(trades: list[dict]):
    print(f"\n{'#':>3} {'opened':>16} {'closed':>16} {'hold_h':>7} "
          f"{'dir':>12} {'entry_bp':>9} {'realized':>10} {'funding':>9} {'net':>9}")
    print("-" * 100)
    for i, t in enumerate(trades, 1):
        sign = "W" if t["net_pnl"] > 0 else "L"
        print(f"{i:>3} {fmt_ts(t['opened_at']):>16} {fmt_ts(t['closed_at']):>16} "
              f"{t['hold_hours']:>7.1f} {t['direction']:>12} "
              f"{t['entry_spread_bps']:>+9.1f} "
              f"${t['realized_pnl'] or 0:>+9.2f} ${t['funding_pnl'] or 0:>+8.2f} "
              f"${t['net_pnl']:>+8.2f} {sign}")


def split_m6_m7(trades: list[dict], cutoff_date: str = "2026-04-07") -> tuple[list, list]:
    """M7 변경 추정일 기준 분할."""
    cutoff_ts = datetime.fromisoformat(cutoff_date).timestamp()
    m6 = [t for t in trades if t["opened_at"] < cutoff_ts]
    m7 = [t for t in trades if t["opened_at"] >= cutoff_ts]
    return m6, m7


def unrealized_for_open(open_trades: list[dict], perp_now: float | None, fut_now: float | None):
    if not open_trades:
        print("\n[Open positions] (없음)")
        return
    print_header(f"Open Positions ({len(open_trades)})")
    if perp_now is None or fut_now is None:
        print(f"  현재가 조회 실패. perp={perp_now}, fut={fut_now}")
        return
    print(f"  현재가:  perp=${perp_now:.2f}  fut=${fut_now:.2f}  "
          f"spread={(perp_now - fut_now) / fut_now * 10_000:+.1f}bp")
    print(f"\n{'#':>3} {'opened':>16} {'age_h':>7} {'dir':>12} "
          f"{'entry_bp':>9} {'p_entry':>9} {'f_entry':>9} "
          f"{'unrealized':>12}")
    print("-" * 90)
    total_unrealized = 0.0
    now = datetime.now().timestamp()
    for i, t in enumerate(open_trades, 1):
        entry_spread = (t["perp_entry"] - t["futures_entry"]) / t["futures_entry"] * 10_000
        direction = "long_basis" if entry_spread > 0 else "short_basis"
        size = t["perp_size"]
        if direction == "long_basis":
            pnl = (t["perp_entry"] - perp_now) * size + (fut_now - t["futures_entry"]) * abs(t["futures_size"])
        else:
            pnl = (perp_now - t["perp_entry"]) * size + (t["futures_entry"] - fut_now) * abs(t["futures_size"])
        total_unrealized += pnl
        age = (now - t["opened_at"]) / 3600
        print(f"{i:>3} {fmt_ts(t['opened_at']):>16} {age:>7.1f} {direction:>12} "
              f"{entry_spread:>+9.1f} {t['perp_entry']:>9.2f} {t['futures_entry']:>9.2f} "
              f"${pnl:>+11.2f}")
    print(f"\n  총 unrealized PnL: ${total_unrealized:+.2f}")


def main():
    parser = argparse.ArgumentParser(description="Analyze paper trading results")
    parser.add_argument("--db", default="data/arbitrage_snapshot.db", help="DB path")
    parser.add_argument("--all", action="store_true", help="Print all individual trades")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row

    closed = load_closed_trades(con)
    open_ = load_open_trades(con)

    print_header(f"Paper Trading Results  (DB: {args.db})")
    print(f"  Closed trades: {len(closed)}   Open positions: {len(open_)}")
    if closed:
        first = fmt_ts(min(t["opened_at"] for t in closed))
        last = fmt_ts(max(t["closed_at"] for t in closed if t["closed_at"]))
        print(f"  Period: {first}  →  {last}")

    # 전체 요약
    summarize(closed, "ALL closed")

    # M6 vs M7 분할 (2026-04-07 기준)
    m6, m7 = split_m6_m7(closed, "2026-04-07")
    summarize(m6, "M6 (before 2026-04-07)")
    summarize(m7, "M7 (from 2026-04-07)")

    # 승/패 분포
    print_header("승자 vs 패자 패턴")
    bucket_by_pnl(closed)

    # 개별 trades
    if args.all:
        print_header("모든 개별 거래")
        print_all_trades(closed)
    else:
        print_header("최근 10건")
        print_all_trades(closed[-10:])

    # 오픈 포지션 unrealized
    perp_now, fut_now = latest_prices(con, "wti")
    unrealized_for_open(open_, perp_now, fut_now)

    # 일별 (daily_pnl)
    print_header("일별 PnL (daily_pnl 테이블)")
    rows = con.execute(
        """SELECT date, product, num_trades,
                  ROUND(trading_pnl,2), ROUND(funding_pnl,2),
                  ROUND(fees,2), ROUND(net_pnl,2)
           FROM daily_pnl ORDER BY date"""
    ).fetchall()
    print(f"  {'date':>12} {'product':>8} {'n':>3} {'trading':>9} "
          f"{'funding':>9} {'fees':>8} {'net':>9}")
    for r in rows:
        print(f"  {r[0]:>12} {r[1]:>8} {r[2]:>3} ${r[3]:>+8.2f} ${r[4]:>+8.2f} "
              f"${r[5]:>7.2f} ${r[6]:>+8.2f}")

    con.close()


if __name__ == "__main__":
    main()
