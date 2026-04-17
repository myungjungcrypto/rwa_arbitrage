from __future__ import annotations
"""좀비 포지션(MCLK26 expired 이후 열려있는 포지션) 마크투마켓 청산.

positions WHERE status='open' → 최신 perp mark price와 최신 futures price로 청산.
기본은 dry-run, --yes 플래그로 실 반영.

caveat:
    DB의 마지막 futures price는 구 contract(MCLK26)의 가격이므로 본 청산은
    "roll 시점 가격 기준 최악의 시나리오" 를 반영한다. 페이퍼 트레이딩 한정.

Usage:
    python scripts/close_zombies.py --db data/arbitrage.db            # dry-run
    python scripts/close_zombies.py --db data/arbitrage.db --yes      # apply
"""

import argparse
import sqlite3
import sys
import time
from datetime import date, datetime
from pathlib import Path


def fetch_open_positions(con: sqlite3.Connection) -> list[dict]:
    rows = con.execute(
        """SELECT id, product, perp_size, perp_entry, futures_size, futures_entry,
                  opened_at, funding_pnl
           FROM positions
           WHERE status='open'
           ORDER BY opened_at"""
    ).fetchall()
    return [dict(r) for r in rows]


def latest_prices(con: sqlite3.Connection) -> tuple[float | None, float | None, float | None]:
    """(perp_mark, fut_price, fut_ts) 반환."""
    perp = con.execute(
        """SELECT mark_price FROM perp_prices
           WHERE ticker LIKE '%CL'
           ORDER BY ts DESC LIMIT 1"""
    ).fetchone()
    fut = con.execute(
        """SELECT price, ts FROM futures_prices
           ORDER BY ts DESC LIMIT 1"""
    ).fetchone()
    return (
        perp[0] if perp else None,
        fut[0] if fut else None,
        fut[1] if fut else None,
    )


def compute_pnl(pos: dict, perp_mark: float, fut_mark: float) -> tuple[float, str]:
    """direction 판정 후 realized PnL 계산."""
    entry_spread_bps = (
        (pos["perp_entry"] - pos["futures_entry"]) / pos["futures_entry"] * 10_000
    )
    size = pos["perp_size"]
    fut_size = abs(pos["futures_size"])

    if entry_spread_bps > 0:
        direction = "long_basis"
        pnl = (pos["perp_entry"] - perp_mark) * size + (fut_mark - pos["futures_entry"]) * fut_size
    else:
        direction = "short_basis"
        pnl = (perp_mark - pos["perp_entry"]) * size + (pos["futures_entry"] - fut_mark) * fut_size
    return pnl, direction


def apply_closure(con: sqlite3.Connection, pos: dict, realized_pnl: float,
                  closed_at: float, exit_reason: str):
    con.execute(
        """UPDATE positions
           SET status='closed', realized_pnl=?, closed_at=?
           WHERE id=?""",
        (realized_pnl, closed_at, pos["id"]),
    )
    con.execute(
        """INSERT INTO orders
           (order_id, product, leg, side, size, price, filled_price, filled_size,
            status, is_paper, ts)
           VALUES (?, ?, 'perp', 'close', ?, ?, ?, ?, 'filled', 1, ?)""",
        (f"zombie_close_{pos['id']}", pos["product"],
         pos["perp_size"], pos["perp_entry"], 0, 0, closed_at),
    )


def update_daily_pnl(con: sqlite3.Connection, product: str, trading_pnl: float,
                     funding_pnl: float, dt: str):
    net = trading_pnl + funding_pnl
    con.execute(
        """INSERT INTO daily_pnl (date, product, trading_pnl, funding_pnl, fees, net_pnl, num_trades)
           VALUES (?, ?, ?, ?, 0, ?, 1)
           ON CONFLICT(date, product) DO UPDATE SET
             trading_pnl = trading_pnl + excluded.trading_pnl,
             funding_pnl = funding_pnl + excluded.funding_pnl,
             net_pnl     = net_pnl     + excluded.net_pnl,
             num_trades  = num_trades  + 1""",
        (dt, product, trading_pnl, funding_pnl, net),
    )


def main():
    parser = argparse.ArgumentParser(description="Close zombie open positions to mark")
    parser.add_argument("--db", default="data/arbitrage.db")
    parser.add_argument("--yes", action="store_true", help="Actually apply changes")
    parser.add_argument("--reason", default="mcl_rollover_zombie_cleanup_2026_04_17",
                        help="exit reason tag for audit trail")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"DB not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    try:
        positions = fetch_open_positions(con)
        if not positions:
            print("No open positions. Nothing to do.")
            return

        perp_mark, fut_mark, fut_ts = latest_prices(con)
        if perp_mark is None or fut_mark is None:
            print(f"No market data to mark. perp={perp_mark}, fut={fut_mark}", file=sys.stderr)
            sys.exit(1)

        now = time.time()
        fut_age_hours = (now - fut_ts) / 3600 if fut_ts else float("inf")

        print("=" * 88)
        print(f"  Zombie Close — db={args.db}  mode={'APPLY' if args.yes else 'DRY-RUN'}")
        print("=" * 88)
        print(f"  perp_mark = ${perp_mark:.4f}")
        print(f"  fut_mark  = ${fut_mark:.4f}   (last futures_prices ts: "
              f"{datetime.fromtimestamp(fut_ts):%Y-%m-%d %H:%M} UTC, age={fut_age_hours:.1f}h)")
        print(f"  closed_at = {datetime.fromtimestamp(now):%Y-%m-%d %H:%M} UTC")
        print()
        print(f"  {'id':>4} {'product':>8} {'opened':>16} {'dir':>12} "
              f"{'p_entry':>9} {'f_entry':>9} {'size':>6} "
              f"{'realized':>11} {'funding':>9} {'net':>11}")
        print("-" * 110)

        total_realized = 0.0
        total_funding = 0.0
        per_product: dict[str, tuple[float, float]] = {}

        for p in positions:
            pnl, direction = compute_pnl(p, perp_mark, fut_mark)
            funding = p["funding_pnl"] or 0.0
            net = pnl + funding
            opened = datetime.fromtimestamp(p["opened_at"]).strftime("%Y-%m-%d %H:%M")
            print(f"  {p['id']:>4} {p['product']:>8} {opened:>16} {direction:>12} "
                  f"{p['perp_entry']:>9.2f} {p['futures_entry']:>9.2f} "
                  f"{p['perp_size']:>6.2f} "
                  f"${pnl:>+10.2f} ${funding:>+8.2f} ${net:>+10.2f}")

            total_realized += pnl
            total_funding += funding
            prev = per_product.get(p["product"], (0.0, 0.0))
            per_product[p["product"]] = (prev[0] + pnl, prev[1] + funding)

            if args.yes:
                apply_closure(con, p, pnl, now, args.reason)
                update_daily_pnl(con, p["product"], pnl, funding,
                                 date.today().isoformat())

        print("-" * 110)
        print(f"  Totals ({len(positions)} positions): "
              f"realized=${total_realized:+.2f}  funding=${total_funding:+.2f}  "
              f"net=${total_realized + total_funding:+.2f}")
        for prod, (r, f) in per_product.items():
            print(f"    [{prod}] realized=${r:+.2f}  funding=${f:+.2f}  net=${r+f:+.2f}")

        if args.yes:
            con.commit()
            print("\n  [OK] Applied. Run scripts/analyze_paper.py to verify.")
        else:
            print("\n  (dry-run) re-run with --yes to apply")
    finally:
        con.close()


if __name__ == "__main__":
    main()
