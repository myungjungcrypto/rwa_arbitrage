from __future__ import annotations
"""수집된 실데이터로 백테스트 실행 + 파라미터 그리드 서치 최적화.

Usage:
    python scripts/run_backtest.py                        # 기본 설정으로 실행
    python scripts/run_backtest.py --hours 24             # 최근 24시간
    python scripts/run_backtest.py --all                  # 전체 데이터
    python scripts/run_backtest.py --optimize             # 파라미터 최적화
    python scripts/run_backtest.py --optimize --top 20    # Top 20 결과
"""


import argparse
import csv
import sys
import time
from pathlib import Path
from itertools import product as iterproduct

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np

from src.data.storage import Storage
from src.strategy.basis_arb import BacktestEngine, BacktestResult
from src.utils.config import load_config


def load_basis_data(
    storage: Storage, product: str, hours: float | None = None
) -> tuple[list[float], list[float], list[float]]:
    """DB에서 베이시스 + 펀딩 데이터 로드."""
    if hours is None:
        rows = storage.get_all_basis(product)
    else:
        rows = storage.get_recent_basis(product, hours=hours)

    if not rows:
        print(f"No basis data for {product}")
        return [], [], []

    basis = [r["basis_bps"] for r in rows]
    funding = [r.get("funding_rate", 0) or 0 for r in rows]
    timestamps = [r["ts"] for r in rows]

    return basis, funding, timestamps


def run_single(
    product: str,
    basis: list[float],
    funding: list[float],
    timestamps: list[float],
    params: dict,
    spread_params: dict | None = None,
) -> BacktestResult:
    """단일 백테스트."""
    sp = spread_params or {}
    engine = BacktestEngine(
        perp_spread_bps=sp.get("perp_spread_bps", 3.0),
        futures_spread_bps=sp.get("futures_spread_bps", 3.0),
    )

    # interval 추정
    if len(timestamps) > 1:
        intervals = [timestamps[i+1] - timestamps[i] for i in range(min(100, len(timestamps)-1))]
        avg_interval = np.median(intervals)
    else:
        avg_interval = 5.0

    return engine.run(
        product=product,
        basis_series=basis,
        funding_series=funding,
        timestamps=timestamps,
        interval_seconds=avg_interval,
        signal_params=params,
    )


def print_top_results(
    all_results: list[tuple[dict, BacktestResult]],
    sort_key: str,
    top_n: int = 10,
    reverse: bool = True,
):
    """결과 Top N 출력."""
    # 거래 1건 이상인 것만 필터
    valid = [(p, r) for p, r in all_results if r.total_trades >= 1]
    if not valid:
        print(f"  No valid results for {sort_key}")
        return

    if sort_key == "win_rate":
        valid.sort(key=lambda x: (x[1].win_rate, x[1].total_pnl_bps), reverse=reverse)
    elif sort_key == "total_pnl":
        valid.sort(key=lambda x: x[1].total_pnl_bps, reverse=reverse)
    elif sort_key == "sharpe":
        valid.sort(key=lambda x: x[1].sharpe_ratio, reverse=reverse)
    elif sort_key == "avg_pnl":
        valid.sort(key=lambda x: x[1].avg_pnl_bps, reverse=reverse)

    print(f"\n{'='*80}")
    print(f"  Top {min(top_n, len(valid))} by {sort_key.upper()} (min 1 trade)")
    print(f"{'='*80}")
    print(f"{'#':>3} {'win%':>5} {'trades':>6} {'total_bp':>9} {'avg_bp':>7} {'sharpe':>7} "
          f"{'hold_h':>6} {'window':>6} {'z':>4} {'entry':>5} {'exit':>4} {'target':>6} {'max_h':>5}")
    print("-" * 80)

    for i, (params, result) in enumerate(valid[:top_n]):
        print(
            f"{i+1:>3} "
            f"{result.win_rate:>5.0%} "
            f"{result.total_trades:>6} "
            f"{result.total_pnl_bps:>+9.1f} "
            f"{result.avg_pnl_bps:>+7.1f} "
            f"{result.sharpe_ratio:>7.2f} "
            f"{result.avg_hold_hours:>6.1f} "
            f"{params.get('window_hours', '-'):>6} "
            f"{params.get('std_multiplier', '-'):>4} "
            f"{params.get('entry_threshold_bps', '-'):>5} "
            f"{params.get('exit_threshold_bps', '-'):>4} "
            f"{params.get('target_profit_bps', '-'):>6} "
            f"{params.get('max_hold_hours', '-'):>5}"
        )


def save_results_csv(
    all_results: list[tuple[dict, BacktestResult]],
    output_path: str,
):
    """모든 결과를 CSV로 저장."""
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "window_hours", "std_multiplier", "entry_threshold_bps",
            "exit_threshold_bps", "target_profit_bps", "max_hold_hours",
            "total_trades", "winning_trades", "losing_trades", "win_rate",
            "total_pnl_bps", "avg_pnl_bps", "max_pnl_bps", "min_pnl_bps",
            "sharpe_ratio", "max_drawdown_bps", "avg_hold_hours",
            "total_fees_bps", "total_funding_pnl_bps", "exit_reasons",
        ])
        for params, result in all_results:
            writer.writerow([
                params.get("window_hours"),
                params.get("std_multiplier"),
                params.get("entry_threshold_bps"),
                params.get("exit_threshold_bps"),
                params.get("target_profit_bps"),
                params.get("max_hold_hours"),
                result.total_trades,
                result.winning_trades,
                result.losing_trades,
                f"{result.win_rate:.4f}",
                f"{result.total_pnl_bps:.2f}",
                f"{result.avg_pnl_bps:.2f}",
                f"{result.max_pnl_bps:.2f}",
                f"{result.min_pnl_bps:.2f}",
                f"{result.sharpe_ratio:.4f}",
                f"{result.max_drawdown_bps:.2f}",
                f"{result.avg_hold_hours:.2f}",
                f"{result.total_fees_bps:.2f}",
                f"{result.total_funding_pnl_bps:.2f}",
                str(result.exit_reasons),
            ])

    print(f"\nResults saved to {path} ({len(all_results)} combinations)")


def optimize(
    product: str,
    basis: list[float],
    funding: list[float],
    timestamps: list[float],
    top_n: int = 10,
    spread_params: dict | None = None,
) -> list[tuple[dict, BacktestResult]]:
    """그리드 서치로 파라미터 최적화."""
    print(f"\n{'='*80}")
    print(f"  Parameter Optimization: {product.upper()}")
    print(f"  Data: {len(basis)} points, "
          f"time span: {(timestamps[-1]-timestamps[0])/3600:.1f}h")
    print(f"  Basis: mean={np.mean(basis):.1f}bp, std={np.std(basis):.1f}bp, "
          f"range=[{min(basis):.1f}, {max(basis):.1f}]bp")
    print(f"{'='*80}")

    param_grid = {
        "window_hours": [4, 8, 12, 24],
        "std_multiplier": [2.0, 2.5, 3.0],
        "entry_threshold_bps": [20, 25, 30, 35],
        "exit_threshold_bps": [3, 5, 8, 12],
        "target_profit_bps": [15, 20, 30],
        "max_hold_hours": [2, 4, 8, 12],
    }

    keys = list(param_grid.keys())
    values = list(param_grid.values())
    total = 1
    for v in values:
        total *= len(v)

    print(f"Testing {total} parameter combinations...\n")

    all_results: list[tuple[dict, BacktestResult]] = []
    count = 0
    t0 = time.time()

    for combo in iterproduct(*values):
        params = dict(zip(keys, combo))
        result = run_single(product, basis, funding, timestamps, params, spread_params)
        all_results.append((params, result))
        count += 1

        if count % 500 == 0:
            elapsed = time.time() - t0
            eta = elapsed / count * (total - count)
            print(f"  {count}/{total} tested... ({elapsed:.0f}s elapsed, ~{eta:.0f}s remaining)")

    elapsed = time.time() - t0
    print(f"\nCompleted {total} combinations in {elapsed:.1f}s")

    # Top N 출력 (여러 기준)
    print_top_results(all_results, "win_rate", top_n)
    print_top_results(all_results, "total_pnl", top_n)
    print_top_results(all_results, "sharpe", top_n)

    return all_results


def main():
    parser = argparse.ArgumentParser(description="Run backtest on collected data")
    parser.add_argument("--hours", type=float, default=None, help="Hours of data (default: all)")
    parser.add_argument("--all", action="store_true", help="Use all available data")
    parser.add_argument("--product", default="wti", help="Product (wti/brent)")
    parser.add_argument("--optimize", action="store_true", help="Run parameter optimization")
    parser.add_argument("--top", type=int, default=10, help="Top N results to show")
    parser.add_argument("--csv", default="data/backtest_results.csv", help="CSV output path")
    parser.add_argument("--db", default="data/arbitrage.db", help="Database path")
    parser.add_argument("--sample", type=int, default=6, help="Downsample factor (1=no sampling, 6=every 30s)")
    args = parser.parse_args()

    # hours 기본값: --all이면 None(전체), 아니면 24시간
    hours = None if args.all else (args.hours or 24)

    storage = Storage(args.db)
    storage.connect()

    # 실제 스프레드 추정
    spread_stats = storage.get_spread_stats(args.product, hours=hours or 999)
    futures_spread = spread_stats.get("avg_spread_bps", 6.0)
    print(f"Futures avg spread: {futures_spread:.1f}bp (half: {futures_spread/2:.1f}bp)")
    spread_params = {
        "perp_spread_bps": 3.0,  # perp은 DB에 없으므로 추정치
        "futures_spread_bps": futures_spread / 2,  # half-spread
    }

    basis, funding, timestamps = load_basis_data(storage, args.product, hours)
    if not basis:
        print("No data found.")
        storage.close()
        return

    # 다운샘플링 (속도 최적화)
    if args.sample > 1:
        basis = basis[::args.sample]
        funding = funding[::args.sample]
        timestamps = timestamps[::args.sample]
        print(f"Downsampled {args.sample}x → {len(basis)} points")

    print(f"\nProduct: {args.product.upper()}")
    print(f"Data points: {len(basis)}, time span: {(timestamps[-1]-timestamps[0])/3600:.1f}h")
    print(f"Basis: mean={np.mean(basis):.1f}bp, std={np.std(basis):.1f}bp, "
          f"range=[{min(basis):.1f}, {max(basis):.1f}]bp")

    if args.optimize:
        all_results = optimize(
            args.product, basis, funding, timestamps,
            top_n=args.top, spread_params=spread_params,
        )
        save_results_csv(all_results, args.csv)
    else:
        # 현재 설정으로 단일 실행
        try:
            config = load_config("config/settings.yaml")
            params = {
                "window_hours": config.strategy.basis_window_hours,
                "std_multiplier": config.strategy.basis_std_multiplier,
                "entry_threshold_bps": config.strategy.entry_threshold_bps,
                "exit_threshold_bps": config.strategy.exit_threshold_bps,
                "target_profit_bps": config.strategy.target_profit_bps,
                "max_hold_hours": config.strategy.max_hold_hours,
            }
        except Exception:
            params = {
                "window_hours": 12,
                "std_multiplier": 3.0,
                "entry_threshold_bps": 25,
                "exit_threshold_bps": 4,
                "target_profit_bps": 20,
                "max_hold_hours": 8,
            }

        result = run_single(args.product, basis, funding, timestamps, params, spread_params)
        print(f"\n{result.summary()}")

        if result.trades:
            print(f"\n--- All trades ---")
            for i, t in enumerate(result.trades):
                sign = "✅" if t.net_pnl_bps > 0 else "❌"
                print(f"  {sign} #{i+1} {t.direction}: "
                      f"entry={t.entry_basis_bps:+.1f}bp → exit={t.exit_basis_bps:+.1f}bp "
                      f"net={t.net_pnl_bps:+.1f}bp hold={t.hold_hours:.2f}h "
                      f"({t.exit_reason[:50]})")

    storage.close()


if __name__ == "__main__":
    main()
