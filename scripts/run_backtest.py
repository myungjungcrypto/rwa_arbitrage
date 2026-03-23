from __future__ import annotations
"""수집된 실데이터로 백테스트 실행.

Usage:
    python scripts/run_backtest.py                    # 기본 설정
    python scripts/run_backtest.py --hours 6          # 최근 6시간
    python scripts/run_backtest.py --product wti      # WTI만
    python scripts/run_backtest.py --optimize          # 파라미터 최적화
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from itertools import product as iterproduct

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np

from src.data.storage import Storage
from src.strategy.basis_arb import BacktestEngine, BacktestResult
from src.utils.config import load_config


def load_basis_data(storage: Storage, product: str, hours: float) -> tuple[list[float], list[float], list[float]]:
    """DB에서 베이시스 + 펀딩 데이터 로드."""
    rows = storage.get_recent_basis(product, hours=hours)
    if not rows:
        print(f"No basis data for {product} in last {hours}h")
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
) -> BacktestResult:
    """단일 백테스트."""
    engine = BacktestEngine()

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


def optimize(
    product: str,
    basis: list[float],
    funding: list[float],
    timestamps: list[float],
) -> tuple[dict, BacktestResult]:
    """그리드 서치로 파라미터 최적화."""
    print(f"\n=== Parameter Optimization: {product} ===")
    print(f"Data: {len(basis)} points\n")

    param_grid = {
        "window_hours": [1, 2, 4, 8],
        "std_multiplier": [1.5, 2.0, 2.5, 3.0],
        "entry_threshold_bps": [10, 20, 30, 50],
        "exit_threshold_bps": [3, 5, 10],
        "target_profit_bps": [15, 25, 40],
        "max_hold_hours": [2, 6, 12, 24],
    }

    best_result = None
    best_params = {}
    best_sharpe = -999

    keys = list(param_grid.keys())
    values = list(param_grid.values())
    total = 1
    for v in values:
        total *= len(v)

    print(f"Testing {total} parameter combinations...")

    count = 0
    for combo in iterproduct(*values):
        params = dict(zip(keys, combo))
        result = run_single(product, basis, funding, timestamps, params)
        count += 1

        # Sharpe 기준으로 최적화 (최소 5개 거래)
        if result.total_trades >= 5 and result.sharpe_ratio > best_sharpe:
            best_sharpe = result.sharpe_ratio
            best_result = result
            best_params = params

        if count % 200 == 0:
            print(f"  {count}/{total} tested...")

    if best_result:
        print(f"\n=== Best Parameters ===")
        for k, v in best_params.items():
            print(f"  {k}: {v}")
        print(f"\n{best_result.summary()}")
    else:
        print("No valid results found (need at least 5 trades)")

    return best_params, best_result


def main():
    parser = argparse.ArgumentParser(description="Run backtest on collected data")
    parser.add_argument("--hours", type=float, default=24, help="Hours of data to use")
    parser.add_argument("--product", default="all", help="Product (wti/brent/all)")
    parser.add_argument("--optimize", action="store_true", help="Run parameter optimization")
    parser.add_argument("--db", default="data/arbitrage.db", help="Database path")
    args = parser.parse_args()

    config = load_config("config/settings.yaml")
    storage = Storage(args.db)
    storage.connect()

    products = list(config.products.keys()) if args.product == "all" else [args.product]

    for prod in products:
        basis, funding, timestamps = load_basis_data(storage, prod, args.hours)
        if not basis:
            continue

        print(f"\n{'='*60}")
        print(f"Product: {prod.upper()}")
        print(f"Data points: {len(basis)}, time span: {(timestamps[-1]-timestamps[0])/3600:.1f}h")
        print(f"Basis: mean={np.mean(basis):.1f}bp, std={np.std(basis):.1f}bp, "
              f"range=[{min(basis):.1f}, {max(basis):.1f}]bp")

        if args.optimize:
            optimize(prod, basis, funding, timestamps)
        else:
            # 기본 파라미터로 실행
            params = {
                "window_hours": config.strategy.basis_window_hours,
                "std_multiplier": config.strategy.basis_std_multiplier,
                "entry_threshold_bps": config.strategy.entry_threshold_bps,
                "exit_threshold_bps": config.strategy.exit_threshold_bps,
                "target_profit_bps": config.strategy.target_profit_bps,
                "max_hold_hours": config.strategy.max_hold_hours,
            }
            result = run_single(prod, basis, funding, timestamps, params)
            print(f"\n{result.summary()}")

            if result.trades:
                print(f"\n--- Recent trades ---")
                for t in result.trades[-5:]:
                    print(f"  {t.direction}: entry={t.entry_basis_bps:.1f}bp → exit={t.exit_basis_bps:.1f}bp "
                          f"net={t.net_pnl_bps:.1f}bp hold={t.hold_hours:.1f}h ({t.exit_reason[:40]})")

    storage.close()


if __name__ == "__main__":
    main()
