"""Signal smoothing 백테스트 — Option B 수익성 검증.

5/18 LIVE 5건 손실 분석: 모두 spike(1-2초 mispricing)을 latency(1-7초) 안에
잡으려다 reversion에 휘말림. spike는 진짜 edge일 수도, oracle lag/WS push
artifact일 수도 있는데, 1-tick fire 정책은 셋 다 trigger.

Option B 가설: 신호가 N개 tick(또는 N초) 연속 유지될 때만 entry → 진짜
mispricing만 잡고 spike artifact 제외. 단점은 entry count 감소 + slow signal.

본 스크립트는 production 코드 수정 없이 historical basis_spread 데이터로
다양한 (min_signal_ticks, entry_threshold_bps) 조합을 시뮬레이션한다.

비교 기준:
  baseline: min_signal_ticks=1 (현재 LIVE 정책)
  candidates: 2, 3, 5, 10
  thresholds: 15, 20, 25, 30, 35bp

Usage:
    python scripts/backtest_signal_smoothing.py                  # 전체 데이터
    python scripts/backtest_signal_smoothing.py --hours 168      # 최근 7일
    python scripts/backtest_signal_smoothing.py --product wti
"""
from __future__ import annotations

import argparse
import math
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.storage import Storage


# ──────────────────────────────────────────────
# 백테스트 모델
# ──────────────────────────────────────────────

@dataclass
class Params:
    window_hours: float = 12.0          # rolling stats window
    std_multiplier: float = 2.5         # entry: |basis - mean| > k*std
    entry_threshold_bps: float = 20.0   # abs min entry (floor)
    min_signal_ticks: int = 1           # N개 tick 연속 위에 있어야 entry (Option B)
    exit_threshold_bps: float = 4.0     # |basis| ≤ 이 값이면 mean-reverted
    target_profit_bps: float = 15.0
    max_hold_hours: float = 8.0
    emergency_close_bps: float = 80.0
    min_hold_seconds: float = 60.0      # e2dba04 가드 동등


@dataclass
class Trade:
    direction: str           # "long_basis" or "short_basis"
    entry_ts: float
    exit_ts: float
    entry_basis_bps: float
    exit_basis_bps: float
    basis_pnl_bps: float
    net_pnl_bps: float       # basis_pnl - round_trip_cost
    hold_seconds: float
    exit_reason: str


@dataclass
class Result:
    params: Params
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_pnl_bps: float = 0.0
    avg_pnl_bps: float = 0.0
    median_pnl_bps: float = 0.0
    max_pnl_bps: float = 0.0
    min_pnl_bps: float = 0.0
    sharpe: float = 0.0
    avg_hold_minutes: float = 0.0
    exit_reasons: dict = field(default_factory=dict)
    trades: list[Trade] = field(default_factory=list)


# round-trip 비용 — production 백테스트와 동일 가정
# perp 0.9bp fee + 3bp spread (편도) × 2 leg × 2 (entry/exit)
# futures 2.8bp fee + ~3bp spread (편도) × 2 leg × 2 (entry/exit)
# 단순화: fees 7.4bp + half-spread 6bp = 약 13.4bp 왕복
ROUND_TRIP_COST_BPS = 13.4


def _percentile(arr: list[float], p: float) -> float:
    if not arr:
        return 0.0
    s = sorted(arr)
    k = (len(s) - 1) * p / 100.0
    f = int(k)
    c = min(f + 1, len(s) - 1)
    return s[f] + (s[c] - s[f]) * (k - f)


class RollingStats:
    """O(1) incremental rolling mean + variance — Welford 풍 sum/sum_sq.

    원래 `_running_stats(deque)`는 매 tick마다 window 전체를 re-sum하여
    O(window_n)이었고 302k row × 21k window = 6.3 billion ops/sim 으로
    실용 불가. 이 클래스는 점 추가/만료만 누적 sum, sum_sq에 반영해
    매 tick O(1) → 전체 백테스트 sub-second.
    """
    __slots__ = ("maxlen", "_buf", "_sum", "_sum_sq")

    def __init__(self, maxlen: int):
        self.maxlen = max(1, int(maxlen))
        self._buf: deque[float] = deque(maxlen=self.maxlen)
        self._sum = 0.0
        self._sum_sq = 0.0

    def add(self, x: float):
        if len(self._buf) >= self.maxlen:
            old = self._buf[0]
            self._sum -= old
            self._sum_sq -= old * old
        self._buf.append(x)
        self._sum += x
        self._sum_sq += x * x

    def __len__(self):
        return len(self._buf)

    def stats(self) -> tuple[float, float]:
        n = len(self._buf)
        if n == 0:
            return 0.0, 0.0
        mean = self._sum / n
        # 부동소수 누적오차로 음수가 될 수 있음 (예: 동일값 반복 시)
        var = self._sum_sq / n - mean * mean
        if var < 0:
            var = 0.0
        return mean, math.sqrt(var)


def simulate(
    basis: list[float], timestamps: list[float], p: Params
) -> Result:
    """one (basis, timestamps) 시계열에 정책 p를 적용해 trade 시퀀스 생성."""
    if len(basis) < 100:
        return Result(params=p)

    # 5초 간격 기준 window 사이즈 추정
    interval_s = (
        (timestamps[-1] - timestamps[0]) / max(1, len(timestamps) - 1)
    ) if len(timestamps) >= 2 else 5.0
    window_n = max(20, int(p.window_hours * 3600 / interval_s))
    stats = RollingStats(maxlen=window_n)

    # signal smoothing state
    consec_above = 0    # 연속 long_basis 신호 카운트
    consec_below = 0    # 연속 short_basis 신호 카운트

    in_pos = False
    pos_dir = ""
    pos_entry_ts = 0.0
    pos_entry_basis = 0.0

    trades: list[Trade] = []

    for b, ts in zip(basis, timestamps):
        stats.add(b)
        if len(stats) < 20:
            continue

        mean, std = stats.stats()
        if std < 1.0:
            consec_above = consec_below = 0
            continue

        upper = mean + p.std_multiplier * std
        lower = mean - p.std_multiplier * std

        # signal smoothing — 현 tick이 entry 조건 만족하면 카운트 증가, 아니면 리셋
        sig_long_basis = (b > upper) and (b > p.entry_threshold_bps)
        sig_short_basis = (b < lower) and (b < -p.entry_threshold_bps)

        if sig_long_basis:
            consec_above += 1
            consec_below = 0
        elif sig_short_basis:
            consec_below += 1
            consec_above = 0
        else:
            consec_above = consec_below = 0

        # ── ENTRY 판단 ──
        if not in_pos:
            if sig_long_basis and consec_above >= p.min_signal_ticks:
                in_pos = True
                pos_dir = "long_basis"
                pos_entry_ts = ts
                pos_entry_basis = b
                consec_above = consec_below = 0
            elif sig_short_basis and consec_below >= p.min_signal_ticks:
                in_pos = True
                pos_dir = "short_basis"
                pos_entry_ts = ts
                pos_entry_basis = b
                consec_above = consec_below = 0
            continue

        # ── EXIT 판단 ──
        hold_s = ts - pos_entry_ts
        hold_h = hold_s / 3600.0

        # min_hold_seconds — entry 직후 즉시 청산 손실 방지
        if hold_s < p.min_hold_seconds:
            continue

        exit_reason = ""
        # 1) emergency: 반대 방향 극단 확대
        if pos_dir == "long_basis" and b < -p.emergency_close_bps:
            exit_reason = "emergency"
        elif pos_dir == "short_basis" and b > p.emergency_close_bps:
            exit_reason = "emergency"
        # 2) max hold
        elif hold_h >= p.max_hold_hours:
            exit_reason = "max_hold"
        # 3) mean reversion: |basis| ≤ exit_threshold
        elif abs(b) <= p.exit_threshold_bps:
            exit_reason = "mean_revert"
        # 4) target profit reached
        else:
            if pos_dir == "long_basis":
                gross = pos_entry_basis - b
            else:
                gross = b - pos_entry_basis
            if gross >= p.target_profit_bps:
                exit_reason = "target_profit"

        if not exit_reason:
            continue

        if pos_dir == "long_basis":
            basis_pnl = pos_entry_basis - b
        else:
            basis_pnl = b - pos_entry_basis
        net_pnl = basis_pnl - ROUND_TRIP_COST_BPS

        trades.append(Trade(
            direction=pos_dir,
            entry_ts=pos_entry_ts,
            exit_ts=ts,
            entry_basis_bps=pos_entry_basis,
            exit_basis_bps=b,
            basis_pnl_bps=basis_pnl,
            net_pnl_bps=net_pnl,
            hold_seconds=hold_s,
            exit_reason=exit_reason,
        ))
        in_pos = False
        pos_dir = ""

    # 결과 집계
    res = Result(params=p, trades=trades, total_trades=len(trades))
    if not trades:
        return res

    pnls = [t.net_pnl_bps for t in trades]
    res.winning_trades = sum(1 for x in pnls if x > 0)
    res.losing_trades = sum(1 for x in pnls if x <= 0)
    res.win_rate = res.winning_trades / res.total_trades
    res.total_pnl_bps = sum(pnls)
    res.avg_pnl_bps = sum(pnls) / len(pnls)
    res.median_pnl_bps = _percentile(pnls, 50)
    res.max_pnl_bps = max(pnls)
    res.min_pnl_bps = min(pnls)
    res.avg_hold_minutes = sum(t.hold_seconds for t in trades) / len(trades) / 60.0
    mean_p = res.avg_pnl_bps
    var_p = sum((x - mean_p) ** 2 for x in pnls) / len(pnls)
    std_p = math.sqrt(var_p)
    res.sharpe = mean_p / std_p if std_p > 0 else 0.0
    res.exit_reasons = dict(Counter(t.exit_reason for t in trades))
    return res


def print_result(label: str, r: Result):
    p = r.params
    print(
        f"{label:>16} | trades={r.total_trades:>4} "
        f"WR={r.win_rate:>5.1%} "
        f"total={r.total_pnl_bps:>+8.1f}bp "
        f"avg={r.avg_pnl_bps:>+6.1f}bp "
        f"med={r.median_pnl_bps:>+6.1f}bp "
        f"sharpe={r.sharpe:>+5.2f} "
        f"hold={r.avg_hold_minutes:>5.1f}min "
        f"| exits={r.exit_reasons}"
    )


def main():
    parser = argparse.ArgumentParser(description="Signal smoothing 백테스트 — Option B 검증")
    parser.add_argument("--db", default="data/arbitrage.db")
    parser.add_argument("--product", default="wti")
    parser.add_argument("--hours", type=float, default=None,
                        help="최근 N시간 (default: 전체 데이터)")
    parser.add_argument("--sample", type=int, default=1,
                        help="downsample 비율 (1=no sample, 6=30s 간격)")
    parser.add_argument("--detail", action="store_true",
                        help="best 정책의 trade-by-trade 출력")
    parser.add_argument("--clip-bp", type=float, default=100.0,
                        help="이 절대값 초과 basis는 제외 — contract mismatch/"
                             "stale data 필터. 0이면 클립 안 함 (default 100bp).")
    parser.add_argument("--since-ts", type=float, default=None,
                        help="이 unix timestamp 이후 데이터만 사용 (rollover-"
                             "contaminated 이전 데이터 제외용).")
    parser.add_argument("--since-date", type=str, default=None,
                        help="이 YYYY-MM-DD 이후 데이터만 사용 (KST 기준).")
    args = parser.parse_args()

    storage = Storage(args.db)
    storage.connect()

    if args.hours:
        rows = storage.get_recent_basis(args.product, hours=args.hours)
    else:
        rows = storage.get_all_basis(args.product)

    if not rows:
        print(f"No basis data for {args.product}")
        storage.close()
        return

    basis_all = [r["basis_bps"] for r in rows]
    ts_all = [r["ts"] for r in rows]
    n_raw = len(basis_all)

    # since-date / since-ts
    since_ts = args.since_ts
    if args.since_date and since_ts is None:
        from datetime import datetime, timezone, timedelta
        kst = timezone(timedelta(hours=9))
        dt = datetime.strptime(args.since_date, "%Y-%m-%d").replace(tzinfo=kst)
        since_ts = dt.timestamp()
    if since_ts:
        filt = [(b, t) for b, t in zip(basis_all, ts_all) if t >= since_ts]
        basis_all = [b for b, _ in filt]
        ts_all = [t for _, t in filt]

    # ── 진단: 분포 + 이상치 비율 ──
    if not basis_all:
        print("No data after since-filter.")
        storage.close()
        return

    p_arr = sorted(basis_all)
    p1 = _percentile(p_arr, 1)
    p5 = _percentile(p_arr, 5)
    p50 = _percentile(p_arr, 50)
    p95 = _percentile(p_arr, 95)
    p99 = _percentile(p_arr, 99)
    mn = min(basis_all)
    mx = max(basis_all)
    avg = sum(basis_all) / len(basis_all)

    n_extreme = sum(1 for b in basis_all if abs(b) > 100)
    n_extreme_pct = n_extreme / len(basis_all) * 100

    print("=== Data quality ===")
    print(f"  rows raw={n_raw}, after since-filter={len(basis_all)}")
    print(f"  basis percentiles (bp):")
    print(f"    min={mn:.1f}  p1={p1:.1f}  p5={p5:.1f}  p50={p50:.1f}  "
          f"p95={p95:.1f}  p99={p99:.1f}  max={mx:.1f}")
    print(f"  mean={avg:.1f}bp")
    print(f"  |basis|>100bp: {n_extreme:,} rows ({n_extreme_pct:.1f}%)  "
          f"← contract mismatch/stale 의심")
    print()

    # ── Clipping: 비현실적 이상치 제거 ──
    if args.clip_bp > 0:
        clipped = [(b, t) for b, t in zip(basis_all, ts_all)
                   if abs(b) <= args.clip_bp]
        n_clipped = len(basis_all) - len(clipped)
        print(f"=== Clip filter (|basis| > {args.clip_bp:.0f}bp 제외) ===")
        print(f"  제외 {n_clipped:,} rows ({n_clipped/len(basis_all)*100:.1f}%) → "
              f"남은 {len(clipped):,} rows")
        basis = [b for b, _ in clipped]
        timestamps = [t for _, t in clipped]
        print()
    else:
        basis = basis_all
        timestamps = ts_all

    if args.sample > 1:
        basis = basis[::args.sample]
        timestamps = timestamps[::args.sample]

    if len(basis) < 100:
        print(f"Not enough data after filters ({len(basis)} rows). 필터 완화 필요.")
        storage.close()
        return

    n = len(basis)
    span_h = (timestamps[-1] - timestamps[0]) / 3600.0
    interval_s = span_h * 3600 / max(1, n - 1)
    print(f"=== Data (filtered) ===")
    print(f"  product={args.product} rows={n} span={span_h:.1f}h "
          f"interval~{interval_s:.1f}s")
    print(f"  basis range=[{min(basis):.1f}, {max(basis):.1f}]bp")
    mean_b = sum(basis) / n
    print(f"  basis mean={mean_b:.1f}bp")
    print()

    # ── Scenario A: signal_ticks 비교 (entry 20bp 고정) ──
    print(f"=== Scenario A: min_signal_ticks 변화 (entry={20.0}bp) ===")
    print(f"  (interval~{interval_s:.0f}s → ticks: 1≈즉시, 3≈{3*interval_s:.0f}s, 5≈{5*interval_s:.0f}s, 10≈{10*interval_s:.0f}s)")
    t0 = time.time()
    for ticks in (1, 2, 3, 5, 7, 10):
        p = Params(min_signal_ticks=ticks, entry_threshold_bps=20.0)
        r = simulate(basis, timestamps, p)
        print_result(f"ticks={ticks}", r)
    print(f"  (Scenario A: {time.time()-t0:.1f}s)")
    print()

    # ── Scenario B: entry_threshold 변화 (ticks=3 고정) ──
    print(f"=== Scenario B: entry_threshold 변화 (ticks=3) ===")
    t0 = time.time()
    for thr in (15.0, 20.0, 25.0, 30.0, 35.0, 40.0):
        p = Params(min_signal_ticks=3, entry_threshold_bps=thr)
        r = simulate(basis, timestamps, p)
        print_result(f"thr={thr:.0f}bp", r)
    print(f"  (Scenario B: {time.time()-t0:.1f}s)")
    print()

    # ── Scenario C: 그리드 (ticks × threshold) — best Sharpe ──
    print(f"=== Scenario C: 그리드 서치 best (Sharpe 기준) ===")
    t0 = time.time()
    grid_results: list[tuple[Params, Result]] = []
    for ticks in (1, 2, 3, 5, 7, 10):
        for thr in (15.0, 20.0, 25.0, 30.0, 35.0):
            p = Params(min_signal_ticks=ticks, entry_threshold_bps=thr)
            r = simulate(basis, timestamps, p)
            if r.total_trades >= 3:
                grid_results.append((p, r))

    grid_results.sort(key=lambda x: x[1].sharpe, reverse=True)
    print(f"  (Scenario C: {time.time()-t0:.1f}s, {len(grid_results)} valid combos)")
    print(f"  {'#':>2} {'ticks':>5} {'thr':>5} {'trades':>6} {'WR':>5} {'total':>8} "
          f"{'avg':>6} {'sharpe':>6} {'hold_m':>6}")
    for i, (p, r) in enumerate(grid_results[:10]):
        print(
            f"  {i+1:>2} {p.min_signal_ticks:>5} {p.entry_threshold_bps:>5.0f} "
            f"{r.total_trades:>6} {r.win_rate:>5.1%} "
            f"{r.total_pnl_bps:>+8.1f} {r.avg_pnl_bps:>+6.1f} "
            f"{r.sharpe:>+6.2f} {r.avg_hold_minutes:>6.1f}"
        )
    print()

    # ── Scenario C2: total_pnl 기준 best 5 ──
    grid_results.sort(key=lambda x: x[1].total_pnl_bps, reverse=True)
    print(f"=== Scenario C2: 그리드 best (total_pnl_bps 기준) ===")
    print(f"  {'#':>2} {'ticks':>5} {'thr':>5} {'trades':>6} {'WR':>5} {'total':>8} "
          f"{'avg':>6} {'sharpe':>6} {'hold_m':>6}")
    for i, (p, r) in enumerate(grid_results[:10]):
        print(
            f"  {i+1:>2} {p.min_signal_ticks:>5} {p.entry_threshold_bps:>5.0f} "
            f"{r.total_trades:>6} {r.win_rate:>5.1%} "
            f"{r.total_pnl_bps:>+8.1f} {r.avg_pnl_bps:>+6.1f} "
            f"{r.sharpe:>+6.2f} {r.avg_hold_minutes:>6.1f}"
        )
    print()

    # ── Baseline 비교: ticks=1 (현재 LIVE 정책) ──
    print(f"=== Baseline 비교 (현재 LIVE = ticks=1, entry=20bp) ===")
    baseline = simulate(basis, timestamps, Params(min_signal_ticks=1, entry_threshold_bps=20.0))
    print_result("baseline", baseline)

    # Best 정책과 비교
    if grid_results:
        best = grid_results[0]
        print_result(f"best", best[1])
        delta = best[1].total_pnl_bps - baseline.total_pnl_bps
        print(f"\n  Δtotal: {delta:+.1f}bp  Δwin_rate: "
              f"{(best[1].win_rate - baseline.win_rate)*100:+.1f}%p  "
              f"Δtrade_count: {best[1].total_trades - baseline.total_trades:+d}")

    if args.detail and grid_results:
        print(f"\n=== Best 정책 trade-by-trade (ticks={grid_results[0][0].min_signal_ticks}, "
              f"thr={grid_results[0][0].entry_threshold_bps:.0f}bp) ===")
        for i, t in enumerate(grid_results[0][1].trades):
            sign = "OK" if t.net_pnl_bps > 0 else "LOSS"
            print(f"  #{i+1:>3} {sign:>4} {t.direction:>12} "
                  f"entry={t.entry_basis_bps:+6.1f}→exit={t.exit_basis_bps:+6.1f}bp "
                  f"net={t.net_pnl_bps:+6.1f}bp hold={t.hold_seconds/60:.1f}min ({t.exit_reason})")

    storage.close()


if __name__ == "__main__":
    main()
