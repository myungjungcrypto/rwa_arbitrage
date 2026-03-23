from __future__ import annotations
"""베이시스 차익거래 백테스트 엔진.

수집된 베이시스 데이터로 전략 시뮬레이션 및 PnL 계산.
"""


import logging
import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

from src.strategy.signals import SignalGenerator, SignalType, Signal

logger = logging.getLogger("arbitrage.backtest")


@dataclass
class Trade:
    """개별 거래 기록."""
    product: str
    direction: str           # "long_basis" / "short_basis"
    entry_time: float
    exit_time: float
    entry_basis_bps: float
    exit_basis_bps: float
    size: float              # 계약 수
    basis_pnl_bps: float     # 베이시스 수렴/확대 수익
    funding_pnl_bps: float   # 펀딩 수익
    gross_pnl_bps: float     # 총 수익 (bp)
    fees_bps: float           # 수수료 (bp)
    net_pnl_bps: float       # 순수익 (bp)
    hold_hours: float
    exit_reason: str


@dataclass
class BacktestResult:
    """백테스트 결과."""
    product: str
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_pnl_bps: float = 0.0
    avg_pnl_bps: float = 0.0
    max_pnl_bps: float = 0.0
    min_pnl_bps: float = 0.0
    avg_hold_hours: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown_bps: float = 0.0
    total_funding_pnl_bps: float = 0.0
    total_fees_bps: float = 0.0
    trades: list[Trade] = field(default_factory=list)

    # 베이시스 통계
    basis_mean: float = 0.0
    basis_std: float = 0.0
    basis_min: float = 0.0
    basis_max: float = 0.0
    data_points: int = 0

    def summary(self) -> str:
        """결과 요약 문자열."""
        return (
            f"=== Backtest: {self.product} ({self.data_points} data points) ===\n"
            f"Basis stats: mean={self.basis_mean:.1f}bp, std={self.basis_std:.1f}bp, "
            f"range=[{self.basis_min:.1f}, {self.basis_max:.1f}]bp\n"
            f"Trades: {self.total_trades} (win={self.winning_trades}, lose={self.losing_trades}, "
            f"rate={self.win_rate:.0%})\n"
            f"PnL: total={self.total_pnl_bps:.1f}bp, avg={self.avg_pnl_bps:.1f}bp, "
            f"best={self.max_pnl_bps:.1f}bp, worst={self.min_pnl_bps:.1f}bp\n"
            f"Funding: {self.total_funding_pnl_bps:.1f}bp, Fees: -{self.total_fees_bps:.1f}bp\n"
            f"Avg hold: {self.avg_hold_hours:.1f}h, Sharpe: {self.sharpe_ratio:.2f}, "
            f"Max DD: {self.max_drawdown_bps:.1f}bp"
        )


class BacktestEngine:
    """백테스트 엔진.

    베이시스 시계열 데이터를 입력받아 전략 시뮬레이션.

    수수료 구조:
    - Hyperliquid perp: maker 0.01%, taker 0.035%
    - 키움 해외선물: ~$4/계약 (CL 기준 약 5.7bp, BZ 기준 약 4.1bp)
    - 진입/청산 각각 양쪽 수수료 발생
    """

    def __init__(
        self,
        perp_fee_bps: float = 3.5,    # taker 기준
        futures_fee_bps: float = 5.0,  # 키움 해외선물 추정
        funding_interval_hours: float = 1.0,
    ):
        self.perp_fee_bps = perp_fee_bps
        self.futures_fee_bps = futures_fee_bps
        self.round_trip_fee_bps = (perp_fee_bps + futures_fee_bps) * 2  # 진입+청산
        self.funding_interval_hours = funding_interval_hours

    def run(
        self,
        product: str,
        basis_series: list[float],
        funding_series: list[float] | None = None,
        timestamps: list[float] | None = None,
        interval_seconds: float = 5.0,
        signal_params: dict | None = None,
    ) -> BacktestResult:
        """백테스트 실행.

        Args:
            product: 상품명
            basis_series: 베이시스 시계열 (bp)
            funding_series: 펀딩레이트 시계열 (None이면 0으로 가정)
            timestamps: 타임스탬프 (None이면 interval_seconds 기반 생성)
            interval_seconds: 데이터 간격 (초)
            signal_params: SignalGenerator 파라미터 오버라이드

        Returns:
            BacktestResult
        """
        n = len(basis_series)
        if n < 100:
            logger.warning(f"Insufficient data: {n} points")
            return BacktestResult(product=product, data_points=n)

        # 타임스탬프 생성
        if timestamps is None:
            base = time.time() - n * interval_seconds
            timestamps = [base + i * interval_seconds for i in range(n)]

        if funding_series is None:
            funding_series = [0.0] * n

        # 시그널 생성기
        params = signal_params or {}
        gen = SignalGenerator(**params)

        # 시뮬레이션
        trades: list[Trade] = []
        last_funding_time = timestamps[0]

        for i in range(n):
            basis = basis_series[i]
            funding = funding_series[i]
            ts = timestamps[i]

            # 펀딩 누적 (매시간)
            if ts - last_funding_time >= self.funding_interval_hours * 3600:
                gen.add_funding(product, funding)
                last_funding_time = ts

            # 시그널의 timestamp를 현재 시점으로 설정
            signal = gen.update_basis(product, basis, funding)
            signal.timestamp = ts

            if signal.type in (SignalType.ENTRY_LONG_BASIS, SignalType.ENTRY_SHORT_BASIS):
                gen.open_position(product, signal)
                logger.debug(f"[{i}] ENTRY: {signal.reason}")

            elif signal.type in (SignalType.EXIT, SignalType.EMERGENCY_CLOSE):
                pos = gen.close_position(product)

                # PnL 계산
                if pos.direction == "long_basis":
                    basis_pnl = pos.entry_basis_bps - basis
                else:
                    basis_pnl = basis - pos.entry_basis_bps

                funding_pnl = pos.cumulative_funding * 10000
                gross_pnl = basis_pnl + funding_pnl
                net_pnl = gross_pnl - self.round_trip_fee_bps

                trade = Trade(
                    product=product,
                    direction=pos.direction,
                    entry_time=pos.entry_time,
                    exit_time=ts,
                    entry_basis_bps=pos.entry_basis_bps,
                    exit_basis_bps=basis,
                    size=pos.size,
                    basis_pnl_bps=basis_pnl,
                    funding_pnl_bps=funding_pnl,
                    gross_pnl_bps=gross_pnl,
                    fees_bps=self.round_trip_fee_bps,
                    net_pnl_bps=net_pnl,
                    hold_hours=(ts - pos.entry_time) / 3600,
                    exit_reason=signal.reason,
                )
                trades.append(trade)
                logger.debug(f"[{i}] EXIT: net={net_pnl:.1f}bp, {signal.reason}")

        # 결과 집계
        return self._aggregate(product, basis_series, trades)

    def _aggregate(
        self, product: str, basis_series: list[float], trades: list[Trade]
    ) -> BacktestResult:
        """거래 결과 집계."""
        basis_arr = np.array(basis_series)

        result = BacktestResult(
            product=product,
            data_points=len(basis_series),
            basis_mean=float(np.mean(basis_arr)),
            basis_std=float(np.std(basis_arr)),
            basis_min=float(np.min(basis_arr)),
            basis_max=float(np.max(basis_arr)),
            trades=trades,
        )

        if not trades:
            return result

        pnls = [t.net_pnl_bps for t in trades]
        result.total_trades = len(trades)
        result.winning_trades = sum(1 for p in pnls if p > 0)
        result.losing_trades = sum(1 for p in pnls if p <= 0)
        result.win_rate = result.winning_trades / result.total_trades
        result.total_pnl_bps = sum(pnls)
        result.avg_pnl_bps = np.mean(pnls)
        result.max_pnl_bps = max(pnls)
        result.min_pnl_bps = min(pnls)
        result.avg_hold_hours = np.mean([t.hold_hours for t in trades])
        result.total_funding_pnl_bps = sum(t.funding_pnl_bps for t in trades)
        result.total_fees_bps = sum(t.fees_bps for t in trades)

        # Sharpe ratio (bp 기준)
        if len(pnls) > 1:
            result.sharpe_ratio = float(np.mean(pnls) / np.std(pnls)) if np.std(pnls) > 0 else 0

        # Max drawdown
        cumulative = np.cumsum(pnls)
        peak = np.maximum.accumulate(cumulative)
        drawdown = peak - cumulative
        result.max_drawdown_bps = float(np.max(drawdown)) if len(drawdown) > 0 else 0

        return result
