from __future__ import annotations
"""진입/청산 시그널 생성 모듈.

베이시스 통계(이동평균, 표준편차)를 기반으로
차익거래 진입/청산 시그널을 생성.
"""


import time
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
from typing import Optional

import numpy as np

logger = logging.getLogger("arbitrage.signals")


class SignalType(str, Enum):
    NONE = "none"
    ENTRY_LONG_BASIS = "entry_long_basis"    # perp > futures → perp SHORT + futures LONG
    ENTRY_SHORT_BASIS = "entry_short_basis"  # perp < futures → perp LONG + futures SHORT
    EXIT = "exit"
    EMERGENCY_CLOSE = "emergency_close"


@dataclass
class Signal:
    """트레이딩 시그널."""
    type: SignalType
    product: str
    basis_bps: float
    basis_mean: float
    basis_std: float
    funding_rate: float = 0.0
    confidence: float = 0.0       # 0~1 확신도
    reason: str = ""
    timestamp: float = field(default_factory=time.time)


@dataclass
class PositionState:
    """현재 포지션 상태 추적."""
    product: str
    is_open: bool = False
    direction: str = ""           # "long_basis" or "short_basis"
    entry_basis_bps: float = 0.0
    entry_time: float = 0.0
    perp_side: str = ""           # "long" or "short"
    futures_side: str = ""        # "long" or "short"
    size: float = 0.0
    cumulative_funding: float = 0.0


class SignalGenerator:
    """베이시스 차익거래 시그널 생성기.

    전략 로직:
    1. 베이시스(perp - futures)의 이동평균과 표준편차를 실시간 계산
    2. 베이시스가 평균 + K*σ를 초과하면 → ENTRY_LONG_BASIS (perp short, futures long)
    3. 베이시스가 평균 - K*σ를 하회하면 → ENTRY_SHORT_BASIS (perp long, futures short)
    4. 베이시스가 평균으로 회귀하거나 목표 수익 도달 시 → EXIT
    5. 펀딩레이트가 유리한 방향이면 가산점
    """

    def __init__(
        self,
        window_hours: float = 24,
        std_multiplier: float = 2.0,
        entry_threshold_bps: float = 50,
        exit_threshold_bps: float = 10,
        target_profit_bps: float = 30,
        max_hold_hours: float = 48,
        funding_rate_weight: float = 1.0,
        min_funding_advantage_bps: float = 5,
        emergency_close_bps: float = 100,
    ):
        self.window_hours = window_hours
        self.std_multiplier = std_multiplier
        self.entry_threshold_bps = entry_threshold_bps
        self.exit_threshold_bps = exit_threshold_bps
        self.target_profit_bps = target_profit_bps
        self.max_hold_hours = max_hold_hours
        self.funding_rate_weight = funding_rate_weight
        self.min_funding_advantage_bps = min_funding_advantage_bps
        self.emergency_close_bps = emergency_close_bps

        # 베이시스 히스토리 (in-memory ring buffer)
        max_points = int(window_hours * 3600 / 5) + 100  # 5초 간격 가정
        self._basis_history: dict[str, deque] = {}
        self._max_points = max_points

        # 포지션 상태
        self._positions: dict[str, PositionState] = {}

    def get_position(self, product: str) -> PositionState:
        if product not in self._positions:
            self._positions[product] = PositionState(product=product)
        return self._positions[product]

    def update_basis(self, product: str, basis_bps: float, funding_rate: float = 0.0) -> Signal:
        """베이시스 데이터 업데이트 + 시그널 생성.

        Args:
            product: 상품명 (wti / brent)
            basis_bps: 현재 베이시스 (bp)
            funding_rate: 현재 펀딩레이트

        Returns:
            생성된 Signal
        """
        # 히스토리 추가
        if product not in self._basis_history:
            self._basis_history[product] = deque(maxlen=self._max_points)
        self._basis_history[product].append(basis_bps)

        # 통계 계산
        history = list(self._basis_history[product])
        if len(history) < 20:
            # 데이터 부족
            return Signal(
                type=SignalType.NONE,
                product=product,
                basis_bps=basis_bps,
                basis_mean=0,
                basis_std=0,
                funding_rate=funding_rate,
                reason="Insufficient data",
            )

        mean = np.mean(history)
        std = np.std(history)
        pos = self.get_position(product)

        if pos.is_open:
            return self._check_exit(product, basis_bps, mean, std, funding_rate, pos)
        else:
            return self._check_entry(product, basis_bps, mean, std, funding_rate)

    def _check_entry(
        self,
        product: str,
        basis_bps: float,
        mean: float,
        std: float,
        funding_rate: float,
    ) -> Signal:
        """진입 시그널 체크."""
        upper = mean + self.std_multiplier * std
        lower = mean - self.std_multiplier * std

        # 최소 임계값 체크
        if std < 1.0:
            return Signal(
                type=SignalType.NONE, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, reason="Volatility too low",
            )

        # Long basis: perp 비쌈 → perp SHORT + futures LONG
        if basis_bps > upper and basis_bps > self.entry_threshold_bps:
            z_score = (basis_bps - mean) / std if std > 0 else 0
            confidence = min(1.0, z_score / 4.0)

            # 펀딩레이트 보너스: perp short일 때 funding > 0이면 유리 (funding 수취)
            funding_bonus = 0
            if funding_rate > 0:
                funding_bonus = abs(funding_rate) * 10000 * self.funding_rate_weight

            return Signal(
                type=SignalType.ENTRY_LONG_BASIS,
                product=product,
                basis_bps=basis_bps,
                basis_mean=mean,
                basis_std=std,
                funding_rate=funding_rate,
                confidence=confidence,
                reason=f"Basis {basis_bps:.1f}bp > upper {upper:.1f}bp (z={z_score:.1f}, funding_bonus={funding_bonus:.1f}bp)",
            )

        # Short basis: perp 쌈 → perp LONG + futures SHORT
        if basis_bps < lower and basis_bps < -self.entry_threshold_bps:
            z_score = (mean - basis_bps) / std if std > 0 else 0
            confidence = min(1.0, z_score / 4.0)

            # 펀딩 보너스: perp long일 때 funding < 0이면 유리 (funding 수취)
            funding_bonus = 0
            if funding_rate < 0:
                funding_bonus = abs(funding_rate) * 10000 * self.funding_rate_weight

            return Signal(
                type=SignalType.ENTRY_SHORT_BASIS,
                product=product,
                basis_bps=basis_bps,
                basis_mean=mean,
                basis_std=std,
                funding_rate=funding_rate,
                confidence=confidence,
                reason=f"Basis {basis_bps:.1f}bp < lower {lower:.1f}bp (z={z_score:.1f}, funding_bonus={funding_bonus:.1f}bp)",
            )

        return Signal(
            type=SignalType.NONE, product=product,
            basis_bps=basis_bps, basis_mean=mean, basis_std=std,
            funding_rate=funding_rate,
            reason=f"In range [{lower:.1f}, {upper:.1f}]bp",
        )

    def _check_exit(
        self,
        product: str,
        basis_bps: float,
        mean: float,
        std: float,
        funding_rate: float,
        pos: PositionState,
    ) -> Signal:
        """청산 시그널 체크."""
        hold_hours = (time.time() - pos.entry_time) / 3600

        # 긴급 청산: 베이시스가 반대로 너무 크게 벌어짐
        if pos.direction == "long_basis" and basis_bps < -(self.emergency_close_bps):
            return Signal(
                type=SignalType.EMERGENCY_CLOSE, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=1.0,
                reason=f"Emergency: basis {basis_bps:.1f}bp reversed beyond -{self.emergency_close_bps}bp",
            )
        if pos.direction == "short_basis" and basis_bps > self.emergency_close_bps:
            return Signal(
                type=SignalType.EMERGENCY_CLOSE, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=1.0,
                reason=f"Emergency: basis {basis_bps:.1f}bp reversed beyond +{self.emergency_close_bps}bp",
            )

        # 최대 보유 시간 초과
        if hold_hours >= self.max_hold_hours:
            return Signal(
                type=SignalType.EXIT, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=0.8,
                reason=f"Max hold time exceeded ({hold_hours:.1f}h >= {self.max_hold_hours}h)",
            )

        # 목표 수익 도달
        if pos.direction == "long_basis":
            pnl_bps = pos.entry_basis_bps - basis_bps  # basis 축소가 수익
        else:
            pnl_bps = basis_bps - pos.entry_basis_bps  # basis 확대가 수익

        # 펀딩 수익 추가
        pnl_bps += pos.cumulative_funding * 10000

        if pnl_bps >= self.target_profit_bps:
            return Signal(
                type=SignalType.EXIT, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=0.9,
                reason=f"Target profit reached: {pnl_bps:.1f}bp >= {self.target_profit_bps}bp",
            )

        # 평균 회귀
        if pos.direction == "long_basis" and basis_bps <= mean + self.exit_threshold_bps:
            return Signal(
                type=SignalType.EXIT, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=0.7,
                reason=f"Mean reversion: basis {basis_bps:.1f}bp <= mean+exit {mean + self.exit_threshold_bps:.1f}bp",
            )
        if pos.direction == "short_basis" and basis_bps >= mean - self.exit_threshold_bps:
            return Signal(
                type=SignalType.EXIT, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=0.7,
                reason=f"Mean reversion: basis {basis_bps:.1f}bp >= mean-exit {mean - self.exit_threshold_bps:.1f}bp",
            )

        return Signal(
            type=SignalType.NONE, product=product,
            basis_bps=basis_bps, basis_mean=mean, basis_std=std,
            funding_rate=funding_rate,
            reason=f"Holding (pnl={pnl_bps:.1f}bp, hold={hold_hours:.1f}h)",
        )

    def open_position(self, product: str, signal: Signal, size: float = 1.0):
        """포지션 오픈 기록."""
        pos = self.get_position(product)
        pos.is_open = True
        pos.entry_basis_bps = signal.basis_bps
        pos.entry_time = signal.timestamp
        pos.size = size
        pos.cumulative_funding = 0.0

        if signal.type == SignalType.ENTRY_LONG_BASIS:
            pos.direction = "long_basis"
            pos.perp_side = "short"
            pos.futures_side = "long"
        elif signal.type == SignalType.ENTRY_SHORT_BASIS:
            pos.direction = "short_basis"
            pos.perp_side = "long"
            pos.futures_side = "short"

    def close_position(self, product: str) -> PositionState:
        """포지션 클로즈 기록. 이전 상태 반환."""
        pos = self.get_position(product)
        closed = PositionState(
            product=pos.product,
            is_open=False,
            direction=pos.direction,
            entry_basis_bps=pos.entry_basis_bps,
            entry_time=pos.entry_time,
            perp_side=pos.perp_side,
            futures_side=pos.futures_side,
            size=pos.size,
            cumulative_funding=pos.cumulative_funding,
        )
        # 리셋
        pos.is_open = False
        pos.direction = ""
        pos.entry_basis_bps = 0
        pos.entry_time = 0
        pos.size = 0
        pos.cumulative_funding = 0
        return closed

    def add_funding(self, product: str, funding_rate: float):
        """펀딩레이트 누적 (포지션 보유 중일 때)."""
        pos = self.get_position(product)
        if pos.is_open:
            # perp short일 때 funding > 0이면 수취
            if pos.perp_side == "short":
                pos.cumulative_funding += funding_rate
            else:
                pos.cumulative_funding -= funding_rate
