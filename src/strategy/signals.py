from __future__ import annotations
"""진입/청산 시그널 생성 모듈.

베이시스 통계(이동평균, 표준편차)를 기반으로
차익거래 진입/청산 시그널을 생성.
"""


import time
import logging
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from collections import deque
from typing import Optional

import math

from .market_hours import (
    from_timestamp,
    is_cme_open,
    next_closure_duration,
    time_until_close,
)

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
    entry_exec_basis_bps: float = 0.0  # 진입 시 executable basis (bid/ask 기반)
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
        exit_threshold_bps: float = 10,       # deprecated (kept for backward compat)
        target_profit_bps: float = 30,         # deprecated
        max_hold_hours: float = 48,
        funding_rate_weight: float = 1.0,
        min_funding_advantage_bps: float = 5,
        emergency_close_bps: float = 100,
        convergence_target_bps: float = 3.0,   # spread ≤ 이 값이면 수렴 완료
        cme_closed_skip_entry: bool = True,
        pre_close_flatten_minutes: int = 30,
        flatten_threshold_hours: float = 4.0,
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
        self.convergence_target_bps = convergence_target_bps
        self.cme_closed_skip_entry = cme_closed_skip_entry
        self.pre_close_flatten_minutes = pre_close_flatten_minutes
        self.flatten_threshold_hours = flatten_threshold_hours

        # 베이시스 히스토리 (in-memory ring buffer)
        max_points = int(window_hours * 3600 / 5) + 100  # 5초 간격 가정
        self._basis_history: dict[str, deque] = {}
        self._max_points = max_points

        # 포지션 상태
        self._positions: dict[str, PositionState] = {}

    def bootstrap_from_db(self, product: str, basis_history: list[float]):
        """DB에서 로드한 과거 basis 데이터로 히스토리 초기화.

        재시작 시 24시간 window를 처음부터 다시 채우지 않아도 되도록
        DB의 basis_spread 테이블에서 최근 데이터를 로드하여 주입.

        Args:
            product: 상품명 (wti / brent)
            basis_history: basis_bps 리스트 (시간순, 오래된→최신)
        """
        if product not in self._basis_history:
            self._basis_history[product] = deque(maxlen=self._max_points)

        for bps in basis_history:
            self._basis_history[product].append(bps)

        logger.info(
            f"[{product.upper()}] Bootstrapped {len(basis_history)} basis points from DB "
            f"(buffer: {len(self._basis_history[product])}/{self._max_points})"
        )

    def get_position(self, product: str) -> PositionState:
        if product not in self._positions:
            self._positions[product] = PositionState(product=product)
        return self._positions[product]

    def update_basis(
        self, product: str, basis_bps: float, funding_rate: float = 0.0,
        perp_bid: float = 0.0, perp_ask: float = 0.0,
        futures_bid: float = 0.0, futures_ask: float = 0.0,
        current_time: float | None = None,
    ) -> Signal:
        """베이시스 데이터 업데이트 + 시그널 생성.

        Args:
            product: 상품명 (wti / brent)
            basis_bps: 현재 베이시스 (bp) — mid price 기준, 통계용
            funding_rate: 현재 펀딩레이트
            perp_bid/ask: perp 오더북 최우선 호가 (실시간)
            futures_bid/ask: futures 오더북 최우선 호가 (KIS 실시간)
            current_time: 현재 시간 (백테스트에서는 시뮬레이션 시간)

        Returns:
            생성된 Signal
        """
        self._current_time = current_time or time.time()
        self._perp_bid = perp_bid
        self._perp_ask = perp_ask
        self._futures_bid = futures_bid
        self._futures_ask = futures_ask

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

        n = len(history)
        mean = sum(history) / n
        variance = sum((x - mean) ** 2 for x in history) / n
        std = math.sqrt(variance)
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

        # CME 장 시간 가드 (폐장 중 진입 불가 + 마감 임박 시 진입 차단)
        if self.cme_closed_skip_entry:
            now = from_timestamp(self._current_time)
            if not is_cme_open(now):
                return Signal(
                    type=SignalType.NONE, product=product,
                    basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                    funding_rate=funding_rate, reason="CME closed — entry skipped",
                )
            tuc = time_until_close(now)
            if tuc is not None and tuc < timedelta(minutes=self.pre_close_flatten_minutes):
                # 긴 휴장 임박 시만 차단. 일일 1h break는 허용 (flatten_threshold_hours 검사)
                upcoming = next_closure_duration(now)
                if upcoming >= timedelta(hours=self.flatten_threshold_hours):
                    return Signal(
                        type=SignalType.NONE, product=product,
                        basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                        funding_rate=funding_rate,
                        reason=(
                            f"Approaching CME close (in {tuc.total_seconds()/60:.0f}min, "
                            f"next closure {upcoming.total_seconds()/3600:.1f}h)"
                        ),
                    )

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
        """청산 시그널 체크 — 스프레드 수렴 기반.

        perp 가격이 futures 가격에 수렴(spread ≈ 0)하면 청산.
        수렴 전까지는 펀딩을 받으며 보유.
        """
        hold_hours = (self._current_time - pos.entry_time) / 3600

        # 1. 긴급 청산: 베이시스가 반대로 극단적 확대
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

        # 2. Pre-CME-close flatten: 긴 휴장 임박 시 무조건 청산
        now = from_timestamp(self._current_time)
        tuc = time_until_close(now)
        if tuc is not None and tuc < timedelta(minutes=self.pre_close_flatten_minutes):
            upcoming = next_closure_duration(now)
            if upcoming >= timedelta(hours=self.flatten_threshold_hours):
                return Signal(
                    type=SignalType.EXIT, product=product,
                    basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                    funding_rate=funding_rate, confidence=0.9,
                    reason=(
                        f"pre_cme_close flatten (close in {tuc.total_seconds()/60:.0f}min, "
                        f"upcoming closure {upcoming.total_seconds()/3600:.1f}h)"
                    ),
                )

        # 3. 최대 보유 시간 초과
        if hold_hours >= self.max_hold_hours:
            return Signal(
                type=SignalType.EXIT, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=0.8,
                reason=f"Max hold time exceeded ({hold_hours:.1f}h >= {self.max_hold_hours}h)",
            )

        # 3. 스프레드 수렴 완료: perp ≈ futures (executable basis 기준)
        #    bid/ask가 있으면 executable spread 계산, 없으면 mid basis 사용
        if self._perp_bid > 0 and self._futures_bid > 0:
            if pos.direction == "short_basis":
                # perp long → exit at bid, futures short → exit at ask
                current_spread_bps = (self._perp_bid - self._futures_ask) / self._futures_ask * 10_000
            else:  # long_basis
                # perp short → exit at ask, futures long → exit at bid
                current_spread_bps = (self._perp_ask - self._futures_bid) / self._futures_bid * 10_000
        else:
            current_spread_bps = basis_bps

        if abs(current_spread_bps) <= self.convergence_target_bps:
            profit_bps = abs(pos.entry_exec_basis_bps) - abs(current_spread_bps)
            funding_bps = pos.cumulative_funding * 10000
            return Signal(
                type=SignalType.EXIT, product=product,
                basis_bps=basis_bps, basis_mean=mean, basis_std=std,
                funding_rate=funding_rate, confidence=0.95,
                reason=(
                    f"Spread converged to {current_spread_bps:.1f}bp "
                    f"(entry={pos.entry_exec_basis_bps:.1f}bp, "
                    f"profit={profit_bps:.1f}bp, funding={funding_bps:.1f}bp, "
                    f"hold={hold_hours:.1f}h)"
                ),
            )

        # 4. 아직 수렴 안 됨 → HOLD (펀딩 받으며 대기)
        funding_bps = pos.cumulative_funding * 10000
        return Signal(
            type=SignalType.NONE, product=product,
            basis_bps=basis_bps, basis_mean=mean, basis_std=std,
            funding_rate=funding_rate,
            reason=f"Holding (spread={current_spread_bps:.1f}bp, entry={pos.entry_exec_basis_bps:.1f}bp, funding={funding_bps:.1f}bp, hold={hold_hours:.1f}h)",
        )

    def open_position(self, product: str, signal: Signal, size: float = 1.0):
        """포지션 오픈 기록."""
        pos = self.get_position(product)
        pos.is_open = True
        pos.entry_basis_bps = signal.basis_bps
        pos.entry_exec_basis_bps = 0.0  # engine에서 설정
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
            entry_exec_basis_bps=pos.entry_exec_basis_bps,
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
        pos.entry_exec_basis_bps = 0
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
