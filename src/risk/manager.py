"""리스크 관리 모듈.

포지션 사이즈, 마진 사용률, 일일 손실, 롤오버 리스크 등을 관리.
"""

from __future__ import annotations

import time
import logging
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional

from src.utils.config import RiskConfig

logger = logging.getLogger("arbitrage.risk")


@dataclass
class RiskCheck:
    """리스크 체크 결과."""
    allowed: bool
    reason: str = ""
    max_size: float = 0.0    # 허용 가능한 최대 사이즈


class RiskManager:
    """리스크 관리자.

    모든 주문 전에 리스크 체크를 수행하여
    사이즈 제한, 마진, 일일 손실 등을 검증.
    """

    def __init__(self, config: RiskConfig):
        self.config = config
        self._daily_pnl: dict[str, float] = {}  # date_str -> cumulative pnl
        self._current_positions: dict[str, float] = {}  # product -> size in USD

    def check_entry(
        self,
        product: str,
        size_usd: float,
        perp_margin_usage_pct: float,
        futures_margin_usage_pct: float,
        current_basis_bps: float,
        is_rollover_period: bool = False,
    ) -> RiskCheck:
        """진입 전 리스크 체크.

        Args:
            product: 상품명
            size_usd: 진입 사이즈 (USD)
            perp_margin_usage_pct: Hyperliquid 마진 사용률
            futures_margin_usage_pct: 키움 마진 사용률
            current_basis_bps: 현재 베이시스
            is_rollover_period: 롤오버 기간 여부

        Returns:
            RiskCheck
        """
        # 최대 포지션 사이즈
        if size_usd > self.config.max_position_usd:
            return RiskCheck(
                allowed=False,
                reason=f"Size ${size_usd:,.0f} exceeds max ${self.config.max_position_usd:,.0f}",
                max_size=self.config.max_position_usd,
            )

        # 마진 사용률
        max_margin = self.config.max_margin_usage_pct
        if perp_margin_usage_pct > max_margin:
            return RiskCheck(
                allowed=False,
                reason=f"Perp margin usage {perp_margin_usage_pct:.0f}% > {max_margin:.0f}%",
            )
        if futures_margin_usage_pct > max_margin:
            return RiskCheck(
                allowed=False,
                reason=f"Futures margin usage {futures_margin_usage_pct:.0f}% > {max_margin:.0f}%",
            )

        # 일일 손실 제한
        today = date.today().isoformat()
        daily_loss = self._daily_pnl.get(today, 0)
        if daily_loss < -self.config.max_daily_loss_usd:
            return RiskCheck(
                allowed=False,
                reason=f"Daily loss ${abs(daily_loss):,.0f} exceeds limit ${self.config.max_daily_loss_usd:,.0f}",
            )

        # 롤오버 기간 포지션 축소
        max_size = self.config.max_position_usd
        if is_rollover_period:
            reduce_pct = self.config.rollover_position_reduce_pct
            max_size *= (1 - reduce_pct / 100)
            if size_usd > max_size:
                return RiskCheck(
                    allowed=False,
                    reason=f"Rollover period: size ${size_usd:,.0f} > reduced limit ${max_size:,.0f}",
                    max_size=max_size,
                )

        return RiskCheck(allowed=True, max_size=max_size)

    def is_rollover_period(self, current_date: date | None = None) -> bool:
        """현재 롤오버 기간인지 체크.

        매월 5~10 영업일.
        """
        dt = current_date or date.today()

        # 해당 월의 영업일 계산 (간이 버전: 주말 제외)
        business_day = 0
        for day in range(1, dt.day + 1):
            d = date(dt.year, dt.month, day)
            if d.weekday() < 5:  # 월~금
                business_day += 1

        return self.config.rollover_start_day <= business_day <= self.config.rollover_end_day

    def record_pnl(self, pnl_usd: float, dt: date | None = None):
        """일일 PnL 기록."""
        key = (dt or date.today()).isoformat()
        self._daily_pnl[key] = self._daily_pnl.get(key, 0) + pnl_usd

    def get_daily_pnl(self, dt: date | None = None) -> float:
        """오늘 누적 PnL."""
        key = (dt or date.today()).isoformat()
        return self._daily_pnl.get(key, 0)
