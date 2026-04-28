"""펀딩 정산 주기 런타임 검증.

거래소가 펀딩 정책을 변경하는 사례가 종종 있다 (Binance 1h→8h 캠페인,
OKX 8h→4h 변경 등). config의 `leg.funding_interval_hours`와 거래소 실제
응답 주기(`FundingInfo.observed_interval_hours`)를 매시간 비교해 불일치
시 WARNING 로그 + (옵션) 콜백.

monitor는 `ExchangeRegistry` + `list[ArbitragePair]` 를 받아, perp leg만
대상으로 검증. dated_futures leg(KIS)는 `get_funding_info`가 None을 반환하므로
자동 skip.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

from src.exchange.base import FundingInfo
from src.exchange.registry import ExchangeRegistry
from src.strategy.pair import ArbitragePair, ExchangeLeg, LegRole

logger = logging.getLogger("arbitrage.funding_monitor")


@dataclass
class FundingMismatch:
    """검출된 불일치 1건."""

    pair_id: str
    leg: str                      # "a" | "b"
    exchange: str
    symbol: str
    expected_hours: float         # config 선언값
    observed_hours: float         # 거래소 보고값
    info: FundingInfo


MismatchCallback = Callable[[FundingMismatch], Optional[Awaitable[None]]]


class FundingIntervalMonitor:
    """모든 perp leg의 펀딩 주기를 주기적으로 검증.

    Usage:
        monitor = FundingIntervalMonitor(registry, pairs)
        # 1회 즉시 검증
        mismatches = await monitor.verify_once()
        # 또는 백그라운드 루프
        task = asyncio.create_task(monitor.run_loop(interval_seconds=3600))
    """

    def __init__(
        self,
        registry: ExchangeRegistry,
        pairs: list[ArbitragePair],
        *,
        tolerance_hours: float = 0.1,
        on_mismatch: Optional[MismatchCallback] = None,
    ):
        self._registry = registry
        self._pairs = pairs
        self._tolerance = tolerance_hours
        self._on_mismatch = on_mismatch

    async def verify_once(self) -> list[FundingMismatch]:
        """모든 perp leg 검증 1회. 불일치 리스트 반환."""
        mismatches: list[FundingMismatch] = []
        for pair in self._pairs:
            for leg_name in ("a", "b"):
                leg = pair.leg(leg_name)
                m = await self._verify_leg(pair.id, leg_name, leg)
                if m is not None:
                    mismatches.append(m)
        return mismatches

    async def _verify_leg(
        self,
        pair_id: str,
        leg_name: str,
        leg: ExchangeLeg,
    ) -> Optional[FundingMismatch]:
        # 월물은 skip
        if leg.role != LegRole.PERP:
            return None
        # 미등록 거래소 skip (config-only 페어, 어댑터 미합류 단계)
        if not self._registry.has(leg.exchange):
            return None
        if leg.funding_interval_hours <= 0:
            return None

        try:
            adapter = self._registry.get(leg.exchange)
            info = await adapter.get_funding_info(leg.symbol)
        except Exception as e:
            logger.error(
                f"[{pair_id}/{leg_name}] funding info fetch error "
                f"({leg.exchange}/{leg.symbol}): {e}"
            )
            return None

        if info is None:
            return None

        if info.matches_expected(leg.funding_interval_hours, self._tolerance):
            logger.debug(
                f"[{pair_id}/{leg_name}] funding interval OK: "
                f"{info.observed_interval_hours:.2f}h "
                f"(expected {leg.funding_interval_hours:.2f}h)"
            )
            return None

        # 불일치 — WARNING 로그 + 콜백
        mismatch = FundingMismatch(
            pair_id=pair_id,
            leg=leg_name,
            exchange=leg.exchange,
            symbol=leg.symbol,
            expected_hours=leg.funding_interval_hours,
            observed_hours=info.observed_interval_hours,
            info=info,
        )
        logger.warning(
            f"[FUNDING_MISMATCH] {pair_id}/{leg_name} "
            f"{leg.exchange}/{leg.symbol}: "
            f"expected={leg.funding_interval_hours:.2f}h "
            f"observed={info.observed_interval_hours:.2f}h "
            f"(diff={info.observed_interval_hours - leg.funding_interval_hours:+.2f}h)"
        )
        if self._on_mismatch is not None:
            try:
                result = self._on_mismatch(mismatch)
                if result is not None and asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"on_mismatch callback error: {e}")
        return mismatch

    async def run_loop(self, interval_seconds: int = 3600, *, stop_event: Optional[asyncio.Event] = None) -> None:
        """백그라운드 주기 검증 루프.

        Args:
            interval_seconds: 검증 주기 (default 1h).
            stop_event: 외부 신호로 루프 종료.
        """
        logger.info(
            f"FundingIntervalMonitor starting "
            f"(interval={interval_seconds}s, pairs={len(self._pairs)})"
        )
        # 부팅 직후 즉시 1회
        try:
            await self.verify_once()
        except Exception as e:
            logger.error(f"funding monitor verify_once error: {e}")

        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("FundingIntervalMonitor stopped by event")
                return
            try:
                await asyncio.wait_for(
                    stop_event.wait() if stop_event else asyncio.sleep(interval_seconds),
                    timeout=interval_seconds,
                )
                if stop_event is not None and stop_event.is_set():
                    return
            except asyncio.TimeoutError:
                pass
            try:
                await self.verify_once()
            except Exception as e:
                logger.error(f"funding monitor verify_once error: {e}")
