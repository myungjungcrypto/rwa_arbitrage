"""ArbitragePair 추상화.

거래소 종류에 무관한 차익거래 페어 정의. 기존 `wti_cme_hl` 한 페어에서
`wti_hl_lighter`, `wti_hl_binance`, `wti_hl_bybit`, `wti_hl_okx`로 확장하기 위한
첫-클래스 dataclass.

페어는 두 leg(leg_a, leg_b)로 구성. Web2-Web3 페어는 leg_a=perp(HL),
leg_b=dated_futures(KIS). Web3-Web3 페어는 양쪽 다 perp이며 leg_a를 HL hub로 통일.

Phase A 스캐폴딩: 데이터 구조만 정의. collector/engine이 실제로 사용하기
시작하는 것은 Phase C.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class LegRole(str, Enum):
    PERP = "perp"
    DATED_FUTURES = "dated_futures"


class PairGate(str, Enum):
    """페어 진입 허용 시간대 게이트.

    CME_HOURS — `src/strategy/market_hours.is_cme_open()` 재사용.
                Strict 모드: 월-목 16:00-17:00 CT 일일 1h 휴장 + 주말 + 휴일 모두 OFF.
                사용자 결정사항: Web3-Web3 페어도 펀딩 비대칭 위험 때문에 동일 게이트.
    ALWAYS    — 24/7. 향후 crypto-only 페어용 옵션.
    """

    CME_HOURS = "cme_hours"
    ALWAYS = "always"


@dataclass
class ExchangeLeg:
    """페어의 한 쪽 다리 정의.

    contract_size: KIS 계약총액→배럴당 가격 변환에 사용 (MCL=100, BZ=1000).
                   perp은 1.0.
    fee_per_contract_usd: KIS 식 고정수수료 (perp은 0).
    taker_fee_bps: perp 식 비율수수료 (월물은 0).
    funding_interval_hours: 1=HL/Lighter, 4=Binance/Bybit, 8=OKX, 0=월물.
    """

    exchange: str
    symbol: str
    role: LegRole
    contract_size: float = 1.0
    fee_per_contract_usd: float = 0.0
    taker_fee_bps: float = 0.0
    funding_interval_hours: float = 0.0
    margin_asset: str = ""


@dataclass
class PairStrategyParams:
    """페어별 전략 파라미터.

    글로벌 settings.yaml `strategy:` 블록을 페어 단위로 override할 때 사용.
    Phase A에서는 데이터 구조만, 실제 사용은 Phase C.
    """

    basis_window_hours: float = 24.0
    basis_std_multiplier: float = 3.0
    entry_threshold_bps: float = 20.0
    convergence_target_bps: float = 3.0
    max_hold_hours: float = 48.0
    min_funding_advantage_bps: float = 2.0
    funding_rate_weight: float = 1.0
    emergency_close_bps: float = 100.0
    pre_close_flatten_minutes: int = 30
    flatten_threshold_hours: float = 4.0


@dataclass
class ArbitragePair:
    """차익거래 페어 정의.

    Examples:
        # 기존 Web2-Web3 페어
        ArbitragePair(
            id="wti_cme_hl", enabled=True, gate=PairGate.CME_HOURS,
            leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                              taker_fee_bps=0.9, funding_interval_hours=1.0,
                              margin_asset="USDC"),
            leg_b=ExchangeLeg(exchange="kis", symbol="MCLM26", role=LegRole.DATED_FUTURES,
                              contract_size=100, fee_per_contract_usd=2.50),
            params=PairStrategyParams(entry_threshold_bps=20),
        )

        # 신규 Web3-Web3 페어
        ArbitragePair(
            id="wti_hl_binance", enabled=False, gate=PairGate.CME_HOURS,
            leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                              taker_fee_bps=0.9, funding_interval_hours=1.0,
                              margin_asset="USDC"),
            leg_b=ExchangeLeg(exchange="binance", symbol="CLUSDT", role=LegRole.PERP,
                              taker_fee_bps=4.0, funding_interval_hours=4.0,
                              margin_asset="USDT"),
            params=PairStrategyParams(entry_threshold_bps=15, max_hold_hours=12),
        )
    """

    id: str
    leg_a: ExchangeLeg
    leg_b: ExchangeLeg
    params: PairStrategyParams = field(default_factory=PairStrategyParams)
    enabled: bool = False
    strategy: str = "basis_convergence"
    gate: PairGate = PairGate.CME_HOURS

    @property
    def fee_round_trip_bps(self) -> float:
        """양 leg 진입+청산 합산 수수료 (bps).

        perp의 taker_fee_bps만 합산 (월물 고정수수료는 별도 처리 필요).
        진입 임계값 floor 비교에 사용.
        """
        return 2.0 * (self.leg_a.taker_fee_bps + self.leg_b.taker_fee_bps)

    def leg(self, name: str) -> ExchangeLeg:
        """leg 이름("a" 또는 "b")으로 leg 반환."""
        if name == "a":
            return self.leg_a
        if name == "b":
            return self.leg_b
        raise ValueError(f"Unknown leg name: {name!r} (expected 'a' or 'b')")

    def opposite_leg(self, name: str) -> ExchangeLeg:
        if name == "a":
            return self.leg_b
        if name == "b":
            return self.leg_a
        raise ValueError(f"Unknown leg name: {name!r}")
