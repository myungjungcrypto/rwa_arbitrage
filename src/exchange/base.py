"""거래소 공통 추상화 레이어.

모든 거래소 어댑터(Hyperliquid, KIS, Binance, Bybit, OKX, Lighter)가 구현하는
공통 protocol과 통합 dataclass 정의.

Phase A 스캐폴딩: 기존 거래소 클래스(HyperliquidClient, KISFuturesClient)는
무수정. 어댑터(HyperliquidExchange, KISExchange)가 이 protocol을 구현하여
collector/engine에서 거래소 종류와 무관하게 동일 인터페이스로 호출 가능.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, Literal, Optional, Protocol, runtime_checkable


class VenueType(str, Enum):
    PERP = "perp"
    DATED_FUTURES = "dated_futures"


OrderSideLiteral = Literal["buy", "sell"]
OrderTypeLiteral = Literal["market", "limit"]


@dataclass
class Quote:
    """거래소 비종속 시세 스냅샷.

    Hyperliquid MarketData, KIS FuturesQuote 등을 통합한 형태.
    venue별로 채워지지 않는 필드는 기본값(0 또는 빈 문자열) 사용.
    """

    exchange: str               # "hyperliquid" | "kis" | "binance" | "bybit" | "okx" | "lighter"
    symbol: str                 # 거래소 native 심볼
    mid_price: float
    bid: float
    ask: float
    bid_qty: float = 0.0
    ask_qty: float = 0.0

    # perp 전용 (CME 월물은 0)
    index_price: float = 0.0
    funding_rate: float = 0.0
    funding_interval_hours: float = 0.0
    predicted_funding_rate: float = 0.0

    # 월물 전용 (perp은 빈 문자열)
    contract_month: str = ""

    # 통계
    open_interest: float = 0.0
    volume_24h: float = 0.0

    timestamp: float = field(default_factory=time.time)

    @property
    def spread_bps(self) -> float:
        if self.mid_price <= 0:
            return 0.0
        return (self.ask - self.bid) / self.mid_price * 10_000

    @property
    def basis_bps(self) -> float:
        """mark - index 베이시스 (perp 전용; index_price=0이면 0)."""
        if self.index_price <= 0:
            return 0.0
        return (self.mid_price - self.index_price) / self.index_price * 10_000


@dataclass
class OrderResult:
    """주문 결과 통합 형태."""

    success: bool
    exchange: str = ""
    symbol: str = ""
    order_id: str = ""
    filled_size: float = 0.0
    filled_price: float = 0.0
    error: str = ""


@dataclass
class Position:
    """포지션 통합 형태."""

    exchange: str
    symbol: str
    size: float                 # 양수 = long, 음수 = short
    entry_price: float
    mark_price: float
    unrealized_pnl: float = 0.0
    margin_used: float = 0.0
    leverage: float = 1.0


QuoteCallback = Callable[[Quote], Optional[Awaitable[None]]]


@runtime_checkable
class ExchangeBase(Protocol):
    """거래소 어댑터 공통 인터페이스.

    Phase A에서는 기존 클라이언트(HyperliquidClient, KISFuturesClient)를
    감싸는 어댑터가 이 protocol을 구현. collector/engine 코드는 이 인터페이스만
    사용하므로 거래소 추가 시 새 어댑터만 작성하면 된다.

    필수 attribute:
      - name: 거래소 식별자 (registry 키)
      - venue_type: "perp" | "dated_futures"
      - margin_asset: "USDC" | "USDT" | 등

    `place_order`/`cancel_order`/`get_positions`/`get_account_value`는
    페이퍼 단계에서 NotImplementedError 또는 시뮬레이션 응답 반환 가능.
    """

    name: str
    venue_type: str
    margin_asset: str

    async def connect(self) -> bool: ...

    async def disconnect(self) -> None: ...

    async def subscribe_quotes(
        self,
        symbol: str,
        callback: QuoteCallback,
        *,
        contract_size: float = 1.0,
    ) -> None: ...

    async def unsubscribe_quotes(self, symbol: str) -> None: ...

    async def get_quote(self, symbol: str) -> Optional[Quote]: ...

    async def place_order(
        self,
        symbol: str,
        side: OrderSideLiteral,
        size: float,
        order_type: OrderTypeLiteral = "market",
        limit_price: Optional[float] = None,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> OrderResult: ...

    async def cancel_order(self, symbol: str, order_id: str) -> bool: ...

    async def get_positions(self) -> list[Position]: ...

    async def get_account_value(self) -> float: ...
