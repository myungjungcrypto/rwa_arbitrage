from __future__ import annotations
"""키움증권 OpenAPI-W 래퍼 (Stub).

실제 구현은 Windows + OCX 환경에서 koapy 또는 pykiwoom을 사용.
이 파일은 인터페이스 정의 + 모의투자 시뮬레이션용 stub.

NOTE: 키움 OpenAPI-W는 Windows OCX 기반이므로,
      Linux/macOS에서는 이 stub을 사용하고,
      실제 Windows 환경에서 koapy/pykiwoom으로 교체.
"""


import time
import logging
import random
from dataclasses import dataclass
from typing import Callable, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger("arbitrage.kiwoom")


# ──────────────────────────────────────────────
# Data Models
# ──────────────────────────────────────────────

@dataclass
class FuturesQuote:
    """해외선물 호가/시세."""
    symbol: str              # 종목코드 (예: CLK6)
    name: str                # 종목명
    price: float             # 현재가
    bid: float               # 매수호가1
    ask: float               # 매도호가1
    volume: int              # 거래량
    open_interest: int       # 미결제약정
    change: float            # 전일 대비
    change_pct: float        # 등락률 (%)
    timestamp: float = 0.0


@dataclass
class FuturesPosition:
    """해외선물 보유 포지션."""
    symbol: str
    side: str                # "long" / "short"
    quantity: int
    avg_price: float
    current_price: float
    unrealized_pnl: float
    margin_used: float


@dataclass
class FuturesOrder:
    """해외선물 주문 결과."""
    success: bool
    order_no: str = ""
    filled_qty: int = 0
    filled_price: float = 0.0
    error: str = ""


# ──────────────────────────────────────────────
# Abstract Interface
# ──────────────────────────────────────────────

class KiwoomBase(ABC):
    """키움 해외선물 API 인터페이스."""

    @abstractmethod
    def connect(self) -> bool:
        """API 연결."""
        ...

    @abstractmethod
    def disconnect(self):
        """연결 해제."""
        ...

    @abstractmethod
    def get_quote(self, symbol: str) -> FuturesQuote | None:
        """시세 조회."""
        ...

    @abstractmethod
    def get_positions(self) -> list[FuturesPosition]:
        """보유 포지션 조회."""
        ...

    @abstractmethod
    def get_margin_info(self) -> dict:
        """증거금 정보 조회."""
        ...

    @abstractmethod
    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float | None = None,
    ) -> FuturesOrder:
        """주문."""
        ...

    @abstractmethod
    def cancel_order(self, order_no: str) -> bool:
        """주문 취소."""
        ...

    @abstractmethod
    def subscribe_quote(self, symbol: str, callback: Callable[[FuturesQuote], None]):
        """실시간 시세 구독."""
        ...


# ──────────────────────────────────────────────
# Mock Implementation (페이퍼 트레이딩용)
# ──────────────────────────────────────────────

class KiwoomMock(KiwoomBase):
    """키움 모의 API.

    실제 키움 API 연결 없이 시뮬레이션.
    Hyperliquid의 index price를 기반으로 mock 시세 생성.
    """

    def __init__(self):
        self._connected = False
        self._positions: list[FuturesPosition] = []
        self._orders: dict[str, dict] = {}
        self._order_counter = 0
        self._quote_callbacks: dict[str, list[Callable]] = {}
        self._base_prices: dict[str, float] = {}  # 외부에서 주입
        self._margin = 100_000.0  # 모의 증거금 (USD)

    def connect(self) -> bool:
        self._connected = True
        logger.info("Kiwoom Mock connected")
        return True

    def disconnect(self):
        self._connected = False
        logger.info("Kiwoom Mock disconnected")

    def set_base_price(self, symbol: str, price: float):
        """외부에서 기준가 설정 (Hyperliquid index price 연동)."""
        self._base_prices[symbol] = price

    def get_quote(self, symbol: str) -> FuturesQuote | None:
        """Mock 시세 생성.

        base price에 약간의 스프레드/노이즈를 추가.
        """
        base = self._base_prices.get(symbol)
        if base is None:
            logger.warning(f"No base price for {symbol}")
            return None

        # 약간의 노이즈 추가 (±0.05%)
        noise = random.gauss(0, base * 0.0005)
        price = base + noise
        spread = base * 0.0003  # 3bp 스프레드

        return FuturesQuote(
            symbol=symbol,
            name=f"{symbol} Futures",
            price=round(price, 2),
            bid=round(price - spread / 2, 2),
            ask=round(price + spread / 2, 2),
            volume=random.randint(1000, 50000),
            open_interest=random.randint(10000, 500000),
            change=round(noise, 2),
            change_pct=round(noise / base * 100, 3),
            timestamp=time.time(),
        )

    def get_positions(self) -> list[FuturesPosition]:
        return self._positions.copy()

    def get_margin_info(self) -> dict:
        used = sum(p.margin_used for p in self._positions)
        return {
            "total_margin": self._margin,
            "used_margin": used,
            "available_margin": self._margin - used,
            "usage_pct": used / self._margin * 100 if self._margin else 0,
        }

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: int,
        price: float | None = None,
    ) -> FuturesOrder:
        """Mock 주문 체결.

        즉시 체결 시뮬레이션 (시장가 또는 현재 호가에 체결).
        """
        quote = self.get_quote(symbol)
        if not quote:
            return FuturesOrder(success=False, error=f"No quote for {symbol}")

        fill_price = price or (quote.ask if side == "buy" else quote.bid)

        self._order_counter += 1
        order_no = f"MOCK-{self._order_counter:06d}"

        # 포지션 업데이트
        self._update_position(symbol, side, quantity, fill_price)

        logger.info(
            f"Mock order filled: {order_no} {side} {quantity}x {symbol} @ {fill_price:.2f}"
        )

        return FuturesOrder(
            success=True,
            order_no=order_no,
            filled_qty=quantity,
            filled_price=fill_price,
        )

    def cancel_order(self, order_no: str) -> bool:
        logger.info(f"Mock order cancelled: {order_no}")
        return True

    def subscribe_quote(self, symbol: str, callback: Callable[[FuturesQuote], None]):
        self._quote_callbacks.setdefault(symbol, []).append(callback)
        logger.info(f"Mock quote subscription: {symbol}")

    def _update_position(self, symbol: str, side: str, qty: int, price: float):
        """포지션 업데이트."""
        # CL 증거금 ~$6,000/계약, BZ ~$5,500/계약
        margin_per_contract = 6000 if "CL" in symbol else 5500

        existing = next((p for p in self._positions if p.symbol == symbol), None)
        if existing:
            if existing.side == side:
                # 같은 방향 추가
                total_qty = existing.quantity + qty
                existing.avg_price = (
                    existing.avg_price * existing.quantity + price * qty
                ) / total_qty
                existing.quantity = total_qty
                existing.margin_used = total_qty * margin_per_contract
            else:
                # 반대 방향 (청산)
                if qty >= existing.quantity:
                    # 전량 또는 초과 청산
                    remaining = qty - existing.quantity
                    self._positions.remove(existing)
                    if remaining > 0:
                        # 반대 포지션 신규
                        self._positions.append(FuturesPosition(
                            symbol=symbol,
                            side=side,
                            quantity=remaining,
                            avg_price=price,
                            current_price=price,
                            unrealized_pnl=0,
                            margin_used=remaining * margin_per_contract,
                        ))
                else:
                    existing.quantity -= qty
                    existing.margin_used = existing.quantity * margin_per_contract
        else:
            # 신규 포지션
            self._positions.append(FuturesPosition(
                symbol=symbol,
                side=side,
                quantity=qty,
                avg_price=price,
                current_price=price,
                unrealized_pnl=0,
                margin_used=qty * margin_per_contract,
            ))


# ──────────────────────────────────────────────
# Real Implementation (Windows OCX — placeholder)
# ──────────────────────────────────────────────

class KiwoomReal(KiwoomBase):
    """키움 실제 API (OpenAPI-W).

    TODO: Windows 환경에서 koapy 또는 pykiwoom으로 구현.
    OCX 기반이므로 pywin32, comtypes 필요.
    """

    def __init__(self, account_number: str = "", account_password: str = ""):
        self.account_number = account_number
        self.account_password = account_password

    def connect(self) -> bool:
        raise NotImplementedError(
            "키움 실 API는 Windows + OCX 환경에서만 동작합니다. "
            "koapy 또는 pykiwoom을 사용하여 구현하세요."
        )

    def disconnect(self):
        pass

    def get_quote(self, symbol: str) -> FuturesQuote | None:
        raise NotImplementedError

    def get_positions(self) -> list[FuturesPosition]:
        raise NotImplementedError

    def get_margin_info(self) -> dict:
        raise NotImplementedError

    def place_order(self, symbol: str, side: str, quantity: int, price: float | None = None) -> FuturesOrder:
        raise NotImplementedError

    def cancel_order(self, order_no: str) -> bool:
        raise NotImplementedError

    def subscribe_quote(self, symbol: str, callback: Callable[[FuturesQuote], None]):
        raise NotImplementedError


def create_kiwoom_client(use_mock: bool = True, **kwargs) -> KiwoomBase:
    """키움 클라이언트 팩토리."""
    if use_mock:
        return KiwoomMock()
    return KiwoomReal(**kwargs)
