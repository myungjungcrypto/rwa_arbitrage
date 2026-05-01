"""Lighter (zkLighter / app.lighter.xyz) WTI perp 어댑터.

ExchangeBase 구현. lighter-python SDK(`lighter-sdk` PyPI 패키지)에 의존.
SDK는 elliottech/lighter-python — base URL `https://mainnet.zklighter.elliot.ai`.

핵심 매핑:
  - SDK Configuration(host=base_url) → ApiClient
  - OrderApi: order_books / order_book_orders / recent_trades
  - FundingApi (CandlestickApi 측): fundings(market_id, ...)
  - WsClient(host=ws_url, order_book_ids=[market_id], on_order_book_update=...)

Phase D scaffolding: read-only paper trading만. 주문은 NotImplementedError.
real WS 구독 wire는 main.py에서 Phase D 후속 commit에 진행.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable, Optional

from src.exchange import base as _base

logger = logging.getLogger("arbitrage.lighter")


# lighter-sdk는 페이퍼 단계에서 옵션 의존. 미설치 환경(테스트)에서는 mock 주입.
try:
    import lighter as _lighter_sdk          # type: ignore
except ImportError:                         # pragma: no cover — runtime only
    _lighter_sdk = None


DEFAULT_BASE_URL = "https://mainnet.zklighter.elliot.ai"
DEFAULT_WS_URL = "wss://mainnet.zklighter.elliot.ai/stream"


class LighterExchange:
    """ExchangeBase 어댑터 — Lighter perpetual.

    Phase D scaffolding. 실 운영 wire는 후속 commit.

    주요 콜:
      - market_id 등록: 심볼 문자열(`WTI` 등) → integer market_id.
        실 환경에선 startup 시 `order_books()` REST로 동적 lookup. scaffold는
        명시 등록 (`set_market_id(symbol, market_id)`) 만 지원.
      - subscribe_quotes(symbol, callback): lighter.WsClient로 orderbook 구독.
      - get_quote(symbol): REST로 best bid/ask 1회 조회.
      - get_funding_info(symbol): FundingApi에서 최근 funding 시계열.
      - place_order: NotImplementedError (Phase I live 단계).
    """

    name = "lighter"
    venue_type = _base.VenueType.PERP.value
    margin_asset = "USDC"

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        ws_url: str = DEFAULT_WS_URL,
        api_client: Optional[Any] = None,
        ws_client_factory: Optional[Callable[..., Any]] = None,
    ):
        self.base_url = base_url
        self.ws_url = ws_url
        # SDK 인스턴스 — 테스트에서 mock 주입 가능
        self._api_client = api_client
        self._ws_client_factory = ws_client_factory
        self._ws_client: Optional[Any] = None
        # 심볼 ↔ market_id 매핑 + 최근 quote 캐시
        self._market_ids: dict[str, int] = {}
        self._latest_funding: dict[str, _base.FundingInfo] = {}
        self._symbol_callbacks: dict[str, list[_base.QuoteCallback]] = {}
        # symbol에 매칭된 contract metadata (price_decimal 등)
        self._symbol_meta: dict[str, dict[str, Any]] = {}

    # ── 외부 명시 등록 (스캐폴딩 단계) ──

    def set_market_id(
        self,
        symbol: str,
        market_id: int,
        *,
        price_decimal: int = 6,
        size_decimal: int = 4,
    ) -> None:
        """심볼 → market_id 명시 매핑. 실 환경에서 `discover_markets` 후 호출."""
        self._market_ids[symbol] = market_id
        self._symbol_meta[symbol] = {
            "price_decimal": price_decimal,
            "size_decimal": size_decimal,
        }
        logger.info(f"[LIGHTER] market_id mapped: {symbol} → {market_id}")

    def get_market_id(self, symbol: str) -> Optional[int]:
        return self._market_ids.get(symbol)

    async def discover_markets(self) -> dict[str, int]:
        """REST `/api/v1/orderBooks`로 active market 메타 일괄 조회.

        SDK 사용:
          OrderApi(api_client).order_books() → OrderBooks { order_books: [...] }

        반환: {symbol: market_id}. 내부 캐시도 갱신.
        """
        if self._api_client is None:
            raise RuntimeError(
                "discover_markets requires api_client (call connect() first or "
                "inject via constructor for tests)"
            )
        # SDK API 변경 가능성 있어 try/except 보호
        try:
            order_api_cls = getattr(_lighter_sdk, "OrderApi", None) if _lighter_sdk else None
            if order_api_cls is None:
                raise RuntimeError("lighter SDK not available (install lighter-sdk)")
            order_api = order_api_cls(self._api_client)
            books = await order_api.order_books()
            mapping: dict[str, int] = {}
            for ob in getattr(books, "order_books", []) or []:
                sym = getattr(ob, "symbol", None) or getattr(ob, "market_symbol", None)
                mid = getattr(ob, "market_id", None)
                if sym and mid is not None:
                    mapping[sym] = int(mid)
                    pd = int(getattr(ob, "price_decimal", 6) or 6)
                    sd = int(getattr(ob, "size_decimal", 4) or 4)
                    self.set_market_id(sym, int(mid), price_decimal=pd, size_decimal=sd)
            return mapping
        except Exception as e:
            logger.error(f"[LIGHTER] discover_markets error: {e}")
            return {}

    # ── ExchangeBase 인터페이스 ──

    async def connect(self) -> bool:
        """SDK ApiClient 초기화. ws_client는 subscribe_quotes 호출 시 lazy 생성."""
        if self._api_client is not None:
            return True   # 이미 주입됨 (테스트 또는 외부 wiring)
        if _lighter_sdk is None:
            logger.warning("lighter SDK not installed; LighterExchange in stub mode")
            return False
        try:
            cfg = _lighter_sdk.Configuration(host=self.base_url)
            self._api_client = _lighter_sdk.ApiClient(cfg)
            return True
        except Exception as e:
            logger.error(f"[LIGHTER] connect error: {e}")
            return False

    async def disconnect(self) -> None:
        try:
            if self._ws_client is not None and hasattr(self._ws_client, "close"):
                await self._ws_client.close()
        except Exception as e:
            logger.warning(f"[LIGHTER] ws close error: {e}")
        self._ws_client = None
        try:
            if self._api_client is not None and hasattr(self._api_client, "close"):
                await self._api_client.close()
        except Exception as e:
            logger.warning(f"[LIGHTER] api_client close error: {e}")
        self._api_client = None

    async def subscribe_quotes(
        self,
        symbol: str,
        callback: _base.QuoteCallback,
        *,
        contract_size: float = 1.0,
    ) -> None:
        """orderbook + funding 구독. lighter.WsClient 콜백을 Quote로 변환."""
        market_id = self._market_ids.get(symbol)
        if market_id is None:
            raise KeyError(
                f"market_id for {symbol!r} not set. "
                f"Call discover_markets() or set_market_id() first."
            )
        self._symbol_callbacks.setdefault(symbol, []).append(callback)

        # WsClient 생성 (lazy) — factory 주입 우선 (테스트), 그 외 SDK 기본
        if self._ws_client is not None:
            return    # 이미 다른 심볼 구독 중

        def _on_order_book_update(mid: int, order_book: dict) -> None:
            # market_id → symbol 역매핑
            sym = next(
                (s for s, m in self._market_ids.items() if m == mid),
                None,
            )
            if sym is None:
                return
            quote = self._build_quote_from_orderbook(sym, order_book)
            for cb in self._symbol_callbacks.get(sym, []):
                try:
                    result = cb(quote)
                    if result is not None and asyncio.iscoroutine(result):
                        asyncio.create_task(result)
                except Exception as e:
                    logger.error(f"[LIGHTER] callback error [{sym}]: {e}")

        market_ids = list(self._market_ids.values())
        if self._ws_client_factory is not None:
            self._ws_client = self._ws_client_factory(
                host=self.ws_url,
                order_book_ids=market_ids,
                on_order_book_update=_on_order_book_update,
            )
        elif _lighter_sdk is not None and hasattr(_lighter_sdk, "WsClient"):
            self._ws_client = _lighter_sdk.WsClient(
                host=self.ws_url,
                order_book_ids=market_ids,
                on_order_book_update=_on_order_book_update,
            )
        else:
            logger.warning("[LIGHTER] WsClient unavailable; subscribe is a no-op")
            return

        # SDK의 `run()`은 blocking — 백그라운드 task로 기동 (main.py가 관리하는 게
        # 더 깔끔하지만 scaffold 단계는 self-contained)
        if hasattr(self._ws_client, "run"):
            asyncio.create_task(self._run_ws_safely())

    async def _run_ws_safely(self) -> None:
        try:
            run = self._ws_client.run    # type: ignore[union-attr]
            if asyncio.iscoroutinefunction(run):
                await run()
            else:
                # sync run() → executor에서 돌림
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, run)
        except Exception as e:
            logger.error(f"[LIGHTER] WS run error: {e}")

    async def unsubscribe_quotes(self, symbol: str) -> None:
        self._symbol_callbacks.pop(symbol, None)
        # WsClient는 보통 재구성이 필요. scaffold 단계는 단순 unset.

    async def get_quote(self, symbol: str) -> Optional[_base.Quote]:
        """REST로 1회 조회 — best bid/ask + 최근 funding."""
        market_id = self._market_ids.get(symbol)
        if market_id is None:
            return None
        if self._api_client is None:
            return None
        try:
            order_api_cls = getattr(_lighter_sdk, "OrderApi", None) if _lighter_sdk else None
            if order_api_cls is None:
                return None
            order_api = order_api_cls(self._api_client)
            ob = await order_api.order_book_orders(market_id=market_id, limit=1)
        except Exception as e:
            logger.error(f"[LIGHTER] get_quote REST error: {e}")
            return None

        return self._build_quote_from_orderbook(symbol, ob)

    async def place_order(
        self,
        symbol: str,
        side: _base.OrderSideLiteral,
        size: float,
        order_type: _base.OrderTypeLiteral = "market",
        limit_price: Optional[float] = None,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> _base.OrderResult:
        raise NotImplementedError(
            "LighterExchange.place_order is paper-only stage. "
            "Live order placement lands in Phase I."
        )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        raise NotImplementedError("LighterExchange.cancel_order: Phase I live only.")

    async def get_positions(self) -> list[_base.Position]:
        return []   # 페이퍼 단계 (live 단계에서 SDK AccountApi.account 활용)

    async def get_account_value(self) -> float:
        return 0.0

    async def get_funding_info(self, symbol: str) -> Optional[_base.FundingInfo]:
        """최근 fundings 시계열에서 정산 주기 + 현재 rate 추정.

        Lighter는 1h funding 표준. observed_interval은 인접 정산 timestamp 차이.
        """
        market_id = self._market_ids.get(symbol)
        if market_id is None or self._api_client is None or _lighter_sdk is None:
            return None
        try:
            funding_api_cls = getattr(_lighter_sdk, "FundingApi", None)
            if funding_api_cls is None:
                return None
            funding_api = funding_api_cls(self._api_client)
            resp = await funding_api.fundings(market_id=market_id)
        except Exception as e:
            logger.error(f"[LIGHTER] funding API error: {e}")
            return None

        events = getattr(resp, "fundings", None) or getattr(resp, "events", None) or []
        if len(events) < 2:
            return None
        try:
            timestamps_s = sorted(int(getattr(e, "timestamp", 0)) for e in events)
        except Exception:
            return None
        if len(timestamps_s) < 2:
            return None
        diffs = [timestamps_s[i + 1] - timestamps_s[i] for i in range(len(timestamps_s) - 1)]
        diffs.sort()
        median_seconds = diffs[len(diffs) // 2]
        observed_hours = median_seconds / 3600.0
        next_settlement = timestamps_s[-1] + median_seconds

        try:
            current_rate = float(getattr(events[-1], "rate", 0) or getattr(events[-1], "funding_rate", 0))
        except Exception:
            current_rate = 0.0

        info = _base.FundingInfo(
            exchange=self.name,
            symbol=symbol,
            current_rate=current_rate,
            next_settlement_ts=next_settlement,
            observed_interval_hours=observed_hours,
        )
        self._latest_funding[symbol] = info
        return info

    # ── 내부: orderbook 응답 → Quote ──

    def _build_quote_from_orderbook(self, symbol: str, order_book: Any) -> _base.Quote:
        """SDK의 OrderBook 응답 또는 dict에서 best bid/ask 추출."""
        bids = self._extract_levels(order_book, "bids") or self._extract_levels(order_book, "buy_orders")
        asks = self._extract_levels(order_book, "asks") or self._extract_levels(order_book, "sell_orders")
        bid = self._level_price(bids, 0)
        ask = self._level_price(asks, 0)
        bid_qty = self._level_size(bids, 0)
        ask_qty = self._level_size(asks, 0)
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else max(bid, ask)
        funding = self._latest_funding.get(symbol)
        return _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=mid,
            bid=bid, ask=ask,
            bid_qty=bid_qty, ask_qty=ask_qty,
            funding_rate=funding.current_rate if funding else 0.0,
            funding_interval_hours=(funding.observed_interval_hours if funding else 1.0),
        )

    @staticmethod
    def _extract_levels(book: Any, key: str) -> list:
        if hasattr(book, key):
            return getattr(book, key) or []
        if isinstance(book, dict):
            return book.get(key, []) or []
        return []

    @staticmethod
    def _level_price(levels: list, idx: int) -> float:
        if not levels or idx >= len(levels):
            return 0.0
        lvl = levels[idx]
        if hasattr(lvl, "price"):
            return float(lvl.price)
        if isinstance(lvl, dict):
            return float(lvl.get("price", 0) or 0)
        if isinstance(lvl, (list, tuple)) and len(lvl) > 0:
            return float(lvl[0])
        return 0.0

    @staticmethod
    def _level_size(levels: list, idx: int) -> float:
        if not levels or idx >= len(levels):
            return 0.0
        lvl = levels[idx]
        for k in ("size", "quantity", "amount"):
            if hasattr(lvl, k):
                return float(getattr(lvl, k))
        if isinstance(lvl, dict):
            for k in ("size", "quantity", "amount"):
                if k in lvl:
                    return float(lvl[k])
        if isinstance(lvl, (list, tuple)) and len(lvl) > 1:
            return float(lvl[1])
        return 0.0
