"""Bybit v5 linear (USDT perpetual) 어댑터 — Phase F.

ExchangeBase 구현. 페이퍼 단계 read-only — 주문은 NotImplementedError.
시세는 v5 REST + WS public stream (인증 불필요):
  REST  https://api.bybit.com
    /v5/market/instruments-info?category=linear&symbol=CLUSDT
    /v5/market/tickers?category=linear&symbol=CLUSDT       — bid/ask + funding
    /v5/market/funding/history?category=linear&symbol=...  — interval 추정
  WS    wss://stream.bybit.com/v5/public/linear
    op=subscribe args=["orderbook.1.CLUSDT", "tickers.CLUSDT"]
      orderbook.1: best bid/ask delta+snapshot
      tickers:     mark/index/funding 1초당 갱신 (markPrice + lastFundingRate)

Funding: 4h interval (CLUSDT 2026-03-27 런칭 시점). 정책 변경 가능 →
funding history로 실측 (FundingIntervalMonitor).

NOTE: CLUSDT는 quote=USDT (USDC 아님) — leg config margin_asset=USDT 명시.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, Optional

from src.exchange import base as _base

logger = logging.getLogger("arbitrage.bybit")

try:
    import aiohttp
except ImportError:                         # pragma: no cover
    aiohttp = None  # type: ignore

try:
    import websockets
except ImportError:                         # pragma: no cover
    websockets = None  # type: ignore


DEFAULT_REST_URL = "https://api.bybit.com"
DEFAULT_WS_URL = "wss://stream.bybit.com/v5/public/linear"
CATEGORY = "linear"   # USDT-margined perpetual


class BybitExchange:
    """ExchangeBase 어댑터 — Bybit v5 linear perpetual.

    Phase F scaffolding. 주문은 Phase I live (HMAC v5 sig).
    """

    name = "bybit"
    venue_type = _base.VenueType.PERP.value
    margin_asset = "USDT"

    def __init__(
        self,
        rest_url: str = DEFAULT_REST_URL,
        ws_url: str = DEFAULT_WS_URL,
        session_factory: Optional[Callable[[], Any]] = None,
        ws_connect: Optional[Callable[..., Any]] = None,
    ):
        self.rest_url = rest_url.rstrip("/")
        self.ws_url = ws_url.rstrip("/")
        self._session_factory = session_factory
        self._ws_connect = ws_connect
        self._session: Optional[Any] = None
        self._symbol_callbacks: dict[str, list[_base.QuoteCallback]] = {}
        self._latest_ticker: dict[str, dict[str, float]] = {}    # mark/funding 캐시
        self._latest_book: dict[str, dict[str, float]] = {}      # bid/ask 캐시
        self._ws_tasks: dict[str, asyncio.Task] = {}
        self._symbol_meta: dict[str, dict[str, Any]] = {}
        self._stopped = False

    # ── 외부 명시 등록 ──

    def set_symbol_meta(
        self, symbol: str, *,
        tick_size: float = 0.01,
        qty_step: float = 0.001,
        min_qty: float = 0.001,
    ) -> None:
        self._symbol_meta[symbol] = dict(
            tick_size=tick_size, qty_step=qty_step, min_qty=min_qty,
        )

    # ── 세션 lifecycle ──

    async def connect(self) -> bool:
        if aiohttp is None:
            logger.warning("aiohttp not installed; BybitExchange in stub mode")
            return False
        if self._session is not None:
            return True
        try:
            self._session = (self._session_factory or aiohttp.ClientSession)()
            return True
        except Exception as e:
            logger.error(f"[BYBIT] connect error: {e}")
            return False

    async def disconnect(self) -> None:
        self._stopped = True
        for sym, t in list(self._ws_tasks.items()):
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
        self._ws_tasks.clear()
        if self._session is not None:
            try:
                await self._session.close()
            except Exception as e:
                logger.warning(f"[BYBIT] session close error: {e}")
        self._session = None

    # ── REST helpers ──

    async def _get(self, path: str, params: Optional[dict] = None) -> Any:
        if self._session is None:
            await self.connect()
        if self._session is None:
            return None
        url = f"{self.rest_url}{path}"
        try:
            async with self._session.get(url, params=params or {}) as r:
                if r.status != 200:
                    text = await r.text()
                    logger.error(f"[BYBIT] {path} {r.status}: {text[:200]}")
                    return None
                return await r.json()
        except Exception as e:
            logger.error(f"[BYBIT] {path} error: {e}")
            return None

    async def discover_symbol(self, symbol: str) -> bool:
        """v5 instruments-info → tickSize / qtyStep / minOrderQty 캐시."""
        data = await self._get(
            "/v5/market/instruments-info",
            {"category": CATEGORY, "symbol": symbol},
        )
        if not isinstance(data, dict) or data.get("retCode") != 0:
            logger.warning(f"[BYBIT] {symbol} instruments-info missing or error: {data}")
            return False
        items = (data.get("result") or {}).get("list") or []
        if not items:
            return False
        item = items[0]
        pf = item.get("priceFilter", {}) or {}
        lf = item.get("lotSizeFilter", {}) or {}
        try:
            tick_size = float(pf.get("tickSize", 0.01) or 0.01)
            qty_step = float(lf.get("qtyStep", 0.001) or 0.001)
            min_qty = float(lf.get("minOrderQty", 0.001) or 0.001)
        except (TypeError, ValueError):
            tick_size, qty_step, min_qty = 0.01, 0.001, 0.001
        self.set_symbol_meta(symbol, tick_size=tick_size,
                              qty_step=qty_step, min_qty=min_qty)
        logger.info(
            f"[BYBIT] {symbol} meta: tick={tick_size} qtyStep={qty_step} minQty={min_qty}"
        )
        return True

    # ── ExchangeBase ──

    async def get_quote(self, symbol: str) -> Optional[_base.Quote]:
        """v5 tickers REST → bid/ask + mark + funding 한 번에."""
        data = await self._get(
            "/v5/market/tickers",
            {"category": CATEGORY, "symbol": symbol},
        )
        if not isinstance(data, dict) or data.get("retCode") != 0:
            return None
        items = (data.get("result") or {}).get("list") or []
        if not items:
            return None
        return self._build_quote_from_ticker(symbol, items[0])

    async def subscribe_quotes(
        self,
        symbol: str,
        callback: _base.QuoteCallback,
        *,
        contract_size: float = 1.0,
    ) -> None:
        """orderbook.1.{symbol} + tickers.{symbol} 동시 구독."""
        self._symbol_callbacks.setdefault(symbol, []).append(callback)
        if symbol in self._ws_tasks:
            return

        async def _runner():
            await self._ws_run(symbol)

        self._ws_tasks[symbol] = asyncio.create_task(_runner())

    async def unsubscribe_quotes(self, symbol: str) -> None:
        self._symbol_callbacks.pop(symbol, None)
        t = self._ws_tasks.pop(symbol, None)
        if t is not None:
            t.cancel()
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    async def _ws_run(self, symbol: str) -> None:
        if websockets is None:
            logger.warning("[BYBIT] websockets not installed; WS disabled")
            return
        backoff = 2
        sub_msg = json.dumps({
            "op": "subscribe",
            "args": [f"orderbook.1.{symbol}", f"tickers.{symbol}"],
        })
        while not self._stopped:
            try:
                connect_fn = self._ws_connect or websockets.connect
                async with connect_fn(self.ws_url, ping_interval=20) as ws:
                    backoff = 2
                    logger.info(f"[BYBIT] WS connected {symbol}")
                    await ws.send(sub_msg)
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await self._handle_ws_msg(symbol, msg)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[BYBIT] WS {symbol} error: {e}; reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_ws_msg(self, symbol: str, msg: dict) -> None:
        """v5 public stream 메시지 분기.

        형식:
          {"topic": "orderbook.1.CLUSDT", "type": "snapshot"|"delta",
           "data": {"s": "CLUSDT", "b": [["80.10","10"]], "a": [["80.20","12"]], ...}, ...}
          {"topic": "tickers.CLUSDT", "type": "snapshot"|"delta",
           "data": {"symbol":"CLUSDT", "markPrice":"80.15",
                    "indexPrice":"80.12", "fundingRate":"0.0001",
                    "nextFundingTime":"1700000000000", ...}}
          {"op":"subscribe","success":true,...}    — 구독 ack (무시)
          {"op":"pong",...}                         — keep-alive
        """
        topic = msg.get("topic", "")
        data = msg.get("data")
        if not topic or data is None:
            return
        if topic.startswith("orderbook."):
            await self._on_orderbook(symbol, data)
        elif topic.startswith("tickers."):
            self._on_tickers(symbol, data)

    async def _on_orderbook(self, symbol: str, data: dict) -> None:
        # b/a: list of [price, qty] strings
        bids = data.get("b") or []
        asks = data.get("a") or []
        # delta 스트림 — 새 best price 있으면 갱신, 없으면 기존 캐시 유지.
        # 단순화: 들어온 best level만 캐시 (orderbook.1이라 1개만 옴).
        cache = self._latest_book.setdefault(symbol, {})
        if bids:
            try:
                cache["bid"] = float(bids[0][0])
                cache["bid_qty"] = float(bids[0][1])
            except (TypeError, ValueError, IndexError):
                pass
        if asks:
            try:
                cache["ask"] = float(asks[0][0])
                cache["ask_qty"] = float(asks[0][1])
            except (TypeError, ValueError, IndexError):
                pass
        # bid 또는 ask 둘 중 하나라도 있으면 fan-out
        await self._emit(symbol)

    def _on_tickers(self, symbol: str, data: dict) -> None:
        # tickers는 delta로 일부 필드만 올 수 있음 → 캐시 누적
        c = self._latest_ticker.setdefault(symbol, {})
        try:
            if "markPrice" in data and data["markPrice"]:
                c["mark_price"] = float(data["markPrice"])
            if "indexPrice" in data and data["indexPrice"]:
                c["index_price"] = float(data["indexPrice"])
            if "fundingRate" in data and data["fundingRate"]:
                c["funding_rate"] = float(data["fundingRate"])
            if "nextFundingTime" in data and data["nextFundingTime"]:
                c["next_funding_ts"] = float(data["nextFundingTime"]) / 1000.0
        except (TypeError, ValueError):
            pass

    async def _emit(self, symbol: str) -> None:
        book = self._latest_book.get(symbol)
        ticker = self._latest_ticker.get(symbol, {})
        if not book:
            return
        bid = book.get("bid", 0.0)
        ask = book.get("ask", 0.0)
        bid_qty = book.get("bid_qty", 0.0)
        ask_qty = book.get("ask_qty", 0.0)
        mark = ticker.get("mark_price", 0.0)
        index = ticker.get("index_price", 0.0)
        funding = ticker.get("funding_rate", 0.0)
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else mark
        quote = _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=mid,
            bid=bid, ask=ask,
            bid_qty=bid_qty, ask_qty=ask_qty,
            index_price=index,
            funding_rate=funding,
            funding_interval_hours=4.0,
        )
        for cb in self._symbol_callbacks.get(symbol, []):
            try:
                result = cb(quote)
                if result is not None and asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception as e:
                logger.error(f"[BYBIT] cb error [{symbol}]: {e}")

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
            "BybitExchange.place_order is paper-only stage. "
            "Live order placement lands in Phase I (HMAC v5 signed)."
        )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        raise NotImplementedError("BybitExchange.cancel_order: Phase I live only.")

    async def get_positions(self) -> list[_base.Position]:
        return []

    async def get_account_value(self) -> float:
        return 0.0

    async def get_funding_info(self, symbol: str) -> Optional[_base.FundingInfo]:
        """tickers의 fundingRate + funding/history로 interval 추정."""
        ticker = await self._get(
            "/v5/market/tickers",
            {"category": CATEGORY, "symbol": symbol},
        )
        current_rate = 0.0
        next_ts = 0.0
        if isinstance(ticker, dict) and ticker.get("retCode") == 0:
            items = (ticker.get("result") or {}).get("list") or []
            if items:
                t0 = items[0]
                try:
                    current_rate = float(t0.get("fundingRate", 0) or 0)
                    next_ts = float(t0.get("nextFundingTime", 0) or 0) / 1000.0
                except (TypeError, ValueError):
                    pass
        # interval 추정
        history = await self._get(
            "/v5/market/funding/history",
            {"category": CATEGORY, "symbol": symbol, "limit": 6},
        )
        observed = 4.0
        if isinstance(history, dict) and history.get("retCode") == 0:
            items = (history.get("result") or {}).get("list") or []
            if len(items) >= 2:
                try:
                    ts_s = sorted(float(it.get("fundingRateTimestamp", 0)) / 1000.0
                                  for it in items)
                    diffs = [ts_s[i + 1] - ts_s[i] for i in range(len(ts_s) - 1)]
                    diffs.sort()
                    median_seconds = diffs[len(diffs) // 2]
                    observed = median_seconds / 3600.0
                except Exception:
                    pass
        return _base.FundingInfo(
            exchange=self.name,
            symbol=symbol,
            current_rate=current_rate,
            next_settlement_ts=next_ts,
            observed_interval_hours=observed,
        )

    # ── 내부: REST tickers 응답 → Quote ──

    def _build_quote_from_ticker(self, symbol: str, t: dict) -> _base.Quote:
        try:
            bid = float(t.get("bid1Price", 0) or 0)
            ask = float(t.get("ask1Price", 0) or 0)
            bid_qty = float(t.get("bid1Size", 0) or 0)
            ask_qty = float(t.get("ask1Size", 0) or 0)
            mark = float(t.get("markPrice", 0) or 0)
            index = float(t.get("indexPrice", 0) or 0)
            funding = float(t.get("fundingRate", 0) or 0)
        except (TypeError, ValueError):
            bid = ask = bid_qty = ask_qty = mark = index = funding = 0.0
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else mark
        return _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=mid,
            bid=bid, ask=ask,
            bid_qty=bid_qty, ask_qty=ask_qty,
            index_price=index,
            funding_rate=funding,
            funding_interval_hours=4.0,
        )
