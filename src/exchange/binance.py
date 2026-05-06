"""Binance USDⓈ-M Futures (CLUSDT) 어댑터 — Phase E.

ExchangeBase 구현. 페이퍼 단계 read-only — 주문은 NotImplementedError.
시세는 fapi REST + WS (인증 불필요) 사용:
  REST  https://fapi.binance.com
    /fapi/v1/exchangeInfo                 — 심볼 메타데이터
    /fapi/v1/premiumIndex?symbol=CLUSDT   — mark+oracle+funding 1회 조회
    /fapi/v1/depth?symbol=CLUSDT&limit=5  — 호가 1회 조회
  WS    wss://fstream.binance.com/ws
    {symbol}@bookTicker                   — best bid/ask 실시간
    {symbol}@markPrice@1s                 — mark/funding 1초 단위

Funding: 4h interval (CLUSDT 기준, 2026-04 런칭 시점). 정책 변경 가능 →
FundingIntervalMonitor가 nextFundingTime 차이로 실측.

NOTE: 모든 가격 응답은 string. CLUSDT는 quote=USDT, USDC가 아님 — 페어 leg에서
margin_asset=USDT 명시 + Phase H에서 USDC/USDT peg 모니터.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, Optional

from src.exchange import base as _base

logger = logging.getLogger("arbitrage.binance")

try:
    import aiohttp
except ImportError:                         # pragma: no cover
    aiohttp = None  # type: ignore

try:
    import websockets
except ImportError:                         # pragma: no cover
    websockets = None  # type: ignore


DEFAULT_REST_URL = "https://fapi.binance.com"
DEFAULT_WS_URL = "wss://fstream.binance.com/ws"


class BinanceExchange:
    """ExchangeBase 어댑터 — Binance USDⓈ-M Futures.

    Phase E scaffolding. 주문은 NotImplementedError (Phase I live).
    """

    name = "binance"
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
        self._session_factory = session_factory   # 테스트 mock 주입
        self._ws_connect = ws_connect             # 테스트 mock 주입
        self._session: Optional[Any] = None
        # 심볼 → 콜백 (다중 등록 가능)
        self._symbol_callbacks: dict[str, list[_base.QuoteCallback]] = {}
        # 심볼별 mark/funding 캐시 (markPrice 스트림이 들어오면 갱신)
        self._latest_mark: dict[str, dict[str, float]] = {}
        # 활성 WS task
        self._ws_tasks: dict[str, asyncio.Task] = {}
        # symbol metadata (tickSize, stepSize 등)
        self._symbol_meta: dict[str, dict[str, Any]] = {}
        # stop flag for ws loops
        self._stopped = False

    # ── 외부 명시 등록 ──

    def set_symbol_meta(
        self,
        symbol: str,
        *,
        tick_size: float = 0.01,
        step_size: float = 0.001,
        min_qty: float = 0.001,
    ) -> None:
        self._symbol_meta[symbol] = dict(
            tick_size=tick_size, step_size=step_size, min_qty=min_qty,
        )

    # ── 세션/연결 lifecycle ──

    async def connect(self) -> bool:
        if aiohttp is None:
            logger.warning("aiohttp not installed; BinanceExchange in stub mode")
            return False
        if self._session is not None:
            return True
        try:
            self._session = (self._session_factory or aiohttp.ClientSession)()
            return True
        except Exception as e:
            logger.error(f"[BINANCE] connect error: {e}")
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
                logger.warning(f"[BINANCE] session close error: {e}")
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
                    logger.error(f"[BINANCE] {path} {r.status}: {text[:200]}")
                    return None
                return await r.json()
        except Exception as e:
            logger.error(f"[BINANCE] {path} error: {e}")
            return None

    async def discover_symbol(self, symbol: str) -> bool:
        """`/fapi/v1/exchangeInfo`에서 symbol filters 추출.

        tickSize / stepSize / minQty를 캐시. 추후 주문 발주 시 사용.
        """
        data = await self._get("/fapi/v1/exchangeInfo")
        if not isinstance(data, dict):
            return False
        for s in data.get("symbols", []) or []:
            if s.get("symbol") != symbol:
                continue
            tick_size = step_size = min_qty = None
            for f in s.get("filters", []) or []:
                ft = f.get("filterType")
                if ft == "PRICE_FILTER":
                    tick_size = float(f.get("tickSize", 0.01))
                elif ft in ("LOT_SIZE", "MARKET_LOT_SIZE"):
                    step_size = float(f.get("stepSize", step_size or 0.001))
                    min_qty = float(f.get("minQty", min_qty or 0.001))
            self.set_symbol_meta(
                symbol,
                tick_size=tick_size or 0.01,
                step_size=step_size or 0.001,
                min_qty=min_qty or 0.001,
            )
            logger.info(
                f"[BINANCE] {symbol} meta: tick={tick_size} step={step_size} minQty={min_qty}"
            )
            return True
        logger.warning(f"[BINANCE] {symbol} not in exchangeInfo")
        return False

    # ── ExchangeBase ──

    async def get_quote(self, symbol: str) -> Optional[_base.Quote]:
        """`bookTicker` + `premiumIndex` 1회 조합 → Quote."""
        bt_task = self._get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})
        pi_task = self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        bt, pi = await asyncio.gather(bt_task, pi_task)
        return self._build_quote(symbol, bt or {}, pi or {})

    async def subscribe_quotes(
        self,
        symbol: str,
        callback: _base.QuoteCallback,
        *,
        contract_size: float = 1.0,
    ) -> None:
        """{symbol}@bookTicker + {symbol}@markPrice@1s 동시 구독.

        bookTicker 스트림이 best bid/ask 실시간 push (tick 단위 변경 시).
        markPrice 스트림은 1초당 mark/oracle/funding 갱신 — 캐시에 저장 후
        bookTicker push 시 합쳐서 Quote 송출.
        """
        self._symbol_callbacks.setdefault(symbol, []).append(callback)
        # 이미 구독 중이면 콜백만 추가하고 끝
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
        """WS 재연결 루프. {symbol}@bookTicker/markPrice 결합 스트림."""
        if websockets is None:
            logger.warning("[BINANCE] websockets not installed; WS disabled")
            return
        ll_sym = symbol.lower()
        # 결합 스트림: <ws_url>/<sym>@bookTicker/<sym>@markPrice@1s
        url = f"{self.ws_url.rstrip('/')}/{ll_sym}@bookTicker/{ll_sym}@markPrice@1s"
        backoff = 2
        while not self._stopped:
            try:
                connect_fn = self._ws_connect or websockets.connect
                async with connect_fn(url, ping_interval=30) as ws:
                    backoff = 2
                    logger.info(f"[BINANCE] WS connected {symbol}")
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except Exception:
                            continue
                        await self._handle_ws_msg(symbol, msg)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error(f"[BINANCE] WS {symbol} error: {e}; reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_ws_msg(self, symbol: str, msg: dict) -> None:
        """결합 스트림 메시지 분기.

        Binance combined-stream 형식 옵션 1: 단일 스트림 — `e` 필드로 구분
          bookTicker:  e=bookTicker (실제 응답에는 e가 없을 수도) — `b`/`a` 필드 존재
          markPrice:   e=markPriceUpdate

        형식 옵션 2: combined wrapper — { "stream": "...", "data": {...} }
        URL path-style 결합 (이번 구현)에서는 옵션 1.
        """
        # combined wrapper인 경우 풀어줌
        if isinstance(msg.get("data"), dict):
            msg = msg["data"]

        event = msg.get("e")
        # bookTicker — e 없을 수도 있음
        if "b" in msg and "a" in msg and ("u" in msg or event == "bookTicker"):
            await self._on_book_ticker(symbol, msg)
        elif event == "markPriceUpdate":
            self._on_mark_price(symbol, msg)

    async def _on_book_ticker(self, symbol: str, msg: dict) -> None:
        bt = {
            "bidPrice": msg.get("b", "0"),
            "askPrice": msg.get("a", "0"),
            "bidQty":   msg.get("B", "0"),
            "askQty":   msg.get("A", "0"),
        }
        # mark/funding은 캐시에서 가져옴 (markPrice 스트림이 별도로 push)
        cached = self._latest_mark.get(symbol, {})
        pi = {
            "markPrice":         str(cached.get("mark_price", 0)),
            "indexPrice":        str(cached.get("index_price", 0)),
            "lastFundingRate":   str(cached.get("funding_rate", 0)),
        }
        quote = self._build_quote(symbol, bt, pi)
        for cb in self._symbol_callbacks.get(symbol, []):
            try:
                result = cb(quote)
                if result is not None and asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception as e:
                logger.error(f"[BINANCE] cb error [{symbol}]: {e}")

    def _on_mark_price(self, symbol: str, msg: dict) -> None:
        # https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream
        # p=markPrice, i=indexPrice, r=fundingRate, T=nextFundingTime(ms)
        try:
            self._latest_mark[symbol] = dict(
                mark_price=float(msg.get("p", 0)),
                index_price=float(msg.get("i", 0)),
                funding_rate=float(msg.get("r", 0)),
                next_funding_ts=float(msg.get("T", 0)) / 1000.0,
            )
        except (TypeError, ValueError):
            pass

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
            "BinanceExchange.place_order is paper-only stage. "
            "Live order placement lands in Phase I (HMAC-SHA256 signed)."
        )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        raise NotImplementedError("BinanceExchange.cancel_order: Phase I live only.")

    async def get_positions(self) -> list[_base.Position]:
        return []

    async def get_account_value(self) -> float:
        return 0.0

    async def get_funding_info(self, symbol: str) -> Optional[_base.FundingInfo]:
        """premiumIndex 응답의 nextFundingTime + lastFundingRate 활용.

        주기는 fundingRate history (`/fapi/v1/fundingRate`) 최근 N개 timestamp
        차이로 추정 (Binance가 정책상 1h/4h/8h를 변경한 사례 있음).
        """
        pi = await self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        if not isinstance(pi, dict):
            return None
        try:
            current_rate = float(pi.get("lastFundingRate", 0) or 0)
            next_ts = float(pi.get("nextFundingTime", 0) or 0) / 1000.0
        except (TypeError, ValueError):
            return None
        # history로 주기 추정 (5개)
        history = await self._get(
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "limit": 6},
        ) or []
        observed = 4.0   # 설계 default (CLUSDT 4h)
        if isinstance(history, list) and len(history) >= 2:
            try:
                ts_s = sorted(float(h.get("fundingTime", 0)) / 1000.0 for h in history)
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

    # ── 내부: 응답 dict → Quote ──

    def _build_quote(self, symbol: str, bt: dict, pi: dict) -> _base.Quote:
        """`bookTicker` REST 응답 + `premiumIndex` REST 응답 → Quote.

        WS 콜백에서도 동일 dict 키 형태로 정규화해서 호출 가능.
        """
        try:
            bid = float(bt.get("bidPrice", 0) or 0)
            ask = float(bt.get("askPrice", 0) or 0)
            bid_qty = float(bt.get("bidQty", 0) or 0)
            ask_qty = float(bt.get("askQty", 0) or 0)
        except (TypeError, ValueError):
            bid = ask = bid_qty = ask_qty = 0.0
        try:
            mark = float(pi.get("markPrice", 0) or 0)
            index = float(pi.get("indexPrice", 0) or 0)
            funding = float(pi.get("lastFundingRate", 0) or 0)
        except (TypeError, ValueError):
            mark = index = funding = 0.0
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else mark
        return _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=mid,
            bid=bid, ask=ask,
            bid_qty=bid_qty, ask_qty=ask_qty,
            index_price=index,
            funding_rate=funding,
            funding_interval_hours=4.0,   # CLUSDT 기본
        )
