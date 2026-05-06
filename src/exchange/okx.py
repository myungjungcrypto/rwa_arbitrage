"""OKX v5 SWAP (CL-USDT-SWAP) 어댑터 — Phase G.

ExchangeBase 구현. 페이퍼 단계 read-only — 주문은 NotImplementedError.
시세는 v5 REST + WS public (인증 불필요):
  REST  https://www.okx.com
    /api/v5/public/instruments?instType=SWAP&instId=CL-USDT-SWAP
    /api/v5/market/ticker?instId=CL-USDT-SWAP             — bid/ask
    /api/v5/public/mark-price?instType=SWAP&instId=...     — mark
    /api/v5/public/funding-rate?instId=...                 — current funding
    /api/v5/public/funding-rate-history?instId=...&limit=6 — interval 추정
  WS    wss://ws.okx.com:8443/ws/v5/public
    op=subscribe args=[{channel:"books5", instId},
                       {channel:"mark-price", instId},
                       {channel:"funding-rate", instId}]
      books5: top-5 호가 (best bid/ask 1번째)
      mark-price: 1초당 mark
      funding-rate: 정산마다 push

Funding: 사용자 정정 — OKX CL-USDT-SWAP은 4h interval (8h 아님).
실측은 funding-rate-history 시각 차이로 검증.

NOTE: CL-USDT-SWAP는 quote=USDT — leg margin_asset=USDT. live (Phase I)에서
HMAC-SHA256 + API-KEY/SIGN/PASSPHRASE 3-header 인증 추가.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable, Optional

from src.exchange import base as _base

logger = logging.getLogger("arbitrage.okx")

try:
    import aiohttp
except ImportError:                         # pragma: no cover
    aiohttp = None  # type: ignore

try:
    import websockets
except ImportError:                         # pragma: no cover
    websockets = None  # type: ignore


DEFAULT_REST_URL = "https://www.okx.com"
DEFAULT_WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
INST_TYPE = "SWAP"


class OKXExchange:
    """ExchangeBase 어댑터 — OKX v5 swap.

    Phase G scaffolding. 주문은 Phase I live (3-header HMAC).
    """

    name = "okx"
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
        # 캐시 — books5 → bid/ask, mark-price → mark, funding-rate → funding
        self._latest_book: dict[str, dict[str, float]] = {}
        self._latest_mark: dict[str, float] = {}
        self._latest_funding: dict[str, float] = {}
        self._ws_tasks: dict[str, asyncio.Task] = {}
        self._symbol_meta: dict[str, dict[str, Any]] = {}
        self._stopped = False

    def set_symbol_meta(
        self, symbol: str, *,
        tick_size: float = 0.01,
        lot_size: float = 1.0,
        min_size: float = 1.0,
        ct_val: float = 1.0,         # contract value (= 계약당 underlying 단위)
    ) -> None:
        self._symbol_meta[symbol] = dict(
            tick_size=tick_size, lot_size=lot_size,
            min_size=min_size, ct_val=ct_val,
        )

    async def connect(self) -> bool:
        if aiohttp is None:
            logger.warning("aiohttp not installed; OKXExchange in stub mode")
            return False
        if self._session is not None:
            return True
        try:
            self._session = (self._session_factory or aiohttp.ClientSession)()
            return True
        except Exception as e:
            logger.error(f"[OKX] connect error: {e}")
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
                logger.warning(f"[OKX] session close error: {e}")
        self._session = None

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
                    logger.error(f"[OKX] {path} {r.status}: {text[:200]}")
                    return None
                return await r.json()
        except Exception as e:
            logger.error(f"[OKX] {path} error: {e}")
            return None

    async def discover_symbol(self, symbol: str) -> bool:
        """v5 instruments → tickSz/lotSz/minSz/ctVal 캐시."""
        data = await self._get(
            "/api/v5/public/instruments",
            {"instType": INST_TYPE, "instId": symbol},
        )
        if not isinstance(data, dict) or data.get("code") not in ("0", 0):
            logger.warning(f"[OKX] {symbol} instruments error: {data}")
            return False
        items = data.get("data") or []
        if not items:
            return False
        item = items[0]
        try:
            self.set_symbol_meta(
                symbol,
                tick_size=float(item.get("tickSz", 0.01) or 0.01),
                lot_size=float(item.get("lotSz", 1) or 1),
                min_size=float(item.get("minSz", 1) or 1),
                ct_val=float(item.get("ctVal", 1) or 1),
            )
        except (TypeError, ValueError):
            self.set_symbol_meta(symbol)
        meta = self._symbol_meta[symbol]
        logger.info(
            f"[OKX] {symbol} meta: tick={meta['tick_size']} lot={meta['lot_size']} "
            f"min={meta['min_size']} ctVal={meta['ct_val']}"
        )
        return True

    # ── ExchangeBase ──

    async def get_quote(self, symbol: str) -> Optional[_base.Quote]:
        """ticker + mark-price + funding-rate 1회 조합."""
        t_task = self._get("/api/v5/market/ticker", {"instId": symbol})
        m_task = self._get("/api/v5/public/mark-price",
                            {"instType": INST_TYPE, "instId": symbol})
        f_task = self._get("/api/v5/public/funding-rate", {"instId": symbol})
        ticker, mark, funding = await asyncio.gather(t_task, m_task, f_task)
        return self._build_quote(symbol, ticker, mark, funding)

    async def subscribe_quotes(
        self,
        symbol: str,
        callback: _base.QuoteCallback,
        *,
        contract_size: float = 1.0,
    ) -> None:
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
            logger.warning("[OKX] websockets not installed; WS disabled")
            return
        backoff = 2
        sub_msg = json.dumps({
            "op": "subscribe",
            "args": [
                {"channel": "books5", "instId": symbol},
                {"channel": "mark-price", "instId": symbol},
                {"channel": "funding-rate", "instId": symbol},
            ],
        })
        while not self._stopped:
            try:
                connect_fn = self._ws_connect or websockets.connect
                async with connect_fn(self.ws_url, ping_interval=25) as ws:
                    backoff = 2
                    logger.info(f"[OKX] WS connected {symbol}")
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
                logger.error(f"[OKX] WS {symbol} error: {e}; reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_ws_msg(self, symbol: str, msg: dict) -> None:
        """v5 public stream 메시지 분기.

        형식:
          {"arg":{"channel":"books5","instId":"CL-USDT-SWAP"},
           "data":[{"asks":[["80.20","12","0","1"]],
                    "bids":[["80.10","10","0","1"]], ...}]}
          {"arg":{"channel":"mark-price",...},
           "data":[{"markPx":"80.15", ...}]}
          {"arg":{"channel":"funding-rate",...},
           "data":[{"fundingRate":"0.0001","nextFundingTime":"1700000000000",...}]}
          {"event":"subscribe",...}    — 구독 ack 무시
          {"event":"error",...}        — 구독 실패
        """
        if msg.get("event"):
            if msg.get("event") == "error":
                logger.error(f"[OKX] subscribe error: {msg}")
            return
        arg = msg.get("arg") or {}
        channel = arg.get("channel", "")
        data = msg.get("data") or []
        if not data:
            return
        first = data[0] if isinstance(data, list) else data
        if channel == "books5":
            await self._on_books5(symbol, first)
        elif channel == "mark-price":
            self._on_mark(symbol, first)
        elif channel == "funding-rate":
            self._on_funding(symbol, first)

    async def _on_books5(self, symbol: str, data: dict) -> None:
        bids = data.get("bids") or []
        asks = data.get("asks") or []
        cache = self._latest_book.setdefault(symbol, {})
        try:
            if bids:
                cache["bid"] = float(bids[0][0])
                cache["bid_qty"] = float(bids[0][1])
            if asks:
                cache["ask"] = float(asks[0][0])
                cache["ask_qty"] = float(asks[0][1])
        except (TypeError, ValueError, IndexError):
            return
        await self._emit(symbol)

    def _on_mark(self, symbol: str, data: dict) -> None:
        try:
            self._latest_mark[symbol] = float(data.get("markPx", 0) or 0)
        except (TypeError, ValueError):
            pass

    def _on_funding(self, symbol: str, data: dict) -> None:
        try:
            self._latest_funding[symbol] = float(data.get("fundingRate", 0) or 0)
        except (TypeError, ValueError):
            pass

    async def _emit(self, symbol: str) -> None:
        book = self._latest_book.get(symbol)
        if not book:
            return
        bid = book.get("bid", 0.0); ask = book.get("ask", 0.0)
        bid_qty = book.get("bid_qty", 0.0); ask_qty = book.get("ask_qty", 0.0)
        mark = self._latest_mark.get(symbol, 0.0)
        funding = self._latest_funding.get(symbol, 0.0)
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else mark
        quote = _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=mid,
            bid=bid, ask=ask,
            bid_qty=bid_qty, ask_qty=ask_qty,
            funding_rate=funding,
            funding_interval_hours=4.0,    # 사용자 정정: OKX CL-USDT-SWAP 4h
        )
        for cb in self._symbol_callbacks.get(symbol, []):
            try:
                result = cb(quote)
                if result is not None and asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception as e:
                logger.error(f"[OKX] cb error [{symbol}]: {e}")

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
            "OKXExchange.place_order is paper-only stage. "
            "Live order placement lands in Phase I (HMAC-SHA256 3-header signed)."
        )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        raise NotImplementedError("OKXExchange.cancel_order: Phase I live only.")

    async def get_positions(self) -> list[_base.Position]:
        return []

    async def get_account_value(self) -> float:
        # Phase G scaffolding — live AccountV5 미구현. polling skip.
        raise NotImplementedError(
            "OKXExchange.get_account_value is paper-only stage. "
            "Live AccountApi balance lands in Phase I."
        )

    async def get_funding_info(self, symbol: str) -> Optional[_base.FundingInfo]:
        """funding-rate + funding-rate-history로 interval 추정."""
        cur = await self._get("/api/v5/public/funding-rate", {"instId": symbol})
        current_rate = 0.0
        next_ts = 0.0
        if isinstance(cur, dict) and cur.get("code") in ("0", 0):
            items = cur.get("data") or []
            if items:
                d0 = items[0]
                try:
                    current_rate = float(d0.get("fundingRate", 0) or 0)
                    next_ts = float(d0.get("nextFundingTime", 0) or 0) / 1000.0
                except (TypeError, ValueError):
                    pass
        # interval 추정
        history = await self._get(
            "/api/v5/public/funding-rate-history",
            {"instId": symbol, "limit": "6"},
        )
        observed = 4.0
        if isinstance(history, dict) and history.get("code") in ("0", 0):
            items = history.get("data") or []
            if len(items) >= 2:
                try:
                    ts_s = sorted(float(it.get("fundingTime", 0)) / 1000.0
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

    # ── 내부: REST 응답 → Quote ──

    def _build_quote(
        self, symbol: str,
        ticker: Optional[dict], mark: Optional[dict], funding: Optional[dict],
    ) -> _base.Quote:
        try:
            t0 = (ticker or {}).get("data") or [{}]
            t = t0[0] if t0 else {}
            bid = float(t.get("bidPx", 0) or 0)
            ask = float(t.get("askPx", 0) or 0)
            bid_qty = float(t.get("bidSz", 0) or 0)
            ask_qty = float(t.get("askSz", 0) or 0)
        except (TypeError, ValueError, IndexError):
            bid = ask = bid_qty = ask_qty = 0.0
        try:
            m0 = (mark or {}).get("data") or [{}]
            mark_px = float((m0[0] if m0 else {}).get("markPx", 0) or 0)
        except (TypeError, ValueError, IndexError):
            mark_px = 0.0
        try:
            f0 = (funding or {}).get("data") or [{}]
            funding_rate = float((f0[0] if f0 else {}).get("fundingRate", 0) or 0)
        except (TypeError, ValueError, IndexError):
            funding_rate = 0.0
        mid = (bid + ask) / 2 if (bid > 0 and ask > 0) else mark_px
        return _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=mid,
            bid=bid, ask=ask,
            bid_qty=bid_qty, ask_qty=ask_qty,
            funding_rate=funding_rate,
            funding_interval_hours=4.0,
        )
