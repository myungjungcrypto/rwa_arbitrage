from __future__ import annotations
"""Hyperliquid (trade.xyz) API 래퍼.

trade.xyz 퍼페추얼 선물 시세 조회, 주문, WebSocket 실시간 데이터를 위한 래퍼.
trade.xyz 상품(WTIOIL, BRENTOIL 등)은 HIP-3 builder-deployed 퍼프로,
네이티브 퍼프와 별도의 perp DEX("xyz")에 존재.

API 차이점:
- 네이티브 퍼프: metaAndAssetCtxs → universe에서 조회
- HIP-3 퍼프: perpsMetaAndAssetCtxs (perpDexes 파라미터 필요)
  또는 개별 조회: l2Book, allMids 등에서 coin="WTIOIL" 직접 사용 가능
"""


import asyncio
import json
import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum

try:
    import aiohttp
except ImportError:
    aiohttp = None  # type: ignore

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore

logger = logging.getLogger("arbitrage.hyperliquid")


# ──────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────

MAINNET_API_URL = "https://api.hyperliquid.xyz"
TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"
MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

# trade.xyz HIP-3 perp DEX 이름
TRADE_XYZ_PERP_DEX = "xyz"


class OrderSide(str, Enum):
    BUY = "B"
    SELL = "A"


class OrderType(str, Enum):
    LIMIT = "Limit"
    MARKET = "Market"


# ──────────────────────────────────────────────
# Data Models
# ──────────────────────────────────────────────

@dataclass
class MarketData:
    """퍼페추얼 시세 데이터."""
    ticker: str
    mark_price: float
    index_price: float          # 오라클 인덱스 (CME 근월물 기반)
    funding_rate: float         # 현재 시간 펀딩레이트
    predicted_funding_rate: float
    open_interest: float
    volume_24h: float
    timestamp: float = field(default_factory=time.time)

    @property
    def basis_bps(self) -> float:
        """베이시스 (bp). mark - index 기준."""
        if self.index_price == 0:
            return 0.0
        return (self.mark_price - self.index_price) / self.index_price * 10_000


@dataclass
class OrderBookLevel:
    price: float
    size: float


@dataclass
class OrderBook:
    ticker: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    timestamp: float = field(default_factory=time.time)

    @property
    def best_bid(self) -> float:
        return self.bids[0].price if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return self.asks[0].price if self.asks else 0.0

    @property
    def mid_price(self) -> float:
        if self.best_bid and self.best_ask:
            return (self.best_bid + self.best_ask) / 2
        return 0.0

    @property
    def spread_bps(self) -> float:
        if self.mid_price == 0:
            return 0.0
        return (self.best_ask - self.best_bid) / self.mid_price * 10_000


@dataclass
class Position:
    ticker: str
    size: float             # 양수 = long, 음수 = short
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    margin_used: float
    leverage: float


@dataclass
class OrderResult:
    success: bool
    order_id: str = ""
    filled_size: float = 0.0
    filled_price: float = 0.0
    error: str = ""


# ──────────────────────────────────────────────
# REST API Client
# ──────────────────────────────────────────────

class HyperliquidClient:
    """Hyperliquid REST API 클라이언트.

    trade.xyz HIP-3 퍼프(WTIOIL, BRENTOIL)를 포함한 시세 조회, 주문, 포지션 관리.

    NOTE: trade.xyz 상품은 메인넷에만 존재합니다 (테스트넷 X).
          use_testnet=False로 설정하세요.
    """

    def __init__(
        self,
        use_testnet: bool = False,
        wallet_address: str = "",
        private_key: str = "",
        perp_dex: str = TRADE_XYZ_PERP_DEX,
    ):
        self.base_url = TESTNET_API_URL if use_testnet else MAINNET_API_URL
        self.wallet_address = wallet_address
        self.private_key = private_key
        self.perp_dex = perp_dex  # HIP-3 DEX 이름 ("xyz" for trade.xyz)
        self._session: aiohttp.ClientSession | None = None
        # 메타 캐시 (5초 TTL)
        self._meta_cache: tuple[list[dict], list[dict]] | None = None
        self._meta_cache_ts: float = 0
        self._meta_cache_ttl: float = 5.0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _post(self, endpoint: str, payload: dict) -> Any:
        """POST 요청."""
        session = await self._get_session()
        url = f"{self.base_url}{endpoint}"
        try:
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error(f"API error {resp.status} for {payload.get('type','?')}: {text[:200]}")
                    return {"error": text}
                return await resp.json()
        except Exception as e:
            logger.error(f"Request failed ({payload.get('type','?')}): {e}")
            return {"error": str(e)}

    # ── HIP-3 Perp DEX 전용 시세 조회 ──

    async def get_perps_meta_and_ctxs(self) -> tuple[list[dict], list[dict]]:
        """HIP-3 perp DEX의 메타 + 시세 컨텍스트.

        metaAndAssetCtxs에 dex 파라미터를 전달하여 HIP-3 퍼프 조회.
        dex="" → 네이티브 퍼프, dex="xyz" → trade.xyz HIP-3 퍼프.

        Returns:
            (universe, assetCtxs) 튜플.
            universe: [{name, szDecimals, maxLeverage, ...}, ...]
            assetCtxs: [{markPx, oraclePx, funding, ...}, ...]
        """
        # 캐시 확인
        now = time.time()
        if self._meta_cache and (now - self._meta_cache_ts) < self._meta_cache_ttl:
            return self._meta_cache

        payload: dict[str, Any] = {"type": "metaAndAssetCtxs"}
        if self.perp_dex:
            payload["dex"] = self.perp_dex

        data = await self._post("/info", payload)

        # 응답 형식: [{universe: [...]}, [{markPx, ...}, ...]]
        if isinstance(data, list) and len(data) == 2:
            meta = data[0]
            ctxs = data[1]

            universe = meta.get("universe", []) if isinstance(meta, dict) else []
            asset_ctxs = ctxs if isinstance(ctxs, list) else []

            if universe:
                logger.debug(
                    f"Loaded {len(universe)} assets from '{self.perp_dex}' perp DEX"
                )
            else:
                logger.warning(f"No assets found in '{self.perp_dex}' perp DEX")

            self._meta_cache = (universe, asset_ctxs)
            self._meta_cache_ts = now
            return universe, asset_ctxs

        logger.warning(f"Unexpected metaAndAssetCtxs response: {type(data)} — {str(data)[:200]}")
        return [], []

    async def get_market_data(self, ticker: str) -> MarketData | None:
        """특정 HIP-3 상품의 시세 데이터 조회.

        Args:
            ticker: 상품 티커 (예: "WTIOIL", "BRENTOIL")

        Returns:
            MarketData 객체 또는 None
        """
        universe, ctxs = await self.get_perps_meta_and_ctxs()

        for i, asset in enumerate(universe):
            if asset.get("name") == ticker and i < len(ctxs):
                ctx = ctxs[i]
                return MarketData(
                    ticker=ticker,
                    mark_price=float(ctx.get("markPx", 0)),
                    index_price=float(ctx.get("oraclePx", 0)),
                    funding_rate=float(ctx.get("funding", 0)),
                    predicted_funding_rate=float(ctx.get("predictedFunding", 0)),
                    open_interest=float(ctx.get("openInterest", 0)),
                    volume_24h=float(ctx.get("dayNtlVlm", 0)),
                )

        logger.warning(f"Ticker {ticker} not found in {self.perp_dex} perp DEX universe")
        return None

    async def get_all_market_data(self) -> dict[str, MarketData]:
        """HIP-3 DEX의 전체 상품 시세 조회.

        Returns:
            {ticker: MarketData} 딕셔너리
        """
        universe, ctxs = await self.get_perps_meta_and_ctxs()
        result = {}

        for i, asset in enumerate(universe):
            if i < len(ctxs):
                name = asset.get("name", "")
                ctx = ctxs[i]
                result[name] = MarketData(
                    ticker=name,
                    mark_price=float(ctx.get("markPx", 0)),
                    index_price=float(ctx.get("oraclePx", 0)),
                    funding_rate=float(ctx.get("funding", 0)),
                    predicted_funding_rate=float(ctx.get("predictedFunding", 0)),
                    open_interest=float(ctx.get("openInterest", 0)),
                    volume_24h=float(ctx.get("dayNtlVlm", 0)),
                )

        return result

    async def list_available_tickers(self) -> list[str]:
        """HIP-3 DEX에서 거래 가능한 전체 티커 목록."""
        universe, _ = await self.get_perps_meta_and_ctxs()
        return [asset.get("name", "") for asset in universe]

    # ── 범용 시세 조회 (네이티브 + HIP-3 공용) ──

    async def get_all_mids(self) -> dict[str, float]:
        """모든 상품의 mid price 조회 (네이티브 + HIP-3 통합)."""
        data = await self._post("/info", {"type": "allMids"})
        if isinstance(data, dict) and "error" not in data:
            return {k: float(v) for k, v in data.items()}
        return {}

    async def get_orderbook(self, ticker: str, depth: int = 20) -> OrderBook | None:
        """오더북 조회 (HIP-3 coin도 직접 사용 가능).

        Args:
            ticker: 상품 티커 (예: "WTIOIL")
            depth: 호가 깊이
        """
        data = await self._post("/info", {
            "type": "l2Book",
            "coin": ticker,
            "nSigFigs": 5,
        })
        if isinstance(data, dict) and "error" in data:
            return None

        levels = data.get("levels", [[], []])
        bids = [
            OrderBookLevel(price=float(b["px"]), size=float(b["sz"]))
            for b in levels[0][:depth]
        ]
        asks = [
            OrderBookLevel(price=float(a["px"]), size=float(a["sz"]))
            for a in levels[1][:depth]
        ]
        return OrderBook(ticker=ticker, bids=bids, asks=asks)

    async def get_funding_history(
        self, ticker: str, start_time: int, end_time: int | None = None
    ) -> list[dict]:
        """펀딩레이트 히스토리 조회.

        Args:
            ticker: 상품 티커 (HIP-3 coin도 가능)
            start_time: 시작 시간 (ms timestamp)
            end_time: 종료 시간 (ms timestamp, None이면 현재)
        """
        payload: dict[str, Any] = {
            "type": "fundingHistory",
            "coin": ticker,
            "startTime": start_time,
        }
        if end_time:
            payload["endTime"] = end_time
        data = await self._post("/info", payload)
        if isinstance(data, list):
            return data
        return []

    # ── 계정/포지션 ──

    async def get_user_state(self) -> dict:
        """유저 상태 (잔고, 포지션 등).

        HIP-3 퍼프 포지션도 포함.
        """
        if not self.wallet_address:
            logger.error("Wallet address not set")
            return {}
        data = await self._post("/info", {
            "type": "clearinghouseState",
            "user": self.wallet_address,
        })
        return data if isinstance(data, dict) else {}

    async def get_positions(self) -> list[Position]:
        """현재 포지션 목록 (HIP-3 포함)."""
        state = await self.get_user_state()
        positions = []
        for pos_info in state.get("assetPositions", []):
            pos = pos_info.get("position", {})
            if float(pos.get("szi", 0)) != 0:
                size = float(pos.get("szi", 0))
                positions.append(Position(
                    ticker=pos.get("coin", ""),
                    size=size,
                    entry_price=float(pos.get("entryPx", 0)),
                    mark_price=float(pos.get("positionValue", 0)) / abs(size) if size else 0,
                    unrealized_pnl=float(pos.get("unrealizedPnl", 0)),
                    margin_used=float(pos.get("marginUsed", 0)),
                    leverage=float(pos.get("leverage", {}).get("value", 1)),
                ))
        return positions

    async def get_account_value(self) -> float:
        """총 계좌 가치 (USDC)."""
        state = await self.get_user_state()
        return float(state.get("marginSummary", {}).get("accountValue", 0))

    # ── 주문 ──

    async def place_order(
        self,
        ticker: str,
        side: OrderSide,
        size: float,
        price: float | None = None,
        reduce_only: bool = False,
    ) -> OrderResult:
        """주문 생성.

        Args:
            ticker: 상품 티커 (HIP-3: "WTIOIL" 등)
            side: BUY or SELL
            size: 수량
            price: 지정가 (None이면 IOC 시장가)
            reduce_only: 포지션 감소만 허용

        Returns:
            OrderResult
        """
        if not self.private_key:
            return OrderResult(success=False, error="Private key not set")

        try:
            from hyperliquid.exchange import Exchange
            from hyperliquid.utils import constants

            base_url = constants.TESTNET_API_URL if "testnet" in self.base_url else constants.MAINNET_API_URL
            exchange = Exchange(
                wallet=None,  # 실제로는 eth_account 객체
                base_url=base_url,
            )

            is_buy = side == OrderSide.BUY
            order_type = {"limit": {"tif": "Gtc"}} if price else {"limit": {"tif": "Ioc"}}

            result = exchange.order(
                coin=ticker,
                is_buy=is_buy,
                sz=size,
                limit_px=price or 0,
                order_type=order_type,
                reduce_only=reduce_only,
            )

            if result.get("status") == "ok":
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses:
                    filled = statuses[0].get("filled", {})
                    return OrderResult(
                        success=True,
                        order_id=str(filled.get("oid", "")),
                        filled_size=float(filled.get("totalSz", 0)),
                        filled_price=float(filled.get("avgPx", 0)),
                    )
            return OrderResult(success=False, error=str(result))

        except ImportError:
            logger.warning("hyperliquid SDK not installed, using raw API")
            return OrderResult(success=False, error="SDK required for order placement")
        except Exception as e:
            return OrderResult(success=False, error=str(e))

    async def cancel_order(self, ticker: str, order_id: int) -> bool:
        """주문 취소."""
        try:
            from hyperliquid.exchange import Exchange
            from hyperliquid.utils import constants

            base_url = constants.TESTNET_API_URL if "testnet" in self.base_url else constants.MAINNET_API_URL
            exchange = Exchange(wallet=None, base_url=base_url)
            result = exchange.cancel(coin=ticker, oid=order_id)
            return result.get("status") == "ok"
        except Exception as e:
            logger.error(f"Cancel order failed: {e}")
            return False


# ──────────────────────────────────────────────
# WebSocket Client
# ──────────────────────────────────────────────

class HyperliquidWebSocket:
    """Hyperliquid WebSocket 클라이언트.

    실시간 시세, 오더북, 트레이드 스트림 수신.
    HIP-3 coin도 동일한 WebSocket으로 구독 가능.

    NOTE: 메인넷 WebSocket 사용 필요 (trade.xyz 상품은 메인넷에만 존재).
    """

    def __init__(
        self,
        use_testnet: bool = False,
        reconnect_delay: int = 5,
        ping_interval: int = 30,
    ):
        self.ws_url = TESTNET_WS_URL if use_testnet else MAINNET_WS_URL
        self.reconnect_delay = reconnect_delay
        self.ping_interval = ping_interval
        self._ws = None
        self._running = False
        self._callbacks: dict[str, list[Callable]] = {}
        self._subscriptions: list[dict] = []

    def on_market_data(self, callback: Callable[[MarketData], None]):
        """시세 업데이트 콜백 등록."""
        self._callbacks.setdefault("market_data", []).append(callback)

    def on_orderbook(self, callback: Callable[[OrderBook], None]):
        """오더북 업데이트 콜백 등록."""
        self._callbacks.setdefault("orderbook", []).append(callback)

    def on_trade(self, callback: Callable[[dict], None]):
        """체결 데이터 콜백 등록."""
        self._callbacks.setdefault("trade", []).append(callback)

    def on_funding(self, callback: Callable[[dict], None]):
        """펀딩레이트 업데이트 콜백 등록."""
        self._callbacks.setdefault("funding", []).append(callback)

    async def subscribe_market(self, ticker: str):
        """시세 + 오더북 구독 (HIP-3 coin도 동일)."""
        self._subscriptions.append({
            "method": "subscribe",
            "subscription": {"type": "l2Book", "coin": ticker},
        })
        self._subscriptions.append({
            "method": "subscribe",
            "subscription": {"type": "trades", "coin": ticker},
        })
        if self._ws:
            for sub in self._subscriptions[-2:]:
                await self._ws.send(json.dumps(sub))

    async def start(self):
        """WebSocket 연결 시작 + 메시지 수신 루프."""
        self._running = True
        while self._running:
            try:
                logger.info(f"Connecting to {self.ws_url}...")
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=self.ping_interval,
                ) as ws:
                    self._ws = ws
                    logger.info("WebSocket connected")

                    # 기존 구독 복원
                    for sub in self._subscriptions:
                        await ws.send(json.dumps(sub))

                    async for raw_msg in ws:
                        try:
                            msg = json.loads(raw_msg)
                            await self._handle_message(msg)
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid JSON: {raw_msg[:100]}")

            except Exception as e:
                if "ConnectionClosed" in type(e).__name__:
                    logger.warning(f"WebSocket closed: {e}")
                else:
                    logger.error(f"WebSocket error: {e}")

            if self._running:
                logger.info(f"Reconnecting in {self.reconnect_delay}s...")
                await asyncio.sleep(self.reconnect_delay)

    async def stop(self):
        """WebSocket 연결 종료."""
        self._running = False
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _handle_message(self, msg: dict):
        """수신 메시지 파싱 및 콜백 호출."""
        channel = msg.get("channel")
        data = msg.get("data")

        if channel == "l2Book":
            await self._handle_orderbook(data)
        elif channel == "trades":
            await self._handle_trades(data)
        elif channel == "subscriptionResponse":
            logger.debug(f"Subscription confirmed: {data}")

    async def _handle_orderbook(self, data: dict):
        if not data:
            return
        coin = data.get("coin", "")
        levels = data.get("levels", [[], []])
        ob = OrderBook(
            ticker=coin,
            bids=[OrderBookLevel(float(b["px"]), float(b["sz"])) for b in levels[0]],
            asks=[OrderBookLevel(float(a["px"]), float(a["sz"])) for a in levels[1]],
        )
        for cb in self._callbacks.get("orderbook", []):
            try:
                cb(ob)
            except Exception as e:
                logger.error(f"Orderbook callback error: {e}")

    async def _handle_trades(self, data: list):
        if not data:
            return
        for trade in data:
            for cb in self._callbacks.get("trade", []):
                try:
                    cb(trade)
                except Exception as e:
                    logger.error(f"Trade callback error: {e}")


# ──────────────────────────────────────────────
# ExchangeBase Adapter (Phase A 스캐폴딩)
# ──────────────────────────────────────────────
#
# 기존 HyperliquidClient + HyperliquidWebSocket을 감싸 ExchangeBase protocol을
# 구현하는 어댑터. main.py와 collector가 거래소 종류와 무관하게 동일 API로
# Hyperliquid를 사용할 수 있게 한다. 기존 클래스는 무수정 유지.

from src.exchange import base as _base   # noqa: E402


class HyperliquidExchange:
    """ExchangeBase 어댑터 — HyperliquidClient + HyperliquidWebSocket 래퍼.

    Phase A에서는 quote 수신 + 단순 정보 조회 메서드만 충실. 주문 메서드는
    기존 HyperliquidClient.place_order에 위임 (주문 시그니처 변환).
    """

    name = "hyperliquid"
    venue_type = _base.VenueType.PERP.value
    margin_asset = "USDC"

    def __init__(self, rest: HyperliquidClient, ws: HyperliquidWebSocket):
        self._rest = rest
        self._ws = ws
        # symbol → callback (외부에서 등록한 ExchangeBase 콜백)
        self._symbol_callbacks: dict[str, list[_base.QuoteCallback]] = {}
        # symbol별 funding/index 캐시 (orderbook 콜백 시 합쳐서 Quote 생성)
        self._latest_meta: dict[str, MarketData] = {}
        self._ws_callback_registered = False

    async def connect(self) -> bool:
        # WS는 main.py의 별도 task에서 start() 호출 — 여기서는 콜백 훅만 부착
        if not self._ws_callback_registered:
            self._ws.on_orderbook(self._on_orderbook)
            self._ws_callback_registered = True
        return True

    async def disconnect(self) -> None:
        await self._rest.close()
        await self._ws.stop()

    async def subscribe_quotes(
        self,
        symbol: str,
        callback: _base.QuoteCallback,
        *,
        contract_size: float = 1.0,
    ) -> None:
        self._symbol_callbacks.setdefault(symbol, []).append(callback)
        await self._ws.subscribe_market(symbol)

    async def unsubscribe_quotes(self, symbol: str) -> None:
        self._symbol_callbacks.pop(symbol, None)
        # HyperliquidWebSocket은 명시적 unsubscribe 미구현 — 추후 확장

    async def get_quote(self, symbol: str) -> Optional[_base.Quote]:
        md = await self._rest.get_market_data(symbol)
        if md is None:
            return None
        ob = await self._rest.get_orderbook(symbol, depth=1)
        bid = ob.best_bid if ob else md.mark_price
        ask = ob.best_ask if ob else md.mark_price
        return _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=md.mark_price,
            bid=bid,
            ask=ask,
            index_price=md.index_price,
            funding_rate=md.funding_rate,
            funding_interval_hours=1.0,
            predicted_funding_rate=md.predicted_funding_rate,
            open_interest=md.open_interest,
            volume_24h=md.volume_24h,
            timestamp=md.timestamp,
        )

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
        rest_side = OrderSide.BUY if side == "buy" else OrderSide.SELL
        price_arg = limit_price if order_type == "limit" else None
        result = await self._rest.place_order(
            ticker=symbol,
            side=rest_side,
            size=size,
            price=price_arg,
            reduce_only=reduce_only,
        )
        return _base.OrderResult(
            success=result.success,
            exchange=self.name,
            symbol=symbol,
            order_id=result.order_id,
            filled_size=result.filled_size,
            filled_price=result.filled_price,
            error=result.error,
        )

    async def cancel_order(self, symbol: str, order_id: str) -> bool:
        try:
            return await self._rest.cancel_order(symbol, int(order_id))
        except (ValueError, TypeError):
            logger.error(f"HL cancel_order: invalid order_id {order_id!r}")
            return False

    async def get_positions(self) -> list[_base.Position]:
        positions = await self._rest.get_positions()
        return [
            _base.Position(
                exchange=self.name,
                symbol=p.ticker,
                size=p.size,
                entry_price=p.entry_price,
                mark_price=p.mark_price,
                unrealized_pnl=p.unrealized_pnl,
                margin_used=p.margin_used,
                leverage=p.leverage,
            )
            for p in positions
        ]

    async def get_account_value(self) -> float:
        return await self._rest.get_account_value()

    # ── 내부 콜백 ──

    def _on_orderbook(self, ob: OrderBook) -> None:
        """WS 오더북 업데이트 → 등록된 ExchangeBase 콜백으로 Quote fan-out.

        funding/index는 별도 REST 폴링이 필요. Phase A에서는 ob 정보만
        Quote에 채워 보내고, funding 필드는 0으로 둔다 (Phase D-E에서
        funding 캐시 합류 시점에 채움).
        """
        symbol = ob.ticker
        callbacks = self._symbol_callbacks.get(symbol)
        if not callbacks:
            return
        meta = self._latest_meta.get(symbol)
        quote = _base.Quote(
            exchange=self.name,
            symbol=symbol,
            mid_price=ob.mid_price,
            bid=ob.best_bid,
            ask=ob.best_ask,
            bid_qty=ob.bids[0].size if ob.bids else 0.0,
            ask_qty=ob.asks[0].size if ob.asks else 0.0,
            index_price=meta.index_price if meta else 0.0,
            funding_rate=meta.funding_rate if meta else 0.0,
            funding_interval_hours=1.0,
            predicted_funding_rate=meta.predicted_funding_rate if meta else 0.0,
            open_interest=meta.open_interest if meta else 0.0,
            volume_24h=meta.volume_24h if meta else 0.0,
            timestamp=ob.timestamp,
        )
        for cb in callbacks:
            try:
                result = cb(quote)
                if result is not None and asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception as e:
                logger.error(f"HL ExchangeBase callback error [{symbol}]: {e}")

    def update_meta(self, symbol: str, market_data: MarketData) -> None:
        """REST 폴링에서 받은 funding/index 정보 캐시 갱신.

        main.py의 funding poll task가 주기적으로 호출하면 다음 ob 콜백에 반영.
        """
        self._latest_meta[symbol] = market_data
