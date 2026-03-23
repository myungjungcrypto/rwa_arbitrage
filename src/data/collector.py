"""실시간 데이터 수집기.

Hyperliquid WebSocket + 키움 시세를 통합하여
베이시스 계산, DB 저장, 콜백 처리를 담당.
"""

from __future__ import annotations

import asyncio
import time
import logging
from typing import Callable, Optional

from src.exchange.hyperliquid import MarketData, OrderBook

# Lazy imports for classes that need aiohttp/websockets
def _get_hl_client():
    from src.exchange.hyperliquid import HyperliquidClient
    return HyperliquidClient

def _get_hl_ws():
    from src.exchange.hyperliquid import HyperliquidWebSocket
    return HyperliquidWebSocket
from src.data.storage import Storage
from src.utils.config import AppConfig, ProductConfig

logger = logging.getLogger("arbitrage.collector")


class DataCollector:
    """통합 데이터 수집기.

    Hyperliquid 퍼프 시세 + 키움 월물 시세를 수집하고,
    베이시스를 계산하여 DB에 저장.
    """

    def __init__(self, config: AppConfig, storage: Storage):
        self.config = config
        self.storage = storage

        # Hyperliquid 클라이언트
        HLClient = _get_hl_client()
        HLWS = _get_hl_ws()
        self.hl_client = HLClient(
            use_testnet=config.hyperliquid.use_testnet,
            wallet_address=config.hyperliquid.wallet_address,
            private_key=config.hyperliquid.private_key,
            perp_dex=config.hyperliquid.perp_dex,
        )
        self.hl_ws = HLWS(
            use_testnet=config.hyperliquid.use_testnet,
            reconnect_delay=config.hyperliquid.ws_reconnect_delay,
            ping_interval=config.hyperliquid.ws_ping_interval,
        )

        # 최신 시세 캐시
        self._latest_perp: dict[str, MarketData] = {}
        self._latest_futures: dict[str, dict] = {}  # symbol -> {price, bid, ask, ...}
        self._latest_orderbook: dict[str, OrderBook] = {}

        # 콜백
        self._basis_callbacks: list[Callable] = []
        self._price_callbacks: list[Callable] = []

        # 폴링 인터벌 (초)
        self.poll_interval = 5
        self._running = False

    def on_basis_update(self, callback: Callable[[str, float, float, float], None]):
        """베이시스 업데이트 콜백 등록.

        callback(product, perp_price, futures_price, basis_bps)
        """
        self._basis_callbacks.append(callback)

    def on_price_update(self, callback: Callable[[str, MarketData], None]):
        """시세 업데이트 콜백 등록."""
        self._price_callbacks.append(callback)

    @property
    def latest_perp(self) -> dict[str, MarketData]:
        return self._latest_perp

    @property
    def latest_futures(self) -> dict[str, dict]:
        return self._latest_futures

    async def start(self):
        """데이터 수집 시작."""
        self._running = True
        logger.info("DataCollector starting...")

        # WebSocket 구독
        for name, product in self.config.products.items():
            await self.hl_ws.subscribe_market(product.perp_ticker)
            logger.info(f"Subscribed to {product.perp_ticker} WebSocket")

        # 콜백 등록
        self.hl_ws.on_orderbook(self._on_orderbook_update)
        self.hl_ws.on_trade(self._on_trade_update)

        # 병렬 태스크 실행
        tasks = [
            asyncio.create_task(self.hl_ws.start()),
            asyncio.create_task(self._poll_perp_data()),
            asyncio.create_task(self._poll_funding_data()),
        ]

        logger.info("DataCollector started")
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("DataCollector stopping...")
            await self.stop()

    async def stop(self):
        """데이터 수집 중지."""
        self._running = False
        await self.hl_ws.stop()
        await self.hl_client.close()
        logger.info("DataCollector stopped")

    async def _poll_perp_data(self):
        """주기적으로 퍼프 시세 폴링 (REST API fallback + 보조)."""
        while self._running:
            try:
                for name, product in self.config.products.items():
                    md = await self.hl_client.get_market_data(product.perp_ticker)
                    if md:
                        self._latest_perp[name] = md

                        # DB 저장
                        self.storage.save_perp_price(
                            ticker=md.ticker,
                            mark_price=md.mark_price,
                            index_price=md.index_price,
                            funding_rate=md.funding_rate,
                            predicted_funding=md.predicted_funding_rate,
                            open_interest=md.open_interest,
                            volume_24h=md.volume_24h,
                            ts=md.timestamp,
                        )

                        # 콜백 (on_price → update_futures_price → _compute_basis 순서로 호출됨)
                        for cb in self._price_callbacks:
                            try:
                                cb(name, md)
                            except Exception as e:
                                logger.error(f"Price callback error: {e}")

                        logger.debug(
                            f"{product.perp_ticker}: mark={md.mark_price:.2f} "
                            f"index={md.index_price:.2f} basis={md.basis_bps:.1f}bp "
                            f"funding={md.funding_rate:.6f}"
                        )

            except Exception as e:
                logger.error(f"Perp poll error: {e}")

            await asyncio.sleep(self.poll_interval)

    async def _poll_funding_data(self):
        """펀딩레이트 기록 (매시간)."""
        while self._running:
            try:
                for name, product in self.config.products.items():
                    md = self._latest_perp.get(name)
                    if md:
                        self.storage.save_funding(
                            ticker=md.ticker,
                            funding_rate=md.funding_rate,
                        )
                        logger.info(
                            f"Funding saved: {md.ticker} rate={md.funding_rate:.6f}"
                        )
            except Exception as e:
                logger.error(f"Funding poll error: {e}")

            # 매시간 체크 (but poll every 5분 for accuracy)
            await asyncio.sleep(300)

    def _on_orderbook_update(self, ob: OrderBook):
        """WebSocket 오더북 업데이트 핸들러."""
        # ticker -> product name 매핑
        for name, product in self.config.products.items():
            if product.perp_ticker == ob.ticker:
                self._latest_orderbook[name] = ob
                break

    def _on_trade_update(self, trade: dict):
        """WebSocket 체결 데이터 핸들러."""
        # 실시간 체결 로깅 (디버그용)
        logger.debug(f"Trade: {trade}")

    def _compute_basis(self, product_name: str, product: ProductConfig):
        """퍼프-월물 베이시스 계산 및 저장."""
        perp = self._latest_perp.get(product_name)
        futures = self._latest_futures.get(product_name)

        if not perp:
            return

        # 키움 월물 시세가 없으면 인덱스(오라클) 가격을 대용으로 사용
        futures_price = futures["price"] if futures else perp.index_price

        if futures_price <= 0:
            return

        basis_bps = (perp.mark_price - futures_price) / futures_price * 10_000

        self.storage.save_basis(
            product=product_name,
            perp_price=perp.mark_price,
            futures_price=futures_price,
            funding_rate=perp.funding_rate,
        )

        # 콜백
        for cb in self._basis_callbacks:
            try:
                cb(product_name, perp.mark_price, futures_price, basis_bps)
            except Exception as e:
                logger.error(f"Basis callback error: {e}")

    def update_futures_price(
        self,
        product_name: str,
        price: float,
        bid: float = 0,
        ask: float = 0,
        contract_month: str = "",
        volume: int = 0,
    ):
        """키움에서 받은 월물 시세 업데이트 (외부에서 호출).

        Args:
            product_name: 상품명 (wti / brent)
            price: 체결가
            bid: 매수호가
            ask: 매도호가
            contract_month: 계약 월 (예: "CLK6")
            volume: 거래량
        """
        self._latest_futures[product_name] = {
            "price": price,
            "bid": bid,
            "ask": ask,
            "contract_month": contract_month,
            "volume": volume,
            "ts": time.time(),
        }

        product = self.config.products.get(product_name)
        if product:
            self.storage.save_futures_price(
                symbol=product.futures_symbol,
                contract_month=contract_month,
                price=price,
                bid=bid,
                ask=ask,
                volume=volume,
            )
            # 베이시스 재계산
            self._compute_basis(product_name, product)
