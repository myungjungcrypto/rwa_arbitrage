from __future__ import annotations
"""실시간 데이터 수집기.

Hyperliquid WebSocket + 키움 시세를 통합하여
베이시스 계산, DB 저장, 콜백 처리를 담당.

Phase C2: pair-keyed 신규 경로 추가. 기존 product-keyed 경로는 무수정 유지
(레거시 봇 운영 호환). 신규 경로는 ExchangeBase 어댑터에서 들어오는 Quote를
`update_leg_quote(pair_id, leg, quote)`로 받아 leg_prices 테이블에 저장 +
양쪽 leg 모두 도착하면 basis 계산 → pair-aware 콜백 fan-out.
"""


import asyncio
import time
import logging
from typing import Awaitable, Callable, Optional

from src.exchange.base import Quote
from src.exchange.hyperliquid import MarketData, OrderBook
from src.strategy.pair import ArbitragePair, LegRole

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


# pair-aware basis callback signature:
#   cb(pair_id, basis_bps, leg_a_quote, leg_b_quote)
PairBasisCallback = Callable[[str, float, Quote, Quote], Optional[Awaitable[None]]]


class DataCollector:
    """통합 데이터 수집기.

    Hyperliquid 퍼프 시세 + 키움 월물 시세를 수집하고,
    베이시스를 계산하여 DB에 저장.

    레거시 product-keyed API와 신규 pair-keyed API를 동시에 제공.
    Phase C5에서 main.py가 pair-keyed로 switch할 때까지 둘 다 살아있음.
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

        # ── 레거시 product-keyed 경로 ──
        self._latest_perp: dict[str, MarketData] = {}
        self._latest_futures: dict[str, dict] = {}     # symbol -> {price, bid, ask, ...}
        self._latest_orderbook: dict[str, OrderBook] = {}
        self._basis_callbacks: list[Callable] = []
        self._price_callbacks: list[Callable] = []

        # ── Phase C2: pair-keyed 경로 ──
        self._pairs: dict[str, ArbitragePair] = {}                  # pair_id → pair
        self._latest_quote: dict[tuple[str, str], Quote] = {}       # (pair_id, leg) → Quote
        self._pair_callbacks: list[PairBasisCallback] = []

        # 폴링 인터벌 (초)
        self.poll_interval = 5
        self._running = False

    def on_basis_update(self, callback: Callable):
        """[Legacy] 베이시스 업데이트 콜백 등록.

        callback(product, perp_price, futures_price, basis_bps,
                 perp_best_bid, perp_best_ask, futures_bid, futures_ask)
        """
        self._basis_callbacks.append(callback)

    def on_price_update(self, callback: Callable[[str, MarketData], None]):
        """[Legacy] 시세 업데이트 콜백 등록."""
        self._price_callbacks.append(callback)

    @property
    def latest_perp(self) -> dict[str, MarketData]:
        return self._latest_perp

    @property
    def latest_futures(self) -> dict[str, dict]:
        return self._latest_futures

    # ──────────────────────────────────────────────
    # Phase C2: pair-keyed API (신규, 레거시와 병존)
    # ──────────────────────────────────────────────

    def register_pair(self, pair: ArbitragePair) -> None:
        """page-keyed 기반 추적 대상 페어 등록. pair_id 중복 시 덮어씀.

        ExchangeBase 어댑터들이 이 페어의 leg 심볼을 구독한 뒤
        `update_leg_quote(pair_id, leg, quote)`로 시세를 push.
        """
        self._pairs[pair.id] = pair
        logger.info(
            f"[PAIR] registered {pair.id}: "
            f"leg_a={pair.leg_a.exchange}/{pair.leg_a.symbol} ({pair.leg_a.role.value}), "
            f"leg_b={pair.leg_b.exchange}/{pair.leg_b.symbol} ({pair.leg_b.role.value})"
        )

    def get_pair(self, pair_id: str) -> Optional[ArbitragePair]:
        return self._pairs.get(pair_id)

    @property
    def registered_pairs(self) -> dict[str, ArbitragePair]:
        return dict(self._pairs)

    def on_pair_basis(self, callback: PairBasisCallback) -> None:
        """pair-keyed basis 콜백 등록.

        callback signature: (pair_id, basis_bps, leg_a_quote, leg_b_quote)
        Quote는 src.exchange.base.Quote 인스턴스. 양쪽 leg 모두 도착했을 때만
        호출됨. 호출자가 동기/비동기 모두 가능.
        """
        self._pair_callbacks.append(callback)

    def update_leg_quote(self, pair_id: str, leg: str, quote: Quote) -> None:
        """page-keyed 신규 경로의 메인 entry point.

        ExchangeBase 어댑터가 호출. 호출 흐름:
          1. (pair_id, leg) 키로 최신 Quote 캐시
          2. leg_prices 테이블에 저장 (실패해도 계속)
          3. 양쪽 leg 모두 캐시에 있으면 orderbook-mid 기반 basis 계산
          4. pair-aware 콜백 fan-out

        leg는 'a' | 'b' 중 하나.
        """
        if leg not in ("a", "b"):
            raise ValueError(f"leg must be 'a' or 'b', got {leg!r}")
        if pair_id not in self._pairs:
            logger.warning(f"[{pair_id}] update_leg_quote: pair not registered (ignored)")
            return

        self._latest_quote[(pair_id, leg)] = quote

        # leg_prices 테이블 INSERT (best-effort)
        try:
            self.storage.save_leg_quote(
                pair_id=pair_id, leg=leg,
                exchange=quote.exchange, symbol=quote.symbol,
                mid_price=quote.mid_price,
                bid=quote.bid, ask=quote.ask,
                bid_qty=quote.bid_qty, ask_qty=quote.ask_qty,
                index_price=quote.index_price,
                funding_rate=quote.funding_rate,
                funding_interval_hours=quote.funding_interval_hours,
                contract_month=quote.contract_month,
                volume_24h=quote.volume_24h,
                ts=quote.timestamp,
            )
        except Exception as e:
            logger.error(f"[{pair_id}/{leg}] leg_prices save failed: {e}")

        # 양쪽 leg 모두 있으면 basis 계산 + 콜백
        leg_a = self._latest_quote.get((pair_id, "a"))
        leg_b = self._latest_quote.get((pair_id, "b"))
        if leg_a is None or leg_b is None:
            return

        basis_bps = self._compute_pair_basis(leg_a, leg_b)
        if basis_bps is None:
            return

        # 콜백 fan-out
        for cb in self._pair_callbacks:
            try:
                result = cb(pair_id, basis_bps, leg_a, leg_b)
                if result is not None and asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception as e:
                logger.error(f"[{pair_id}] pair callback error: {e}")

    @staticmethod
    def _compute_pair_basis(leg_a: Quote, leg_b: Quote) -> Optional[float]:
        """양 leg의 orderbook-mid 기반 basis (bp). leg_b 가격이 0이면 None.

        호가 누락 시 mid_price fallback (mark/last 등 ExchangeBase가 채운 값).
        엔진은 별도 exec_filter로 진입 차단하므로 stats 흐름만 유지.
        """
        # orderbook mid 우선
        if leg_a.bid > 0 and leg_a.ask > 0 and leg_b.bid > 0 and leg_b.ask > 0:
            a_mid = (leg_a.bid + leg_a.ask) / 2
            b_mid = (leg_b.bid + leg_b.ask) / 2
        else:
            a_mid = leg_a.mid_price
            b_mid = leg_b.mid_price
        if b_mid <= 0:
            return None
        return (a_mid - b_mid) / b_mid * 10_000

    def latest_pair_quote(self, pair_id: str, leg: str) -> Optional[Quote]:
        """캐시된 최신 Quote 반환."""
        return self._latest_quote.get((pair_id, leg))

    def has_both_legs(self, pair_id: str) -> bool:
        return (pair_id, "a") in self._latest_quote and (pair_id, "b") in self._latest_quote

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
        """퍼프-월물 베이시스 계산 및 저장.

        mid 기준 basis (통계/DB용)와 executable basis (거래 판단용)을
        모두 계산하여 콜백에 전달.
        """
        perp = self._latest_perp.get(product_name)
        futures = self._latest_futures.get(product_name)

        if not perp:
            return

        # 키움 월물 시세가 없으면 인덱스(오라클) 가격을 대용으로 사용
        futures_price = futures["price"] if futures else perp.index_price
        # bid/ask는 실제 호가가 없으면 0 전달 — 엔진이 exec_filter로 진입 차단.
        # (fallback 금지: mid로 대체하면 exec_basis ≈ mid_basis가 되어 필터 우회)
        futures_bid = futures["bid"] if futures and futures["bid"] > 0 else 0.0
        futures_ask = futures["ask"] if futures and futures["ask"] > 0 else 0.0

        if futures_price <= 0:
            return

        # perp 오더북 bid/ask (WebSocket에서 수신) — 미수신이면 0 전달 (mid fallback 금지)
        ob = self._latest_orderbook.get(product_name)
        perp_best_bid = ob.best_bid if ob else 0.0
        perp_best_ask = ob.best_ask if ob else 0.0

        # basis 계산 — orderbook-mid 기반이 우선 (HL mark_price는 oracle 추적이라
        # HL 자체 orderbook과 ~20bp 괴리 가능. 신호의 "phantom basis" 원인).
        # 양쪽 다 호가 있으면 orderbook-mid, 아니면 mark-based (fallback — 신호는
        # exec_filter가 자동 차단하므로 stats 흐름 유지가 목적).
        if perp_best_bid > 0 and perp_best_ask > 0 and futures_bid > 0 and futures_ask > 0:
            perp_mid_ob = (perp_best_bid + perp_best_ask) / 2
            fut_mid_ob = (futures_bid + futures_ask) / 2
            basis_bps = (perp_mid_ob - fut_mid_ob) / fut_mid_ob * 10_000
            basis_perp_price = perp_mid_ob
            basis_fut_price = fut_mid_ob
        else:
            # mark-based fallback (호가 미수신 윈도우)
            basis_bps = (perp.mark_price - futures_price) / futures_price * 10_000
            basis_perp_price = perp.mark_price
            basis_fut_price = futures_price

        self.storage.save_basis(
            product=product_name,
            perp_price=basis_perp_price,
            futures_price=basis_fut_price,
            funding_rate=perp.funding_rate,
        )

        # 콜백 — futures bid/ask도 전달
        for cb in self._basis_callbacks:
            try:
                cb(product_name, perp.mark_price, futures_price, basis_bps,
                   perp_best_bid, perp_best_ask,
                   futures_bid, futures_ask)
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
