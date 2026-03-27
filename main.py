from __future__ import annotations
"""RWA Arbitrage Bot — 메인 엔트리포인트.

실행 모드:
    python main.py --mode collect   # 데이터 수집만
    python main.py --mode paper     # 페이퍼 트레이딩 (자동 매매 시뮬레이션)
    python main.py --mode live      # 실거래 (Phase 3)
"""


import asyncio
import argparse
import signal
import sys
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.config import load_config
from src.utils.logger import setup_logger, get_logger
from src.utils.notifier import TelegramNotifier
from src.data.storage import Storage
from src.data.collector import DataCollector
from src.exchange.kiwoom import create_kiwoom_client
from src.exchange.kis import KISAuth, KISFuturesClient


# ──────────────────────────────────────────────
# 공통 설정
# ──────────────────────────────────────────────

def _setup(config_path: str):
    """설정 로드 + 초기화."""
    config = load_config(config_path)
    logger = setup_logger(level=config.log_level, log_file=config.log_file)
    storage = Storage(config.db_path)
    storage.connect()
    kiwoom = create_kiwoom_client(use_mock=config.kiwoom.use_mock)
    kiwoom.connect()
    return config, logger, storage, kiwoom


def _register_collector_callbacks(collector, kiwoom, config):
    """데이터 수집기 기본 콜백 등록 (collect + paper 공용).

    KIS 활성화 시: perp 가격 콜백은 등록하되 futures 업데이트는 하지 않음
                  (KIS WebSocket이 독립적으로 futures 가격 공급)
    KIS 비활성화 시: 기존 방식 (Kiwoom mock이 index_price 기반 가짜 futures 생성)
    """
    if config.kis.enabled:
        # KIS 모드: Kiwoom mock에서 futures 가격을 주입하지 않음
        # KIS WebSocket이 별도로 collector.update_futures_price() 호출
        logger = get_logger()
        logger.info("KIS enabled — futures prices from KIS WebSocket (independent source)")
    else:
        # 기존 모드: Kiwoom mock이 index_price 기반으로 futures 가격 생성
        def on_price(product, md):
            kiwoom.set_base_price(
                config.products[product].futures_symbol,
                md.index_price,
            )
            quote = kiwoom.get_quote(config.products[product].futures_symbol)
            if quote:
                collector.update_futures_price(
                    product_name=product,
                    price=quote.price,
                    bid=quote.bid,
                    ask=quote.ask,
                    contract_month=config.products[product].futures_symbol,
                    volume=quote.volume,
                )

        collector.on_price_update(on_price)


async def _setup_kis(config, collector, kiwoom=None) -> KISFuturesClient | None:
    """KIS 클라이언트 초기화 + 실시간 구독.

    Returns:
        KISFuturesClient 또는 None (비활성화 시)
    """
    if not config.kis.enabled:
        return None

    logger = get_logger()

    auth = KISAuth(
        app_key=config.kis.app_key,
        app_secret=config.kis.app_secret,
        base_url=config.kis.base_url,
        is_paper=config.kis.is_paper,
    )

    client = KISFuturesClient(
        auth=auth,
        ws_url=config.kis.ws_url,
        is_paper=config.kis.is_paper,
    )

    connected = await client.connect()
    if not connected:
        logger.error("KIS connection failed — falling back to Kiwoom mock")
        return None

    # KIS 종목 매핑 로드
    settings_path = Path("config/settings.yaml")
    import yaml
    with open(settings_path) as f:
        raw = yaml.safe_load(f) or {}
    kis_map = raw.get("kis_symbol_map", {})

    # 종목별 구독
    for product_name, kis_symbol in kis_map.items():
        if product_name not in config.products:
            continue

        def make_callback(pname):
            def on_kis_quote(quote):
                collector.update_futures_price(
                    product_name=pname,
                    price=quote.price,
                    bid=quote.bid,
                    ask=quote.ask,
                    contract_month=quote.contract_month,
                    volume=quote.volume,
                )
                # Kiwoom mock에도 가격 주입 (paper trading 주문 시뮬레이션용)
                if kiwoom:
                    futures_symbol = config.products[pname].futures_symbol
                    kiwoom.set_base_price(
                        futures_symbol, quote.price,
                        bid=quote.bid, ask=quote.ask,
                    )
            return on_kis_quote

        # KIS는 계약총액 기준 호가 → contract_size로 나눠서 배럴당 가격으로 변환
        price_divisor = float(config.products[product_name].contract_size)
        await client.subscribe(kis_symbol, make_callback(product_name), price_divisor=price_divisor)
        logger.info(f"KIS subscribed: {product_name} → {kis_symbol} (price_divisor={price_divisor})")

    return client


# ──────────────────────────────────────────────
# 데이터 수집 모드
# ──────────────────────────────────────────────

async def run_collector(config_path: str = "config/settings.yaml"):
    """데이터 수집 모드 실행."""
    config, logger, storage, kiwoom = _setup(config_path)
    logger.info(f"Starting RWA Arbitrage Bot (mode: COLLECT)")
    logger.info(f"Products: {list(config.products.keys())}")

    collector = DataCollector(config, storage)

    # 콜백 등록
    _register_collector_callbacks(collector, kiwoom, config)

    # KIS 실시간 호가 (활성화 시)
    kis_client = await _setup_kis(config, collector, kiwoom)

    def on_basis(product, perp_price, futures_price, basis_bps,
                 perp_best_bid=0.0, perp_best_ask=0.0):
        logger.info(
            f"[{product.upper()}] "
            f"perp={perp_price:.2f} futures={futures_price:.2f} "
            f"basis={basis_bps:+.1f}bp"
        )

    collector.on_basis_update(on_basis)

    # 종료 핸들링
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    collect_task = asyncio.create_task(collector.start())

    # 상태 출력 루프
    async def status_loop():
        while not stop_event.is_set():
            await asyncio.sleep(30)
            for name in config.products:
                stats = storage.get_basis_stats(name, hours=1)
                if stats["count"] > 0:
                    logger.info(
                        f"[{name.upper()} 1h stats] "
                        f"mean={stats['mean']:.1f}bp "
                        f"std={stats['std']:.1f}bp "
                        f"range=[{stats['min']:.1f}, {stats['max']:.1f}]bp "
                        f"n={stats['count']}"
                    )

    status_task = asyncio.create_task(status_loop())

    await stop_event.wait()
    logger.info("Shutting down...")
    collect_task.cancel()
    status_task.cancel()
    try:
        await collect_task
    except asyncio.CancelledError:
        pass
    try:
        await status_task
    except asyncio.CancelledError:
        pass
    await collector.stop()
    if kis_client:
        await kis_client.disconnect()
    kiwoom.disconnect()
    storage.close()
    logger.info("Shutdown complete")


# ──────────────────────────────────────────────
# 페이퍼 트레이딩 모드
# ──────────────────────────────────────────────

async def run_paper(config_path: str = "config/settings.yaml"):
    """페이퍼 트레이딩 모드 실행.

    데이터 수집 + 시그널 생성 + 자동 주문 시뮬레이션.
    """
    config, logger, storage, kiwoom = _setup(config_path)
    logger.info(f"Starting RWA Arbitrage Bot (mode: PAPER TRADING)")
    logger.info(f"Products: {list(config.products.keys())}")

    # Telegram 알림 (설정에 있으면 활성화)
    notifier = TelegramNotifier(enabled=False)

    # 데이터 수집기
    collector = DataCollector(config, storage)
    _register_collector_callbacks(collector, kiwoom, config)

    # KIS 실시간 호가 (활성화 시)
    kis_client = await _setup_kis(config, collector, kiwoom)

    # 페이퍼 트레이딩 엔진
    from src.paper.engine import PaperTradingEngine
    engine = PaperTradingEngine(
        config=config,
        storage=storage,
        kiwoom=kiwoom,
    )

    # DB에서 최근 basis 데이터 부트스트랩 (재시작 시 window 즉시 복원)
    for product_name in config.products:
        history = storage.get_basis_history(product_name, hours=config.strategy.basis_window_hours)
        if history:
            engine.signal_gen.bootstrap_from_db(product_name, history)
        else:
            logger.info(f"[{product_name.upper()}] No basis history in DB — starting fresh")

    # ── 콜백 연결 ──

    # 1) 베이시스 업데이트 → 엔진에 전달
    def on_basis(product, perp_price, futures_price, basis_bps,
                 perp_best_bid=0.0, perp_best_ask=0.0):
        # 펀딩레이트 가져오기
        md = collector.latest_perp.get(product)
        funding_rate = md.funding_rate if md else 0.0

        engine.process_basis_update(
            product=product,
            perp_price=perp_price,
            futures_price=futures_price,
            basis_bps=basis_bps,
            funding_rate=funding_rate,
            perp_best_bid=perp_best_bid,
            perp_best_ask=perp_best_ask,
        )

    collector.on_basis_update(on_basis)

    # 2) 트레이드 이벤트 → 로그 + 알림
    def on_trade(trade, event):
        if event == "open":
            notifier.notify_trade_open(
                product=trade.product,
                direction=trade.direction,
                basis_bps=trade.entry_basis_bps,
                perp_price=trade.perp_entry_price,
                futures_price=trade.futures_entry_price,
                contracts=trade.size_contracts,
            )
        elif event == "close":
            hold_h = (trade.exit_time - trade.entry_time) / 3600
            notifier.notify_trade_close(
                product=trade.product,
                direction=trade.direction,
                pnl_usd=trade.net_pnl_usd,
                reason=trade.exit_reason,
                hold_hours=hold_h,
            )

    engine.on_trade(on_trade)

    # 3) 시그널 로그
    def on_signal(sig):
        logger.info(
            f"[{sig.product.upper()}] SIGNAL {sig.type.value} | "
            f"basis={sig.basis_bps:+.1f}bp | "
            f"confidence={sig.confidence:.2f} | {sig.reason}"
        )

    engine.on_signal(on_signal)

    # 종료 핸들링
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def shutdown():
        logger.info("Shutdown signal received")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown)

    collect_task = asyncio.create_task(collector.start())

    # 상태 출력 루프 (60초마다)
    async def paper_status_loop():
        while not stop_event.is_set():
            await asyncio.sleep(60)

            # 엔진 요약
            logger.info(engine.get_summary())

            # 베이시스 통계
            for name in config.products:
                stats = storage.get_basis_stats(name, hours=1)
                if stats["count"] > 0:
                    logger.info(
                        f"[{name.upper()} 1h] "
                        f"mean={stats['mean']:.1f}bp "
                        f"std={stats['std']:.1f}bp "
                        f"[{stats['min']:.1f}, {stats['max']:.1f}]bp "
                        f"n={stats['count']}"
                    )

    status_task = asyncio.create_task(paper_status_loop())

    # 펀딩 정산 루프 (1시간마다)
    async def funding_loop():
        while not stop_event.is_set():
            await asyncio.sleep(3600)
            for name in config.products:
                md = collector.latest_perp.get(name)
                if md:
                    engine.process_funding_update(name, md.funding_rate)
                    logger.info(
                        f"[{name.upper()}] Funding settled: rate={md.funding_rate:.6f}"
                    )

    funding_task = asyncio.create_task(funding_loop())

    logger.info("Paper trading engine started — waiting for signals...")
    await stop_event.wait()

    logger.info("Shutting down paper trading...")
    collect_task.cancel()
    status_task.cancel()
    funding_task.cancel()

    for task in [collect_task, status_task, funding_task]:
        try:
            await task
        except asyncio.CancelledError:
            pass

    await collector.stop()
    if kis_client:
        await kis_client.disconnect()
    kiwoom.disconnect()

    # 최종 요약
    logger.info("=== Final Paper Trading Report ===")
    logger.info(engine.get_summary())

    # 일일 PnL
    daily = storage.get_daily_pnl_summary(days=7)
    if daily:
        logger.info("--- Daily PnL ---")
        for d in daily:
            logger.info(
                f"  {d['date']}: net=${d['net_pnl']:+.2f} "
                f"(trading=${d['trading_pnl']:+.2f} "
                f"funding=${d['funding_pnl']:+.2f} "
                f"fees=-${d['fees']:.2f}) "
                f"trades={d['num_trades']}"
            )

    storage.close()
    logger.info("Shutdown complete")


# ──────────────────────────────────────────────
# 엔트리포인트
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="RWA Arbitrage Bot")
    parser.add_argument(
        "--mode", choices=["collect", "paper", "live"],
        default="collect", help="실행 모드"
    )
    parser.add_argument(
        "--config", default="config/settings.yaml",
        help="설정 파일 경로"
    )
    args = parser.parse_args()

    if args.mode == "collect":
        asyncio.run(run_collector(args.config))
    elif args.mode == "paper":
        asyncio.run(run_paper(args.config))
    elif args.mode == "live":
        print("Live trading mode — Phase 3에서 구현 예정")


if __name__ == "__main__":
    main()
