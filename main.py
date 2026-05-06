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
from datetime import date
from pathlib import Path

# 프로젝트 루트를 path에 추가
sys.path.insert(0, str(Path(__file__).parent))

from src.utils.config import load_config
from src.utils.logger import setup_logger, get_logger
from src.utils.notifier import TelegramNotifier
from src.data.storage import LEGACY_PRODUCT_PAIR_MAP, Storage
from src.data.collector import DataCollector
from src.exchange.base import Quote
from src.exchange.hyperliquid import HyperliquidExchange
from src.exchange.kis import KISAuth, KISExchange, KISFuturesClient
from src.exchange.kiwoom import create_kiwoom_client
from src.exchange.lighter import LighterExchange
from src.exchange.registry import ExchangeRegistry
from src.strategy.pair import LegRole
from src.strategy.rollover import get_active_contract, us_market_holidays


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


async def _setup_kis(config, collector, kiwoom=None):
    """KIS 클라이언트 초기화 + 실시간 구독.

    Returns:
        (client, subs_state) 튜플. 비활성화 시 (None, None).
        subs_state: {"current_subs", "callbacks", "divisors"} — rollover_watch_loop에서 사용.
    """
    if not config.kis.enabled:
        return None, None

    logger = get_logger()

    auth = KISAuth(
        app_key=config.kis.app_key,
        app_secret=config.kis.app_secret,
        base_url=config.kis.base_url,
        is_paper=config.kis.is_paper,
        account_number=config.kis.account_number,
    )

    client = KISFuturesClient(
        auth=auth,
        ws_url=config.kis.ws_url,
        is_paper=config.kis.is_paper,
    )

    connected = await client.connect()
    if not connected:
        logger.error("KIS connection failed — falling back to Kiwoom mock")
        return None, None

    subs_state = {
        "current_subs": {},   # product → active KIS symbol
        "callbacks": {},      # product → callback fn
        "divisors": {},       # product → price divisor
    }

    today = date.today()
    hols = us_market_holidays(today.year)

    for product_name, configured_symbol in config.kis_symbol_map.items():
        if product_name not in config.products:
            continue

        prefix = config.products[product_name].futures_symbol
        active_symbol = get_active_contract(today, prefix=prefix, holidays=hols)
        if active_symbol != configured_symbol:
            logger.warning(
                f"[{product_name}] config symbol {configured_symbol} != computed active "
                f"{active_symbol} — using {active_symbol} (rollover auto-correction)"
            )
        target_symbol = active_symbol

        def make_callback(pname, kis_sym):
            legacy_sym = config.products[pname].futures_symbol
            def on_kis_quote(quote):
                collector.update_futures_price(
                    product_name=pname,
                    price=quote.price,
                    bid=quote.bid,
                    ask=quote.ask,
                    contract_month=quote.contract_month,
                    volume=quote.volume,
                )
                if kiwoom:
                    # 양 경로 호환: legacy product-keyed는 "MCL", pair-keyed는
                    # 실제 KIS 코드(예: "MCLM26"). 두 키 모두 base_price 등록.
                    kiwoom.set_base_price(
                        legacy_sym, quote.price,
                        bid=quote.bid, ask=quote.ask,
                    )
                    if kis_sym and kis_sym != legacy_sym:
                        kiwoom.set_base_price(
                            kis_sym, quote.price,
                            bid=quote.bid, ask=quote.ask,
                        )
            return on_kis_quote

        cb = make_callback(product_name, target_symbol)
        price_divisor = float(config.products[product_name].contract_size)

        await client.subscribe(target_symbol, cb, price_divisor=price_divisor)
        subs_state["current_subs"][product_name] = target_symbol
        subs_state["callbacks"][product_name] = cb
        subs_state["divisors"][product_name] = price_divisor
        logger.info(
            f"KIS subscribed: {product_name} → {target_symbol} "
            f"(price_divisor={price_divisor})"
        )

    return client, subs_state


async def rollover_watch_loop(
    kis_client: KISFuturesClient,
    config,
    subs_state: dict,
    stop_event: asyncio.Event,
    check_interval: int = 3600,
):
    """매시간 active contract 재계산 후 변화 시 resubscribe.

    stop_event 설정 시 즉시 종료.
    """
    logger = get_logger()
    if kis_client is None or subs_state is None:
        return
    logger.info(f"[ROLLOVER] watch loop started (interval={check_interval}s)")

    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=check_interval)
            break
        except asyncio.TimeoutError:
            pass

        today = date.today()
        hols = us_market_holidays(today.year)

        for product, current in list(subs_state["current_subs"].items()):
            prefix = config.products[product].futures_symbol
            desired = get_active_contract(today, prefix=prefix, holidays=hols)
            if desired == current:
                continue

            logger.warning(
                f"[ROLLOVER] {product}: {current} → {desired} (today={today}), resubscribing"
            )
            ok = await kis_client.resubscribe(
                old_symbol=current,
                new_symbol=desired,
                callback=subs_state["callbacks"][product],
                price_divisor=subs_state["divisors"][product],
            )
            if ok:
                subs_state["current_subs"][product] = desired
                logger.warning(f"[ROLLOVER] {product}: now subscribed to {desired}")
            else:
                logger.error(
                    f"[ROLLOVER] {product}: resubscribe failed, will retry next cycle"
                )

    logger.info("[ROLLOVER] watch loop stopped")


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
    kis_client, kis_subs = await _setup_kis(config, collector, kiwoom)

    def on_basis(product, perp_price, futures_price, basis_bps,
                 perp_best_bid=0.0, perp_best_ask=0.0,
                 futures_bid=0.0, futures_ask=0.0):
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
    rollover_task = asyncio.create_task(
        rollover_watch_loop(kis_client, config, kis_subs, stop_event)
    )

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
    for task in (collect_task, status_task, rollover_task):
        try:
            await task
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

    # Telegram 알림 (Phase 11d) — secrets.yaml의 token/chat_id 있으면 자동 활성화.
    notifier = TelegramNotifier(
        bot_token=config.telegram.bot_token,
        chat_id=config.telegram.chat_id,
        enabled=config.telegram.enabled,
    )
    engine_notifier_ref = notifier

    # 데이터 수집기
    collector = DataCollector(config, storage)
    _register_collector_callbacks(collector, kiwoom, config)

    # KIS 실시간 호가 (활성화 시)
    kis_client, kis_subs = await _setup_kis(config, collector, kiwoom)

    # 페이퍼 트레이딩 엔진
    from src.paper.engine import PaperTradingEngine
    engine = PaperTradingEngine(
        config=config,
        storage=storage,
        kiwoom=kiwoom,
    )

    # ── Phase C5: pair-keyed 경로 wiring ──
    # ArbitragePair 합성 + ExchangeRegistry + 어댑터 등록 + 페어 등록
    pairs = config.get_pairs()
    registry = ExchangeRegistry()
    # HL adapter는 collector가 이미 들고 있는 client + ws를 재사용 (구독 중복 방지)
    registry.register(HyperliquidExchange(rest=collector.hl_client, ws=collector.hl_ws))
    if kis_client is not None:
        registry.register(KISExchange(client=kis_client))

    # ── Phase D: Lighter 어댑터 (활성화 시) ──
    lighter_adapter: LighterExchange | None = None
    if config.lighter.enabled:
        lighter_adapter = LighterExchange(
            base_url=config.lighter.base_url,
            ws_url=config.lighter.ws_url,
        )
        ok = await lighter_adapter.connect()
        if ok:
            registry.register(lighter_adapter)
            logger.info("Lighter adapter registered (Phase D)")
            try:
                discovered = await lighter_adapter.discover_markets()
                logger.info(f"Lighter markets: {len(discovered)} symbols discovered")
            except Exception as e:
                logger.warning(f"Lighter discover_markets failed: {e}")
        else:
            logger.warning("Lighter connect failed; skipping registration")
            lighter_adapter = None

    engine.set_exchange_registry(registry)
    engine.set_notifier(notifier)
    for pair in pairs:
        engine.register_pair(pair)
        collector.register_pair(pair)
        logger.info(
            f"[PAIR] {pair.id} enabled={pair.enabled} "
            f"leg_a={pair.leg_a.exchange}/{pair.leg_a.symbol} "
            f"leg_b={pair.leg_b.exchange}/{pair.leg_b.symbol}"
        )

    # Lighter 페어가 등록됐고 adapter도 connect됐으면 leg_b 심볼 구독 시도.
    # Quote 도착하면 collector.update_leg_quote 호출 — 일반 페어 callback 흐름과 동일.
    if lighter_adapter is not None:
        for pair in pairs:
            if pair.leg_b.exchange != "lighter":
                continue

            # symbol 매핑 확인 — discover_markets에서 못 찾았으면 명시 등록 (수동 fallback)
            if lighter_adapter.get_market_id(pair.leg_b.symbol) is None:
                logger.warning(
                    f"[LIGHTER] market_id for {pair.leg_b.symbol} not auto-discovered; "
                    "skipping subscribe (manual set_market_id required)"
                )
                continue

            def _make_lighter_cb(pair_id: str):
                def _cb(q: Quote) -> None:
                    collector.update_leg_quote(pair_id, "b", q)
                return _cb

            try:
                await lighter_adapter.subscribe_quotes(
                    pair.leg_b.symbol, _make_lighter_cb(pair.id),
                )
                logger.info(f"[LIGHTER] subscribed {pair.id} leg_b → {pair.leg_b.symbol}")
            except Exception as e:
                logger.error(f"[LIGHTER] subscribe failed for {pair.id}: {e}")
    pair_by_product: dict[str, "object"] = {
        product: collector.get_pair(LEGACY_PRODUCT_PAIR_MAP.get(product, product))
        for product in config.products
    }

    # HL leg_a를 공유하는 모든 페어 — bridge가 HL Quote를 fan-out할 대상
    hl_hub_pairs_by_perp_symbol: dict[str, list] = {}
    for pair in pairs:
        if pair.leg_a.exchange == "hyperliquid":
            hl_hub_pairs_by_perp_symbol.setdefault(pair.leg_a.symbol, []).append(pair)

    # DB에서 최근 basis 데이터 부트스트랩 (재시작 시 window 즉시 복원)
    # legacy 'wti' 키와 pair_id 'wti_cme_hl' 키 둘 다에 주입 → 양 경로 모두 stats 보유.
    for product_name in config.products:
        history = storage.get_basis_history(product_name, hours=config.strategy.basis_window_hours)
        if history:
            engine.signal_gen.bootstrap_from_db(product_name, history)
            pair_id = LEGACY_PRODUCT_PAIR_MAP.get(product_name)
            if pair_id:
                engine.signal_gen.bootstrap_from_db_for_pair(pair_id, history)
        else:
            logger.info(f"[{product_name.upper()}] No basis history in DB — starting fresh")

    # ── 콜백 연결 ──

    # 1) 베이시스 업데이트 → pair-keyed 경로로 bridge
    #    legacy collector callback에서 받은 product/price/bid/ask를 Quote로 변환해
    #    collector.update_leg_quote 로 push. update_leg_quote가 양 leg 도착 시
    #    on_pair_basis 콜백을 fire → engine.process_pair_basis_update (async).
    #    leg_a (HL Quote)는 같은 perp_symbol을 leg_a로 쓰는 모든 페어에 fan-out.
    def on_basis(product, perp_price, futures_price, basis_bps,
                 perp_best_bid=0.0, perp_best_ask=0.0,
                 futures_bid=0.0, futures_ask=0.0):
        pair = pair_by_product.get(product)
        if pair is None:
            return

        md = collector.latest_perp.get(product)
        funding_rate = md.funding_rate if md else 0.0
        index_price = md.index_price if md else 0.0
        predicted_funding = md.predicted_funding_rate if md else 0.0

        leg_a_q = Quote(
            exchange=pair.leg_a.exchange,
            symbol=pair.leg_a.symbol,
            mid_price=perp_price,
            bid=perp_best_bid, ask=perp_best_ask,
            index_price=index_price,
            funding_rate=funding_rate,
            funding_interval_hours=pair.leg_a.funding_interval_hours,
            predicted_funding_rate=predicted_funding,
        )
        leg_b_q = Quote(
            exchange=pair.leg_b.exchange,
            symbol=pair.leg_b.symbol,
            mid_price=futures_price,
            bid=futures_bid, ask=futures_ask,
            contract_month=(pair.leg_b.symbol if pair.leg_b.role == LegRole.DATED_FUTURES else ""),
        )
        collector.update_leg_quote(pair.id, "a", leg_a_q)
        collector.update_leg_quote(pair.id, "b", leg_b_q)

        # HL leg_a를 공유하는 다른 페어(예: wti_hl_lighter)에도 leg_a 동일 push
        for other_pair in hl_hub_pairs_by_perp_symbol.get(leg_a_q.symbol, []):
            if other_pair.id == pair.id:
                continue
            collector.update_leg_quote(other_pair.id, "a", leg_a_q)

    collector.on_basis_update(on_basis)
    collector.on_pair_basis(engine.process_pair_basis_update)

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
    rollover_task = asyncio.create_task(
        rollover_watch_loop(kis_client, config, kis_subs, stop_event)
    )

    # 대시보드용 engine_state 스냅샷 (30초마다 DB에 dump)
    state_snapshot_task = asyncio.create_task(
        engine.state_snapshot_loop(interval_seconds=30, stop_event=stop_event)
    )

    # Phase 11d — LIVE 모드 WS freshness 워치독 (PAPER에서는 즉시 self-exit)
    watchdog_task = asyncio.create_task(
        engine.quote_freshness_watchdog(interval_seconds=15, stop_event=stop_event)
    )

    # 부팅 알림
    if notifier.enabled:
        try:
            await notifier.send(
                f"🟢 <b>Bot started</b>\n"
                f"mode={config.mode} | pairs={len(pairs)}"
            )
        except Exception as e:
            logger.warning(f"startup notify error: {e}")

    logger.info("Paper trading engine started — waiting for signals...")
    await stop_event.wait()

    logger.info("Shutting down paper trading...")
    collect_task.cancel()
    status_task.cancel()
    funding_task.cancel()

    state_snapshot_task.cancel()
    watchdog_task.cancel()
    for task in [collect_task, status_task, funding_task, rollover_task,
                 state_snapshot_task, watchdog_task]:
        try:
            await task
        except asyncio.CancelledError:
            pass

    if notifier.enabled:
        try:
            await notifier.send("🔴 <b>Bot stopped</b>")
        except Exception:
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
