"""Phase C5 회귀 — main.py wiring 패턴 통합.

main.py 자체는 외부 IO에 의존하지만 wiring 로직(registry 구성, pair 등록,
legacy on_basis → pair-keyed update_leg_quote → engine.process_pair_basis_update)
은 본 파일에서 fixture로 재현 검증 가능.
"""

from __future__ import annotations

import asyncio
import math
import time
from pathlib import Path

import pytest

from src.data.collector import DataCollector
from src.data.storage import LEGACY_PRODUCT_PAIR_MAP, Storage
from src.exchange.base import Quote
from src.exchange.kiwoom import KiwoomMock
from src.exchange.registry import ExchangeRegistry
from src.paper.engine import PaperTradingEngine
from src.risk.manager import RiskManager
from src.strategy.pair import LegRole
from src.utils.config import (
    AppConfig, HyperliquidConfig, KISConfig, KiwoomConfig,
    ProductConfig, RiskConfig, StrategyConfig,
)


@pytest.fixture
def cfg() -> AppConfig:
    return AppConfig(
        products={
            "wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL",
                                  contract_size=100, futures_fee_per_contract=2.5),
        },
        kis_symbol_map={"wti": "MCLM26"},
        hyperliquid=HyperliquidConfig(use_testnet=False),
        kiwoom=KiwoomConfig(use_mock=True),
        kis=KISConfig(),
        strategy=StrategyConfig(
            cme_closed_skip_entry=False,    # 테스트에서 24/7
            entry_threshold_bps=20,
            min_abs_entry_bps=10,
        ),
        risk=RiskConfig(),
    )


@pytest.fixture
def wired(cfg, tmp_path: Path):
    """main.py의 wire 시퀀스 재현. 반환: (collector, engine, on_basis_bridge)."""
    storage = Storage(str(tmp_path / "wired.db"))
    storage.connect()
    kw = KiwoomMock()
    kw.set_base_price("MCLM26", 80.0)
    collector = DataCollector(cfg, storage)
    engine = PaperTradingEngine(cfg, storage, kw, risk_mgr=RiskManager(cfg.risk))

    # Phase C5 wiring (main.py 동치)
    pairs = cfg.get_pairs()
    registry = ExchangeRegistry()
    engine.set_exchange_registry(registry)
    for pair in pairs:
        engine.register_pair(pair)
        collector.register_pair(pair)
    pair_by_product = {
        product: collector.get_pair(LEGACY_PRODUCT_PAIR_MAP.get(product, product))
        for product in cfg.products
    }

    # warmup bootstrap (std≈3.5)
    history = [5.0 * math.sin(i * 0.07) for i in range(4000)]
    for product, pair in pair_by_product.items():
        engine.signal_gen.bootstrap_from_db_for_pair(pair.id, history)

    collector.on_pair_basis(engine.process_pair_basis_update)

    def on_basis_bridge(
        product, perp_price, futures_price, basis_bps,
        perp_best_bid=0.0, perp_best_ask=0.0,
        futures_bid=0.0, futures_ask=0.0,
    ):
        pair = pair_by_product.get(product)
        if pair is None:
            return
        leg_a_q = Quote(
            exchange=pair.leg_a.exchange, symbol=pair.leg_a.symbol,
            mid_price=perp_price, bid=perp_best_bid, ask=perp_best_ask,
            funding_interval_hours=pair.leg_a.funding_interval_hours,
        )
        leg_b_q = Quote(
            exchange=pair.leg_b.exchange, symbol=pair.leg_b.symbol,
            mid_price=futures_price, bid=futures_bid, ask=futures_ask,
            contract_month=(pair.leg_b.symbol if pair.leg_b.role == LegRole.DATED_FUTURES else ""),
        )
        collector.update_leg_quote(pair.id, "a", leg_a_q)
        collector.update_leg_quote(pair.id, "b", leg_b_q)

    collector.on_basis_update(on_basis_bridge)
    return collector, engine, on_basis_bridge


# ──────────────────────────────────────────────
# Wiring sanity
# ──────────────────────────────────────────────


def test_pairs_registered_in_engine_and_collector(wired):
    collector, engine, _ = wired
    assert "wti_cme_hl" in engine.registered_pairs
    assert "wti_cme_hl" in collector.registered_pairs


def test_legacy_callback_routes_to_collector_leg_quote(wired):
    """sync 호출에서 collector cache까지는 즉시 채워짐.
    engine cache는 async process_pair_basis_update가 fill하므로 별도 async 테스트.
    """
    collector, engine, on_basis = wired
    on_basis(
        "wti", 80.30, 80.00, 30.0,
        perp_best_bid=80.29, perp_best_ask=80.31,
        futures_bid=79.99, futures_ask=80.01,
    )
    a = collector.latest_pair_quote("wti_cme_hl", "a")
    b = collector.latest_pair_quote("wti_cme_hl", "b")
    assert a is not None and a.exchange == "hyperliquid"
    assert b is not None and b.exchange == "kis"


@pytest.mark.asyncio
async def test_engine_cache_populated_after_async_dispatch(wired):
    collector, engine, on_basis = wired
    on_basis(
        "wti", 80.30, 80.00, 30.0,
        perp_best_bid=80.29, perp_best_ask=80.31,
        futures_bid=79.99, futures_ask=80.01,
    )
    await asyncio.sleep(0.05)   # async task가 engine.cache_pair_quote 호출할 시간
    assert engine.latest_pair_quote("wti_cme_hl", "a") is not None
    assert engine.latest_pair_quote("wti_cme_hl", "b") is not None


# ──────────────────────────────────────────────
# End-to-end: legacy callback → engine 진입까지
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_e2e_entry_via_wired_pair_keyed_path(wired):
    """legacy on_basis 콜백 1회 → pair-keyed 경로 거쳐 engine 진입까지 작동."""
    collector, engine, on_basis = wired

    # 30bp long_basis 시그널을 트리거
    on_basis(
        "wti", 80.30, 80.00, 30.0,
        perp_best_bid=80.29, perp_best_ask=80.31,
        futures_bid=79.99, futures_ask=80.01,
    )
    # process_pair_basis_update는 async로 schedule됨. 잠시 대기.
    await asyncio.sleep(0.05)

    # 진입 성사 검증
    assert engine._state.total_entries == 1, (
        f"expected 1 entry; state={engine._state}"
    )
    trade = engine.get_pair_open_trade("wti_cme_hl")
    assert trade is not None
    assert trade.direction == "long_basis"


@pytest.mark.asyncio
async def test_e2e_full_cycle_entry_then_exit(wired):
    collector, engine, on_basis = wired

    # ENTRY: 30bp long_basis
    on_basis(
        "wti", 80.30, 80.00, 30.0,
        perp_best_bid=80.29, perp_best_ask=80.31,
        futures_bid=79.99, futures_ask=80.01,
    )
    # async task가 끝날 때까지 충분히 대기 (다른 백그라운드 태스크 없음)
    await asyncio.sleep(0.1)
    assert engine._state.total_entries == 1

    # EXIT: spread 1bp로 수렴
    on_basis(
        "wti", 80.01, 80.00, 1.0,
        perp_best_bid=80.00, perp_best_ask=80.01,
        futures_bid=79.99, futures_ask=80.01,
    )
    await asyncio.sleep(0.1)
    assert engine._state.total_exits == 1
    assert engine._state.closed_trades == 1
    assert engine.get_pair_open_trade("wti_cme_hl") is None


# ──────────────────────────────────────────────
# Legacy 경로 단절 확인 — bridge 콜백만 등록, 옛 process_basis_update 미사용
# ──────────────────────────────────────────────


def test_legacy_open_trades_dict_remains_empty(wired):
    """Phase C5 후엔 _open_trades(레거시 dict)는 더 이상 채워지지 않음."""
    collector, engine, on_basis = wired
    on_basis(
        "wti", 80.30, 80.00, 30.0,
        perp_best_bid=80.29, perp_best_ask=80.31,
        futures_bid=79.99, futures_ask=80.01,
    )
    # 레거시 dict는 비어있고 pair-keyed dict만 채워짐
    assert engine._open_trades == {}
