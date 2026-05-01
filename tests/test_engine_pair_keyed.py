"""Phase C4a 회귀 — PaperTradingEngine pair-keyed 인프라.

검증:
  - register_pair / set_exchange_registry / cache_pair_quote 인터페이스
  - has_both_legs / latest_pair_quote 정확
  - compute_pair_exec_basis: long_basis/short_basis bid-ask cross 계산
  - dispatch_pair_order: registry 통한 어댑터 호출, Semaphore(1) 직렬화,
    NotImplementedError graceful 처리, 미등록 거래소 graceful 실패
  - 레거시 _open_trades / _latest_perp* 무영향
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.data.storage import Storage
from src.exchange.base import ExchangeBase, OrderResult, Position, Quote, VenueType
from src.exchange.registry import ExchangeRegistry
from src.exchange.kiwoom import KiwoomMock
from src.paper.engine import PaperTradingEngine
from src.risk.manager import RiskManager
from src.strategy.pair import (
    ArbitragePair, ExchangeLeg, LegRole, PairGate, PairStrategyParams,
)
from src.utils.config import (
    AppConfig, HyperliquidConfig, KISConfig, KiwoomConfig,
    ProductConfig, RiskConfig, StrategyConfig,
)


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


class _StubAdapter:
    """ExchangeBase stub — order 호출 기록 + 결과 제어."""

    def __init__(
        self,
        name: str,
        success: bool = True,
        sleep_s: float = 0.0,
        raise_exc: Exception | None = None,
        not_implemented: bool = False,
    ):
        self.name = name
        self.venue_type = VenueType.PERP.value
        self.margin_asset = "USDC"
        self._success = success
        self._sleep_s = sleep_s
        self._raise = raise_exc
        self._not_implemented = not_implemented
        self.calls: list[dict] = []

    async def connect(self): return True
    async def disconnect(self): pass
    async def subscribe_quotes(self, *args, **kwargs): pass
    async def unsubscribe_quotes(self, *args, **kwargs): pass
    async def get_quote(self, symbol): return None

    async def place_order(self, symbol, side, size, order_type="market",
                          limit_price=None, reduce_only=False,
                          client_order_id=None):
        self.calls.append(dict(
            symbol=symbol, side=side, size=size, order_type=order_type,
            limit_price=limit_price, reduce_only=reduce_only,
        ))
        if self._sleep_s:
            await asyncio.sleep(self._sleep_s)
        if self._not_implemented:
            raise NotImplementedError("paper-only adapter")
        if self._raise:
            raise self._raise
        return OrderResult(
            success=self._success, exchange=self.name, symbol=symbol,
            order_id=f"{self.name}-{len(self.calls)}",
            filled_size=size if self._success else 0.0,
            filled_price=80.0 if self._success else 0.0,
            error="" if self._success else "stub_failure",
        )

    async def cancel_order(self, *args, **kwargs): return False
    async def get_positions(self): return []
    async def get_account_value(self): return 100_000.0


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
        strategy=StrategyConfig(),
        risk=RiskConfig(),
    )


@pytest.fixture
def engine(cfg, tmp_path: Path) -> PaperTradingEngine:
    s = Storage(str(tmp_path / "engine.db"))
    s.connect()
    return PaperTradingEngine(cfg, s, KiwoomMock(), risk_mgr=RiskManager(cfg.risk))


def _pair(pair_id: str, leg_a_ex="hyperliquid", leg_b_ex="kis",
           leg_b_role: LegRole = LegRole.DATED_FUTURES) -> ArbitragePair:
    return ArbitragePair(
        id=pair_id, enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange=leg_a_ex, symbol="xyz:CL", role=LegRole.PERP,
                          taker_fee_bps=0.9, funding_interval_hours=1.0,
                          margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange=leg_b_ex,
                          symbol="MCLM26" if leg_b_role == LegRole.DATED_FUTURES else "CLUSDT",
                          role=leg_b_role,
                          contract_size=100 if leg_b_role == LegRole.DATED_FUTURES else 1.0,
                          fee_per_contract_usd=2.5 if leg_b_role == LegRole.DATED_FUTURES else 0.0,
                          taker_fee_bps=0.0 if leg_b_role == LegRole.DATED_FUTURES else 4.0),
        params=PairStrategyParams(),
    )


def _q(exchange, symbol, mid, bid=0.0, ask=0.0) -> Quote:
    return Quote(exchange=exchange, symbol=symbol, mid_price=mid, bid=bid, ask=ask)


# ──────────────────────────────────────────────
# register_pair / state
# ──────────────────────────────────────────────


def test_register_pair_stores_in_engine(engine):
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    assert engine.get_registered_pair("wti_cme_hl") is p
    assert "wti_cme_hl" in engine.registered_pairs


def test_register_pair_creates_exchange_semaphores(engine):
    p = _pair("wti_cme_hl", leg_a_ex="hyperliquid", leg_b_ex="kis")
    engine.register_pair(p)
    assert "hyperliquid" in engine._exchange_semaphores
    assert "kis" in engine._exchange_semaphores


def test_register_pair_overwrites_same_id(engine):
    engine.register_pair(_pair("p1"))
    engine.register_pair(_pair("p1", leg_b_ex="binance"))
    assert engine.get_registered_pair("p1").leg_b.exchange == "binance"


def test_no_pair_open_trade_initially(engine):
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    assert engine.get_pair_open_trade("wti_cme_hl") is None
    assert engine.get_pair_open_trades() == {}


# ──────────────────────────────────────────────
# Quote 캐시
# ──────────────────────────────────────────────


def test_cache_pair_quote_invalid_leg_raises(engine):
    with pytest.raises(ValueError):
        engine.cache_pair_quote("wti_cme_hl", "c", _q("hl", "x", 80))


def test_cache_pair_quote_and_lookup(engine):
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    q = _q("hyperliquid", "xyz:CL", 80, bid=79.99, ask=80.01)
    engine.cache_pair_quote("wti_cme_hl", "a", q)
    assert engine.latest_pair_quote("wti_cme_hl", "a") is q
    assert engine.has_both_legs("wti_cme_hl") is False


def test_has_both_legs_true_when_both_cached(engine):
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    engine.cache_pair_quote("wti_cme_hl", "a", _q("hl", "x", 80, 80, 80))
    engine.cache_pair_quote("wti_cme_hl", "b", _q("kis", "y", 80, 80, 80))
    assert engine.has_both_legs("wti_cme_hl") is True


# ──────────────────────────────────────────────
# compute_pair_exec_basis
# ──────────────────────────────────────────────


def test_exec_basis_long_basis():
    a = _q("hl", "x", 80.20, bid=80.19, ask=80.21)
    b = _q("kis", "y", 80.00, bid=79.99, ask=80.01)
    bps = PaperTradingEngine.compute_pair_exec_basis("long_basis", a, b)
    # (80.19 - 80.01) / 80.01 * 10000 ≈ 22.5bp
    assert 22 < bps < 23


def test_exec_basis_short_basis():
    a = _q("hl", "x", 79.80, bid=79.79, ask=79.81)
    b = _q("kis", "y", 80.00, bid=79.99, ask=80.01)
    bps = PaperTradingEngine.compute_pair_exec_basis("short_basis", a, b)
    # (79.81 - 79.99) / 79.99 * 10000 ≈ -22.5bp
    assert -23 < bps < -22


def test_exec_basis_long_basis_returns_zero_when_a_bid_missing():
    a = _q("hl", "x", 80, bid=0, ask=80.01)
    b = _q("kis", "y", 80, bid=79.99, ask=80.01)
    assert PaperTradingEngine.compute_pair_exec_basis("long_basis", a, b) == 0.0


def test_exec_basis_long_basis_returns_zero_when_b_ask_missing():
    a = _q("hl", "x", 80, bid=80, ask=80.01)
    b = _q("kis", "y", 80, bid=79.99, ask=0)
    assert PaperTradingEngine.compute_pair_exec_basis("long_basis", a, b) == 0.0


def test_exec_basis_short_basis_returns_zero_when_a_ask_missing():
    a = _q("hl", "x", 80, bid=80, ask=0)
    b = _q("kis", "y", 80, bid=79.99, ask=80.01)
    assert PaperTradingEngine.compute_pair_exec_basis("short_basis", a, b) == 0.0


def test_exec_basis_short_basis_returns_zero_when_b_bid_missing():
    a = _q("hl", "x", 80, bid=79.99, ask=80.01)
    b = _q("kis", "y", 80, bid=0, ask=80.01)
    assert PaperTradingEngine.compute_pair_exec_basis("short_basis", a, b) == 0.0


def test_exec_basis_invalid_direction_raises():
    a = _q("hl", "x", 80, 80, 80)
    b = _q("kis", "y", 80, 80, 80)
    with pytest.raises(ValueError):
        PaperTradingEngine.compute_pair_exec_basis("flat", a, b)


# ──────────────────────────────────────────────
# dispatch_pair_order
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dispatch_requires_registry(engine):
    engine.register_pair(_pair("wti_cme_hl"))
    with pytest.raises(RuntimeError):
        await engine.dispatch_pair_order("wti_cme_hl", "a", "buy", 1.0)


@pytest.mark.asyncio
async def test_dispatch_unknown_pair_raises(engine):
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid"))
    engine.set_exchange_registry(reg)
    with pytest.raises(KeyError):
        await engine.dispatch_pair_order("nonexistent", "a", "buy", 1.0)


@pytest.mark.asyncio
async def test_dispatch_routes_to_correct_exchange(engine):
    reg = ExchangeRegistry()
    hl = _StubAdapter("hyperliquid")
    kis = _StubAdapter("kis")
    reg.register(hl); reg.register(kis)
    engine.set_exchange_registry(reg)
    engine.register_pair(_pair("wti_cme_hl"))

    r_a = await engine.dispatch_pair_order("wti_cme_hl", "a", "sell", 200.0)
    r_b = await engine.dispatch_pair_order("wti_cme_hl", "b", "buy", 2.0)

    assert r_a.success and r_a.exchange == "hyperliquid"
    assert r_b.success and r_b.exchange == "kis"
    assert hl.calls[0]["symbol"] == "xyz:CL"
    assert hl.calls[0]["side"] == "sell"
    assert hl.calls[0]["size"] == 200.0
    assert kis.calls[0]["symbol"] == "MCLM26"


@pytest.mark.asyncio
async def test_dispatch_handles_not_implemented(engine):
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid"))
    reg.register(_StubAdapter("kis", not_implemented=True))
    engine.set_exchange_registry(reg)
    engine.register_pair(_pair("wti_cme_hl"))
    r = await engine.dispatch_pair_order("wti_cme_hl", "b", "buy", 2.0)
    assert r.success is False
    assert "NotImplementedError" in r.error


@pytest.mark.asyncio
async def test_dispatch_handles_arbitrary_exception(engine):
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", raise_exc=RuntimeError("boom")))
    reg.register(_StubAdapter("kis"))
    engine.set_exchange_registry(reg)
    engine.register_pair(_pair("wti_cme_hl"))
    r = await engine.dispatch_pair_order("wti_cme_hl", "a", "buy", 1.0)
    assert r.success is False
    assert "boom" in r.error


@pytest.mark.asyncio
async def test_dispatch_unregistered_exchange_returns_failure(engine):
    """페어는 등록됐는데 어댑터가 registry에 없는 경우."""
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid"))   # kis 미등록
    engine.set_exchange_registry(reg)
    engine.register_pair(_pair("wti_cme_hl"))
    r = await engine.dispatch_pair_order("wti_cme_hl", "b", "buy", 1.0)
    assert r.success is False
    assert "kis" in r.error


@pytest.mark.asyncio
async def test_dispatch_serializes_via_per_exchange_semaphore(engine):
    """같은 거래소 동시 2건 주문 시 직렬화 (Semaphore=1)."""
    reg = ExchangeRegistry()
    hl = _StubAdapter("hyperliquid", sleep_s=0.05)
    reg.register(hl)
    reg.register(_StubAdapter("kis"))
    engine.set_exchange_registry(reg)
    engine.register_pair(_pair("p1"))
    engine.register_pair(_pair("p2", leg_a_ex="hyperliquid", leg_b_ex="binance",
                                  leg_b_role=LegRole.PERP))
    reg.register(_StubAdapter("binance"))

    # 두 페어의 leg_a (둘 다 hyperliquid) 동시 발사
    import time as _t
    t0 = _t.monotonic()
    results = await asyncio.gather(
        engine.dispatch_pair_order("p1", "a", "sell", 1.0),
        engine.dispatch_pair_order("p2", "a", "buy", 1.0),
    )
    elapsed = _t.monotonic() - t0
    assert all(r.success for r in results)
    # 직렬화 → 최소 2 * 0.05 = 0.10 이상
    assert elapsed >= 0.09


# ──────────────────────────────────────────────
# 레거시 무영향
# ──────────────────────────────────────────────


def test_legacy_state_dicts_unaffected_by_pair_path(engine):
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    engine.cache_pair_quote("wti_cme_hl", "a", _q("hl", "x", 80, 80, 80))
    # 레거시 dict 비어있음
    assert engine._open_trades == {}
    assert engine._latest_perp_bid == {}
    assert engine._latest_futures_ask == {}
