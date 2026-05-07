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
from src.strategy.signals import Signal, SignalType
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
        strategy=StrategyConfig(
            cme_closed_skip_entry=False,    # 테스트에서 24/7 가정
            entry_threshold_bps=20,
            min_abs_entry_bps=10,
        ),
        risk=RiskConfig(
            rollover_block_entry_days=0,
            rollover_position_reduce_pct=0,
        ),
    )


@pytest.fixture
def engine(cfg, tmp_path: Path) -> PaperTradingEngine:
    s = Storage(str(tmp_path / "engine.db"))
    s.connect()
    kw = KiwoomMock()
    # base price 등록 (paper fill 시뮬에 필요)
    kw.set_base_price("MCLM26", 80.0)
    return PaperTradingEngine(cfg, s, kw, risk_mgr=RiskManager(cfg.risk))


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


# ──────────────────────────────────────────────
# Phase C4b: process_pair_basis_update / _handle_pair_entry / _handle_pair_exit
# ──────────────────────────────────────────────


def _bootstrap_warmup(engine, pair_id: str, mean: float = 0.0, n: int = 4000):
    """워밍업 통과 위해 가짜 history + 분산 주입.

    SignalGenerator는 std<1.0이면 'Volatility too low'로 차단하므로 std≥3 주입.
    """
    import math as _m
    history = [mean + 5.0 * _m.sin(i * 0.07) for i in range(n)]
    engine.signal_gen.bootstrap_from_db_for_pair(pair_id, history)


@pytest.mark.asyncio
async def test_process_pair_basis_update_unregistered_pair_returns_none(engine):
    sig = await engine.process_pair_basis_update(
        "ghost", 10.0, _q("hl", "x", 80, 80, 80), _q("kis", "y", 80, 80, 80),
    )
    assert sig is None


@pytest.mark.asyncio
async def test_process_pair_basis_update_caches_and_increments_signals(engine):
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    sig = await engine.process_pair_basis_update(
        "wti_cme_hl", 0.0,
        _q("hl", "xyz:CL", 80, 79.99, 80.01),
        _q("kis", "MCLM26", 80, 79.99, 80.01),
    )
    assert engine._state.total_signals == 1
    assert engine.has_both_legs("wti_cme_hl")


@pytest.mark.asyncio
async def test_pair_entry_blocked_by_warmup(engine):
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    # 워밍업 부족 (기본 빈 history) — std<1 fallthrough도 경계 케이스
    engine.signal_gen.bootstrap_from_db_for_pair("wti_cme_hl", [0.0, 0.0, 25.0] * 30)
    sig = await engine.process_pair_basis_update(
        "wti_cme_hl", 30.0,
        _q("hl", "xyz:CL", 80.30, 80.30, 80.30),
        _q("kis", "MCLM26", 80.00, 79.99, 80.01),
    )
    # 진입 시그널이 떴어도 warmup 부족으로 차단
    assert engine._state.total_entries == 0
    # warmup_skip이 0이거나 entry 자체가 안 났을 수 있음 — 둘 다 정상
    assert "wti_cme_hl" not in engine._open_trades_by_pair


@pytest.mark.asyncio
async def test_pair_entry_blocked_by_exec_filter(engine):
    """mid_basis는 큰데 exec_basis가 작은 phantom 시나리오."""
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    _bootstrap_warmup(engine, "wti_cme_hl")

    # mid는 25bp인데 bid/ask cross는 0bp (양 leg가 같은 가격)
    sig = await engine.process_pair_basis_update(
        "wti_cme_hl", 25.0,
        _q("hl", "xyz:CL", 80.20, 80.00, 80.00),       # bid=ask=80
        _q("kis", "MCLM26", 80.00, 80.00, 80.00),
    )
    # entry signal 발생 → exec_filter 차단
    assert engine._state.entry_exec_filter_skip >= 1
    assert "wti_cme_hl" not in engine._open_trades_by_pair


@pytest.mark.asyncio
async def test_pair_entry_blocked_by_min_abs_when_exec_just_above_threshold(engine, cfg):
    """exec=20bp는 entry_threshold 통과하지만 min_abs=10bp는 어차피 통과 — 정상 진입.
    별도로 min_abs 차단은 entry_threshold가 낮을 때만 의미. 현재 두 값이 같으니
    skip 시나리오는 외부 설정에서만 가능. 여기서는 정상 흐름 회귀."""
    cfg.strategy.entry_threshold_bps = 5
    cfg.strategy.min_abs_entry_bps = 15
    s = Storage(":memory:")
    s.connect()
    engine2 = PaperTradingEngine(cfg, s, KiwoomMock(), risk_mgr=RiskManager(cfg.risk))
    p = _pair("wti_cme_hl")
    p.params.entry_threshold_bps = 5
    engine2.register_pair(p)
    _bootstrap_warmup(engine2, "wti_cme_hl")

    # exec_basis ≈ 8bp (entry_threshold=5 통과), min_abs=15 차단
    await engine2.process_pair_basis_update(
        "wti_cme_hl", 10.0,
        _q("hl", "xyz:CL", 80.08, 80.07, 80.09),
        _q("kis", "MCLM26", 80.00, 79.99, 80.01),
    )
    assert engine2._state.entry_min_abs_skip >= 1
    assert "wti_cme_hl" not in engine2._open_trades_by_pair


@pytest.mark.asyncio
async def test_pair_entry_succeeds_and_records_state(engine):
    """깨끗한 진입 시그널 — 양 leg fill + state 기록 + DB 저장."""
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    _bootstrap_warmup(engine, "wti_cme_hl")

    # 30bp long_basis: leg_a(perp)이 leg_b 위
    await engine.process_pair_basis_update(
        "wti_cme_hl", 30.0,
        _q("hl", "xyz:CL", 80.24, 80.23, 80.25),
        _q("kis", "MCLM26", 80.00, 79.99, 80.01),
    )
    assert engine._state.total_entries == 1
    assert engine._state.open_positions == 1
    assert "wti_cme_hl" in engine._open_trades_by_pair
    trade = engine._open_trades_by_pair["wti_cme_hl"]
    assert trade.direction == "long_basis"
    assert trade.perp_side == "short"
    assert trade.futures_side == "long"
    # leg_a fill = bid (sell)
    assert trade.perp_entry_price == 80.23
    # DB
    rows = engine.storage.conn.execute(
        "SELECT pair_id, exchange FROM orders WHERE pair_id='wti_cme_hl'"
    ).fetchall()
    assert len(rows) == 2
    exchanges = {r["exchange"] for r in rows}
    assert exchanges == {"hyperliquid", "kis"}


@pytest.mark.asyncio
async def test_pair_entry_then_exit_cycle(engine):
    """진입 → 수렴 → 청산 1싸이클."""
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    _bootstrap_warmup(engine, "wti_cme_hl")

    # ENTRY 30bp long_basis
    await engine.process_pair_basis_update(
        "wti_cme_hl", 30.0,
        _q("hl", "xyz:CL", 80.24, 80.23, 80.25),
        _q("kis", "MCLM26", 80.00, 79.99, 80.01),
    )
    assert engine._state.total_entries == 1

    # 수렴 — basis가 1bp로 줄어들어 spread 수렴 → EXIT 시그널
    # (signals.py의 convergence_target_bps=3 기본값)
    # short_basis 청산을 위해 leg_a 가격 leg_b와 동일
    await engine.process_pair_basis_update(
        "wti_cme_hl", 1.0,
        _q("hl", "xyz:CL", 80.01, 80.00, 80.01),
        _q("kis", "MCLM26", 80.00, 79.99, 80.01),
    )
    assert engine._state.total_exits == 1
    assert engine._state.open_positions == 0
    assert engine._state.closed_trades == 1
    assert "wti_cme_hl" not in engine._open_trades_by_pair
    assert len(engine._closed_trades) == 1


@pytest.mark.asyncio
async def test_pair_exit_without_open_trade_logs_warning(engine, caplog):
    """오픈 포지션 없는데 exit 시그널 — 경고 로그만, state 변화 없음."""
    p = _pair("wti_cme_hl")
    engine.register_pair(p)
    _bootstrap_warmup(engine, "wti_cme_hl")
    # 직접 exit 시그널 시뮬: signal_gen이 EXIT 내려야 하지만 포지션이 없으면 안 내려옴.
    # 우회: 직접 _handle_pair_exit 호출
    fake_sig = Signal(
        type=SignalType.EXIT, product="wti_cme_hl",
        basis_bps=0, basis_mean=0, basis_std=0,
    )
    await engine._handle_pair_exit(
        "wti_cme_hl", fake_sig,
        _q("hl", "xyz:CL", 80, 80, 80), _q("kis", "MCLM26", 80, 80, 80),
    )
    # 변화 없음
    assert engine._state.total_exits == 0


def test_calculate_pair_contracts_uses_leg_b_contract_size(engine):
    p = _pair("wti_cme_hl")     # leg_b.contract_size=100 (MCL)
    engine.register_pair(p)
    n = engine._calculate_pair_contracts(p, leg_b_price=80.0)
    assert n >= 1
    # max_position_contracts cap 적용 (config 기본 max=2)
    n_high = engine._calculate_pair_contracts(p, leg_b_price=10.0)
    assert n_high <= engine.config.risk.max_position_contracts


# ──────────────────────────────────────────────
# Phase D: pair.enabled=False shadow gate
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_shadow_pair_blocks_entry(engine):
    """enabled=False 페어는 진입 차단 — basis stats만 누적."""
    p = _pair("wti_hl_lighter")
    p.enabled = False         # shadow
    engine.register_pair(p)
    _bootstrap_warmup(engine, "wti_hl_lighter")

    # 30bp 깨끗한 long_basis 시그널
    await engine.process_pair_basis_update(
        "wti_hl_lighter", 30.0,
        _q("hl", "xyz:CL", 80.24, 80.23, 80.25),
        _q("lighter", "WTI", 80.00, 79.99, 80.01),
    )
    # signal은 발생하지만 enabled=False라 진입 차단
    assert engine._state.total_entries == 0
    assert "wti_hl_lighter" not in engine._open_trades_by_pair
    # signals counter는 증가 (basis update 처리됨)
    assert engine._state.total_signals >= 1


@pytest.mark.asyncio
async def test_enabled_pair_still_enters_normally(engine):
    """enabled=True 페어는 정상 진입."""
    p = _pair("wti_cme_hl")
    p.enabled = True
    engine.register_pair(p)
    _bootstrap_warmup(engine, "wti_cme_hl")
    await engine.process_pair_basis_update(
        "wti_cme_hl", 30.0,
        _q("hl", "xyz:CL", 80.24, 80.23, 80.25),
        _q("kis", "MCLM26", 80.00, 79.99, 80.01),
    )
    assert engine._state.total_entries == 1
