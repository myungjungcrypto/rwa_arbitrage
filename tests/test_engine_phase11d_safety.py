"""Phase 11d 회귀 — 안전장치.

1. live_max_contracts_per_pair hard cap (LIVE only).
2. WS quote_freshness_watchdog stale 감지 + auto-flatten.
3. Notifier hook (risk-block, fill-fail, ws-stale, flatten).
"""

from __future__ import annotations

import asyncio
import time
from unittest.mock import MagicMock

import pytest

from src.data.storage import Storage
from src.exchange.base import OrderResult, Quote, VenueType
from src.exchange.kiwoom import KiwoomMock
from src.exchange.registry import ExchangeRegistry
from src.paper.engine import PaperTradingEngine, TradeRecord
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
    def __init__(self, name: str, fill_price: float = 80.0, success: bool = True):
        self.name = name
        self.venue_type = VenueType.PERP.value
        self.margin_asset = "USDC"
        self._fill_price = fill_price
        self._success = success
        self.calls: list[dict] = []

    async def connect(self): return True
    async def disconnect(self): pass
    async def subscribe_quotes(self, *a, **kw): pass
    async def unsubscribe_quotes(self, *a, **kw): pass
    async def get_quote(self, symbol): return None

    async def place_order(self, symbol, side, size, order_type="market",
                          limit_price=None, reduce_only=False, client_order_id=None):
        self.calls.append(dict(symbol=symbol, side=side, size=size,
                               reduce_only=reduce_only))
        return OrderResult(
            success=self._success, exchange=self.name, symbol=symbol,
            order_id=f"{self.name}-{len(self.calls)}",
            filled_size=size if self._success else 0.0,
            filled_price=self._fill_price if self._success else 0.0,
        )

    async def cancel_order(self, *a, **kw): return False
    async def get_positions(self): return []
    async def get_account_value(self): return 0.0


def _engine(mode: str = "LIVE", risk: RiskConfig | None = None) -> PaperTradingEngine:
    cfg = AppConfig(
        mode=mode,
        products={"wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL",
                                        contract_size=100, futures_fee_per_contract=2.5)},
        kis_symbol_map={"wti": "MCLM26"},
        hyperliquid=HyperliquidConfig(use_testnet=False),
        kiwoom=KiwoomConfig(use_mock=True),
        kis=KISConfig(),
        strategy=StrategyConfig(cme_closed_skip_entry=False),
        risk=risk or RiskConfig(),
    )
    s = Storage(":memory:"); s.connect()
    kw = KiwoomMock(); kw.set_base_price("MCLM26", 80.0)
    return PaperTradingEngine(cfg, s, kw, risk_mgr=RiskManager(cfg.risk))


def _pair(pid: str = "wti_cme_hl") -> ArbitragePair:
    return ArbitragePair(
        id=pid, enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                          taker_fee_bps=0.9, funding_interval_hours=1.0,
                          margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange="kis", symbol="MCLM26", role=LegRole.DATED_FUTURES,
                          contract_size=100, fee_per_contract_usd=2.5),
        params=PairStrategyParams(),
    )


def _q(exchange, symbol, mid, bid=0.0, ask=0.0) -> Quote:
    return Quote(exchange=exchange, symbol=symbol, mid_price=mid, bid=bid, ask=ask)


# ──────────────────────────────────────────────
# 1. live_max_contracts_per_pair cap
# ──────────────────────────────────────────────


def test_live_per_pair_cap_clips_size():
    """LIVE 모드에서 per-pair cap이 max_position_contracts보다 작으면 그것을 적용."""
    risk = RiskConfig(
        max_position_usd=1_000_000,    # 매우 큼 (cap 우회)
        max_position_contracts=10,
        live_max_contracts_per_pair={"wti_cme_hl": 1},
    )
    e = _engine("LIVE", risk=risk)
    p = _pair()
    e.register_pair(p)
    contracts = e._calculate_pair_contracts(p, leg_b_price=80.0)
    assert contracts == 1


def test_paper_mode_ignores_per_pair_cap():
    """PAPER 모드에서는 live_max_contracts_per_pair 무시 (시뮬용 그대로)."""
    risk = RiskConfig(
        max_position_usd=1_000_000,
        max_position_contracts=10,
        live_max_contracts_per_pair={"wti_cme_hl": 1},
    )
    e = _engine("PAPER", risk=risk)
    p = _pair()
    e.register_pair(p)
    contracts = e._calculate_pair_contracts(p, leg_b_price=80.0)
    # max_position_usd=1M, leg_b_price=80, contract_size=100 → max 125 → cap by max_position_contracts=10
    assert contracts == 10


def test_live_per_pair_cap_falls_through_when_unset():
    """LIVE 모드인데 페어 cap 미등록이면 max_position_contracts만 적용."""
    risk = RiskConfig(
        max_position_usd=1_000_000,
        max_position_contracts=5,
        live_max_contracts_per_pair={"other_pair": 1},
    )
    e = _engine("LIVE", risk=risk)
    p = _pair()
    e.register_pair(p)
    contracts = e._calculate_pair_contracts(p, leg_b_price=80.0)
    assert contracts == 5


# ──────────────────────────────────────────────
# 2. quote_freshness_watchdog
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_watchdog_paper_mode_exits_immediately():
    e = _engine("PAPER")
    # interval=1 이지만 mode=PAPER라 즉시 return
    await asyncio.wait_for(
        e.quote_freshness_watchdog(interval_seconds=1, stop_event=asyncio.Event()),
        timeout=1.0,
    )


@pytest.mark.asyncio
async def test_watchdog_emits_stale_alert_in_live():
    """LIVE 모드 + stale leg → notifier로 STALE 알림."""
    risk = RiskConfig(ws_stale_seconds=1.0, ws_stale_auto_flatten=False)
    e = _engine("LIVE", risk=risk)
    p = _pair()
    e.register_pair(p)
    # leg_a, leg_b 모두 2초 전 timestamp로 stale 만들기
    e._last_quote_ts[(p.id, "a")] = time.time() - 5
    e._last_quote_ts[(p.id, "b")] = time.time() - 5

    notif = MagicMock()
    notif.send_sync = MagicMock()
    e.set_notifier(notif)

    stop = asyncio.Event()

    async def stop_after():
        await asyncio.sleep(0.3)
        stop.set()

    await asyncio.gather(
        e.quote_freshness_watchdog(interval_seconds=0.1, stop_event=stop),
        stop_after(),
    )
    # 적어도 한 leg에 대한 STALE 알림
    msgs = [c.args[0] for c in notif.send_sync.call_args_list]
    assert any("STALE" in m for m in msgs), msgs


@pytest.mark.asyncio
async def test_watchdog_auto_flatten_calls_emergency_flatten():
    """ws_stale_auto_flatten=True + 오픈 포지션 있으면 emergency_flatten_pair 호출."""
    risk = RiskConfig(ws_stale_seconds=0.5, ws_stale_auto_flatten=True)
    e = _engine("LIVE", risk=risk)
    p = _pair()
    e.register_pair(p)

    # 오픈 포지션 직접 주입
    e._open_trades_by_pair[p.id] = TradeRecord(
        trade_id=1, product="wti", direction="short_basis",
        entry_time=time.time(), entry_basis_bps=20.0,
        perp_entry_price=80.0, futures_entry_price=80.05,
        perp_side="long", futures_side="short",
        size_contracts=1, perp_units=100.0, status="open",
    )
    # stale state
    e._last_quote_ts[(p.id, "a")] = time.time() - 5
    e._last_quote_ts[(p.id, "b")] = time.time() - 5

    # registry로 stub adapter 등록 → flatten이 dispatch_pair_order로 실 호출
    reg = ExchangeRegistry()
    hl = _StubAdapter("hyperliquid", fill_price=80.05)
    kis = _StubAdapter("kis", fill_price=80.10)
    reg.register(hl); reg.register(kis)
    e.set_exchange_registry(reg)

    stop = asyncio.Event()

    async def stop_after():
        await asyncio.sleep(0.3)
        stop.set()

    await asyncio.gather(
        e.quote_freshness_watchdog(interval_seconds=0.1, stop_event=stop),
        stop_after(),
    )
    # 양 leg 모두 reduce_only 청산 호출
    assert any(c["reduce_only"] for c in hl.calls), hl.calls
    assert any(c["reduce_only"] for c in kis.calls), kis.calls
    # 청산 성공 시 open_trades에서 제거
    assert p.id not in e._open_trades_by_pair


# ──────────────────────────────────────────────
# 3. emergency_flatten_pair direct
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_emergency_flatten_paper_mode_noop():
    """PAPER 모드에서는 flatten False 반환 (시뮬 외 경로 X)."""
    e = _engine("PAPER")
    p = _pair()
    e.register_pair(p)
    e._open_trades_by_pair[p.id] = TradeRecord(
        trade_id=1, product="wti", direction="long_basis",
        entry_time=time.time(), entry_basis_bps=-20.0,
        perp_entry_price=80.0, futures_entry_price=80.05,
        perp_side="short", futures_side="long",
        size_contracts=1, perp_units=100.0, status="open",
    )
    ok = await e.emergency_flatten_pair(p.id, reason="test")
    assert ok is False
    # 포지션 그대로 남음
    assert p.id in e._open_trades_by_pair


@pytest.mark.asyncio
async def test_emergency_flatten_long_basis_reverses_sides():
    """long_basis 진입 (leg_a sell, leg_b buy) 청산 → leg_a buy, leg_b sell."""
    e = _engine("LIVE")
    p = _pair()
    e.register_pair(p)

    e._open_trades_by_pair[p.id] = TradeRecord(
        trade_id=1, product="wti", direction="long_basis",
        entry_time=time.time(), entry_basis_bps=-20.0,
        perp_entry_price=80.0, futures_entry_price=80.05,
        perp_side="short", futures_side="long",
        size_contracts=2, perp_units=200.0, status="open",
    )
    reg = ExchangeRegistry()
    hl = _StubAdapter("hyperliquid")
    kis = _StubAdapter("kis")
    reg.register(hl); reg.register(kis)
    e.set_exchange_registry(reg)

    ok = await e.emergency_flatten_pair(p.id, reason="manual")
    assert ok is True
    assert hl.calls[0]["side"] == "buy"     # leg_a 반대
    assert hl.calls[0]["size"] == 200.0
    assert hl.calls[0]["reduce_only"] is True
    assert kis.calls[0]["side"] == "sell"   # leg_b 반대
    assert kis.calls[0]["size"] == 2.0
    assert kis.calls[0]["reduce_only"] is True
    assert p.id not in e._open_trades_by_pair
