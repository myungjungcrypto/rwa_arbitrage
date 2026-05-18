"""Phase 11d follow-ups — entry lock (no double-fire) + unwind notifier.

5/15 LIVE incident:
1. KIS 초당 거래 한도 초과 → 같은 signal로 1초 안 다중 entry 발사.
   asyncio.Lock per pair로 직렬화.
2. HL fill + KIS fail 케이스에서 unwind logs는 logger.warning 으로 떴지만
   Telegram 알림은 없어서 사용자가 즉각 모름. UNWIND OK / FAILED /
   EXCEPTION 모두 Telegram으로 명시 전송.
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
    def __init__(self, name: str, fill_price: float = 80.0, success: bool = True,
                 slow_seconds: float = 0.0):
        self.name = name
        self.venue_type = VenueType.PERP.value
        self.margin_asset = "USDC"
        self._fill_price = fill_price
        self._success = success
        self._slow = slow_seconds
        self.calls: list[dict] = []

    async def connect(self): return True
    async def disconnect(self): pass
    async def subscribe_quotes(self, *a, **kw): pass
    async def unsubscribe_quotes(self, *a, **kw): pass
    async def get_quote(self, sym): return None

    async def place_order(self, symbol, side, size, order_type="market",
                          limit_price=None, reduce_only=False, client_order_id=None):
        if self._slow > 0:
            await asyncio.sleep(self._slow)
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
        kis_symbol_map={"wti": "MCLN26"},
        hyperliquid=HyperliquidConfig(use_testnet=False),
        kiwoom=KiwoomConfig(use_mock=True),
        kis=KISConfig(),
        strategy=StrategyConfig(cme_closed_skip_entry=False),
        risk=risk or RiskConfig(rollover_block_entry_days=0,
                                rollover_position_reduce_pct=0),
    )
    s = Storage(":memory:"); s.connect()
    kw = KiwoomMock(); kw.set_base_price("MCLN26", 80.0)
    return PaperTradingEngine(cfg, s, kw, risk_mgr=RiskManager(cfg.risk))


def _pair() -> ArbitragePair:
    return ArbitragePair(
        id="wti_cme_hl", enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                          taker_fee_bps=0.9, funding_interval_hours=1.0,
                          margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange="kis", symbol="MCLN26", role=LegRole.DATED_FUTURES,
                          contract_size=100, fee_per_contract_usd=2.5),
        params=PairStrategyParams(),
    )


def _sig() -> Signal:
    return Signal(type=SignalType.ENTRY_SHORT_BASIS, basis_bps=25.0,
                  product="wti", confidence=1.0, reason="test",
                  basis_mean=0.0, basis_std=10.0)


def _quotes() -> tuple[Quote, Quote]:
    leg_a = Quote(exchange="hyperliquid", symbol="xyz:CL", mid_price=80.0,
                  bid=79.95, ask=80.05)
    leg_b = Quote(exchange="kis", symbol="MCLN26", mid_price=80.10,
                  bid=80.09, ask=80.11)
    return leg_a, leg_b


# ──────────────────────────────────────────────
# 1. asyncio.Lock per pair — 동시 entry 차단
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_entry_lock_serializes_concurrent_handle_pair_entry():
    """동시에 _handle_pair_entry 5번 호출되어도 실 진입은 1번만."""
    e = _engine("LIVE")
    p = _pair()
    e.register_pair(p)

    reg = ExchangeRegistry()
    # 첫 호출이 1초 걸리는 동안 다른 호출 들어와도 차단되어야
    hl = _StubAdapter("hyperliquid", fill_price=80.05, slow_seconds=0.3)
    kis = _StubAdapter("kis", fill_price=80.10, slow_seconds=0.3)
    reg.register(hl); reg.register(kis)
    e.set_exchange_registry(reg)

    sig = _sig()
    leg_a, leg_b = _quotes()

    # 5번 동시 발사
    await asyncio.gather(*[
        e._handle_pair_entry(p.id, sig, leg_a, leg_b)
        for _ in range(5)
    ])

    # 양 leg 모두 1번씩만 호출돼야 함
    assert len(hl.calls) == 1, f"HL called {len(hl.calls)} times — lock broken"
    assert len(kis.calls) == 1, f"KIS called {len(kis.calls)} times — lock broken"
    # 진입 기록도 1번
    assert e._state.total_entries == 1


@pytest.mark.asyncio
async def test_entry_lock_released_after_completion():
    """첫 entry가 끝나면 lock 해제 — 다음 entry 진입 가능 (close 후)."""
    e = _engine("LIVE")
    p = _pair()
    e.register_pair(p)
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", fill_price=80.05))
    reg.register(_StubAdapter("kis", fill_price=80.10))
    e.set_exchange_registry(reg)

    sig = _sig()
    leg_a, leg_b = _quotes()
    # 첫 entry
    await e._handle_pair_entry(p.id, sig, leg_a, leg_b)
    assert e._state.total_entries == 1
    # lock 해제됐는지 — locked() False
    assert not e._pair_entry_locks[p.id].locked()


@pytest.mark.asyncio
async def test_entry_lock_skipped_when_already_open():
    """이미 open된 페어면 lock 획득 전에 즉시 skip."""
    e = _engine("LIVE")
    p = _pair()
    e.register_pair(p)

    # 이미 open 상태로 주입
    from src.paper.engine import TradeRecord
    e._open_trades_by_pair[p.id] = TradeRecord(
        trade_id=999, product="wti", direction="short_basis",
        entry_time=time.time(), entry_basis_bps=20.0,
        perp_entry_price=80.0, futures_entry_price=80.05,
        perp_side="long", futures_side="short",
        size_contracts=1, perp_units=100.0, status="open",
    )

    reg = ExchangeRegistry()
    hl = _StubAdapter("hyperliquid")
    reg.register(hl); reg.register(_StubAdapter("kis"))
    e.set_exchange_registry(reg)

    sig = _sig()
    leg_a, leg_b = _quotes()
    await e._handle_pair_entry(p.id, sig, leg_a, leg_b)
    # adapter 호출 0번
    assert len(hl.calls) == 0


# ──────────────────────────────────────────────
# 2. emergency unwind — notifier 알림
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_unwind_ok_sends_telegram_notification():
    """leg_a 체결 + leg_b 실패 → unwind 성공 → 🛟 UNWIND OK 알림."""
    e = _engine("LIVE")
    p = _pair()
    e.register_pair(p)
    reg = ExchangeRegistry()
    hl = _StubAdapter("hyperliquid", fill_price=80.0)
    kis = _StubAdapter("kis", fill_price=80.10)
    reg.register(hl); reg.register(kis)
    e.set_exchange_registry(reg)

    notif = MagicMock(); notif.send_sync = MagicMock()
    e.set_notifier(notif)

    # leg_a만 체결한 상황 시뮬
    await e._emergency_unwind_partial_entry(
        p, leg_a_side="buy", leg_a_size=100.0, a_price=80.0, a_oid="hl-1",
        leg_b_side="sell", leg_b_size=1.0, b_price=0.0, b_oid="",
    )

    # HL stub이 sell reduce_only 호출됐어야
    assert any(c["reduce_only"] for c in hl.calls)
    msgs = [c.args[0] for c in notif.send_sync.call_args_list]
    assert any("UNWIND OK" in m for m in msgs), msgs


@pytest.mark.asyncio
async def test_unwind_failed_sends_critical_alert():
    """unwind dispatch가 실패하면 🚨 UNWIND FAILED + MANUAL INTERVENTION 알림."""
    e = _engine("LIVE")
    p = _pair()
    e.register_pair(p)
    reg = ExchangeRegistry()
    # HL이 fail 응답
    hl = _StubAdapter("hyperliquid", success=False)
    reg.register(hl); reg.register(_StubAdapter("kis"))
    e.set_exchange_registry(reg)

    notif = MagicMock(); notif.send_sync = MagicMock()
    e.set_notifier(notif)

    await e._emergency_unwind_partial_entry(
        p, leg_a_side="buy", leg_a_size=100.0, a_price=80.0, a_oid="hl-1",
        leg_b_side="sell", leg_b_size=1.0, b_price=0.0, b_oid="",
    )

    msgs = [c.args[0] for c in notif.send_sync.call_args_list]
    assert any("UNWIND FAILED" in m and "MANUAL INTERVENTION" in m for m in msgs), msgs


@pytest.mark.asyncio
async def test_unwind_both_failed_no_notification():
    """양 leg 모두 fail이면 unwind 호출 안 함 → 알림 0회."""
    e = _engine("LIVE")
    p = _pair()
    e.register_pair(p)
    reg = ExchangeRegistry()
    hl = _StubAdapter("hyperliquid")
    reg.register(hl); reg.register(_StubAdapter("kis"))
    e.set_exchange_registry(reg)

    notif = MagicMock(); notif.send_sync = MagicMock()
    e.set_notifier(notif)

    await e._emergency_unwind_partial_entry(
        p, leg_a_side="buy", leg_a_size=100.0, a_price=0.0, a_oid="",
        leg_b_side="sell", leg_b_size=1.0, b_price=0.0, b_oid="",
    )
    # adapter 호출 0번, 알림 0번
    assert len(hl.calls) == 0
    assert notif.send_sync.call_count == 0
