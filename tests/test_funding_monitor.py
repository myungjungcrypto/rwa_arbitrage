"""FundingIntervalMonitor + FundingInfo 회귀 테스트."""

from __future__ import annotations

import asyncio
import time
from typing import Optional

import pytest

from src.exchange.base import (
    ExchangeBase,
    FundingInfo,
    OrderResult,
    Position,
    Quote,
    VenueType,
)
from src.exchange.registry import ExchangeRegistry
from src.strategy.funding_monitor import FundingIntervalMonitor, FundingMismatch
from src.strategy.pair import (
    ArbitragePair,
    ExchangeLeg,
    LegRole,
    PairGate,
    PairStrategyParams,
)


# ──────────────────────────────────────────────
# FundingInfo dataclass
# ──────────────────────────────────────────────


def test_funding_info_matches_expected_within_tolerance():
    info = FundingInfo(exchange="binance", symbol="CLUSDT",
                      current_rate=0.0001, next_settlement_ts=time.time() + 3600,
                      observed_interval_hours=4.0)
    assert info.matches_expected(4.0)
    assert info.matches_expected(4.05, tolerance_hours=0.1)
    assert info.matches_expected(3.95, tolerance_hours=0.1)


def test_funding_info_does_not_match_outside_tolerance():
    info = FundingInfo(exchange="okx", symbol="CL-USDT-SWAP",
                      current_rate=0.0, next_settlement_ts=0,
                      observed_interval_hours=8.0)
    assert not info.matches_expected(4.0)


# ──────────────────────────────────────────────
# Stub adapter
# ──────────────────────────────────────────────


class _StubAdapter:
    def __init__(self, name: str, funding_info: Optional[FundingInfo] = None,
                 raise_on_funding: bool = False):
        self.name = name
        self.venue_type = VenueType.PERP.value
        self.margin_asset = "USDC"
        self._funding_info = funding_info
        self._raise = raise_on_funding
        self.calls: list[str] = []

    async def connect(self): return True
    async def disconnect(self): pass
    async def subscribe_quotes(self, symbol, callback, *, contract_size=1.0): pass
    async def unsubscribe_quotes(self, symbol): pass
    async def get_quote(self, symbol): return None
    async def place_order(self, *args, **kwargs): return OrderResult(success=False)
    async def cancel_order(self, symbol, order_id): return False
    async def get_positions(self): return []
    async def get_account_value(self): return 0.0

    async def get_funding_info(self, symbol):
        self.calls.append(symbol)
        if self._raise:
            raise RuntimeError("boom")
        return self._funding_info


def _build_pair(pair_id: str, leg_b_exchange: str, leg_b_symbol: str,
                leg_b_funding: float = 4.0) -> ArbitragePair:
    return ArbitragePair(
        id=pair_id, enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                         taker_fee_bps=0.9, funding_interval_hours=1.0,
                         margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange=leg_b_exchange, symbol=leg_b_symbol, role=LegRole.PERP,
                         taker_fee_bps=4.0, funding_interval_hours=leg_b_funding,
                         margin_asset="USDT"),
        params=PairStrategyParams(),
    )


# ──────────────────────────────────────────────
# verify_once
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_monitor_no_mismatch_when_intervals_match():
    hl_info = FundingInfo(exchange="hyperliquid", symbol="xyz:CL",
                         current_rate=0.0001, next_settlement_ts=0,
                         observed_interval_hours=1.0)
    bn_info = FundingInfo(exchange="binance", symbol="CLUSDT",
                         current_rate=0.0001, next_settlement_ts=0,
                         observed_interval_hours=4.0)
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", hl_info))
    reg.register(_StubAdapter("binance", bn_info))
    pair = _build_pair("wti_hl_binance", "binance", "CLUSDT", leg_b_funding=4.0)
    monitor = FundingIntervalMonitor(reg, [pair])
    mismatches = await monitor.verify_once()
    assert mismatches == []


@pytest.mark.asyncio
async def test_monitor_detects_mismatch_on_leg_b():
    """OKX가 정책 변경해 8h로 보고하는데 config는 4h인 시나리오."""
    hl_info = FundingInfo(exchange="hyperliquid", symbol="xyz:CL",
                         current_rate=0.0, next_settlement_ts=0,
                         observed_interval_hours=1.0)
    okx_info = FundingInfo(exchange="okx", symbol="CL-USDT-SWAP",
                          current_rate=0.0, next_settlement_ts=0,
                          observed_interval_hours=8.0)
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", hl_info))
    reg.register(_StubAdapter("okx", okx_info))
    pair = _build_pair("wti_hl_okx", "okx", "CL-USDT-SWAP", leg_b_funding=4.0)
    monitor = FundingIntervalMonitor(reg, [pair])
    mismatches = await monitor.verify_once()
    assert len(mismatches) == 1
    m = mismatches[0]
    assert m.pair_id == "wti_hl_okx"
    assert m.leg == "b"
    assert m.exchange == "okx"
    assert m.expected_hours == 4.0
    assert m.observed_hours == 8.0


@pytest.mark.asyncio
async def test_monitor_skips_dated_futures_leg():
    """CME 월물은 funding 개념 없음 → check 안 함."""
    hl_info = FundingInfo(exchange="hyperliquid", symbol="xyz:CL",
                         current_rate=0.0, next_settlement_ts=0,
                         observed_interval_hours=1.0)
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", hl_info))
    # KIS adapter는 funding_info=None 반환 (이 테스트에선 굳이 등록 안 해도 됨)
    pair = ArbitragePair(
        id="wti_cme_hl", enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                         taker_fee_bps=0.9, funding_interval_hours=1.0,
                         margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange="kis", symbol="MCLM26", role=LegRole.DATED_FUTURES,
                         contract_size=100, fee_per_contract_usd=2.50,
                         funding_interval_hours=0.0),
        params=PairStrategyParams(),
    )
    monitor = FundingIntervalMonitor(reg, [pair])
    mismatches = await monitor.verify_once()
    assert mismatches == []


@pytest.mark.asyncio
async def test_monitor_skips_unregistered_exchange():
    """어댑터 합류 전 단계 — config에는 있지만 registry에 없는 거래소."""
    reg = ExchangeRegistry()
    pair = _build_pair("wti_hl_lighter", "lighter", "WTI", leg_b_funding=1.0)
    # 어떤 adapter도 등록 X
    monitor = FundingIntervalMonitor(reg, [pair])
    mismatches = await monitor.verify_once()
    assert mismatches == []


@pytest.mark.asyncio
async def test_monitor_skips_when_funding_info_returns_none():
    """get_funding_info가 일시적으로 None 반환 — skip."""
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", None))   # None 반환
    reg.register(_StubAdapter("binance", None))
    pair = _build_pair("wti_hl_binance", "binance", "CLUSDT", leg_b_funding=4.0)
    monitor = FundingIntervalMonitor(reg, [pair])
    mismatches = await monitor.verify_once()
    assert mismatches == []


@pytest.mark.asyncio
async def test_monitor_continues_after_adapter_exception():
    """한 거래소가 raise해도 다른 페어/leg는 계속 검증."""
    bn_info = FundingInfo(exchange="binance", symbol="CLUSDT",
                         current_rate=0.0, next_settlement_ts=0,
                         observed_interval_hours=8.0)   # mismatch
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", None, raise_on_funding=True))
    reg.register(_StubAdapter("binance", bn_info))
    pair = _build_pair("wti_hl_binance", "binance", "CLUSDT", leg_b_funding=4.0)
    monitor = FundingIntervalMonitor(reg, [pair])
    mismatches = await monitor.verify_once()
    # leg_a (HL)는 raise → skip, leg_b (Binance)는 mismatch 1개 검출
    assert len(mismatches) == 1
    assert mismatches[0].leg == "b"


@pytest.mark.asyncio
async def test_monitor_callback_invoked_on_mismatch():
    okx_info = FundingInfo(exchange="okx", symbol="CL-USDT-SWAP",
                          current_rate=0.0, next_settlement_ts=0,
                          observed_interval_hours=8.0)
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", FundingInfo(
        exchange="hyperliquid", symbol="xyz:CL", current_rate=0.0,
        next_settlement_ts=0, observed_interval_hours=1.0)))
    reg.register(_StubAdapter("okx", okx_info))
    pair = _build_pair("wti_hl_okx", "okx", "CL-USDT-SWAP", leg_b_funding=4.0)

    received: list[FundingMismatch] = []

    async def cb(m: FundingMismatch) -> None:
        received.append(m)

    monitor = FundingIntervalMonitor(reg, [pair], on_mismatch=cb)
    mismatches = await monitor.verify_once()
    assert len(mismatches) == 1
    assert len(received) == 1
    assert received[0].observed_hours == 8.0


@pytest.mark.asyncio
async def test_monitor_multiple_pairs_aggregated():
    """3개 페어 중 2개만 mismatch — 모두 보고."""
    hl_info = FundingInfo(exchange="hyperliquid", symbol="xyz:CL",
                         current_rate=0.0, next_settlement_ts=0,
                         observed_interval_hours=1.0)
    bn_info_ok = FundingInfo(exchange="binance", symbol="CLUSDT",
                            current_rate=0.0, next_settlement_ts=0,
                            observed_interval_hours=4.0)
    okx_info_bad = FundingInfo(exchange="okx", symbol="CL-USDT-SWAP",
                              current_rate=0.0, next_settlement_ts=0,
                              observed_interval_hours=8.0)
    bb_info_bad = FundingInfo(exchange="bybit", symbol="CLUSDT",
                             current_rate=0.0, next_settlement_ts=0,
                             observed_interval_hours=1.0)
    reg = ExchangeRegistry()
    reg.register(_StubAdapter("hyperliquid", hl_info))
    reg.register(_StubAdapter("binance", bn_info_ok))
    reg.register(_StubAdapter("okx", okx_info_bad))
    reg.register(_StubAdapter("bybit", bb_info_bad))
    pairs = [
        _build_pair("wti_hl_binance", "binance", "CLUSDT", leg_b_funding=4.0),
        _build_pair("wti_hl_okx", "okx", "CL-USDT-SWAP", leg_b_funding=4.0),
        _build_pair("wti_hl_bybit", "bybit", "CLUSDT", leg_b_funding=4.0),
    ]
    monitor = FundingIntervalMonitor(reg, pairs)
    mismatches = await monitor.verify_once()
    assert len(mismatches) == 2
    pair_ids = {m.pair_id for m in mismatches}
    assert pair_ids == {"wti_hl_okx", "wti_hl_bybit"}
