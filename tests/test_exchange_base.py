"""Phase A 회귀 — ExchangeBase protocol + Quote dataclass + Registry."""

from __future__ import annotations

import pytest

from src.exchange.base import (
    ExchangeBase,
    OrderResult,
    Position,
    Quote,
    VenueType,
)
from src.exchange.registry import ExchangeRegistry


# ──────────────────────────────────────────────
# Quote dataclass
# ──────────────────────────────────────────────


def test_quote_default_optional_fields():
    q = Quote(exchange="hyperliquid", symbol="xyz:CL",
              mid_price=80.0, bid=79.95, ask=80.05)
    assert q.bid_qty == 0.0
    assert q.ask_qty == 0.0
    assert q.index_price == 0.0
    assert q.funding_rate == 0.0
    assert q.funding_interval_hours == 0.0
    assert q.contract_month == ""
    assert q.timestamp > 0


def test_quote_spread_bps():
    q = Quote(exchange="binance", symbol="CLUSDT",
              mid_price=100.0, bid=99.95, ask=100.05)
    # spread = (100.05 - 99.95) / 100.0 * 10000 = 10.0
    assert q.spread_bps == pytest.approx(10.0)


def test_quote_spread_bps_zero_mid():
    q = Quote(exchange="x", symbol="y", mid_price=0.0, bid=0.0, ask=0.0)
    assert q.spread_bps == 0.0


def test_quote_basis_bps_perp():
    q = Quote(exchange="hyperliquid", symbol="xyz:CL",
              mid_price=80.50, bid=80.48, ask=80.52,
              index_price=80.0, funding_rate=0.00001,
              funding_interval_hours=1.0)
    # (80.50 - 80.0) / 80.0 * 10000 = 62.5 bps
    assert q.basis_bps == pytest.approx(62.5)


def test_quote_basis_bps_dated_futures_returns_zero():
    """월물(KIS)은 index_price=0 → basis_bps = 0 (의미 없음)."""
    q = Quote(exchange="kis", symbol="MCLM26",
              mid_price=80.0, bid=79.95, ask=80.05,
              contract_month="MCLM26")
    assert q.basis_bps == 0.0


# ──────────────────────────────────────────────
# OrderResult / Position
# ──────────────────────────────────────────────


def test_order_result_default():
    r = OrderResult(success=False)
    assert r.success is False
    assert r.exchange == ""
    assert r.order_id == ""
    assert r.error == ""


def test_order_result_success():
    r = OrderResult(success=True, exchange="hyperliquid", symbol="xyz:CL",
                    order_id="123", filled_size=1.0, filled_price=80.5)
    assert r.success
    assert r.filled_size == 1.0


def test_position_default():
    p = Position(exchange="hyperliquid", symbol="xyz:CL",
                 size=2.0, entry_price=80.0, mark_price=80.5)
    assert p.unrealized_pnl == 0.0
    assert p.leverage == 1.0


# ──────────────────────────────────────────────
# Registry
# ──────────────────────────────────────────────


class _StubExchange:
    """ExchangeBase 호환 stub — registry 테스트용."""

    def __init__(self, name: str):
        self.name = name
        self.venue_type = VenueType.PERP.value
        self.margin_asset = "USDC"

    async def connect(self): return True
    async def disconnect(self): pass
    async def subscribe_quotes(self, symbol, callback, *, contract_size=1.0): pass
    async def unsubscribe_quotes(self, symbol): pass
    async def get_quote(self, symbol): return None
    async def place_order(self, *args, **kwargs): return OrderResult(success=False)
    async def cancel_order(self, symbol, order_id): return False
    async def get_positions(self): return []
    async def get_account_value(self): return 0.0


def test_registry_register_and_get():
    reg = ExchangeRegistry()
    ex = _StubExchange("binance")
    reg.register(ex)
    assert reg.has("binance")
    assert reg.get("binance") is ex
    assert "binance" in reg
    assert reg.names() == ["binance"]
    assert len(reg) == 1


def test_registry_get_unknown_raises():
    reg = ExchangeRegistry()
    with pytest.raises(KeyError, match="Available"):
        reg.get("unknown")


def test_registry_register_overwrites():
    reg = ExchangeRegistry()
    ex1 = _StubExchange("hyperliquid")
    ex2 = _StubExchange("hyperliquid")
    reg.register(ex1)
    reg.register(ex2)
    assert reg.get("hyperliquid") is ex2
    assert len(reg) == 1


def test_registry_register_multiple():
    reg = ExchangeRegistry()
    for n in ("hyperliquid", "kis", "binance", "bybit", "okx", "lighter"):
        reg.register(_StubExchange(n))
    assert reg.names() == sorted(["hyperliquid", "kis", "binance", "bybit", "okx", "lighter"])


def test_registry_register_requires_name():
    reg = ExchangeRegistry()
    bad = _StubExchange("")
    with pytest.raises(ValueError):
        reg.register(bad)


# ──────────────────────────────────────────────
# Adapter protocol conformance (existing adapters)
# ──────────────────────────────────────────────


def test_hyperliquid_adapter_satisfies_protocol():
    """HyperliquidExchange가 ExchangeBase protocol을 만족하는지."""
    from src.exchange.hyperliquid import (
        HyperliquidClient, HyperliquidExchange, HyperliquidWebSocket,
    )
    rest = HyperliquidClient(use_testnet=False)
    ws = HyperliquidWebSocket(use_testnet=False)
    adapter = HyperliquidExchange(rest=rest, ws=ws)
    assert adapter.name == "hyperliquid"
    assert adapter.venue_type == VenueType.PERP.value
    assert adapter.margin_asset == "USDC"
    assert isinstance(adapter, ExchangeBase)


def test_kis_adapter_satisfies_protocol():
    from src.exchange.kis import KISAuth, KISExchange, KISFuturesClient
    auth = KISAuth(app_key="test", app_secret="test")
    client = KISFuturesClient(auth)
    adapter = KISExchange(client)
    assert adapter.name == "kis"
    assert adapter.venue_type == VenueType.DATED_FUTURES.value
    assert isinstance(adapter, ExchangeBase)
