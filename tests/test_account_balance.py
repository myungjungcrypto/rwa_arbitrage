"""Phase: account_balance v4 마이그레이션 + balance polling 테스트."""

from __future__ import annotations

import asyncio
import os
import tempfile
import time

import pytest

from src.data.storage import Storage, SCHEMA_VERSION
from src.exchange.base import OrderResult, VenueType
from src.exchange.kiwoom import KiwoomMock
from src.exchange.registry import ExchangeRegistry
from src.paper.engine import PaperTradingEngine
from src.risk.manager import RiskManager
from src.utils.config import (
    AppConfig, HyperliquidConfig, KISConfig, KiwoomConfig,
    ProductConfig, RiskConfig, StrategyConfig,
)


# ──────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────


@pytest.fixture
def fresh_db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    yield path
    try: os.remove(path)
    except OSError: pass


class _BalAdapter:
    """get_account_value()만 정의된 stub adapter."""
    venue_type = VenueType.PERP.value
    def __init__(self, name: str, value: float = 0.0,
                 currency: str = "USD", raise_exc: Exception | None = None):
        self.name = name
        self.margin_asset = currency
        self._value = value
        self._raise = raise_exc
    async def connect(self): return True
    async def disconnect(self): pass
    async def subscribe_quotes(self, *a, **kw): pass
    async def unsubscribe_quotes(self, *a, **kw): pass
    async def get_quote(self, sym): return None
    async def place_order(self, **kw):
        return OrderResult(success=True)
    async def cancel_order(self, *a, **kw): return False
    async def get_positions(self): return []
    async def get_account_value(self):
        if self._raise:
            raise self._raise
        return self._value


# ──────────────────────────────────────────────
# Schema v4 + Storage methods
# ──────────────────────────────────────────────


def test_schema_v4_has_account_balance_table(fresh_db):
    s = Storage(fresh_db); s.connect()
    assert SCHEMA_VERSION >= 4
    n = s.conn.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='account_balance'"
    ).fetchone()[0]
    assert n == 1
    s.close()


def test_save_and_get_latest_balances(fresh_db):
    s = Storage(fresh_db); s.connect()
    s.save_account_balance("hyperliquid", 5000.50, "USDC")
    s.save_account_balance("kis", 12345.67, "USD")
    # 같은 거래소 두 번째 row — get_latest는 새 것 반환
    s.save_account_balance("hyperliquid", 5100.00, "USDC")
    out = s.get_latest_balances()
    by_ex = {b["exchange"]: b for b in out}
    assert len(by_ex) == 2
    assert by_ex["hyperliquid"]["value"] == 5100.00
    assert by_ex["kis"]["value"] == 12345.67
    s.close()


def test_save_account_balance_with_error_note(fresh_db):
    s = Storage(fresh_db); s.connect()
    s.save_account_balance("kis", 0.0, "USD", note="error: timeout")
    rows = s.get_latest_balances()
    assert rows[0]["note"] == "error: timeout"
    assert rows[0]["value"] == 0.0
    s.close()


def test_get_balance_history_filters_by_window(fresh_db):
    s = Storage(fresh_db); s.connect()
    # 과거 + 현재 row 직접 INSERT
    now = time.time()
    s.conn.execute(
        "INSERT INTO account_balance (exchange, ts, value, currency, note) "
        "VALUES (?, ?, ?, ?, ?)",
        ("hyperliquid", now - 2 * 3600, 4900.0, "USDC", "ok"),
    )
    s.conn.execute(
        "INSERT INTO account_balance (exchange, ts, value, currency, note) "
        "VALUES (?, ?, ?, ?, ?)",
        ("hyperliquid", now - 30 * 60, 5000.0, "USDC", "ok"),
    )
    s.conn.commit()
    one_hour = s.get_balance_history("hyperliquid", hours=1.0)
    three_hours = s.get_balance_history("hyperliquid", hours=3.0)
    assert len(one_hour) == 1
    assert len(three_hours) == 2
    s.close()


# ──────────────────────────────────────────────
# Engine balance_poll_loop
# ──────────────────────────────────────────────


def _engine(fresh_db: str) -> PaperTradingEngine:
    cfg = AppConfig(
        mode="LIVE",
        products={"wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL",
                                        contract_size=100, futures_fee_per_contract=2.5)},
        kis_symbol_map={"wti": "MCLM26"},
        hyperliquid=HyperliquidConfig(use_testnet=False),
        kiwoom=KiwoomConfig(use_mock=True),
        kis=KISConfig(),
        strategy=StrategyConfig(cme_closed_skip_entry=False),
        risk=RiskConfig(),
    )
    s = Storage(fresh_db); s.connect()
    kw = KiwoomMock(); kw.set_base_price("MCLM26", 80.0)
    return PaperTradingEngine(cfg, s, kw, risk_mgr=RiskManager(cfg.risk))


@pytest.mark.asyncio
async def test_balance_poll_loop_writes_each_adapter(fresh_db):
    e = _engine(fresh_db)
    reg = ExchangeRegistry()
    reg.register(_BalAdapter("hyperliquid", value=5000.0, currency="USDC"))
    reg.register(_BalAdapter("kis", value=12000.0, currency="USD"))
    e.set_exchange_registry(reg)

    stop = asyncio.Event()

    async def stop_after():
        await asyncio.sleep(0.2)
        stop.set()

    await asyncio.gather(
        e.balance_poll_loop(interval_seconds=10, stop_event=stop),
        stop_after(),
    )
    out = e.storage.get_latest_balances()
    by_ex = {b["exchange"]: b for b in out}
    assert by_ex["hyperliquid"]["value"] == 5000.0
    assert by_ex["hyperliquid"]["currency"] == "USDC"
    assert by_ex["kis"]["value"] == 12000.0
    assert all(b["note"] == "ok" for b in out)


@pytest.mark.asyncio
async def test_balance_poll_loop_handles_exception_silently(fresh_db):
    e = _engine(fresh_db)
    reg = ExchangeRegistry()
    reg.register(_BalAdapter("hyperliquid", value=5000.0))
    reg.register(_BalAdapter("kis", raise_exc=RuntimeError("network")))
    e.set_exchange_registry(reg)

    stop = asyncio.Event()

    async def stop_after():
        await asyncio.sleep(0.2)
        stop.set()

    await asyncio.gather(
        e.balance_poll_loop(interval_seconds=10, stop_event=stop),
        stop_after(),
    )
    by_ex = {b["exchange"]: b for b in e.storage.get_latest_balances()}
    assert by_ex["hyperliquid"]["value"] == 5000.0
    assert by_ex["hyperliquid"]["note"] == "ok"
    # kis는 raise → value=0, note에 error
    assert by_ex["kis"]["value"] == 0.0
    assert "network" in by_ex["kis"]["note"]


@pytest.mark.asyncio
async def test_balance_poll_loop_skips_not_implemented_adapters(fresh_db):
    """paper-only scaffolding adapters (lighter/binance/bybit/okx) — NotImplementedError 시 row 안 씀."""
    e = _engine(fresh_db)
    reg = ExchangeRegistry()
    reg.register(_BalAdapter("hyperliquid", value=5000.0))
    reg.register(_BalAdapter("lighter", raise_exc=NotImplementedError("paper only")))
    e.set_exchange_registry(reg)

    stop = asyncio.Event()

    async def stop_after():
        await asyncio.sleep(0.2)
        stop.set()

    await asyncio.gather(
        e.balance_poll_loop(interval_seconds=10, stop_event=stop),
        stop_after(),
    )
    out = e.storage.get_latest_balances()
    names = {b["exchange"] for b in out}
    assert "hyperliquid" in names
    assert "lighter" not in names    # NotImplementedError → skip


@pytest.mark.asyncio
async def test_balance_poll_loop_no_registry_exits_early(fresh_db):
    e = _engine(fresh_db)
    # registry 미설정 → 즉시 self-exit
    await asyncio.wait_for(
        e.balance_poll_loop(interval_seconds=10, stop_event=asyncio.Event()),
        timeout=1.0,
    )
    assert e.storage.get_latest_balances() == []
