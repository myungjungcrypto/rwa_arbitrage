"""Phase G 회귀 — OKXExchange 어댑터.

REST/WS public 인터페이스만 (인증 미사용). place_order는 NotImplementedError.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from src.exchange import base as _base
from src.exchange.okx import OKXExchange


# ──────────────────────────────────────────────
# REST mock helpers
# ──────────────────────────────────────────────


class _FakeResp:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload
    async def json(self): return self._payload
    async def text(self): return json.dumps(self._payload)
    async def __aenter__(self): return self
    async def __aexit__(self, *a): return False


class _FakeSession:
    def __init__(self, route_table: dict[str, object]):
        self._routes = route_table
        self.last_calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict | None = None):
        path = url.split(".com", 1)[-1]
        self.last_calls.append((path, params or {}))
        entry = self._routes.get(path)
        if entry is None:
            return _FakeResp(404, {"err": "not found"})
        if isinstance(entry, tuple):
            return _FakeResp(*entry)
        return _FakeResp(200, entry)

    async def close(self): pass


def _adapter_with(routes: dict) -> OKXExchange:
    factory = lambda: _FakeSession(routes)   # noqa
    return OKXExchange(session_factory=factory)


# ──────────────────────────────────────────────
# connect / disconnect
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_connect_idempotent():
    a = _adapter_with({})
    assert await a.connect() is True
    sess1 = a._session
    assert await a.connect() is True
    assert a._session is sess1
    await a.disconnect()


# ──────────────────────────────────────────────
# discover_symbol — v5 instruments
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_discover_symbol_extracts_filters():
    routes = {
        "/api/v5/public/instruments": {
            "code": "0",
            "data": [{
                "instId": "CL-USDT-SWAP",
                "tickSz": "0.01", "lotSz": "1", "minSz": "1", "ctVal": "1",
            }]
        }
    }
    a = _adapter_with(routes)
    await a.connect()
    found = await a.discover_symbol("CL-USDT-SWAP")
    assert found is True
    meta = a._symbol_meta["CL-USDT-SWAP"]
    assert meta["tick_size"] == 0.01
    assert meta["lot_size"] == 1.0
    assert meta["min_size"] == 1.0
    assert meta["ct_val"] == 1.0


@pytest.mark.asyncio
async def test_discover_symbol_code_nonzero():
    routes = {"/api/v5/public/instruments": {"code": "50001", "msg": "x"}}
    a = _adapter_with(routes)
    await a.connect()
    assert await a.discover_symbol("CL-USDT-SWAP") is False


# ──────────────────────────────────────────────
# get_quote — ticker + mark + funding 결합
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_quote_combines_three_endpoints():
    routes = {
        "/api/v5/market/ticker": {
            "code": "0", "data": [{
                "instId": "CL-USDT-SWAP",
                "bidPx": "80.10", "askPx": "80.20",
                "bidSz": "10", "askSz": "12",
            }]
        },
        "/api/v5/public/mark-price": {
            "code": "0", "data": [{"markPx": "80.15"}]
        },
        "/api/v5/public/funding-rate": {
            "code": "0", "data": [{"fundingRate": "0.0001"}]
        },
    }
    a = _adapter_with(routes)
    await a.connect()
    q = await a.get_quote("CL-USDT-SWAP")
    assert q is not None
    assert q.exchange == "okx"
    assert q.symbol == "CL-USDT-SWAP"
    assert q.bid == 80.10 and q.ask == 80.20
    assert q.bid_qty == 10 and q.ask_qty == 12
    assert q.mid_price == pytest.approx(80.15)
    assert q.funding_rate == 0.0001
    assert q.funding_interval_hours == 4.0


@pytest.mark.asyncio
async def test_get_quote_handles_missing_endpoints():
    routes = {
        "/api/v5/market/ticker": (404, {"err": "x"}),
        "/api/v5/public/mark-price": (404, {"err": "x"}),
        "/api/v5/public/funding-rate": (404, {"err": "x"}),
    }
    a = _adapter_with(routes)
    await a.connect()
    q = await a.get_quote("CL-USDT-SWAP")
    assert q is not None
    assert q.mid_price == 0.0


# ──────────────────────────────────────────────
# WS message handling
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_books5_msg_emits_with_cached_mark_funding():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CL-USDT-SWAP"] = [lambda q: received.append(q)]
    # mark/funding 캐시 미리 채움
    a._on_mark("CL-USDT-SWAP", {"markPx": "80.50"})
    a._on_funding("CL-USDT-SWAP", {"fundingRate": "0.0002"})
    # books5 push (data is list)
    await a._handle_ws_msg("CL-USDT-SWAP", {
        "arg": {"channel": "books5", "instId": "CL-USDT-SWAP"},
        "data": [{
            "asks": [["80.20", "12", "0", "1"]],
            "bids": [["80.10", "10", "0", "1"]],
        }],
    })
    assert len(received) == 1
    q = received[0]
    assert q.bid == 80.10 and q.ask == 80.20
    assert q.bid_qty == 10 and q.ask_qty == 12
    assert q.mid_price == pytest.approx(80.15)
    assert q.funding_rate == 0.0002


@pytest.mark.asyncio
async def test_mark_price_msg_only_caches_no_emit():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CL-USDT-SWAP"] = [lambda q: received.append(q)]
    await a._handle_ws_msg("CL-USDT-SWAP", {
        "arg": {"channel": "mark-price", "instId": "CL-USDT-SWAP"},
        "data": [{"markPx": "80.50"}],
    })
    assert received == []
    assert a._latest_mark["CL-USDT-SWAP"] == 80.50


@pytest.mark.asyncio
async def test_subscribe_event_message_ignored():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CL-USDT-SWAP"] = [lambda q: received.append(q)]
    await a._handle_ws_msg("CL-USDT-SWAP", {"event": "subscribe", "arg": {}})
    assert received == []


# ──────────────────────────────────────────────
# get_funding_info — interval 추정
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_funding_info_estimates_4h_from_history():
    routes = {
        "/api/v5/public/funding-rate": {
            "code": "0",
            "data": [{
                "fundingRate": "0.00012",
                "nextFundingTime": "1704067200000",
            }]
        },
        "/api/v5/public/funding-rate-history": {
            "code": "0",
            "data": [
                {"fundingTime": str(t)} for t in [
                    1704000000000, 1704014400000, 1704028800000,
                    1704043200000, 1704057600000, 1704067200000,
                ]
            ]
        }
    }
    a = _adapter_with(routes)
    await a.connect()
    info = await a.get_funding_info("CL-USDT-SWAP")
    assert info is not None
    assert info.exchange == "okx"
    assert info.current_rate == pytest.approx(0.00012)
    assert info.observed_interval_hours == pytest.approx(4.0, abs=0.01)


@pytest.mark.asyncio
async def test_get_funding_info_default_when_history_empty():
    routes = {
        "/api/v5/public/funding-rate": {
            "code": "0", "data": [{"fundingRate": "0", "nextFundingTime": "0"}]
        },
        "/api/v5/public/funding-rate-history": {"code": "0", "data": []}
    }
    a = _adapter_with(routes)
    await a.connect()
    info = await a.get_funding_info("CL-USDT-SWAP")
    assert info.observed_interval_hours == 4.0


# ──────────────────────────────────────────────
# subscribe_quotes — single task per symbol
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_subscribe_quotes_single_task_per_symbol():
    a = _adapter_with({})
    await a.connect()

    @asynccontextmanager
    async def _fake_ws_ctx(*args, **kwargs):
        await asyncio.sleep(0)
        yield _NoopWs()

    a._ws_connect = _fake_ws_ctx
    cb1, cb2 = MagicMock(), MagicMock()
    await a.subscribe_quotes("CL-USDT-SWAP", cb1)
    await a.subscribe_quotes("CL-USDT-SWAP", cb2)
    assert a._symbol_callbacks["CL-USDT-SWAP"] == [cb1, cb2]
    assert len(a._ws_tasks) == 1
    await a.disconnect()


class _NoopWs:
    async def send(self, *a, **kw): return None
    async def __aiter__(self):
        if False:    # pragma: no cover
            yield ""


# ──────────────────────────────────────────────
# place_order — paper-only NotImplementedError
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_place_order_raises_not_implemented():
    a = _adapter_with({})
    with pytest.raises(NotImplementedError):
        await a.place_order("CL-USDT-SWAP", "buy", 1.0)


@pytest.mark.asyncio
async def test_cancel_order_raises_not_implemented():
    a = _adapter_with({})
    with pytest.raises(NotImplementedError):
        await a.cancel_order("CL-USDT-SWAP", "1")


@pytest.mark.asyncio
async def test_get_account_value_raises_not_implemented_in_paper():
    """paper-only — balance polling이 skip하도록 raise."""
    a = _adapter_with({})
    with pytest.raises(NotImplementedError):
        await a.get_account_value()


@pytest.mark.asyncio
async def test_get_positions_returns_empty_in_paper():
    a = _adapter_with({})
    assert await a.get_positions() == []


@pytest.mark.asyncio
async def test_adapter_implements_exchange_base_protocol():
    a = _adapter_with({})
    assert isinstance(a, _base.ExchangeBase)
    assert a.name == "okx"
    assert a.venue_type == _base.VenueType.PERP.value
    assert a.margin_asset == "USDT"
