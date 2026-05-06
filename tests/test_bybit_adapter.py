"""Phase F 회귀 — BybitExchange 어댑터.

REST/WS public 인터페이스만 테스트 (인증 미사용). place_order는
NotImplementedError 검증.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import MagicMock

import pytest

from src.exchange import base as _base
from src.exchange.bybit import BybitExchange


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


def _adapter_with(routes: dict) -> BybitExchange:
    factory = lambda: _FakeSession(routes)   # noqa
    return BybitExchange(session_factory=factory)


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
# discover_symbol — v5 instruments-info
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_discover_symbol_extracts_filters():
    routes = {
        "/v5/market/instruments-info": {
            "retCode": 0,
            "result": {
                "list": [{
                    "symbol": "CLUSDT",
                    "priceFilter": {"tickSize": "0.01"},
                    "lotSizeFilter": {"qtyStep": "0.001", "minOrderQty": "0.001"},
                }]
            }
        }
    }
    a = _adapter_with(routes)
    await a.connect()
    found = await a.discover_symbol("CLUSDT")
    assert found is True
    meta = a._symbol_meta["CLUSDT"]
    assert meta["tick_size"] == 0.01
    assert meta["qty_step"] == 0.001
    assert meta["min_qty"] == 0.001


@pytest.mark.asyncio
async def test_discover_symbol_retCode_nonzero():
    routes = {
        "/v5/market/instruments-info": {"retCode": 10001, "retMsg": "x"}
    }
    a = _adapter_with(routes)
    await a.connect()
    assert await a.discover_symbol("CLUSDT") is False


# ──────────────────────────────────────────────
# get_quote — v5 tickers
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_quote_builds_from_tickers_payload():
    routes = {
        "/v5/market/tickers": {
            "retCode": 0,
            "result": {"list": [{
                "symbol": "CLUSDT",
                "bid1Price": "80.10", "ask1Price": "80.20",
                "bid1Size": "10", "ask1Size": "12",
                "markPrice": "80.15", "indexPrice": "80.12",
                "fundingRate": "0.0001",
                "nextFundingTime": "1700000000000",
            }]}
        }
    }
    a = _adapter_with(routes)
    await a.connect()
    q = await a.get_quote("CLUSDT")
    assert q is not None
    assert q.exchange == "bybit"
    assert q.symbol == "CLUSDT"
    assert q.bid == 80.10 and q.ask == 80.20
    assert q.bid_qty == 10 and q.ask_qty == 12
    assert q.mid_price == pytest.approx(80.15)
    assert q.index_price == 80.12
    assert q.funding_rate == 0.0001
    assert q.funding_interval_hours == 4.0


@pytest.mark.asyncio
async def test_get_quote_returns_none_when_retCode_error():
    routes = {"/v5/market/tickers": {"retCode": 10001, "retMsg": "err"}}
    a = _adapter_with(routes)
    await a.connect()
    assert await a.get_quote("CLUSDT") is None


# ──────────────────────────────────────────────
# WS message handling
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_orderbook_msg_emits_with_cached_ticker():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CLUSDT"] = [lambda q: received.append(q)]
    # ticker 캐시 미리 채움
    a._on_tickers("CLUSDT", {
        "markPrice": "80.50", "indexPrice": "80.45",
        "fundingRate": "0.0002", "nextFundingTime": "1700000000000",
    })
    # orderbook delta
    await a._handle_ws_msg("CLUSDT", {
        "topic": "orderbook.1.CLUSDT", "type": "snapshot",
        "data": {"s": "CLUSDT", "b": [["80.10", "5"]], "a": [["80.20", "7"]]},
    })
    assert len(received) == 1
    q = received[0]
    assert q.bid == 80.10 and q.ask == 80.20
    assert q.bid_qty == 5 and q.ask_qty == 7
    assert q.mid_price == pytest.approx(80.15)
    # ticker 캐시에서 합류
    assert q.index_price == 80.45
    assert q.funding_rate == 0.0002


@pytest.mark.asyncio
async def test_tickers_msg_only_updates_cache_no_emit():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CLUSDT"] = [lambda q: received.append(q)]
    await a._handle_ws_msg("CLUSDT", {
        "topic": "tickers.CLUSDT", "type": "snapshot",
        "data": {"markPrice": "80.5", "fundingRate": "0.0003"},
    })
    # tickers 단독은 emit 안 함
    assert received == []
    assert a._latest_ticker["CLUSDT"]["mark_price"] == 80.5
    assert a._latest_ticker["CLUSDT"]["funding_rate"] == 0.0003


@pytest.mark.asyncio
async def test_subscribe_ack_message_ignored():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CLUSDT"] = [lambda q: received.append(q)]
    # bybit subscribe ack
    await a._handle_ws_msg("CLUSDT", {"op": "subscribe", "success": True})
    assert received == []


# ──────────────────────────────────────────────
# get_funding_info — interval 추정
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_funding_info_estimates_4h_from_history():
    routes = {
        "/v5/market/tickers": {
            "retCode": 0,
            "result": {"list": [{
                "symbol": "CLUSDT", "fundingRate": "0.00012",
                "nextFundingTime": "1704067200000",
            }]}
        },
        "/v5/market/funding/history": {
            "retCode": 0,
            "result": {"list": [
                {"fundingRateTimestamp": str(t)} for t in [
                    1704000000000, 1704014400000, 1704028800000,
                    1704043200000, 1704057600000, 1704067200000,
                ]
            ]}
        }
    }
    a = _adapter_with(routes)
    await a.connect()
    info = await a.get_funding_info("CLUSDT")
    assert info is not None
    assert info.exchange == "bybit"
    assert info.current_rate == pytest.approx(0.00012)
    assert info.observed_interval_hours == pytest.approx(4.0, abs=0.01)


@pytest.mark.asyncio
async def test_get_funding_info_default_when_history_empty():
    routes = {
        "/v5/market/tickers": {
            "retCode": 0, "result": {"list": [{"fundingRate": "0", "nextFundingTime": "0"}]}
        },
        "/v5/market/funding/history": {"retCode": 0, "result": {"list": []}}
    }
    a = _adapter_with(routes)
    await a.connect()
    info = await a.get_funding_info("CLUSDT")
    assert info.observed_interval_hours == 4.0


# ──────────────────────────────────────────────
# subscribe_quotes — task 1개만, 콜백 다중
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
    await a.subscribe_quotes("CLUSDT", cb1)
    await a.subscribe_quotes("CLUSDT", cb2)
    assert a._symbol_callbacks["CLUSDT"] == [cb1, cb2]
    assert len(a._ws_tasks) == 1
    await a.disconnect()


class _NoopWs:
    async def send(self, *a, **kw): return None
    async def __aiter__(self):
        if False:    # pragma: no cover
            yield ""


# ──────────────────────────────────────────────
# place_order — NotImplementedError (paper-only)
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_place_order_raises_not_implemented():
    a = _adapter_with({})
    with pytest.raises(NotImplementedError):
        await a.place_order("CLUSDT", "buy", 1.0)


@pytest.mark.asyncio
async def test_cancel_order_raises_not_implemented():
    a = _adapter_with({})
    with pytest.raises(NotImplementedError):
        await a.cancel_order("CLUSDT", "1")


@pytest.mark.asyncio
async def test_get_positions_returns_empty_in_paper():
    a = _adapter_with({})
    assert await a.get_positions() == []


@pytest.mark.asyncio
async def test_adapter_implements_exchange_base_protocol():
    a = _adapter_with({})
    assert isinstance(a, _base.ExchangeBase)
    assert a.name == "bybit"
    assert a.venue_type == _base.VenueType.PERP.value
    assert a.margin_asset == "USDT"
