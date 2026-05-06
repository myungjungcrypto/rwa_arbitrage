"""Phase E 회귀 — BinanceExchange 어댑터.

REST/WS public 인터페이스만 테스트 (인증 미사용). place_order는
NotImplementedError 검증.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.exchange import base as _base
from src.exchange.binance import BinanceExchange


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
        # route_table[path] -> payload OR (status, payload)
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


def _adapter_with(routes: dict) -> BinanceExchange:
    factory = lambda: _FakeSession(routes)   # noqa
    return BinanceExchange(session_factory=factory)


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
# discover_symbol
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_discover_symbol_extracts_filters():
    routes = {
        "/fapi/v1/exchangeInfo": {
            "symbols": [
                {
                    "symbol": "CLUSDT",
                    "filters": [
                        {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                        {"filterType": "LOT_SIZE", "stepSize": "0.001",
                         "minQty": "0.001"},
                    ],
                }
            ],
        }
    }
    a = _adapter_with(routes)
    await a.connect()
    found = await a.discover_symbol("CLUSDT")
    assert found is True
    meta = a._symbol_meta["CLUSDT"]
    assert meta["tick_size"] == 0.01
    assert meta["step_size"] == 0.001
    assert meta["min_qty"] == 0.001


@pytest.mark.asyncio
async def test_discover_symbol_missing_returns_false():
    routes = {"/fapi/v1/exchangeInfo": {"symbols": []}}
    a = _adapter_with(routes)
    await a.connect()
    assert await a.discover_symbol("ZZZUSDT") is False


# ──────────────────────────────────────────────
# get_quote — bookTicker + premiumIndex 결합
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_quote_combines_bookTicker_and_premiumIndex():
    routes = {
        "/fapi/v1/ticker/bookTicker": {
            "symbol": "CLUSDT", "bidPrice": "80.10", "askPrice": "80.20",
            "bidQty": "10", "askQty": "12",
        },
        "/fapi/v1/premiumIndex": {
            "symbol": "CLUSDT", "markPrice": "80.15", "indexPrice": "80.12",
            "lastFundingRate": "0.0001",
        },
    }
    a = _adapter_with(routes)
    await a.connect()
    q = await a.get_quote("CLUSDT")
    assert q is not None
    assert q.exchange == "binance"
    assert q.symbol == "CLUSDT"
    assert q.bid == 80.10
    assert q.ask == 80.20
    assert q.bid_qty == 10
    assert q.ask_qty == 12
    assert q.mid_price == pytest.approx(80.15)
    assert q.index_price == 80.12
    assert q.funding_rate == 0.0001
    assert q.funding_interval_hours == 4.0


@pytest.mark.asyncio
async def test_get_quote_handles_missing_data():
    routes = {
        "/fapi/v1/ticker/bookTicker": (404, {"err": "x"}),
        "/fapi/v1/premiumIndex": (404, {"err": "x"}),
    }
    a = _adapter_with(routes)
    await a.connect()
    q = await a.get_quote("CLUSDT")
    # 둘 다 404여도 빈 dict로 quote 만듦 (mid=0)
    assert q is not None
    assert q.mid_price == 0.0
    assert q.bid == 0.0


# ──────────────────────────────────────────────
# WS message handling
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_book_ticker_msg_fires_callback_with_cached_mark():
    a = _adapter_with({})
    await a.connect()
    received: list[_base.Quote] = []
    a._symbol_callbacks["CLUSDT"] = [lambda q: received.append(q)]
    # mark cache 미리 채움 (markPrice 스트림 도착 시 동작 시뮬)
    a._on_mark_price("CLUSDT", {
        "p": "80.50", "i": "80.45", "r": "0.0002", "T": "1700000000000",
    })
    # bookTicker push
    await a._handle_ws_msg("CLUSDT", {
        "u": 1, "b": "80.10", "a": "80.20", "B": "5", "A": "7",
    })
    assert len(received) == 1
    q = received[0]
    assert q.bid == 80.10 and q.ask == 80.20
    assert q.mid_price == pytest.approx(80.15)
    # mark/funding은 캐시에서 합류
    assert q.index_price == 80.45
    assert q.funding_rate == 0.0002


@pytest.mark.asyncio
async def test_combined_stream_wrapper_unwraps_data_field():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CLUSDT"] = [lambda q: received.append(q)]
    # combined stream 형식: {"stream": "...", "data": {bookTicker payload}}
    await a._handle_ws_msg("CLUSDT", {
        "stream": "clusdt@bookTicker",
        "data": {"u": 1, "b": "80.0", "a": "80.1", "B": "1", "A": "1"},
    })
    assert len(received) == 1


@pytest.mark.asyncio
async def test_mark_price_msg_does_not_fire_callback_only_caches():
    a = _adapter_with({})
    await a.connect()
    received = []
    a._symbol_callbacks["CLUSDT"] = [lambda q: received.append(q)]
    await a._handle_ws_msg("CLUSDT", {
        "e": "markPriceUpdate", "p": "80.50", "i": "80.45",
        "r": "0.0001", "T": "1700000000000",
    })
    # markPrice 이벤트는 캐시 갱신만, 콜백 X
    assert received == []
    assert a._latest_mark["CLUSDT"]["mark_price"] == 80.50
    assert a._latest_mark["CLUSDT"]["funding_rate"] == 0.0001


# ──────────────────────────────────────────────
# get_funding_info — interval 추정
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_funding_info_estimates_interval_from_history():
    """4h 간격 6개 history → observed=4.0"""
    routes = {
        "/fapi/v1/premiumIndex": {
            "lastFundingRate": "0.00012",
            "nextFundingTime": "1704067200000",   # 2024-01-01 00:00 UTC
        },
        "/fapi/v1/fundingRate": [
            {"fundingTime": str(t)} for t in [
                1704000000000, 1704014400000, 1704028800000,
                1704043200000, 1704057600000, 1704067200000,
            ]   # 4h gaps
        ],
    }
    a = _adapter_with(routes)
    await a.connect()
    info = await a.get_funding_info("CLUSDT")
    assert info is not None
    assert info.exchange == "binance"
    assert info.current_rate == pytest.approx(0.00012)
    assert info.observed_interval_hours == pytest.approx(4.0, abs=0.01)


@pytest.mark.asyncio
async def test_get_funding_info_falls_back_to_default_when_no_history():
    routes = {
        "/fapi/v1/premiumIndex": {
            "lastFundingRate": "0", "nextFundingTime": "0",
        },
        "/fapi/v1/fundingRate": [],
    }
    a = _adapter_with(routes)
    await a.connect()
    info = await a.get_funding_info("CLUSDT")
    assert info is not None
    assert info.observed_interval_hours == 4.0


# ──────────────────────────────────────────────
# subscribe_quotes — 다중 콜백, idempotency
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_subscribe_quotes_registers_multiple_callbacks_single_ws_task():
    a = _adapter_with({})
    await a.connect()
    cb1 = MagicMock()
    cb2 = MagicMock()

    # WS task 자체는 실 connect 시도 없이 mock으로 차단
    @asynccontextmanager
    async def _fake_ws_ctx(*args, **kwargs):
        # 즉시 양보 → 실제 메시지 처리는 없음
        await asyncio.sleep(0)
        yield _NoopWs()

    a._ws_connect = _fake_ws_ctx
    await a.subscribe_quotes("CLUSDT", cb1)
    await a.subscribe_quotes("CLUSDT", cb2)
    assert a._symbol_callbacks["CLUSDT"] == [cb1, cb2]
    # 같은 심볼은 task 1개만
    assert len(a._ws_tasks) == 1
    await a.disconnect()


class _NoopWs:
    async def __aiter__(self):
        if False:    # pragma: no cover — 빈 iterator
            yield ""


# ──────────────────────────────────────────────
# place_order — 페이퍼 단계 NotImplementedError
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
    # runtime_checkable Protocol — duck-typed 인터페이스
    assert isinstance(a, _base.ExchangeBase)
    assert a.name == "binance"
    assert a.venue_type == _base.VenueType.PERP.value
    assert a.margin_asset == "USDT"
