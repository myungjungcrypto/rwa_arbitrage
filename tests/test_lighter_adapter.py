"""Phase D 회귀 — LighterExchange 어댑터.

lighter-sdk가 환경에 없어도 통과하도록 mock 주입 패턴 사용.
실 SDK 통합 검증은 Phase D 후속 commit에서 EC2 shadow 운영으로.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from typing import Any

import pytest

from src.exchange import base as _base
from src.exchange.lighter import LighterExchange


# ──────────────────────────────────────────────
# Protocol conformance
# ──────────────────────────────────────────────


def test_lighter_adapter_satisfies_protocol():
    ex = LighterExchange()
    assert ex.name == "lighter"
    assert ex.venue_type == _base.VenueType.PERP.value
    assert ex.margin_asset == "USDC"
    assert isinstance(ex, _base.ExchangeBase)


def test_default_urls_set():
    ex = LighterExchange()
    assert ex.base_url.startswith("https://")
    assert ex.ws_url.startswith("wss://")


# ──────────────────────────────────────────────
# Market id mapping
# ──────────────────────────────────────────────


def test_set_market_id_caches_metadata():
    ex = LighterExchange()
    ex.set_market_id("WTI", 42, price_decimal=3, size_decimal=2)
    assert ex.get_market_id("WTI") == 42
    assert ex._symbol_meta["WTI"] == {"price_decimal": 3, "size_decimal": 2}


def test_get_market_id_unknown_returns_none():
    ex = LighterExchange()
    assert ex.get_market_id("NONEXISTENT") is None


# ──────────────────────────────────────────────
# discover_markets — mock SDK
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_discover_markets_no_client_raises():
    ex = LighterExchange()
    with pytest.raises(RuntimeError, match="api_client"):
        await ex.discover_markets()


# ──────────────────────────────────────────────
# get_quote — mock SDK
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_quote_unknown_market_returns_none():
    ex = LighterExchange()
    ex._api_client = object()    # mock 존재만
    q = await ex.get_quote("WTI")
    assert q is None


@pytest.mark.asyncio
async def test_get_quote_no_api_client_returns_none():
    ex = LighterExchange()
    ex.set_market_id("WTI", 0)
    q = await ex.get_quote("WTI")
    assert q is None


# ──────────────────────────────────────────────
# Quote 빌더 — orderbook 형식 변환
# ──────────────────────────────────────────────


def test_build_quote_from_dict_orderbook():
    ex = LighterExchange()
    ex.set_market_id("WTI", 0)
    ob = {
        "bids": [{"price": "80.10", "size": "100"}],
        "asks": [{"price": "80.12", "size": "50"}],
    }
    q = ex._build_quote_from_orderbook("WTI", ob)
    assert q.exchange == "lighter"
    assert q.symbol == "WTI"
    assert q.bid == 80.10
    assert q.ask == 80.12
    assert q.bid_qty == 100
    assert q.ask_qty == 50
    assert q.mid_price == pytest.approx(80.11)


def test_build_quote_from_object_orderbook():
    ex = LighterExchange()
    ex.set_market_id("WTI", 0)
    ob = SimpleNamespace(
        bids=[SimpleNamespace(price=80.20, size=10)],
        asks=[SimpleNamespace(price=80.22, size=5)],
    )
    q = ex._build_quote_from_orderbook("WTI", ob)
    assert q.bid == 80.20
    assert q.ask == 80.22
    assert q.mid_price == pytest.approx(80.21)


def test_build_quote_handles_empty_orderbook():
    ex = LighterExchange()
    ob = {"bids": [], "asks": []}
    q = ex._build_quote_from_orderbook("WTI", ob)
    assert q.bid == 0.0
    assert q.ask == 0.0
    assert q.mid_price == 0.0


def test_build_quote_alternate_keys():
    """SDK 버전에 따라 buy_orders/sell_orders 변형 대응."""
    ex = LighterExchange()
    ex.set_market_id("WTI", 0)
    ob = {
        "buy_orders": [{"price": "80", "size": "5"}],
        "sell_orders": [{"price": "80.05", "size": "3"}],
    }
    q = ex._build_quote_from_orderbook("WTI", ob)
    assert q.bid == 80.0
    assert q.ask == 80.05


def test_build_quote_handles_tuple_levels():
    ex = LighterExchange()
    ob = {"bids": [(80.0, 10)], "asks": [(80.05, 5)]}
    q = ex._build_quote_from_orderbook("WTI", ob)
    assert q.bid == 80.0 and q.bid_qty == 10
    assert q.ask == 80.05 and q.ask_qty == 5


# ──────────────────────────────────────────────
# place_order / cancel — paper 단계는 NotImplementedError
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_place_order_raises_in_paper_stage():
    ex = LighterExchange()
    with pytest.raises(NotImplementedError):
        await ex.place_order("WTI", "buy", 1.0)


@pytest.mark.asyncio
async def test_cancel_order_raises_in_paper_stage():
    ex = LighterExchange()
    with pytest.raises(NotImplementedError):
        await ex.cancel_order("WTI", "id-123")


# ──────────────────────────────────────────────
# subscribe_quotes — WsClient factory 주입으로 검증
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_subscribe_quotes_unknown_symbol_raises():
    ex = LighterExchange()
    async def cb(q): pass
    with pytest.raises(KeyError, match="market_id"):
        await ex.subscribe_quotes("WTI", cb)


@pytest.mark.asyncio
async def test_subscribe_quotes_uses_factory_and_invokes_callback_on_update():
    """factory가 만든 WsClient가 on_order_book_update를 호출하면 사용자 콜백까지 전달."""
    received: list[_base.Quote] = []

    captured: dict[str, Any] = {}

    class FakeWsClient:
        def __init__(self, host, order_book_ids, on_order_book_update):
            captured["host"] = host
            captured["order_book_ids"] = order_book_ids
            captured["on_update"] = on_order_book_update
        # run() 없음 → adapter는 그냥 skip

    ex = LighterExchange(ws_client_factory=FakeWsClient)
    ex.set_market_id("WTI", 7)

    def cb(q):
        received.append(q)

    await ex.subscribe_quotes("WTI", cb)
    assert captured["host"].startswith("wss://")
    assert captured["order_book_ids"] == [7]

    # WsClient가 보낸 update 시뮬
    captured["on_update"](7, {
        "bids": [{"price": "80.10", "size": "100"}],
        "asks": [{"price": "80.11", "size": "50"}],
    })
    assert len(received) == 1
    assert received[0].bid == 80.10
    assert received[0].ask == 80.11


@pytest.mark.asyncio
async def test_subscribe_quotes_idempotent_on_second_symbol():
    """동일 ws_client는 한 번만 생성. 2번째 심볼은 콜백만 등록."""
    create_count = 0

    class FakeWsClient:
        def __init__(self, **kw):
            nonlocal create_count
            create_count += 1

    ex = LighterExchange(ws_client_factory=FakeWsClient)
    ex.set_market_id("WTI", 7)
    ex.set_market_id("BTC-PERP", 1)
    async def cb(q): pass
    await ex.subscribe_quotes("WTI", cb)
    await ex.subscribe_quotes("BTC-PERP", cb)
    assert create_count == 1


# ──────────────────────────────────────────────
# get_funding_info — Lighter는 1h funding 가정
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_funding_info_no_client_returns_none():
    ex = LighterExchange()
    ex.set_market_id("WTI", 0)
    info = await ex.get_funding_info("WTI")
    assert info is None
