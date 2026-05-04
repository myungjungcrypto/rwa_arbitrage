"""Phase 11a 회귀 — KISExchange 실주문/취소/포지션 REST.

aiohttp 응답을 monkey-patch로 mock. 실 endpoint 호출 안 함.
검증 포인트:
  - 주문 body가 KIS 필수 파라미터 형식 따름 (CANO/ACNT_PRDT_CD 분리 등)
  - 매수/매도, 시장가/지정가 매핑
  - tr_id 실전(OTFM3001U) vs 모의(VTFM3001U) 분기
  - rt_cd='0' → success, 그 외 → failure with message
  - get_positions: SLL_BUY_DVSN_CD에 따라 signed quantity
"""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.exchange import base as _base
from src.exchange.kis import KISAuth, KISExchange, KISFuturesClient


# ──────────────────────────────────────────────
# Helpers / Fixtures
# ──────────────────────────────────────────────


def _make_exchange(is_paper: bool = False, account: str = "12345678-08") -> tuple[KISExchange, dict]:
    """KIS auth + client + exchange fixture. captured = {} 에 마지막 요청 기록."""
    auth = KISAuth(
        app_key="ak", app_secret="as",
        base_url="https://openapi.koreainvestment.com:9443",
        is_paper=is_paper,
        account_number=account,
    )
    auth._access_token = "test_token"      # 토큰 발급 단계 우회
    auth._token_expires = 9999999999.0
    client = KISFuturesClient(auth=auth)
    ex = KISExchange(client=client)
    return ex, {}


def _patch_aiohttp(captured: dict, response_json: dict, status: int = 200):
    """aiohttp.ClientSession.post를 monkey-patch.

    호출 시 (url, headers, json) 캡처하고 response_json 반환.
    """
    class FakeResponse:
        def __init__(self, payload, status):
            self._payload = payload
            self.status = status
        async def json(self): return self._payload
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False

    class FakeSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False
        def post(self, url, headers=None, json=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["body"] = json
            return FakeResponse(response_json, status)
        def get(self, url, headers=None, params=None):
            captured["url"] = url
            captured["headers"] = headers
            captured["params"] = params
            return FakeResponse(response_json, status)

    return patch("src.exchange.kis.aiohttp.ClientSession", lambda: FakeSession())


# ──────────────────────────────────────────────
# account_cano_prdt
# ──────────────────────────────────────────────


def test_account_split_with_hyphen():
    auth = KISAuth("k", "s", account_number="12345678-08")
    assert auth.account_cano_prdt == ("12345678", "08")


def test_account_split_without_hyphen():
    auth = KISAuth("k", "s", account_number="1234567808")
    assert auth.account_cano_prdt == ("12345678", "08")


def test_account_split_invalid_raises():
    auth = KISAuth("k", "s", account_number="1234567")
    with pytest.raises(ValueError):
        _ = auth.account_cano_prdt


def test_account_split_empty_raises():
    auth = KISAuth("k", "s", account_number="")
    with pytest.raises(ValueError):
        _ = auth.account_cano_prdt


# ──────────────────────────────────────────────
# place_order — 실전 매수 시장가
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_place_order_market_buy_real():
    ex, captured = _make_exchange(is_paper=False)
    response = {
        "rt_cd": "0", "msg_cd": "OPSP0000",
        "msg1": "정상처리되었습니다",
        "output": {"ODNO": "00360686", "KRX_FWDG_ORD_ORGNO": "01023"},
    }
    with _patch_aiohttp(captured, response):
        r = await ex.place_order("MCLM26", "buy", 1.0, order_type="market")
    assert r.success is True
    assert r.exchange == "kis"
    assert r.symbol == "MCLM26"
    assert r.order_id == "00360686"
    # body 검증
    body = captured["body"]
    assert body["CANO"] == "12345678"
    assert body["ACNT_PRDT_CD"] == "08"
    assert body["OVRS_FUTR_FX_PDNO"] == "MCLM26"
    assert body["SLL_BUY_DVSN_CD"] == "02"   # buy
    assert body["PRIC_DVSN_CD"] == "2"        # market
    assert body["FM_LIMIT_ORD_PRIC"] == "0"
    assert body["FM_ORD_QTY"] == "1"
    # 실전 tr_id
    assert captured["headers"]["tr_id"] == "OTFM3001U"


@pytest.mark.asyncio
async def test_place_order_limit_sell_paper():
    ex, captured = _make_exchange(is_paper=True)
    response = {"rt_cd": "0", "msg1": "OK", "output": {"ODNO": "999"}}
    with _patch_aiohttp(captured, response):
        r = await ex.place_order("MCLM26", "sell", 2.0,
                                  order_type="limit", limit_price=80.05)
    assert r.success is True
    body = captured["body"]
    assert body["SLL_BUY_DVSN_CD"] == "01"   # sell
    assert body["PRIC_DVSN_CD"] == "1"        # limit
    assert body["FM_LIMIT_ORD_PRIC"] == "80.05"
    assert body["FM_ORD_QTY"] == "2"
    # 모의 tr_id
    assert captured["headers"]["tr_id"] == "VTFM3001U"


@pytest.mark.asyncio
async def test_place_order_failure_returns_error():
    ex, captured = _make_exchange()
    response = {"rt_cd": "1", "msg1": "잔고부족", "msg_cd": "EGW00123"}
    with _patch_aiohttp(captured, response):
        r = await ex.place_order("MCLM26", "buy", 1.0)
    assert r.success is False
    assert "잔고부족" in r.error
    assert r.order_id == ""


@pytest.mark.asyncio
async def test_place_order_invalid_account_returns_error():
    ex, captured = _make_exchange(account="invalid")
    r = await ex.place_order("MCLM26", "buy", 1.0)
    assert r.success is False
    assert "account" in r.error.lower()


# ──────────────────────────────────────────────
# cancel_order
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_order_real_success():
    ex, captured = _make_exchange()
    with _patch_aiohttp(captured, {"rt_cd": "0", "msg1": "취소완료"}):
        ok = await ex.cancel_order("MCLM26", "00360686")
    assert ok is True
    body = captured["body"]
    assert body["ORGN_ODNO"] == "00360686"
    assert body["CANO"] == "12345678"
    assert captured["headers"]["tr_id"] == "OTFM3003U"


@pytest.mark.asyncio
async def test_cancel_order_paper_uses_v_trid():
    ex, captured = _make_exchange(is_paper=True)
    with _patch_aiohttp(captured, {"rt_cd": "0"}):
        await ex.cancel_order("MCLM26", "12345678")
    assert captured["headers"]["tr_id"] == "VTFM3003U"


@pytest.mark.asyncio
async def test_cancel_order_empty_id_returns_false():
    ex, captured = _make_exchange()
    ok = await ex.cancel_order("MCLM26", "")
    assert ok is False


@pytest.mark.asyncio
async def test_cancel_order_failure_returns_false():
    ex, captured = _make_exchange()
    with _patch_aiohttp(captured, {"rt_cd": "1", "msg1": "이미 체결됨"}):
        ok = await ex.cancel_order("MCLM26", "abc")
    assert ok is False


# ──────────────────────────────────────────────
# get_positions
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_get_positions_signed_by_sll_buy_dvsn():
    ex, captured = _make_exchange()
    response = {
        "rt_cd": "0",
        "output1": [
            {
                "OVRS_FUTR_FX_PDNO": "MCLM26",
                "CCLD_QTY": 2,
                "SLL_BUY_DVSN_CD": "02",     # buy → +2
                "AVG_BUY_UNPR": "80.10",
                "FM_TOT_EVLU_PFLS_AMT": "150.00",
                "FM_OBJ_AMT": "16000",
            },
            {
                "OVRS_FUTR_FX_PDNO": "BZN26",
                "CCLD_QTY": 1,
                "SLL_BUY_DVSN_CD": "01",     # sell → -1
                "AVG_BUY_UNPR": "85.00",
                "FM_TOT_EVLU_PFLS_AMT": "-30.00",
                "FM_OBJ_AMT": "8500",
            },
            {
                "OVRS_FUTR_FX_PDNO": "FLAT",
                "CCLD_QTY": 0,
                "SLL_BUY_DVSN_CD": "02",
            },
        ],
    }
    with _patch_aiohttp(captured, response):
        positions = await ex.get_positions()
    # qty=0 row는 skip
    assert len(positions) == 2
    p_buy = next(p for p in positions if p.symbol == "MCLM26")
    p_sell = next(p for p in positions if p.symbol == "BZN26")
    assert p_buy.size == 2
    assert p_buy.entry_price == 80.10
    assert p_buy.unrealized_pnl == 150.0
    assert p_sell.size == -1            # sell direction
    assert p_sell.entry_price == 85.0
    # tr_id
    assert captured["headers"]["tr_id"] == "OTFM1412R"


@pytest.mark.asyncio
async def test_get_positions_empty_when_rt_cd_not_zero():
    ex, captured = _make_exchange()
    with _patch_aiohttp(captured, {"rt_cd": "1", "msg1": "조회 실패"}):
        positions = await ex.get_positions()
    assert positions == []


@pytest.mark.asyncio
async def test_get_positions_invalid_account_returns_empty():
    ex, captured = _make_exchange(account="abc")
    positions = await ex.get_positions()
    assert positions == []


# ──────────────────────────────────────────────
# 네트워크 에러 graceful
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_place_order_network_error_returns_failure():
    ex, _ = _make_exchange()

    class BoomSession:
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return False
        def post(self, *args, **kwargs):
            raise RuntimeError("connection reset")

    with patch("src.exchange.kis.aiohttp.ClientSession", lambda: BoomSession()):
        r = await ex.place_order("MCLM26", "buy", 1.0)
    assert r.success is False
    assert "network" in r.error.lower()
