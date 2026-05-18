"""Phase 11b 회귀 — HL eth_account 서명자 빌드 + place_order 흐름.

실 SDK 호출은 mock. private_key 누락/형식 변형 안전성 검증.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.exchange.hyperliquid import (
    HyperliquidClient, MAINNET_API_URL, OrderSide,
)


def _make_client(pk: str = "", addr: str = "") -> HyperliquidClient:
    return HyperliquidClient(
        use_testnet=False, wallet_address=addr, private_key=pk,
    )


# ──────────────────────────────────────────────
# _build_signer
# ──────────────────────────────────────────────


def test_build_signer_returns_none_when_no_private_key():
    c = _make_client(pk="")
    assert c._build_signer() is None


def test_build_signer_accepts_hex_with_or_without_0x_prefix():
    pk_hex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    c1 = _make_client(pk=pk_hex)
    c2 = _make_client(pk="0x" + pk_hex)
    s1 = c1._build_signer()
    s2 = c2._build_signer()
    assert s1 is not None and s2 is not None
    # 동일 키이므로 동일 주소
    assert s1.address == s2.address


def test_build_signer_invalid_key_returns_none():
    c = _make_client(pk="not-a-hex-key")
    assert c._build_signer() is None


# ──────────────────────────────────────────────
# place_order — pre-conditions + SDK 호출 mock
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_place_order_returns_failure_when_no_private_key():
    c = _make_client(pk="")
    r = await c.place_order(ticker="xyz:CL", side=OrderSide.BUY, size=1.0)
    assert r.success is False
    assert "private_key" in r.error.lower()


@pytest.mark.asyncio
async def test_place_order_filled_response():
    c = _make_client(
        pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
    )
    fake_exchange = MagicMock()
    fake_exchange.order.return_value = {
        "status": "ok",
        "response": {
            "data": {
                "statuses": [
                    {"filled": {"oid": 12345, "totalSz": "1.0", "avgPx": "80.50"}}
                ]
            }
        },
    }
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange):
        r = await c.place_order(
            ticker="xyz:CL", side=OrderSide.BUY, size=1.0, price=80.5,
        )
    assert r.success is True
    assert r.order_id == "12345"
    assert r.filled_size == 1.0
    assert r.filled_price == 80.5
    # SDK 호출 인자 검증
    args, kwargs = fake_exchange.order.call_args
    assert kwargs["name"] == "xyz:CL"
    assert kwargs["is_buy"] is True
    assert kwargs["sz"] == 1.0
    # limit price 지정한 경우 GTC
    assert kwargs["order_type"] == {"limit": {"tif": "Gtc"}}


@pytest.mark.asyncio
async def test_place_order_market_uses_ioc_with_slippage_buffer():
    """시장가 주문은 mark price 조회 후 ±slip% 버퍼 limit_px로 IOC 발주."""
    from src.exchange.hyperliquid import MarketData
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.order.return_value = {
        "status": "ok",
        "response": {"data": {"statuses": [{"filled": {"oid": 1, "totalSz": "1", "avgPx": "80"}}]}},
    }
    # mark_price=100, sell이면 limit_px = 100 * 0.95 = 95
    md = MarketData(ticker="xyz:CL", mark_price=100.0, index_price=100.0,
                    funding_rate=0, predicted_funding_rate=0,
                    open_interest=0, volume_24h=0)
    async def _mock_md(t): return md
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange), \
         patch.object(HyperliquidClient, "get_market_data", side_effect=_mock_md):
        r = await c.place_order(ticker="xyz:CL", side=OrderSide.SELL, size=1.0, price=None)
    assert r.success is True
    args, kwargs = fake_exchange.order.call_args
    assert kwargs["order_type"] == {"limit": {"tif": "Ioc"}}
    assert kwargs["is_buy"] is False
    # sell → mark * (1 - 0.05) = 95
    assert kwargs["limit_px"] == pytest.approx(95.0)


@pytest.mark.asyncio
async def test_place_order_market_buy_uses_upward_slippage():
    from src.exchange.hyperliquid import MarketData
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.order.return_value = {
        "status": "ok",
        "response": {"data": {"statuses": [{"filled": {"oid": 1, "totalSz": "1", "avgPx": "100"}}]}},
    }
    md = MarketData(ticker="xyz:CL", mark_price=100.0, index_price=100.0,
                    funding_rate=0, predicted_funding_rate=0,
                    open_interest=0, volume_24h=0)
    async def _mock_md(t): return md
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange), \
         patch.object(HyperliquidClient, "get_market_data", side_effect=_mock_md):
        r = await c.place_order(ticker="xyz:CL", side=OrderSide.BUY, size=1.0, price=None)
    assert r.success is True
    args, kwargs = fake_exchange.order.call_args
    # buy → mark * (1 + 0.05) = 105
    assert kwargs["limit_px"] == pytest.approx(105.0)


@pytest.mark.asyncio
async def test_place_order_market_fails_when_no_mark_price():
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    async def _no_md(t): return None
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange), \
         patch.object(HyperliquidClient, "get_market_data", side_effect=_no_md):
        r = await c.place_order(ticker="xyz:CL", side=OrderSide.BUY, size=1.0, price=None)
    assert r.success is False
    assert "mark price" in r.error.lower()


@pytest.mark.asyncio
async def test_place_order_resting_returns_success_with_oid():
    """체결 안 된 채 호가창 대기 시 'resting' 응답 — 접수 OK + filled=0."""
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.order.return_value = {
        "status": "ok",
        "response": {"data": {"statuses": [{"resting": {"oid": 9999}}]}},
    }
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange):
        r = await c.place_order(ticker="xyz:CL", side=OrderSide.BUY, size=1.0, price=80.0)
    assert r.success is True
    assert r.order_id == "9999"
    assert r.filled_size == 0.0


@pytest.mark.asyncio
async def test_place_order_status_error_returns_failure():
    from src.exchange.hyperliquid import MarketData
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.order.return_value = {
        "status": "ok",
        "response": {"data": {"statuses": [{"error": "insufficient margin"}]}},
    }
    md = MarketData(ticker="xyz:CL", mark_price=80.0, index_price=80.0,
                    funding_rate=0, predicted_funding_rate=0,
                    open_interest=0, volume_24h=0)
    async def _mock_md(t): return md
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange), \
         patch.object(HyperliquidClient, "get_market_data", side_effect=_mock_md):
        r = await c.place_order(ticker="xyz:CL", side=OrderSide.BUY, size=1.0)
    assert r.success is False
    assert "insufficient margin" in r.error


@pytest.mark.asyncio
async def test_place_order_exchange_exception_returns_failure():
    from src.exchange.hyperliquid import MarketData
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.order.side_effect = RuntimeError("network down")
    md = MarketData(ticker="xyz:CL", mark_price=80.0, index_price=80.0,
                    funding_rate=0, predicted_funding_rate=0,
                    open_interest=0, volume_24h=0)
    async def _mock_md(t): return md
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange), \
         patch.object(HyperliquidClient, "get_market_data", side_effect=_mock_md):
        r = await c.place_order(ticker="xyz:CL", side=OrderSide.BUY, size=1.0)
    assert r.success is False
    assert "network down" in r.error


# ──────────────────────────────────────────────
# cancel_order
# ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_order_no_private_key_returns_false():
    c = _make_client(pk="")
    assert await c.cancel_order("xyz:CL", 1) is False


@pytest.mark.asyncio
async def test_cancel_order_success_path():
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.cancel.return_value = {"status": "ok"}
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange):
        ok = await c.cancel_order("xyz:CL", 12345)
    assert ok is True
    args, kwargs = fake_exchange.cancel.call_args
    assert kwargs["name"] == "xyz:CL"
    assert kwargs["oid"] == 12345


@pytest.mark.asyncio
async def test_cancel_order_failure_path():
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.cancel.return_value = {"status": "err", "msg": "not found"}
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange):
        ok = await c.cancel_order("xyz:CL", 12345)
    assert ok is False


@pytest.mark.asyncio
async def test_cancel_order_exception_returns_false():
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.cancel.side_effect = RuntimeError("boom")
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange):
        ok = await c.cancel_order("xyz:CL", 12345)
    assert ok is False


# ──────────────────────────────────────────────
# HL 5-sig-figs price rounding (avoid 'invalid price')
# ──────────────────────────────────────────────


def test_hl_round_price_5_sig_figs():
    from src.exchange.hyperliquid import _hl_round_price_sig_figs as r
    # 6 sig figs → 5 sig figs
    assert r(104.034) == 104.03
    assert r(99.0825) == 99.082 or r(99.0825) == 99.083   # banker's round OK
    # 이미 5 sig 이내 → 그대로
    assert r(99.080) == 99.08
    # 작은 가격
    assert r(0.0001234567) == pytest.approx(0.00012346, abs=1e-10)
    # 0 또는 음수 → 그대로
    assert r(0) == 0
    assert r(-1) == -1


@pytest.mark.asyncio
async def test_place_order_market_rounds_to_5_sig_figs():
    """ref * 1.05가 6 sig figs 나와도 limit_px는 5 sig로 round되어 SDK 전달."""
    from src.exchange.hyperliquid import MarketData
    c = _make_client(pk="ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")
    fake_exchange = MagicMock()
    fake_exchange.order.return_value = {
        "status": "ok",
        "response": {"data": {"statuses": [{"filled": {"oid": 1, "totalSz": "1", "avgPx": "104"}}]}},
    }
    # mark=99.08, buy → 99.08 * 1.05 = 104.034 (6 sig)
    md = MarketData(ticker="xyz:CL", mark_price=99.08, index_price=99.08,
                    funding_rate=0, predicted_funding_rate=0,
                    open_interest=0, volume_24h=0)
    async def _mock_md(t): return md
    with patch.object(HyperliquidClient, "_build_exchange", return_value=fake_exchange), \
         patch.object(HyperliquidClient, "get_market_data", side_effect=_mock_md):
        r = await c.place_order(ticker="xyz:CL", side=OrderSide.BUY, size=1.0, price=None)
    assert r.success is True
    args, kwargs = fake_exchange.order.call_args
    # 6 sig (104.034) → 5 sig (104.03)
    assert kwargs["limit_px"] == 104.03
