"""Phase C2 회귀 — DataCollector pair-keyed API.

NEW 메서드 (`register_pair`, `update_leg_quote`, `on_pair_basis`)가 정상 동작
하면서 레거시 API(`on_basis_update`, `_latest_perp/futures` 등)는 무수정으로
계속 작동하는지 검증.
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.data.collector import DataCollector
from src.data.storage import Storage
from src.exchange.base import Quote
from src.strategy.pair import (
    ArbitragePair, ExchangeLeg, LegRole, PairGate, PairStrategyParams,
)
from src.utils.config import (
    AppConfig, HyperliquidConfig, KISConfig, KiwoomConfig,
    ProductConfig, RiskConfig, StrategyConfig,
)


@pytest.fixture
def cfg() -> AppConfig:
    return AppConfig(
        products={
            "wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL",
                                  contract_size=100, futures_fee_per_contract=2.5),
        },
        kis_symbol_map={"wti": "MCLM26"},
        hyperliquid=HyperliquidConfig(use_testnet=False),
        kiwoom=KiwoomConfig(use_mock=True),
        kis=KISConfig(),
        strategy=StrategyConfig(),
        risk=RiskConfig(),
    )


@pytest.fixture
def storage(tmp_path: Path) -> Storage:
    s = Storage(str(tmp_path / "test.db"))
    s.connect()
    return s


@pytest.fixture
def collector(cfg, storage) -> DataCollector:
    return DataCollector(cfg, storage)


@pytest.fixture
def wti_pair() -> ArbitragePair:
    return ArbitragePair(
        id="wti_cme_hl", enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                          taker_fee_bps=0.9, funding_interval_hours=1.0,
                          margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange="kis", symbol="MCLM26", role=LegRole.DATED_FUTURES,
                          contract_size=100, fee_per_contract_usd=2.50),
        params=PairStrategyParams(),
    )


def _q(exchange: str, symbol: str, mid: float, bid: float = 0.0, ask: float = 0.0) -> Quote:
    return Quote(exchange=exchange, symbol=symbol, mid_price=mid, bid=bid, ask=ask)


# ──────────────────────────────────────────────
# register_pair
# ──────────────────────────────────────────────


def test_register_pair_stores_in_registry(collector, wti_pair):
    collector.register_pair(wti_pair)
    assert collector.get_pair("wti_cme_hl") is wti_pair
    assert "wti_cme_hl" in collector.registered_pairs


def test_register_pair_overwrites_same_id(collector, wti_pair):
    collector.register_pair(wti_pair)
    other = ArbitragePair(
        id="wti_cme_hl", leg_a=wti_pair.leg_a, leg_b=wti_pair.leg_b,
        gate=PairGate.ALWAYS,
    )
    collector.register_pair(other)
    assert collector.get_pair("wti_cme_hl").gate == PairGate.ALWAYS


def test_get_pair_unknown_returns_none(collector):
    assert collector.get_pair("nonexistent") is None


# ──────────────────────────────────────────────
# update_leg_quote — basic flow
# ──────────────────────────────────────────────


def test_update_leg_quote_invalid_leg_raises(collector, wti_pair):
    collector.register_pair(wti_pair)
    with pytest.raises(ValueError):
        collector.update_leg_quote("wti_cme_hl", "c", _q("hl", "xyz:CL", 80))


def test_update_leg_quote_unregistered_pair_ignored(collector):
    # 등록 안 된 pair_id는 silently ignore (warning만)
    collector.update_leg_quote("nonexistent", "a", _q("hl", "xyz:CL", 80))
    assert ("nonexistent", "a") not in collector._latest_quote


def test_update_leg_quote_caches_quote(collector, wti_pair):
    collector.register_pair(wti_pair)
    q = _q("hyperliquid", "xyz:CL", 80.0, bid=79.95, ask=80.05)
    collector.update_leg_quote("wti_cme_hl", "a", q)
    cached = collector.latest_pair_quote("wti_cme_hl", "a")
    assert cached is q
    assert collector.has_both_legs("wti_cme_hl") is False


def test_update_leg_quote_persists_to_leg_prices(collector, wti_pair, storage):
    collector.register_pair(wti_pair)
    q = Quote(exchange="hyperliquid", symbol="xyz:CL",
              mid_price=80.0, bid=79.95, ask=80.05,
              bid_qty=10, ask_qty=12,
              funding_rate=0.0001, funding_interval_hours=1.0)
    collector.update_leg_quote("wti_cme_hl", "a", q)
    rows = storage.conn.execute(
        "SELECT * FROM leg_prices WHERE pair_id='wti_cme_hl' AND leg='a'"
    ).fetchall()
    assert len(rows) == 1
    r = rows[0]
    assert r["exchange"] == "hyperliquid"
    assert r["symbol"] == "xyz:CL"
    assert r["mid_price"] == 80.0
    assert r["bid"] == 79.95
    assert r["funding_interval_hours"] == 1.0


# ──────────────────────────────────────────────
# Both legs → callback fires
# ──────────────────────────────────────────────


def test_callback_fires_only_when_both_legs_present(collector, wti_pair):
    collector.register_pair(wti_pair)
    received = []

    def cb(pair_id, basis_bps, a, b):
        received.append((pair_id, basis_bps, a, b))

    collector.on_pair_basis(cb)

    # leg_a only — no callback
    collector.update_leg_quote(
        "wti_cme_hl", "a",
        Quote(exchange="hl", symbol="xyz:CL", mid_price=80.05,
              bid=80.04, ask=80.06),
    )
    assert received == []

    # leg_b arrives — callback fires
    collector.update_leg_quote(
        "wti_cme_hl", "b",
        Quote(exchange="kis", symbol="MCLM26", mid_price=80.00,
              bid=79.99, ask=80.01),
    )
    assert len(received) == 1
    pair_id, basis_bps, a, b = received[0]
    assert pair_id == "wti_cme_hl"
    # mid_a = 80.05, mid_b = 80.00 → 6.25bp
    assert 5.5 < basis_bps < 6.5
    assert a.exchange == "hl"
    assert b.exchange == "kis"


def test_orderbook_mid_used_when_quotes_complete(collector, wti_pair):
    """exec_basis와 align — orderbook mid 기반."""
    collector.register_pair(wti_pair)
    received = []
    collector.on_pair_basis(lambda *args: received.append(args))

    # mid는 99.40인데 orderbook은 99.18/99.18 → orderbook mid 99.18
    collector.update_leg_quote(
        "wti_cme_hl", "a",
        Quote(exchange="hl", symbol="xyz:CL", mid_price=99.40,
              bid=99.18, ask=99.18),     # mark과 orderbook 괴리
    )
    collector.update_leg_quote(
        "wti_cme_hl", "b",
        Quote(exchange="kis", symbol="MCLM26", mid_price=99.10,
              bid=99.18, ask=99.19),
    )
    pair_id, basis_bps, a, b = received[0]
    # orderbook 기반: (99.18 - 99.185) / 99.185 * 10000 ≈ -0.5bp
    # mark 기반이었다면 ~30bp의 phantom 신호
    assert abs(basis_bps) < 1.5


def test_falls_back_to_mid_when_orderbook_missing(collector, wti_pair):
    collector.register_pair(wti_pair)
    received = []
    collector.on_pair_basis(lambda *args: received.append(args))

    collector.update_leg_quote(
        "wti_cme_hl", "a",
        Quote(exchange="hl", symbol="xyz:CL", mid_price=80.10, bid=0.0, ask=0.0),
    )
    collector.update_leg_quote(
        "wti_cme_hl", "b",
        Quote(exchange="kis", symbol="MCLM26", mid_price=80.00, bid=0.0, ask=0.0),
    )
    pair_id, basis_bps, a, b = received[0]
    # mid_a/mid_b 기반: (80.10 - 80.00)/80.00 * 10000 = 12.5bp
    assert 12.0 < basis_bps < 13.0


def test_callback_fires_on_each_subsequent_update(collector, wti_pair):
    """양쪽 leg 모두 도착 후 한쪽 update만 와도 callback 재호출."""
    collector.register_pair(wti_pair)
    received = []
    collector.on_pair_basis(lambda *args: received.append(args))

    collector.update_leg_quote("wti_cme_hl", "a",
                                Quote(exchange="hl", symbol="x", mid_price=80, bid=80, ask=80))
    collector.update_leg_quote("wti_cme_hl", "b",
                                Quote(exchange="kis", symbol="y", mid_price=80, bid=80, ask=80))
    assert len(received) == 1

    # leg_a만 update → callback 또 한 번
    collector.update_leg_quote("wti_cme_hl", "a",
                                Quote(exchange="hl", symbol="x", mid_price=80.1, bid=80.1, ask=80.1))
    assert len(received) == 2


def test_multiple_pairs_isolated(collector):
    """다른 pair_id 끼리 콜백/state 분리."""
    p1 = ArbitragePair(
        id="wti_cme_hl",
        leg_a=ExchangeLeg(exchange="hl", symbol="xyz:CL", role=LegRole.PERP),
        leg_b=ExchangeLeg(exchange="kis", symbol="MCLM26", role=LegRole.DATED_FUTURES),
    )
    p2 = ArbitragePair(
        id="wti_hl_binance",
        leg_a=ExchangeLeg(exchange="hl", symbol="xyz:CL", role=LegRole.PERP),
        leg_b=ExchangeLeg(exchange="binance", symbol="CLUSDT", role=LegRole.PERP),
    )
    collector.register_pair(p1)
    collector.register_pair(p2)
    received = []
    collector.on_pair_basis(lambda *args: received.append(args[0]))    # pair_id만

    collector.update_leg_quote("wti_cme_hl", "a",
                                Quote(exchange="hl", symbol="x", mid_price=80, bid=80, ask=80))
    collector.update_leg_quote("wti_cme_hl", "b",
                                Quote(exchange="kis", symbol="y", mid_price=80, bid=80, ask=80))
    collector.update_leg_quote("wti_hl_binance", "a",
                                Quote(exchange="hl", symbol="x", mid_price=80, bid=80, ask=80))
    # binance leg 아직 없음 → 두 번째 콜백은 안 발생
    assert received == ["wti_cme_hl"]

    collector.update_leg_quote("wti_hl_binance", "b",
                                Quote(exchange="binance", symbol="z", mid_price=80, bid=80, ask=80))
    assert received == ["wti_cme_hl", "wti_hl_binance"]


# ──────────────────────────────────────────────
# 레거시 product-keyed 경로 무영향 검증
# ──────────────────────────────────────────────


def test_legacy_basis_callbacks_separate_from_pair_callbacks(collector, wti_pair):
    """on_basis_update 콜백은 pair-keyed 경로 영향 안 받음."""
    legacy_calls = []
    pair_calls = []
    collector.on_basis_update(lambda *args: legacy_calls.append(args))
    collector.on_pair_basis(lambda *args: pair_calls.append(args))
    collector.register_pair(wti_pair)

    collector.update_leg_quote("wti_cme_hl", "a",
                                Quote(exchange="hl", symbol="x", mid_price=80, bid=80, ask=80))
    collector.update_leg_quote("wti_cme_hl", "b",
                                Quote(exchange="kis", symbol="y", mid_price=80, bid=80, ask=80))
    # 레거시 콜백은 update_leg_quote에서 트리거 안 됨
    assert legacy_calls == []
    assert len(pair_calls) == 1


def test_legacy_caches_untouched_by_pair_path(collector, wti_pair):
    """update_leg_quote는 _latest_perp / _latest_futures 안 건드림."""
    collector.register_pair(wti_pair)
    collector.update_leg_quote("wti_cme_hl", "a",
                                Quote(exchange="hl", symbol="x", mid_price=80, bid=80, ask=80))
    assert collector._latest_perp == {}
    assert collector._latest_futures == {}
