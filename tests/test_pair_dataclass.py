"""Phase A 회귀 — ArbitragePair, ExchangeLeg, PairGate, PairStrategyParams."""

from __future__ import annotations

import pytest

from src.strategy.pair import (
    ArbitragePair,
    ExchangeLeg,
    LegRole,
    PairGate,
    PairStrategyParams,
)


# ──────────────────────────────────────────────
# ExchangeLeg
# ──────────────────────────────────────────────


def test_exchange_leg_perp_defaults():
    leg = ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                     taker_fee_bps=0.9, funding_interval_hours=1.0,
                     margin_asset="USDC")
    assert leg.contract_size == 1.0
    assert leg.fee_per_contract_usd == 0.0
    assert leg.role == LegRole.PERP


def test_exchange_leg_dated_futures():
    leg = ExchangeLeg(exchange="kis", symbol="MCLM26", role=LegRole.DATED_FUTURES,
                     contract_size=100, fee_per_contract_usd=2.50)
    assert leg.contract_size == 100
    assert leg.fee_per_contract_usd == 2.50
    assert leg.taker_fee_bps == 0.0
    assert leg.funding_interval_hours == 0.0


# ──────────────────────────────────────────────
# PairGate / LegRole enums
# ──────────────────────────────────────────────


def test_pair_gate_values():
    assert PairGate.CME_HOURS.value == "cme_hours"
    assert PairGate.ALWAYS.value == "always"


def test_leg_role_values():
    assert LegRole.PERP.value == "perp"
    assert LegRole.DATED_FUTURES.value == "dated_futures"


# ──────────────────────────────────────────────
# PairStrategyParams
# ──────────────────────────────────────────────


def test_pair_strategy_params_defaults():
    p = PairStrategyParams()
    assert p.basis_window_hours == 24.0
    assert p.entry_threshold_bps == 20.0
    assert p.convergence_target_bps == 3.0
    assert p.max_hold_hours == 48.0
    assert p.emergency_close_bps == 100.0


def test_pair_strategy_params_override():
    p = PairStrategyParams(entry_threshold_bps=15.0, max_hold_hours=12.0)
    assert p.entry_threshold_bps == 15.0
    assert p.max_hold_hours == 12.0


# ──────────────────────────────────────────────
# ArbitragePair
# ──────────────────────────────────────────────


def _build_cme_hl_pair() -> ArbitragePair:
    return ArbitragePair(
        id="wti_cme_hl", enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                         taker_fee_bps=0.9, funding_interval_hours=1.0,
                         margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange="kis", symbol="MCLM26", role=LegRole.DATED_FUTURES,
                         contract_size=100, fee_per_contract_usd=2.50),
        params=PairStrategyParams(entry_threshold_bps=20),
    )


def _build_hl_binance_pair() -> ArbitragePair:
    return ArbitragePair(
        id="wti_hl_binance", enabled=False, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL", role=LegRole.PERP,
                         taker_fee_bps=0.9, funding_interval_hours=1.0,
                         margin_asset="USDC"),
        leg_b=ExchangeLeg(exchange="binance", symbol="CLUSDT", role=LegRole.PERP,
                         taker_fee_bps=4.0, funding_interval_hours=4.0,
                         margin_asset="USDT"),
        params=PairStrategyParams(entry_threshold_bps=15, max_hold_hours=12),
    )


def test_arbitrage_pair_cme_hl_construction():
    pair = _build_cme_hl_pair()
    assert pair.id == "wti_cme_hl"
    assert pair.enabled is True
    assert pair.gate == PairGate.CME_HOURS
    assert pair.strategy == "basis_convergence"
    assert pair.leg_a.exchange == "hyperliquid"
    assert pair.leg_b.exchange == "kis"


def test_arbitrage_pair_hl_binance_construction():
    pair = _build_hl_binance_pair()
    assert pair.leg_a.exchange == "hyperliquid"
    assert pair.leg_b.exchange == "binance"
    assert pair.leg_a.funding_interval_hours == 1.0
    assert pair.leg_b.funding_interval_hours == 4.0
    assert pair.params.max_hold_hours == 12.0


def test_arbitrage_pair_default_disabled():
    pair = ArbitragePair(
        id="x",
        leg_a=ExchangeLeg(exchange="a", symbol="A", role=LegRole.PERP),
        leg_b=ExchangeLeg(exchange="b", symbol="B", role=LegRole.PERP),
    )
    assert pair.enabled is False
    assert pair.gate == PairGate.CME_HOURS
    assert pair.params.entry_threshold_bps == 20.0


def test_fee_round_trip_bps_perp_perp():
    """양 leg 모두 perp인 경우 — 4*entry/exit fee."""
    pair = _build_hl_binance_pair()
    # 2 * (0.9 + 4.0) = 9.8
    assert pair.fee_round_trip_bps == pytest.approx(9.8)


def test_fee_round_trip_bps_perp_dated_futures():
    """월물 leg는 taker_fee_bps=0이라 perp 쪽만 카운트."""
    pair = _build_cme_hl_pair()
    # 2 * (0.9 + 0.0) = 1.8
    assert pair.fee_round_trip_bps == pytest.approx(1.8)


def test_pair_leg_lookup_by_name():
    pair = _build_cme_hl_pair()
    assert pair.leg("a") is pair.leg_a
    assert pair.leg("b") is pair.leg_b
    with pytest.raises(ValueError):
        pair.leg("c")


def test_pair_opposite_leg():
    pair = _build_cme_hl_pair()
    assert pair.opposite_leg("a") is pair.leg_b
    assert pair.opposite_leg("b") is pair.leg_a
    with pytest.raises(ValueError):
        pair.opposite_leg("c")
