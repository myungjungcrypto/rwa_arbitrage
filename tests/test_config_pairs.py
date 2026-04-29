"""Phase C1 회귀 — AppConfig.get_pairs() 합성.

기존 products + kis_symbol_map 구성에서 ArbitragePair 리스트로 자동 합성.
실제 settings.yaml로도 한 번 검증.
"""

from __future__ import annotations

from src.utils.config import AppConfig, ProductConfig, StrategyConfig, RiskConfig
from src.strategy.pair import ArbitragePair, ExchangeLeg, LegRole, PairGate


def _make_wti_config() -> AppConfig:
    return AppConfig(
        products={
            "wti": ProductConfig(
                perp_ticker="xyz:CL",
                futures_symbol="MCL",
                contract_size=100,
                min_order_size=1,
                futures_fee_per_contract=2.50,
            ),
        },
        kis_symbol_map={"wti": "MCLM26"},
        strategy=StrategyConfig(
            basis_window_hours=24,
            basis_std_multiplier=3.0,
            entry_threshold_bps=20,
            convergence_target_bps=3,
            max_hold_hours=48,
            min_abs_entry_bps=10,
        ),
        risk=RiskConfig(emergency_close_threshold=100),
    )


# ──────────────────────────────────────────────
# Synthesis
# ──────────────────────────────────────────────


def test_synthesizes_single_wti_pair():
    cfg = _make_wti_config()
    pairs = cfg.get_pairs()
    assert len(pairs) == 1
    assert pairs[0].id == "wti_cme_hl"


def test_pair_has_hl_kis_legs():
    cfg = _make_wti_config()
    pair = cfg.get_pairs()[0]
    assert pair.leg_a.exchange == "hyperliquid"
    assert pair.leg_a.symbol == "xyz:CL"
    assert pair.leg_a.role == LegRole.PERP
    assert pair.leg_a.taker_fee_bps == 0.9
    assert pair.leg_a.funding_interval_hours == 1.0
    assert pair.leg_a.margin_asset == "USDC"

    assert pair.leg_b.exchange == "kis"
    assert pair.leg_b.symbol == "MCLM26"
    assert pair.leg_b.role == LegRole.DATED_FUTURES
    assert pair.leg_b.contract_size == 100.0
    assert pair.leg_b.fee_per_contract_usd == 2.50


def test_pair_inherits_strategy_params():
    cfg = _make_wti_config()
    pair = cfg.get_pairs()[0]
    assert pair.params.basis_window_hours == 24
    assert pair.params.basis_std_multiplier == 3.0
    assert pair.params.entry_threshold_bps == 20
    assert pair.params.convergence_target_bps == 3
    assert pair.params.max_hold_hours == 48
    assert pair.params.emergency_close_bps == 100


def test_pair_default_gate_is_cme_hours():
    cfg = _make_wti_config()
    pair = cfg.get_pairs()[0]
    assert pair.gate == PairGate.CME_HOURS


def test_pair_enabled_by_default():
    cfg = _make_wti_config()
    pair = cfg.get_pairs()[0]
    assert pair.enabled is True


# ──────────────────────────────────────────────
# Lookup
# ──────────────────────────────────────────────


def test_get_pair_by_id():
    cfg = _make_wti_config()
    pair = cfg.get_pair("wti_cme_hl")
    assert pair is not None
    assert pair.id == "wti_cme_hl"


def test_get_pair_unknown_returns_none():
    cfg = _make_wti_config()
    assert cfg.get_pair("nonexistent") is None


# ──────────────────────────────────────────────
# Multi-product (brent + wti)
# ──────────────────────────────────────────────


def test_synthesizes_multiple_pairs():
    cfg = AppConfig(
        products={
            "wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL",
                                  contract_size=100, futures_fee_per_contract=2.5),
            "brent": ProductConfig(perp_ticker="xyz:BZ", futures_symbol="BZ",
                                    contract_size=1000, futures_fee_per_contract=7.5),
        },
        kis_symbol_map={"wti": "MCLM26", "brent": "BZN26"},
        strategy=StrategyConfig(),
        risk=RiskConfig(),
    )
    pairs = cfg.get_pairs()
    assert len(pairs) == 2
    pair_ids = sorted(p.id for p in pairs)
    assert pair_ids == ["brent_cme_hl", "wti_cme_hl"]


def test_brent_pair_uses_correct_contract_size():
    cfg = AppConfig(
        products={
            "brent": ProductConfig(perp_ticker="xyz:BZ", futures_symbol="BZ",
                                    contract_size=1000, futures_fee_per_contract=7.5),
        },
        kis_symbol_map={"brent": "BZN26"},
        strategy=StrategyConfig(),
        risk=RiskConfig(),
    )
    pair = cfg.get_pairs()[0]
    assert pair.leg_b.contract_size == 1000.0
    assert pair.leg_b.fee_per_contract_usd == 7.5


# ──────────────────────────────────────────────
# kis_symbol_map missing → fallback to product.futures_symbol
# ──────────────────────────────────────────────


def test_falls_back_when_kis_symbol_map_missing():
    cfg = AppConfig(
        products={
            "wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL",
                                  contract_size=100, futures_fee_per_contract=2.5),
        },
        kis_symbol_map={},   # 빈 매핑
        strategy=StrategyConfig(),
        risk=RiskConfig(),
    )
    pair = cfg.get_pairs()[0]
    # fallback: futures_symbol 사용
    assert pair.leg_b.symbol == "MCL"


# ──────────────────────────────────────────────
# Real settings.yaml round-trip
# ──────────────────────────────────────────────


def test_real_settings_yaml_roundtrip():
    """실제 운영 settings.yaml 로드 → 기대 페어 1개 (wti_cme_hl)."""
    from src.utils.config import load_config
    cfg = load_config("config/settings.yaml")
    pairs = cfg.get_pairs()
    assert len(pairs) >= 1
    wti = cfg.get_pair("wti_cme_hl")
    assert wti is not None
    assert wti.leg_a.exchange == "hyperliquid"
    assert wti.leg_b.exchange == "kis"
    assert wti.leg_b.symbol.startswith("MCL")  # MCLM26 또는 MCLN26 등
