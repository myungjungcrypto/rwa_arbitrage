"""Phase D: settings.yaml `pairs:` 블록 → ArbitragePair 합성.

Web3-Web3 페어(wti_hl_lighter 등)를 명시 등록하는 경로 검증.
기존 product-based 합성과 공존.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from src.strategy.pair import LegRole, PairGate
from src.utils.config import (
    AppConfig, LighterConfig, ProductConfig, RiskConfig, StrategyConfig,
    _parse_extra_pairs, load_config,
)


# ──────────────────────────────────────────────
# LighterConfig defaults
# ──────────────────────────────────────────────


def test_lighter_config_defaults():
    cfg = LighterConfig()
    assert cfg.enabled is False
    assert cfg.base_url.startswith("https://")
    assert cfg.ws_url.startswith("wss://")


def test_appconfig_lighter_defaults_disabled():
    cfg = AppConfig()
    assert cfg.lighter.enabled is False


# ──────────────────────────────────────────────
# _parse_extra_pairs
# ──────────────────────────────────────────────


def test_parse_extra_pairs_empty():
    assert _parse_extra_pairs([]) == []
    assert _parse_extra_pairs(None) == []


def test_parse_extra_pairs_minimal():
    pairs = _parse_extra_pairs([{
        "id": "wti_hl_lighter",
        "enabled": False,
        "leg_a": {"exchange": "hyperliquid", "symbol": "xyz:CL",
                   "role": "perp", "taker_fee_bps": 0.9,
                   "funding_interval_hours": 1.0, "margin_asset": "USDC"},
        "leg_b": {"exchange": "lighter", "symbol": "WTI",
                   "role": "perp", "taker_fee_bps": 0.0,
                   "funding_interval_hours": 1.0, "margin_asset": "USDC"},
        "params": {"entry_threshold_bps": 12, "max_hold_hours": 12},
    }])
    assert len(pairs) == 1
    p = pairs[0]
    assert p.id == "wti_hl_lighter"
    assert p.enabled is False
    assert p.leg_a.exchange == "hyperliquid"
    assert p.leg_a.role == LegRole.PERP
    assert p.leg_b.exchange == "lighter"
    assert p.leg_b.symbol == "WTI"
    assert p.params.entry_threshold_bps == 12
    assert p.params.max_hold_hours == 12


def test_parse_extra_pairs_skips_invalid_entries():
    pairs = _parse_extra_pairs([
        {},                     # id 없음
        "not-a-dict",
        {"id": "valid", "leg_a": {}, "leg_b": {}},   # 비어있어도 진행
    ])
    # invalid 2개 skip, 1개만
    assert len(pairs) == 1
    assert pairs[0].id == "valid"


def test_parse_extra_pairs_default_gate_is_cme_hours():
    pairs = _parse_extra_pairs([{"id": "x", "leg_a": {}, "leg_b": {}}])
    assert pairs[0].gate == PairGate.CME_HOURS


def test_parse_extra_pairs_explicit_gate_always():
    pairs = _parse_extra_pairs([{"id": "x", "gate": "always",
                                   "leg_a": {}, "leg_b": {}}])
    assert pairs[0].gate == PairGate.ALWAYS


# ──────────────────────────────────────────────
# AppConfig.get_pairs() — synthesized + extra 합산
# ──────────────────────────────────────────────


def test_get_pairs_includes_synthesized_and_extra():
    extra = _parse_extra_pairs([{
        "id": "wti_hl_lighter", "enabled": False,
        "leg_a": {"exchange": "hyperliquid", "symbol": "xyz:CL", "role": "perp"},
        "leg_b": {"exchange": "lighter", "symbol": "WTI", "role": "perp"},
    }])
    cfg = AppConfig(
        products={"wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL",
                                        contract_size=100, futures_fee_per_contract=2.5)},
        kis_symbol_map={"wti": "MCLM26"},
        strategy=StrategyConfig(),
        risk=RiskConfig(),
        extra_pairs=extra,
    )
    pairs = cfg.get_pairs()
    pair_ids = {p.id for p in pairs}
    assert pair_ids == {"wti_cme_hl", "wti_hl_lighter"}


def test_get_pair_finds_extra_pair():
    extra = _parse_extra_pairs([{
        "id": "wti_hl_binance", "enabled": True,
        "leg_a": {"exchange": "hyperliquid", "symbol": "xyz:CL", "role": "perp"},
        "leg_b": {"exchange": "binance", "symbol": "CLUSDT", "role": "perp"},
    }])
    cfg = AppConfig(
        products={"wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="MCL")},
        kis_symbol_map={"wti": "MCL"},
        strategy=StrategyConfig(),
        risk=RiskConfig(),
        extra_pairs=extra,
    )
    p = cfg.get_pair("wti_hl_binance")
    assert p is not None
    assert p.leg_b.exchange == "binance"


# ──────────────────────────────────────────────
# Real settings.yaml — wti_hl_lighter shadow 페어 정상 로드
# ──────────────────────────────────────────────


def test_real_settings_yaml_loads_lighter_pair():
    cfg = load_config("config/settings.yaml")
    pair_ids = {p.id for p in cfg.get_pairs()}
    assert "wti_cme_hl" in pair_ids
    assert "wti_hl_lighter" in pair_ids
    lp = cfg.get_pair("wti_hl_lighter")
    # shadow 모드로 시작
    assert lp.enabled is False
    assert lp.leg_a.exchange == "hyperliquid"
    assert lp.leg_b.exchange == "lighter"
    assert lp.leg_b.funding_interval_hours == 1.0
    assert lp.leg_b.margin_asset == "USDC"


def test_real_settings_yaml_loads_lighter_section():
    cfg = load_config("config/settings.yaml")
    assert cfg.lighter.enabled is False    # 시작 시 OFF
    assert "lighter" in cfg.lighter.base_url
