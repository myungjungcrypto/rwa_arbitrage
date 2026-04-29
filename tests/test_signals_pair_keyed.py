"""Phase C3 회귀 — SignalGenerator pair-keyed 호환성.

목표: 같은 SignalGenerator 인스턴스가 product key('wti')와 pair_id key
('wti_cme_hl', 'wti_hl_binance')를 동시에 사용해도 서로 간섭 없이 동작.

레거시 코드 (engine.py)는 product='wti'를 그대로 쓰고, Phase C5 이후 신규
코드는 pair_id를 사용. 둘 다 같은 deque dict 키 공간을 공유하므로 충돌
없음을 명시적으로 검증.
"""

from __future__ import annotations

from src.strategy.signals import (
    PositionState, Signal, SignalGenerator, SignalType,
)


def _gen() -> SignalGenerator:
    return SignalGenerator(
        window_hours=1, std_multiplier=2.0,
        entry_threshold_bps=20, max_hold_hours=24,
        cme_closed_skip_entry=False,    # 테스트에서 24/7 가정
    )


# ──────────────────────────────────────────────
# Key-agnostic basics
# ──────────────────────────────────────────────


def test_pair_id_can_be_used_as_key():
    g = _gen()
    g.bootstrap_from_db("wti_cme_hl", [1.0, 2.0, 3.0])
    assert "wti_cme_hl" in g._basis_history
    assert len(g._basis_history["wti_cme_hl"]) == 3


def test_product_key_independent_from_pair_key():
    g = _gen()
    g.bootstrap_from_db("wti", [1.0, 2.0])
    g.bootstrap_from_db("wti_cme_hl", [10.0, 20.0, 30.0])
    assert len(g._basis_history["wti"]) == 2
    assert len(g._basis_history["wti_cme_hl"]) == 3


def test_get_basis_stats_per_key(tmp_path):
    g = _gen()
    g.bootstrap_from_db("wti", [0.0, 0.0, 0.0])           # 평균 0
    g.bootstrap_from_db("wti_cme_hl", [10.0, 10.0, 10.0]) # 평균 10
    s_legacy = g.get_basis_stats("wti")
    s_pair = g.get_basis_stats("wti_cme_hl")
    assert s_legacy["mean"] == 0.0
    assert s_pair["mean"] == 10.0


def test_get_position_per_key():
    g = _gen()
    p1 = g.get_position("wti")
    p2 = g.get_position("wti_cme_hl")
    assert p1 is not p2
    assert p1.product == "wti"
    assert p2.product == "wti_cme_hl"
    p1.is_open = True
    assert g.get_position("wti").is_open is True
    assert g.get_position("wti_cme_hl").is_open is False


# ──────────────────────────────────────────────
# Aliases
# ──────────────────────────────────────────────


def test_bootstrap_from_db_for_pair_alias():
    g = _gen()
    g.bootstrap_from_db_for_pair("wti_hl_binance", [1.0, 2.0, 3.0, 4.0])
    assert len(g._basis_history["wti_hl_binance"]) == 4


def test_update_basis_for_pair_routes_to_same_state():
    g = _gen()
    # bootstrap with enough history (need >= 20)
    history = [0.0] * 30
    g.bootstrap_from_db_for_pair("wti_cme_hl", history)
    sig = g.update_basis_for_pair("wti_cme_hl", basis_bps=0.0)
    # 동일 키로 호출하면 통계가 정상 반영
    stats = g.get_basis_stats_for_pair("wti_cme_hl")
    assert stats["count"] >= 30


def test_get_position_for_pair_alias():
    g = _gen()
    p = g.get_position_for_pair("wti_hl_okx")
    assert p.product == "wti_hl_okx"


def test_open_close_for_pair_alias():
    g = _gen()
    pair_id = "wti_hl_lighter"
    sig = Signal(
        type=SignalType.ENTRY_LONG_BASIS, product=pair_id,
        basis_bps=25.0, basis_mean=0.0, basis_std=5.0,
    )
    g.open_position_for_pair(pair_id, sig, size=1.0)
    pos = g.get_position_for_pair(pair_id)
    assert pos.is_open
    assert pos.direction == "long_basis"
    g.close_position_for_pair(pair_id)
    assert not g.get_position_for_pair(pair_id).is_open


def test_add_funding_for_pair_alias():
    g = _gen()
    pair_id = "wti_hl_binance"
    sig = Signal(
        type=SignalType.ENTRY_LONG_BASIS, product=pair_id,
        basis_bps=25.0, basis_mean=0.0, basis_std=5.0,
    )
    g.open_position_for_pair(pair_id, sig)
    g.add_funding_for_pair(pair_id, funding_rate=0.0001)
    pos = g.get_position_for_pair(pair_id)
    # perp_side가 "short"이면 funding > 0이면 수취
    assert pos.cumulative_funding != 0


# ──────────────────────────────────────────────
# Multiple pair_ids 동시 운용
# ──────────────────────────────────────────────


def test_three_pair_ids_isolated():
    g = _gen()
    history = [0.0] * 30
    for pid in ("wti_cme_hl", "wti_hl_binance", "wti_hl_lighter"):
        g.bootstrap_from_db_for_pair(pid, history)

    g.update_basis_for_pair("wti_cme_hl", basis_bps=10.0)
    g.update_basis_for_pair("wti_hl_binance", basis_bps=-5.0)
    g.update_basis_for_pair("wti_hl_lighter", basis_bps=2.0)

    # 각 키의 마지막 값이 다른지 확인
    assert g._basis_history["wti_cme_hl"][-1] == 10.0
    assert g._basis_history["wti_hl_binance"][-1] == -5.0
    assert g._basis_history["wti_hl_lighter"][-1] == 2.0


def test_position_state_isolated_across_pair_ids():
    g = _gen()
    sig_long = Signal(
        type=SignalType.ENTRY_LONG_BASIS, product="ignored",
        basis_bps=25.0, basis_mean=0.0, basis_std=5.0,
    )
    sig_short = Signal(
        type=SignalType.ENTRY_SHORT_BASIS, product="ignored",
        basis_bps=-25.0, basis_mean=0.0, basis_std=5.0,
    )
    g.open_position_for_pair("wti_cme_hl", sig_long)
    g.open_position_for_pair("wti_hl_binance", sig_short)
    p1 = g.get_position_for_pair("wti_cme_hl")
    p2 = g.get_position_for_pair("wti_hl_binance")
    assert p1.direction == "long_basis"
    assert p2.direction == "short_basis"
    # 한쪽 close해도 다른 건 그대로
    g.close_position_for_pair("wti_cme_hl")
    assert not p1.is_open
    assert p2.is_open


# ──────────────────────────────────────────────
# Backward compat — 레거시 호출도 그대로 작동
# ──────────────────────────────────────────────


def test_legacy_product_key_still_works_alongside_pair_key():
    """기존 코드가 product='wti'로 호출해도 새 pair_id 코드와 충돌 없음."""
    g = _gen()
    history = [0.0] * 30

    # 두 키 모두 부트스트랩
    g.bootstrap_from_db("wti", history)               # legacy
    g.bootstrap_from_db_for_pair("wti_cme_hl", history)  # new

    # 다른 값 push
    g.update_basis("wti", basis_bps=5.0)
    g.update_basis_for_pair("wti_cme_hl", basis_bps=15.0)

    # 키별로 마지막 값 다름
    assert g._basis_history["wti"][-1] == 5.0
    assert g._basis_history["wti_cme_hl"][-1] == 15.0
