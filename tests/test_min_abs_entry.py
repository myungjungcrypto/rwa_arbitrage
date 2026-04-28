"""min_abs_entry_bps 절대값 진입 floor 회귀 테스트.

전략적 통계 신호(z-score) 통과해도 |exec_basis|가 floor 미달이면 진입 차단.
2026-04-21~04-27 페이퍼 분석에 따른 hotfix:
  <10bp 진입 14건 중 2승 -$202, 10bp+ 진입 16건 중 15승 +$199.
"""

from __future__ import annotations

from src.paper.engine import EngineState
from src.utils.config import StrategyConfig


# ──────────────────────────────────────────────
# Config field
# ──────────────────────────────────────────────


def test_min_abs_entry_bps_default_disabled():
    """기존 운영 호환 — 기본값 0이면 floor 비활성."""
    cfg = StrategyConfig()
    assert cfg.min_abs_entry_bps == 0.0


def test_min_abs_entry_bps_settable():
    cfg = StrategyConfig(min_abs_entry_bps=10.0)
    assert cfg.min_abs_entry_bps == 10.0


# ──────────────────────────────────────────────
# Engine state counter
# ──────────────────────────────────────────────


def test_engine_state_has_min_abs_skip_counter():
    state = EngineState()
    assert state.entry_min_abs_skip == 0


def test_engine_state_other_counters_unchanged():
    """기존 카운터는 그대로 유지 (회귀 방지)."""
    state = EngineState()
    assert state.entry_signals_generated == 0
    assert state.entry_exec_filter_skip == 0
    assert state.entry_warmup_skip == 0
    assert state.entry_min_abs_skip == 0


# ──────────────────────────────────────────────
# Floor logic 회귀 (직접 비교 — engine.py:_process_signal 안의 분기 미러)
# ──────────────────────────────────────────────


def _abs_floor_blocks(exec_basis_bps: float, min_abs: float) -> bool:
    """engine.py 안의 가드 동치식 — 단위 테스트용 미러."""
    return min_abs > 0 and abs(exec_basis_bps) < min_abs


def test_floor_disabled_lets_everything_through():
    assert not _abs_floor_blocks(0.5, 0.0)
    assert not _abs_floor_blocks(-3.0, 0.0)
    assert not _abs_floor_blocks(50.0, 0.0)


def test_floor_blocks_below_threshold_long_basis():
    """+5bp exec basis (long_basis 약진입) — min_abs=10 이면 차단."""
    assert _abs_floor_blocks(5.0, 10.0)
    assert _abs_floor_blocks(9.99, 10.0)


def test_floor_blocks_below_threshold_short_basis():
    """-7bp exec basis (short_basis 약진입) — min_abs=10 이면 차단."""
    assert _abs_floor_blocks(-7.0, 10.0)
    assert _abs_floor_blocks(-9.99, 10.0)


def test_floor_lets_through_at_or_above_threshold():
    """경계값 + 큰 spread는 통과."""
    assert not _abs_floor_blocks(10.0, 10.0)
    assert not _abs_floor_blocks(-10.0, 10.0)
    assert not _abs_floor_blocks(25.0, 10.0)
    assert not _abs_floor_blocks(-50.0, 10.0)


def test_floor_realistic_buckets_match_historical_analysis():
    """과거 분석과 동치 — <10bp는 차단, 10-20bp/20-50bp는 통과."""
    floor = 10.0
    blocked = [-5.0, -8.5, 2.4, 4.2, 8.2, -2.5]
    passed = [10.0, 12.4, 16.2, 28.3, 30.0, -28.3, -534.1]
    for b in blocked:
        assert _abs_floor_blocks(b, floor), f"expected {b}bp to be blocked at floor={floor}"
    for p in passed:
        assert not _abs_floor_blocks(p, floor), f"expected {p}bp to pass at floor={floor}"
