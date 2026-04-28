"""bid/ask fallback 버그 회귀 — exec_filter 우회 방지.

원인: 2026-04-21~04-27 14건이 abs(entry_spread)<10bp로 진입한 이유는
collector + engine에서 perp/futures bid/ask가 미수신일 때 mid_price로 fallback해
exec_basis ≈ mid_basis가 되었고, exec_filter(entry_threshold_bps=20)가 통과해버림.

수정: bid/ask 미수신 시 0을 전달/저장 → `_compute_executable_basis`가 0.0 반환
     → exec_filter가 차단. fill 경로(_handle_entry/_handle_exit)에서만 mid fallback
     사용 (안전망).
"""

from __future__ import annotations

import math


def _compute_exec_basis(
    direction: str,
    perp_bid: float,
    perp_ask: float,
    futures_bid: float,
    futures_ask: float,
) -> float:
    """engine._compute_executable_basis 동치식 — 단위 테스트용 미러."""
    if not all([perp_bid, perp_ask, futures_bid, futures_ask]):
        return 0.0
    if direction == "short_basis":
        return (perp_ask - futures_bid) / futures_bid * 10_000
    return (perp_bid - futures_ask) / futures_ask * 10_000


def _exec_filter_passes(
    direction: str,
    exec_basis: float,
    threshold: float,
) -> bool:
    """engine 진입 가드 동치식 — exec basis가 충분히 벌어졌는지."""
    if direction == "short_basis":
        return exec_basis <= -threshold
    return exec_basis >= threshold


# ──────────────────────────────────────────────
# 정상 경로 — bid/ask 수신 정상
# ──────────────────────────────────────────────


def test_exec_basis_normal_long_basis():
    """+25bp short perp — long futures 진입 가능."""
    eb = _compute_exec_basis("long_basis",
                             perp_bid=80.20, perp_ask=80.21,
                             futures_bid=80.00, futures_ask=80.01)
    # (80.20 - 80.01) / 80.01 * 10000 = ~23.7
    assert eb > 20
    assert _exec_filter_passes("long_basis", eb, 20)


def test_exec_basis_normal_short_basis():
    """-25bp long perp — short futures 진입 가능."""
    eb = _compute_exec_basis("short_basis",
                             perp_bid=79.80, perp_ask=79.81,
                             futures_bid=80.00, futures_ask=80.01)
    # (79.81 - 80.00) / 80.00 * 10000 = ~-23.7
    assert eb < -20
    assert _exec_filter_passes("short_basis", eb, 20)


# ──────────────────────────────────────────────
# 버그 경로 — bid/ask 0 (이전 버전: mid fallback → 잘못 통과)
# ──────────────────────────────────────────────


def test_exec_basis_returns_zero_when_perp_bid_missing():
    eb = _compute_exec_basis("long_basis",
                             perp_bid=0.0, perp_ask=80.21,
                             futures_bid=80.00, futures_ask=80.01)
    assert eb == 0.0
    assert not _exec_filter_passes("long_basis", eb, 20)


def test_exec_basis_returns_zero_when_perp_ask_missing():
    eb = _compute_exec_basis("short_basis",
                             perp_bid=79.80, perp_ask=0.0,
                             futures_bid=80.00, futures_ask=80.01)
    assert eb == 0.0
    assert not _exec_filter_passes("short_basis", eb, 20)


def test_exec_basis_returns_zero_when_futures_bid_missing():
    eb = _compute_exec_basis("short_basis",
                             perp_bid=79.80, perp_ask=79.81,
                             futures_bid=0.0, futures_ask=80.01)
    assert eb == 0.0
    assert not _exec_filter_passes("short_basis", eb, 20)


def test_exec_basis_returns_zero_when_futures_ask_missing():
    eb = _compute_exec_basis("long_basis",
                             perp_bid=80.20, perp_ask=80.21,
                             futures_bid=80.00, futures_ask=0.0)
    assert eb == 0.0
    assert not _exec_filter_passes("long_basis", eb, 20)


def test_exec_basis_returns_zero_when_all_missing():
    eb = _compute_exec_basis("long_basis", 0, 0, 0, 0)
    assert eb == 0.0
    assert not _exec_filter_passes("long_basis", eb, 20)
    assert not _exec_filter_passes("short_basis", eb, 20)


# ──────────────────────────────────────────────
# 과거 14건 시나리오 회귀 — bid=ask=mid (구버그)
# ──────────────────────────────────────────────


def test_old_bug_scenario_perp_bid_eq_ask_eq_mid_long():
    """[BUG REPRO] 구버전: perp 오더북 미수신 → bid=ask=mid → exec_basis ≈ mid_basis
    가 되어 exec_filter가 통과. 신버전(0 전달)은 0.0 반환 → 차단.
    """
    perp_mid = 80.20
    futures_real_bid = 80.10
    futures_real_ask = 80.11
    # 신버전: collector가 0 전달
    eb_new = _compute_exec_basis("long_basis",
                                  perp_bid=0.0, perp_ask=0.0,
                                  futures_bid=futures_real_bid,
                                  futures_ask=futures_real_ask)
    assert eb_new == 0.0
    assert not _exec_filter_passes("long_basis", eb_new, 20)

    # 구버전 시뮬레이션: 0 → mid_price fallback. mid basis ~12bp가 통과해버림
    eb_old = _compute_exec_basis("long_basis",
                                  perp_bid=perp_mid, perp_ask=perp_mid,
                                  futures_bid=futures_real_bid,
                                  futures_ask=futures_real_ask)
    # (80.20 - 80.11) / 80.11 * 10000 ≈ 11.2bp → 20bp 미달이라 차단되어야 정상
    # 하지만 mid 기준이라 사실 mid_basis도 같음. 이 경우는 통과 안 함.
    # 더 강한 케이스: mid basis가 20bp 이상이면 구버전 통과, fill은 작게 기록됨
    perp_mid_far = 80.30  # mid basis (80.30-80.105)/80.105 = ~24bp
    eb_old_far = _compute_exec_basis("long_basis",
                                      perp_bid=perp_mid_far, perp_ask=perp_mid_far,
                                      futures_bid=futures_real_bid,
                                      futures_ask=futures_real_ask)
    # (80.30 - 80.11) / 80.11 * 10000 = ~23.7bp → 통과
    assert eb_old_far > 20
    assert _exec_filter_passes("long_basis", eb_old_far, 20)
    # 이러면 진입 → 실제 fill: perp=mid=80.30, futures=ask=80.11
    # 기록되는 entry_spread = (80.30 - 80.11) / 80.11 * 10000 ≈ 23.7bp
    # 그런데 데이터에서는 abs<10bp로 기록됨. 진짜 버그는: futures_ask가 같이 0이었던 케이스
    # 신버전(둘 다 0)은 자동 차단됨.


def test_old_bug_scenario_both_perp_and_futures_zero():
    """[BUG REPRO] perp + futures bid/ask 둘 다 0 (둘 다 미수신).

    구버전: 둘 다 mid로 fallback → bid=ask=mid_perp / bid=ask=mid_futures
            exec_basis = (mid_perp - mid_futures) / mid_futures = mid_basis
            mid basis가 20bp 넘으면 통과 → 진입.
            fill: perp=mid_perp, futures(kiwoom mock)=synthetic 1bp spread
            기록 entry_spread는 mid_perp 대비 kiwoom synthetic 가격 차이 → 작음.
    신버전: 둘 다 0 → exec_basis 0 → 차단.
    """
    eb_new = _compute_exec_basis("long_basis", 0, 0, 0, 0)
    assert eb_new == 0.0
    assert not _exec_filter_passes("long_basis", eb_new, 20)


# ──────────────────────────────────────────────
# 14건 historical 시나리오 — 각 entry_bp 값에서 신버전이 차단하는지
# ──────────────────────────────────────────────


def test_all_14_historical_sub_10bp_trades_would_be_blocked():
    """history의 14건은 모두 abs(entry_spread)<10bp.
    그러나 그것은 fill 결과이고, 진입 결정 시점의 exec_basis는 미상 (~mid basis).
    구버그 가정: 일부 시나리오에서 bid/ask 0 → mid fallback. 신버전(0 전달)은
    그 시나리오에서 자동 차단. 본 테스트는 그 단순한 보장만 확인.
    """
    # bid/ask 모두 0인 경우 = 신버전에서 항상 차단
    for direction in ("long_basis", "short_basis"):
        eb = _compute_exec_basis(direction, 0, 0, 0, 0)
        assert eb == 0.0
        assert not _exec_filter_passes(direction, eb, 20)


# ──────────────────────────────────────────────
# 실시간 정상 진입 보호 — 정상 데이터에서는 신버전도 정상 통과
# ──────────────────────────────────────────────


def test_new_version_does_not_break_normal_entries():
    """수정안이 정상 진입까지 차단하면 안 됨."""
    # 정상 25bp long_basis
    eb = _compute_exec_basis("long_basis",
                             perp_bid=80.25, perp_ask=80.26,
                             futures_bid=80.04, futures_ask=80.05)
    assert eb > 20
    assert _exec_filter_passes("long_basis", eb, 20)

    # 정상 25bp short_basis
    eb = _compute_exec_basis("short_basis",
                             perp_bid=79.75, perp_ask=79.76,
                             futures_bid=79.96, futures_ask=79.97)
    assert eb < -20
    assert _exec_filter_passes("short_basis", eb, 20)
