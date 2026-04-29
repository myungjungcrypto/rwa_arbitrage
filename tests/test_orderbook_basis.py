"""orderbook-mid 기반 basis 계산 회귀.

이전: mark_price - futures_mid (HL 오라클 추적이라 HL orderbook과 20bp 괴리 가능
      → phantom 신호)
이후: (perp_bid+ask)/2 - (futures_bid+ask)/2 (실제 호가 mid)
      → exec_basis와 ±half-spread 수준의 차이만 남음
호가 미수신 시: mark-based fallback (stats 흐름 유지, exec_filter가 진입 자동 차단)
"""

from __future__ import annotations


def _compute_basis_bps(
    perp_mark: float, futures_mid: float,
    perp_bid: float, perp_ask: float,
    futures_bid: float, futures_ask: float,
) -> float:
    """collector._compute_basis 동치식 — 단위 테스트 미러."""
    if perp_bid > 0 and perp_ask > 0 and futures_bid > 0 and futures_ask > 0:
        perp_mid_ob = (perp_bid + perp_ask) / 2
        fut_mid_ob = (futures_bid + futures_ask) / 2
        return (perp_mid_ob - fut_mid_ob) / fut_mid_ob * 10_000
    return (perp_mark - futures_mid) / futures_mid * 10_000


# ──────────────────────────────────────────────
# 정상 경로 — 모든 호가 수신
# ──────────────────────────────────────────────


def test_uses_orderbook_mid_when_all_quotes_present():
    """오더북 정상 시 orderbook-mid 기반."""
    # mark가 99.40인데 orderbook은 99.18/99.18 ↔ 99.18/99.19 → mid_perp=99.18, mid_fut=99.185
    bps = _compute_basis_bps(
        perp_mark=99.40, futures_mid=99.10,
        perp_bid=99.18, perp_ask=99.18,
        futures_bid=99.18, futures_ask=99.19,
    )
    # (99.18 - 99.185) / 99.185 * 10000 = -0.5bp (orderbook-based)
    # mark-based였다면 (99.40 - 99.10) / 99.10 * 10000 = +30.3bp (phantom)
    assert -1.0 < bps < 0.0


def test_phantom_basis_eliminated_in_real_log_scenario():
    """실제 로그 16:53:38 시나리오 재현 — mark가 +20bp 보이지만 orderbook은 0bp."""
    bps = _compute_basis_bps(
        perp_mark=99.38,           # mark이 mid보다 20bp 위
        futures_mid=99.18,
        perp_bid=99.18, perp_ask=99.18,
        futures_bid=99.18, futures_ask=99.19,
    )
    # orderbook-mid: (99.18 - 99.185) / 99.185 * 10000 ≈ -0.5bp
    # 절대값 1bp 이내 — 임계값 20bp 한참 아래라 신호 안 남
    assert abs(bps) < 2.0


def test_real_basis_when_market_actually_diverges():
    """진짜 차이가 있을 때는 정상적으로 신호."""
    bps = _compute_basis_bps(
        perp_mark=99.40,
        futures_mid=99.10,
        perp_bid=99.39, perp_ask=99.40,    # perp 정상 위
        futures_bid=99.09, futures_ask=99.11,  # futures 아래
    )
    # (99.395 - 99.10) / 99.10 * 10000 ≈ 29.8bp
    assert 28.0 < bps < 32.0


# ──────────────────────────────────────────────
# Fallback 경로 — 호가 미수신
# ──────────────────────────────────────────────


def test_falls_back_to_mark_when_perp_orderbook_missing():
    """perp WS 끊기면 mark-based로 stats 유지."""
    bps = _compute_basis_bps(
        perp_mark=99.20, futures_mid=99.10,
        perp_bid=0.0, perp_ask=0.0,
        futures_bid=99.09, futures_ask=99.11,
    )
    # (99.20 - 99.10) / 99.10 * 10000 ≈ 10.1bp (mark-based)
    assert 9.5 < bps < 10.5


def test_falls_back_to_mark_when_futures_quotes_missing():
    """KIS WS 끊기면 mark vs futures_mid."""
    bps = _compute_basis_bps(
        perp_mark=99.20, futures_mid=99.10,
        perp_bid=99.18, perp_ask=99.21,
        futures_bid=0.0, futures_ask=0.0,
    )
    assert 9.5 < bps < 10.5


def test_falls_back_when_only_one_side_partial():
    """한 쪽만 일부 누락도 fallback."""
    bps = _compute_basis_bps(
        perp_mark=99.20, futures_mid=99.10,
        perp_bid=99.18, perp_ask=0.0,           # perp ask 누락
        futures_bid=99.09, futures_ask=99.11,
    )
    # mark-based fallback
    assert 9.5 < bps < 10.5


# ──────────────────────────────────────────────
# Negative basis (perp < futures)
# ──────────────────────────────────────────────


def test_orderbook_basis_negative():
    """short_basis 시나리오 — perp이 futures보다 쌈."""
    bps = _compute_basis_bps(
        perp_mark=99.00, futures_mid=99.20,
        perp_bid=98.99, perp_ask=99.01,
        futures_bid=99.19, futures_ask=99.21,
    )
    # (99.00 - 99.20) / 99.20 * 10000 ≈ -20.2bp
    assert -22.0 < bps < -18.0


# ──────────────────────────────────────────────
# Sanity: orderbook-mid vs exec_basis 차이는 half-spread 수준
# ──────────────────────────────────────────────


def test_orderbook_mid_close_to_exec_basis_for_long_basis():
    """long_basis exec = perp_bid - futures_ask. mid_basis는 그 값과 half-spread 차이."""
    perp_bid, perp_ask = 99.18, 99.20
    fut_bid, fut_ask = 99.05, 99.07
    mid_bps = _compute_basis_bps(
        perp_mark=99.40, futures_mid=99.06,    # mark은 무시됨
        perp_bid=perp_bid, perp_ask=perp_ask,
        futures_bid=fut_bid, futures_ask=fut_ask,
    )
    # mid_basis = (99.19 - 99.06) / 99.06 * 10000 ≈ 13.1bp
    # long_basis exec = (99.18 - 99.07) / 99.07 * 10000 ≈ 11.1bp
    # 차이 ≈ 2bp = (perp_spread + futures_spread) / 2
    long_exec_bps = (perp_bid - fut_ask) / fut_ask * 10_000
    assert abs(mid_bps - long_exec_bps) < 3.0   # half-spread 수준
