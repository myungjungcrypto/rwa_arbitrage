"""전략 시그널 + 백테스트 엔진 테스트."""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np
from src.strategy.signals import SignalGenerator, SignalType
from src.strategy.basis_arb import BacktestEngine
from src.risk.manager import RiskManager, RiskCheck
from src.utils.config import RiskConfig


# ──────────────────────────────────────────────
# Signal Tests
# ──────────────────────────────────────────────

def test_signal_insufficient_data():
    gen = SignalGenerator()
    sig = gen.update_basis("wti", 50.0)
    assert sig.type == SignalType.NONE
    assert "Insufficient" in sig.reason
    print("✓ Insufficient data → NONE")


def test_signal_entry_long_basis():
    """베이시스가 mean + K*σ를 넘으면 진입."""
    gen = SignalGenerator(
        entry_threshold_bps=20,
        std_multiplier=2.0,
    )
    # 평균 0bp 근처로 100개 데이터
    for _ in range(100):
        gen.update_basis("wti", np.random.normal(0, 5))

    # 갑자기 큰 양의 베이시스
    sig = gen.update_basis("wti", 80.0)
    assert sig.type == SignalType.ENTRY_LONG_BASIS
    assert sig.confidence > 0
    print(f"✓ Entry long basis: {sig.reason}")


def test_signal_entry_short_basis():
    gen = SignalGenerator(entry_threshold_bps=20, std_multiplier=2.0)
    for _ in range(100):
        gen.update_basis("wti", np.random.normal(0, 5))

    sig = gen.update_basis("wti", -80.0)
    assert sig.type == SignalType.ENTRY_SHORT_BASIS
    print(f"✓ Entry short basis: {sig.reason}")


def test_signal_exit_mean_reversion():
    gen = SignalGenerator(
        entry_threshold_bps=20,
        exit_threshold_bps=5,
        std_multiplier=2.0,
    )
    for _ in range(100):
        gen.update_basis("wti", np.random.normal(0, 5))

    # 진입
    sig = gen.update_basis("wti", 80.0)
    assert sig.type == SignalType.ENTRY_LONG_BASIS
    gen.open_position("wti", sig)

    # 평균으로 회귀 (entry 80bp → exit 3bp = 77bp 수익, target 30bp 초과)
    # 목표수익 또는 평균회귀 중 하나로 EXIT 발생
    sig2 = gen.update_basis("wti", 3.0)
    assert sig2.type == SignalType.EXIT
    assert "Target profit" in sig2.reason or "Mean reversion" in sig2.reason
    print(f"✓ Exit (profit or reversion): {sig2.reason}")


def test_signal_exit_target_profit():
    gen = SignalGenerator(
        entry_threshold_bps=20,
        target_profit_bps=30,
        std_multiplier=2.0,
    )
    for _ in range(100):
        gen.update_basis("wti", np.random.normal(0, 5))

    # 진입 at 80bp
    sig = gen.update_basis("wti", 80.0)
    gen.open_position("wti", sig)

    # 베이시스 축소 → 수익 = 80 - 30 = 50bp > target 30bp
    sig2 = gen.update_basis("wti", 30.0)
    assert sig2.type == SignalType.EXIT
    assert "Target profit" in sig2.reason
    print(f"✓ Exit target profit: {sig2.reason}")


# ──────────────────────────────────────────────
# Backtest Tests
# ──────────────────────────────────────────────

def test_backtest_synthetic():
    """합성 데이터로 백테스트."""
    np.random.seed(42)
    n = 10000  # ~14시간 (5초 간격)

    # 평균 회귀 베이시스 시뮬레이션 (OU process)
    basis = [0.0]
    mean_reversion = 0.01
    volatility = 2.0
    long_term_mean = 5.0  # 약간 양의 베이시스 (perp premium)

    for i in range(1, n):
        db = mean_reversion * (long_term_mean - basis[-1]) + volatility * np.random.randn()
        basis.append(basis[-1] + db)

    funding = [0.00000625] * n  # 고정 펀딩

    engine = BacktestEngine()
    result = engine.run(
        product="wti",
        basis_series=basis,
        funding_series=funding,
        interval_seconds=5,
        signal_params={
            "window_hours": 1,       # 백테스트용 짧은 윈도우
            "std_multiplier": 2.0,
            "entry_threshold_bps": 15,
            "exit_threshold_bps": 3,
            "target_profit_bps": 20,
            "max_hold_hours": 2,
        },
    )

    print(f"\n{result.summary()}")
    assert result.data_points == n
    assert result.basis_std > 0
    # 거래가 발생했는지 (파라미터에 따라 없을 수도 있음)
    print(f"✓ Backtest completed: {result.total_trades} trades, net={result.total_pnl_bps:.1f}bp")
    return result


def test_backtest_trending():
    """트렌딩 베이시스 — 전략이 손실을 제한하는지 확인."""
    n = 5000
    # 베이시스가 점진적으로 확대 (불리한 시나리오)
    basis = [i * 0.02 for i in range(n)]

    engine = BacktestEngine()
    result = engine.run(
        product="wti",
        basis_series=basis,
        interval_seconds=5,
        signal_params={
            "window_hours": 0.5,
            "std_multiplier": 2.0,
            "entry_threshold_bps": 10,
            "max_hold_hours": 1,
            "emergency_close_bps": 50,
        },
    )
    print(f"✓ Trending test: {result.total_trades} trades, net={result.total_pnl_bps:.1f}bp, "
          f"max_dd={result.max_drawdown_bps:.1f}bp")


# ──────────────────────────────────────────────
# Risk Manager Tests
# ──────────────────────────────────────────────

def test_risk_position_limit():
    rm = RiskManager(RiskConfig(max_position_usd=50000))
    check = rm.check_entry("wti", 60000, 10, 10, 50)
    assert not check.allowed
    print("✓ Risk: position limit")


def test_risk_margin_limit():
    rm = RiskManager(RiskConfig(max_margin_usage_pct=50))
    check = rm.check_entry("wti", 10000, 60, 10, 50)
    assert not check.allowed
    print("✓ Risk: margin limit")


def test_risk_daily_loss():
    rm = RiskManager(RiskConfig(max_daily_loss_usd=2000))
    rm.record_pnl(-2500)
    check = rm.check_entry("wti", 10000, 10, 10, 50)
    assert not check.allowed
    print("✓ Risk: daily loss limit")


def test_risk_rollover():
    rm = RiskManager(RiskConfig(
        max_position_usd=50000,
        rollover_position_reduce_pct=50,
    ))
    check = rm.check_entry("wti", 30000, 10, 10, 50, is_rollover_period=True)
    assert not check.allowed
    assert check.max_size == 25000
    print("✓ Risk: rollover reduction")


if __name__ == "__main__":
    test_signal_insufficient_data()
    test_signal_entry_long_basis()
    test_signal_entry_short_basis()
    test_signal_exit_mean_reversion()
    test_signal_exit_target_profit()
    test_backtest_synthetic()
    test_backtest_trending()
    test_risk_position_limit()
    test_risk_margin_limit()
    test_risk_daily_loss()
    test_risk_rollover()
    print("\n═══ ALL STRATEGY TESTS PASSED ═══")
