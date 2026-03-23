"""페이퍼 트레이딩 엔진 테스트 (unittest 기반)."""

import os
import sys
import time
import tempfile
import unittest
from pathlib import Path

# 프로젝트 루트 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.paper.engine import PaperTradingEngine, TradeRecord, EngineState
from src.strategy.signals import SignalGenerator, Signal, SignalType
from src.risk.manager import RiskManager
from src.data.storage import Storage
from src.exchange.kiwoom import KiwoomMock
from src.utils.config import (
    AppConfig, ProductConfig, HyperliquidConfig, KiwoomConfig,
    StrategyConfig, RiskConfig,
)


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def make_config() -> AppConfig:
    return AppConfig(
        mode="PAPER",
        products={
            "wti": ProductConfig(perp_ticker="xyz:CL", futures_symbol="CL", min_order_size=1),
            "brent": ProductConfig(perp_ticker="xyz:BRENTOIL", futures_symbol="BZ", min_order_size=1),
        },
        hyperliquid=HyperliquidConfig(use_testnet=False, perp_dex="xyz"),
        kiwoom=KiwoomConfig(use_mock=True),
        strategy=StrategyConfig(
            basis_window_hours=1,
            basis_std_multiplier=2.0,
            entry_threshold_bps=30,
            exit_threshold_bps=5,
            target_profit_bps=20,
            max_hold_hours=24,
        ),
        risk=RiskConfig(
            max_position_usd=50000,
            max_position_contracts=5,
            max_margin_usage_pct=80,
            max_daily_loss_usd=5000,
        ),
    )


def make_storage(tmp_dir: str) -> Storage:
    db_path = os.path.join(tmp_dir, "test.db")
    s = Storage(db_path)
    s.connect()
    return s


def make_engine(tmp_dir: str, config=None):
    config = config or make_config()
    storage = make_storage(tmp_dir)
    kiwoom = KiwoomMock()
    kiwoom.connect()
    kiwoom.set_base_price("CL", 70.0)
    kiwoom.set_base_price("BZ", 75.0)

    engine = PaperTradingEngine(
        config=config,
        storage=storage,
        kiwoom=kiwoom,
    )
    return engine, storage, kiwoom


# ──────────────────────────────────────────────
# Tests
# ──────────────────────────────────────────────

class TestPaperTradingEngine(unittest.TestCase):
    """페이퍼 트레이딩 엔진 단위 테스트."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def test_engine_init(self):
        """엔진 초기화 확인."""
        engine, storage, _ = make_engine(self.tmp_dir)
        state = engine.get_state()
        self.assertEqual(state.total_signals, 0)
        self.assertEqual(state.open_positions, 0)
        self.assertEqual(state.closed_trades, 0)
        self.assertAlmostEqual(state.cumulative_pnl_usd, 0.0)

    def test_no_signal_on_insufficient_data(self):
        """데이터 부족 시 시그널 없음."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(10):
            engine.process_basis_update("wti", 70.0, 69.95, 7.0)
        self.assertEqual(engine.get_state().total_entries, 0)

    def test_no_entry_within_range(self):
        """베이시스가 범위 내면 진입 안 함."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(50):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        self.assertEqual(engine.get_state().total_entries, 0)

    def test_entry_long_basis(self):
        """Long basis 진입: perp > futures."""
        engine, storage, _ = make_engine(self.tmp_dir)
        # 데이터 축적 (basis ~5bp)
        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        # 베이시스 급등 (100bp)
        engine.process_basis_update("wti", 70.70, 70.0, 100.0, 0.0001)

        state = engine.get_state()
        self.assertEqual(state.total_entries, 1)
        self.assertEqual(state.open_positions, 1)
        self.assertIn("wti", engine.get_open_trades())

        trade = engine.get_open_trades()["wti"]
        self.assertEqual(trade.direction, "long_basis")
        self.assertEqual(trade.perp_side, "short")
        self.assertEqual(trade.futures_side, "long")

    def test_entry_short_basis(self):
        """Short basis 진입: perp < futures."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(30):
            engine.process_basis_update("wti", 69.965, 70.0, -5.0)
        engine.process_basis_update("wti", 69.30, 70.0, -100.0, -0.0001)

        state = engine.get_state()
        self.assertEqual(state.total_entries, 1)
        trade = engine.get_open_trades()["wti"]
        self.assertEqual(trade.direction, "short_basis")
        self.assertEqual(trade.perp_side, "long")
        self.assertEqual(trade.futures_side, "short")

    def test_no_duplicate_entry(self):
        """포지션 있으면 중복 진입 불가."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        engine.process_basis_update("wti", 70.70, 70.0, 100.0)
        self.assertEqual(engine.get_state().total_entries, 1)
        engine.process_basis_update("wti", 70.80, 70.0, 114.0)
        self.assertEqual(engine.get_state().total_entries, 1)

    def test_exit_on_profit(self):
        """수렴 시 청산 (target profit 또는 mean reversion)."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        # 진입 @ 100bp
        engine.process_basis_update("wti", 70.70, 70.0, 100.0)
        self.assertEqual(engine.get_state().open_positions, 1)
        # 수렴 @ 5bp
        engine.process_basis_update("wti", 70.035, 70.0, 5.0)

        state = engine.get_state()
        self.assertEqual(state.total_exits, 1)
        self.assertEqual(state.open_positions, 0)
        self.assertEqual(state.closed_trades, 1)

    def test_risk_rejection(self):
        """리스크 체크에 의한 진입 거부."""
        config = make_config()
        config.risk.max_daily_loss_usd = 0
        engine, _, _ = make_engine(self.tmp_dir, config)
        engine.risk_mgr.record_pnl(-1.0)

        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        engine.process_basis_update("wti", 70.70, 70.0, 100.0)

        state = engine.get_state()
        self.assertEqual(state.rejected_by_risk, 1)
        self.assertEqual(state.total_entries, 0)

    def test_closed_trade_pnl(self):
        """청산 후 PnL 기록."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        engine.process_basis_update("wti", 70.70, 70.0, 100.0)
        engine.process_basis_update("wti", 70.035, 70.0, 5.0)

        trades = engine.get_closed_trades()
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0].status, "closed")
        self.assertNotEqual(trades[0].exit_reason, "")

    def test_multiple_products(self):
        """WTI + Brent 동시 거래."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
            engine.process_basis_update("brent", 75.035, 75.0, 4.7)

        engine.process_basis_update("wti", 70.70, 70.0, 100.0)
        engine.process_basis_update("brent", 75.75, 75.0, 100.0)

        state = engine.get_state()
        self.assertEqual(state.open_positions, 2)
        self.assertIn("wti", engine.get_open_trades())
        self.assertIn("brent", engine.get_open_trades())

    def test_trade_callbacks(self):
        """트레이드 콜백 호출."""
        engine, _, _ = make_engine(self.tmp_dir)
        events = []
        engine.on_trade(lambda trade, event: events.append((trade.product, event)))

        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)

        engine.process_basis_update("wti", 70.70, 70.0, 100.0)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0], ("wti", "open"))

        engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        self.assertEqual(len(events), 2)
        self.assertEqual(events[1], ("wti", "close"))

    def test_funding_accumulation(self):
        """펀딩레이트 누적."""
        engine, _, _ = make_engine(self.tmp_dir)
        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        engine.process_basis_update("wti", 70.70, 70.0, 100.0, 0.0001)

        # 펀딩 정산 (perp short + funding > 0 = 수취)
        engine.process_funding_update("wti", 0.0001)
        engine.process_funding_update("wti", 0.0001)

        trade = engine.get_open_trades()["wti"]
        self.assertGreater(trade.funding_pnl_bps, 0)

    def test_summary_string(self):
        """get_summary() 호출."""
        engine, _, _ = make_engine(self.tmp_dir)
        summary = engine.get_summary()
        self.assertIn("Paper Trading Summary", summary)
        self.assertIn("Signals: 0", summary)

    def test_db_persistence(self):
        """DB에 주문/포지션 저장."""
        engine, storage, _ = make_engine(self.tmp_dir)
        for i in range(30):
            engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        engine.process_basis_update("wti", 70.70, 70.0, 100.0)

        positions = storage.get_open_positions()
        self.assertGreaterEqual(len(positions), 1)
        self.assertEqual(positions[0]["product"], "wti")

        orders = storage.get_trade_history(limit=10)
        self.assertGreaterEqual(len(orders), 2)

        # 청산
        engine.process_basis_update("wti", 70.035, 70.0, 5.0)
        open_pos = storage.get_open_positions()
        self.assertEqual(len(open_pos), 0)


class TestStorageExtensions(unittest.TestCase):
    """Storage 확장 메서드 테스트."""

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()

    def test_save_and_close_position(self):
        storage = make_storage(self.tmp_dir)
        storage.save_position("wti", -1.0, 70.5, 1.0, 70.0)

        open_pos = storage.get_open_positions()
        self.assertEqual(len(open_pos), 1)
        self.assertEqual(open_pos[0]["product"], "wti")

        storage.close_position("wti", realized_pnl=15.5, funding_pnl=2.0)
        self.assertEqual(len(storage.get_open_positions()), 0)
        storage.close()

    def test_daily_pnl_upsert(self):
        storage = make_storage(self.tmp_dir)
        storage.update_daily_pnl("wti", trading_pnl=10.0, funding_pnl=2.0, fees=1.0)
        storage.update_daily_pnl("wti", trading_pnl=5.0, funding_pnl=1.0, fees=0.5)

        daily = storage.get_daily_pnl_summary(days=1)
        self.assertEqual(len(daily), 1)
        self.assertAlmostEqual(daily[0]["trading_pnl"], 15.0)
        self.assertEqual(daily[0]["num_trades"], 2)
        storage.close()

    def test_trade_history(self):
        storage = make_storage(self.tmp_dir)
        storage.save_order("wti", "perp", "sell", 1.0, price=70.5,
                          filled_price=70.5, filled_size=1.0, status="filled")
        storage.save_order("wti", "futures", "buy", 1.0, price=70.0,
                          filled_price=70.0, filled_size=1.0, status="filled")

        history = storage.get_trade_history(limit=10)
        self.assertEqual(len(history), 2)
        storage.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
