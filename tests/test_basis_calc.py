from __future__ import annotations
"""베이시스 계산 및 데이터 저장 테스트."""

import time
import os
import sys
import tempfile
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.exchange.hyperliquid import MarketData, OrderBook, OrderBookLevel
from src.data.storage import Storage
from src.exchange.kiwoom import KiwoomMock


# ──────────────────────────────────────────────
# MarketData Tests
# ──────────────────────────────────────────────

class TestMarketData:
    def test_basis_bps_positive(self):
        """perp > index → 양의 베이시스."""
        md = MarketData(
            ticker="WTIOIL",
            mark_price=70.50,
            index_price=70.00,
            funding_rate=0.0001,
            predicted_funding_rate=0.0001,
            open_interest=1000,
            volume_24h=5_000_000,
        )
        # (70.50 - 70.00) / 70.00 * 10000 = 71.43bp
        assert abs(md.basis_bps - 71.43) < 0.1

    def test_basis_bps_negative(self):
        """perp < index → 음의 베이시스."""
        md = MarketData(
            ticker="WTIOIL",
            mark_price=69.50,
            index_price=70.00,
            funding_rate=-0.0001,
            predicted_funding_rate=-0.0001,
            open_interest=1000,
            volume_24h=5_000_000,
        )
        assert md.basis_bps < 0

    def test_basis_bps_zero_index(self):
        """index=0 → 안전 처리."""
        md = MarketData(
            ticker="WTIOIL",
            mark_price=70.00,
            index_price=0,
            funding_rate=0,
            predicted_funding_rate=0,
            open_interest=0,
            volume_24h=0,
        )
        assert md.basis_bps == 0.0


# ──────────────────────────────────────────────
# OrderBook Tests
# ──────────────────────────────────────────────

class TestOrderBook:
    def test_spread_bps(self):
        ob = OrderBook(
            ticker="WTIOIL",
            bids=[OrderBookLevel(69.95, 10), OrderBookLevel(69.90, 20)],
            asks=[OrderBookLevel(70.05, 10), OrderBookLevel(70.10, 20)],
        )
        assert ob.best_bid == 69.95
        assert ob.best_ask == 70.05
        assert abs(ob.mid_price - 70.00) < 0.01
        # spread = 0.10 / 70.00 * 10000 ≈ 14.3bp
        assert abs(ob.spread_bps - 14.3) < 0.5

    def test_empty_orderbook(self):
        ob = OrderBook(ticker="WTIOIL", bids=[], asks=[])
        assert ob.best_bid == 0.0
        assert ob.best_ask == 0.0
        assert ob.mid_price == 0.0
        assert ob.spread_bps == 0.0


# ──────────────────────────────────────────────
# Storage Tests
# ──────────────────────────────────────────────

class TestStorage:
    def setup_method(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.storage = Storage(self.tmp.name)
        self.storage.connect()

    def teardown_method(self):
        self.storage.close()
        os.unlink(self.tmp.name)

    def test_save_and_retrieve_perp_price(self):
        self.storage.save_perp_price(
            ticker="WTIOIL",
            mark_price=70.50,
            index_price=70.00,
            funding_rate=0.0001,
        )
        rows = self.storage.get_recent_perp_prices("WTIOIL", hours=1)
        assert len(rows) == 1
        assert rows[0]["mark_price"] == 70.50
        assert rows[0]["index_price"] == 70.00

    def test_save_basis_and_stats(self):
        now = time.time()
        # 여러 데이터 포인트 저장
        for i in range(100):
            basis = 30 + (i % 20) - 10  # 20~40bp 범위
            perp = 70.00 + basis / 10000 * 70.00
            self.storage.save_basis(
                product="wti",
                perp_price=perp,
                futures_price=70.00,
                ts=now - (100 - i) * 60,  # 1분 간격
            )

        stats = self.storage.get_basis_stats("wti", hours=2)
        assert stats["count"] == 100
        assert stats["mean"] > 0
        assert stats["std"] > 0
        assert stats["min"] < stats["max"]

    def test_save_funding_cumulative(self):
        now = time.time()
        for i in range(24):
            self.storage.save_funding(
                ticker="WTIOIL",
                funding_rate=0.0001,
                ts=now - (24 - i) * 3600,
            )

        total = self.storage.get_cumulative_funding("WTIOIL", hours=25)
        assert abs(total - 0.0024) < 0.0001

    def test_save_order(self):
        row_id = self.storage.save_order(
            product="wti",
            leg="perp",
            side="sell",
            size=1.0,
            price=70.50,
            filled_price=70.48,
            filled_size=1.0,
            order_id="TEST-001",
            status="filled",
        )
        assert row_id > 0


# ──────────────────────────────────────────────
# Kiwoom Mock Tests
# ──────────────────────────────────────────────

class TestKiwoomMock:
    def setup_method(self):
        self.kiwoom = KiwoomMock()
        self.kiwoom.connect()

    def test_mock_quote(self):
        self.kiwoom.set_base_price("CL", 70.00)
        quote = self.kiwoom.get_quote("CL")
        assert quote is not None
        assert abs(quote.price - 70.00) < 1.0  # ±1% 이내

    def test_mock_order_and_position(self):
        self.kiwoom.set_base_price("CL", 70.00)
        result = self.kiwoom.place_order("CL", "buy", 2, 70.05)
        assert result.success
        assert result.filled_qty == 2

        positions = self.kiwoom.get_positions()
        assert len(positions) == 1
        assert positions[0].symbol == "CL"
        assert positions[0].side == "buy"
        assert positions[0].quantity == 2

    def test_mock_close_position(self):
        self.kiwoom.set_base_price("CL", 70.00)
        self.kiwoom.place_order("CL", "buy", 2)
        self.kiwoom.place_order("CL", "sell", 2)

        positions = self.kiwoom.get_positions()
        assert len(positions) == 0

    def test_margin_info(self):
        self.kiwoom.set_base_price("CL", 70.00)
        self.kiwoom.place_order("CL", "buy", 1)

        info = self.kiwoom.get_margin_info()
        assert info["used_margin"] == 6000
        assert info["available_margin"] == 94000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
