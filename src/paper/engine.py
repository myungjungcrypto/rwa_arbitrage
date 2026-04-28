from __future__ import annotations
"""페이퍼 트레이딩 엔진.

시그널 생성기 + 리스크 매니저를 실시간 데이터 수집기에 연결하여
자동으로 진입/청산 시뮬레이션을 수행.

핵심 플로우:
1. DataCollector에서 베이시스 업데이트 수신
2. SignalGenerator로 시그널 생성
3. RiskManager로 리스크 검증
4. 양 레그(perp + futures) 동시 주문 시뮬레이션
5. 포지션 + PnL 추적 (DB + in-memory)
"""


import time
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional, Callable

from src.strategy.signals import SignalGenerator, Signal, SignalType, PositionState
from src.risk.manager import RiskManager, RiskCheck
from src.data.storage import Storage
from src.exchange.kiwoom import KiwoomBase, FuturesOrder
from src.utils.config import AppConfig, StrategyConfig, RiskConfig

logger = logging.getLogger("arbitrage.paper")


# ──────────────────────────────────────────────
# Data Models
# ──────────────────────────────────────────────

@dataclass
class TradeRecord:
    """개별 트레이드 기록."""
    trade_id: int = 0
    product: str = ""
    direction: str = ""          # "long_basis" or "short_basis"

    # 진입
    entry_time: float = 0.0
    entry_basis_bps: float = 0.0
    perp_entry_price: float = 0.0
    futures_entry_price: float = 0.0
    perp_side: str = ""          # "long" or "short"
    futures_side: str = ""       # "long" or "short"
    size_contracts: int = 1        # CME 계약 수
    perp_units: int = 1             # trade.xyz 퍼프 단위 (= 배럴 수)

    # 청산
    exit_time: float = 0.0
    exit_basis_bps: float = 0.0
    perp_exit_price: float = 0.0
    futures_exit_price: float = 0.0
    exit_reason: str = ""

    # PnL
    basis_pnl_bps: float = 0.0
    funding_pnl_bps: float = 0.0
    perp_fees_usd: float = 0.0
    futures_fees_usd: float = 0.0
    net_pnl_usd: float = 0.0

    # 상태
    status: str = "open"         # "open" / "closed"


@dataclass
class EngineState:
    """엔진 실행 상태 요약."""
    total_signals: int = 0
    total_entries: int = 0
    total_exits: int = 0
    rejected_by_risk: int = 0
    failed_orders: int = 0
    open_positions: int = 0
    closed_trades: int = 0
    cumulative_pnl_usd: float = 0.0
    # 엔트리 near-miss 진단 카운터 (mid signal 있었지만 진입 안 됨)
    entry_signals_generated: int = 0     # signal generator가 ENTRY type 반환
    entry_exec_filter_skip: int = 0      # exec basis < threshold로 skip
    entry_warmup_skip: int = 0            # 워밍업 부족으로 skip
    entry_min_abs_skip: int = 0           # min_abs_entry_bps floor 미달로 skip


# ──────────────────────────────────────────────
# Paper Trading Engine
# ──────────────────────────────────────────────

class PaperTradingEngine:
    """페이퍼 트레이딩 엔진.

    DataCollector의 콜백으로 등록되어,
    실시간 베이시스 업데이트를 받을 때마다 시그널을 체크하고
    조건 충족 시 양 레그 주문을 시뮬레이션.
    """

    # Perp 수수료 (basis points of notional)
    PERP_TAKER_FEE_BPS = 0.9       # trade.xyz HIP-3 taker (0.009%)
    # Futures 수수료: config의 futures_fee_per_contract 사용 (고정 $/계약)

    def __init__(
        self,
        config: AppConfig,
        storage: Storage,
        kiwoom: KiwoomBase,
        signal_gen: SignalGenerator | None = None,
        risk_mgr: RiskManager | None = None,
    ):
        self.config = config
        self.storage = storage
        self.kiwoom = kiwoom

        # 전략 컴포넌트
        self.signal_gen = signal_gen or SignalGenerator(
            window_hours=config.strategy.basis_window_hours,
            std_multiplier=config.strategy.basis_std_multiplier,
            entry_threshold_bps=config.strategy.entry_threshold_bps,
            max_hold_hours=config.strategy.max_hold_hours,
            funding_rate_weight=config.strategy.funding_rate_weight,
            min_funding_advantage_bps=config.strategy.min_funding_advantage_bps,
            convergence_target_bps=config.strategy.convergence_target_bps,
            cme_closed_skip_entry=config.strategy.cme_closed_skip_entry,
            pre_close_flatten_minutes=config.strategy.pre_close_flatten_minutes,
            flatten_threshold_hours=config.strategy.flatten_threshold_hours,
        )
        self.risk_mgr = risk_mgr or RiskManager(config.risk)

        # 상태 추적
        self._open_trades: dict[str, TradeRecord] = {}  # product -> open trade
        self._closed_trades: list[TradeRecord] = []
        self._trade_counter = 0
        self._state = EngineState()

        # 최신 가격 캐시
        self._latest_perp_prices: dict[str, float] = {}   # product -> mark_price
        self._latest_index_prices: dict[str, float] = {}   # product -> index_price
        self._latest_futures_prices: dict[str, float] = {}  # product -> futures_price (mid)
        self._latest_perp_bid: dict[str, float] = {}       # product -> best bid
        self._latest_perp_ask: dict[str, float] = {}       # product -> best ask
        self._latest_futures_bid: dict[str, float] = {}    # product -> futures best bid
        self._latest_futures_ask: dict[str, float] = {}    # product -> futures best ask

        # 최소 워밍업 데이터 수 (이 이하면 거래 안 함)
        self.MIN_WARMUP_POINTS = 3600  # 약 1시간 분량

        # 이벤트 콜백
        self._on_trade_callbacks: list[Callable] = []
        self._on_signal_callbacks: list[Callable] = []

    # ── 콜백 등록 ──

    def on_trade(self, callback: Callable[[TradeRecord, str], None]):
        """트레이드 이벤트 콜백 등록.

        Args:
            callback(trade: TradeRecord, event: str):
                event = "open" | "close"
        """
        self._on_trade_callbacks.append(callback)

    def on_signal(self, callback: Callable[[Signal], None]):
        """시그널 이벤트 콜백 (NONE 제외)."""
        self._on_signal_callbacks.append(callback)

    # ── 메인 처리 루프 ──

    def _compute_executable_basis(
        self,
        product: str,
        direction: str,
    ) -> float:
        """실제 체결 가능 가격 기반 executable basis 계산.

        진입 시: 매수 측은 ask, 매도 측은 bid 사용
        - short_basis 진입: perp BUY(ask) + futures SELL(bid)
          → exec_basis = (perp_ask - futures_bid) / futures_bid * 10000
        - long_basis 진입: perp SELL(bid) + futures BUY(ask)
          → exec_basis = (perp_bid - futures_ask) / futures_ask * 10000
        """
        perp_bid = self._latest_perp_bid.get(product, 0)
        perp_ask = self._latest_perp_ask.get(product, 0)
        futures_bid = self._latest_futures_bid.get(product, 0)
        futures_ask = self._latest_futures_ask.get(product, 0)

        if not all([perp_bid, perp_ask, futures_bid, futures_ask]):
            return 0.0

        if direction == "short_basis":
            # perp LONG(ask) + futures SHORT(bid)
            return (perp_ask - futures_bid) / futures_bid * 10_000
        else:  # long_basis
            # perp SHORT(bid) + futures LONG(ask)
            return (perp_bid - futures_ask) / futures_ask * 10_000

    def process_basis_update(
        self,
        product: str,
        perp_price: float,
        futures_price: float,
        basis_bps: float,
        funding_rate: float = 0.0,
        perp_best_bid: float = 0.0,
        perp_best_ask: float = 0.0,
        futures_bid: float = 0.0,
        futures_ask: float = 0.0,
    ):
        """베이시스 업데이트 처리 — DataCollector 콜백으로 사용.

        Args:
            product: 상품명 (wti / brent)
            perp_price: 퍼프 mark price
            futures_price: 선물 mid 가격
            basis_bps: 베이시스 (bp) — mid 기준, 통계용
            funding_rate: 현재 펀딩레이트
            perp_best_bid: 퍼프 오더북 최우선 매수호가
            perp_best_ask: 퍼프 오더북 최우선 매도호가
            futures_bid: 선물 매수 최우선호가
            futures_ask: 선물 매도 최우선호가
        """
        # 가격 캐시 업데이트
        self._latest_perp_prices[product] = perp_price
        self._latest_futures_prices[product] = futures_price
        self._latest_perp_bid[product] = perp_best_bid or perp_price
        self._latest_perp_ask[product] = perp_best_ask or perp_price
        self._latest_futures_bid[product] = futures_bid or futures_price
        self._latest_futures_ask[product] = futures_ask or futures_price

        # 시그널 생성 (mid basis + bid/ask 전달)
        signal = self.signal_gen.update_basis(
            product, basis_bps, funding_rate,
            perp_bid=self._latest_perp_bid.get(product, 0),
            perp_ask=self._latest_perp_ask.get(product, 0),
            futures_bid=self._latest_futures_bid.get(product, 0),
            futures_ask=self._latest_futures_ask.get(product, 0),
        )
        self._state.total_signals += 1

        if signal.type == SignalType.NONE:
            return

        # 시그널 콜백
        for cb in self._on_signal_callbacks:
            try:
                cb(signal)
            except Exception as e:
                logger.error(f"Signal callback error: {e}")

        # 진입 시그널 — executable basis 검증 후 실행
        if signal.type in (SignalType.ENTRY_LONG_BASIS, SignalType.ENTRY_SHORT_BASIS):
            direction = "long_basis" if signal.type == SignalType.ENTRY_LONG_BASIS else "short_basis"
            self._state.entry_signals_generated += 1
            logger.warning(
                f"[{product.upper()}] ENTRY_SIGNAL {direction} mid_basis={signal.basis_bps:+.1f}bp | "
                f"{signal.reason}"
            )

            # 워밍업 체크
            history = self.signal_gen._basis_history.get(product)
            if history and len(history) < self.MIN_WARMUP_POINTS:
                self._state.entry_warmup_skip += 1
                logger.warning(f"[{product.upper()}] ENTRY_SKIP warmup: {len(history)}/{self.MIN_WARMUP_POINTS}")
                return  # 데이터 부족, 거래 안 함

            # Executable basis 계산
            exec_basis = self._compute_executable_basis(product, direction)

            # executable basis가 entry threshold를 넘지 않으면 무시
            if direction == "short_basis" and exec_basis > -self.config.strategy.entry_threshold_bps:
                self._state.entry_exec_filter_skip += 1
                logger.warning(
                    f"[{product.upper()}] ENTRY_SKIP exec_filter: exec={exec_basis:.1f}bp > "
                    f"-{self.config.strategy.entry_threshold_bps}bp "
                    f"(perp_bid={self._latest_perp_bid.get(product, 0):.2f} "
                    f"ask={self._latest_perp_ask.get(product, 0):.2f} "
                    f"fut_bid={self._latest_futures_bid.get(product, 0):.2f} "
                    f"ask={self._latest_futures_ask.get(product, 0):.2f})"
                )
                return
            if direction == "long_basis" and exec_basis < self.config.strategy.entry_threshold_bps:
                self._state.entry_exec_filter_skip += 1
                logger.warning(
                    f"[{product.upper()}] ENTRY_SKIP exec_filter: exec={exec_basis:.1f}bp < "
                    f"+{self.config.strategy.entry_threshold_bps}bp "
                    f"(perp_bid={self._latest_perp_bid.get(product, 0):.2f} "
                    f"ask={self._latest_perp_ask.get(product, 0):.2f} "
                    f"fut_bid={self._latest_futures_bid.get(product, 0):.2f} "
                    f"ask={self._latest_futures_ask.get(product, 0):.2f})"
                )
                return

            # 절대값 진입 floor — historical analysis(<10bp는 14% WR -$202,
            # 10bp+는 94% WR +$199)에 따라 통계 신호와 무관한 추가 가드
            min_abs = self.config.strategy.min_abs_entry_bps
            if min_abs > 0 and abs(exec_basis) < min_abs:
                self._state.entry_min_abs_skip += 1
                logger.warning(
                    f"[{product.upper()}] ENTRY_SKIP min_abs: |exec|={abs(exec_basis):.1f}bp "
                    f"< min_abs={min_abs:.1f}bp (mid={basis_bps:+.1f}bp dir={direction})"
                )
                return

            logger.warning(
                f"[{product.upper()}] ENTRY_EXEC_OK: mid={basis_bps:+.1f}bp exec={exec_basis:+.1f}bp"
            )
            self._handle_entry(product, signal, perp_price, futures_price)

        # 청산 시그널
        elif signal.type in (SignalType.EXIT, SignalType.EMERGENCY_CLOSE):
            self._handle_exit(product, signal, perp_price, futures_price)

    def process_funding_update(self, product: str, funding_rate: float):
        """펀딩레이트 정산 처리.

        오픈 포지션이 있을 때 펀딩 누적.
        """
        self.signal_gen.add_funding(product, funding_rate)

        trade = self._open_trades.get(product)
        if trade:
            # 펀딩 PnL 계산 (bps)
            if trade.perp_side == "short" and funding_rate > 0:
                trade.funding_pnl_bps += funding_rate * 10000
            elif trade.perp_side == "long" and funding_rate < 0:
                trade.funding_pnl_bps += abs(funding_rate) * 10000
            else:
                trade.funding_pnl_bps -= abs(funding_rate) * 10000

            logger.debug(
                f"[{product.upper()}] Funding: rate={funding_rate:.6f}, "
                f"cumulative={trade.funding_pnl_bps:.2f}bp"
            )

    # ── 진입 처리 ──

    def _handle_entry(
        self,
        product: str,
        signal: Signal,
        perp_price: float,
        futures_price: float,
    ):
        """진입 시그널 처리."""
        # 이미 포지션이 있으면 무시
        if product in self._open_trades:
            logger.debug(f"[{product.upper()}] Already has open position, ignoring entry signal")
            return

        # 리스크 체크
        size_usd = self._calculate_position_size_usd(product, futures_price)
        perp_margin_pct = self._get_perp_margin_usage()
        futures_margin = self.kiwoom.get_margin_info()
        futures_margin_pct = futures_margin.get("usage_pct", 0)
        is_rollover = self.risk_mgr.is_rollover_period()

        risk_check = self.risk_mgr.check_entry(
            product=product,
            size_usd=size_usd,
            perp_margin_usage_pct=perp_margin_pct,
            futures_margin_usage_pct=futures_margin_pct,
            current_basis_bps=signal.basis_bps,
            is_rollover_period=is_rollover,
        )

        if not risk_check.allowed:
            self._state.rejected_by_risk += 1
            logger.warning(
                f"[{product.upper()}] Entry REJECTED by risk: {risk_check.reason}"
            )
            return

        # 주문 사이즈 결정
        product_config = self.config.products[product]
        contracts = self._calculate_contracts(product, risk_check.max_size, futures_price)
        if contracts < 1:
            logger.warning(f"[{product.upper()}] Calculated contracts < 1, skipping")
            return
        perp_units = contracts * product_config.contract_size  # 배럴 수

        # 방향 결정
        if signal.type == SignalType.ENTRY_LONG_BASIS:
            perp_side = "sell"   # perp SHORT
            futures_side = "buy"  # futures LONG
        else:
            perp_side = "buy"    # perp LONG
            futures_side = "sell" # futures SHORT

        # ── 양 레그 동시 주문 ──

        # 1) Futures 주문 (Mock)
        futures_symbol = self.config.products[product].futures_symbol
        futures_order = self.kiwoom.place_order(
            symbol=futures_symbol,
            side=futures_side,
            quantity=contracts,
        )

        if not futures_order.success:
            self._state.failed_orders += 1
            logger.error(
                f"[{product.upper()}] Futures order FAILED: {futures_order.error}"
            )
            return

        # 2) Perp 주문 (시뮬레이션 — 오더북 bid/ask로 체결)
        #    buy → ask 가격, sell → bid 가격으로 체결
        #    size = perp_units (배럴 수, not CME 계약 수)
        if perp_side == "buy":
            perp_fill_price = self._latest_perp_ask.get(product, perp_price)
        else:
            perp_fill_price = self._latest_perp_bid.get(product, perp_price)

        # ── 트레이드 기록 ──
        self._trade_counter += 1
        trade = TradeRecord(
            trade_id=self._trade_counter,
            product=product,
            direction="long_basis" if signal.type == SignalType.ENTRY_LONG_BASIS else "short_basis",
            entry_time=time.time(),
            entry_basis_bps=signal.basis_bps,
            perp_entry_price=perp_fill_price,
            futures_entry_price=futures_order.filled_price,
            perp_side="short" if signal.type == SignalType.ENTRY_LONG_BASIS else "long",
            futures_side="long" if signal.type == SignalType.ENTRY_LONG_BASIS else "short",
            size_contracts=contracts,
            perp_units=perp_units,
            status="open",
        )
        self._open_trades[product] = trade
        self._state.total_entries += 1
        self._state.open_positions += 1

        # 시그널 생성기에 포지션 기록 + executable basis 저장
        self.signal_gen.open_position(product, signal, size=contracts)
        pos = self.signal_gen.get_position(product)
        pos.entry_exec_basis_bps = self._compute_executable_basis(product, trade.direction)

        # DB 저장 — 주문
        self.storage.save_order(
            product=product, leg="perp",
            side=perp_side, size=perp_units,
            price=perp_fill_price, filled_price=perp_fill_price,
            filled_size=perp_units, status="filled", is_paper=True,
        )
        self.storage.save_order(
            product=product, leg="futures",
            side=futures_side, size=contracts,
            price=futures_order.filled_price,
            filled_price=futures_order.filled_price,
            filled_size=futures_order.filled_qty,
            order_id=futures_order.order_no,
            status="filled", is_paper=True,
        )

        # DB 저장 — 포지션
        self.storage.save_position(
            product=product,
            perp_size=perp_units if trade.perp_side == "long" else -perp_units,
            perp_entry=perp_fill_price,
            futures_size=contracts if trade.futures_side == "long" else -contracts,
            futures_entry=futures_order.filled_price,
        )

        logger.info(
            f"[{product.upper()}] ▶ ENTRY {trade.direction} | "
            f"basis={signal.basis_bps:+.1f}bp | "
            f"perp {trade.perp_side} {perp_units}units @ {perp_fill_price:.2f} | "
            f"futures {trade.futures_side} {contracts}x{product_config.contract_size}bbl @ {futures_order.filled_price:.2f} | "
            f"confidence={signal.confidence:.2f} | {signal.reason}"
        )

        # 콜백
        for cb in self._on_trade_callbacks:
            try:
                cb(trade, "open")
            except Exception as e:
                logger.error(f"Trade callback error: {e}")

    # ── 청산 처리 ──

    def _handle_exit(
        self,
        product: str,
        signal: Signal,
        perp_price: float,
        futures_price: float,
    ):
        """청산 시그널 처리."""
        trade = self._open_trades.get(product)
        if not trade:
            logger.warning(f"[{product.upper()}] Exit signal but no open position")
            return

        contracts = trade.size_contracts

        # ── 양 레그 청산 주문 ──

        # Perp 청산 (반대 방향) — 오더북 bid/ask로 체결
        perp_close_side = "buy" if trade.perp_side == "short" else "sell"
        if perp_close_side == "buy":
            perp_fill_price = self._latest_perp_ask.get(product, perp_price)
        else:
            perp_fill_price = self._latest_perp_bid.get(product, perp_price)

        # Futures 청산
        futures_close_side = "sell" if trade.futures_side == "long" else "buy"
        futures_symbol = self.config.products[product].futures_symbol
        futures_order = self.kiwoom.place_order(
            symbol=futures_symbol,
            side=futures_close_side,
            quantity=contracts,
        )

        if not futures_order.success:
            self._state.failed_orders += 1
            logger.error(
                f"[{product.upper()}] Futures close order FAILED: {futures_order.error}. "
                "Emergency: will retry on next update."
            )
            return

        # ── PnL 계산 ──
        pnl = self._calculate_pnl(trade, perp_fill_price, futures_order.filled_price)

        trade.exit_time = time.time()
        trade.exit_basis_bps = signal.basis_bps
        trade.perp_exit_price = perp_fill_price
        trade.futures_exit_price = futures_order.filled_price
        trade.exit_reason = signal.reason
        trade.basis_pnl_bps = pnl["basis_pnl_bps"]
        trade.perp_fees_usd = pnl["perp_fees_usd"]
        trade.futures_fees_usd = pnl["futures_fees_usd"]
        trade.net_pnl_usd = pnl["net_pnl_usd"]
        trade.status = "closed"

        # 상태 업데이트
        self._state.total_exits += 1
        self._state.open_positions -= 1
        self._state.closed_trades += 1
        self._state.cumulative_pnl_usd += pnl["net_pnl_usd"]

        # 리스크 매니저에 PnL 기록
        self.risk_mgr.record_pnl(pnl["net_pnl_usd"])

        # 시그널 생성기 포지션 리셋
        self.signal_gen.close_position(product)

        # 이동
        del self._open_trades[product]
        self._closed_trades.append(trade)

        # DB 저장 — 주문
        self.storage.save_order(
            product=product, leg="perp",
            side=perp_close_side, size=contracts,
            price=perp_fill_price, filled_price=perp_fill_price,
            filled_size=contracts, status="filled", is_paper=True,
        )
        self.storage.save_order(
            product=product, leg="futures",
            side=futures_close_side, size=contracts,
            price=futures_order.filled_price,
            filled_price=futures_order.filled_price,
            filled_size=futures_order.filled_qty,
            order_id=futures_order.order_no,
            status="filled", is_paper=True,
        )

        # DB — 포지션 클로즈
        self.storage.close_position(
            product=product,
            realized_pnl=pnl["net_pnl_usd"],
            funding_pnl=pnl["funding_pnl_usd"],
        )

        # DB — 일일 PnL
        self.storage.update_daily_pnl(
            product=product,
            trading_pnl=pnl["trading_pnl_usd"],
            funding_pnl=pnl["funding_pnl_usd"],
            fees=pnl["total_fees_usd"],
        )

        hold_hours = (trade.exit_time - trade.entry_time) / 3600
        emoji = "✅" if pnl["net_pnl_usd"] >= 0 else "❌"

        logger.info(
            f"[{product.upper()}] {emoji} EXIT {trade.direction} | "
            f"basis: {trade.entry_basis_bps:+.1f} → {signal.basis_bps:+.1f}bp | "
            f"perp {trade.perp_entry_price:.2f}→{perp_fill_price:.2f} | "
            f"futures {trade.futures_entry_price:.2f}→{futures_order.filled_price:.2f} | "
            f"pnl=${pnl['net_pnl_usd']:+.2f} (basis=${pnl['trading_pnl_usd']:+.2f} "
            f"funding=${pnl['funding_pnl_usd']:+.2f} fees=-${pnl['total_fees_usd']:.2f}) | "
            f"hold={hold_hours:.1f}h | {signal.reason}"
        )

        # 콜백
        for cb in self._on_trade_callbacks:
            try:
                cb(trade, "close")
            except Exception as e:
                logger.error(f"Trade callback error: {e}")

    # ── PnL 계산 ──

    def _calculate_pnl(
        self,
        trade: TradeRecord,
        perp_exit_price: float,
        futures_exit_price: float,
    ) -> dict:
        """트레이드 PnL 계산.

        모든 PnL은 배럴(perp_units) 기준으로 계산.
        perp_units = CME계약수 × contract_size (예: 1 MCL = 100배럴)

        Returns:
            dict with: basis_pnl_bps, trading_pnl_usd, funding_pnl_usd,
                        perp_fees_usd, futures_fees_usd, total_fees_usd, net_pnl_usd
        """
        barrels = trade.perp_units  # 배럴 수 (양쪽 동일)

        # Perp PnL (USD) — 배럴 × 가격차
        if trade.perp_side == "short":
            perp_pnl = (trade.perp_entry_price - perp_exit_price) * barrels
        else:
            perp_pnl = (perp_exit_price - trade.perp_entry_price) * barrels

        # Futures PnL (USD) — 배럴 × 가격차
        if trade.futures_side == "long":
            futures_pnl = (futures_exit_price - trade.futures_entry_price) * barrels
        else:
            futures_pnl = (trade.futures_entry_price - futures_exit_price) * barrels

        trading_pnl = perp_pnl + futures_pnl

        # 베이시스 PnL (bps)
        basis_pnl_bps = trade.entry_basis_bps - trade.exit_basis_bps
        if trade.direction == "short_basis":
            basis_pnl_bps = -basis_pnl_bps

        # 펀딩 PnL (USD) — bps to USD
        avg_price = (trade.perp_entry_price + perp_exit_price) / 2
        funding_pnl_usd = trade.funding_pnl_bps / 10000 * avg_price * barrels

        # 수수료
        # Perp: 노셔널 기반 (bps)
        perp_notional = avg_price * barrels
        perp_fees = perp_notional * self.PERP_TAKER_FEE_BPS / 10000 * 2  # entry + exit

        # Futures: 고정 per-contract ($)
        product_cfg = self.config.products.get(trade.product)
        fee_per_contract = product_cfg.futures_fee_per_contract if product_cfg else 7.5
        futures_fees = fee_per_contract * trade.size_contracts * 2  # entry + exit

        total_fees = perp_fees + futures_fees

        net_pnl = trading_pnl + funding_pnl_usd - total_fees

        return {
            "basis_pnl_bps": basis_pnl_bps,
            "trading_pnl_usd": trading_pnl,
            "funding_pnl_usd": funding_pnl_usd,
            "perp_fees_usd": perp_fees,
            "futures_fees_usd": futures_fees,
            "total_fees_usd": total_fees,
            "net_pnl_usd": net_pnl,
        }

    # ── 사이즈 계산 ──

    def _calculate_position_size_usd(self, product: str, futures_price: float) -> float:
        """포지션 사이즈 (USD) 계산. 배럴 기준 노셔널."""
        product_cfg = self.config.products[product]
        barrels = product_cfg.min_order_size * product_cfg.contract_size
        return futures_price * barrels

    def _calculate_contracts(
        self, product: str, max_size_usd: float, futures_price: float
    ) -> int:
        """최대 허용 사이즈 내에서 CME 계약 수 계산."""
        product_cfg = self.config.products[product]
        if futures_price <= 0:
            return 0
        # 1계약 노셔널 = 가격 × contract_size(배럴)
        notional_per_contract = futures_price * product_cfg.contract_size
        max_contracts = int(max_size_usd / notional_per_contract)
        min_size = product_cfg.min_order_size
        return max(min_size, min(max_contracts, self.config.risk.max_position_contracts))

    def _get_perp_margin_usage(self) -> float:
        """Perp 마진 사용률 추정 (페이퍼 모드)."""
        # 페이퍼 모드에서는 오픈 포지션 수 기반 추정
        if not self._open_trades:
            return 0.0
        # 간이 추정: 포지션 1개당 ~10% 사용 가정
        return len(self._open_trades) * 10.0

    # ── 상태 조회 ──

    def get_state(self) -> EngineState:
        """엔진 상태 반환."""
        return self._state

    def get_open_trades(self) -> dict[str, TradeRecord]:
        """현재 오픈 트레이드."""
        return self._open_trades.copy()

    def get_closed_trades(self) -> list[TradeRecord]:
        """완료된 트레이드 목록."""
        return self._closed_trades.copy()

    def get_unrealized_pnl(self, product: str) -> dict | None:
        """오픈 포지션의 미실현 PnL."""
        trade = self._open_trades.get(product)
        if not trade:
            return None

        perp_price = self._latest_perp_prices.get(product, trade.perp_entry_price)
        futures_price = self._latest_futures_prices.get(product, trade.futures_entry_price)

        return self._calculate_pnl(trade, perp_price, futures_price)

    def get_summary(self) -> str:
        """엔진 상태 요약 문자열."""
        s = self._state
        lines = [
            f"=== Paper Trading Summary ===",
            f"Signals: {s.total_signals} | Entries: {s.total_entries} | Exits: {s.total_exits}",
            f"Risk rejected: {s.rejected_by_risk} | Order failures: {s.failed_orders}",
            f"Entry signals: {s.entry_signals_generated} | exec_skip: {s.entry_exec_filter_skip} | warmup_skip: {s.entry_warmup_skip}",
            f"Open: {s.open_positions} | Closed: {s.closed_trades}",
            f"Cumulative PnL: ${s.cumulative_pnl_usd:+.2f}",
        ]

        # 오픈 포지션 상세
        for product, trade in self._open_trades.items():
            upnl = self.get_unrealized_pnl(product)
            hold_h = (time.time() - trade.entry_time) / 3600
            if upnl:
                lines.append(
                    f"  [{product.upper()}] {trade.direction} | "
                    f"entry={trade.entry_basis_bps:+.1f}bp | "
                    f"unrealized=${upnl['net_pnl_usd']:+.2f} | "
                    f"hold={hold_h:.1f}h"
                )

        # 최근 청산 3건
        if self._closed_trades:
            lines.append("--- Recent Closed ---")
            for t in self._closed_trades[-3:]:
                lines.append(
                    f"  [{t.product.upper()}] {t.direction} | "
                    f"pnl=${t.net_pnl_usd:+.2f} | {t.exit_reason}"
                )

        return "\n".join(lines)
