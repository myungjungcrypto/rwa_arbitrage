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


import asyncio
import time
import logging
from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Optional, Callable

from src.exchange.base import OrderResult as BaseOrderResult, Quote
from src.exchange.registry import ExchangeRegistry
from src.strategy.signals import SignalGenerator, Signal, SignalType, PositionState
from src.strategy.pair import ArbitragePair, LegRole
from src.risk.manager import RiskManager, RiskCheck
from src.data.storage import Storage, LEGACY_PRODUCT_PAIR_MAP
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
        registry: ExchangeRegistry | None = None,
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

        # 상태 추적 (legacy product-keyed)
        self._open_trades: dict[str, TradeRecord] = {}  # product -> open trade
        self._closed_trades: list[TradeRecord] = []
        self._trade_counter = 0
        self._state = EngineState()

        # 최신 가격 캐시 (legacy product-keyed)
        self._latest_perp_prices: dict[str, float] = {}   # product -> mark_price
        self._latest_index_prices: dict[str, float] = {}   # product -> index_price
        self._latest_futures_prices: dict[str, float] = {}  # product -> futures_price (mid)
        self._latest_perp_bid: dict[str, float] = {}       # product -> best bid
        self._latest_perp_ask: dict[str, float] = {}       # product -> best ask
        self._latest_futures_bid: dict[str, float] = {}    # product -> futures best bid
        self._latest_futures_ask: dict[str, float] = {}    # product -> futures best ask

        # ── Phase C4a: pair-keyed 인프라 (Phase C5에서 main.py가 wire) ──
        self._registry: Optional[ExchangeRegistry] = registry
        self._registered_pairs: dict[str, ArbitragePair] = {}
        self._open_trades_by_pair: dict[str, TradeRecord] = {}     # pair_id -> open trade
        self._latest_pair_quote: dict[tuple[str, str], Quote] = {}  # (pair_id, leg) -> Quote
        # 각 거래소당 동시 in-flight 주문 1건 보장 (HL이 5개 페어의 leg_a라 충돌 위험)
        self._exchange_semaphores: dict[str, asyncio.Semaphore] = {}

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
        # bid/ask가 0이면 0 그대로 저장 — exec_filter가 진입 차단.
        # 이전 `or perp_price` fallback은 bid=ask=mid가 되어 exec_basis ≈ mid_basis,
        # 결과적으로 exec_filter가 무력화돼 sub-10bp 진입 다수 발생 (2026-04-21~04-27 14건).
        self._latest_perp_bid[product] = perp_best_bid
        self._latest_perp_ask[product] = perp_best_ask
        self._latest_futures_bid[product] = futures_bid
        self._latest_futures_ask[product] = futures_ask

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
        # cache가 0(오더북 미수신)이면 mid로 fallback — entry는 어차피 exec_filter가
        # 0을 차단하므로 여기 도달하지 않음. 안전망.
        if perp_side == "buy":
            perp_fill_price = self._latest_perp_ask.get(product) or perp_price
        else:
            perp_fill_price = self._latest_perp_bid.get(product) or perp_price

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
        # cache가 0이면 mid(perp_price)로 fallback (안전망 — exit은 exec_filter 거치지 않음)
        perp_close_side = "buy" if trade.perp_side == "short" else "sell"
        if perp_close_side == "buy":
            perp_fill_price = self._latest_perp_ask.get(product) or perp_price
        else:
            perp_fill_price = self._latest_perp_bid.get(product) or perp_price

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

    # ── State snapshot loop (Phase M1, 대시보드용) ──

    def snapshot_state_to_db(self) -> int:
        """현재 EngineState + 페어별 basis 통계를 engine_state 테이블에 1 row INSERT.

        반환: 작성된 row 수 (페어 수). 호출자가 주기적으로 부르거나 백그라운드
        루프에서 호출.
        """
        n = 0
        state_dict = asdict(self._state)
        for product in self.config.products:
            pair_id = LEGACY_PRODUCT_PAIR_MAP.get(product, product)
            try:
                basis_stats = self.signal_gen.get_basis_stats(product)
            except Exception:
                basis_stats = None
            try:
                self.storage.save_engine_state(pair_id, state_dict, basis_stats)
                n += 1
            except Exception as e:
                logger.error(f"engine_state save failed [{pair_id}]: {e}")
        return n

    async def state_snapshot_loop(
        self,
        interval_seconds: int = 30,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """백그라운드: 매 N초마다 engine_state 스냅샷 dump.

        main.py가 `asyncio.create_task(engine.state_snapshot_loop(stop_event=...))`
        로 기동. stop_event.set()으로 종료.
        """
        logger.info(f"[STATE_SNAPSHOT] loop started (interval={interval_seconds}s)")
        # 부팅 직후 첫 스냅샷
        try:
            self.snapshot_state_to_db()
        except Exception as e:
            logger.error(f"state_snapshot_loop initial dump error: {e}")

        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("[STATE_SNAPSHOT] loop stopped")
                return
            try:
                if stop_event is not None:
                    await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
                    if stop_event.is_set():
                        return
                else:
                    await asyncio.sleep(interval_seconds)
            except asyncio.TimeoutError:
                pass
            try:
                self.snapshot_state_to_db()
            except Exception as e:
                logger.error(f"state_snapshot_loop dump error: {e}")

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

    # ──────────────────────────────────────────────
    # Phase C4a: pair-keyed 인프라 (registration / quote 캐시 / dispatch helper)
    # main.py(C5)가 ExchangeRegistry 주입 후 페어 등록 + collector 콜백 wire.
    # 실제 entry/exit 흐름은 C4b에서 추가.
    # ──────────────────────────────────────────────

    def set_exchange_registry(self, registry: ExchangeRegistry) -> None:
        """ExchangeRegistry 주입. main.py가 어댑터 등록 후 호출."""
        self._registry = registry

    def register_pair(self, pair: ArbitragePair) -> None:
        """추적 대상 페어 등록. 이미 등록된 pair_id면 덮어씀."""
        self._registered_pairs[pair.id] = pair
        # 거래소별 세마포어 lazy-create (페어가 사용하는 양 leg 모두)
        for leg in (pair.leg_a, pair.leg_b):
            if leg.exchange not in self._exchange_semaphores:
                self._exchange_semaphores[leg.exchange] = asyncio.Semaphore(1)
        logger.info(
            f"[ENGINE] pair registered: {pair.id} "
            f"({pair.leg_a.exchange}/{pair.leg_a.symbol} ↔ "
            f"{pair.leg_b.exchange}/{pair.leg_b.symbol})"
        )

    def get_registered_pair(self, pair_id: str) -> Optional[ArbitragePair]:
        return self._registered_pairs.get(pair_id)

    @property
    def registered_pairs(self) -> dict[str, ArbitragePair]:
        return dict(self._registered_pairs)

    def get_pair_open_trade(self, pair_id: str) -> Optional[TradeRecord]:
        return self._open_trades_by_pair.get(pair_id)

    def get_pair_open_trades(self) -> dict[str, TradeRecord]:
        return self._open_trades_by_pair.copy()

    def cache_pair_quote(self, pair_id: str, leg: str, quote: Quote) -> None:
        """페어 leg별 최신 Quote 캐시. collector callback 또는 외부 어댑터에서 호출."""
        if leg not in ("a", "b"):
            raise ValueError(f"leg must be 'a' or 'b', got {leg!r}")
        self._latest_pair_quote[(pair_id, leg)] = quote

    def latest_pair_quote(self, pair_id: str, leg: str) -> Optional[Quote]:
        return self._latest_pair_quote.get((pair_id, leg))

    def has_both_legs(self, pair_id: str) -> bool:
        return ((pair_id, "a") in self._latest_pair_quote
                and (pair_id, "b") in self._latest_pair_quote)

    @staticmethod
    def compute_pair_exec_basis(
        direction: str, leg_a: Quote, leg_b: Quote
    ) -> float:
        """exec_basis (체결 가능 spread, bp).

        long_basis 진입: leg_a 매도(bid) + leg_b 매수(ask)
                          → exec = (a.bid - b.ask) / b.ask × 10000
        short_basis 진입: leg_a 매수(ask) + leg_b 매도(bid)
                          → exec = (a.ask - b.bid) / b.bid × 10000

        bid/ask 누락 시 0 반환 (호출자가 차단 결정).
        """
        if direction == "long_basis":
            if leg_a.bid <= 0 or leg_b.ask <= 0:
                return 0.0
            return (leg_a.bid - leg_b.ask) / leg_b.ask * 10_000
        if direction == "short_basis":
            if leg_a.ask <= 0 or leg_b.bid <= 0:
                return 0.0
            return (leg_a.ask - leg_b.bid) / leg_b.bid * 10_000
        raise ValueError(f"direction must be 'long_basis' or 'short_basis', got {direction!r}")

    # ──────────────────────────────────────────────
    # Phase C4b: pair-keyed entry/exit 흐름
    # ──────────────────────────────────────────────

    async def process_pair_basis_update(
        self,
        pair_id: str,
        basis_bps: float,
        leg_a: Quote,
        leg_b: Quote,
        funding_rate: float | None = None,
        current_time: float | None = None,
    ) -> Optional[Signal]:
        """Pair-keyed 메인 entry point. collector.on_pair_basis 콜백에 등록.

        흐름:
          1. quote 캐시
          2. SignalGenerator pair-keyed update
          3. ENTRY/EXIT 시그널이면 해당 핸들러 dispatch
        반환: 생성된 Signal (콜백/디버깅용).
        """
        if pair_id not in self._registered_pairs:
            logger.warning(f"[{pair_id}] basis update on unregistered pair — ignored")
            return None

        # 1. 캐시
        self.cache_pair_quote(pair_id, "a", leg_a)
        self.cache_pair_quote(pair_id, "b", leg_b)

        # funding_rate 미지정 시 leg_a 우선 (perp leg)
        funding = funding_rate if funding_rate is not None else leg_a.funding_rate

        # 2. signal generator update
        self._state.total_signals += 1
        signal = self.signal_gen.update_basis_for_pair(
            pair_id, basis_bps, funding_rate=funding,
            leg_a_bid=leg_a.bid, leg_a_ask=leg_a.ask,
            leg_b_bid=leg_b.bid, leg_b_ask=leg_b.ask,
            current_time=current_time,
        )

        # 시그널 콜백 (NONE 제외)
        if signal.type != SignalType.NONE:
            for cb in self._on_signal_callbacks:
                try:
                    cb(signal)
                except Exception as e:
                    logger.error(f"signal callback error: {e}")

        # 3. dispatch
        if signal.type in (SignalType.ENTRY_LONG_BASIS, SignalType.ENTRY_SHORT_BASIS):
            self._state.entry_signals_generated += 1
            await self._handle_pair_entry(pair_id, signal, leg_a, leg_b)
        elif signal.type in (SignalType.EXIT, SignalType.EMERGENCY_CLOSE):
            await self._handle_pair_exit(pair_id, signal, leg_a, leg_b)
        return signal

    async def _handle_pair_entry(
        self, pair_id: str, signal: Signal, leg_a: Quote, leg_b: Quote
    ) -> None:
        """진입 처리 (pair-keyed). 레거시 _handle_entry와 미러 — 회귀 위험 없도록
        flow 동일. 차이: kiwoom 대신 dispatch_pair_order 사용 (KIS 어댑터 placeholder),
        sizing은 pair.leg_b.contract_size 직접 참조."""
        if pair_id in self._open_trades_by_pair:
            return

        pair = self._registered_pairs[pair_id]
        direction = "long_basis" if signal.type == SignalType.ENTRY_LONG_BASIS else "short_basis"

        # 워밍업 체크
        history = self.signal_gen._basis_history.get(pair_id)
        if history and len(history) < self.MIN_WARMUP_POINTS:
            self._state.entry_warmup_skip += 1
            logger.warning(
                f"[{pair_id}] ENTRY_SKIP warmup: {len(history)}/{self.MIN_WARMUP_POINTS}"
            )
            return

        # exec_filter
        exec_basis = self.compute_pair_exec_basis(direction, leg_a, leg_b)
        threshold = pair.params.entry_threshold_bps
        if direction == "short_basis" and exec_basis > -threshold:
            self._state.entry_exec_filter_skip += 1
            logger.warning(
                f"[{pair_id}] ENTRY_SKIP exec_filter: exec={exec_basis:.1f}bp > -{threshold}bp"
            )
            return
        if direction == "long_basis" and exec_basis < threshold:
            self._state.entry_exec_filter_skip += 1
            logger.warning(
                f"[{pair_id}] ENTRY_SKIP exec_filter: exec={exec_basis:.1f}bp < +{threshold}bp"
            )
            return

        # min_abs_entry_bps floor (절대값 가드)
        min_abs = self.config.strategy.min_abs_entry_bps
        if min_abs > 0 and abs(exec_basis) < min_abs:
            self._state.entry_min_abs_skip += 1
            logger.warning(
                f"[{pair_id}] ENTRY_SKIP min_abs: |exec|={abs(exec_basis):.1f}bp "
                f"< min_abs={min_abs:.1f}bp"
            )
            return

        # 사이징 — leg_b 기준 notional × max_position_usd cap
        leg_b_price = leg_b.mid_price or (leg_b.bid + leg_b.ask) / 2
        contracts = self._calculate_pair_contracts(pair, leg_b_price)
        if contracts < 1:
            logger.warning(f"[{pair_id}] contracts < 1, skipping entry")
            return

        # leg_b의 contract_size 단위로 leg_a units 환산 (CME MCL=100배럴 등)
        leg_a_size = contracts * pair.leg_b.contract_size
        leg_b_size = float(contracts)

        # 리스크 체크 (legacy 인터페이스 — product='wti' 매핑)
        size_usd = leg_b_price * leg_a_size
        legacy_product = pair_id.split("_", 1)[0]
        risk_check = self.risk_mgr.check_entry(
            product=legacy_product,
            size_usd=size_usd,
            perp_margin_usage_pct=self._get_perp_margin_usage(),
            futures_margin_usage_pct=self.kiwoom.get_margin_info().get("usage_pct", 0),
            current_basis_bps=signal.basis_bps,
            is_rollover_period=self.risk_mgr.is_rollover_period(),
        )
        if not risk_check.allowed:
            self._state.rejected_by_risk += 1
            logger.warning(f"[{pair_id}] Entry REJECTED by risk: {risk_check.reason}")
            return

        # ── 양 leg 동시 발주 (paper: kiwoom for KIS, simulate for perp) ──
        leg_a_side = "sell" if direction == "long_basis" else "buy"
        leg_b_side = "buy" if direction == "long_basis" else "sell"

        async def _fill_a() -> tuple[float, str]:
            return await self._fill_pair_leg(pair, "a", leg_a_side, leg_a_size, leg_a)

        async def _fill_b() -> tuple[float, str]:
            return await self._fill_pair_leg(pair, "b", leg_b_side, leg_b_size, leg_b)

        (a_price, a_oid), (b_price, b_oid) = await asyncio.gather(_fill_a(), _fill_b())
        if a_price <= 0 or b_price <= 0:
            self._state.failed_orders += 1
            logger.error(
                f"[{pair_id}] one or both legs failed to fill "
                f"(a={a_price}, b={b_price}); skipping entry"
            )
            return

        # 동시 진입 race 방어 — gather 동안 다른 coroutine이 이미 진입했는지 재확인
        if pair_id in self._open_trades_by_pair:
            logger.info(f"[{pair_id}] ENTRY race — already opened by concurrent task")
            return

        # ── 기록 ──
        self._trade_counter += 1
        trade = TradeRecord(
            trade_id=self._trade_counter,
            product=legacy_product,
            direction=direction,
            entry_time=time.time(),
            entry_basis_bps=signal.basis_bps,
            perp_entry_price=a_price,
            futures_entry_price=b_price,
            perp_side="short" if direction == "long_basis" else "long",
            futures_side="long" if direction == "long_basis" else "short",
            size_contracts=contracts,
            perp_units=leg_a_size,
            status="open",
        )
        self._open_trades_by_pair[pair_id] = trade
        self._state.total_entries += 1
        self._state.open_positions += 1

        self.signal_gen.open_position_for_pair(pair_id, signal, size=contracts)
        pos = self.signal_gen.get_position_for_pair(pair_id)
        pos.entry_exec_basis_bps = exec_basis

        # DB
        self.storage.save_order(
            product=legacy_product, leg="perp", side=leg_a_side,
            size=leg_a_size, price=a_price, filled_price=a_price,
            filled_size=leg_a_size, status="filled", is_paper=True,
            pair_id=pair_id, exchange=pair.leg_a.exchange,
        )
        self.storage.save_order(
            product=legacy_product, leg="futures", side=leg_b_side,
            size=leg_b_size, price=b_price, filled_price=b_price,
            filled_size=leg_b_size, order_id=b_oid, status="filled",
            is_paper=True, pair_id=pair_id, exchange=pair.leg_b.exchange,
        )
        self.storage.save_position(
            product=legacy_product,
            perp_size=leg_a_size if trade.perp_side == "long" else -leg_a_size,
            perp_entry=a_price,
            futures_size=leg_b_size if trade.futures_side == "long" else -leg_b_size,
            futures_entry=b_price,
            pair_id=pair_id,
        )

        logger.info(
            f"[{pair_id}] ▶ ENTRY {direction} | basis={signal.basis_bps:+.1f}bp | "
            f"leg_a {leg_a_side} {leg_a_size}@{a_price:.2f} | "
            f"leg_b {leg_b_side} {leg_b_size}@{b_price:.2f} | "
            f"exec={exec_basis:+.1f}bp"
        )
        for cb in self._on_trade_callbacks:
            try:
                cb(trade, "open")
            except Exception as e:
                logger.error(f"Trade callback error: {e}")

    async def _handle_pair_exit(
        self, pair_id: str, signal: Signal, leg_a: Quote, leg_b: Quote
    ) -> None:
        """청산 처리 (pair-keyed)."""
        trade = self._open_trades_by_pair.get(pair_id)
        if not trade:
            logger.warning(f"[{pair_id}] EXIT signal but no open trade")
            return

        pair = self._registered_pairs[pair_id]
        leg_a_close_side = "buy" if trade.perp_side == "short" else "sell"
        leg_b_close_side = "sell" if trade.futures_side == "long" else "buy"

        leg_a_size = trade.perp_units
        leg_b_size = float(trade.size_contracts)

        async def _fill_a():
            return await self._fill_pair_leg(pair, "a", leg_a_close_side, leg_a_size, leg_a)

        async def _fill_b():
            return await self._fill_pair_leg(pair, "b", leg_b_close_side, leg_b_size, leg_b)

        (a_exit, _), (b_exit, b_exit_oid) = await asyncio.gather(_fill_a(), _fill_b())
        if a_exit <= 0 or b_exit <= 0:
            self._state.failed_orders += 1
            logger.error(
                f"[{pair_id}] EXIT fill failed (a={a_exit}, b={b_exit}); will retry"
            )
            return

        # 동시 청산 race 방어 — gather 동안 다른 coroutine이 이미 청산했는지 atomic pop
        trade = self._open_trades_by_pair.pop(pair_id, None)
        if trade is None:
            logger.info(f"[{pair_id}] EXIT race — already closed by concurrent task")
            return

        pnl = self._calculate_pnl(trade, a_exit, b_exit)

        trade.exit_time = time.time()
        trade.exit_basis_bps = signal.basis_bps
        trade.perp_exit_price = a_exit
        trade.futures_exit_price = b_exit
        trade.exit_reason = signal.reason
        trade.basis_pnl_bps = pnl["basis_pnl_bps"]
        trade.perp_fees_usd = pnl["perp_fees_usd"]
        trade.futures_fees_usd = pnl["futures_fees_usd"]
        trade.net_pnl_usd = pnl["net_pnl_usd"]
        trade.status = "closed"

        self._state.total_exits += 1
        self._state.open_positions -= 1
        self._state.closed_trades += 1
        self._state.cumulative_pnl_usd += pnl["net_pnl_usd"]
        self.risk_mgr.record_pnl(pnl["net_pnl_usd"])
        self.signal_gen.close_position_for_pair(pair_id)

        legacy_product = trade.product
        self._closed_trades.append(trade)

        self.storage.save_order(
            product=legacy_product, leg="perp", side=leg_a_close_side,
            size=leg_a_size, price=a_exit, filled_price=a_exit,
            filled_size=leg_a_size, status="filled", is_paper=True,
            pair_id=pair_id, exchange=pair.leg_a.exchange,
        )
        self.storage.save_order(
            product=legacy_product, leg="futures", side=leg_b_close_side,
            size=leg_b_size, price=b_exit, filled_price=b_exit,
            filled_size=leg_b_size, order_id=b_exit_oid, status="filled",
            is_paper=True, pair_id=pair_id, exchange=pair.leg_b.exchange,
        )
        self.storage.close_position_by_pair(
            pair_id=pair_id,
            realized_pnl=pnl["net_pnl_usd"],
            funding_pnl=pnl["funding_pnl_usd"],
        )
        self.storage.update_daily_pnl(
            product=legacy_product,
            trading_pnl=pnl["trading_pnl_usd"],
            funding_pnl=pnl["funding_pnl_usd"],
            fees=pnl["total_fees_usd"],
            pair_id=pair_id,
        )

        hold_hours = (trade.exit_time - trade.entry_time) / 3600
        emoji = "✅" if pnl["net_pnl_usd"] >= 0 else "❌"
        logger.info(
            f"[{pair_id}] {emoji} EXIT {trade.direction} | "
            f"basis: {trade.entry_basis_bps:+.1f} → {signal.basis_bps:+.1f}bp | "
            f"pnl=${pnl['net_pnl_usd']:+.2f} | hold={hold_hours:.1f}h | {signal.reason}"
        )
        for cb in self._on_trade_callbacks:
            try:
                cb(trade, "close")
            except Exception as e:
                logger.error(f"Trade callback error: {e}")

    async def _fill_pair_leg(
        self,
        pair: ArbitragePair,
        leg: str,
        side: str,
        size: float,
        leg_quote: Quote,
    ) -> tuple[float, str]:
        """페어 leg에 paper-fill 주문. (filled_price, order_id) 반환.

        - leg.exchange == 'kis': legacy KiwoomMock 사용 (paper 한정).
        - 그 외 (HL/Binance/Bybit/OKX/Lighter): 캐시된 bid/ask로 시뮬레이션 fill.
          (실거래는 Phase I에서 dispatch_pair_order 통해 실 주문)

        실패 시 (0.0, "") 반환.
        """
        leg_cfg = pair.leg(leg)
        if leg_cfg.exchange == "kis":
            futures_symbol = leg_cfg.symbol
            kw_order = self.kiwoom.place_order(
                symbol=futures_symbol, side=side, quantity=int(size),
            )
            if not kw_order.success:
                return 0.0, ""
            return kw_order.filled_price, kw_order.order_no

        # perp 시뮬: buy → ask, sell → bid. 누락 시 mid fallback.
        if side == "buy":
            fill = leg_quote.ask if leg_quote.ask > 0 else leg_quote.mid_price
        else:
            fill = leg_quote.bid if leg_quote.bid > 0 else leg_quote.mid_price
        if fill <= 0:
            return 0.0, ""
        return fill, f"PAPER-{int(time.time()*1000)}"

    def _calculate_pair_contracts(
        self, pair: ArbitragePair, leg_b_price: float
    ) -> int:
        """페어 leg_b 기준 계약수 결정.

        max_position_usd cap + max_position_contracts cap 적용.
        leg_b가 perp(contract_size=1)이면 1배럴/USDC 단위 size로 동작.
        """
        if leg_b_price <= 0:
            return 0
        cs = pair.leg_b.contract_size or 1.0
        notional_per_contract = leg_b_price * cs
        if notional_per_contract <= 0:
            return 1
        max_size_usd = self.config.risk.max_position_usd
        max_contracts = int(max_size_usd / notional_per_contract)
        return max(1, min(max_contracts, self.config.risk.max_position_contracts))

    async def dispatch_pair_order(
        self,
        pair_id: str,
        leg: str,
        side: str,                           # "buy" | "sell"
        size: float,
        order_type: str = "market",
        limit_price: Optional[float] = None,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> BaseOrderResult:
        """페어 leg에 주문 발사. ExchangeRegistry → ExchangeBase.place_order.

        거래소별 Semaphore(1)로 in-flight 1건 보장 (HL은 5개 페어의 leg_a라
        동시 진입 시 충돌 위험).
        """
        if self._registry is None:
            raise RuntimeError("ExchangeRegistry not set; call set_exchange_registry first")
        pair = self._registered_pairs.get(pair_id)
        if pair is None:
            raise KeyError(f"pair {pair_id!r} not registered")
        leg_cfg = pair.leg(leg)

        if not self._registry.has(leg_cfg.exchange):
            return BaseOrderResult(
                success=False, exchange=leg_cfg.exchange, symbol=leg_cfg.symbol,
                error=f"exchange {leg_cfg.exchange!r} not in registry",
            )
        adapter = self._registry.get(leg_cfg.exchange)
        sem = self._exchange_semaphores.setdefault(
            leg_cfg.exchange, asyncio.Semaphore(1)
        )
        async with sem:
            try:
                return await adapter.place_order(
                    symbol=leg_cfg.symbol,
                    side=side,                    # type: ignore[arg-type]
                    size=size,
                    order_type=order_type,        # type: ignore[arg-type]
                    limit_price=limit_price,
                    reduce_only=reduce_only,
                    client_order_id=client_order_id,
                )
            except NotImplementedError as e:
                # KIS 등 페이퍼 단계 미구현 어댑터
                return BaseOrderResult(
                    success=False, exchange=leg_cfg.exchange, symbol=leg_cfg.symbol,
                    error=f"NotImplementedError: {e}",
                )
            except Exception as e:
                logger.error(f"[{pair_id}/{leg}] place_order error: {e}")
                return BaseOrderResult(
                    success=False, exchange=leg_cfg.exchange, symbol=leg_cfg.symbol,
                    error=str(e),
                )
