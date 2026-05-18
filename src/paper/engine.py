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
        # pair별 entry-in-flight 락 — 동일 페어의 _handle_pair_entry가 동시에 두
        # 번 이상 돌면 같은 신호로 양 leg 다중 주문 발사되어 KIS rate-limit (초당
        # 거래 한도) 위반. 락으로 직렬화 + 락 안에서 _open_trades_by_pair 체크해
        # 중복 진입 차단.
        self._pair_entry_locks: dict[str, asyncio.Lock] = {}
        # pair별 EXIT 락 — entry와 동일 이유 (1초 안 다중 EXIT 호출 → KIS rate
        # limit + 이미 청산된 포지션에 또 sell). 5/18 07:58 incident fix.
        self._pair_exit_locks: dict[str, asyncio.Lock] = {}
        # 마지막 EXIT 시도 시각 (성공/실패 무관) — 5s cooldown으로 KIS 보호.
        self._last_exit_attempt_ts: dict[str, float] = {}

        # 최소 워밍업 데이터 수 (이 이하면 거래 안 함)
        self.MIN_WARMUP_POINTS = 3600  # 약 1시간 분량

        # 이벤트 콜백
        self._on_trade_callbacks: list[Callable] = []
        self._on_signal_callbacks: list[Callable] = []

        # Phase 11d — 알림 / 워치독.
        self._notifier = None  # set_notifier()로 wire
        # leg_a/leg_b 구독 freshness 워치독용
        self._last_quote_ts: dict[tuple[str, str], float] = {}  # (pair_id, leg) -> ts
        # rollover blackout 알림 cooldown — 상태 전이 시만 1회 알림
        self._rollover_blackout_announced: bool = False

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

    def set_notifier(self, notifier) -> None:
        """Telegram (또는 호환) notifier 등록 (Phase 11d).

        notifier에 send_sync(str), send_error_alert(coroutine) 시그니처 기대.
        None이면 알림 비활성화. PAPER 모드에서도 set 되면 거래/에러 알림 동작.
        """
        self._notifier = notifier

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

    async def kis_license_health_loop(
        self,
        interval_seconds: int = 1800,    # 30분 — 가벼운 체크
        stale_threshold_minutes: float = 5.0,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """KIS 시세 라이센스 만료 자가 감지.

        매 N분마다:
          1. CME 시장이 open인지 (휴장 시 quote 없는 게 정상이라 skip)
          2. futures_prices 최근 row age > stale_threshold_minutes
          3. 위 둘 모두 yes → 라이센스 만료 또는 KIS 시스템 장애 의심
          4. 한 번만 알림 (상태 전이 — 라이센스 만료 → 회복 시도 cooldown)

        WS_WATCHDOG (60s)는 즉각 알림이고 이건 5분 슬랙 + 라이센스 명시.
        """
        from src.strategy.market_hours import is_cme_open
        from datetime import datetime, timezone

        # legacy KIS-only 페어 한정 — 다른 페어는 leg_b가 perp라 무관
        kis_pair_ids = [
            pid for pid, p in self._registered_pairs.items()
            if p.leg_b.role == LegRole.DATED_FUTURES and p.leg_b.exchange == "kis"
        ]
        if not kis_pair_ids:
            logger.info("[KIS_LICENSE] no KIS pairs registered — health loop disabled")
            return
        logger.info(
            f"[KIS_LICENSE] health monitor started interval={interval_seconds}s "
            f"stale_threshold={stale_threshold_minutes}min pairs={kis_pair_ids}"
        )
        alerted = False

        while True:
            if stop_event is not None and stop_event.is_set():
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

            # 1. CME open?
            if not is_cme_open(datetime.now(timezone.utc)):
                continue   # 휴장 — quote 없는 게 정상

            # 2. futures_prices stale?
            try:
                row = self.storage.conn.execute(
                    "SELECT MAX(ts) AS t FROM futures_prices"
                ).fetchone()
                last_ts = row["t"] if row else None
            except Exception as e:
                logger.error(f"[KIS_LICENSE] DB query error: {e}")
                continue
            if last_ts is None:
                continue
            age_min = (time.time() - last_ts) / 60.0
            if age_min < stale_threshold_minutes:
                # 회복됨 — alerted였으면 recovery 알림 1회
                if alerted:
                    alerted = False
                    logger.info(f"[KIS_LICENSE] feed recovered ({age_min:.1f}m ago)")
                    if self._notifier is not None:
                        try:
                            self._notifier.send_sync(
                                f"✅ <b>KIS LICENSE RECOVERED</b>\n"
                                f"feed back online (last quote {age_min:.1f}m ago)"
                            )
                        except Exception:
                            pass
                continue

            # 3. stale + CME open → 라이센스 의심
            if not alerted:
                alerted = True
                logger.error(
                    f"[KIS_LICENSE] feed stale {age_min:.1f}m + CME OPEN — "
                    "라이센스 만료 의심"
                )
                if self._notifier is not None:
                    try:
                        self._notifier.send_sync(
                            f"🚨 <b>KIS 시세 STALE</b>\n"
                            f"채널: KIS WS ws://ops.koreainvestment.com:21000\n"
                            f"마지막 futures_prices: <b>{age_min:.1f}분 전</b> "
                            f"(CME OPEN 상태)\n\n"
                            f"의심 원인 우선순위:\n"
                            f"1) NYMEX 실시간 시세 결제 만료\n"
                            f"   → https://apiportal.koreainvestment.com\n"
                            f"2) 같은 KIS app_key를 다른 봇이 동시 WS 사용\n"
                            f"   → pm2 logs <봇> | grep -i kis\n"
                            f"3) KIS 서버측 maintenance / break\n"
                            f"4) 봇의 KIS WS reconnect 실패\n"
                            f"   → pm2 logs rwa-arb | grep -i 'kis.*disconnect'"
                        )
                    except Exception as e:
                        logger.error(f"notifier kis-license error: {e}")

    async def balance_poll_loop(
        self,
        interval_seconds: int = 120,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """등록된 모든 거래소 어댑터의 잔고를 N초마다 DB에 저장.

        대시보드가 읽음. 어댑터별로 try/except로 보호 (한 거래소 실패해도
        나머지는 계속). registry 미등록 시 즉시 종료.
        """
        if self._registry is None:
            logger.info("[BALANCE_POLL] no registry — loop disabled")
            return

        names = list(self._registry.names())
        logger.info(
            f"[BALANCE_POLL] started interval={interval_seconds}s "
            f"adapters={names}"
        )
        # 첫 polling 즉시 1회
        await self._poll_all_balances()
        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("[BALANCE_POLL] stopped")
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
            await self._poll_all_balances()

    async def _poll_all_balances(self) -> None:
        if self._registry is None:
            return
        for name in self._registry.names():
            adapter = self._registry.get(name)
            try:
                value = await adapter.get_account_value()
                note = "ok"
            except NotImplementedError:
                continue   # paper-only 어댑터 (lighter/binance/bybit/okx scaffold) 스킵
            except Exception as e:
                logger.warning(f"[BALANCE_POLL] {name} failed: {e}")
                value = 0.0
                note = f"error: {str(e)[:80]}"
            currency = getattr(adapter, "margin_asset", "USD") or "USD"
            try:
                self.storage.save_account_balance(
                    exchange=name, value=float(value),
                    currency=currency, note=note,
                )
            except Exception as e:
                logger.error(f"[BALANCE_POLL] save {name} failed: {e}")

    async def quote_freshness_watchdog(
        self,
        interval_seconds: int = 15,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """Phase 11d — LIVE 모드 WS freshness 워치독.

        등록된 페어 각 leg의 마지막 Quote 수신 시각을 검사. 임계값
        (config.risk.ws_stale_seconds) 초과 시:
          1. notifier로 stale 알림
          2. ws_stale_auto_flatten=True면 모든 오픈 포지션 reduce_only 청산
          3. cooldown — 한 번 알린 leg는 다시 fresh해지기 전까지 재알림 X

        PAPER 모드에서는 no-op (시작 직후 즉시 종료). LIVE 모드에서만 의미.
        """
        if (self.config.mode or "").upper() != "LIVE":
            logger.info("[WS_WATCHDOG] PAPER mode — watchdog disabled")
            return

        threshold = float(self.config.risk.ws_stale_seconds or 60.0)
        auto_flatten = bool(self.config.risk.ws_stale_auto_flatten)
        logger.info(
            f"[WS_WATCHDOG] started threshold={threshold}s "
            f"auto_flatten={auto_flatten} interval={interval_seconds}s"
        )
        # 알림 cooldown: leg가 stale 상태 동안 1회만 알림
        alerted: set[tuple[str, str]] = set()

        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("[WS_WATCHDOG] stopped")
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

            now = time.time()
            stale_legs: list[tuple[str, str, float]] = []
            for pair_id, pair in self._registered_pairs.items():
                if not pair.enabled:
                    continue
                for leg in ("a", "b"):
                    last_ts = self._last_quote_ts.get((pair_id, leg))
                    if last_ts is None:
                        continue   # 아직 첫 Quote 안 들어옴 — 부팅 중일 수 있음
                    age = now - last_ts
                    if age > threshold:
                        stale_legs.append((pair_id, leg, age))

            # fresh로 복귀한 leg는 cooldown 해제
            still_stale = {(p, l) for p, l, _ in stale_legs}
            recovered = alerted - still_stale
            for r in recovered:
                rec_pair = self._registered_pairs.get(r[0])
                rec_cfg = rec_pair.leg(r[1]) if rec_pair else None
                rec_exch = rec_cfg.exchange if rec_cfg else "?"
                rec_sym = rec_cfg.symbol if rec_cfg else "?"
                logger.info(
                    f"[WS_WATCHDOG] recovered {r[0]}/{r[1]} ({rec_exch}/{rec_sym})"
                )
                if self._notifier is not None:
                    try:
                        self._notifier.send_sync(
                            f"✅ <b>QUOTE RECOVERED [{r[0]}]</b>\n"
                            f"leg_{r[1]}: {rec_exch}/{rec_sym}"
                        )
                    except Exception:
                        pass
            alerted -= recovered

            new_alerts = still_stale - alerted
            if new_alerts:
                pairs_with_open_pos = [
                    p for p in self._open_trades_by_pair.keys()
                ]
                for pair_id, leg in new_alerts:
                    age = next(a for p, l, a in stale_legs if p == pair_id and l == leg)
                    pair = self._registered_pairs.get(pair_id)
                    # 어느 거래소/심볼인지 명시 (leg_a/leg_b 모호함 해소)
                    leg_cfg = pair.leg(leg) if pair else None
                    exch = leg_cfg.exchange if leg_cfg else "?"
                    sym = leg_cfg.symbol if leg_cfg else "?"
                    role = leg_cfg.role.value if leg_cfg else "?"
                    # 마지막 fresh quote 시각 (절대) — KST 변환 포함
                    last_ts = self._last_quote_ts.get((pair_id, leg))
                    if last_ts:
                        from datetime import datetime, timezone, timedelta
                        last_kst = datetime.fromtimestamp(
                            last_ts, tz=timezone(timedelta(hours=9))
                        ).strftime("%H:%M:%S KST")
                    else:
                        last_kst = "never"
                    logger.error(
                        f"[WS_WATCHDOG] STALE {pair_id}/{leg} ({exch}/{sym}) "
                        f"age={age:.0f}s last={last_kst} > threshold={threshold:.0f}s"
                    )
                    # 데이터 채널 명시 — 시세 WS (주문 채널과 별개)
                    channel_hint = ""
                    if exch == "kis":
                        channel_hint = " (KIS 시세 WS ws://ops.koreainvestment.com:21000)"
                    elif exch == "hyperliquid":
                        channel_hint = " (HL 시세 WS wss://api.hyperliquid.xyz/ws)"
                    elif exch == "lighter":
                        channel_hint = " (Lighter WS)"
                    # leg_b가 KIS dated_futures인 페어에서 stale이면 시세 라이센스
                    # 만료 의심 — 메시지에 명시.
                    license_hint = ""
                    if (leg == "b" and pair is not None
                            and pair.leg_b.role == LegRole.DATED_FUTURES
                            and pair.leg_b.exchange == "kis"):
                        license_hint = (
                            "\n\n⚠️ KIS 시세 라이센스 만료 가능성 — "
                            "apiportal.koreainvestment.com 결제 확인. "
                            "또는 .venv/bin/python scripts/diagnose_kis_feed.py 검증.\n"
                            "(라이센스 OK면 다른 봇과 app_key 동시 WS 세션 충돌 의심)"
                        )
                    if self._notifier is not None:
                        try:
                            self._notifier.send_sync(
                                f"🚨 <b>QUOTE STALE [{pair_id}]</b>\n"
                                f"leg_{leg}: <b>{exch}/{sym}</b> ({role}){channel_hint}\n"
                                f"age={age:.0f}s (threshold {threshold:.0f}s)\n"
                                f"last quote: {last_kst}\n"
                                f"open_positions={len(pairs_with_open_pos)} "
                                f"auto_flatten={auto_flatten}"
                                f"{license_hint}"
                            )
                        except Exception as e:
                            logger.error(f"notifier ws-stale error: {e}")
                alerted |= new_alerts

                # auto-flatten — stale leg가 어떤 페어든 1개라도 있고
                # 오픈 포지션이 있으면 즉시 모두 청산 시도.
                if auto_flatten and pairs_with_open_pos:
                    logger.error(
                        f"[WS_WATCHDOG] AUTO-FLATTEN {len(pairs_with_open_pos)} "
                        f"open positions due to stale WS"
                    )
                    for pid in pairs_with_open_pos:
                        try:
                            await self.emergency_flatten_pair(pid, reason="ws_stale")
                        except Exception as e:
                            logger.error(
                                f"[WS_WATCHDOG] flatten {pid} raised: {e}"
                            )

    async def contract_alignment_monitor_loop(
        self,
        interval_seconds: int = 60,
        stop_event: Optional[asyncio.Event] = None,
    ) -> None:
        """HL oracle index ↔ KIS mid 일치성 모니터.

        |diff_bps| > contract_alignment_max_bps 임계 초과 시:
          1. notifier로 ALIGNMENT 경보 (한 번만, recovery 시 해제)
          2. contract_alignment_auto_flatten=True면 dated_futures 페어 flatten

        dated_futures leg를 가진 페어만 의미 있음 (Web3-Web3는 양 leg 모두 perp).
        """
        threshold = float(self.config.risk.contract_alignment_max_bps or 50.0)
        auto_flatten = bool(self.config.risk.contract_alignment_auto_flatten)
        logger.info(
            f"[ALIGNMENT] monitor started threshold={threshold}bp "
            f"auto_flatten={auto_flatten} interval={interval_seconds}s"
        )
        alerted: set[str] = set()
        while True:
            if stop_event is not None and stop_event.is_set():
                logger.info("[ALIGNMENT] monitor stopped")
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

            for pair_id, pair in self._registered_pairs.items():
                if pair.leg_b.role != LegRole.DATED_FUTURES:
                    continue
                diff = self.check_contract_alignment(pair_id)
                if diff is None:
                    continue
                if diff > threshold:
                    if pair_id not in alerted:
                        logger.error(
                            f"[ALIGNMENT] {pair_id} mismatch |diff|={diff:.1f}bp "
                            f"> threshold={threshold:.1f}bp"
                        )
                        if self._notifier is not None:
                            try:
                                self._notifier.send_sync(
                                    f"⚠️ <b>CONTRACT ALIGNMENT [{pair_id}]</b>\n"
                                    f"|HL_index − KIS_mid| = {diff:.1f}bp "
                                    f"(threshold {threshold:.0f}bp)\n"
                                    f"auto_flatten={auto_flatten}"
                                )
                            except Exception:
                                pass
                        alerted.add(pair_id)
                        if auto_flatten and pair_id in self._open_trades_by_pair:
                            try:
                                await self.emergency_flatten_pair(
                                    pair_id, reason=f"alignment_{diff:.0f}bp"
                                )
                            except Exception as e:
                                logger.error(
                                    f"[ALIGNMENT] flatten {pair_id} raised: {e}"
                                )
                else:
                    if pair_id in alerted:
                        logger.info(
                            f"[ALIGNMENT] {pair_id} recovered |diff|={diff:.1f}bp"
                        )
                        if self._notifier is not None:
                            try:
                                self._notifier.send_sync(
                                    f"✅ <b>ALIGNMENT RECOVERED [{pair_id}]</b> "
                                    f"|diff|={diff:.1f}bp"
                                )
                            except Exception:
                                pass
                        alerted.discard(pair_id)

    async def rollover_blackout_check(self) -> int:
        """롤오버 blackout 상태 전이 감지 + dated_futures 페어 flatten.

        rollover_watch_loop가 매시간 호출. 알림은 상태 전이 시 1회만:
          - OFF → ON: '📅 BLACKOUT ENTERED' (+ flatten 결과)
          - ON → OFF: '✅ BLACKOUT CLEARED'

        Returns: flattened pair 수 (이번 호출에서).
        """
        is_blackout = self.risk_mgr.is_rollover_blackout()
        announced = self._rollover_blackout_announced

        # 상태 전이: OFF → ON
        if is_blackout and not announced:
            n = 0
            for pair_id, pair in list(self._registered_pairs.items()):
                if pair.leg_b.role != LegRole.DATED_FUTURES:
                    continue
                if pair_id not in self._open_trades_by_pair:
                    continue
                try:
                    ok = await self.emergency_flatten_pair(
                        pair_id, reason="rollover_blackout",
                    )
                    if ok:
                        n += 1
                except Exception as e:
                    logger.error(f"[{pair_id}] rollover blackout flatten raised: {e}")
            self._rollover_blackout_announced = True
            if self._notifier is not None:
                bd = self.risk_mgr._business_day()
                divergence_day = self.config.risk.rollover_start_day + 1
                end_day = self.config.risk.rollover_end_day
                try:
                    self._notifier.send_sync(
                        f"📅 <b>ROLLOVER BLACKOUT ENTERED</b>\n"
                        f"BD={bd} (divergence first day BD {divergence_day}, "
                        f"clears after BD {end_day})\n"
                        f"flattened {n} dated_futures pair(s); "
                        f"new entries blocked until end."
                    )
                except Exception:
                    pass
            return n

        # 상태 전이: ON → OFF
        if not is_blackout and announced:
            self._rollover_blackout_announced = False
            if self._notifier is not None:
                try:
                    self._notifier.send_sync(
                        f"✅ <b>ROLLOVER BLACKOUT CLEARED</b>\n"
                        f"BD={self.risk_mgr._business_day()} → entries re-enabled"
                    )
                except Exception:
                    pass

        # 같은 상태 유지 시 — 추가 알림 X. blackout 중에도 어딘가에서 새 포지션이
        # 생겼다면 (예: 사용자 수동 진입) flatten 시도하되 알림은 안 보냄.
        if is_blackout:
            n = 0
            for pair_id, pair in list(self._registered_pairs.items()):
                if pair.leg_b.role != LegRole.DATED_FUTURES:
                    continue
                if pair_id not in self._open_trades_by_pair:
                    continue
                try:
                    ok = await self.emergency_flatten_pair(
                        pair_id, reason="rollover_blackout",
                    )
                    if ok:
                        n += 1
                except Exception as e:
                    logger.error(f"[{pair_id}] rollover blackout flatten raised: {e}")
            return n
        return 0

    def check_contract_alignment(self, pair_id: str) -> Optional[float]:
        """HL oracle index_price ↔ KIS mid_price 추종 일치성 체크.

        Returns: |diff_bps| (둘 다 양수일 때) 또는 None (정보 부족).
        대시보드/감사 로그용 — 임계 초과 시 알림 + (옵션) flatten.
        """
        leg_a = self._latest_pair_quote.get((pair_id, "a"))
        leg_b = self._latest_pair_quote.get((pair_id, "b"))
        if leg_a is None or leg_b is None:
            return None
        # leg_a (HL perp): index_price = HL oracle (CME 근월물 기반)
        # leg_b (KIS futures): mid_price = 실제 KIS 호가 mid
        ref = leg_a.index_price
        actual = leg_b.mid_price
        if ref <= 0 or actual <= 0:
            return None
        return abs(ref - actual) / actual * 10_000

    async def emergency_flatten_pair(self, pair_id: str, reason: str = "manual") -> bool:
        """Phase 11d — 지정 페어의 오픈 포지션 즉시 양 leg 반대 fill로 청산.

        WS-stale 워치독 / 수동 호출에서 사용. 주문은 reduce_only=True 시장가.
        LIVE 모드 전용 (PAPER에서는 dispatch_pair_order 자체가 시뮬용 경로 X).

        Returns: True if 양 leg flatten 성공 (best-effort).
        """
        if (self.config.mode or "").upper() != "LIVE":
            return False
        trade = self._open_trades_by_pair.get(pair_id)
        if not trade:
            return False
        pair = self._registered_pairs.get(pair_id)
        if pair is None or self._registry is None:
            logger.error(f"[{pair_id}] flatten failed: pair/registry missing")
            return False

        # 진입 방향과 반대로 청산 (entry: short_basis = leg_a buy + leg_b sell)
        is_long_basis = trade.direction == "long_basis"
        # long_basis 진입: leg_a sell, leg_b buy → 청산: leg_a buy, leg_b sell
        # short_basis 진입: leg_a buy, leg_b sell → 청산: leg_a sell, leg_b buy
        leg_a_close_side = "buy" if is_long_basis else "sell"
        leg_b_close_side = "sell" if is_long_basis else "buy"
        leg_a_size = trade.perp_units
        leg_b_size = float(trade.size_contracts)

        logger.error(
            f"[{pair_id}] EMERGENCY FLATTEN ({reason}) "
            f"leg_a {leg_a_close_side} {leg_a_size} | "
            f"leg_b {leg_b_close_side} {leg_b_size}"
        )

        async def _close_a():
            return await self.dispatch_pair_order(
                pair_id=pair_id, leg="a", side=leg_a_close_side,
                size=leg_a_size, order_type="market", reduce_only=True,
            )

        async def _close_b():
            return await self.dispatch_pair_order(
                pair_id=pair_id, leg="b", side=leg_b_close_side,
                size=leg_b_size, order_type="market", reduce_only=True,
            )

        ra, rb = await asyncio.gather(_close_a(), _close_b(), return_exceptions=True)
        ok_a = not isinstance(ra, Exception) and getattr(ra, "success", False)
        ok_b = not isinstance(rb, Exception) and getattr(rb, "success", False)

        if self._notifier is not None:
            try:
                self._notifier.send_sync(
                    f"🛑 <b>EMERGENCY FLATTEN [{pair_id}]</b>\n"
                    f"reason: {reason}\n"
                    f"leg_a={'OK' if ok_a else 'FAIL'} | leg_b={'OK' if ok_b else 'FAIL'}"
                )
            except Exception:
                pass

        # 성공한 leg는 open_trades에서 제거 (한쪽이라도 실패하면 남겨두고 수동 개입)
        if ok_a and ok_b:
            self._open_trades_by_pair.pop(pair_id, None)
            self._state.open_positions = max(0, self._state.open_positions - 1)
        return ok_a and ok_b

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
        # 페어별 entry 락 lazy-create
        if pair.id not in self._pair_entry_locks:
            self._pair_entry_locks[pair.id] = asyncio.Lock()
        if pair.id not in self._pair_exit_locks:
            self._pair_exit_locks[pair.id] = asyncio.Lock()
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
        # WS freshness 워치독용 (Phase 11d)
        self._last_quote_ts[(pair_id, leg)] = time.time()

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
        sizing은 pair.leg_b.contract_size 직접 참조.

        Lock: 같은 페어의 entry는 한 번에 하나만 in-flight. asyncio.gather가
        1-2초 걸리는 사이 새 basis update가 또 entry 호출하면 같은 신호로
        다중 발사되어 KIS 초당 거래 한도 위반.
        """
        if pair_id in self._open_trades_by_pair:
            return

        lock = self._pair_entry_locks.get(pair_id)
        if lock is None:
            # 등록 안 된 페어 — race or programming error. 보수적으로 skip.
            logger.warning(f"[{pair_id}] entry attempted on unregistered pair")
            return

        if lock.locked():
            # 이미 entry in-flight — silent skip (logger only, no notify spam).
            logger.debug(f"[{pair_id}] entry already in-flight, skip")
            return

        async with lock:
            await self._do_pair_entry(pair_id, signal, leg_a, leg_b)

    async def _do_pair_entry(
        self, pair_id: str, signal: Signal, leg_a: Quote, leg_b: Quote
    ) -> None:
        """실제 entry 로직 — lock 안에서 호출."""
        # lock 획득 후 다시 한 번 open 체크 (이전 await 사이에 다른 entry가 끝났을 수 있음)
        if pair_id in self._open_trades_by_pair:
            return

        pair = self._registered_pairs[pair_id]

        # Shadow mode 가드 — pair.enabled=False면 진입 차단 (basis stats만 누적).
        # 신규 거래소 합류 시 분포 검증 단계용.
        if not pair.enabled:
            return

        # Rollover blackout 가드 — 롤 window 시작 N영업일 전부터 진입 차단.
        # KIS leg가 dated_futures인 페어에만 적용 (Web3-Web3 페어는 무관).
        # 알림은 상태 전이 시 (rollover_blackout_check) 1회만 — 매 신호마다
        # 알림 spam 방지. 여기서는 logger.warning만.
        if pair.leg_b.role == LegRole.DATED_FUTURES and self.risk_mgr.is_rollover_blackout():
            self._state.rejected_by_risk += 1
            divergence_day = self.config.risk.rollover_start_day + 1
            blackout_from = divergence_day - self.config.risk.rollover_block_entry_days
            logger.warning(
                f"[{pair_id}] ENTRY_BLOCKED rollover_blackout — "
                f"BD={self.risk_mgr._business_day()} "
                f"(blackout BD {blackout_from}–{self.config.risk.rollover_end_day}, "
                f"divergence first day = BD {divergence_day})"
            )
            return

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

        # 5/18 spike-chasing 가드 — signal vs exec slip cap.
        # latency가 spike를 잡아먹어 exec가 의도와 다르면 진입 skip.
        # signal +25 vs exec -30 같은 부호 반대 케이스는 |slip|이 매우 커서 자동 차단.
        max_slip = self.config.strategy.max_entry_slip_bps
        slip_bps = signal.basis_bps - exec_basis
        if max_slip > 0 and abs(slip_bps) > max_slip:
            self._state.entry_exec_filter_skip += 1
            logger.warning(
                f"[{pair_id}] ENTRY_SKIP slip cap: |slip|={abs(slip_bps):.1f}bp "
                f"> {max_slip:.1f}bp (signal={signal.basis_bps:+.1f}, exec={exec_basis:+.1f}) "
                f"— spike 사라진 후 fill 시도, 진입 무의미"
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
            # 일일 손실 제한처럼 거래 자체를 막는 사유는 LIVE에서 즉시 알림.
            if self._notifier is not None and "daily loss" in risk_check.reason.lower():
                try:
                    self._notifier.send_sync(
                        f"⛔ <b>RISK BLOCK [{pair_id}]</b>\n"
                        f"<code>{risk_check.reason}</code>"
                    )
                except Exception as e:
                    logger.error(f"notifier risk-block error: {e}")
            return

        # ── 양 leg 동시 발주 (paper: kiwoom for KIS, simulate for perp) ──
        leg_a_side = "sell" if direction == "long_basis" else "buy"
        leg_b_side = "buy" if direction == "long_basis" else "sell"

        # signal_ts = signal 도착 시점 (entry attempt 시작 직전). 양 leg
        # gather 직전에 기록해서 fill 후 latency 측정.
        signal_ts_value = time.time()

        async def _fill_a_timed() -> tuple[float, str, float]:
            t0 = time.time()
            price, oid = await self._fill_pair_leg(pair, "a", leg_a_side, leg_a_size, leg_a)
            return price, oid, (time.time() - t0) * 1000   # ms

        async def _fill_b_timed() -> tuple[float, str, float]:
            t0 = time.time()
            price, oid = await self._fill_pair_leg(pair, "b", leg_b_side, leg_b_size, leg_b)
            return price, oid, (time.time() - t0) * 1000

        (a_price, a_oid, a_latency_ms), (b_price, b_oid, b_latency_ms) = \
            await asyncio.gather(_fill_a_timed(), _fill_b_timed())
        if a_price <= 0 or b_price <= 0:
            self._state.failed_orders += 1
            logger.error(
                f"[{pair_id}] one or both legs failed to fill "
                f"(a={a_price} oid={a_oid}, b={b_price} oid={b_oid}); skipping entry"
            )
            # LIVE 모드: 한쪽만 체결됐으면 즉시 반대 fill로 unwind (단방향 노출 방지)
            if (self.config.mode or "").upper() == "LIVE":
                if self._notifier is not None:
                    try:
                        self._notifier.send_sync(
                            f"⚠️ <b>FILL FAIL [{pair_id}]</b>\n"
                            f"leg_a={a_price:.2f} oid={a_oid or 'NONE'}\n"
                            f"leg_b={b_price:.2f} oid={b_oid or 'NONE'}\n"
                            f"emergency unwind triggered"
                        )
                    except Exception as e:
                        logger.error(f"notifier fill-fail error: {e}")
                await self._emergency_unwind_partial_entry(
                    pair, leg_a_side, leg_a_size, a_price, a_oid,
                    leg_b_side, leg_b_size, b_price, b_oid,
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
        # 진단 metric — v5 schema (signal vs exec, latency)
        exec_basis_bps = (
            (a_price - b_price) / b_price * 10_000 if b_price else 0.0
        )
        self.storage.save_position(
            product=legacy_product,
            perp_size=leg_a_size if trade.perp_side == "long" else -leg_a_size,
            perp_entry=a_price,
            futures_size=leg_b_size if trade.futures_side == "long" else -leg_b_size,
            futures_entry=b_price,
            pair_id=pair_id,
            signal_basis_bps=signal.basis_bps,
            exec_basis_bps=exec_basis_bps,
            entry_latency_ms_a=a_latency_ms,
            entry_latency_ms_b=b_latency_ms,
            signal_ts=signal_ts_value,
        )

        slip_bps = signal.basis_bps - exec_basis_bps
        max_latency = max(a_latency_ms, b_latency_ms)
        logger.info(
            f"[{pair_id}] ▶ ENTRY {direction} | "
            f"signal={signal.basis_bps:+.1f}bp exec={exec_basis_bps:+.1f}bp "
            f"slip={slip_bps:+.1f}bp latency={max_latency:.0f}ms | "
            f"leg_a {leg_a_side} {leg_a_size}@{a_price:.2f} ({a_latency_ms:.0f}ms) | "
            f"leg_b {leg_b_side} {leg_b_size}@{b_price:.2f} ({b_latency_ms:.0f}ms)"
        )
        for cb in self._on_trade_callbacks:
            try:
                cb(trade, "open")
            except Exception as e:
                logger.error(f"Trade callback error: {e}")

    async def _handle_pair_exit(
        self, pair_id: str, signal: Signal, leg_a: Quote, leg_b: Quote
    ) -> None:
        """청산 처리 (pair-keyed).

        Lock + cooldown — 5/18 07:58 incident에서 EXIT fail 후 매 quote tick
        마다 EXIT 재시도 → 1초 안 수십 번 KIS sell 호출 → 이미 청산된 포지션에
        '주문가능금액 부족' reject 폭주. Lock으로 동시 호출 차단 + 마지막 시도
        후 5s 이내 재시도 skip (KIS rate limit + busy-loop 방지).
        """
        trade = self._open_trades_by_pair.get(pair_id)
        if not trade:
            return   # 이미 청산됨 (race) — silent

        # Lock 잡혀있으면 silent skip (logger debug만, no notify spam)
        lock = self._pair_exit_locks.get(pair_id)
        if lock is None or lock.locked():
            return

        # Cooldown — 5초 안에 EXIT 시도 있었으면 skip (KIS rate limit 회피)
        now = time.time()
        last = self._last_exit_attempt_ts.get(pair_id, 0)
        if now - last < 5.0:
            return

        async with lock:
            self._last_exit_attempt_ts[pair_id] = time.time()
            # lock 획득 후 다시 trade 체크 (await 사이 다른 호출이 끝났을 수 있음)
            trade = self._open_trades_by_pair.get(pair_id)
            if not trade:
                return
            await self._do_pair_exit(pair_id, signal, leg_a, leg_b, trade)

    async def _do_pair_exit(
        self, pair_id: str, signal: Signal, leg_a: Quote, leg_b: Quote, trade,
    ) -> None:
        """실제 청산 로직 — lock 안에서 호출."""
        # 5/18 spike-chasing 가드 — min_hold_seconds 미만이면 즉시 exit 차단.
        # 진입 후 즉시 청산되면 수수료만 손실 (5/18 hold 0초 거래 5건 모두 손실).
        # 단 EMERGENCY_CLOSE 같은 안전 청산은 별도 path (이 함수 통과 X).
        min_hold = self.config.strategy.min_hold_seconds
        if min_hold > 0:
            elapsed = time.time() - trade.entry_time
            if elapsed < min_hold:
                logger.debug(
                    f"[{pair_id}] EXIT skip: hold={elapsed:.0f}s < min_hold={min_hold}s "
                    f"(spike-chasing 방지)"
                )
                return

        pair = self._registered_pairs[pair_id]
        leg_a_close_side = "buy" if trade.perp_side == "short" else "sell"
        leg_b_close_side = "sell" if trade.futures_side == "long" else "buy"

        leg_a_size = trade.perp_units
        leg_b_size = float(trade.size_contracts)

        # EXIT는 반드시 reduce_only=True — 5/18 incident: retry 시 HL이
        # reverse position 누적해서 978 배럴 의도치 않은 long 발생. 반드시 명시.
        async def _fill_a():
            return await self._fill_pair_leg(
                pair, "a", leg_a_close_side, leg_a_size, leg_a, reduce_only=True,
            )

        async def _fill_b():
            return await self._fill_pair_leg(
                pair, "b", leg_b_close_side, leg_b_size, leg_b, reduce_only=True,
            )

        (a_exit, _), (b_exit, b_exit_oid) = await asyncio.gather(_fill_a(), _fill_b())
        if a_exit <= 0 or b_exit <= 0:
            self._state.failed_orders += 1
            logger.error(
                f"[{pair_id}] EXIT fill failed (a={a_exit}, b={b_exit}); "
                f"retry in 5s cooldown"
            )
            # KIS가 '주문가능금액 부족' reject → 이미 청산됐을 가능성. notifier 알림.
            if self._notifier is not None:
                try:
                    self._notifier.send_sync(
                        f"⚠️ <b>EXIT FAIL [{pair_id}]</b>\n"
                        f"leg_a={a_exit:.2f} leg_b={b_exit:.2f}\n"
                        f"5초 cooldown 후 재시도 — KIS HTS에서 실제 포지션 직접 확인 권장"
                    )
                except Exception:
                    pass
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
        reduce_only: bool = False,
    ) -> tuple[float, str]:
        """페어 leg에 fill 발생시킴. (filled_price, order_id) 반환.

        reduce_only — EXIT/unwind 경로에서 반드시 True. 5/18 incident에서
        EXIT retry가 reduce_only=False로 발사되어 HL이 reverse position 누적
        (978 배럴) → critical bug. ENTRY는 False (새 position 생성 의도).
        """
        leg_cfg = pair.leg(leg)
        is_live = (self.config.mode or "").upper() == "LIVE"

        # ── LIVE 모드: 실 어댑터 dispatch ──
        if is_live:
            t0 = time.time()
            try:
                result = await self.dispatch_pair_order(
                    pair_id=pair.id, leg=leg, side=side, size=size,
                    order_type="market", reduce_only=reduce_only,
                )
            except Exception as e:
                latency_ms = (time.time() - t0) * 1000
                logger.error(
                    f"[{pair.id}/{leg}] LIVE dispatch raised: {e} "
                    f"latency={latency_ms:.0f}ms"
                )
                return 0.0, ""
            if not result.success:
                latency_ms = (time.time() - t0) * 1000
                logger.error(
                    f"[{pair.id}/{leg}] LIVE order FAILED on {leg_cfg.exchange}: "
                    f"{result.error} latency={latency_ms:.0f}ms"
                )
                return 0.0, ""
            # filled_price가 0이면(미체결 resting / KIS 시장가 응답 누락) leg_quote 추정 사용
            filled_price = result.filled_price
            if filled_price <= 0:
                if side == "buy":
                    filled_price = leg_quote.ask or leg_quote.mid_price
                else:
                    filled_price = leg_quote.bid or leg_quote.mid_price
            latency_ms = (time.time() - t0) * 1000
            logger.warning(
                f"[{pair.id}/{leg}] LIVE FILL {leg_cfg.exchange}/{leg_cfg.symbol} "
                f"{side} {size} @ {filled_price:.4f} order_id={result.order_id} "
                f"latency={latency_ms:.0f}ms"
            )
            return filled_price, result.order_id

        # ── PAPER 모드: 시뮬 fill ──
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

    async def _emergency_unwind_partial_entry(
        self,
        pair: ArbitragePair,
        leg_a_side: str, leg_a_size: float, a_price: float, a_oid: str,
        leg_b_side: str, leg_b_size: float, b_price: float, b_oid: str,
    ) -> None:
        """LIVE 모드 한쪽 leg only 체결 시 반대 fill로 즉시 unwind.

        leg_a 체결됨 + leg_b 실패 → leg_a 반대 방향으로 시장가 close.
        그 반대도 동일. 중복 unwind 방지를 위해 필요한 leg만 호출.
        """
        if a_price > 0 and b_price <= 0:
            # leg_b 실패. leg_a를 반대 방향으로 close.
            opp_side = "sell" if leg_a_side == "buy" else "buy"
            logger.warning(
                f"[{pair.id}] EMERGENCY_UNWIND leg_a START "
                f"{leg_a_side}→{opp_side} size={leg_a_size} (filled @ {a_price})"
            )
            try:
                r = await self.dispatch_pair_order(
                    pair_id=pair.id, leg="a", side=opp_side, size=leg_a_size,
                    order_type="market", reduce_only=True,
                )
                if r.success:
                    msg = (f"[{pair.id}] EMERGENCY_UNWIND leg_a OK "
                           f"oid={r.order_id} filled @ {r.filled_price}")
                    logger.warning(msg)
                    if self._notifier is not None:
                        try:
                            self._notifier.send_sync(
                                f"🛟 <b>UNWIND OK [{pair.id}]</b>\n"
                                f"leg_a {leg_a_side}@{a_price} → "
                                f"{opp_side}@{r.filled_price} (reduce_only)"
                            )
                        except Exception: pass
                else:
                    msg = (f"[{pair.id}] EMERGENCY_UNWIND leg_a FAILED: {r.error} "
                           f"— MANUAL INTERVENTION REQUIRED (open leg_a oid={a_oid} "
                           f"side={leg_a_side} size={leg_a_size} @ {a_price})")
                    logger.error(msg)
                    if self._notifier is not None:
                        try:
                            self._notifier.send_sync(
                                f"🚨 <b>UNWIND FAILED [{pair.id}]</b>\n"
                                f"<b>MANUAL INTERVENTION REQUIRED</b>\n"
                                f"open leg_a oid={a_oid} {leg_a_side} "
                                f"size={leg_a_size} @ {a_price}\n"
                                f"error: <code>{(r.error or '?')[:200]}</code>"
                            )
                        except Exception: pass
            except Exception as e:
                logger.error(f"[{pair.id}] EMERGENCY_UNWIND leg_a raised: {e}")
                if self._notifier is not None:
                    try:
                        self._notifier.send_sync(
                            f"🚨 <b>UNWIND EXCEPTION [{pair.id}]</b>\n"
                            f"open leg_a oid={a_oid} {leg_a_side} "
                            f"size={leg_a_size} @ {a_price}\n"
                            f"exception: <code>{str(e)[:200]}</code>\n"
                            f"<b>MANUAL INTERVENTION REQUIRED</b>"
                        )
                    except Exception: pass
        elif b_price > 0 and a_price <= 0:
            opp_side = "sell" if leg_b_side == "buy" else "buy"
            logger.warning(
                f"[{pair.id}] EMERGENCY_UNWIND leg_b START "
                f"{leg_b_side}→{opp_side} size={leg_b_size} (filled @ {b_price})"
            )
            try:
                r = await self.dispatch_pair_order(
                    pair_id=pair.id, leg="b", side=opp_side, size=leg_b_size,
                    order_type="market", reduce_only=True,
                )
                if r.success:
                    logger.warning(
                        f"[{pair.id}] EMERGENCY_UNWIND leg_b OK "
                        f"oid={r.order_id} filled @ {r.filled_price}"
                    )
                    if self._notifier is not None:
                        try:
                            self._notifier.send_sync(
                                f"🛟 <b>UNWIND OK [{pair.id}]</b>\n"
                                f"leg_b {leg_b_side}@{b_price} → "
                                f"{opp_side}@{r.filled_price} (reduce_only)"
                            )
                        except Exception: pass
                else:
                    logger.error(
                        f"[{pair.id}] EMERGENCY_UNWIND leg_b FAILED: {r.error} "
                        f"— MANUAL INTERVENTION REQUIRED (open leg_b oid={b_oid} "
                        f"side={leg_b_side} size={leg_b_size} @ {b_price})"
                    )
                    if self._notifier is not None:
                        try:
                            self._notifier.send_sync(
                                f"🚨 <b>UNWIND FAILED [{pair.id}]</b>\n"
                                f"<b>MANUAL INTERVENTION REQUIRED</b>\n"
                                f"open leg_b oid={b_oid} {leg_b_side} "
                                f"size={leg_b_size} @ {b_price}\n"
                                f"error: <code>{(r.error or '?')[:200]}</code>"
                            )
                        except Exception: pass
            except Exception as e:
                logger.error(f"[{pair.id}] EMERGENCY_UNWIND leg_b raised: {e}")
                if self._notifier is not None:
                    try:
                        self._notifier.send_sync(
                            f"🚨 <b>UNWIND EXCEPTION [{pair.id}]</b>\n"
                            f"open leg_b oid={b_oid} {leg_b_side} "
                            f"size={leg_b_size} @ {b_price}\n"
                            f"exception: <code>{str(e)[:200]}</code>\n"
                            f"<b>MANUAL INTERVENTION REQUIRED</b>"
                        )
                    except Exception: pass
        # 둘 다 실패 → unwind 불필요

    def _calculate_pair_contracts(
        self, pair: ArbitragePair, leg_b_price: float
    ) -> int:
        """페어 leg_b 기준 계약수 결정.

        max_position_usd cap + max_position_contracts cap + per-pair LIVE cap 적용.
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
        cap = min(max_contracts, self.config.risk.max_position_contracts)
        # Phase 11d — LIVE 모드 페어별 hard cap (settings.yaml에서 명시).
        # PAPER 모드에서는 적용 안 함 (시뮬 그대로 돌리기 위해).
        if (self.config.mode or "").upper() == "LIVE":
            per_pair_cap = self.config.risk.live_max_contracts_per_pair.get(pair.id)
            if per_pair_cap is not None and per_pair_cap > 0:
                cap = min(cap, int(per_pair_cap))
        return max(1, cap)

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
