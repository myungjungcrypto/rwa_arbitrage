from __future__ import annotations
"""설정 관리 모듈."""


import yaml
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ProductConfig:
    perp_ticker: str
    futures_symbol: str
    contract_size: int = 1             # 배럴/CME계약 (MCL=100, BZ=1000)
    min_order_size: int = 1
    futures_fee_per_contract: float = 7.5  # CME 편도 수수료 ($/계약)


@dataclass
class HyperliquidConfig:
    use_testnet: bool = False       # trade.xyz HIP-3 퍼프는 메인넷에만 존재
    perp_dex: str = "xyz"           # trade.xyz HIP-3 perp DEX 이름
    ws_reconnect_delay: int = 5
    ws_ping_interval: int = 30
    wallet_address: str = ""
    private_key: str = ""


@dataclass
class KiwoomConfig:
    use_mock: bool = True
    server_type: str = "mock"
    account_number: str = ""
    account_password: str = ""


@dataclass
class KISConfig:
    """KIS (한국투자증권) API 설정."""
    enabled: bool = False
    app_key: str = ""
    app_secret: str = ""
    account_number: str = ""
    base_url: str = "https://openapi.koreainvestment.com:9443"
    ws_url: str = "ws://ops.koreainvestment.com:21000"
    is_paper: bool = False       # 모의투자 여부
    cme_realtime: bool = False   # CME 유료시세 신청 여부


@dataclass
class StrategyConfig:
    basis_window_hours: int = 24
    basis_std_multiplier: float = 2.0
    entry_threshold_bps: float = 50
    exit_threshold_bps: float = 10         # deprecated (backward compat)
    target_profit_bps: float = 30          # deprecated
    convergence_target_bps: float = 3.0    # spread ≤ 이 값이면 수렴 완료 → 청산
    max_hold_hours: int = 48
    funding_rate_weight: float = 1.0
    min_funding_advantage_bps: float = 5

    # 절대값 진입 floor — exec basis가 이 값 이상이어야 진입 (statistical band과 무관)
    # 30건 표본 분석(2026-04-21~04-27): <10bp 진입 14/14건 14% 승률 -$202,
    # 10bp+ 진입 16/16건 94% 승률 +$199. 따라서 절대 floor 필수.
    # 0 = 비활성화 (backward compat).
    min_abs_entry_bps: float = 0.0

    # CME 장 시간 가드 — 폐장 중 진입 차단 + 장기 휴장 전 flatten
    cme_closed_skip_entry: bool = True
    pre_close_flatten_minutes: int = 30     # 마감 몇 분 전부터 진입 차단 + 청산 시작
    flatten_threshold_hours: float = 4.0    # 다가오는 휴장이 이 시간 이상이면 flatten


@dataclass
class RiskConfig:
    max_position_usd: float = 50000
    max_position_contracts: int = 10
    max_margin_usage_pct: float = 50
    max_slippage_bps: float = 20
    max_daily_loss_usd: float = 2000
    emergency_close_threshold: float = 100
    rollover_start_day: int = 5
    rollover_end_day: int = 10
    rollover_position_reduce_pct: float = 50


@dataclass
class AppConfig:
    mode: str = "PAPER"
    products: dict[str, ProductConfig] = field(default_factory=dict)
    kis_symbol_map: dict[str, str] = field(default_factory=dict)  # product → KIS contract symbol
    hyperliquid: HyperliquidConfig = field(default_factory=HyperliquidConfig)
    kiwoom: KiwoomConfig = field(default_factory=KiwoomConfig)
    kis: KISConfig = field(default_factory=KISConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    db_path: str = "data/arbitrage.db"
    log_level: str = "INFO"
    log_file: str = "logs/arbitrage.log"

    def get_pairs(self) -> "list":
        """레거시 products+kis_symbol_map 구성에서 ArbitragePair 리스트 합성.

        Phase C 멀티 페어 인프라가 사용. 추후 settings.yaml에 명시적 `pairs:`
        블록이 추가되면 그것을 우선시 (현재는 합성만).

        매핑 규칙 (legacy → pair):
          product='wti'   → pair_id='wti_cme_hl'
                            leg_a = HL  xyz:CL  perp
                            leg_b = KIS MCLM26  dated_futures (kis_symbol_map['wti'])
          product='brent' → pair_id='brent_cme_hl' (brent 운영 시작 후)
        """
        # 늦은 import — config.py가 strategy 모듈에 의존하지 않게
        from src.strategy.pair import (
            ArbitragePair, ExchangeLeg, LegRole, PairGate, PairStrategyParams,
        )

        params = PairStrategyParams(
            basis_window_hours=self.strategy.basis_window_hours,
            basis_std_multiplier=self.strategy.basis_std_multiplier,
            entry_threshold_bps=self.strategy.entry_threshold_bps,
            convergence_target_bps=self.strategy.convergence_target_bps,
            max_hold_hours=self.strategy.max_hold_hours,
            min_funding_advantage_bps=self.strategy.min_funding_advantage_bps,
            funding_rate_weight=self.strategy.funding_rate_weight,
            emergency_close_bps=self.risk.emergency_close_threshold,
            pre_close_flatten_minutes=self.strategy.pre_close_flatten_minutes,
            flatten_threshold_hours=self.strategy.flatten_threshold_hours,
        )

        pairs: list[ArbitragePair] = []
        for product_key, prod in self.products.items():
            kis_symbol = self.kis_symbol_map.get(product_key, prod.futures_symbol)
            pair_id = f"{product_key}_cme_hl"
            pair = ArbitragePair(
                id=pair_id,
                enabled=True,
                strategy="basis_convergence",
                gate=PairGate.CME_HOURS,
                leg_a=ExchangeLeg(
                    exchange="hyperliquid",
                    symbol=prod.perp_ticker,
                    role=LegRole.PERP,
                    contract_size=1.0,                 # HL은 배럴 단위
                    taker_fee_bps=0.9,                  # HIP-3 taker
                    funding_interval_hours=1.0,
                    margin_asset="USDC",
                ),
                leg_b=ExchangeLeg(
                    exchange="kis",
                    symbol=kis_symbol,
                    role=LegRole.DATED_FUTURES,
                    contract_size=float(prod.contract_size),
                    fee_per_contract_usd=prod.futures_fee_per_contract,
                    margin_asset="KRW_equiv",
                ),
                params=params,
            )
            pairs.append(pair)
        return pairs

    def get_pair(self, pair_id: str) -> "object | None":
        """pair_id로 ArbitragePair 조회."""
        for p in self.get_pairs():
            if p.id == pair_id:
                return p
        return None


def load_config(
    settings_path: str = "config/settings.yaml",
    secrets_path: str = "config/secrets.yaml",
) -> AppConfig:
    """YAML 설정 파일 로드.

    Args:
        settings_path: 메인 설정 파일 경로
        secrets_path: 시크릿 파일 경로

    Returns:
        AppConfig 객체
    """
    settings: dict[str, Any] = {}
    secrets: dict[str, Any] = {}

    settings_file = Path(settings_path)
    if settings_file.exists():
        with open(settings_file) as f:
            settings = yaml.safe_load(f) or {}

    secrets_file = Path(secrets_path)
    if secrets_file.exists():
        with open(secrets_file) as f:
            secrets = yaml.safe_load(f) or {}

    # 상품 설정
    products = {}
    for key, val in settings.get("products", {}).items():
        products[key] = ProductConfig(**val)

    # Hyperliquid 설정
    hl_settings = settings.get("hyperliquid", {})
    hl_secrets = secrets.get("hyperliquid", {})
    hl_config = HyperliquidConfig(
        use_testnet=hl_settings.get("use_testnet", False),
        perp_dex=hl_settings.get("perp_dex", "xyz"),
        ws_reconnect_delay=hl_settings.get("ws_reconnect_delay", 5),
        ws_ping_interval=hl_settings.get("ws_ping_interval", 30),
        wallet_address=hl_secrets.get("wallet_address", ""),
        private_key=hl_secrets.get("private_key", ""),
    )

    # 키움 설정
    kw_settings = settings.get("kiwoom", {})
    kw_secrets = secrets.get("kiwoom", {})
    kw_config = KiwoomConfig(
        use_mock=kw_settings.get("use_mock", True),
        server_type=kw_settings.get("server_type", "mock"),
        account_number=kw_secrets.get("account_number", ""),
        account_password=kw_secrets.get("account_password", ""),
    )

    # KIS 설정
    kis_settings = settings.get("kis", {})
    kis_secrets = secrets.get("kis", {})
    kis_config = KISConfig(
        enabled=kis_settings.get("enabled", False),
        app_key=kis_secrets.get("app_key", ""),
        app_secret=kis_secrets.get("app_secret", ""),
        account_number=kis_secrets.get("account_number", ""),
        base_url=kis_settings.get("base_url", "https://openapi.koreainvestment.com:9443"),
        ws_url=kis_settings.get("ws_url", "ws://ops.koreainvestment.com:21000"),
        is_paper=kis_settings.get("is_paper", False),
        cme_realtime=kis_settings.get("cme_realtime", False),
    )

    # 전략/리스크 설정
    strat = settings.get("strategy", {})
    risk = settings.get("risk", {})
    db = settings.get("database", {})
    log = settings.get("logging", {})

    return AppConfig(
        mode=settings.get("mode", "PAPER"),
        products=products,
        kis_symbol_map=dict(settings.get("kis_symbol_map", {})),
        hyperliquid=hl_config,
        kiwoom=kw_config,
        kis=kis_config,
        strategy=StrategyConfig(**{k: v for k, v in strat.items() if k in StrategyConfig.__dataclass_fields__}),
        risk=RiskConfig(**{k: v for k, v in risk.items() if k in RiskConfig.__dataclass_fields__}),
        db_path=db.get("path", "data/arbitrage.db"),
        log_level=log.get("level", "INFO"),
        log_file=log.get("file", "logs/arbitrage.log"),
    )
