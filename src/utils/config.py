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
    contract_size: float = 1.0
    min_order_size: int = 1


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
class StrategyConfig:
    basis_window_hours: int = 24
    basis_std_multiplier: float = 2.0
    entry_threshold_bps: float = 50
    exit_threshold_bps: float = 10
    target_profit_bps: float = 30
    max_hold_hours: int = 48
    funding_rate_weight: float = 1.0
    min_funding_advantage_bps: float = 5


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
    hyperliquid: HyperliquidConfig = field(default_factory=HyperliquidConfig)
    kiwoom: KiwoomConfig = field(default_factory=KiwoomConfig)
    strategy: StrategyConfig = field(default_factory=StrategyConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    db_path: str = "data/arbitrage.db"
    log_level: str = "INFO"
    log_file: str = "logs/arbitrage.log"


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

    # 전략/리스크 설정
    strat = settings.get("strategy", {})
    risk = settings.get("risk", {})
    db = settings.get("database", {})
    log = settings.get("logging", {})

    return AppConfig(
        mode=settings.get("mode", "PAPER"),
        products=products,
        hyperliquid=hl_config,
        kiwoom=kw_config,
        strategy=StrategyConfig(**{k: v for k, v in strat.items() if k in StrategyConfig.__dataclass_fields__}),
        risk=RiskConfig(**{k: v for k, v in risk.items() if k in RiskConfig.__dataclass_fields__}),
        db_path=db.get("path", "data/arbitrage.db"),
        log_level=log.get("level", "INFO"),
        log_file=log.get("file", "logs/arbitrage.log"),
    )
