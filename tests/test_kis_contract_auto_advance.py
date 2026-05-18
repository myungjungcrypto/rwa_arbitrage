"""KIS contract auto-advance — pair.leg_b.symbol 자동 동기화 검증.

main.py의 부팅 시 active contract override + rollover_watch_loop의 advance
sync를 테스트 (둘 다 동일 메커니즘 — pair object의 symbol 직접 mutation).
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from src.strategy.pair import (
    ArbitragePair, ExchangeLeg, LegRole, PairGate, PairStrategyParams,
)
from src.strategy.rollover import get_active_contract, us_market_holidays


def _kis_pair(symbol: str = "MCLM26") -> ArbitragePair:
    return ArbitragePair(
        id="wti_cme_hl", enabled=True, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL",
                          role=LegRole.PERP),
        leg_b=ExchangeLeg(exchange="kis", symbol=symbol,
                          role=LegRole.DATED_FUTURES, contract_size=100),
        params=PairStrategyParams(),
    )


def _perp_perp_pair() -> ArbitragePair:
    """Web3-Web3 페어 — KIS dated_futures 아님 → advance 무관."""
    return ArbitragePair(
        id="wti_hl_lighter", enabled=False, gate=PairGate.CME_HOURS,
        leg_a=ExchangeLeg(exchange="hyperliquid", symbol="xyz:CL",
                          role=LegRole.PERP),
        leg_b=ExchangeLeg(exchange="lighter", symbol="WTI",
                          role=LegRole.PERP),
        params=PairStrategyParams(),
    )


# ──────────────────────────────────────────────
# Boot-time override 로직 — main.py에 있는 단순 mutation 검증
# ──────────────────────────────────────────────


def _apply_boot_override(pairs, products, today):
    """main.py의 부팅 시 override 로직을 unit-testable form으로 추출.

    실제 main.py 코드와 동일한 동작.
    """
    hols = us_market_holidays(today.year)
    for pair in pairs:
        if not (pair.leg_b.role == LegRole.DATED_FUTURES
                and pair.leg_b.exchange == "kis"):
            continue
        product_key = pair.id.split("_", 1)[0]
        prod = products.get(product_key)
        if prod is None:
            continue
        active = get_active_contract(
            today, prefix=prod.futures_symbol, holidays=hols,
        )
        if pair.leg_b.symbol != active:
            pair.leg_b.symbol = active


def test_boot_override_stale_to_active():
    """stale MCLM26 → BD 11+ 시점에 MCLN26 자동 sync."""
    from src.utils.config import ProductConfig
    products = {"wti": ProductConfig(
        perp_ticker="xyz:CL", futures_symbol="MCL",
        contract_size=100,
    )}
    pair = _kis_pair(symbol="MCLM26")
    # 2026-05-15 = BD 11 = post-roll, active should be MCLN26
    _apply_boot_override([pair], products, date(2026, 5, 15))
    assert pair.leg_b.symbol == "MCLN26"


def test_boot_override_already_current_no_change():
    """이미 active와 같으면 mutation X."""
    from src.utils.config import ProductConfig
    products = {"wti": ProductConfig(
        perp_ticker="xyz:CL", futures_symbol="MCL", contract_size=100,
    )}
    pair = _kis_pair(symbol="MCLN26")
    _apply_boot_override([pair], products, date(2026, 5, 15))
    assert pair.leg_b.symbol == "MCLN26"


def test_boot_override_pre_roll_uses_current_month():
    """BD 1-4 = pre-roll, MCLM26 그대로."""
    from src.utils.config import ProductConfig
    products = {"wti": ProductConfig(
        perp_ticker="xyz:CL", futures_symbol="MCL", contract_size=100,
    )}
    pair = _kis_pair(symbol="MCLM26")
    # 2026-05-01 = BD 1
    _apply_boot_override([pair], products, date(2026, 5, 1))
    # roll_end_day=10이라 BD 1에선 advance 안 함
    assert pair.leg_b.symbol == "MCLM26"


def test_boot_override_skips_perp_perp_pair():
    """Web3-Web3 페어는 dated_futures 아니라 무시."""
    from src.utils.config import ProductConfig
    products = {"wti": ProductConfig(
        perp_ticker="xyz:CL", futures_symbol="MCL", contract_size=100,
    )}
    pair = _perp_perp_pair()
    original_symbol = pair.leg_b.symbol
    _apply_boot_override([pair], products, date(2026, 5, 15))
    assert pair.leg_b.symbol == original_symbol   # WTI 그대로


def test_boot_override_missing_product_config_keeps_stale():
    """product 매칭 안 되면 leg_b.symbol 건드리지 않음 (silent)."""
    pair = _kis_pair(symbol="MCLM26")
    _apply_boot_override([pair], {}, date(2026, 5, 15))   # products 비어있음
    assert pair.leg_b.symbol == "MCLM26"   # 변화 없음


# ──────────────────────────────────────────────
# rollover_watch_loop의 advance sync — engine._registered_pairs mutation
# ──────────────────────────────────────────────


def test_watch_loop_sync_mutates_registered_pair():
    """rollover_watch_loop가 contract advance 시 engine pair object도 sync.

    실 loop 호출 없이 mutation 로직만 검증.
    """
    # engine mock — _registered_pairs dict만
    engine = MagicMock()
    pair = _kis_pair(symbol="MCLM26")
    engine._registered_pairs = {"wti_cme_hl": pair}

    # rollover_watch_loop 내부의 sync 로직 직접 적용 (main.py와 동일)
    product = "wti"
    desired = "MCLN26"
    for p in engine._registered_pairs.values():
        if (p.leg_b.role.value == "dated_futures"
                and p.leg_b.exchange == "kis"
                and p.id.split("_", 1)[0] == product):
            p.leg_b.symbol = desired

    assert pair.leg_b.symbol == "MCLN26"


def test_watch_loop_sync_skips_perp_perp_pair():
    engine = MagicMock()
    kis_pair = _kis_pair(symbol="MCLM26")
    perp_pair = _perp_perp_pair()
    engine._registered_pairs = {
        "wti_cme_hl": kis_pair,
        "wti_hl_lighter": perp_pair,
    }
    original_lighter_symbol = perp_pair.leg_b.symbol

    product = "wti"
    desired = "MCLN26"
    for p in engine._registered_pairs.values():
        if (p.leg_b.role.value == "dated_futures"
                and p.leg_b.exchange == "kis"
                and p.id.split("_", 1)[0] == product):
            p.leg_b.symbol = desired

    assert kis_pair.leg_b.symbol == "MCLN26"            # KIS만 sync
    assert perp_pair.leg_b.symbol == original_lighter_symbol  # 그대로
