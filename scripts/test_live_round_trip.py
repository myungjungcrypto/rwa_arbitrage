"""LIVE round-trip 테스트 — 1 MCL 양 leg 진입 + 5초 후 청산.

위험고지/HTS ID/시세 권한/API key 변경 후 실 거래 흐름 검증용.
실 손익은 slippage + 수수료만 (수 천원 ~ 수 만원).

사용:
    # 봇은 PAPER로 두고 별도 실행
    .venv/bin/python scripts/test_live_round_trip.py

진행:
    1. config + active KIS contract 표시
    2. 현재 HL 시세 표시 + 사용자 'YES' 확인
    3. 양 leg 동시 market 진입 (HL 100 배럴 BUY + KIS 1 계약 SELL)
    4. 둘 다 성공 → 5초 대기 / 한쪽만 → 즉시 청산
    5. 양 leg reduce_only market 청산
    6. 체결 가격/지연/총 비용 보고
"""

from __future__ import annotations

import asyncio
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.config import load_config
from src.exchange.hyperliquid import (
    HyperliquidClient, HyperliquidExchange, HyperliquidWebSocket,
)
from src.exchange.kis import KISAuth, KISFuturesClient, KISExchange
from src.strategy.rollover import get_active_contract, us_market_holidays


async def main() -> int:
    cfg = load_config("config/settings.yaml", "config/secrets.yaml")

    print("=" * 70)
    print("LIVE Round-Trip Test")
    print("=" * 70)

    # 활성 KIS contract (rollover 반영)
    today = date.today()
    hols = us_market_holidays(today.year)
    active_kis = get_active_contract(today, prefix="MCL", holidays=hols)
    print(f"\n[CONFIG]")
    print(f"  today              = {today}")
    print(f"  active KIS symbol  = {active_kis}")
    print(f"  config map (stale) = {cfg.kis_symbol_map.get('wti', 'N/A')}")
    print(f"  HL wallet          = {cfg.hyperliquid.wallet_address[:10]}...")
    print(f"  KIS account        = {cfg.kis.account_number}")

    if cfg.kis_symbol_map.get("wti") != active_kis:
        print(f"\n⚠️  WARNING: settings.yaml kis_symbol_map.wti = "
              f"{cfg.kis_symbol_map.get('wti')!r}, active = {active_kis!r}")
        print(f"     이 스크립트는 active({active_kis})로 주문합니다.")

    # HL adapter
    hl_rest = HyperliquidClient(
        use_testnet=cfg.hyperliquid.use_testnet,
        wallet_address=cfg.hyperliquid.wallet_address,
        private_key=cfg.hyperliquid.private_key,
        perp_dex=cfg.hyperliquid.perp_dex,
    )
    hl_ws = HyperliquidWebSocket(use_testnet=cfg.hyperliquid.use_testnet)
    hl = HyperliquidExchange(rest=hl_rest, ws=hl_ws)

    # KIS adapter
    auth = KISAuth(
        app_key=cfg.kis.app_key,
        app_secret=cfg.kis.app_secret,
        account_number=cfg.kis.account_number,
        is_paper=cfg.kis.is_paper,
        hts_id=cfg.kis.hts_id,
    )
    await auth.get_access_token()
    kis_client = KISFuturesClient(
        auth=auth, ws_url=cfg.kis.ws_url, is_paper=cfg.kis.is_paper,
    )
    kis = KISExchange(client=kis_client)

    # 현재 HL 시세
    hl_q = await hl.get_quote("xyz:CL")
    if not hl_q:
        print("\n[FAIL] HL 시세 조회 실패"); return 1
    notional = hl_q.mid_price * 100
    print(f"\n[QUOTE] HL xyz:CL: bid={hl_q.bid:.2f} ask={hl_q.ask:.2f} "
          f"mid={hl_q.mid_price:.2f}")
    print(f"        notional ~= ${notional:.0f} (1 MCL = 100 배럴)")

    print(f"\n[PLAN]")
    print(f"  HL  : BUY  100 배럴 xyz:CL  market (limit_px ~= ${hl_q.ask * 1.05:.2f})")
    print(f"  KIS : SELL 1 계약 {active_kis}  market")
    print(f"  → 5초 대기 → 양 leg reduce_only 청산")
    print(f"\n예상 비용: slippage (~$10) + 수수료 (~$5 HL + ~$5 KIS) = 약 $15-30")

    confirm = input("\n진행하려면 'YES' 입력: ").strip()
    if confirm != "YES":
        print("취소됨"); return 0

    # ── ENTRY: 양 leg 동시 ──
    print(f"\n[ENTRY] {time.strftime('%H:%M:%S')} 양 leg market 진입...")
    t0 = time.time()
    hl_task = hl.place_order(
        symbol="xyz:CL", side="buy", size=100.0, order_type="market",
    )
    kis_task = kis.place_order(
        symbol=active_kis, side="sell", size=1.0, order_type="market",
    )
    hl_res, kis_res = await asyncio.gather(hl_task, kis_task, return_exceptions=True)
    elapsed = time.time() - t0
    print(f"  HL  result: {_fmt(hl_res)}")
    print(f"  KIS result: {_fmt(kis_res)}")
    print(f"  elapsed: {elapsed*1000:.0f}ms")

    hl_ok = _ok(hl_res)
    kis_ok = _ok(kis_res)

    if not hl_ok and not kis_ok:
        print(f"\n[RESULT] 양 leg 모두 fail — 청산 불필요")
        await _cleanup(hl_rest, kis_client); return 2

    # ── EXIT ──
    if hl_ok and kis_ok:
        print(f"\n[HOLD] 양 leg 진입 OK — 5초 대기...")
        await asyncio.sleep(5)
    else:
        print(f"\n⚠️ [PARTIAL] 한 leg만 진입 — 즉시 unwind")

    print(f"\n[EXIT] {time.strftime('%H:%M:%S')} reduce_only 청산...")
    t1 = time.time()
    exit_tasks = []
    if hl_ok:
        exit_tasks.append(("HL", hl.place_order(
            symbol="xyz:CL", side="sell", size=100.0,
            order_type="market", reduce_only=True,
        )))
    if kis_ok:
        exit_tasks.append(("KIS", kis.place_order(
            symbol=active_kis, side="buy", size=1.0,
            order_type="market", reduce_only=True,
        )))
    exit_results = await asyncio.gather(
        *(t for _, t in exit_tasks), return_exceptions=True,
    )
    for (name, _), res in zip(exit_tasks, exit_results):
        print(f"  {name} unwind: {_fmt(res)}")
    print(f"  elapsed: {(time.time()-t1)*1000:.0f}ms")

    # ── 비용 분석 ──
    print(f"\n[COST]")
    if hl_ok and _ok(exit_results[0] if exit_results else None):
        hl_entry = hl_res.filled_price
        hl_exit = exit_results[0].filled_price
        slip_hl = (hl_exit - hl_entry) * 100   # buy 후 sell, gain이면 +
        print(f"  HL  : entry={hl_entry:.4f} exit={hl_exit:.4f} "
              f"slippage_pnl=${-slip_hl:+.2f}")
    if kis_ok and len(exit_results) >= (2 if hl_ok else 1):
        kis_idx = 1 if hl_ok else 0
        if _ok(exit_results[kis_idx]):
            kis_entry = kis_res.filled_price
            kis_exit = exit_results[kis_idx].filled_price
            slip_kis = (kis_entry - kis_exit) * 100   # sell 후 buy
            print(f"  KIS : entry={kis_entry:.4f} exit={kis_exit:.4f} "
                  f"slippage_pnl=${slip_kis:+.2f}")

    print(f"\n[DONE] 완료 — KIS HTS / trade.xyz 에서 청산된 포지션 (open=0) 직접 확인 권장")
    await _cleanup(hl_rest, kis_client)
    return 0


def _ok(res) -> bool:
    if isinstance(res, Exception): return False
    return getattr(res, "success", False)


def _fmt(res) -> str:
    if isinstance(res, Exception):
        return f"EXCEPTION {type(res).__name__}: {str(res)[:120]}"
    if not getattr(res, "success", False):
        return f"FAIL error={getattr(res, 'error', '?')[:120]}"
    return (f"OK oid={getattr(res, 'order_id', '?')} "
            f"filled={getattr(res, 'filled_size', 0)}@"
            f"{getattr(res, 'filled_price', 0):.4f}")


async def _cleanup(hl_rest, kis_client):
    try: await hl_rest.close()
    except Exception: pass
    try: await kis_client.disconnect()
    except Exception: pass


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
