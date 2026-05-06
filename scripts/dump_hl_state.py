"""HL 계정 상태 raw dump — clearinghouseState + spot balance.

bot.get_account_value()가 0을 반환하는 이유 진단:
  1. clearinghouseState.marginSummary.accountValue (perp + builder dex)
  2. spotState.balances (spot USDC)
  3. xyz dex (HIP-3 builder dex) margin 별도

사용법: .venv/bin/python scripts/dump_hl_state.py
"""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp

from src.utils.config import load_config


async def main() -> int:
    cfg = load_config("config/settings.yaml", "config/secrets.yaml")
    addr = cfg.hyperliquid.wallet_address
    if not addr:
        print("[FAIL] hyperliquid.wallet_address 비어있음"); return 1
    print(f"wallet_address: {addr}")

    base = (
        "https://api.hyperliquid-testnet.xyz" if cfg.hyperliquid.use_testnet
        else "https://api.hyperliquid.xyz"
    )

    async with aiohttp.ClientSession() as session:
        async def post(payload: dict) -> dict:
            async with session.post(f"{base}/info", json=payload) as r:
                return await r.json()

        # 1) clearinghouseState — perp + builder dex margin
        print("\n[1] clearinghouseState (native perp)")
        st = await post({"type": "clearinghouseState", "user": addr})
        ms = (st or {}).get("marginSummary") or {}
        print(f"  marginSummary.accountValue: {ms.get('accountValue', 'N/A')}")
        print(f"  marginSummary.totalRawUsd: {ms.get('totalRawUsd', 'N/A')}")
        print(f"  withdrawable:              {st.get('withdrawable', 'N/A')}")
        positions = [p for p in st.get("assetPositions", [])
                     if float((p.get("position") or {}).get("szi", 0)) != 0]
        print(f"  open positions: {len(positions)}")

        # 2) HIP-3 builder dex (trade.xyz "xyz") clearinghouseState 별도
        print(f"\n[2] clearinghouseState dex='{cfg.hyperliquid.perp_dex}' (HIP-3)")
        st_xyz = await post({
            "type": "clearinghouseState",
            "user": addr,
            "dex": cfg.hyperliquid.perp_dex,
        })
        ms_xyz = (st_xyz or {}).get("marginSummary") or {}
        print(f"  marginSummary.accountValue: {ms_xyz.get('accountValue', 'N/A')}")
        print(f"  marginSummary.totalRawUsd: {ms_xyz.get('totalRawUsd', 'N/A')}")
        print(f"  withdrawable:              {st_xyz.get('withdrawable', 'N/A')}")

        # 3) spotState — spot USDC balance
        print("\n[3] spotClearinghouseState")
        spot = await post({"type": "spotClearinghouseState", "user": addr})
        for b in (spot or {}).get("balances", []) or []:
            print(f"  {b.get('coin')}: total={b.get('total')} hold={b.get('hold')}")

        # 4) Unified state (전체 합산)
        print("\n[4] portfolio (전체 sum)")
        port = await post({"type": "portfolio", "user": addr})
        if isinstance(port, list) and len(port) > 0:
            try:
                # portfolio는 [["allTime", {...}], ...] 형태
                latest = port[0][1] if isinstance(port[0], list) else port[0]
                print(f"  raw[0]: {json.dumps(latest, ensure_ascii=False)[:300]}")
            except Exception as e:
                print(f"  parse error: {e}; raw: {str(port)[:300]}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
