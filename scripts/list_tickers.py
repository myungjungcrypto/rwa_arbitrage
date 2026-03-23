from __future__ import annotations
"""trade.xyz (HIP-3) perp DEX에서 사용 가능한 전체 티커 목록 출력.

Usage:
    python scripts/list_tickers.py
"""

import asyncio
import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.utils.config import load_config

async def main():
    config = load_config("config/settings.yaml")

    # aiohttp로 직접 호출
    import aiohttp

    base_url = "https://api.hyperliquid.xyz"
    perp_dex = config.hyperliquid.perp_dex

    async with aiohttp.ClientSession() as session:
        # HIP-3 perp DEX 조회
        payload = {"type": "metaAndAssetCtxs", "dex": perp_dex}
        async with session.post(f"{base_url}/info", json=payload) as resp:
            data = await resp.json()

        if not isinstance(data, list) or len(data) != 2:
            print(f"Unexpected response: {type(data)}")
            print(json.dumps(data, indent=2)[:500])
            return

        meta = data[0]
        ctxs = data[1]
        universe = meta.get("universe", [])

        print(f"=== '{perp_dex}' Perp DEX: {len(universe)} assets ===\n")
        print(f"{'#':<4} {'Name':<25} {'Mark Price':>12} {'Oracle Price':>12} {'Funding':>12} {'24h Volume':>15}")
        print("-" * 85)

        oil_found = []
        for i, asset in enumerate(universe):
            name = asset.get("name", "?")
            ctx = ctxs[i] if i < len(ctxs) else {}
            mark = ctx.get("markPx", "?")
            oracle = ctx.get("oraclePx", "?")
            funding = ctx.get("funding", "?")
            vol = ctx.get("dayNtlVlm", "?")

            # Oil 관련 상품 하이라이트
            is_oil = any(kw in name.upper() for kw in ["OIL", "WTI", "BRENT", "CRUDE", "CL", "BZ"])
            marker = " ◀ OIL" if is_oil else ""

            print(f"{i:<4} {name:<25} {str(mark):>12} {str(oracle):>12} {str(funding):>12} {str(vol):>15}{marker}")

            if is_oil:
                oil_found.append((name, mark, oracle))

        if oil_found:
            print(f"\n=== Oil-related tickers found ===")
            for name, mark, oracle in oil_found:
                print(f"  {name}: mark={mark}, oracle={oracle}")
        else:
            print(f"\n⚠ No oil-related tickers found in '{perp_dex}' DEX")

if __name__ == "__main__":
    asyncio.run(main())
