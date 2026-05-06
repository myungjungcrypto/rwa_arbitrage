"""KIS 잔고 응답 raw dump — 어느 키에 USD 예수금이 들어있는지 확인.

사용법:
    .venv/bin/python scripts/dump_kis_balance.py

계정 비밀 정보는 출력하지 않음 (output1 dict + rt_cd만).
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp

from src.utils.config import load_config


async def main() -> int:
    cfg = load_config("config/settings.yaml", "config/secrets.yaml")
    kis = cfg.kis
    if not kis.app_key or not kis.app_secret:
        print("[FAIL] secrets.yaml missing kis.app_key/app_secret"); return 1

    from src.exchange.kis import KISAuth
    auth = KISAuth(
        app_key=kis.app_key, app_secret=kis.app_secret,
        base_url=kis.base_url, is_paper=kis.is_paper,
        account_number=kis.account_number,
    )
    cano, acnt_prdt = auth.account_cano_prdt
    print(f"[Config] CANO={cano} ACNT_PRDT_CD={acnt_prdt} is_paper={kis.is_paper}")

    await auth.get_access_token()
    inqr_dt = datetime.now(timezone.utc).strftime("%Y%m%d")

    # 다양한 tr_id × currency 조합 시도 (KIS docs상 가장 가능성 높은 순)
    candidates = [
        ("OTFM1411R", "USD"),
        ("OTFM1411R", "KRW"),
        ("OTFM3115R", "USD"),    # 이전 시도
    ]
    for tr_id, crcy in candidates:
        if kis.is_paper:
            tr_id = tr_id.replace("OTFM", "VTFM")
        headers = auth.get_rest_headers(tr_id)
        params = {
            "CANO": cano, "ACNT_PRDT_CD": acnt_prdt,
            "OVRS_EXCG_CD": "CME", "CRCY_CD": crcy,
            "INQR_DT": inqr_dt,
        }
        url = f"{auth.base_url}/uapi/overseas-futureoption/v1/trading/inquire-deposit"
        print(f"\n--- tr_id={tr_id}, CRCY_CD={crcy}, INQR_DT={inqr_dt} ---")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, params=params) as r:
                    data = await r.json()
        except Exception as e:
            print(f"  HTTP error: {e}"); continue

        rt_cd = data.get("rt_cd")
        msg = (data.get("msg1") or "").strip()
        print(f"  rt_cd: {rt_cd!r}")
        print(f"  msg:   {msg[:120]!r}")

        if rt_cd == "0":
            out1 = data.get("output1") or {}
            print(f"  output1 keys: {list(out1.keys())}")
            for k, v in (out1.items() if isinstance(out1, dict) else []):
                # 숫자처럼 보이는 필드만
                try:
                    f = float(v) if v else 0.0
                    if "amt" in k.lower() or "bal" in k.lower() or "dncl" in k.lower() or f != 0:
                        print(f"    {k}: {v}")
                except (TypeError, ValueError):
                    pass
            # output2/output3가 있을 수도 (배열 형태로 통화별 잔고)
            for ok in ("output2", "output3"):
                if ok in data:
                    print(f"  {ok}: {json.dumps(data[ok], ensure_ascii=False)[:400]}")
        else:
            # 에러 케이스 — 다른 파라미터 조합으로 hint
            print(f"  → tr_id={tr_id} 또는 파라미터 조합이 잘못됨. KIS 콘솔에서 정확한 tr_id 확인 필요.")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
