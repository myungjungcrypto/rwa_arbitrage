"""KIS 잔고 응답 raw dump v2 — 전체 JSON + 대안 endpoint sweep.

KIS 해외선물 잔고 조회는 endpoint × tr_id × params 조합이 다양함.
empty output1을 보면 endpoint 자체가 다른 거. 후보 모두 시도.

사용법:
    .venv/bin/python scripts/dump_kis_balance_v2.py
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
    if not kis.app_key or not kis.app_secret: return 1

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

    # KIS 해외선물 endpoint × tr_id 후보 sweep.
    # 각 후보별로 응답 JSON 통째로 출력 (raw dict 키 전부 노출).
    base_params_common = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt,
    }
    candidates = [
        # (path, tr_id, extra_params, http_method)
        # 1) 예수금 현황
        ("/uapi/overseas-futureoption/v1/trading/inquire-deposit", "OTFM1411R",
         {"OVRS_EXCG_CD": "CME", "CRCY_CD": "USD", "INQR_DT": inqr_dt}, "GET"),
        ("/uapi/overseas-futureoption/v1/trading/inquire-deposit", "OTFM1411R",
         {"OVRS_EXCG_CD": "", "CRCY_CD": "USD", "INQR_DT": inqr_dt}, "GET"),
        # 2) 잔고 현황 (예수금 X, 잔고/포지션)
        ("/uapi/overseas-futureoption/v1/trading/inquire-balance", "OTFM1412R",
         {"OVRS_EXCG_CD": "", "INQR_DT": inqr_dt}, "GET"),
        # 3) 정산내역
        ("/uapi/overseas-futureoption/v1/trading/inquire-deposit", "OTFM3115R",
         {"OVRS_EXCG_CD": "CME", "CRCY_CD": "USD", "INQR_DT": inqr_dt}, "GET"),
        # 4) 미결제 잔고 — positions 가져오는 endpoint (inquire-unpd)
        ("/uapi/overseas-futureoption/v1/trading/inquire-unpd", "OTFM1412R",
         {"OVRS_EXCG_CD": "@@@@", "CRCY_CD": "TUS",
          "WCRC_FRCR_DVSN_CD": "02", "INQR_DT": inqr_dt}, "GET"),
        # 5) 증거금 현황
        ("/uapi/overseas-futureoption/v1/trading/inquire-margin-detail", "OTFM3117R",
         {"OVRS_EXCG_CD": "CME", "INQR_DT": inqr_dt}, "GET"),
    ]

    async with aiohttp.ClientSession() as session:
        for i, (path, tr_id, extra, method) in enumerate(candidates, 1):
            tr_id = tr_id.replace("OTFM", "VTFM") if kis.is_paper else tr_id
            params = {**base_params_common, **extra}
            headers = auth.get_rest_headers(tr_id)
            url = f"{auth.base_url}{path}"
            print(f"\n{'='*70}")
            print(f"[{i}] {method} {path}")
            print(f"    tr_id={tr_id}  params={extra}")
            try:
                async with session.request(method, url, headers=headers,
                                            params=params) as r:
                    data = await r.json()
            except Exception as e:
                print(f"    ERROR: {e}"); continue

            rt_cd = data.get("rt_cd")
            msg = (data.get("msg1") or "").strip()
            print(f"    rt_cd: {rt_cd!r}  msg: {msg[:100]!r}")
            if rt_cd == "0":
                # 응답 통째로 — output*/output1/2/3/output 모든 키 + sample
                top_keys = [k for k in data.keys() if k not in ("rt_cd", "msg_cd", "msg1")]
                print(f"    top-level keys: {top_keys}")
                for k in top_keys:
                    v = data[k]
                    if isinstance(v, dict):
                        print(f"    {k}: dict with {len(v)} keys: {list(v.keys())[:30]}")
                        # 숫자처럼 보이는 필드만
                        for sk, sv in list(v.items())[:30]:
                            if sv and any(t in sk.lower() for t in ("amt","bal","dncl","mgn","evl","pchs")):
                                print(f"      {sk}: {sv}")
                    elif isinstance(v, list):
                        print(f"    {k}: list[{len(v)}]")
                        if v:
                            sample = v[0]
                            if isinstance(sample, dict):
                                print(f"      sample[0] keys: {list(sample.keys())[:20]}")
                                # 첫 row 의 amt/bal/dncl 키 노출
                                for sk, sv in sample.items():
                                    if sv and any(t in sk.lower() for t in ("amt","bal","dncl","mgn","evl","crcy","cd")):
                                        print(f"        {sk}: {sv!r}")
                            else:
                                print(f"      sample[0]: {sample!r}")
                    else:
                        print(f"    {k}: {v!r}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
