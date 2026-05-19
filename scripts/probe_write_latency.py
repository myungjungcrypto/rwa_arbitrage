"""HL + KIS write endpoint latency probe — 무비용 (invalid order로 reject 측정).

목적: read endpoint (info/inquire)는 8-91ms로 측정됨. write endpoint는
서명 + 서버 검증 + (KIS) broker risk + CME 라우팅 포함이라 더 걸림.
실 거래 없이 invalid order를 보내 서버 reject 시간으로 write path latency
근사 측정.

방식:
- HL: `Exchange.order(name="NONEXISTENT_COIN", ...)` — universe lookup fail
       또는 size=0 → 서명까지 가서 서버 reject. 비용 0.
- KIS: 동일하게 invalid 주문 (size=0 또는 invalid PDNO). 서버가 rt_cd!=0 응답.
       KIS는 broker risk 거치므로 실 order endpoint latency 정확히 측정 가능.
- asyncio.gather로 양 leg 병렬 발사 (실 진입 패턴 동일)

⚠️ KIS rate limit (order endpoint): 초당 1회 권장. --interval 3s default
   (15회 × 3s = 45초 소요).
⚠️ 일부 invalid 주문이 서버 sanity check 통과해서 실 fail 응답 받을 수
   있음 — 이건 의도된 동작 (latency 측정 목적).
⚠️ 너무 많이 invalid 주문 보내면 거래소가 IP blacklist 가능성 — 30회 이내
   추천.

Usage:
    .venv/bin/python scripts/probe_write_latency.py --n 15
    .venv/bin/python scripts/probe_write_latency.py --n 20 --interval 4.0 --csv data/write_latency.csv
"""
from __future__ import annotations

import argparse
import asyncio
import csv
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.exchange.kis import KISAuth
from src.utils.config import load_config


# ──────────────────────────────────────────────
# HL invalid order probe
# ──────────────────────────────────────────────

def _build_hl_exchange(wallet_address: str, private_key: str, use_testnet: bool, perp_dex: str):
    """HL SDK Exchange object — 실 주문 path와 동일하게 구성."""
    from hyperliquid.exchange import Exchange
    from hyperliquid.utils import constants
    from eth_account import Account
    pk = private_key
    if not pk.startswith("0x"):
        pk = "0x" + pk
    signer = Account.from_key(pk)
    base_url = constants.TESTNET_API_URL if use_testnet else constants.MAINNET_API_URL
    account_address = wallet_address or signer.address
    return Exchange(
        wallet=signer,
        base_url=base_url,
        account_address=account_address,
        perp_dexs=[perp_dex] if perp_dex else None,
    )


async def hl_probe_write(exchange, ticker: str) -> tuple[float, str]:
    """HL invalid order → 서버 reject 시간 측정.

    전략: name=ticker (실 ticker, e.g. xyz:CL) + size=1 + limit_px=0.000001
    (말도 안 되는 가격, 절대 체결 안 됨, 서버가 'invalid price' or 'no liquidity'
    reject). 서명 → 네트워크 → 서버 validation 풀패스 측정.
    """
    from hyperliquid.utils.signing import sign_l1_action  # placeholder
    t0 = time.time()
    try:
        # invalid limit_px — 서버가 즉시 reject ('Order has invalid price')
        result = await asyncio.to_thread(
            exchange.order,
            name=ticker,
            is_buy=True,
            sz=0.001,                 # 매우 작은 size (혹시 통과 시 비용 최소)
            limit_px=0.0001,           # 절대 체결 불가능한 가격
            order_type={"limit": {"tif": "Ioc"}},
            reduce_only=False,
        )
        ms = (time.time() - t0) * 1000
        status = result.get("status", "?")
        if status == "ok":
            # 혹시 통과되면 어떤 응답인지 출력 — 거의 reject일 것
            statuses = result.get("response", {}).get("data", {}).get("statuses", [])
            if statuses and "error" in statuses[0]:
                return ms, f"rejected: {statuses[0]['error'][:40]}"
            return ms, f"unexpected ok: {status}"
        return ms, f"reject:{str(result)[:40]}"
    except Exception as e:
        ms = (time.time() - t0) * 1000
        return ms, f"exc:{str(e)[:40]}"


# ──────────────────────────────────────────────
# KIS invalid order probe
# ──────────────────────────────────────────────

async def kis_probe_write(auth: KISAuth, symbol: str) -> tuple[float, str]:
    """KIS invalid order → 서버 reject 시간 측정.

    전략: 실 symbol(MCLM26 등) + size=1 + 가격 0.01 (절대 체결 불가능).
    KIS server가 'FM_LIMIT_ORD_PRIC' 검증 후 rt_cd!=0 응답.
    persistent session + 실 endpoint = 실 주문 latency 정확 측정.
    """
    t0 = time.time()
    cano, acnt_prdt = auth.account_cano_prdt
    body = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt,
        "OVRS_FUTR_FX_PDNO": symbol,
        "SLL_BUY_DVSN_CD": "02",         # buy
        "PRIC_DVSN_CD": "1",             # 지정가
        "FM_LIMIT_ORD_PRIC": "0.01",     # 절대 체결 불가능한 가격
        "FM_STOP_ORD_PRIC": "0",
        "FM_ORD_QTY": "1",
        "CCLD_CNDT_CD": "6",
        "FM_LQD_USTL_CCLD_DT": "",
        "FM_LQD_USTL_CCNO": "",
        "CPLX_ORD_DVSN_CD": "0",
        "ECIS_RSVN_ORD_YN": "N",
        "FM_HDGE_ORD_SCRN_YN": "N",
    }
    tr_id = "VTFM3001U" if auth.is_paper else "OTFM3001U"
    headers = auth.get_rest_headers(tr_id)
    url = f"{auth.base_url}/uapi/overseas-futureoption/v1/trading/order"
    session = auth.get_rest_session()
    try:
        async with session.post(url, headers=headers, json=body) as resp:
            data = await resp.json()
    except Exception as e:
        ms = (time.time() - t0) * 1000
        return ms, f"exc:{str(e)[:40]}"
    ms = (time.time() - t0) * 1000
    rt_cd = str(data.get("rt_cd", "?"))
    msg = data.get("msg1", "")[:30]
    return ms, f"rt_cd={rt_cd} {msg}"


def summarize(label: str, samples: list[float]) -> str:
    if not samples:
        return f"{label}: no samples"
    s = sorted(samples)
    n = len(s)

    def pct(p: float) -> float:
        return s[min(n - 1, int(n * p))]

    return (
        f"{label}: n={n} "
        f"min={s[0]:>6.0f} "
        f"p50={pct(0.50):>6.0f} "
        f"p90={pct(0.90):>6.0f} "
        f"p99={pct(0.99):>6.0f} "
        f"max={s[-1]:>6.0f}ms "
        f"mean={statistics.mean(s):>6.0f} "
        f"std={statistics.stdev(s) if n>1 else 0:>5.0f}"
    )


async def run(args):
    config = load_config("config/settings.yaml")

    hl_cfg = config.hyperliquid
    if not hl_cfg.private_key:
        print("ERROR: HL private_key not set in secrets.yaml")
        return

    print("Building HL Exchange object...")
    try:
        exchange = _build_hl_exchange(
            wallet_address=hl_cfg.wallet_address or "",
            private_key=hl_cfg.private_key,
            use_testnet=hl_cfg.use_testnet,
            perp_dex=hl_cfg.perp_dex,
        )
        print(f"  HL Exchange ready (dex={hl_cfg.perp_dex})")
    except Exception as e:
        print(f"  HL Exchange FAILED: {e}")
        return

    kis_cfg = config.kis
    kis_auth = KISAuth(
        app_key=kis_cfg.app_key,
        app_secret=kis_cfg.app_secret,
        account_number=kis_cfg.account_number,
        is_paper=kis_cfg.is_paper,
        base_url=kis_cfg.base_url,
        hts_id=kis_cfg.hts_id,
    )
    print("Pre-warming KIS access_token...")
    try:
        await kis_auth.get_access_token()
        print(f"  KIS token OK (paper={kis_cfg.is_paper})")
    except Exception as e:
        print(f"  KIS token FAILED: {e}")
        await kis_auth.close_rest_session()
        return

    hl_lats: list[float] = []
    kis_lats: list[float] = []
    gather_lats: list[float] = []

    csv_writer = None
    csv_file = None
    if args.csv:
        Path(args.csv).parent.mkdir(parents=True, exist_ok=True)
        csv_file = open(args.csv, "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["iter", "ts", "hl_ms", "hl_msg", "kis_ms", "kis_msg", "gather_ms"])

    print()
    print(f"=== Write endpoint probe — {args.n} iterations, {args.interval}s interval ===")
    print(f"  HL ticker:  {args.hl_ticker}")
    print(f"  KIS symbol: {args.kis_symbol}")
    print(f"  expect both legs to REJECT (no real fill)")
    print()
    print(f"{'#':>3} {'HL_ms':>7} {'KIS_ms':>7} {'gather_ms':>10} {'HL_msg':<35} {'KIS_msg':<30}")
    print("-" * 100)

    for i in range(args.n):
        t0 = time.time()
        try:
            (hl_ms, hl_msg), (kis_ms, kis_msg) = await asyncio.gather(
                hl_probe_write(exchange, args.hl_ticker),
                kis_probe_write(kis_auth, args.kis_symbol),
            )
        except Exception as e:
            print(f"[{i:>3}] error: {e}")
            await asyncio.sleep(args.interval)
            continue
        gather_ms = (time.time() - t0) * 1000
        hl_lats.append(hl_ms)
        kis_lats.append(kis_ms)
        gather_lats.append(gather_ms)
        print(f"{i:>3} {hl_ms:>7.0f} {kis_ms:>7.0f} {gather_ms:>10.0f} {hl_msg:<35} {kis_msg:<30}")
        if csv_writer:
            csv_writer.writerow([i, time.time(), f"{hl_ms:.1f}", hl_msg, f"{kis_ms:.1f}", kis_msg, f"{gather_ms:.1f}"])
            csv_file.flush()
        await asyncio.sleep(args.interval)

    print()
    print("=== Summary (write endpoint latency) ===")
    print(summarize("HL    ", hl_lats))
    print(summarize("KIS   ", kis_lats))
    print(summarize("gather", gather_lats))

    if hl_lats and kis_lats:
        per_iter_max = [max(h, k) for h, k in zip(hl_lats, kis_lats)]
        sum_gather = sum(gather_lats)
        sum_max = sum(per_iter_max)
        if sum_max > 0:
            par_ratio = sum_gather / sum_max
            print()
            print(f"Parallelism: gather/max = {par_ratio:.2f}")
            if par_ratio < 1.10:
                print("  → 진짜 병렬. to_thread offload OK.")
            else:
                print(f"  → 일부 직렬화 ({par_ratio:.2f}x overhead)")

    print()
    print("=== Read vs Write 비교 (probe_latency.py 결과와 비교) ===")
    print("  Read endpoint (probe_latency):")
    print("    HL: p50=86, p99=567ms  /  KIS: p50=8, p99=15ms")
    print("  Write endpoint (this script): 위 결과")
    print()
    print("권장 entry threshold 계산:")
    if hl_lats and kis_lats:
        s_hl = sorted(hl_lats); s_kis = sorted(kis_lats)
        p99_max = max(s_hl[min(len(s_hl)-1, int(len(s_hl)*0.99))],
                      s_kis[min(len(s_kis)-1, int(len(s_kis)*0.99))])
        print(f"  양 leg p99 max = {p99_max:.0f}ms")
        print(f"  spike 1-2s 지속 가정 시: latency {p99_max:.0f}ms 동안 시장 ~{p99_max/1000*10:.0f}bp 움직임 가정")
        print(f"  → entry_threshold = 20 + 안전마진 = {max(20, int(p99_max/1000*15)+15)}bp 추천")

    await kis_auth.close_rest_session()
    if csv_file:
        csv_file.close()
        print(f"\nCSV saved: {args.csv}")


def main():
    parser = argparse.ArgumentParser(description="HL + KIS write endpoint latency probe (invalid orders, no cost)")
    parser.add_argument("--n", type=int, default=15, help="iterations (default 15)")
    parser.add_argument("--interval", type=float, default=3.0,
                        help="sleep between iterations (KIS rate-limit 보호, default 3s)")
    parser.add_argument("--hl-ticker", default="xyz:CL", help="HL ticker for invalid order")
    parser.add_argument("--kis-symbol", default="MCLN26",
                        help="KIS symbol for invalid order (현재 active contract)")
    parser.add_argument("--csv", default=None, help="output CSV (optional)")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
