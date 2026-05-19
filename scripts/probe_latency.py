"""실 거래 없이 HL + KIS REST 호출 latency 측정 (PAPER 모드 호환).

목적: 5/18 LIVE에서 본 양 leg ~1700-1800ms latency의 원인이
1) 네트워크 RTT 자체인지 (EC2 ↔ HL/KIS 거리),
2) KIS persistent session reuse 실패인지,
3) HL `to_thread` offload가 실제 병렬화되었는지
를 분리 진단.

방식:
- HL: SDK `Info.meta()` — /info POST, 주문과 같은 host. sync 함수라
  asyncio.to_thread로 wrap (실 주문 path와 동일).
- KIS: 주문에 쓰는 persistent session으로 `inquire-unpd` (positions) POST.
- asyncio.gather로 양 leg 병렬 호출 (실 진입 패턴 동일).
- N 반복, latency 분포 (min/p50/p90/p99/max/mean/std) 보고.
- 마지막에 parallelism index — gather가 max(HL,KIS)에 가까우면 진짜 병렬,
  sum에 가까우면 직렬화됨 (to_thread 미작동 의심).

⚠️ KIS rate limit: inquire endpoint는 보통 초당 ~1회 권장.
   `--interval 2` 디폴트로 안전 마진.

Usage:
    python3 scripts/probe_latency.py                 # 30회 반복, 2초 간격
    python3 scripts/probe_latency.py --n 100 --interval 1.0
    python3 scripts/probe_latency.py --continuous    # Ctrl+C까지 계속
    python3 scripts/probe_latency.py --csv data/latency_probe.csv
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


def _build_hl_info(base_url: str):
    """HL SDK Info object — universe + asset ctx 캐시용."""
    from hyperliquid.info import Info
    from hyperliquid.utils import constants
    url = (
        constants.TESTNET_API_URL if "testnet" in base_url
        else constants.MAINNET_API_URL
    )
    return Info(base_url=url, skip_ws=True)


async def hl_probe(info) -> float:
    """HL /info meta 호출 wall-clock ms.

    실 주문 path와 동일하게 to_thread offload (sync SDK가 이벤트 루프 블록
    하지 않도록). 이게 KIS leg 병렬화의 핵심.
    """
    t0 = time.time()
    await asyncio.to_thread(info.meta)
    return (time.time() - t0) * 1000


async def kis_probe(auth: KISAuth) -> float:
    """KIS inquire-unpd (positions) — persistent session으로 POST.

    실 주문(`OTFM3001U`)과 같은 base_url + 같은 ClientSession → 같은 connection
    pool. 매 호출 latency가 안정되면 persistent session reuse 작동 증거.
    """
    t0 = time.time()
    cano, acnt_prdt = auth.account_cano_prdt
    body = {
        "CANO": cano, "ACNT_PRDT_CD": acnt_prdt,
        "FUOP_DVSN": "01",
        "CTX_AREA_FK100": "", "CTX_AREA_NK100": "",
    }
    tr_id = "VTFM1412R" if auth.is_paper else "OTFM1412R"
    headers = auth.get_rest_headers(tr_id)
    url = f"{auth.base_url}/uapi/overseas-futureoption/v1/trading/inquire-unpd"
    session = auth.get_rest_session()
    async with session.post(url, headers=headers, json=body) as resp:
        await resp.json()
    return (time.time() - t0) * 1000


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

    # HL Info object — universe lazy load (첫 호출이 cold)
    hl_cfg = config.hyperliquid
    info = _build_hl_info(hl_cfg.base_url)

    # KIS Auth — persistent session 공유 (실 주문 path 동일)
    kis_cfg = config.kis
    kis_auth = KISAuth(
        app_key=kis_cfg.app_key,
        app_secret=kis_cfg.app_secret,
        account_number=kis_cfg.account_number,
        is_paper=kis_cfg.is_paper,
        base_url=kis_cfg.base_url,
        hts_id=kis_cfg.hts_id,
    )

    # KIS access_token 사전 발급 (첫 회 latency 왜곡 방지)
    print("Pre-warming KIS access_token...")
    try:
        await kis_auth.get_access_token()
        print(f"  KIS token OK (paper={kis_cfg.is_paper})")
    except Exception as e:
        print(f"  KIS token FAILED: {e}")
        await kis_auth.close_rest_session()
        return

    # HL Info pre-warm (universe cache)
    print("Pre-warming HL info object...")
    try:
        await hl_probe(info)
        print("  HL info ready")
    except Exception as e:
        print(f"  HL info FAILED: {e}")
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
        csv_writer.writerow(["iter", "ts", "hl_ms", "kis_ms", "gather_ms"])

    print()
    n_target = "INF" if args.continuous else str(args.n)
    print(f"Probe loop — interval={args.interval}s, n={n_target}")
    print(f"{'#':>4} {'HL_ms':>7} {'KIS_ms':>7} {'gather_ms':>10}")
    print("-" * 32)

    i = 0
    try:
        while True:
            if not args.continuous and i >= args.n:
                break
            t0 = time.time()
            try:
                hl_ms, kis_ms = await asyncio.gather(
                    hl_probe(info), kis_probe(kis_auth)
                )
            except Exception as e:
                print(f"[{i:>4}] error: {e}")
                await asyncio.sleep(args.interval)
                i += 1
                continue
            gather_ms = (time.time() - t0) * 1000
            hl_lats.append(hl_ms)
            kis_lats.append(kis_ms)
            gather_lats.append(gather_ms)
            print(f"{i:>4} {hl_ms:>7.0f} {kis_ms:>7.0f} {gather_ms:>10.0f}")
            if csv_writer:
                csv_writer.writerow([i, time.time(), f"{hl_ms:.1f}", f"{kis_ms:.1f}", f"{gather_ms:.1f}"])
                csv_file.flush()
            await asyncio.sleep(args.interval)
            i += 1
    except KeyboardInterrupt:
        print("\nInterrupted by user.")

    print()
    print("=== Summary ===")
    print(summarize("HL    ", hl_lats))
    print(summarize("KIS   ", kis_lats))
    print(summarize("gather", gather_lats))

    if hl_lats and kis_lats:
        # parallelism index — gather가 max(HL,KIS)에 가까우면 진짜 병렬
        per_iter_max = [max(h, k) for h, k in zip(hl_lats, kis_lats)]
        per_iter_sum = [h + k for h, k in zip(hl_lats, kis_lats)]
        sum_gather = sum(gather_lats)
        sum_max = sum(per_iter_max)
        sum_serial = sum(per_iter_sum)
        # gather vs max (1.0 = perfect parallel, >1 = some overhead, ≫1 = 직렬)
        if sum_max > 0:
            par_ratio = sum_gather / sum_max
            # gather vs serial (1.0 = 완전 직렬, 0.5 = 완전 병렬 (양 leg 같을 때))
            serial_ratio = sum_gather / sum_serial if sum_serial > 0 else 0
            print()
            print("=== Parallelism analysis ===")
            print(f"  sum(HL+KIS) per-call serial = {sum_serial:>7.0f}ms")
            print(f"  sum(max(HL,KIS)) ideal-par  = {sum_max:>7.0f}ms")
            print(f"  sum(gather wall-clock)     = {sum_gather:>7.0f}ms")
            print(f"  gather / max  ratio = {par_ratio:>5.2f}  (1.0 = perfect parallel)")
            print(f"  gather / sum  ratio = {serial_ratio:>5.2f}  (0.5 = perfect par when HL≈KIS)")
            if par_ratio < 1.10:
                print("  → 진짜 병렬 작동 중. to_thread offload OK.")
            elif par_ratio < 1.50:
                print("  → 일부 직렬화 (small blocking). 추가 진단 필요.")
            else:
                print("  → 직렬화됨. HL sync 호출이 이벤트 루프 블록 의심.")

    # cleanup
    await kis_auth.close_rest_session()
    if csv_file:
        csv_file.close()
        print(f"\nCSV saved: {args.csv}")


def main():
    parser = argparse.ArgumentParser(description="HL + KIS REST latency probe (no real trades)")
    parser.add_argument("--n", type=int, default=30, help="iterations (default 30)")
    parser.add_argument("--interval", type=float, default=2.0,
                        help="sleep between iterations (KIS rate-limit 보호, default 2s)")
    parser.add_argument("--continuous", action="store_true",
                        help="Ctrl+C까지 계속 — 분포 분석용")
    parser.add_argument("--csv", default=None, help="output CSV (optional)")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
