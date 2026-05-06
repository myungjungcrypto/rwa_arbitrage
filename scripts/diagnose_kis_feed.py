"""KIS 시세 freshness 진단 — CME 유료시세 라이센스 만료 감지.

EC2에서 실행:
    .venv/bin/python scripts/diagnose_kis_feed.py

체크 항목:
  1. KIS 인증 토큰 발급 가능한지 (app_key/app_secret 유효성)
  2. WebSocket approval_key 발급 가능한지 (CME 시세 신청 활성 검증)
  3. 최근 N분간 futures_prices DB row 인입 빈도
  4. 마지막 quote ts와 현재 시각의 차이 (stale 여부)
  5. CME 시장 시간 vs 데이터 흐름 비교
"""

from __future__ import annotations

import asyncio
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.utils.config import load_config
from src.exchange.kis import KISAuth, KISFuturesClient
from src.strategy.market_hours import is_cme_open


async def main() -> int:
    print("=" * 70)
    print("KIS CME 시세 진단")
    print("=" * 70)

    cfg = load_config("config/settings.yaml", "config/secrets.yaml")
    kis = cfg.kis

    print(f"\n[Config]")
    print(f"  enabled        = {kis.enabled}")
    print(f"  is_paper       = {kis.is_paper}")
    print(f"  cme_realtime   = {kis.cme_realtime}")
    print(f"  app_key set    = {bool(kis.app_key)} (len={len(kis.app_key)})")
    print(f"  app_secret set = {bool(kis.app_secret)} (len={len(kis.app_secret)})")
    print(f"  account_number = {kis.account_number or 'EMPTY'}")
    print(f"  base_url       = {kis.base_url}")

    if not kis.enabled:
        print("\n[FAIL] kis.enabled=false — 활성화부터 필요")
        return 1
    if not kis.app_key or not kis.app_secret:
        print("\n[FAIL] secrets.yaml의 kis.app_key/app_secret 비어있음")
        return 1

    # 1) REST 인증 토큰 (24h 유효, 1일 1회 발급)
    print(f"\n[1] REST OAuth 토큰 발급 시도...")
    auth = KISAuth(
        app_key=kis.app_key, app_secret=kis.app_secret,
        base_url=kis.base_url, is_paper=kis.is_paper,
        account_number=kis.account_number,
    )
    try:
        token = await auth.get_access_token()
    except Exception as e:
        print(f"  [FAIL] get_access_token raised: {e}")
        return 2
    if not token:
        print(f"  [FAIL] access_token 발급 실패")
        return 2
    print(f"  [OK ] access_token 발급됨 (length={len(token)})")

    # 2) WebSocket approval_key (CME 시세 라이센스 활성 시에만 발급)
    print(f"\n[2] WebSocket approval_key 발급 시도 (CME 시세 라이센스 검증)...")
    try:
        approval = await auth.get_approval_key()
    except Exception as e:
        print(f"  [FAIL] approval_key raised: {e}")
        print(f"        ↑ 라이센스 만료 시 흔히 발생 — 보통 KIS API 응답에 메시지 포함")
        return 3
    if not approval:
        print(f"  [FAIL] approval_key 비어있음")
        print(f"        ↑ CME 유료시세 신청 만료 가능성 큼")
        return 3
    print(f"  [OK ] approval_key 발급됨 (length={len(approval)})")

    # 3) DB에서 최근 KIS quote 도착 빈도 + 최신 ts
    print(f"\n[3] DB futures_prices 인입 빈도 확인...")
    db_path = cfg.db_path
    if not Path(db_path).exists():
        print(f"  [WARN] DB 없음: {db_path}")
        return 4
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    now = time.time()
    row = con.execute(
        "SELECT MAX(ts) AS last_ts, COUNT(*) AS n_total FROM futures_prices"
    ).fetchone()
    last_ts = row["last_ts"] or 0
    n_total = row["n_total"] or 0
    age_seconds = now - last_ts if last_ts else float("inf")

    print(f"  total rows           = {n_total:,}")
    print(f"  last quote ts        = {last_ts:.0f} ({_fmt_age(age_seconds)} 전)")

    # 최근 N분 윈도우별 카운트
    for minutes in (5, 30, 60):
        cutoff = now - minutes * 60
        n = con.execute(
            "SELECT COUNT(*) AS n FROM futures_prices WHERE ts >= ?", (cutoff,)
        ).fetchone()["n"]
        print(f"  rows in last {minutes:>3}m   = {n:,}")
    con.close()

    # 4) CME 시장 시간 + 판정
    from datetime import datetime, timezone
    cme_now = is_cme_open(datetime.now(timezone.utc))
    print(f"\n[4] CME 시장 상태: {'OPEN' if cme_now else 'CLOSED'}")
    print()

    if cme_now and age_seconds > 60:
        print(f"  [PROBLEM] CME 개장 중인데 마지막 quote {_fmt_age(age_seconds)} 전 — "
              f"stale (라이센스 만료 의심)")
        return 5
    elif cme_now and age_seconds < 60:
        print(f"  [OK ] CME 개장 + 최근 quote 정상 인입 → 라이센스 ACTIVE")
        return 0
    else:
        print(f"  [INFO] CME 폐장 시간이라 quote 인입 멈춰있는 게 정상")
        print(f"         라이센스 자체 유효성은 [1] [2] 단계로 확인 (둘 다 OK면 활성)")
        return 0


def _fmt_age(seconds: float) -> str:
    if seconds == float("inf"):
        return "데이터 없음"
    if seconds < 60: return f"{seconds:.0f}s"
    if seconds < 3600: return f"{seconds/60:.1f}m"
    if seconds < 86400: return f"{seconds/3600:.1f}h"
    return f"{seconds/86400:.1f}d"


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
