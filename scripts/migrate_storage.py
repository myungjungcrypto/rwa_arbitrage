"""DB schema 마이그레이션 CLI (v1 → v2).

Storage.connect()가 자동 마이그레이션을 수행하므로 일반 운영에서는 이 스크립트
실행 불필요. 하지만 다음 경우에 유용:
  - 봇 정지 중에 사전 마이그레이션 (다운타임 단축)
  - 마이그레이션 결과 명시적으로 검증/리포트
  - --dry-run 으로 영향 범위 미리 확인

Usage:
    python3 scripts/migrate_storage.py                    # 실 마이그레이션
    python3 scripts/migrate_storage.py --db data/x.db     # 다른 DB 대상
    python3 scripts/migrate_storage.py --dry-run          # 변경 없이 분석만
    python3 scripts/migrate_storage.py --report           # 마이그레이션 후 row 카운트 리포트
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


# repo root를 path에 추가 (스크립트 단독 실행용)
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.data.storage import (
    SCHEMA_VERSION,
    MIGRATION_V2_ALTER_COLUMNS,
    Storage,
)


def report(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    print("=" * 60)
    print(f"  DB: {db_path}")
    print("=" * 60)

    # schema version
    try:
        row = con.execute(
            "SELECT value FROM schema_meta WHERE key='version'"
        ).fetchone()
        print(f"  schema_version: {row['value'] if row else '<missing>'}")
    except sqlite3.OperationalError:
        print("  schema_version: 1 (legacy, no schema_meta table)")

    # row counts
    tables = [
        "perp_prices", "futures_prices", "basis_spread", "funding_history",
        "orders", "positions", "daily_pnl", "leg_prices", "pairs",
    ]
    print("\n  Table row counts:")
    for t in tables:
        try:
            n = con.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
            print(f"    {t:<20} {n:>12,}")
        except sqlite3.OperationalError:
            print(f"    {t:<20} {'<missing>':>12}")

    # backfill check
    print("\n  pair_id backfill:")
    for tbl in ("basis_spread", "orders", "positions", "daily_pnl"):
        try:
            null_n = con.execute(
                f"SELECT COUNT(*) AS n FROM {tbl} WHERE pair_id IS NULL"
            ).fetchone()["n"]
            total = con.execute(f"SELECT COUNT(*) AS n FROM {tbl}").fetchone()["n"]
            print(f"    {tbl:<20} {total - null_n:>10,} / {total:>10,} 채워짐")
        except sqlite3.OperationalError:
            print(f"    {tbl:<20} <pair_id 컬럼 없음>")

    # pairs registry
    print("\n  Registered pairs:")
    try:
        rows = con.execute("SELECT * FROM pairs ORDER BY pair_id").fetchall()
        for r in rows:
            print(f"    {r['pair_id']}: {r['leg_a_exchange']}/{r['leg_a_symbol']} "
                  f"<-> {r['leg_b_exchange']}/{r['leg_b_symbol']}  ({r['gate']})")
    except sqlite3.OperationalError:
        print("    <pairs 테이블 없음>")
    con.close()


def dry_run(db_path: str) -> None:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    print("=" * 60)
    print(f"  DRY-RUN: {db_path}")
    print("=" * 60)

    # 현재 버전
    try:
        row = con.execute("SELECT value FROM schema_meta WHERE key='version'").fetchone()
        current = int(row["value"]) if row else 1
    except sqlite3.OperationalError:
        current = 1

    print(f"  current version: {current}")
    print(f"  target version : {SCHEMA_VERSION}")

    if current >= SCHEMA_VERSION:
        print("  ✓ 이미 최신 — 변경 없음")
        con.close()
        return

    # 추가될 컬럼 점검
    print("\n  변경 예정:")
    print("    - schema_meta 테이블 (이미 있으면 skip)")
    print("    - pairs, leg_prices 테이블 신규 생성")
    for table, col, type_def in MIGRATION_V2_ALTER_COLUMNS:
        existing = con.execute(f"PRAGMA table_info({table})").fetchall()
        has = any(r["name"] == col for r in existing)
        mark = "skip" if has else "ADD"
        print(f"    [{mark:<4}] {table}.{col} {type_def}")

    # backfill 영향
    print("\n  Backfill 영향 (legacy product='wti' → pair_id='wti_cme_hl'):")
    for tbl in ("basis_spread", "positions", "daily_pnl"):
        try:
            n = con.execute(
                f"SELECT COUNT(*) AS n FROM {tbl} WHERE product='wti'"
            ).fetchone()["n"]
            print(f"    {tbl:<20} {n:>10,} rows update 예정")
        except sqlite3.OperationalError:
            print(f"    {tbl:<20} <테이블 없음>")
    con.close()


def main():
    parser = argparse.ArgumentParser(description="Run DB schema migration to v2")
    parser.add_argument("--db", default="data/arbitrage.db", help="DB path")
    parser.add_argument("--dry-run", action="store_true",
                        help="변경 없이 영향 분석만 출력")
    parser.add_argument("--report", action="store_true",
                        help="마이그레이션 후 결과 리포트만 출력")
    args = parser.parse_args()

    if not Path(args.db).exists() and not args.dry_run:
        print(f"ERROR: DB file not found: {args.db}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        dry_run(args.db)
        return

    if args.report:
        report(args.db)
        return

    # 실 마이그레이션 — Storage.connect() 가 자동 처리
    print(f"Migrating {args.db} → v{SCHEMA_VERSION} ...")
    storage = Storage(args.db)
    storage.connect()
    storage.close()
    print("Done.")
    report(args.db)


if __name__ == "__main__":
    main()
