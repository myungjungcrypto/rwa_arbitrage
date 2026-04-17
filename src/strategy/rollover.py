from __future__ import annotations
"""CME 근월물 자동 롤오버 판별.

trade.xyz HIP-3 commodity perp oracle은 매월 5~10 영업일에 근월→차월 가중 롤오버.
본 모듈은 pure function으로 오늘 날짜가 주어지면 다음을 계산한다:
  - get_active_contract: KIS에 구독할 CME 심볼 (예: "MCLM26")
  - get_roll_weights: 롤 기간 중 (w_near, w_next) — 모니터링/로깅 용도

WTI/MCL 계약 구조 주의:
  CL/MCL의 delivery month는 전월에 만료된다. e.g., MCLK26(5월 인도분)은
  2026-04-21경 만기. 따라서 캘린더 4월의 front는 MCLK26(May)이고, 5월의
  front는 MCLM26(June). front_month_offset=1 로 이를 표현한다.

결정 규칙:
  base = today.year/month + front_month_offset
  BD(today) > roll_end_day 이면 base에 한 달 더 advance
  그 외는 base 유지
"""

from datetime import date, timedelta

# CME 월별 코드 (Jan..Dec)
MONTH_CODES = "FGHJKMNQUVXZ"


def month_code(month: int) -> str:
    if not 1 <= month <= 12:
        raise ValueError(f"month must be 1..12, got {month}")
    return MONTH_CODES[month - 1]


def build_symbol(prefix: str, year: int, month: int) -> str:
    """'MCL' + month_code + YY 형식."""
    return f"{prefix}{month_code(month)}{year % 100:02d}"


def next_contract(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def business_day_of_month(d: date, holidays: frozenset[date] = frozenset()) -> int:
    """해당 일자가 그 달의 몇 번째 영업일인지 반환.

    영업일 = 월-금, `holidays` 제외. 주말/휴일인 날은 "이전 영업일" 기준으로 카운트.

    Returns:
        >=1  해당 일이 영업일인 경우 n번째 영업일
        >=1  해당 일이 비영업일인 경우 그 월에서 당일 이전 마지막 영업일의 번호
             (해당 월 첫 영업일 이전이면 0)
    """
    count = 0
    last_bd = 0
    for day in range(1, d.day + 1):
        cur = date(d.year, d.month, day)
        if cur.weekday() < 5 and cur not in holidays:
            count += 1
            if cur <= d:
                last_bd = count
    return last_bd


def get_active_contract(
    today: date,
    prefix: str = "MCL",
    roll_end_day: int = 10,
    holidays: frozenset[date] = frozenset(),
    front_month_offset: int = 1,
) -> str:
    """오늘 시점의 활성 CME contract 심볼 반환.

    CL/MCL front-month는 "delivery month"가 캘린더 다음 달(=offset 1)인 계약.
    roll_end_day 초과 시 한 달 더 advance하여 post-roll contract 반환.

    Args:
        today: 기준 일자
        prefix: CME root symbol (MCL, CL, BZ 등)
        roll_end_day: 이 영업일 초과 시 post-roll 상태로 간주 (기본 10)
        holidays: 영업일 계산 시 제외할 휴일
        front_month_offset: front contract가 캘린더 월 대비 몇 개월 뒤인지
                           (MCL/CL/BZ=1, energy 현금정산 상품 등은 0)
    """
    y, m = today.year, today.month
    for _ in range(front_month_offset):
        y, m = next_contract(y, m)

    bd = business_day_of_month(today, holidays)
    if bd > roll_end_day:
        y, m = next_contract(y, m)

    return build_symbol(prefix, y, m)


def get_roll_weights(
    today: date,
    roll_start_day: int = 5,
    roll_end_day: int = 10,
    holidays: frozenset[date] = frozenset(),
) -> tuple[float, float]:
    """(w_near, w_next) 반환. 모니터링 용도 — 실제 oracle blending은 exchange가 수행.

    - BD < roll_start_day: (1.0, 0.0)
    - BD > roll_end_day: (0.0, 1.0)
    - 내부: 선형 보간 (k - start) / (end - start)
    """
    bd = business_day_of_month(today, holidays)
    if bd < roll_start_day:
        return 1.0, 0.0
    if bd > roll_end_day:
        return 0.0, 1.0
    span = roll_end_day - roll_start_day
    if span == 0:
        return 0.0, 1.0
    w_next = (bd - roll_start_day) / span
    return 1.0 - w_next, w_next


# ── CME 휴일 (미국 증권/선물 거래소 공통 휴일 근사치) ──

def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    if month == 12:
        first_next = date(year + 1, 1, 1)
    else:
        first_next = date(year, month + 1, 1)
    for days in range(1, 8):
        d = first_next - timedelta(days=days)
        if d.weekday() == weekday:
            return d
    raise RuntimeError("unreachable")


def us_market_holidays(year: int) -> frozenset[date]:
    """CME/NYSE 대략적인 시장 휴일. 완벽하지 않으므로 연말 검토 필요."""
    hols: set[date] = set()

    def adjust(d: date) -> date:
        if d.weekday() == 5:
            return d - timedelta(days=1)
        if d.weekday() == 6:
            return d + timedelta(days=1)
        return d

    hols.add(adjust(date(year, 1, 1)))
    hols.add(_nth_weekday(year, 1, 0, 3))
    hols.add(_nth_weekday(year, 2, 0, 3))
    hols.add(_good_friday(year))
    hols.add(_last_weekday(year, 5, 0))
    hols.add(adjust(date(year, 6, 19)))
    hols.add(adjust(date(year, 7, 4)))
    hols.add(_nth_weekday(year, 9, 0, 1))
    hols.add(_nth_weekday(year, 11, 3, 4))
    hols.add(adjust(date(year, 12, 25)))
    return frozenset(hols)


def _good_friday(year: int) -> date:
    # Anonymous Gregorian algorithm for Easter → Friday before
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    easter_month = (h + l - 7 * m + 114) // 31
    easter_day = ((h + l - 7 * m + 114) % 31) + 1
    easter = date(year, easter_month, easter_day)
    return easter - timedelta(days=2)
