from __future__ import annotations
"""CME WTI/MCL 장 시간 판정 모듈.

CME Globex hours (CL, MCL):
  - 정상: 일 17:00 CT ~ 금 16:00 CT
  - 일일 휴장: Mon-Thu 16:00 ~ 17:00 CT (1시간 유지보수 break)
  - 주말: 금 16:00 CT ~ 일 17:00 CT (~49h)
  - 토요일: 전일 closed

DST 처리는 `zoneinfo.ZoneInfo("America/Chicago")` 로 UTC→CT 변환 후 판정.
CME 전용 거래소 휴일(예: Good Friday, Christmas)은 `us_market_holidays()` 재사용.
"""

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from .rollover import us_market_holidays

CT = ZoneInfo("America/Chicago")
UTC = timezone.utc

# 일일 세션 경계 (CT 기준)
SESSION_OPEN = time(17, 0)   # 17:00 CT open
SESSION_CLOSE = time(16, 0)  # 16:00 CT close


def _to_ct(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    return now.astimezone(CT)


def is_cme_open(now: datetime, holidays: frozenset[date] | None = None) -> bool:
    """현재 CME WTI 장중 여부.

    규칙 (CT 기준):
      - 토요일 전일 closed
      - 일요일: 17:00 이후 open
      - 월~목: 17:00 이전 open (전일 세션 연속) / 16:00~17:00 break / 17:00 이후 new session open
      - 금요일: 16:00 이전 open
      - holidays 해당 일자: 전일 closed
    """
    hols = holidays if holidays is not None else us_market_holidays(now.year)
    ct = _to_ct(now)

    if ct.date() in hols:
        return False

    wd = ct.weekday()  # 0=Mon ... 6=Sun
    t = ct.time()

    if wd == 5:  # Sat
        return False
    if wd == 6:  # Sun — open 17:00 이후
        return t >= SESSION_OPEN
    if wd == 4:  # Fri — 16:00 전까지
        return t < SESSION_CLOSE
    # Mon-Thu — 16:00-17:00 휴장만 제외
    if SESSION_CLOSE <= t < SESSION_OPEN:
        return False
    return True


def _next_transition(now: datetime, holidays: frozenset[date]) -> tuple[datetime, bool]:
    """현재 상태에서 다음 전환 시점(UTC)과 전환 후 is_open 여부.

    is_open(now) == True  → 반환값 (다음 close 시각, False)
    is_open(now) == False → 반환값 (다음 open 시각, True)

    후보 전환 시점:
      - 일요일 17:00 CT 세션 open
      - 월-목 16:00 CT break close, 17:00 CT reopen
      - 금요일 16:00 CT 주간 close
      - 휴일 경계: 휴일 시작/종료 자정 (non-holiday→holiday 또는 반대)
    전환 시점에서 실제 is_cme_open 상태가 open_now와 다른 것을 첫 번째로 찾음.
    """
    from datetime import time as _time
    ct_now = _to_ct(now)
    open_now = is_cme_open(now, holidays)

    candidates: list[datetime] = []
    # 현재일 포함 최대 14일 전방 스캔
    for day_offset in range(0, 15):
        d = ct_now.date() + timedelta(days=day_offset)
        wd = d.weekday()
        is_hol = d in holidays

        # 세션 경계 (휴일이면 제외)
        if not is_hol:
            if wd == 6:  # Sunday
                candidates.append(datetime.combine(d, SESSION_OPEN, tzinfo=CT))
            elif wd in (0, 1, 2, 3):  # Mon-Thu
                candidates.append(datetime.combine(d, SESSION_CLOSE, tzinfo=CT))
                candidates.append(datetime.combine(d, SESSION_OPEN, tzinfo=CT))
            elif wd == 4:  # Friday
                candidates.append(datetime.combine(d, SESSION_CLOSE, tzinfo=CT))

        # 휴일 경계: 전일과 상태 달라지면 자정이 전환점
        prev_hol = (d - timedelta(days=1)) in holidays
        if prev_hol != is_hol:
            candidates.append(datetime.combine(d, _time(0, 0), tzinfo=CT))

    future = sorted([c for c in candidates if c > ct_now])
    for c in future:
        state = is_cme_open(c, holidays)
        if state != open_now:
            return c.astimezone(UTC), state

    raise RuntimeError("No transition found within 14-day window")


def time_until_close(
    now: datetime, holidays: frozenset[date] | None = None
) -> timedelta | None:
    """장중일 때 다음 close까지 시간. 폐장 중이면 None."""
    hols = holidays if holidays is not None else us_market_holidays(now.year)
    if not is_cme_open(now, hols):
        return None
    next_ts, open_after = _next_transition(now, hols)
    return next_ts - now


def time_until_open(
    now: datetime, holidays: frozenset[date] | None = None
) -> timedelta | None:
    """폐장 중일 때 다음 open까지 시간. 장중이면 None."""
    hols = holidays if holidays is not None else us_market_holidays(now.year)
    if is_cme_open(now, hols):
        return None
    next_ts, open_after = _next_transition(now, hols)
    return next_ts - now


def next_closure_duration(
    now: datetime, holidays: frozenset[date] | None = None
) -> timedelta:
    """지금 또는 다음 closure의 지속시간.

    - 장중: 다음 close부터 그 후 open까지 (즉 다가오는 휴장 길이)
    - 폐장중: 지금부터 다음 open까지
    """
    hols = holidays if holidays is not None else us_market_holidays(now.year)

    if not is_cme_open(now, hols):
        next_open, _ = _next_transition(now, hols)
        return next_open - now

    next_close, _ = _next_transition(now, hols)
    # next_close 이후의 다음 open 찾기
    after_close = next_close + timedelta(seconds=1)
    next_open, _ = _next_transition(after_close, hols)
    return next_open - next_close


def from_timestamp(ts: float) -> datetime:
    """epoch → UTC-aware datetime. signals.py에서 편의상 사용."""
    return datetime.fromtimestamp(ts, tz=UTC)
