from __future__ import annotations
"""market_hours 모듈 단위 테스트."""

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from src.strategy.market_hours import (
    CT,
    is_cme_open,
    next_closure_duration,
    time_until_close,
    time_until_open,
)
from src.strategy.rollover import us_market_holidays

UTC = timezone.utc


def ct_dt(y, m, d, h=0, minute=0) -> datetime:
    return datetime(y, m, d, h, minute, tzinfo=CT)


def utc_dt(y, m, d, h=0, minute=0) -> datetime:
    return datetime(y, m, d, h, minute, tzinfo=UTC)


class TestIsCmeOpen:
    def test_weekday_midday_open(self):
        # 2026-04-15 수 12:00 CT — 정상 장중
        assert is_cme_open(ct_dt(2026, 4, 15, 12, 0)) is True

    def test_weekday_daily_break(self):
        # 2026-04-15 수 16:30 CT — 일일 휴장
        assert is_cme_open(ct_dt(2026, 4, 15, 16, 30)) is False

    def test_weekday_after_reopen(self):
        # 2026-04-15 수 17:01 CT — 재개 직후
        assert is_cme_open(ct_dt(2026, 4, 15, 17, 1)) is True

    def test_friday_before_close(self):
        # 2026-04-17 금 15:59 CT
        assert is_cme_open(ct_dt(2026, 4, 17, 15, 59)) is True

    def test_friday_after_close(self):
        # 2026-04-17 금 16:00 CT — close 시각 포함 closed
        assert is_cme_open(ct_dt(2026, 4, 17, 16, 0)) is False
        assert is_cme_open(ct_dt(2026, 4, 17, 18, 0)) is False

    def test_saturday_closed(self):
        # 2026-04-18 토 — 전일 closed
        assert is_cme_open(ct_dt(2026, 4, 18, 10, 0)) is False
        assert is_cme_open(ct_dt(2026, 4, 18, 23, 59)) is False

    def test_sunday_before_reopen(self):
        # 2026-04-19 일 16:59 CT — 아직 closed
        assert is_cme_open(ct_dt(2026, 4, 19, 16, 59)) is False

    def test_sunday_after_reopen(self):
        # 2026-04-19 일 17:00 CT — 재개
        assert is_cme_open(ct_dt(2026, 4, 19, 17, 0)) is True

    def test_good_friday_closed(self):
        # 2026-04-03 Good Friday
        assert is_cme_open(ct_dt(2026, 4, 3, 12, 0)) is False

    def test_utc_input_converts(self):
        # UTC 21:00 on a Wednesday = CT 16:00 (CDT) → closed (break)
        # 2026-04-15 수 CDT, UTC offset -5h → 21:00 UTC = 16:00 CT
        assert is_cme_open(utc_dt(2026, 4, 15, 21, 0)) is False
        # 23:00 UTC = 18:00 CT → open
        assert is_cme_open(utc_dt(2026, 4, 15, 23, 0)) is True


class TestDSTBoundary:
    """DST 전환 — 2026 미국: 3/8 (CDT 시작), 11/1 (CST 시작)."""

    def test_pre_dst_winter(self):
        # 2026-03-05 목 12:00 CT (CST) = 18:00 UTC
        assert is_cme_open(utc_dt(2026, 3, 5, 18, 0)) is True

    def test_post_dst_summer(self):
        # 2026-03-12 목 12:00 CT (CDT) = 17:00 UTC
        assert is_cme_open(utc_dt(2026, 3, 12, 17, 0)) is True

    def test_post_fall_back(self):
        # 2026-11-05 목 12:00 CT (CST) = 18:00 UTC
        assert is_cme_open(utc_dt(2026, 11, 5, 18, 0)) is True


class TestTimeUntilClose:
    def test_open_returns_delta(self):
        # 2026-04-15 수 15:00 CT → close 16:00 CT → 1h
        d = time_until_close(ct_dt(2026, 4, 15, 15, 0))
        assert d == timedelta(hours=1)

    def test_closed_returns_none(self):
        assert time_until_close(ct_dt(2026, 4, 18, 12, 0)) is None  # Saturday

    def test_friday_afternoon_to_weekend_close(self):
        # 2026-04-17 금 14:30 CT → 16:00 = 1h30m
        d = time_until_close(ct_dt(2026, 4, 17, 14, 30))
        assert d == timedelta(hours=1, minutes=30)


class TestTimeUntilOpen:
    def test_saturday_to_sunday_open(self):
        # 2026-04-18 토 10:00 CT → 2026-04-19 일 17:00 CT
        d = time_until_open(ct_dt(2026, 4, 18, 10, 0))
        assert d == timedelta(hours=31)

    def test_open_returns_none(self):
        assert time_until_open(ct_dt(2026, 4, 15, 12, 0)) is None

    def test_daily_break_to_reopen(self):
        # 2026-04-15 수 16:30 CT → 17:00 CT = 30m
        d = time_until_open(ct_dt(2026, 4, 15, 16, 30))
        assert d == timedelta(minutes=30)


class TestNextClosureDuration:
    def test_weekday_midday_returns_daily_break(self):
        # 2026-04-15 수 12:00 CT → 다음 close는 당일 16:00, reopen 17:00 → 1h
        d = next_closure_duration(ct_dt(2026, 4, 15, 12, 0))
        assert d == timedelta(hours=1)

    def test_friday_afternoon_returns_weekend(self):
        # 2026-04-17 금 12:00 CT → close 16:00, reopen 일 17:00 = 49h
        d = next_closure_duration(ct_dt(2026, 4, 17, 12, 0))
        assert d == timedelta(hours=49)

    def test_weekend_to_reopen(self):
        # 2026-04-18 토 12:00 CT → 일 17:00 = 29h
        d = next_closure_duration(ct_dt(2026, 4, 18, 12, 0))
        assert d == timedelta(hours=29)

    def test_thu_afternoon_before_good_friday_returns_daily_break(self):
        # 2026-04-02 목 14:00 CT → 다음 close는 당일 16:00 daily break
        # break 종료 17:00 CT reopen (Fri 00:00 holiday 시작까지는 open 상태)
        # → 이 closure의 length = 1h (다음 long closure는 별도)
        d = next_closure_duration(ct_dt(2026, 4, 2, 14, 0))
        assert d == timedelta(hours=1)

    def test_thu_late_evening_returns_good_friday_closure(self):
        # 2026-04-02 목 23:00 CT → 다음 close는 Fri 4/3 00:00 (holiday 시작)
        # Fri 4/3 00:00 CT → Sun 4/5 17:00 CT = 65h
        d = next_closure_duration(ct_dt(2026, 4, 2, 23, 0))
        assert d == timedelta(hours=65)


class TestHolidayOverride:
    def test_custom_holidays_parameter(self):
        custom = frozenset([date(2026, 6, 15)])
        assert is_cme_open(ct_dt(2026, 6, 15, 12, 0), holidays=custom) is False
        # 기본은 휴일 아님
        assert is_cme_open(ct_dt(2026, 6, 15, 12, 0), holidays=frozenset()) is True
