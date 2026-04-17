from __future__ import annotations
"""rollover 모듈 단위 테스트."""

from datetime import date

import pytest

from src.strategy.rollover import (
    build_symbol,
    business_day_of_month,
    get_active_contract,
    get_roll_weights,
    month_code,
    next_contract,
    us_market_holidays,
)


class TestMonthCode:
    @pytest.mark.parametrize("month,expected", [
        (1, "F"), (2, "G"), (3, "H"), (4, "J"),
        (5, "K"), (6, "M"), (7, "N"), (8, "Q"),
        (9, "U"), (10, "V"), (11, "X"), (12, "Z"),
    ])
    def test_all_months(self, month, expected):
        assert month_code(month) == expected

    def test_invalid(self):
        with pytest.raises(ValueError):
            month_code(0)
        with pytest.raises(ValueError):
            month_code(13)


class TestBuildSymbol:
    def test_mcl_2026(self):
        assert build_symbol("MCL", 2026, 4) == "MCLJ26"
        assert build_symbol("MCL", 2026, 5) == "MCLK26"
        assert build_symbol("MCL", 2026, 6) == "MCLM26"

    def test_year_wrap(self):
        assert build_symbol("MCL", 2027, 1) == "MCLF27"
        assert build_symbol("CL", 2030, 12) == "CLZ30"


class TestNextContract:
    def test_mid_year(self):
        assert next_contract(2026, 4) == (2026, 5)

    def test_december_wraps(self):
        assert next_contract(2026, 12) == (2027, 1)


class TestBusinessDayOfMonth:
    def test_first_business_day(self):
        # 2026-04-01 수요일 → 1st BD
        assert business_day_of_month(date(2026, 4, 1)) == 1

    def test_after_weekend(self):
        # 2026-04-06 월 → 4th BD (4/1 수, 2 목, 3 금, 6 월)
        assert business_day_of_month(date(2026, 4, 6)) == 4

    def test_weekend_uses_previous_bd(self):
        # 4/4 토 → 4/3 금이 3번째 영업일 → 3
        assert business_day_of_month(date(2026, 4, 4)) == 3

    def test_holiday_skipped(self):
        # Good Friday 2026 = 4월 3일. 휴일로 취급하면 4/3 = 2번째 BD (1수, 2목, 3금→skip)
        hols = us_market_holidays(2026)
        assert date(2026, 4, 3) in hols
        # 4/6 월: 1수, 2목, 3금(skip), 6월 → 3번째 영업일
        assert business_day_of_month(date(2026, 4, 6), hols) == 3

    def test_bd10_of_april_2026_without_holidays(self):
        # BD 10 (no-holiday): 4/14 화
        assert business_day_of_month(date(2026, 4, 14)) == 10

    def test_bd10_of_april_2026_with_holidays(self):
        # Good Friday 제외 시 BD 10 = 4/15 수
        hols = us_market_holidays(2026)
        assert business_day_of_month(date(2026, 4, 15), hols) == 10


class TestGetActiveContract:
    """MCL front-month는 캘린더 월 + 1 (delivery month 기준)."""

    def test_pre_roll_uses_front_month(self):
        # 2026-04-06 월 (BD=4, no-holiday) → front = May = MCLK26
        assert get_active_contract(date(2026, 4, 6)) == "MCLK26"

    def test_during_roll_keeps_front_month(self):
        # 2026-04-10 금 (BD=8, no-holiday) → roll window 내, front 유지
        assert get_active_contract(date(2026, 4, 10)) == "MCLK26"

    def test_after_roll_uses_next_month_no_holidays(self):
        # 2026-04-15 수 (BD=11, no-holiday) → post-roll = June = MCLM26
        assert get_active_contract(date(2026, 4, 15)) == "MCLM26"

    def test_today_2026_04_17_after_roll(self):
        # 2026-04-17 금, BD=13 → MCLM26 (project CLAUDE.md와 일치)
        assert get_active_contract(date(2026, 4, 17)) == "MCLM26"

    def test_december_wraps_to_next_year(self):
        # 12월: front = 다음해 1월(F). BD>10이면 2월(G) → MCLG27
        assert get_active_contract(date(2026, 12, 31)) == "MCLG27"

    def test_november_pre_roll(self):
        # 2026-11-03 화, BD=2 → front = Dec = MCLZ26
        assert get_active_contract(date(2026, 11, 3)) == "MCLZ26"

    def test_november_post_roll_wraps_to_january(self):
        # 2026-11-16 월, BD>10 → post-roll = Jan27 = MCLF27
        assert get_active_contract(date(2026, 11, 16)) == "MCLF27"

    def test_with_holidays_shifts_roll(self):
        # Good Friday 2026-04-03 휴일 시 4/14 = BD 9 → front 유지 (MCLK26)
        hols = us_market_holidays(2026)
        assert get_active_contract(date(2026, 4, 14), holidays=hols) == "MCLK26"
        # 4/16 목 = BD 11 → post-roll (MCLM26)
        assert get_active_contract(date(2026, 4, 16), holidays=hols) == "MCLM26"

    def test_custom_prefix(self):
        # BZ (Brent) 6월 시점 → front = Jul = BZN26
        assert get_active_contract(date(2026, 6, 2), prefix="BZ") == "BZN26"

    def test_offset_zero_returns_current_month(self):
        # offset=0 (정산 인덱스 선물 등 가상 케이스)
        assert get_active_contract(date(2026, 4, 6), front_month_offset=0) == "MCLJ26"


class TestGetRollWeights:
    def test_before_roll(self):
        # BD 1~4 → (1, 0)
        assert get_roll_weights(date(2026, 4, 1)) == (1.0, 0.0)
        assert get_roll_weights(date(2026, 4, 6)) == (1.0, 0.0)

    def test_at_roll_start(self):
        # BD=5 → (1, 0)
        w_near, w_next = get_roll_weights(date(2026, 4, 7))
        assert w_near == 1.0
        assert w_next == 0.0

    def test_mid_roll(self):
        # BD=8 → (0.4, 0.6)
        w_near, w_next = get_roll_weights(date(2026, 4, 10))
        assert abs(w_near - 0.4) < 1e-9
        assert abs(w_next - 0.6) < 1e-9

    def test_after_roll(self):
        # BD > 10 → (0, 1)
        assert get_roll_weights(date(2026, 4, 15)) == (0.0, 1.0)


class TestUSMarketHolidays:
    def test_2026_includes_good_friday(self):
        hols = us_market_holidays(2026)
        assert date(2026, 4, 3) in hols

    def test_2026_includes_new_year(self):
        hols = us_market_holidays(2026)
        assert date(2026, 1, 1) in hols

    def test_2026_christmas(self):
        hols = us_market_holidays(2026)
        # 2026-12-25 금
        assert date(2026, 12, 25) in hols

    def test_juneteenth_saturday_adjusted(self):
        # 2026-06-19 is Friday — no adjustment
        hols = us_market_holidays(2026)
        assert date(2026, 6, 19) in hols
