"""이전 영업일 계산 테스트"""
import sys
import os
from datetime import datetime, date

# 상위 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.date_utils import DateUtils

def test_previous_business_day_from_business_day():
    """영업일에서 이전 영업일 테스트"""
    test_dates = [
        (date(2025, 1, 24), "금"),  # 금요일
        (date(2025, 5, 29), "목"),  # 목요일
        (date(2025, 6, 9), "월"),   # 월요일
        (date(2025, 2, 12), "수"),  # 수요일
        (date(2025, 1, 17), "금"),  # 금요일
    ]
    
    print("\n===== 영업일에서 이전 영업일 계산 테스트 =====")
    for test_date, day_name in test_dates:
        # 현재 날짜가 영업일인지 확인
        is_bd = DateUtils.is_business_day(test_date)
        if not is_bd:
            print(f"{test_date}는 영업일이 아닙니다. 테스트 실패!")
            continue
            
        # 이전 영업일 계산
        prev_day = DateUtils.get_previous_business_day(test_date, 1)
        
        # 이전 날짜와의 차이 계산 (영업일 아닌 날 제외)
        days_diff = (test_date - prev_day).days
        weekdays = ['월', '화', '수', '목', '금', '토', '일']
        
        print(f"날짜: {test_date} ({day_name}) → 이전 영업일: {prev_day} ({weekdays[prev_day.weekday()]}) → 차이: {days_diff}일")
        
        # 검증: 이전 영업일이 맞는지 확인
        assert DateUtils.is_business_day(prev_day), f"{prev_day}는 영업일이 아닙니다!"

def test_consecutive_business_days():
    """연속된 영업일에서 이전 영업일 테스트"""
    # 2025년 1월 20일(월), 21일(화), 22일(수)은 연속된 영업일
    day1 = date(2025, 1, 20)  # 월요일
    day2 = date(2025, 1, 21)  # 화요일
    day3 = date(2025, 1, 22)  # 수요일
    
    print("\n===== 연속된 영업일 테스트 =====")
    
    # day3에서 1일 전 영업일은 day2여야 함
    prev_day = DateUtils.get_previous_business_day(day3, 1)
    print(f"{day3}의 1일 전 영업일: {prev_day} (예상: {day2})")
    assert prev_day == day2, f"1일 전 영업일이 {day2}가 아닙니다!"
    
    # day3에서 2일 전 영업일은 day1이어야 함
    prev_day = DateUtils.get_previous_business_day(day3, 2)
    print(f"{day3}의 2일 전 영업일: {prev_day} (예상: {day1})")
    assert prev_day == day1, f"2일 전 영업일이 {day1}이 아닙니다!"

if __name__ == "__main__":
    test_previous_business_day_from_business_day()
    test_consecutive_business_days()
    print("\n모든 테스트가 성공적으로 완료되었습니다.")
