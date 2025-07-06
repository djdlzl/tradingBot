"""한국 공휴일 및 영업일 판단 로직 테스트"""
import sys
import os
from datetime import datetime, date, timedelta
import random

# 상위 디렉토리를 모듈 검색 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 필요한 모듈 임포트
import holidayskr
from utils.date_utils import DateUtils


class HolidayTest:
    """날짜 유틸리티 기능 테스트 클래스"""
    
    @staticmethod
    def get_all_holidays_2025():
        """2025년 한국 공휴일 목록 반환"""
        holidays_2025 = holidayskr.year_holidays(2025)
        return [(holiday[0], holiday[1]) for holiday in holidays_2025]

    @staticmethod
    def is_weekend(test_date):
        """주말 여부 확인"""
        return test_date.weekday() >= 5
    
    @staticmethod
    def generate_test_dates():
        """다양한 테스트용 날짜 생성 (토요일, 일요일, 공휴일, 영업일)"""
        # 2025년 공휴일 목록 가져오기
        holidays = HolidayTest.get_all_holidays_2025()
        holiday_dates = [h[0] for h in holidays]
        
        # 테스트 날짜 세트 초기화
        test_dates = {
            '주말': [],
            '공휴일': [],
            '영업일': [],
            '연속_영업일': [],
            '연속_주말_공휴일': []
        }
        
        # 2025년 1월부터 12월까지 날짜 순회
        start_date = date(2025, 1, 1)
        end_date = date(2025, 12, 31)
        delta = end_date - start_date
        
        # 주말, 공휴일, 영업일 분류
        for i in range(delta.days + 1):
            day = start_date + timedelta(days=i)
            
            if day in holiday_dates:
                test_dates['공휴일'].append(day)
            elif day.weekday() >= 5:  # 토요일(5) 또는 일요일(6)
                test_dates['주말'].append(day)
            else:
                test_dates['영업일'].append(day)
        
        # 무작위 선택
        random.seed(2025)  # 결과 재현성을 위한 시드 설정
        test_dates['주말'] = random.sample(test_dates['주말'], min(5, len(test_dates['주말'])))
        test_dates['공휴일'] = random.sample(test_dates['공휴일'], min(5, len(test_dates['공휴일'])))
        test_dates['영업일'] = random.sample(test_dates['영업일'], min(5, len(test_dates['영업일'])))
        
        # 연속 영업일 (3일) 찾기
        for i in range(len(test_dates['영업일']) - 2):
            d1 = test_dates['영업일'][i]
            d2 = test_dates['영업일'][i + 1]
            d3 = test_dates['영업일'][i + 2]
            if (d2 - d1).days == 1 and (d3 - d2).days == 1:
                test_dates['연속_영업일'] = [d1, d2, d3]
                break
        
        # 연속 주말/공휴일 찾기 (금요일이 공휴일인 경우 포함)
        for holiday in holiday_dates:
            if holiday.weekday() == 4:  # 금요일이 공휴일
                test_dates['연속_주말_공휴일'] = [
                    holiday,  # 금요일(공휴일)
                    holiday + timedelta(days=1),  # 토요일
                    holiday + timedelta(days=2)   # 일요일
                ]
                break
        
        return test_dates

    @staticmethod
    def test_previous_business_day(test_dates):
        """get_previous_business_day 함수 테스트"""
        print("\n===== 이전 영업일 테스트 =====")
        
        # 주말 테스트
        print("\n[주말 날짜에서 이전 영업일 테스트]")
        for day in test_dates['주말']:
            prev_business_day = DateUtils.get_previous_business_day(day, 1)
            print(f"날짜: {day} ({['월','화','수','목','금','토','일'][day.weekday()]}) → 이전 영업일: {prev_business_day} ({['월','화','수','목','금','토','일'][prev_business_day.weekday()]})")
            # 검증: 이전 영업일이 주말이 아니고, 공휴일이 아니어야 함
            assert prev_business_day.weekday() < 5, "이전 영업일이 주말입니다!"
            assert prev_business_day not in [h[0] for h in HolidayTest.get_all_holidays_2025()], "이전 영업일이 공휴일입니다!"
        
        # 공휴일 테스트
        print("\n[공휴일에서 이전 영업일 테스트]")
        for day in test_dates['공휴일']:
            holiday_name = next((h[1] for h in HolidayTest.get_all_holidays_2025() if h[0] == day), "알 수 없음")
            prev_business_day = DateUtils.get_previous_business_day(day, 1)
            print(f"날짜: {day} ({['월','화','수','목','금','토','일'][day.weekday()]}, {holiday_name}) → 이전 영업일: {prev_business_day} ({['월','화','수','목','금','토','일'][prev_business_day.weekday()]})")
            # 검증: 이전 영업일이 주말이 아니고, 공휴일이 아니어야 함
            assert prev_business_day.weekday() < 5, "이전 영업일이 주말입니다!"
            assert prev_business_day not in [h[0] for h in HolidayTest.get_all_holidays_2025()], "이전 영업일이 공휴일입니다!"
        
        # 영업일 테스트
        print("\n[영업일에서 이전 영업일 테스트]")
        for day in test_dates['영업일']:
            prev_business_day = DateUtils.get_previous_business_day(day, 1)
            print(f"날짜: {day} ({['월','화','수','목','금','토','일'][day.weekday()]}) → 이전 영업일: {prev_business_day} ({['월','화','수','목','금','토','일'][prev_business_day.weekday()]})")
            # 검증: 이전 영업일이 주말이 아니고, 공휴일이 아니어야 함
            assert prev_business_day.weekday() < 5, "이전 영업일이 주말입니다!"
            assert prev_business_day not in [h[0] for h in HolidayTest.get_all_holidays_2025()], "이전 영업일이 공휴일입니다!"
        
        # 연속 영업일 테스트
        if test_dates['연속_영업일']:
            print("\n[연속 영업일에서 여러 영업일 뒤로 테스트]")
            day = test_dates['연속_영업일'][2]  # 세번째 날
            for days_back in [1, 2, 3]:
                prev_business_day = DateUtils.get_previous_business_day(day, days_back)
                expected_day = test_dates['연속_영업일'][2 - days_back]
                print(f"날짜: {day} → {days_back}일 전 영업일: {prev_business_day} (예상: {expected_day})")
                assert prev_business_day == expected_day, f"{days_back}일 전 영업일이 예상과 다릅니다!"
        
        # 연속 주말/공휴일 테스트
        if test_dates['연속_주말_공휴일']:
            print("\n[연속 주말/공휴일 전후 테스트]")
            consecutive_days = test_dates['연속_주말_공휴일']
            monday_after = consecutive_days[2] + timedelta(days=1)  # 일요일 다음 월요일
            
            # 월요일에서 이전 영업일 확인 (목요일이어야 함)
            prev_business_day = DateUtils.get_previous_business_day(monday_after, 1)
            print(f"월요일({monday_after})의 이전 영업일: {prev_business_day}")
            
            # 금요일이 공휴일인 경우, 그 이전 목요일이 나와야 함
            assert (monday_after - prev_business_day).days >= 4, "연속 주말/공휴일 이후의 이전 영업일이 잘못되었습니다!"

    @staticmethod
    def test_is_business_day(test_dates):
        """is_business_day 함수 테스트"""
        print("\n===== 영업일 여부 테스트 =====")
        
        # 주말 테스트
        print("\n[주말 날짜의 영업일 여부 테스트]")
        for day in test_dates['주말']:
            result = DateUtils.is_business_day(day)
            print(f"날짜: {day} ({['월','화','수','목','금','토','일'][day.weekday()]}) → 영업일: {result}")
            assert not result, f"{day}는 주말인데 영업일로 판단됨!"
        
        # 공휴일 테스트
        print("\n[공휴일의 영업일 여부 테스트]")
        for day in test_dates['공휴일']:
            holiday_name = next((h[1] for h in HolidayTest.get_all_holidays_2025() if h[0] == day), "알 수 없음")
            result = DateUtils.is_business_day(day)
            print(f"날짜: {day} ({['월','화','수','목','금','토','일'][day.weekday()]}, {holiday_name}) → 영업일: {result}")
            assert not result, f"{day}는 공휴일인데 영업일로 판단됨!"
        
        # 영업일 테스트
        print("\n[영업일의 영업일 여부 테스트]")
        for day in test_dates['영업일']:
            result = DateUtils.is_business_day(day)
            print(f"날짜: {day} ({['월','화','수','목','금','토','일'][day.weekday()]}) → 영업일: {result}")
            assert result, f"{day}는 영업일인데 영업일이 아닌 것으로 판단됨!"

    @staticmethod
    def test_special_cases():
        """특수 케이스 테스트"""
        print("\n===== 특수 케이스 테스트 =====")
        
        # 7월 4일 테스트 (미국 독립기념일이지만 한국에서는 영업일)
        july_4 = date(2025, 7, 4)
        is_business_day = DateUtils.is_business_day(july_4)
        print(f"\n2025년 7월 4일 영업일 여부: {is_business_day}")
        assert is_business_day, "7월 4일은 한국에서 영업일이어야 합니다!"
        
        # 7월 5일(토요일)의 이전 영업일은 7월 4일이어야 함
        july_5 = date(2025, 7, 5)
        prev_day = DateUtils.get_previous_business_day(july_5, 1)
        print(f"2025년 7월 5일(토요일)의 이전 영업일: {prev_day}")
        assert prev_day == july_4, "7월 5일(토요일)의 이전 영업일은 7월 4일이어야 합니다!"
        
        # 연속 공휴일 테스트 (설날 연휴)
        seollal_eve = date(2025, 1, 28)  # 설날 전날
        seollal = date(2025, 1, 29)      # 설날
        seollal_next = date(2025, 1, 30) # 설날 다음날
        
        # 설날 다음날의 이전 영업일은 설날 전 마지막 영업일이어야 함
        prev_day = DateUtils.get_previous_business_day(seollal_next, 1)
        print(f"설날 다음날({seollal_next})의 이전 영업일: {prev_day}")
        assert prev_day < seollal_eve, "설날 연휴의 이전 영업일 계산이 잘못되었습니다!"


def run_tests():
    """모든 테스트 실행"""
    print("===== 한국 공휴일 및 영업일 판단 로직 테스트 시작 =====")
    
    # 테스트용 날짜 생성
    test_dates = HolidayTest.generate_test_dates()
    
    # 각 테스트 케이스 실행
    HolidayTest.test_is_business_day(test_dates)
    HolidayTest.test_previous_business_day(test_dates)
    HolidayTest.test_special_cases()
    
    print("\n===== 모든 테스트 완료 =====")


if __name__ == "__main__":
    run_tests()