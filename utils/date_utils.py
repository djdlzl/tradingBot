""" 날짜를 워킹데이로 변환하는 모듈 """
from datetime import timedelta, date as dt # date 클래스를 추가로 가져옵니다.
import holidays

class DateUtils:
    """날짜 관련 유틸리티 기능을 제공하는 클래스입니다."""

    @staticmethod
    def get_business_days(start_date, end_date):
        """
        주어진 기간 내의 영업일을 반환합니다.

        Args:
            start_date (datetime): 시작 날짜
            end_date (datetime): 종료 날짜

        Returns:
            list: 영업일 목록
        """
        kr_holidays = holidays.country_holidays('KR')
        business_days = []
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5 and current_date not in kr_holidays:
                business_days.append(current_date)
            current_date += timedelta(days=1)
        return business_days

    @staticmethod
    def get_previous_business_day(date, days_back):
        """
        주어진 날짜로부터 지정된 영업일 수만큼 이전의 영업일을 반환합니다.

        Args:
            date (datetime): 기준 날짜
            days_back (int, optional): 이전으로 갈 영업일 수. 기본값은 1.

        Returns:
            datetime: 계산된 이전 영업일
        """
        # 한국의 2024년 공휴일 가져오기
        kr_holidays = holidays.CountryHoliday('KR', years=2024)  
        
        # 추가 공휴일 수동으로 추가 (datetime.date 객체로 추가)
        additional_holidays = [
            dt(2024, 10, 1)
        ]

        # 기존 공휴일에 추가 공휴일 합치기
        all_holidays = set(kr_holidays.keys()).union(additional_holidays)
        current_date = date
        current_date = date.date() 

        while days_back > 0:

            # 주말 건너뛰기
            if current_date.weekday() == 5:
                current_date -= timedelta(days=1)
                continue
            elif current_date.weekday() == 6:
                current_date -= timedelta(days=2)
                continue

            current_date -= timedelta(days=1)
            days_back -= 1

            #공휴일 건너뛰기
            if (current_date in all_holidays) and (current_date.weekday() < 5):
                current_date -= timedelta(days=1)

        return current_date

    @staticmethod
    def is_business_day(date):
        """
        주어진 날짜가 영업일인지 확인합니다.

        Args:
            date (datetime): 확인할 날짜

        Returns:
            bool: 영업일이면 True, 아니면 False
        """
        # 한국의 2024년 공휴일 가져오기
        kr_holidays = holidays.CountryHoliday('KR', years=2024)  
        
        # 추가 공휴일 수동으로 추가 (datetime.date 객체로 추가)
        additional_holidays = [
            dt(2024, 10, 1)
        ]

        # 기존 공휴일에 추가 공휴일 합치기
        all_holidays = set(kr_holidays.keys()).union(additional_holidays)
        current_date = date
        current_date = date.date() 

        while current_date.weekday() > 4:
            # 주말 건너뛰기
            if current_date.weekday() == 5:
                current_date -= timedelta(days=1)
            elif current_date.weekday() == 6:
                current_date -= timedelta(days=2)

            #공휴일 건너뛰기
            current_date -= timedelta(days=1)

        while (current_date in all_holidays) and (current_date.weekday() < 5):
            current_date -= timedelta(days=1)
            
        return current_date

    @staticmethod
    def get_holidays():
        """
        공휴일 받아오기
        """
        
        # 한국의 2024년 공휴일 가져오기
        kr_holidays = holidays.CountryHoliday('KR', years=2024)  
        
        # 추가 공휴일 수동으로 추가 (datetime.date 객체로 추가)
        additional_holidays = [
            dt(2024, 10, 1)
        ]

        # 기존 공휴일에 추가 공휴일 합치기
        all_holidays = set(kr_holidays.keys()).union(additional_holidays)
        
        return all_holidays
