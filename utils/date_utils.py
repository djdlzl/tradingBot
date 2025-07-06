""" 날짜를 워킹데이로 변환하는 모듈 """
from datetime import timedelta, date as dt # date 클래스를 추가로 가져옵니다.
# import holidays
import holidayskr

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
        # 해당 기간에 포함된 년도들 추출
        years = range(start_date.year, end_date.year + 1)
        kr_holidays_list = []
        
        # 각 년도별 공휴일 가져오기
        for year in years:
            holidays_for_year = holidayskr.year_holidays(year)
            kr_holidays_list.extend([holiday[0] for holiday in holidays_for_year])
        
        business_days = []
        current_date = start_date
        while current_date <= end_date:
            # 주말이 아니고 공휴일이 아닌 경우에만 영업일로 추가
            if current_date.weekday() < 5 and current_date.date() not in kr_holidays_list:
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
        # 공휴일 목록 가져오기
        all_holidays = DateUtils.get_holidays()
        
        # date가 datetime 객체인 경우 date 객체로 변환
        if hasattr(date, 'date'):
            current_date = date.date()
        else:
            current_date = date

        # 시작일이 영업일이 아닌 경우
        if current_date.weekday() >= 5 or current_date in all_holidays:
            # days_back이 1이면 가장 최근 영업일 반환
            if days_back == 1:
                while current_date.weekday() >= 5 or current_date in all_holidays:
                    current_date -= timedelta(days=1)
                return current_date
            # days_back이 1보다 크면 최근 영업일을 찾고 그로부터 days_back-1만큼 더 이동
            else:
                while current_date.weekday() >= 5 or current_date in all_holidays:
                    current_date -= timedelta(days=1)
                return DateUtils.get_previous_business_day(current_date, days_back - 1)
        
        # 시작일이 영업일인 경우
        business_days_count = 0
        while business_days_count < days_back:
            current_date -= timedelta(days=1)
            
            # 영업일인 경우만 카운트
            if current_date.weekday() < 5 and current_date not in all_holidays:
                business_days_count += 1
                
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
        # 날짜가 datetime 객체인 경우 date 객체로 변환
        if hasattr(date, 'date'):
            check_date = date.date()
        else:
            check_date = date
            
        # 한국의 공휴일 가져오기
        all_holidays = DateUtils.get_holidays()
        
        # 주말이거나 공휴일이면 영업일이 아님 (False 반환)
        if check_date.weekday() >= 5 or check_date in all_holidays:
            return False
            
        # 주말도 아니고 공휴일도 아니라면 영업일 (True 반환)
        return True


    @staticmethod
    def get_target_date(date, later=1):
        """
        강제 매도일자 계산. 당일에서 later일 후의 영업일 반환

        Args:
            date (datetime): 기준 날짜
            later (int): 추가할 영업일 수 (기본값: 1)

        Returns:
            datetime: 계산된 후일의 영업일
        """
        # 날짜가 datetime 객체인 경우 date 객체로 변환
        if hasattr(date, 'date'):
            target_date = date.date()
        else:
            target_date = date
        
        # 한국의 공휴일 가져오기
        all_holidays = DateUtils.get_holidays()
        
        # 출발 날짜가 영업일이 아니면 가장 가까운 영업일로 설정
        if not DateUtils.is_business_day(target_date):
            # 출발 날짜가 영업일이 될 때까지 다음 날로 이동
            while not DateUtils.is_business_day(target_date):
                target_date += timedelta(days=1)

        # later만큼 영업일 이동
        business_days_passed = 0
        while business_days_passed < later:
            target_date += timedelta(days=1)
            
            # 영업일인 경우만 카운트
            if DateUtils.is_business_day(target_date):
                business_days_passed += 1
    #############################################################################
        
        return target_date

    @staticmethod
    def get_holidays():
        """
        공휴일 받아오기 (holidayskr 패키지 활용)
        """
        # holidayskr 패키지를 사용하여 한국 공휴일 가져오기
        year = 2025
        kr_holidays_dict = {}
        
        # holidayskr로 한국 공휴일 가져오기 (year_holidays 함수 사용)
        holidays_list = holidayskr.year_holidays(year)
        
        # 날짜와 이름을 딕셔너리로 변환
        for holiday in holidays_list:
            date = holiday[0].strftime('%Y-%m-%d')
            name = holiday[1]
            kr_holidays_dict[date] = name
        
        # 추가 공휴일 수동으로 추가 (필요한 경우)
        additional_holidays = [
            dt(year, 6, 3),     # 기존에 추가했던 공휴일
            dt(year, 12, 31),    # 휴장일
            dt(year-1, 12, 31)    # 휴장일
        ]
        
        # 날짜 문자열을 date 객체로 변환하여 set에 추가
        holiday_dates_set = set()
        for date_str in kr_holidays_dict.keys():
            # 'YYYY-MM-DD' 형식을 year, month, day로 변환
            y, m, d = map(int, date_str.split('-'))
            holiday_dates_set.add(dt(y, m, d))
        
        # 추가 공휴일과 합치기
        all_holidays = holiday_dates_set.union(additional_holidays)
        
        # 디버깅을 위해 공휴일 목록 출력
        # print(f"2025년 공휴일 목록: {sorted([h.strftime('%Y-%m-%d') for h in all_holidays])}")
        
        return all_holidays
