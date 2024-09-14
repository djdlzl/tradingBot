from datetime import timedelta
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
    def get_previous_business_day(date, days_back=1):
        """
        주어진 날짜로부터 지정된 영업일 수만큼 이전의 영업일을 반환합니다.

        Args:
            date (datetime): 기준 날짜
            days_back (int, optional): 이전으로 갈 영업일 수. 기본값은 1.

        Returns:
            datetime: 계산된 이전 영업일
        """
        kr_holidays = holidays.country_holidays('KR')
        current_date = date
        while days_back > 0:
            current_date -= timedelta(days=1)
            if current_date.weekday() < 5 and current_date not in kr_holidays:
                days_back -= 1
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
        kr_holidays = holidays.country_holidays('KR')
        return date.weekday() < 5 and date not in kr_holidays