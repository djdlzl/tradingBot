from datetime import datetime, timedelta
import holidays

class DateUtils:
    @staticmethod
    def get_business_days(start_date, end_date):
        kr_holidays = holidays.KR()
        business_days = []
        current_date = start_date
        while current_date <= end_date:
            if current_date.weekday() < 5 and current_date not in kr_holidays:
                business_days.append(current_date)
            current_date += timedelta(days=1)
        return business_days

    @staticmethod
    def get_previous_business_day(date, days_back=1):
        kr_holidays = holidays.KR()
        current_date = date
        while days_back > 0:
            current_date -= timedelta(days=1)
            if current_date.weekday() < 5 and current_date not in kr_holidays:
                days_back -= 1
        return current_date

    @staticmethod
    def is_business_day(date):
        kr_holidays = holidays.KR()
        return date.weekday() < 5 and date not in kr_holidays