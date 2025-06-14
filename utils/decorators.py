from datetime import datetime
from functools import wraps
from utils.date_utils import DateUtils

def business_day_only(default=None):
    """
    데코레이터: 영업일이 아닐 경우 함수 실행을 건너뛰고 기본값을 반환합니다.
    default: 함수 실행을 생략할 경우 반환할 값 (None 또는 빈 리스트 등)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            now = datetime.now()
            if not DateUtils.is_business_day(now):
                print(f"{now.strftime('%Y-%m-%d')}은(는) 영업일이 아니므로 {func.__name__} 실행을 건너뜁니다.")
                return default
            return func(*args, **kwargs)
        return wrapper
    return decorator
