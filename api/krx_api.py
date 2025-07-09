import pandas as pd
import datetime
import time
from pykrx import stock
from utils.date_utils import DateUtils



class KRXApi:
    def __init__(self):
        self.date_utils = DateUtils()
        
    def get_OHLCV(self, ticker, day_ago, upper_day_ago): 
        time.sleep(1)
        # 오늘 날짜
        today = datetime.datetime.now()
        # day_ago일 전 날짜
        day_ago = day_ago + upper_day_ago

        start_date = self.date_utils.get_previous_business_day(today,day_ago).strftime('%Y%m%d')
        end_date = self.date_utils.get_previous_business_day(today,upper_day_ago).strftime('%Y%m%d')

        # 특정 종목의 n일간 OHLCV 데이터 가져오기
        df = stock.get_market_ohlcv(start_date, end_date, ticker)

        return df
