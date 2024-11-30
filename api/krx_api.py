from pykrx import stock
import pandas as pd
import datetime
import time


class KRXApi:
        
    def get_OHLCV(self, ticker, day_ago): 
        time.sleep(1)
        # 오늘 날짜
        today = datetime.datetime.now()
        # 15일 전 날짜
        start_date = (today - datetime.timedelta(days=day_ago)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')

        # 특정 종목의 15일간 OHLCV 데이터 가져오기
        df = stock.get_market_ohlcv(start_date, end_date, ticker)
        # print(df)


        return df