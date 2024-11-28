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

        # 종가 대비 다음날 고가의 등락률 계산
        percentage_diff = []
        for i in range(len(df)-1):
            close_price = df.iloc[i]['종가']
            next_day_high = df.iloc[i+1]['고가']
            percent_change = ((next_day_high - close_price) / close_price) * 100
            percentage_diff.append(percent_change)

        # 결과를 데이터프레임으로 만들고 포맷팅
        result_df = pd.DataFrame(percentage_diff, index=df.index[:-1], columns=['등락률'])
        # 등락률이 20% 이상인 값이 있으면 False, 없으면 True를 리턴
        result = not (result_df['등락률'] >= 20).any()
        return result