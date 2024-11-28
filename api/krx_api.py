from pykrx import stock
import datetime

# 오늘 날짜
today = datetime.datetime.now()
# 15일 전 날짜
start_date = (today - datetime.timedelta(days=5)).strftime('%Y%m%d')
end_date = today.strftime('%Y%m%d')

# 특정 종목의 15일간 OHLCV 데이터 가져오기
df = stock.get_market_ohlcv(start_date, end_date, "060370")
print(df)

# 20% 이상 고가 확인
high_price_threshold = df['고가'].iloc[0] * 1.2
over_20_percent_days = df[df['고가'] > high_price_threshold]
print(over_20_percent_days)