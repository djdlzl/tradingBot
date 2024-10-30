"""
트레이딩 조건 설정 파일
"""


# 매수 시작일로부터 days_later일 후 매도
DAYS_LATER = 7 #마지막 매수로부터 4일째


#selling_point_1 이상일 때 1차 매도 - 일단 이것만 사용
SELLING_POINT = 1.15
#selling_point_2 이상일 때 2차 매도
selling_point_2 = 1


# Trading parameters
MAX_PRICE_DROP_PERCENT = -8

# About Time
GET_ULS_HOUR = 15
GET_ULS_MINUTE = 30

GET_SELECT_HOUR = 8
GET_SELECT_MINUTE = 50

# 매수 시간 1
ORDER_HOUR_1 = 14
ORDER_MINUTE_1 = 20
# 매수 시간 2
ORDER_HOUR_2 = 14
ORDER_MINUTE_2 = 40
# 매수 시간 3
ORDER_HOUR_3 = 15
ORDER_MINUTE_3 = 00
