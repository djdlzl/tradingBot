"""
트레이딩 조건 설정 파일
"""

import os
from datetime import time

# 한국거래소(KRX) 장 시작·종료 시간 (환경변수로 덮어쓰기 가능)
KRX_START_HOUR = int(os.getenv("KRX_START_HOUR", 9))
KRX_START_MINUTE = int(os.getenv("KRX_START_MINUTE", 0))
KRX_END_HOUR = int(os.getenv("KRX_END_HOUR", 15))
KRX_END_MINUTE = int(os.getenv("KRX_END_MINUTE", 20))

KRX_TRADING_START = time(KRX_START_HOUR, KRX_START_MINUTE)
KRX_TRADING_END = time(KRX_END_HOUR, KRX_END_MINUTE)

# ------------------------------------------------------------------------------

######################################################
####################    매수   ########################
######################################################

# 매수 조건 퍼센테이지
BUY_PERCENT = 0.92
# 매수 주문 후 대기 / 초
BUY_WAIT = 20
# N일 전 상한가 종목으로 매수
BUY_DAY_AGO = 2
# 매수 횟수(최초 9회였음)
COUNT = 6

# Slippage buffer to account for possible price increase between quote and execution
PRICE_BUFFER = 0.01  # 1% buffer added on top of quoted price when calculating buy quantity



######################################################
####################    매도   ########################
######################################################


# 수익률이 이 값 이상일 때 매도
SELLING_POINT_UPPER = 1.02

# 손실 위험 관리 값 (이 값보다 낮아지면 매도)
RISK_MGMT_UPPER = 0.93
RISK_MGMT_STRONG_MOMENTUM = 0.90  # 강력 모멘텀 종목에 대한 리스크 관리 기준 (7% 손실)

# 트레일링스탑 하락 비율 (%) - 고점 대비 몇 % 하락 시 매도할지 설정
# SELLING_POINT_UPPER 이상 수익 시 활성화
TRAILING_STOP_PERCENTAGE = 1.04

# 매도 주문 후 대기 / 초
SELL_WAIT = 3

# 매수 시작일로부터 days_later일 후 매도
DAYS_LATER = 7 #마지막 매수로부터 4(7-3)일째 매도

######################################################
##################    스케줄링   ######################
######################################################

# 종목 받아오는 시간
GET_ULS_HOUR = 20
GET_ULS_MINUTE = 0
# 종목 중 선별하는 시간
GET_SELECT_HOUR = 8
GET_SELECT_MINUTE = 55

# 매수 시간 1
ORDER_HOUR_1 = 9
ORDER_MINUTE_1 = 7
# 매수 시간 2
ORDER_HOUR_2 = 10
ORDER_MINUTE_2 = 40
# # 매수 시간 3
ORDER_HOUR_3 = 14
ORDER_MINUTE_3 = 41



###############################################################
###############################################################


# 상승 눌림목 매매

UPPER_DAY_AGO_CHECK = 10

BUY_PERCENT_UPPER = 0.925 # -7.5% 이상

COUNT_UPPER = 2 # 총 매수 회수 (현재 1일 2회 매수)

SLOT_UPPER = 2 # 매수 종목 개수

BUY_DAY_AGO_UPPER = 2 # DB 상승 종목에서 며칠 전 종목 받아올건지

DAYS_LATER_UPPER = 1 #마지막 매수로부터 +1일



#### 상한가 눌림목 매매 때 사용 #### 

#selling_point_1 이상일 때 1차 매도 - 일단 이것만 사용
# SELLING_POINT = 1.0

#selling_point_1 이상일 때 1차 매도 - 일단 이것만 사용
# RISK_MGMT = 0.99

######################################################
#################    전략 조건   ######################
######################################################

STRONG_MOMENTUM = "strong_momentum"

