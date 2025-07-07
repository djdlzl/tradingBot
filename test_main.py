"""
이 모듈은 한국투자증권(KIS) API를 사용하여 주식 시장 데이터를 조회하는 메인 스크립트입니다.

주요 기능:
- KIS API 초기화
- 상한가 종목 조회
- (주석 처리됨) 상승/하락 순위 조회

사용법:
이 스크립트를 직접 실행하면 KIS API를 통해 주식 시장 데이터를 가져와 출력합니다.

의존성:
- api.kis_api 모듈의 KISApi 클래스
"""

from codecs import ascii_encode
import threading
import time
import json
import asyncio
from datetime import datetime
from requests import session
from trading.trading_upper import TradingUpper
from config.condition import GET_ULS_HOUR, GET_ULS_MINUTE
from database.db_manager_upper import DatabaseManager
from utils.date_utils import DateUtils
from api.kis_api import KISApi
from api.krx_api import KRXApi
from main import MainProcess


def fetch_and_save_upper_limit_stocks():
    """
    주기적으로 상한가 종목을 DB에 저장
    """
    trading = TradingUpper()
    # trading_instacne.set_headers(is_mock=False, tr_id="FHKST130000C0")
    while True:
        now = datetime.now()
        if now.hour == GET_ULS_HOUR and now.minute == GET_ULS_MINUTE:  # 매일 15시 30분에 실행
            trading.fetch_and_save_previous_upper_stocks()
        time.sleep(1)  # 1분 대기

def add_stocks():
    trading = TradingUpper()
    stocks = [
        ("211270", "AP위성", 14950.0, 29.99),
        ("361390", "제노코", 22100.0, 29.97),
        ("460930", "현대힘스", 13390.0, 29.97)
    ]

    trading.add_upper_stocks("2024-11-07", stocks)

def delete_stocks():
    """
    특정일자 상한가 종목 삭제
    필요 시 사용
    """
    db = DatabaseManager()
    db.delete_upper_limit_stocks("2024-10-10")
    db.close()

def threaded_job(func):
    """
    APscheduler 사용을 위한 래퍼함수
    스레드에서 실행할 작업을 감싸는 함수입니다.
    """
    thread = threading.Thread(target=func, daemon=True)
    thread.start()


def test():
    """
    테스트 프로세스
    """
    
    main = MainProcess()
    trading_upper = TradingUpper()
    kis_api = KISApi()
    krx_api = KRXApi()
    date_utils = DateUtils()
    db = DatabaseManager()

    ############# sell logic fix ##################
    # result = kis_api.balance_inquiry()
    # print(result)

    ############# DB에 상승종목 저장 ###############
    # trading_upper.select_stocks_to_buy()

    ############# buy logic fix ##################
    # available_cash = kis_api.get_available_cash()
    # print(available_cash)
    # session_info = trading_upper.add_new_trading_session()
    # print(session_info)

    # available_cash = kis_api.purchase_availability_inquiry()
    # print(available_cash)

    # ########### 매수 재시도 로직 수리 #################
    conclusion_result = kis_api.daily_order_execution_inquiry('0000005847')
    print(json.dumps(conclusion_result, indent=2, ensure_ascii=False))
    real_quantity = int(conclusion_result.get('output1', [{}])[0].get('tot_ccld_qty', 0))
    real_spent_fund = int(conclusion_result.get('output1', [{}])[0].get('tot_ccld_amt', 0))

    # print("결과:",real_quantity, real_spent_fund)

    # ######### 과열 종목 확인 ##########
    # stock = kis_api.get_stock_price('035420')
    # print(stock)

    ######### 정합성 맞추기 ###########
    # sessions = db.load_trading_session_upper()
    # print(sessions)
    # for session in sessions:
    #     trading_upper.validate_db_data(session)

    ######### 매수 로직 ###########
    # # 선별 종목 매수
    # order_list = trading_upper.start_trading_session()

    # # 매수 정보 세션에 저장
    # trading_upper.load_and_update_trading_session(order_list)
    
    ############ 세션 데이터 타입 확인 #############
    # sessions = db.load_trading_session_upper()
    # print(sessions)
    # print("=== 세션 데이터 ===")
    # for idx, session in enumerate(sessions, 1):
    #     print(f"\n[세션 {idx}]")
    #     for key, value in session.items():
    #         print(f"  {key}: {value} (타입: {type(value).__name__})")
    
    ################### 매도 로직 수리 중 #######################
    # price = 300
    # price = int(price)
    # current_price_result = kis_api.get_current_price('021880')
    # current_price = int(current_price_result[0])  # 문자열 또는 리스트 처리
    # print(current_price)
    # if price < current_price * 0.95 or price > current_price * 1.05:
    #     print('비교 완료')

    ################### 자금 할당 문제 수리 중 ####################
    # data = kis_api.purchase_availability_inquiry()
    # print(data)


    # ###### 선별 종목 저장 ######
    # stocks = trading_upper.select_stocks_to_buy()
    # print(stocks)


    # #####상한가 조회#############    
    # # print("시작")
    # trading.fetch_and_save_previous_upper_limit_stocks()
    # trading_upper.fetch_and_save_previous_upper_stocks()
    # # print("상한가 저장")

    # # # ######매수가능 상한가 종목 조회###########
    # trading.select_stocks_to_buy() # 3일째 장 마감때 저장
    # # print("상한가 선별 및 저장 완료")
    
    # print("start_trading_session 실행 시작")
    # order_list = trading.start_trading_session()
    
    # # time.sleep(20)
    # # print("load_and_update_trading_session 실행 시작")
    # trading.load_and_update_trading_session(order_list)

    ###### websocket 모니터링 실행
    # sessions_info = trading_upper.get_session_info_upper()
    # asyncio.run(trading_upper.monitor_for_selling_upper(sessions_info))


        
if __name__ == "__main__":
    test()
    # try:
    #     main_process = MainProcess()
    #     main_process.start_all()
        
    #     # 무한 루프로 메인 스레드 유지
    #     while True:
    #         time.sleep(1)
            
    # except (KeyboardInterrupt, SystemExit):
    #     print("\n프로그램 종료 요청됨")
    #     main_process.stop_event.set()
    # finally:
    #     main_process.cleanup()