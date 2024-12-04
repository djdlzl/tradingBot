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

import threading
import time
import json
import asyncio
from datetime import datetime
from trading.trading import TradingLogic
from trading.trading_upper import TradingUpper
from config.condition import GET_ULS_HOUR, GET_ULS_MINUTE
from database.db_manager import DatabaseManager
from utils.date_utils import DateUtils
from api.kis_api import KISApi
from api.krx_api import KRXApi


def fetch_and_save_upper_limit_stocks():
    """
    주기적으로 상한가 종목을 DB에 저장
    """
    trading = TradingLogic()
    # trading_instacne.set_headers(is_mock=False, tr_id="FHKST130000C0")
    while True:
        now = datetime.now()
        if now.hour == GET_ULS_HOUR and now.minute == GET_ULS_MINUTE:  # 매일 15시 30분에 실행
            trading.fetch_and_save_previous_upper_limit_stocks()
        time.sleep(1)  # 1분 대기

def add_stocks():
    trading = TradingLogic()
    stocks = [
        ("211270", "AP위성", 14950.0, 29.99),
        ("361390", "제노코", 22100.0, 29.97),
        ("460930", "현대힘스", 13390.0, 29.97)
    ]

    trading.add_upper_limit_stocks("2024-11-07", stocks)

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
    
    
    trading = TradingLogic()
    trading_upper = TradingUpper()
    kis_api = KISApi()
    krx_api = KRXApi()
    date_utils = DateUtils()
    

    # trading_upper.fetch_and_save_previous_upper_stocks()
    trading_upper.select_stocks_to_buy()
    # krx_api.get_OHLCV('239340', 16)

    

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