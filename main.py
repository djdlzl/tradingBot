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
from datetime import datetime
from trading.trading import TradingLogic
from apscheduler.schedulers.background import BackgroundScheduler
from config.config import GET_ULS_HOUR, GET_ULS_MINUTE
from database.db_manager import DatabaseManager



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
        ("219550", "디와이디", 455.0, 29.99),
        ("088800", "에이스테크", 1059.0, 29.97),
        ("239610", "에이치엘사이언스", 19200.0, 29.82),
        ("084440", "유비온", 1105.0, 30.00),
        ("900270", "헝셩그룹", 392.0, 29.99),
		("192410", "휴림네트웍스", 3025.0, 29.91),
    ]
    trading.add_upper_limit_stocks("2024-09-30", stocks)

def delete_stocks():
    """
    특정일자 상한가 종목 삭제
    필요 시 사용
    """
    db = DatabaseManager()
    db.delete_upper_limit_stocks("2024-10-09")
    db.close()

def threaded_job(func):
    """
    APscheduler 사용을 위한 래퍼함수
    스레드에서 실행할 작업을 감싸는 함수입니다.
    """
    thread = threading.Thread(target=func, daemon=True)
    thread.start()



# def session_start():
#     # 거래 세션 확인
#     trading = TradingLogic()
#     scheduler = BackgroundScheduler()
    
#     #2주 전 데이터 주기적으로 삭제
#     trading.delete_old_stocks()
    
#     #거래 세션 확인
#     session_info = trading.check_trading_session()
#     print(f"현재 세션 수: {session_info['session']}, 슬롯 수: {session_info['slot']}")
    
#     #새 세션에 할당할 자금 계산
#     funds = trading.calculate_funds(session_info['slot'])
    


def test():
    """
    테스트 프로세스
    """
    trading = TradingLogic()
    #####상한가 조회#############    
    print("시작")
    trading.fetch_and_save_previous_upper_limit_stocks()
    print("상한가 저장")

    ######매수가능 상한가 종목 조회###########
    trading.init_selected_stocks()
    trading.select_stocks_to_buy()
    print("상한가 선별 및 저장")
    
    result = DatabaseManager().get_selected_stocks()

if __name__ == "__main__":

    scheduler = BackgroundScheduler()

    test()
    # 매일 15시 30분에 fetch_and_save_upper_limit_stocks 실행
    scheduler.add_job(threaded_job, 'cron', hour=GET_ULS_HOUR, minute=GET_ULS_MINUTE, args=[fetch_and_save_upper_limit_stocks])
    scheduler.start()
    # 프로그램이 종료되지 않도록 유지
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()