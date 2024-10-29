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
from apscheduler.triggers.cron import CronTrigger
from config.condition import GET_ULS_HOUR, GET_ULS_MINUTE, GET_SELECT_HOUR, GET_SELECT_MINUTE
from database.db_manager import DatabaseManager
from api.kis_api import KISApi
from api.kis_websocket import KISWebSocket
import asyncio

class MainProcess:
    def __init__(self):
        self.stop_event = threading.Event()
        self.db_lock = threading.Lock()
        self.threads = {}
        self.scheduler = BackgroundScheduler(timezone='Asia/Seoul')
        self.trading = TradingLogic()

######################################################################################
#################################    메서드 합치기   #####################################
######################################################################################

    def schedule_manager(self):
        """스케줄 작업을 관리하는 메서드"""
        try:
            # 첫 번째 스케줄 작업: 상한가 종목 조회 (15:30)
            self.scheduler.add_job(
                self.trading.fetch_and_save_previous_upper_limit_stocks,
                CronTrigger(hour=GET_ULS_HOUR, minute=GET_ULS_MINUTE)
                )

            # 두 번째 스케줄 작업: 매수 종목 선정 (08:50)
            self.scheduler.add_job(
                self.trading.select_stocks_to_buy,
                CronTrigger(hour=GET_SELECT_HOUR, minute=GET_SELECT_MINUTE)
                )
            
            # 스케줄러 시작
            self.scheduler.start()
            print("스케줄러 시작됨")
            
            # 스케줄러 실행 유지
            while not self.stop_event.is_set():
                time.sleep(1)

        except Exception as e:
            print(f"스케줄 관리자 에러: {str(e)}")
        finally:
            self.scheduler.shutdown()

    def trading_cycle(self):        
        """매수-모니터링-매도 사이클 실행"""
        while not self.stop_event.is_set():
            try:
                with self.db_lock:
                    #  print("세션 시작: start_trading_session 실행 시작")
                    order_list = self.trading.start_trading_session()
                    
                time.sleep(20)
                with self.db_lock:
                    #  print("세션 저장: load_and_update_trading_session 실행 시작")
                    self.trading.load_and_update_trading_session(order_list)

                ####### websocket 모니터링 실행
                sessions_info = self.trading.get_session_info()
                asyncio.run(self.trading.monitor_for_selling(sessions_info[0]))

            except Exception as e:
                print(f"트레이딩 사이클 에러: {str(e)}")

######################################################################################
##################################    스레드 관리   #####################################
######################################################################################

    def start_all(self):
        """모든 스레드 시작"""
        # 스케줄 관리 스레드
        scheduler_thread = threading.Thread(
            target=self.schedule_manager,
            name="Schedule_Manager"
        )
        
        scheduler_thread.start()
        self.threads['scheduler'] = scheduler_thread
        
        # 트레이딩 스레드들
        for i in range(3):
            thread = threading.Thread(
                target=self.trading_cycle,
                name=f"Trading_{i}"
            )
            thread.start()
            self.threads[f'trading_{i}'] = thread


    def stop_all(self):
        """모든 스레드 종료"""
        self.stop_event.set()
        for name, thread in self.threads.items():
            thread.join()
            print(f"{name} 스레드 종료됨")

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
        ("027970", "한국제지", 1350.0, 29.99),
        ("016450", "한세예스24홀딩스", 7600.0, 29.97),
        ("138610", "나이벡", 23450.0, 29.82),
        ("361670", "삼영에스앤씨", 7020.0, 30.00),
        ("053280", "예스24", 8290.0, 29.99),
		("033320", "제이씨현시스템", 4120.0, 29.91),
		("314130", "지놈앤컴퍼니", 4445.0, 29.91),
		("066980", "한성크린텍", 2320.0, 29.91),
    ]

    trading.add_upper_limit_stocks("2024-10-14", stocks)

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
    kis_api = KISApi()

    #####상한가 조회#############    
    print("시작")
    trading.fetch_and_save_previous_upper_limit_stocks()
    print("상한가 저장")

    ######매수가능 상한가 종목 조회###########
    trading.select_stocks_to_buy() # 2일째 장 마감때 저장
    print("상한가 선별 및 저장 완료")
    
    print("start_trading_session 실행 시작")
    order_list = trading.start_trading_session()
    
    time.sleep(20)
    print("load_and_update_trading_session 실행 시작")
    trading.load_and_update_trading_session(order_list)

    ####### websocket 모니터링 실행
    sessions_info = trading.get_session_info()
    asyncio.run(trading.monitor_for_selling(sessions_info[0]))

    # trading.sell_order("037270", "121")
    # kis_api.get_my_cash()
    
async def test_websocket():
    kis_websocket = KISWebSocket()





if __name__ == "__main__":

    scheduler = BackgroundScheduler()
    trading = TradingLogic()
    # asyncio.run(test_websocket())
    test()

    # 매일 15시 30분에 fetch_and_save_upper_limit_stocks 실행
    scheduler.add_job(threaded_job, CronTrigger(hour=GET_ULS_HOUR, minute=GET_ULS_MINUTE), args=[trading.fetch_and_save_previous_upper_limit_stocks])
    # 매일 8시 50분에 select_stocks_to_buy 실행
    scheduler.add_job(threaded_job, CronTrigger(hour=GET_SELECT_HOUR, minute=GET_SELECT_MINUTE), args=[trading.select_stocks_to_buy])
    scheduler.start()
    # 프로그램이 종료되지 않도록 유지
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()