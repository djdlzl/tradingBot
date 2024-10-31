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
from config.condition import GET_ULS_HOUR, GET_ULS_MINUTE, GET_SELECT_HOUR, GET_SELECT_MINUTE, ORDER_HOUR_1, ORDER_HOUR_2, ORDER_HOUR_3, ORDER_MINUTE_1, ORDER_MINUTE_2, ORDER_MINUTE_3
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

            # 매수 실행 - 첫번째 (14:20)
            self.scheduler.add_job(
                self.execute_buy_task,
                CronTrigger(hour=ORDER_HOUR_1, minute=ORDER_MINUTE_1)
            )

            # 매수 실행 - 두번째 (14:40)
            self.scheduler.add_job(
                self.execute_buy_task,
                CronTrigger(hour=ORDER_HOUR_2, minute=ORDER_MINUTE_2)
            )

            # 매수 실행 - 세번째 (15:00)
            self.scheduler.add_job(
                self.execute_buy_task,
                CronTrigger(hour=ORDER_HOUR_3, minute=ORDER_MINUTE_3)
            )

            # 스케줄러 시작
            self.scheduler.start()
            print("스케줄러 시작됨")
            print(f"등록된 작업: 상한가 조회({GET_ULS_HOUR}:{GET_ULS_MINUTE}), " 
                f"종목 선정({GET_SELECT_HOUR}:{GET_SELECT_MINUTE}), "
                f"매수 시간({ORDER_HOUR_1}:{ORDER_MINUTE_1}, "
                f"{ORDER_HOUR_2}:{ORDER_MINUTE_2}, "
                f"{ORDER_HOUR_3}:{ORDER_MINUTE_3})")
            
            # 스케줄러 실행 유지
            while not self.stop_event.is_set():
                time.sleep(1)

        except Exception as e:
            print(f"스케줄 관리자 에러: {str(e)}")
        finally:
            if self.scheduler.running:  # 스케줄러가 실행 중일 때만 종료
                self.scheduler.shutdown()

    def execute_buy_task(self):
        """매수 태스크 실행"""
        try:
            # 선별 종목 매수
            with self.db_lock:
                order_list = self.trading.start_trading_session()
            
            # 주문 처리 대기
            time.sleep(20)  
            
            # 매수 정보 세션에 저장
            with self.db_lock:
                self.trading.load_and_update_trading_session(order_list)
        except Exception as e:
            print(f"매수 태스크 실행 에러: {str(e)}")


    def trading_cycle(self):
        """모니터링-매도 사이클 실행"""
        threads = []  # 실행 중인 스레드를 추적하는 리스트
        
        while not self.stop_event.is_set():
            try:
                # 현재 활성화된 세션 정보 가져오기
                with self.db_lock:
                    sessions_info = self.trading.get_session_info()
                
                for session_info in sessions_info:
                    # session_info는 (session_id, ticker, quantity, avr_price, target_date) 형태의 튜플
                    session_id = str(session_info[0])  # session_id를 문자열로 변환
                    
                    # 이미 실행 중인 스레드가 있는지 확인
                    if not any(thread.name == session_id for thread in threads):
                        # 새로운 세션에 대해 스레드 생성
                        thread = threading.Thread(
                            target=self.run_monitoring,
                            args=(session_info, )
                        )
                        thread.name = session_id
                        thread.daemon = True  # 데몬 스레드로 설정
                        thread.start()
                        threads.append(thread)
                
                # 완료된 스레드 제거 (죽은 스레드 정리)
                threads = [t for t in threads if t.is_alive()]
                
                time.sleep(1)  # CPU 사용률 감소를 위한 짧은 대기

            except Exception as e:
                print(f"모니터링 사이클 에러: {str(e)}")
                time.sleep(10)

    def run_monitoring(self):
        """새로운 이벤트 루프를 생성하여 모니터링 실행"""
        try:
            # 새로운 이벤트 루프 생성
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            trading = TradingLogic()
            sessions_info = trading.get_session_info()
            
            # 이벤트 루프에서 코루틴 실행
            loop.run_until_complete(trading.monitor_for_selling(sessions_info))
            
        except Exception as e:
            print(f"모니터링 실행 오류: {e}")
        finally:
            # 이벤트 루프 정리
            try:
                loop.close()
            except:
                pass


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
     
        # trading_thread = threading.Thread(
        #     target=self.run_monitoring,
        #     name="run_monitoring"
        #     )
        
        # trading_thread.start()
        # self.threads['trading'] = trading_thread  # threads 딕셔너리에 추가



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

    # #####상한가 조회#############    
    # print("시작")
    # trading.fetch_and_save_previous_upper_limit_stocks()
    # print("상한가 저장")

    # ######매수가능 상한가 종목 조회###########
    # trading.select_stocks_to_buy() # 2일째 장 마감때 저장
    # print("상한가 선별 및 저장 완료")
    
    # print("start_trading_session 실행 시작")
    # order_list = trading.start_trading_session()
    
    # time.sleep(20)
    # print("load_and_update_trading_session 실행 시작")
    # trading.load_and_update_trading_session(order_list)

    ####### websocket 모니터링 실행
    sessions_info = trading.get_session_info()
    asyncio.run(trading.monitor_for_selling(sessions_info))

    # trading.sell_order("037270", "121")
    # kis_api.get_my_cash()



if __name__ == "__main__":
    # test()
    main_process = MainProcess()
    main_process.start_all()
    main_process.run_monitoring()