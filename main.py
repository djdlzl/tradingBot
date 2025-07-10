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
import atexit
import asyncio
import signal, sys
from datetime import datetime
from trading.trading_upper import TradingUpper
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from config.condition import GET_ULS_HOUR, GET_ULS_MINUTE, GET_SELECT_HOUR, GET_SELECT_MINUTE, ORDER_HOUR_1, ORDER_HOUR_2, ORDER_MINUTE_1, ORDER_MINUTE_2, ORDER_HOUR_3, ORDER_MINUTE_3
from api.kis_websocket import KISWebSocket
from utils.decorators import business_day_only
from utils.slack_logger import SlackLogger
from utils.trading_logger import TradingLogger

class MainProcess:
    def __init__(self):
        self.stop_event = threading.Event()
        self.db_lock = threading.Lock()
        self.threads = {}
        self.scheduler = BackgroundScheduler(timezone='Asia/Seoul')
        self.trading_upper = TradingUpper()            
        self.monitor_loop = None
        self.slack_logger = SlackLogger()
        self.logger = TradingLogger()

        # 스케줄러 설정
        executors = {
            'default': ThreadPoolExecutor(20)
        }
        self.scheduler = BackgroundScheduler(executors=executors)
        # 프로그램 종료 시 스케줄러 정리
        atexit.register(self.cleanup)

######################################################################################
#################################    스케줄링 메서드   #####################################
######################################################################################
    def cleanup(self):
        """리소스 정리"""
        try:
            if self.scheduler and self.scheduler.running:
                self.scheduler.shutdown(wait=False)
        except:
            pass

    def schedule_manager(self):
        """스케줄 작업을 관리하는 메서드"""
        
        try:
            # 스케줄러 설정
            executors = {
                'default': ThreadPoolExecutor(20)
            }
            self.scheduler = BackgroundScheduler(
                executors=executors,
                timezone='Asia/Seoul',
                daemon=False  # 데몬 스레드 비활성화
            )

            # 작업 추가
            self.scheduler.add_job(
                self.fetch_and_save_previous_upper_stocks,
                CronTrigger(hour=GET_ULS_HOUR, minute=GET_ULS_MINUTE),
                id='fetch_stocks',
                replace_existing=True
            )

            self.scheduler.add_job(
                self.select_stocks_to_buy,
                CronTrigger(hour=GET_SELECT_HOUR, minute=GET_SELECT_MINUTE),
                id='select_stocks',
                replace_existing=True
            )

            self.scheduler.add_job(
                self.execute_buy_task,
                CronTrigger(hour=ORDER_HOUR_1, minute=ORDER_MINUTE_1),
                id='buy_task_1',
                replace_existing=True
            )

            self.scheduler.add_job(
                self.execute_buy_task,
                CronTrigger(hour=ORDER_HOUR_2, minute=ORDER_MINUTE_2),
                id='buy_task_2',
                replace_existing=True
            )

            self.scheduler.add_job(
                self.execute_buy_task,
                CronTrigger(hour=ORDER_HOUR_3, minute=ORDER_MINUTE_3),
                id='buy_task_3',
                replace_existing=True
            )

            # 스케줄러 시작
            self.scheduler.start()
            
            print(f"등록된 작업: 상승 추세매매 조회({GET_ULS_HOUR}:{GET_ULS_MINUTE}), " 
                  f"종목 선정({GET_SELECT_HOUR}:{GET_SELECT_MINUTE}), "
                  f"매수 시간({ORDER_HOUR_1}:{ORDER_MINUTE_1}, "
                  f"{ORDER_HOUR_2}:{ORDER_MINUTE_2}) ")

            # 명시적인 무한 루프로 스케줄러 유지
            while True:
                if self.stop_event.is_set():
                    break
                time.sleep(1)
                
        except Exception as e:
            print(f"스케줄 관리자 에러: {str(e)}")
        finally:
            if self.scheduler.running:
                self.scheduler.shutdown(wait=False)

    @business_day_only()
    def fetch_and_save_previous_upper_stocks(self):
        try:            
            self.trading_upper.fetch_and_save_previous_upper_stocks()
        except Exception as e:
            print('오류가 발생했습니다. error: ', e)

    @business_day_only()
    def select_stocks_to_buy(self):
        try:            
            self.trading_upper.select_stocks_to_buy()
        except Exception as e:
            print('오류가 발생했습니다. error: ', e)

    @business_day_only()
    def execute_buy_task(self):
        """매수 태스크 실행"""
        try:
            # trading = TradingLogic()
            # 선별 종목 매수
            with self.db_lock:
                order_list = self.trading_upper.start_trading_session()
            
            if order_list is not None:
                # 매수 정보 세션에 저장
                with self.db_lock:
                    self.trading_upper.load_and_update_trading_session(order_list)
            else:
                self.logger.info("선별 종목이 없어 매수 태스크 종료")
        except Exception as e:
            print(f"매수 태스크 실행 에러: {str(e)}")

######################################################################################
#################################    모니터링 실행   #####################################
######################################################################################

    @business_day_only()
    def run_monitoring(self):
        """새로운 이벤트 루프를 생성하여 모니터링 실행"""
        loop = asyncio.new_event_loop()
        self.monitor_loop = loop
        # TradingUpper에서도 동일 루프를 참조할 수 있도록 공유
        self.trading_upper._monitor_loop = loop
        asyncio.set_event_loop(loop)
        
        try:    
            # trading = TradingLogic()
            kis_websocket = KISWebSocket(self.trading_upper.sell_order)
            self.trading_upper.kis_websocket = kis_websocket  # KISWebSocket 인스턴스 설정
            sessions_info = self.trading_upper.get_session_info_upper()

            # 초기 세션 구독 코루틴을 등록하고 루프를 지속 실행
            loop.create_task(self.trading_upper.monitor_for_selling_upper(sessions_info))
            loop.run_forever()
            
        except Exception as e:
            print(f"모니터링 실행 오류: {e}")
        finally:
            try:

                # 실행 중인 모든 태스크 가져오기
                pending = asyncio.all_tasks(loop)
                
                # 현재 태스크 확인 및 제거
                current = asyncio.current_task(loop)
                if current and current in pending:
                    pending.remove(current)
                               
                if not loop.is_closed():
                    # 여기서는 close만, run_until_complete 호출 X
                    loop.close()
            except Exception as e:
                print(f"이벤트 루프 정리 중 오류: {e}")


######################################################################################
##################################    스레드 관리   #####################################
######################################################################################

    def start_all(self):
        """모든 스레드 시작"""
        try:
            # 1) 모니터링 루프 스레드를 가장 먼저 시작하여 이벤트 루프를 준비
            trading_thread = threading.Thread(
                target=self.run_monitoring,
                name="run_monitoring"
            )
            trading_thread.start()
            self.threads['trading'] = trading_thread
            print("모니터링 스레드 시작됨")

            # 2) 스케줄 관리 스레드는 루프 이후에 실행
            scheduler_thread = threading.Thread(
                target=self.schedule_manager,
                name="Schedule_Manager",
                daemon=True  # 메인 프로그램 종료 시 함께 종료
            )
            scheduler_thread.start()
            self.threads['scheduler'] = scheduler_thread
            print("스케줄러 스레드 시작됨")

        except Exception as e:
            print(f"스레드 시작 중 오류 발생: {e}")
            self.cleanup()
          
            
    def stop_all(self):
        self.stop_event.set()

        if self.monitor_loop and self.monitor_loop.is_running():
            async def _shutdown(loop):
                # 현재 실행 중인 _shutdown 태스크는 제외하고 모든 태스크를 가져옴
                current_task = asyncio.current_task()
                tasks = [t for t in asyncio.all_tasks(loop) if t is not current_task and not t.done()]

                if not tasks:
                    loop.stop()
                    return

                # 다른 모든 태스크를 취소
                for task in tasks:
                    task.cancel()

                # 모든 태스크가 취소될 때까지 대기
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # 이벤트 루프 중지
                loop.stop()

            asyncio.run_coroutine_threadsafe(_shutdown(self.monitor_loop), self.monitor_loop)

    def graceful_shutdown(self, signum, frame):
        print("Ctrl-C 감지 → 종료 루틴 실행")
        self.stop_event.set()   # 루프 중단 신호
        self.stop_all()
        self.cleanup()
    
##################################  이까지 클래스  ####################################

if __name__ == "__main__":
    main_process = None
    try:
        main_process = MainProcess()
        signal.signal(signal.SIGINT, main_process.graceful_shutdown)
        main_process.start_all()
        
        start_time = datetime.now().strftime('%y%m%d - %X')
        print("리버모어 시작 - ", start_time)
        
        # 무한 루프로 메인 스레드 유지
        while not main_process.stop_event.is_set():
            time.sleep(1)

    except Exception as e:
        print("예상치 못한 예외:", e)
    finally:
        if main_process:
            main_process.stop_all()
            main_process.cleanup()