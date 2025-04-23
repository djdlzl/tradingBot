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
import traceback
import threading
import time
import atexit
import asyncio
from datetime import datetime
from trading.trading import TradingLogic
from trading.trading_upper import TradingUpper
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor
from config.condition import GET_ULS_HOUR, GET_ULS_MINUTE, GET_SELECT_HOUR, GET_SELECT_MINUTE, ORDER_HOUR_1, ORDER_HOUR_2, ORDER_HOUR_3, ORDER_MINUTE_1, ORDER_MINUTE_2, ORDER_MINUTE_3
from database.db_manager import DatabaseManager
from api.kis_api import KISApi

class MainProcess:
    def __init__(self):
        self.stop_event = threading.Event()
        self.db_lock = threading.Lock()
        self.threads = {}
        self.scheduler = BackgroundScheduler(timezone='Asia/Seoul')
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
            trading = TradingLogic()

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
                trading.save_upper_stocks,
                CronTrigger(hour=GET_ULS_HOUR, minute=GET_ULS_MINUTE),
                id='fetch_stocks',
                replace_existing=True
            )

            self.scheduler.add_job(
                trading.select_stocks_to_buy,
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
            
            print(f"등록된 작업: 상한가 조회({GET_ULS_HOUR}:{GET_ULS_MINUTE}), " 
                  f"종목 선정({GET_SELECT_HOUR}:{GET_SELECT_MINUTE}), "
                  f"매수 시간({ORDER_HOUR_1}:{ORDER_MINUTE_1}, "
                  f"{ORDER_HOUR_2}:{ORDER_MINUTE_2}, "
                  f"{ORDER_HOUR_3}:{ORDER_MINUTE_3})")

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

    def save_upper_stocks(self):
        try:
            trading = TradingLogic()
            trading_upper = TradingUpper()
            
            trading.fetch_and_save_previous_upper_limit_stocks()
            trading_upper.fetch_and_save_previous_upper_stocks()
        except Exception as e:
            print('오류가 발생했습니다. error: ', e)

    def execute_buy_task(self):
        """매수 태스크 실행"""
        try:
            trading = TradingLogic()
            # 선별 종목 매수
            with self.db_lock:
                order_list = trading.start_trading_session()

            # 매수 정보 세션에 저장
            with self.db_lock:
                trading.load_and_update_trading_session(order_list)
        except Exception as e:
            print(f"매수 태스크 실행 에러: {str(e)}")

######################################################################################
#################################    모니터링 실행   #####################################
######################################################################################

    def run_monitoring(self):
        """새로운 이벤트 루프를 생성하여 모니터링 실행"""
        # 새로운 이벤트 루프 생성
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:    
            trading = TradingLogic()
            sessions_info = trading.get_session_info()
            
            # 이벤트 루프에서 코루틴 실행
            loop.run_until_complete(trading.monitor_for_selling(sessions_info))
            
        except Exception as e:
            print(f"모니터링 실행 오류: {e}")
            traceback.print_exc()
        finally:
            try:
                # 실행 중인 모든 태스크 가져오기
                pending = asyncio.all_tasks(loop)
                
                # 현재 태스크 확인 및 제거
                current = asyncio.current_task(loop)
                if current and current in pending:
                    pending.remove(current)
                
                # 남은 태스크들 취소
                if pending:
                    # 모든 태스크 취소
                    for task in pending:
                        task.cancel()
                    
                    # 태스크들이 정리될 때까지 대기
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                
                # 루프 종료
                loop.close()

                # 루프 종료
                loop.close()
            except Exception as e:
                print(f"이벤트 루프 정리 중 오류: {e}")


######################################################################################
##################################    스레드 관리   #####################################
######################################################################################

    def start_all(self):
        """모든 스레드 시작"""
        try:
            # 스케줄 관리 스레드
            scheduler_thread = threading.Thread(
                target=self.schedule_manager,
                name="Schedule_Manager"
            )
            scheduler_thread.daemon = True  # 메인 프로그램 종료시 함께 종료
            scheduler_thread.start()
            self.threads['scheduler'] = scheduler_thread
            
            print("스케줄러 스레드 시작됨")
            
            # 트레이딩 스레드들
        
            trading_thread = threading.Thread(
                target=self.run_monitoring,
                name="run_monitoring"
                )
            
            trading_thread.start()
            self.threads['trading'] = trading_thread  # threads 딕셔너리에 추가
            print("모니터링 스레드 시작됨")

        except Exception as e:
            print(f"스레드 시작 중 오류 발생: {e}")
            self.cleanup()
          
          
    def stop_all(self):
        """모든 스레드 종료"""
        self.stop_event.set()
        for name, thread in self.threads.items():
            thread.join()
            print(f"{name} 스레드 종료됨")

##################################  이까지 클래스  ####################################

if __name__ == "__main__":
    try:
        main_process = MainProcess()
        main_process.start_all()
        
        start_time = datetime.now().strftime('%y%m%d - %X')
        print("리버모어 시작 - ", start_time)
        
        # 무한 루프로 메인 스레드 유지
        while True:
            time.sleep(1)

    except (KeyboardInterrupt, SystemExit):
        print("\n프로그램 종료 요청됨")
        main_process.stop_event.set()
    finally:
        main_process.cleanup()