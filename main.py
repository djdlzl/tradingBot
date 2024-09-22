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
from config.config import get_uls_hour, get_uls_minute


def fetch_and_save_upper_limit_stocks():
    """
    주기적으로 상한가 종목을 DB에 저장
    """
    trading = TradingLogic()
    # trading_instacne.set_headers(is_mock=False, tr_id="FHKST130000C0")
    while True:
        now = datetime.now()
        if now.hour == get_uls_hour and now.minute == get_uls_minute:  # 매일 15시 30분에 실행
            trading.fetch_and_save_previous_upper_limit_stocks()
            print("상한가 저장")
        time.sleep(60)  # 1분 대기

def threaded_job(func):
    """
    APscheduler 사용을 위한 래퍼함수
    스레드에서 실행할 작업을 감싸는 함수입니다.
    """
    thread = threading.Thread(target=func, daemon=True)
    thread.start()

def test(trading_instance):
    """
    테스트 프로세스
    """
    trading_instance.fetch_and_save_previous_upper_limit_stocks()
    print("상한가 저장")
    


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    
    # 매일 15시 30분에 fetch_and_save_upper_limit_stocks 실행
    scheduler.add_job(threaded_job, 'cron', hour=get_uls_hour, minute=get_uls_minute, args=[fetch_and_save_upper_limit_stocks])
    scheduler.start()
    # test(trading)
    # 프로그램이 종료되지 않도록 유지
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()