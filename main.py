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
from api.kis_api import KISApi
from apscheduler.schedulers.background import BackgroundScheduler

def fetch_and_save_upper_limit_stocks(api_instanace):
    """
    주기적으로 상한가 종목을 DB에 저장
    """
    api_instanace.set_headers(is_mock=False, tr_id="FHKST130000C0")
    while True:
        now = datetime.now()
        if now.hour == 15 and now.minute == 30:  # 매일 15시 30분에 실행
            api_instanace.fetch_and_save_previous_upper_limit_stocks()
        time.sleep(60)  # 1분 대기

def threaded_job(func, *args):
    """
    APscheduler 사용을 위한 래퍼함수
    스레드에서 실행할 작업을 감싸는 함수입니다.
    """
    thread = threading.Thread(target=func, args=args, daemon=True)
    thread.start()

def test():
    """
    테스트 프로세스
    """



if __name__ == "__main__":
    kis_api = KISApi()  # KISApi 인스턴스 생성
    scheduler = BackgroundScheduler()
    
    # 매일 15시 30분에 fetch_and_save_upper_limit_stocks 실행
    scheduler.add_job(threaded_job, 'cron', hour=15, minute=30, args=[fetch_and_save_upper_limit_stocks, kis_api])
    scheduler.start()

    # 프로그램이 종료되지 않도록 유지
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()