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
from database.db_manager import DatabaseManager
from utils.date_utils import DateUtils

def fetch_and_save_upper_limit_stocks():
    while True:
        now = datetime.now()
        if now.hour == 15 and now.minute == 30:  # 매일 15시 30분에 실행
            fetch_and_save_previous_upper_limit_stocks()
        time.sleep(60)  # 1분 대기

def main():
    """
    메인 프로세스
    """
    kis_api = KISApi()

    kis_api.set_headers(is_mock=False, tr_id="FHKST130000C0")

    upper_limit_stocks = kis_api.get_upper_limit_stocks()
    if upper_limit_stocks:
        print("Upper Limit Stocks:")
        kis_api.print_korean_response(upper_limit_stocks)
        
        # 상한가 종목 정보 추출
        stocks_info = [(stock['mksc_shrn_iscd'], stock['hts_kor_isnm'], stock['stck_prpr'], stock['prdy_ctrt']) 
                       for stock in upper_limit_stocks['output']]
        
        # 영업일 기준 날짜 가져오기
        today = date.today()
        if not DateUtils.is_business_day(today):
            today = DateUtils.get_previous_business_day(today)
        # 데이터베이스에 저장
        db = DatabaseManager()
        db.save_upper_limit_stocks(today.isoformat(), stocks_info)
        db.close()



if __name__ == "__main__":
    main()