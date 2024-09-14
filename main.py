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

from api.kis_api import KISApi
from database.db_manager import DatabaseManager
from datetime import date

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
        stocks_info = [(stock['mksc_shrn_iscd'], stock['hts_kor_isnm'], stock['stck_prpr'], stock['prdy_ctrt']) for stock in upper_limit_stocks['output']]
        
        # 데이터베이스에 저장
        db = DatabaseManager()
        today = date.today().isoformat()
        db.save_upper_limit_stocks(today, stocks_info)
        db.close()

    # # 상승/하락 순위 조회
    # updown_rank = kis_api.get_upAndDown_rank()
    # if updown_rank:
    #     print("\nUp and Down Rank:")
    #     kis_api.print_korean_response(updown_rank)

if __name__ == "__main__":
    main()