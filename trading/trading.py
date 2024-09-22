"""
api와 db 모듈을 사용해서 실제 트레이딩에 사용될 로직 모듈을 개발
"""

from utils.date_utils import DateUtils
from database.db_manager import DatabaseManager
from datetime import date
from api.kis_api import KISApi




class TradingLogic:
    """
    트레이딩과 관련된 로직들. main에는 TradinLogic 클래스만 있어야 함.
    """
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.kis_api = KISApi()
        
    def fetch_and_save_previous_upper_limit_stocks(self):
        """
        상한가 종목을 받아온 후,
        DB에 상한가 종목을 저장
        """
        self.kis_api.set_headers(is_mock=False, tr_id="FHKST130000C0")
        upper_limit_stocks = self.kis_api.get_upper_limit_stocks()
        if upper_limit_stocks:
            print("Upper Limit Stocks:")
            self.kis_api.print_korean_response(upper_limit_stocks)
            
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