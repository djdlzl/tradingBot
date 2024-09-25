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
        self.kis_api = KISApi()
        
    def fetch_and_save_previous_upper_limit_stocks(self):
        """
        상한가 종목을 받아온 후,
        DB에 상한가 종목을 저장
        """
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
            
    def select_stocks_to_buy(self):
        """
        3일 전 상한가 종목의 가격과 현재가를 비교하여 매수할 종목을 선정합니다.
        """
        db = DatabaseManager()
        selected_stocks = []
        tickers_with_prices = db.get_upper_limit_stocks_two_days_ago()  # ticker와 price를 가져옴

        for ticker, name, previous_price in tickers_with_prices:
            # 현재가 가져오기
            current_price, temp_stop_yn = self.kis_api.get_current_price(ticker)
            # print(f"################# Ticker: {ticker}, Previous Price: {previous_price}, Current Price: {current_price}")

            # 매수 조건: 현재가가 상한가 당시 가격보다 -8% 이상 하락한 경우
            if int(current_price) > (int(previous_price) * 0.92) and temp_stop_yn=='N':  # -8% 이상 하락, 거래정지 N
                selected_stocks.append(ticker)
                print(f"################ 매수 후보 종목: {ticker}, 종목명: {name} (현재가: {current_price}, 상한가 당시 가격: {previous_price})")
        db.close()
        return selected_stocks
        
        

    def add_upper_limit_stocks(self, add_date, stocks):
        """
        상한가 종목 추가
        """
        db = DatabaseManager()
        try:
            db.save_upper_limit_stocks(add_date, stocks)
            print(f"Successfully added stocks for date: {add_date}")
        except Exception as e:
            print(f"Error adding stocks: {e}")
        finally:
            db.close()
                
    def manage_fund(self):
        self.kis_api.get_my_cash2()