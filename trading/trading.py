"""
api와 db 모듈을 사용해서 실제 트레이딩에 사용될 로직 모듈을 개발
"""

from datetime import datetime, timedelta
from database.db_manager import DatabaseManager
from utils.date_utils import DateUtils
from api.kis_api import KISApi
import random
import queue
import threading

class TradingLogic:
    """
    트레이딩과 관련된 로직들. main에는 TradinLogic 클래스만 있어야 함.
    """
    def __init__(self):
        self.kis_api = KISApi()
        self.date_utils = DateUtils()
        
    def fetch_and_save_previous_upper_limit_stocks(self):
        """
        상한가 종목을 받아온 후,
        DB에 상한가 종목을 저장
        """
        upper_limit_stocks = self.kis_api.get_upper_limit_stocks()
        if upper_limit_stocks:
            # self.kis_api.print_korean_response(upper_limit_stocks)

            # 상한가 종목 정보 추출
            stocks_info = [(stock['mksc_shrn_iscd'], stock['hts_kor_isnm'], stock['stck_prpr'], stock['prdy_ctrt']) 
                        for stock in upper_limit_stocks['output']]
            # 오늘 날짜 가져오기
            today = datetime.now()  # 현재 날짜와 시간 가져오기
            current_day = self.date_utils.is_business_day(today)
            # 데이터베이스에 저장
            db = DatabaseManager()
            if stocks_info:  # 리스트가 비어있지 않은 경우
                db.save_upper_limit_stocks(current_day.strftime('%Y-%m-%d'), stocks_info)  # 날짜를 문자열로 변환하여 저장
            else:
                print("상한가 종목이 없습니다.")
            db.close()

    def select_stocks_to_buy(self):
        """
        2일 전 상한가 종목의 가격과 현재가를 비교하여 매수할 종목을 선정(선별)합니다.
        """
        db = DatabaseManager()
        selected_stocks = []
        tickers_with_prices = db.get_upper_limit_stocks_two_days_ago()  # 2일 전 상한가 종목 가져오기
        print(tickers_with_prices)
        for stock in tickers_with_prices:
            # 현재가 가져오기
            current_price, temp_stop_yn = self.kis_api.get_current_price(stock[0])
            print(f"################ 매수 후보 종목: {stock[0]}, 종목명: {stock[1]} (현재가: {current_price}, 상한가 당시 가격: {stock[2]})")
            # 매수 조건: 현재가가 상한가 당시 가격보다 -8% 이상 하락하지 않은 경우
            if int(current_price) > (int(stock[2]) * 0.92) and temp_stop_yn=='N':  # -8% 이상 하락, 거래정지 N
                

                selected_stocks.append(stock)
      
        # 선택된 종목을 selected_stocks 테이블에 저장
        if selected_stocks:
            db.save_selected_stocks(selected_stocks)  # 선택된 종목 저장

        db.close()
        
        return selected_stocks

    def delete_old_stocks(self):
        """
        2개월 전 데이터 삭제
        """
        today = datetime.now()  # 현재 날짜와 시간 가져오기
        current_day = self.date_utils.is_business_day(today)
        all_holidays = self.date_utils.get_holidays()
        
        old_data = current_day
        for _ in range(40):
            old_data -= timedelta(days=1)
            while old_data.weekday() >= 5 or old_data in all_holidays:
                old_data -= timedelta(days=1)
                if old_data.weekday() == 6:
                    old_data -= timedelta(days=1)
        # 2개월 전의 날짜를 문자열로 변환
        old_data_str = old_data.strftime('%Y-%m-%d')
        
        #DB에서 2개월 전 데이터 삭제
        db = DatabaseManager()
        db.delete_old_stocks(old_data_str)
        db.close()

    def add_upper_limit_stocks(self, add_date, stocks):
        """
        상한가 종목 추가
        fetch_and_save_previous_upper_limit_stocks 메서드 못돌렸을 때 사용
        """
        db = DatabaseManager()
        try:
            db.save_upper_limit_stocks(add_date, stocks)
            print(f"Successfully added stocks for date: {add_date}")
        except Exception as e:
            print(f"Error adding stocks: {e}")
        finally:
            db.close()

    def init_selected_stocks(self):
        """
        selected_stock 테이블 초기화
        """
        db = DatabaseManager()
        db.delete_selected_stocks()
        db.close()


######################################################################################
#########################    트레이딩 세션 관련 메서드   ###################################
######################################################################################

    def start_trading_session(self):
        """
        거래 시작
        """
        session_info = self.check_trading_session()
        fund = self.calculate_funds(session_info['slot'])
        self.add_new_trading_session(fund)
        


    def check_trading_session(self):
        """
        거래 전, 트레이딩 세션 테이블에 진행 중인 거래세션이 있는지 확인하고,
        세션 수에 따라 거래를 진행하거나 새로운 세션을 생성합니다.
        """
        db = DatabaseManager()
        
        # 현재 거래 세션의 수를 확인
        db.cursor.execute('SELECT COUNT(*) FROM trading_session')
        session_count = db.cursor.fetchone()[0]
        slot_count = 3 - session_count

        db.close()
        
        return {'session': session_count, 'slot': slot_count}


    def add_new_trading_session(self, fund):
        """
        새로운 세션 생성
        """
        db = DatabaseManager()
        #세션 받아오는 메서드, trading_session의 id값을 받아와서 exclude_set에 저장
        exclude_num = []
        random_id = self.generate_random_id(exclude=exclude_num)
        today = datetime.now()
        stock = self.allocate_stock()
        
        # db.save_trading_session(today, today, )


    def generate_random_id(self, min_value=1000, max_value=9999, exclude=None):
        """
        아이디값 랜덤 생성
        """
        if exclude is None:
            exclude = []
        
        # 제외할 숫자를 집합으로 변환하여 빠른 검색을 가능하게 함
        exclude_set = set(exclude)
        
        while True:
            random_id = random.randint(min_value, max_value)  # 랜덤 숫자 생성
            if random_id not in exclude_set:  # 제외할 숫자가 아닐 경우
                return random_id  # 유효한 랜덤 숫자 반환


    def calculate_funds(self, slot):
        """
        슬롯 개수에 따라 자금을 할당합니다.
        """
        #전체 예수금 조회
        data = self.kis_api.get_balance()
        balance = float(data.get('output2')[0].get('dnca_tot_amt'))

        try:
            if slot == 3:
                allocated_funds = [balance * 0.33] * 3  # 3개 슬롯에 33%씩 할당
            elif slot == 2:
                allocated_funds = [balance * 0.50] + [balance * 0.50]  # 2개 슬롯에 50%씩 할당
            elif slot == 1:
                allocated_funds = [balance]  # 1개 슬롯에 전체 자금 할당
            else:
                allocated_funds = []  # 슬롯이 없으면 빈 리스트 반환

            return allocated_funds
        except Exception as e:
            print(f"Error allocating funds: {e}")
            return []

     
    def allocate_stock(self):
        """
        세션에 거래할 종목을 할당
        """
        db = DatabaseManager()

        try:
            # selected_stocks에서 첫 번째 종목 가져오기
            selected_stock = db.get_selected_stocks()  # selected_stocks 조회
            if selected_stock:
                db.delete_selected_stock_by_no(selected_stock[0][0])  # no로 삭제 
            return selected_stock

        except Exception as e:
            print(f"Error allocating funds: {e}")
            return []
