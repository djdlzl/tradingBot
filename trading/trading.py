"""
api와 db 모듈을 사용해서 실제 트레이딩에 사용될 로직 모듈을 개발
"""

from datetime import date
import schedule
from utils.date_utils import DateUtils
from database.db_manager import DatabaseManager
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
            # self.kis_api.print_korean_response(upper_limit_stocks)
            
            # 상한가 종목 정보 추출
            stocks_info = [(stock['mksc_shrn_iscd'], stock['hts_kor_isnm'], stock['stck_prpr'], stock['prdy_ctrt']) 
                        for stock in upper_limit_stocks['output']]
            # 영업일 기준 날짜 가져오기
            today = date.today()
            if not DateUtils.is_business_day(today):
                today = DateUtils.get_previous_business_day(today)
            # 데이터베이스에 저장
            db = DatabaseManager()
            if stocks_info:  # 상한가 종목이 있는 경우에만 저장
                db.save_upper_limit_stocks(today.isoformat(), stocks_info)
            else:
                print("상한가 종목이 없습니다.")
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
        """
        자금을 관리하고 매수할 종목에 따라 사용할 금액을 결정합니다.
        """
        balance = self.kis_api.get_balance()  # 현재 예수금 조회
        cash = int(balance.get('output2')[0].get('dnca_tot_amt'))  # 현재 예수금 조회
        current_stocks = [stock['prdt_name'] for stock in balance['output1']]
        num_stocks = len(current_stocks)
        
        if num_stocks == 0:
            amount_to_use = cash * 0.33  # 전체 예수금의 33%
        elif num_stocks == 1:
            amount_to_use = cash * 0.50  # 남은 예수금의 50%
        elif num_stocks == 2:
            amount_to_use = cash  # 나머지 예수금 100%
        else:
            amount_to_use = 0  # 슬롯이 가득 찼을 경우

        return amount_to_use

    def allocate_funds(self, selected_stocks):
        """
        선택된 종목에 자금을 할당합니다.

        :param selected_stocks: 매수할 종목 리스트
        :return: 각 종목에 할당된 자금 리스트
        """
        balance = self.kis_api.get_balance()  # 현재 예수금 조회
        cash = int(balance.get('output2')[0].get('dnca_tot_amt'))  # 현재 예수금 조회
        current_stocks = [stock['prdt_name'] for stock in balance['output1']]
        num_current_stocks = len(current_stocks)

        # 자금 할당 로직
        if num_current_stocks == 0:
            # 계좌에 종목이 없을 때
            allocation = [cash * 0.33, cash * 0.33, cash]  # 1/3, 1/3, 전체
        elif num_current_stocks == 1:
            # 계좌에 종목이 1개 있을 때
            allocation = [cash * 0.50, cash]  # 50%, 전체
        elif num_current_stocks == 2:
            # 계좌에 종목이 2개 있을 때
            allocation = [cash]  # 전체
        else:
            allocation = []  # 슬롯이 가득 찼을 경우

        return allocation[:len(selected_stocks)]  # 선택된 종목 수에 맞게 자금 할당

    def execute_trading_cycle(self):
        """
        매수 사이클을 실행합니다.
        거래 세션 생성
        """
        db_manager = DatabaseManager()  # DB 연결 생성
        selected_stocks = self.select_stocks_to_buy()  # 매수할 종목 선택

        if selected_stocks:
            for stock in selected_stocks:
                session_id = db_manager.create_trade_session()  # 거래 세션 생성
                self.schedule_buy(stock, session_id, db_manager)  # 매수 예약
        else:
            print("매수할 종목이 없습니다.")

        db_manager.close()  # 모든 세션 종료 후 DB 연결 닫기

    def schedule_buy(self, stock, session_id, db_manager):
        """
        매수 작업을 예약합니다.
        
        :param stock: 매수할 종목
        :param session_id: 거래 세션 ID
        :param db_manager: 데이터베이스 매니저 인스턴스
        """
        # 매수 시간 설정
        buy_times = [
            (14, 10),  # 14:10
            (14, 40),  # 14:40
            (15, 10)   # 15:10
        ]

        for hour, minute in buy_times:
            schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(self.buy_stocks, stock, session_id, self.allocate_funds([stock])[0], db_manager)

        # day_count 업데이트
        db_manager.update_trade_session_status(session_id, 1)  # 초기값 1로 설정

    def buy_stocks(self, stock, session_id, allocation, db_manager):
        """
        선택된 종목에 대해 분할 매수를 수행합니다.

        :param stock: 매수할 종목
        :param session_id: 거래 세션 ID
        :param allocation: 할당된 자금
        :param db_manager: 데이터베이스 매니저 인스턴스
        """
        # 매수 금액 설정
        buy_amount_per_time = allocation * 0.11  # 1회 매수 금액
        current_price, _ = self.kis_api.get_current_price(stock)  # 현재가 조회
        max_shares = int(buy_amount_per_time // current_price)  # 최대 매수 가능 주식 수

        # 매수 실행
        if max_shares > 0:
            self.kis_api.place_order(stock, "1", max_shares)  # 매수 주문
            db_manager.save_trade_session(session_id, stock, max_shares, current_price, "active")  # 거래 세션 저장
            print(f"매수 완료: {stock}, 수량: {max_shares}, 가격: {current_price}")
        else:
            print(f"매수할 주식이 없습니다: {stock}")

        # day_count 업데이트
        current_day_count = db_manager.get_active_trade_sessions(session_id)[0][5]  # 현재 day_count 조회
        db_manager.update_trade_session_day_count(session_id, current_day_count + 1)  # 거래 일수 증가