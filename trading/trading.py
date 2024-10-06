"""
api와 db 모듈을 사용해서 실제 트레이딩에 사용될 로직 모듈을 개발
"""

from datetime import datetime
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
            # self.kis_api.print_korean_response(upper_limit_stocks)
            
            # 상한가 종목 정보 추출
            stocks_info = [(stock['mksc_shrn_iscd'], stock['hts_kor_isnm'], stock['stck_prpr'], stock['prdy_ctrt']) 
                        for stock in upper_limit_stocks['output']]
            # 오늘 날짜 가져오기
            today = datetime.now()  # 현재 날짜와 시간 가져오기
            # 데이터베이스에 저장
            db = DatabaseManager()
            if stocks_info:  # 리스트가 비어있지 않은 경우
                db.save_upper_limit_stocks(today.strftime('%Y-%m-%d'), stocks_info)  # 날짜를 문자열로 변환하여 저장
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

    def allocate_funds(self, total_fund):
        """
        슬롯 개수에 따라 자금을 할당합니다.

        :param total_fund: 총 자금
        :return: 각 세션에 할당된 자금 리스트
        """
        db = DatabaseManager()
        try:
            # 현재 trading_session 테이블의 슬롯 개수 확인
            db.cursor.execute('SELECT COUNT(*) FROM trading_session')
            slot_count = db.cursor.fetchone()[0]

            if slot_count == 3:
                allocated_funds = [total_fund * 0.33] * 3  # 3개 슬롯에 33%씩 할당
            elif slot_count == 2:
                allocated_funds = [total_fund * 0.50] + [total_fund * 0.50]  # 2개 슬롯에 50%씩 할당
            elif slot_count == 1:
                allocated_funds = [total_fund]  # 1개 슬롯에 전체 자금 할당
            else:
                allocated_funds = []  # 슬롯이 없으면 빈 리스트 반환

            return allocated_funds
        except Exception as e:
            print(f"Error allocating funds: {e}")
            return []
        finally:
            db.close()
            
    def allocate_stock(self):
        """
        세션에 거래할 종목을 할당
        """
        db = DatabaseManager()

        # selected_stocks에서 첫 번째 종목 가져오기
        selected_stock = db.get_selected_stocks()  # selected_stocks 조회
        if selected_stock:
            db.delete_selected_stock_by_no(selected_stock[0][0])  # no로 삭제
            
        # ... 기존 코드 ...

    def init_selected_stocks(self):
        """
        selected_stock 테이블 초기화
        """
        db = DatabaseManager()
        db.delete_selected_stocks()
        db.close()
