"""
api와 db 모듈을 사용해서 실제 트레이딩에 사용될 로직 모듈을 개발
"""

from datetime import datetime, timedelta
from database.db_manager import DatabaseManager
from utils.date_utils import DateUtils
from api.kis_api import KISApi
import random
import time

class TradingLogic:
    """
    트레이딩과 관련된 로직들. main에는 TradinLogic 클래스만 있어야 함.
    """
    def __init__(self):
        self.kis_api = KISApi()
        self.date_utils = DateUtils()
        
######################################################################################
#########################    주문 메서드   ###################################
######################################################################################

    def place_order(self, ticker, quantity):
        """
        주식 매수
        """
        res = self.kis_api.place_order(ticker, quantity)
        return res
    
    
    def sell_order(self, ticker, quantity):
        """
        주식 매수
        """
        res = self.kis_api.sell_order(ticker, quantity)
        return res
        
######################################################################################
#########################    상한가 조회 관련 메서드   #####################################
######################################################################################

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

######################################################################################
#########################    상한가 셀렉 메서드   ###################################
######################################################################################

    def select_stocks_to_buy(self):
        """
        2일 전 상한가 종목의 가격과 현재가를 비교하여 매수할 종목을 선정(선별)합니다.
        """
        db = DatabaseManager()
        
        # # 이전 selected_stocks 정보 삭제
        # self.init_selected_stocks()
        
        selected_stocks = []
        tickers_with_prices = db.get_upper_limit_stocks_two_days_ago()  # 2일 전 상한가 종목 가져오기
        print(tickers_with_prices)
        for stock in tickers_with_prices:
            # 현재가 가져오기
            current_price, temp_stop_yn = self.kis_api.get_current_price(stock[0])
            # 매수 조건: 현재가가 상한가 당시 가격보다 -8% 이상 하락하지 않은 경우
            if int(current_price) > (int(stock[2]) * 0.92) and temp_stop_yn=='N':  # -8% 이상 하락, 거래정지 N
                print(f"################ 매수 후보 종목: {stock[0]}, 종목명: {stock[1]} (현재가: {current_price}, 상한가 당시 가격: {stock[2]})")
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
        db = DatabaseManager()
        session_info = self.check_trading_session()
        fund = self.calculate_funds(session_info['slot'])
        
        # 새로운 세션을 DB에 저장
        for _ in range(int(session_info['slot'])):
            result = self.add_new_trading_session(fund)
            if result == None:
                break
        
        print("add_new_trading_session 새 세션 생성 완료")
        
        try:
            # 거래 세션을 조회
            sessions = db.load_trading_session()
            if not sessions:
                print("진행 중인 거래 세션이 없습니다.")
                return
            
            # 주문 결과 리스트로 저장
            order_list = []
            tot_ccld_qty
            for session in sessions:
                random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count = session

                # 세션 정보로 주식 주문
                # self.place_order_and_update_session(random_id, start_date, current_date, ticker, name, fund, spent_fund, count)
                order_result = self.place_order_session(random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count)
                order_list.append(order_result)
            return order_list
        except Exception as e:
            print("Error in trading session: ", e)




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
        # id: 세션 받아오는 메서드, trading_session의 id값을 받아와서 exclude_set에 저장
        db = DatabaseManager()
        
        exclude_num = []
        random_id = self.generate_random_id(exclude=exclude_num)
        
        # 날짜
        today = datetime.now()
        
        # 종목 정보
        stock = self.allocate_stock()

        if stock == None:
            return None

        # 컬럼 초기화
        count = 0
        spent_fund = 0
        quantity = 0
        avr_price = 0
        # 거래 세션 생성
        db.save_trading_session(random_id, today, today, stock['ticker'], stock['name'], fund, spent_fund, quantity, avr_price, count)
        
        return True


    def place_order_session(self, id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count):
        """
        주식 매수 주문을 진행

        :param ticker: 종목 코드
        :param fund: 투자 금액
        :param count: 매수 수량
        :param id: 세션 ID
        :param start_date: 거래 시작 날짜
        :param current_date: 현재 날짜
        :param name: 종목명
        """
        # API 초과 방지
        time.sleep(0.9)
        
        db = DatabaseManager()
        
        result = self.kis_api.get_current_price(ticker)
        price = int(result[0])

        # 매수할 금액 계산
        amount_per_order = int(fund * 0.11)  # 각 매수 시 사용할 금액

        if count < 8:  # 0부터 7까지는 일반 매수
            quantity = amount_per_order / price  # 매수 수량 계산
        else:  # 9회차 매수 (count가 8일 때)
            remaining_fund = fund - spent_fund  # 남은 자금 계산
            quantity = remaining_fund / price  # 남은 자금으로 매수 수량 계산

        # 매수 주문을 진행
        quantity = int(quantity)
        order_result = self.kis_api.place_order(ticker, quantity)
        print("place_order_session:  주문 실행", order_result)
        
        # 'rt_cd': '1' 세션 취소
        print(order_result['rt_cd'], count, ticker)
        if order_result['rt_cd'] and count == 0:
            print("실행됨")
            db.delete_session_one_row(id)

        db.close()
        
        return order_result


    def load_and_update_trading_session(self, order_list):
        """
        트레이딩 세션을 불러옵니다.
        """
        db = DatabaseManager()
        
        try:
            # 거래 세션을 조회
            sessions = db.load_trading_session()
            if not sessions:
                print("진행 중인 거래 세션이 없습니다.")
                return
            
            # 주문 결과 리스트로 저장
            for index ,session in enumerate(sessions, start=0):
                random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count = session
                self.update_session(random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count, order_list[index])
                
        except Exception as e:
            print("Error in update_trading_session: ", e)
            

    def update_session(self, random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count, order_result):
        """
        세션을 업데이트합니다.

        :param random_id: 세션 ID
        :param start_date: 거래 시작 날짜
        :param current_date: 현재 날짜
        :param ticker: 종목 코드
        :param name: 종목명
        :param fund: 투자 금액
        :param spent_fund: 사용한 총 금액
        :param avr_price: 평균 단가
        :param quantity: 매수한 총 주식 수
        :param count: 거래 횟수
        """
        # API 초과 방지
        time.sleep(0.8)
        
        db = DatabaseManager()
        odno = order_result.get('output', {}).get('ODNO')
        if odno is not None:
            # 사용 금액, 체결 주식 수
            result = self.kis_api.daily_order_execution_inquiry(odno)
            real_spent_fund = result.get('output1')[0].get('tot_ccld_amt')
            real_quantity = result.get('output1')[0].get('tot_ccld_qty')
            
            # 매입단가
            result = self.kis_api.balance_inquiry()
            index_of_odno = next((index for index, d in enumerate(result) if d.get('pdno') == ticker), -1)
            avr_price = result[index_of_odno].get("pchs_avg_pric")
            avr_price = int(avr_price)
            print("update_session - avr_price", avr_price)
        else:
            print("주문 번호가 없습니다. 사유: ", order_result.get('msg1'))
            return

        # current date 갱신
        current_date = datetime.now()

        # 주문 결과에 따라 세션 업데이트
        if order_result.get('rt_cd') == "0":  # 주문 성공 여부 확인
            spent_fund += int(real_spent_fund)  # 총 사용 금액 업데이트
            quantity += int(real_quantity)
            
            #count 증가
            count = count + 1

            # 세션 업데이트
            db.save_trading_session(random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count)
            db.close()

            print("세션 업데이트 완료")
        else:
            print(f"주문 실패: {order_result.get('message')}")
            db.close()


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
        
        # 세션에 할당된 자금 조회
        db = DatabaseManager()
        sessions = db.load_trading_session()
        session_fund = 0
        for session in sessions:
            random_id, start_date, current_date, ticker, name, fund, spent_fund, count = session
            session_fund += int(fund)
        
        try:
            if slot == 3:
                allocated_funds = balance / 3  # 3개 슬롯에 33%씩 할당
            elif slot == 2:
                allocated_funds = (balance - session_fund) / 2  # 2개 슬롯에 50%씩 할당
            elif slot == 1:
                allocated_funds = (balance - session_fund)  # 1개 슬롯에 전체 자금 할당
            else:
                allocated_funds = 0  # 슬롯이 없으면 빈 리스트 반환
                
            print("calculate_funds:  ", allocated_funds)
            return int(allocated_funds)
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
            if selected_stock is not None:
                db.delete_selected_stock_by_no(selected_stock['no'])  # no로 삭제 
                return selected_stock
            else:
                return None

        except Exception as e:
            print(f"Error allocating funds: {e}")
            return None


######################################################################################
###############################    실시간 메서드   ####################################
######################################################################################

    def live_get_price(self):
        db =DatabaseManager()
        sessions = db.load_trading_session()
        
        tickers = []
        
        for session in sessions:
            tickers.append(session[3])
            
        print(tickers)
        
        for ticker in tickers:
            live_price = self.kis_api.live_get_price(ticker)
            print(live_price)