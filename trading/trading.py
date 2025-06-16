"""
api와 db 모듈을 사용해서 실제 트레이딩에 사용될 로직 모듈을 개발
"""

import random
import time
import asyncio
from datetime import datetime, timedelta, date
from database.db_manager import DatabaseManager
from utils.date_utils import DateUtils
from utils.slack_logger import SlackLogger
from api.kis_api import KISApi
from api.kis_websocket import KISWebSocket
from config.condition import DAYS_LATER, BUY_PERCENT, BUY_WAIT, SELL_WAIT, COUNT
from concurrent.futures import ThreadPoolExecutor


class TradingLogic:
    """
    트레이딩과 관련된 로직들. main에는 TradinLogic 클래스만 있어야 함.
    """
    def __init__(self):
        self.kis_api = KISApi()
        self.date_utils = DateUtils()
        self.slack_logger = SlackLogger()

    # def test(self):


######################################################################################
##################################    주문 메서드   ###################################
######################################################################################

    def buy_order(self, ticker, quantity):
        """
        주식 매수
        """
        try:
            # 매수 주문 전 로그
            self.slack_logger.send_log(
                level="INFO",
                message="매수 주문 시작",
                context={
                    "종목코드": ticker,
                    "주문수량": quantity,
                    "주문타입": "매수"
                }
            )
            
            ##########################################################
            
            # 매수 주문
            order_result = self.kis_api.place_order(ticker, quantity, order_type='buy')
            print("place_order_session:  주문 실행", ticker, order_result)
            
            if order_result['rt_cd'] == '1':
                # 주문 실패 결과 로그
                self.slack_logger.send_log(
                    level="INFO" if order_result['rt_cd'] == "0" else "ERROR",
                    message="매수 주문 결과",
                    context={
                        "종목코드": ticker,
                        "주문번호": order_result.get('output', {}).get('ODNO'),
                        "상태": "성공" if order_result['rt_cd'] == "0" else "실패",
                        "메시지": order_result.get('msg1')
                    }
                )
                return order_result
            
            time.sleep(BUY_WAIT)
            # 미체결 수량 확인
            unfilled_qty = self.order_complete_check(order_result)
            
            #정상 매수일 경우 return
            if unfilled_qty == 0:              
                return order_result
            
            new_order_result = order_result
            
            # 미체결 시 재주문 - 모듈화 필요
            while unfilled_qty > 0:
                
                # 미체결 주문 취소
                cancel_result = self.kis_api.cancel_order(new_order_result.get('output').get('ODNO'))
                print("cancel_result:- ",cancel_result)

                # 초당 거래 건수 초과 방지
                time.sleep(1)

                # 재주문
                new_order_result = self.kis_api.place_order(ticker, unfilled_qty, order_type='buy')
                print("새로운 매수 결과 new_order_result",new_order_result)
                
                # 매수 주문 후 대기
                time.sleep(BUY_WAIT)
                
                # 체결 완료 검증
                unfilled_qty = self.order_complete_check(new_order_result)
            
                        
            ###############################################################
            
            # 주문 결과 로그
            self.slack_logger.send_log(
                level="INFO" if order_result['rt_cd'] == "0" else "ERROR",
                message="매수 주문 결과",
                context={
                    "종목코드": ticker,
                    "주문번호": order_result.get('output', {}).get('ODNO'),
                    "상태": "성공" if order_result['rt_cd'] == "0" else "실패",
                    "메시지": order_result.get('msg1')
                }
            )
            
            return order_result
        except Exception as e:
            print("buy_order 중 에러 발생 : ", e)
    
    def sell_order(self, session_id, ticker, quantity, price=None):
        """
        주식 매수
        미체결 다시 주문하는 로직 업데이트
        """
        try:
            order_result = self.kis_api.place_order(ticker, quantity, order_type='sell', price=price)
            print("sell_order:- ",ticker, quantity, price, order_result)
            
            #주문 체결까지 기다리기
            time.sleep(SELL_WAIT)
            
            ### 미체결 확인
            unfilled_qty = self.order_complete_check(order_result)

            new_order_result = order_result

            ### 미체결 수량이 0이상이면 실행 = 미체결이 존재하면 실행        
            while unfilled_qty > 0:
                
                # 주문취소
                cancel_result = self.kis_api.cancel_order(new_order_result.get('output').get('ODNO'))
                print("cancel_result: - ",cancel_result)

                # 초당 거래 건수 초과 방지
                time.sleep(1)
                
                # 재주문
                new_order_result = self.kis_api.place_order(ticker, unfilled_qty, order_type='sell')
                print("새로운 매도 결과 new_order_result",new_order_result)

                # 주문 체결까지 기다리기
                time.sleep(SELL_WAIT)
                
                # 체결 완료 검증
                unfilled_qty = self.order_complete_check(new_order_result)
            
            self.delete_finished_session(session_id)

            return True
        except Exception as e:
            print("sell_order 중 에러 발생 : ", e)

    def order_complete_check(self, order_result):
        print("order_complete_check 실행")
        
        #일별주문체결에서 총주문수량, 체결수량 확인
        conclusion_result = self.kis_api.daily_order_execution_inquiry(order_result.get('output').get('ODNO'))
        print("일별체결 결과: ", conclusion_result)
        
        #미체결수량 계산
        unfilled_qty = int(conclusion_result.get('output1')[0].get('rmn_qty'))
        print("주문확인 order_complete_check: - ", unfilled_qty)

        return unfilled_qty 
        
######################################################################################
#########################    종목 조회 관련 메서드   #################################
######################################################################################

    def fetch_and_save_previous_upper_limit_stocks(self):
        """
        상한가 종목을 받아온 후,
        DB에 상한가 종목을 저장
        """
        upper_limit_stocks = self.kis_api.get_upper_limit_stocks()
        if upper_limit_stocks:

            # 상한가 종목 정보 추출
            stocks_info = [(stock['mksc_shrn_iscd'], stock['hts_kor_isnm'], stock['stck_prpr'], stock['prdy_ctrt']) for stock in upper_limit_stocks['output']]
            
            # 오늘 날짜 가져오기
            today = datetime.now().date()
            # 영업일 여부 확인 후 날짜 결정
            if self.date_utils.is_business_day(today):
                current_day = today
            else:
                # 오늘이 영업일이 아니면 가장 가까운 이전 영업일 사용
                current_day = self.date_utils.get_previous_business_day(datetime.now(), 1)
            
            # 데이터베이스에 저장
            db = DatabaseManager()
            if stocks_info:  # 리스트가 비어있지 않은 경우
                db.save_upper_limit_stocks(current_day.strftime('%Y-%m-%d'), stocks_info)  # 날짜를 문자열로 변환하여 저장
                print(current_day.strftime('%Y-%m-%d'), stocks_info)
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


    def get_volume(self, ticker):
        volumes = self.kis_api.get_stock_volume(ticker)
        
        print(f"최근 3일간 거래량: {volumes}")

        # 거래량 비교
        diff_1_2, diff_2_3 = self.kis_api.compare_volumes(volumes)
        print(f"1일 전과 2일 전의 거래량 차이: {diff_1_2}%")
        print(f"2일 전과 3일 전의 거래량 차이: {diff_2_3}%")
        
        
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
        tickers_with_prices = db.get_upper_limit_stocks_days_ago()  # N일 전 상한가 종목 가져오기
        print('tickers_with_prices:  ',tickers_with_prices)
        for stock in tickers_with_prices:
            current_price, temp_stop_yn = self.kis_api.get_current_price(stock.get('ticker'))
            # 매수 조건: 현재가가 상한가 당시 가격보다 -8% 이상 하락하지 않은 경우
            if int(current_price) > (int(stock.get('closing_price')) * BUY_PERCENT) and temp_stop_yn=='N':  # -8% 이상 하락, 거래정지 N
                print(f"################ 매수 후보 종목: {stock.get('ticker')}, 종목명: {stock.get('name')} (현재가: {current_price}, 상한가 당시 가격: {stock.get('closing_price')})")
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
        today = datetime.now().date()
        # 영업일 여부 확인 후 날짜 결정
        if self.date_utils.is_business_day(today):
            current_day = today
        else:
            current_day = self.date_utils.get_previous_business_day(datetime.now(), 1)
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
        
        # 새로운 세션을 DB에 저장
        for _ in range(int(session_info['slot'])):
            self.add_new_trading_session(session_info['slot'])

        
        try:
            #SLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACK
            # 세션 시작 로그
            self.slack_logger.send_log(
                level="INFO",
                message="상한가 매매 트레이딩 세션 시작",
                context={
                    "세션수": session_info['session'],
                    "가용슬롯": session_info['slot']
                }
            )
            
            # 거래 세션을 조회 및 검증
            sessions = db.load_trading_session()
            db.close()
            if not sessions:
                print("start_trading_session - 진행 중인 거래 세션이 없습니다.")
                return
            
            # 주문 결과 리스트로 저장
            order_list = []

            for session in sessions:
                # 6번 거래한 종목은 더이상 매수하지 않고 대기
                if session.get("count") == COUNT:
                    print(session.get('name'),"은 6번의 거래를 진행해 넘어갔습니다.")
                    continue
                
                # 세션 정보로 주식 주문
                order_result = self.place_order_session(session)
                order_list.append(order_result)
                
            return order_list
        except Exception as e:
            db.close()
            print("Error in trading session: ", e)


    def check_trading_session(self):
        """
        거래 전, 트레이딩 세션 테이블에 진행 중인 거래세션이 있는지 확인하고,
        세션 수에 따라 거래를 진행하거나 새로운 세션을 생성합니다.
        """
        db = DatabaseManager()
        
        # 현재 거래 세션의 수를 확인
        db.cursor.execute('SELECT COUNT(*) FROM trading_session')
        result = db.cursor.fetchone()
        session_count = result.get('COUNT(*)')
        slot_count = 3 - session_count
        db.close()
        
        return {'session': session_count, 'slot': slot_count}


    def add_new_trading_session(self, slot):
        """
        새로운 세션 생성
        """
        # id: 세션 받아오는 메서드, trading_session의 id값을 받아와서 exclude_set에 저장
        db = DatabaseManager()
        fund = self.calculate_funds(slot)

        exclude_num = []
        random_id = self.generate_random_id(exclude=exclude_num)
        
        # 날짜
        today = datetime.now()
        

        # 컬럼 초기화
        count = 0
        spent_fund = 0
        quantity = 0
        avr_price = 0
        
        # 종목 정보
        stock = self.allocate_stock()
        if stock is None:
            return None
        
        # 거래 세션 생성
        db.save_trading_session(random_id, today, today, stock['ticker'], stock['name'], fund, spent_fund, quantity, avr_price, count)
        db.close()
        

        return


    def place_order_session(self, session):
        """
        주식 매수 주문을 진행

        :param ticker: 종목 코드
        :param fund: 투자 금액
        :param count: 매수 횟수
        :param random_id: 세션 ID
        :param start_date: 거래 시작 날짜
        :param current_date: 현재 날짜
        :param name: 종목명
        """
        # API 초과 방지
        time.sleep(0.9)
        
        db = DatabaseManager()
        
        result = self.kis_api.get_current_price(session.get('ticker'))

        price = int(result[0])

        # 매수할 금액 계산
        ratio = round(100/COUNT) / 100 
        fund_per_order = int(float(session.get('fund')) * ratio)  # 각 매수 시 사용할 금액

        if session.get('count') < COUNT-1:  # 0부터 COUNT-1까지는 일반 매수
            quantity = fund_per_order / price  # 매수 수량 계산
        else:  # COUNT회차 (마지막) 매수
            remaining_fund = float(session.get('fund')) - session.get('spent_fund')  # 남은 자금 계산
            quantity = remaining_fund / price  # 남은 자금으로 매수 수량 계산

        # 매수 주문을 진행
        quantity = int(quantity)
        
        if session.get('count') < COUNT:
            while True:
                order_result = self.buy_order(session.get('ticker'), quantity)
                
                if order_result['msg1'] == '초당 거래건수를 초과하였습니다.':
                    print("초당 거래건수 초과 시 재시도")
                    continue
                
                if order_result['msg1'] == '모의투자 주문처리가 안되었습니다(매매불가 종목)':
                    self.add_new_trading_session(1) #슬롯 개수. 새 트레이딩 세션 생성
                    sessions = db.load_trading_session()
                    session = sessions[-1]
                    order_result = self.place_order_session(session)
                    
                break
        
        # 'rt_cd': '1' 세션 취소
        if order_result['rt_cd'] == '1' and session.get('count') == 0:
            print("첫 주문 실패 시 세션 삭제", session)
            db.delete_session_one_row(session.get('id'))

        db.close()
        
        return order_result


    def load_and_update_trading_session(self, order_list):
        """
        트레이딩 세션을 불러옵니다.
        """
        db = DatabaseManager()
        print('load_and_update_trading_session',order_list)
        try:
            # 거래 세션을 조회
            sessions = db.load_trading_session()
            if not sessions:
                print("load_and_update_trading_session - 진행 중인 거래 세션이 없습니다.")
                return
            
            # 주문 결과 리스트로 저장
            for index ,session in enumerate(sessions, start=0):
                print("주문 결과 리스트로 저장", index ,session)
                self.update_session(session, order_list[index])
            db.close()
        except Exception as e:
            print("Error in update_trading_session: ", e)
            db.close()
            

    def update_session(self, session, order_result):
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
        try:
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
                index_of_odno = next((index for index, d in enumerate(result) if d.get('pdno') == session.get('ticker')), -1)
                avr_price = result[index_of_odno].get("pchs_avg_pric")
                avr_price = int(float(avr_price))
                print("update_session - avr_price", avr_price)
            else:
                print("주문 번호가 없습니다. 저장을 취소합니다. 사유: ", order_result.get('msg1'))
                return

            # current date 갱신
            current_date = datetime.now()

            # 주문 결과에 따라 세션 업데이트
            if order_result.get('rt_cd') == "0":  # 주문 성공 여부 확인
                spent_fund = int(session.get('spent_fund')) + int(real_spent_fund)  # 총 사용 금액 업데이트
                quantity = int(session.get('quantity')) + int(real_quantity)
                
                #count 증가
                count = count + 1

                # 세션 업데이트
                db.save_trading_session(session.get('id'), session.get('start_date'), current_date, session.get('ticker'), session.get('name'), session.get('fund'), spent_fund, quantity, avr_price, count)
                db.close()

                # 새로운 모니터링 스레드 생성
                monitoring_thread = threading.Thread(
                    target=self._run_monitoring_for_session,
                    args=(session,)
                )
                monitoring_thread.start()

                #SLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACK
                # 세션 업데이트 로그
                self.slack_logger.send_log(
                    level="INFO",
                    message="세션 업데이트",
                    context={
                        "세션ID": session.get('id'),
                        "종목명": session.get('name'),
                        "투자금액": session.get('fund'),
                        "사용금액": spent_fund,
                        "평균단가": avr_price,
                        "보유수량": quantity,
                        "거래횟수": count
                    }
                )
                
            else:
                print(f"주문 실패: {order_result.get('message')}")
                db.close()
        except Exception as e:
            print(f"업데이트 중 에러 발생 {e}")

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
        data = self.kis_api.purchase_availability_inquiry()
        balance = float(data.get('output').get('nrcvb_buy_amt'))
        print('calculate_funds - 가용 가능 현금: ',balance)
        
        # 세션에 할당된 자금 조회
        # 데이터베이스 연결 및 쿼리 실행을 with 문으로 관리
        session_fund = 0
        with DatabaseManager() as db:
            sessions = db.load_trading_session()
            for session in sessions:
                fund = session.get('spent_fund')
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
                
            print("calculate_funds - allocated_funds: ", allocated_funds)
            return int(allocated_funds)
        except Exception as e:
            print(f"Error allocating 여기냐 funds: {e}")
            return 0

     
    def allocate_stock(self):
        """
        세션에 거래할 종목을 할당
        """
        db = DatabaseManager()

        try:
            # selected_stocks에서 첫 번째 종목 가져오기
            selected_stock = db.get_selected_stocks()  # selected_stocks 조회
            
            if selected_stock is not None:
                db.delete_selected_stock_by_no(selected_stock.get('no'))  # no로 삭제 
                db.close()
                return selected_stock
            else:
                db.close()
                return None
            
        except Exception as e:
            print(f"여기냐 Error allocating funds: {e}")
            return None


        
######################################################################################
###############################    모니터링 메서드   ####################################
######################################################################################

    async def monitor_for_selling(self, sessions_info):
        """
        sessions_info 요소:
        session_id, ticker, quantity, avr_price, target_date
        """
        # session_id, ticker, quantity, avr_price, target_date = sessions_info(리스트임)
        try:
            kis_websocket = KISWebSocket(self.sell_order)
            # 웹소켓 연결 및 모니터링 시작
            complete = await kis_websocket.real_time_monitoring(sessions_info)
            print("콜백함수 정상 실행")
            
            # 모니터링 상태 확인
            if complete:
                print("모니터링이 정상적으로 종료되었습니다.")
            else:
                print("진행 중인 세션 없음")
                
        except Exception as e:
            print(f"모니터링 오류: {e}")



    def delete_finished_session(self, session_id):
        db = DatabaseManager()
        db.delete_session_one_row(session_id)
        print(session_id, " 세션을 삭제했습니다.")


    def get_session_info(self):
        """
        매도 모니터링에 필요한 세션 정보 받아오기
        """
        db = DatabaseManager()
        sessions = db.load_trading_session()
        db.close()
        
        sessions_info = []
        
        for session in sessions:
            #강제 매도 일자
            target_date = self.date_utils.get_target_date(date.fromisoformat(str(session.get('start_date')).split()[0]), DAYS_LATER)
            info_list = session.get('id'), session.get('ticker'), session.get('name'), session.get('quantity'), session.get('avr_price'), session.get('start_date'), target_date
            sessions_info.append(info_list)
            
        return sessions_info

    async def start_monitoring_for_session(self, session):
        ticker = session.get('ticker')
        name = session.get('name')
        quantity = session.get('quantity')
        avr_price = session.get('avr_price')
        start_date = session.get('start_date')
        target_date = self.date_utils.get_target_date(start_date, DAYS_LATER_UPPER)
        
        session_info = [(session.get('id'), ticker, name, quantity, avr_price, start_date, target_date)]
        
        if self.kis_websocket is None:
            self.kis_websocket = KISWebSocket(self.sell_order)
        await self.kis_websocket.real_time_monitoring(session_info)
        

    def _run_monitoring_for_session(self, session):
        # 새로운 이벤트 루프 생성
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self.start_monitoring_for_session(session))
        finally:
            loop.close()
    
######################################################################################
###############################    리포트 메서드   ####################################
######################################################################################

    # def daily_report(self):
    #     sessions_info = self.get_session_info()
    #     for session_id, ticker, qty, price, start_date, target_date in sessions_info:


######################################################################################
#########################    호가 돌파매매 메서드   #################################
######################################################################################

    def breakout_trading(self,):
        result = self.kis_api.get_volume_rank()
        output = result.get('output')
        selected_stocks = []
        for stock in output:
            
            if int(prdy_ctrt) > 2:
                hts_kor_isnm = stock['hts_kor_isnm'] #이름
                mksc_shrn_iscd = stock['mksc_shrn_iscd'] #종목코드
                prdy_ctrt = stock['prdy_ctrt'] #전일대비 상승률
                vol_inrt = stock['vol_inrt'] #거래량증가율
                acml_tr_pbmn = stock['acml_tr_pbmn'] #누적 거래 대금
                info_list = hts_kor_isnm, mksc_shrn_iscd
                selected_stocks.append()
            
        # 
