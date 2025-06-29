"""
api와 db 모듈을 사용해서 상승 눌림목매매 로직 모듈을 개발
"""

from nt import error
import random
import time
import asyncio
from tkinter import NONE
from numpy import int32
import pandas as pd
from datetime import datetime, timedelta, date
from database.db_manager_upper import DatabaseManager
from utils.date_utils import DateUtils
from utils.slack_logger import SlackLogger
from utils.trading_logger import TradingLogger
from api.kis_api import KISApi
from api.krx_api import KRXApi
from api.kis_websocket import KISWebSocket
from config.condition import DAYS_LATER_UPPER, BUY_PERCENT_UPPER, BUY_WAIT, SELL_WAIT, COUNT_UPPER, SLOT_UPPER, UPPER_DAY_AGO_CHECK, BUY_DAY_AGO_UPPER, PRICE_BUFFER
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import List, Dict, Optional, Union
from threading import Lock
from collections import deque

class TradingUpper():
    """
    트레이딩과 관련된 로직들. main에는 TradinLogic 클래스만 있어야 함.
    """
    # 체결 대기 세션 관리용 dict
    pending_sessions = {}

    def __init__(self):
        self.kis_api = KISApi()
        self.krx_api = KRXApi()
        self.date_utils = DateUtils()
        self.slack_logger = SlackLogger()
        self.logger = TradingLogger()  # 파일 로깅을 위한 TradingLogger 추가
        self.kis_websocket = None
        self.session_lock = Lock()  # 세션 업데이트용 락
        self.api_lock = Lock()  # API 호출용 락
        # 모니터링 루프 참조 (MainProcess에서 주입)
        self._monitor_loop: Optional[asyncio.AbstractEventLoop] = None

######################################################################################
#########################    상승 종목 받아오기 / 저장   ###################################
######################################################################################

    def fetch_and_save_previous_upper_stocks(self):
        self.logger.info("상승 종목 데이터 조회 시작")
        upper_stocks = self.kis_api.get_upAndDown_rank()
        if not upper_stocks or not isinstance(upper_stocks.get('output'), list):
            error_msg = "상승 종목 데이터가 유효하지 않습니다."
            print(error_msg)
            self.logger.error(error_msg)
            return

        stocks_info = []
        for stock in upper_stocks['output']:
            try:
                stocks_info.append((
                    stock['stck_shrn_iscd'],
                    stock['hts_kor_isnm'],
                    stock['stck_prpr'],
                    stock['prdy_ctrt']
                ))
            except KeyError as e:
                error_msg = f"상승 종목 데이터 누락: {e}"
                print(error_msg)
                self.logger.error(error_msg, {"stock_data": str(stock)})
                continue

        today = datetime.now().date()
        # is_business_day은 bool을 반환하므로 날짜 계산에 사용하지 않는다.
        is_bd = self.date_utils.is_business_day(today)
        # 영업일이 아니라면 가장 최근 영업일(전 영업일)을 구한다.
        if not is_bd:
            current_day = self.date_utils.get_previous_business_day(datetime.now(), 1)
        else:
            current_day = today
        date_str = current_day.strftime('%Y-%m-%d')
        
        #DB에서 상승 종목 데이터 저장
        db = DatabaseManager()
        if stocks_info:
            self.logger.info(f"상승 종목 데이터 저장", {"date": date_str, "count": len(stocks_info)})
            db.save_upper_stocks(date_str, stocks_info)
            print(date_str, stocks_info)
        else:
            no_data_msg = "상승 종목이 없습니다."
            print(no_data_msg)
            self.logger.warning(no_data_msg, {"date": date_str})
        db.close()

######################################################################################
#########################    셀렉 메서드   ###################################
######################################################################################

    def select_stocks_to_buy(self):
        """
        2일 전 상한가 종목의 가격과 현재가를 비교하여 매수할 종목을 선정(선별)합니다.
        """
        db = DatabaseManager()
        
        # # 이전 selected_stocks 정보 삭제
        self.init_selected_stocks()
        
        selected_stocks = []
        tickers_with_prices = db.get_upper_stocks_days_ago(BUY_DAY_AGO_UPPER) or []  # N일 전 상승 종목 가져오기
        print('tickers_with_prices:  ',tickers_with_prices)
        for stock in tickers_with_prices:
            if stock is None:
                self.logger.warning("종목 정보가 None 입니다. 건너뜁니다.")
                continue
            ticker = stock.get('ticker')
            if not ticker:
                self.logger.warning(f"{stock} ticker 정보 누락, 건너뜁니다.")
                continue
            ### 조건1: 상승일 기준 10일 전까지 고가 20% 넘은 이력 여부 체크
            df = self.krx_api.get_OHLCV(stock.get('ticker'), UPPER_DAY_AGO_CHECK) # D+2일 8시55분에 실행이라 10일
            # 데이터프레임에서 최하단 2개 행을 제외
            filtered_df = df.iloc[:-2]
              # 종가 대비 다음날 고가의 등락률 계산
            percentage_diff = []
            for i in range(len(filtered_df)-1):
                close_price = filtered_df.iloc[i]['종가']
                next_day_high = filtered_df.iloc[i+1]['고가']
                percent_change = ((next_day_high - close_price) / close_price) * 100
                percentage_diff.append(percent_change)
              # 결과를 데이터프레임으로 만들고 포맷팅
            result_df = pd.DataFrame({'등락률': percentage_diff}, index=filtered_df.index[:-1])
              # 등락률이 20% 이상인 값이 있으면 False, 없으면 True를 리턴
            result_high_price = not (result_df['등락률'] >= 20).any()
            
            
            ### 조건2: 상승일 고가 - 매수일 현재가 -7.5% 체크 -> 매수하면서 체크
            last_high_price = df['고가'].iloc[-2]
            result_decline = False
            current_price_opt, trht_yn = self.kis_api.get_current_price(stock.get('ticker'))
            if current_price_opt is None:
                # 가격 조회 실패 시 건너뛰거나 기본 처리
                self.logger.warning(f"{stock.get('ticker')} 현재가 조회 실패")
                continue

            current_price = int(current_price_opt)
            if current_price > last_high_price * BUY_PERCENT_UPPER:
                result_decline = True
            
                
            ### 조건3: 상승일 거래량 대비 다음날 거래량 20% 이상인지 체크
            result_volume = self.get_volume_check(stock.get('ticker'))
            
            
            ### 조건4: 상장일 이후 1년 체크
            result_lstg = self.check_listing_date(stock.get('ticker'))
            
            ### 조건5: 과열 종목 제외
            stock_info = self.kis_api.get_stock_price(stock.get('ticker'))
            result_short_over = stock_info.get('output', {}).get('short_over_yn', 'N')
            
            print(stock.get('name'))
            # print('조건1: result_high_price:',result_high_price)
            # print('조건2: result_decline:',result_decline)
            # print('조건3: result_volume:',result_volume)
            # print('조건4: result_lstg:',result_lstg)
            # print('조건5: result_lstg:',result_lstg)
            print('매매 확인을 위해 임시로 모든 조건 통과')

            # if result_high_price and result_decline and result_volume and result_lstg and (result_short_over=='N'):
            if True:
                print(f"################ 매수 후보 종목: {stock.get('ticker')}, 종목명: {stock.get('name')} (현재가: {current_price}, 상한가 당시 가격: {stock.get('closing_price')})")
                selected_stocks.append(stock)
      
        # 선택된 종목을 selected_stocks 테이블에 저장
        if selected_stocks:
            db.save_selected_stocks(selected_stocks)  # 선택된 종목 저장

        db.close()
        
        return selected_stocks

######################################################################################
################################    삭제   ##########################################
######################################################################################

    def delete_old_stocks(self):
        """
        2개월 전 데이터 삭제
        """
        today = datetime.now().date()  # 현재 날짜
        all_holidays = self.date_utils.get_holidays()

        # 기준 날짜를 실제 날짜(today)로 설정. 영업일이 아니면 이전 영업일을 찾음
        if not self.date_utils.is_business_day(today):
            current_day = self.date_utils.get_previous_business_day(datetime.now(), 1)
        else:
            current_day = today

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
###############################    트레이딩 세션   ###################################
######################################################################################

    def start_trading_session(self):
        """
        거래 시작
        """

        # DB 트레이딩 세션 추가가
        session_info = self.add_new_trading_session()
        
        try:
            #SLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACK
            # 세션 시작 로그
            self.slack_logger.send_log(
                level="INFO",
                message="상승 추세매매 트레이딩 세션 시작",
                context={
                    "추가된 세션 종목": session_info['session'],
                    "추가된 슬롯": session_info['slot']
                }
            )
            # 거래 세션을 조회 및 검증
            with DatabaseManager() as db:
                sessions = db.load_trading_session_upper()
            
                if not sessions:
                    print("start_trading_session - 진행 중인 거래 세션이 없습니다.")
                    return
                
                print("세션 확인 완료:", sessions)

                # 주문 결과 리스트로 저장
                sessions_que = deque(sessions)
                order_lists = []
                processed_sessions = []  # 처리 완료된 세션 추적

                while sessions_que:
                    session = sessions_que.popleft()

                    # 이미 처리된 세션인지 확인
                    if session["id"] in processed_sessions:
                        print(f"세션 ID {session['id']}는 이미 처리되었습니다.")
                        continue
                    
                    # 2번 거래한 종목은 더이상 매수하지 않고 대기
                    if session.get("count") == COUNT_UPPER:
                        print(session.get('name'),"은 2번의 거래를 진행해 넘어갔습니다.")
                        processed_sessions.append(session["id"])
                        continue
                    
                    # 세션 정보로 주식 주문
                    print(f"세션 {session['id']} ({session['name']}, {session['ticker']}) 주문 시작")
                    order_result = self.place_order_session_upper(session)

                    # 주문 불가 종목으로 재주문할 경우 501
                    if order_result == 501:
                        print(f"에러코드 501: 세션 {session['id']} ({session['name']}, {session['ticker']}) 삭제 후 새 종목 세션 추가")
                        db.delete_session_one_row(session.get('id'))
                        processed_sessions.append(session["id"])  # 처리 완료로 표시

                        # 새 종목 추가
                        add_info = self.add_new_trading_session()
                        self.logger.info(f"새 종목 추가: {add_info}")
                        
                        # 새로 추가된 세션만 가져오기
                        new_sessions = db.load_trading_session_upper()
                        if new_sessions:
                            # 마지막 세션(가장 최근 추가된 세션) 확인
                            new_session = new_sessions[-1]
                            if new_session["id"] not in processed_sessions:
                                print(f"새 세션 {new_session['id']} ({new_session['name']}, {new_session['ticker']}) 큐에 추가")
                                sessions_que.append(new_session)
                                # 아직 미처리 세션이므로 processed_sessions 에 추가하지 않음 (주문 후에 추가)
                        # 다음 세션으로 진행
                        continue

                    if order_result:
                        order_lists.append(order_result)
                        processed_sessions.append(session["id"])
                        continue
                
            # 모든 세션 처리 완료 후 결과 반환
            return order_lists
        except Exception as e:
            print("Error in trading session: ", e)


    # def check_trading_session(self):
    #     """
    #     거래 전, 트레이딩 세션 테이블에 진행 중인 거래세션이 있는지 확인하고,
    #     세션 수에 따라 거래를 진행하거나 새로운 세션을 생성합니다.
    #     """
    #     with DatabaseManager() as db:
            
    #         # 현재 거래 세션의 수를 확인
    #         sessions = db.load_trading_session_upper()
    #         slot_count = SLOT_UPPER - len(sessions)
    #         print({'session': len(sessions), 'slot': slot_count})
            
    #         return {'session': len(sessions), 'slot': slot_count}


    def add_new_trading_session(self):
        with DatabaseManager() as db:
            # 현재 거래 세션의 수를 확인
            sessions = db.load_trading_session_upper()
            counted_slot = SLOT_UPPER - len(sessions)
            print({'session': len(sessions), 'slot': counted_slot})

            # 추가된 세션
            session_stocks = []

            for slot in range(counted_slot, 0, -1):
                calculated_fund = self.calculate_funds(slot)

                # 기존 세션 ID 조회
                sessions = db.load_trading_session_upper()
                exclude_num = [session.get('id') for session in sessions]

                random_id = self.generate_random_id(exclude=exclude_num)
                today = datetime.now()
                count = 0
                fund = calculated_fund
                spent_fund = 0
                quantity = 0
                avr_price = 0
                high_price = 0

                stock = self.allocate_stock()
                if stock is None:
                    print("selected_upper_stocks is None: 매수가 가능한 종목을 찾지 못했습니다.")
                    return

                result = self.kis_api.get_stock_price(stock['ticker'])
                time.sleep(1)
                if result.get('output').get('trht_yn') != 'N':
                    print(f"{stock['name']} - 매수가 불가능하여 다시 받아옵니다.")
                    continue

                db.save_trading_session_upper(random_id, today, today, stock['ticker'], stock['name'], high_price, fund, spent_fund, quantity, avr_price, count)
                session_stocks.append(stock["name"])
                
            self.logger.info(f"세션에 종목 추가: {session_stocks}")
            return {'session': session_stocks, 'slot': counted_slot}


    def place_order_session_upper(self, session: Dict) -> Union[Optional[List[Dict]], int]:
        """
        세션 정보를 바탕으로 분할 매수 주문을 실행합니다.
        - COUNT_UPPER 회차로 자금을 분할하여 주문
        - 첫 주문 실패 시 세션 삭제

        DB세션 데이터 타입
        - id: (타입: int)
        - start_date: (타입: date)
        - current_date: (타입: date)
        - ticker: (타입: str)
        - high_price: (타입: int)
        - fund: (타입: int)
        - spent_fund: (타입: int)
        - quantity: (타입: int)
        - avr_price: (타입: int)
        - count: (타입: int)
        """     
        # DB 연결
        with DatabaseManager() as db:
            try:
                time.sleep(0.9)  # API 호출 속도 제한

                # 1. 예외 처리
                ## 현재가 조회 (None, None 반환 가능)
                price, trht_yn = self.kis_api.get_current_price(session.get('ticker'))
                ###    -- api/kis_api.py: 실패 시 (None, None) 반환
                if price is None:
                    print(f"현재가 조회 실패로 건너뛰기: {session.get('ticker')}")
                    db.close()  # DB 세션 정리
                    return None

                ## 거래정지 여부 확인: 업데이트 해야함 -> 정지일 경우 삭제하고 다시 종목 추가
                if trht_yn != 'N':
                    print(f"거래 정지 종목으로 건너뛰기: {session.get('ticker')}")
                    db.close()  # DB 세션 정리
                    return None

                ## 과열 종목 여부 확인: 업데이트 해야함 -> 정지일 경우 삭제하고 다시 종목 추가
                stock_info = self.kis_api.get_stock_price(session.get('ticker'))
                if stock_info.get('output', {}).get('short_over_yn') == 'Y':
                    print(f"과열 종목으로 건너뛰기: {session.get('ticker')}")
                    db.close()  # DB 세션 정리
                    return None

                ## 분할 매수 비율 계산
                ratio = 1 / COUNT_UPPER
                fund_per_order = int(float(session.get('fund', 0)) * ratio)

                ## 슬리피지 버퍼 적용
                effective_price = price * (1 + PRICE_BUFFER)

                ## 회차(count)에 따라 주문 수량 산정
                if session.get('count', 0) < COUNT_UPPER - 1:
                    ### 첫 회차: 균등 분할
                    quantity = int(fund_per_order / effective_price)
                else:
                    ### 마지막 회차: 남은 자금 전부 사용
                    remaining_fund = float(session.get('fund', 0)) - float(session.get('spent_fund', 0))
                    quantity = int(remaining_fund / effective_price)

                ## 오버바잉 방지: 계산된 수량이 실제 잔금보다 많으면 조정
                if quantity * price > (float(session.get('fund', 0)) - float(session.get('spent_fund', 0))):
                    quantity = max(
                        int((float(session.get('fund', 0)) - float(session.get('spent_fund', 0))) / effective_price),
                        0
                    )

                ## 수량 0일 경우 예외 처리
                if quantity <= 0:
                    ### 수량이 0 이면 주문 불필요
                    print("⚠ 주문수량 0 이거나 음수입니다. 세션 건너뜀")
                    self.slack_logger.send_log(
                        level="WARNING",
                        message="주문수량 0 이거나 음수입니다.",
                        context={"세션ID": session.get('id'), "종목코드": session.get('ticker')}
                    )

                    return None

                # 2. 실제 매수 주문 실행 + 미체결 관리 (완전 체결될 때까지 반복)
                name = session.get('name')
                ticker = session.get('ticker')
                if not isinstance(ticker, str):
                    print(f"잘못된 티커: {ticker}")

                    return None


                remaining_quantity = quantity     # 남은 수량
                current_price = price             # 매수 가격
                
                ## 주문 시작 시간 기록 (1분 초과 주문 중단용)
                session['order_start_time'] = time.time()

                ## 매수 주문 실행
                order_price = None                
                ## 모든 주문(첫 주문 포함)에서 지정가 사용, 두 틱 위로 주문
                try:
                    if current_price is not None:
                        # 두 틱 위로 주문 가격 조정
                        tick_size = self._get_tick_size(current_price)
                        order_price = current_price + (tick_size * 2)  # 두 틱 위로 설정
                        self.logger.info(f"현재가 {current_price}, 두 틱 상향 주문가 {order_price} 적용")
                except Exception as e:
                    self.logger.warning(f"현재가 조회 실패, 시장가로 진행: {e}")
                
                ### 매수 주문 실행 (지정가)
                order_result = self.buy_order(name, ticker, remaining_quantity, order_price)

                ### 재주문: 첫 주문 시 주문에 실패할 경우 501
                if not order_result or (isinstance(order_result, dict) and order_result.get('rt_cd') != '0') and session['count'] == 0:
                    self.logger.error("매수 주문 실패", order_result)
                    return 501

                # order_result 반환
                return order_result

            except Exception as e:
                print("place_order_session_upper 실행 중 오류 발생:", e)


    def load_and_update_trading_session(self, order_lists):
        db = DatabaseManager()
        try:
            sessions = db.load_trading_session_upper()
            if not sessions:
                print("load_and_update_trading_session - 진행 중인 거래 세션이 없습니다.")
                return

            for session, order_result in zip(sessions, order_lists):
                if order_result:
                    self.update_session(session, order_result)
                    print("update_session이 종료되었습니다.")
                else:
                    print(f"ticker {session.get('ticker')}에 대한 주문 결과가 없습니다.")

            db.close()
        except Exception as e:
            print("Error in update_trading_session: ", e)
            db.close()


    def update_session(self, session, order_result, increment_count=True):
        # count 증가 여부를 제어하는 매개변수 추가

        print("\n[DEBUG] ====== update_session 진입 ======")
        print(f"[DEBUG] 세션ID: {session.get('id')}, 주문 결과(order_result): {order_result}, count 증가: {increment_count}")
        print(f"[DEBUG] 세션: {session}")
        
        MAX_RETRY = 15  # 체결 지연 대응을 위한 재시도 횟수 대폭 증가
        RETRY_DELAY = 2  # 대기 시간도 2초로 증가 (총 최대 30초 이상 대기)
        try:
            with self.session_lock:
                with DatabaseManager() as db:
                    # 최신 세션 정보 다시 조회하여 count 동기화
                    db_session = db.get_session_by_id(session.get('id'))
                    if db_session:
                        # DB에서 최신 count 값을 가져옴
                        current_count = int(db_session.get('count', 0))
                    else:
                        current_count = int(session.get('count', 0))
                    
                    # count 증가 여부에 따라 처리
                    count = current_count + 1 if increment_count else current_count

                    # 주문 결과 유효성 검사
                    if not order_result:
                        error_msg = f"유효하지 않은 주문 결과: {order_result}"
                        print(error_msg)
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="세션 업데이트 실패",
                            context={
                                "세션ID": session.get('id'),
                                "종목코드": session.get('ticker'),
                                "에러": error_msg
                            }
                        )
                        return

                    # 주문 실패 체크
                    if order_result.get('rt_cd') != '0':
                        error_msg = f"주문 실패: {order_result.get('msg1', '알 수 없는 주문 오류')}"
                        print(error_msg)
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="주문 실패",
                            context={
                                "세션ID": session.get('id'),
                                "종목코드": session.get('ticker'),
                                "에러": error_msg
                            }
                        )
                        return

                    total_spent_fund = int(session.get('spent_fund', 0))
                    total_quantity = int(session.get('quantity', 0))
                    current_date = datetime.now()
                    updated = False

                    # 단일 주문 결과 처리
                    odno = order_result.get('output', {}).get('ODNO')
                    print(f"[DEBUG] 주문번호: {odno}")

                    if not odno:
                        print(f"주문 번호 누락: {order_result.get('msg1')}")
                        return

                        # 체결 정보 재시도 루프
                    for retry in range(1, MAX_RETRY+1):
                        try:
                            result = self.kis_api.daily_order_execution_inquiry(odno)
                            if not result or 'output2' not in result or not result['output2']:
                                print(f"주문 체결 정보 없음: {odno} (재시도 {retry}/{MAX_RETRY})")
                                if retry < MAX_RETRY:
                                    time.sleep(RETRY_DELAY)
                                    continue
                                else:
                                    break

                            real_spent_fund = int(result['output2']['tot_ccld_amt'])
                            real_quantity = int(result['output2']['tot_ccld_qty'])
                            
                            if real_quantity <= 0 or real_spent_fund <= 0:
                                print(f"유효하지 않은 체결 수량 또는 금액: 수량={real_quantity}, 금액={real_spent_fund} (재시도 {retry}/{MAX_RETRY})")
                                if retry < MAX_RETRY:
                                    time.sleep(RETRY_DELAY)
                                    continue
                                else:
                                    break

                            total_spent_fund += real_spent_fund
                            total_quantity += real_quantity
                            updated = True
                            break  # 성공 시 루프 탈출
                        except Exception as e:
                                    print(f"주문 체결 조회 중 오류: {e} (재시도 {retry}/{MAX_RETRY})")
                                    if retry < MAX_RETRY:
                                        time.sleep(RETRY_DELAY)
                                        continue
                                    else:
                                        break

                    if not updated:
                        print("유효한 주문 체결 정보가 없습니다. (최대 재시도 후에도 실패)")
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="체결 정보 조회 실패, 세션 미업데이트",
                            context={
                                "세션ID": session.get('id'),
                                "종목코드": session.get('ticker')
                            }
                        )
                        return

                    # 잔고 조회 재시도
                    balance_data = None
                    for retry in range(1, MAX_RETRY+1):
                        try:
                            balance_result = self.kis_api.balance_inquiry()
                            if not balance_result:
                                print(f"잔고 조회 실패: 응답 없음 (재시도 {retry}/{MAX_RETRY})")
                                if retry < MAX_RETRY:
                                    time.sleep(RETRY_DELAY)
                                    continue
                                else:
                                    break
                            balance_data = next(
                                (item for item in balance_result if item.get('pdno') == session.get('ticker')),
                                None
                            )
                            if not balance_data:
                                print(f"종목 잔고 정보 없음: {session.get('ticker')} (재시도 {retry}/{MAX_RETRY})")
                                if retry < MAX_RETRY:
                                    time.sleep(RETRY_DELAY)
                                    continue
                                else:
                                    break
                            break  # 성공 시 루프 탈출
                        except Exception as e:
                            print(f"잔고 조회 중 오류: {e} (재시도 {retry}/{MAX_RETRY})")
                            if retry < MAX_RETRY:
                                time.sleep(RETRY_DELAY)
                                continue
                            else:
                                break
                    # balance_data 조회 성공 후 다음 코드를 추가해야 합니다 (약 519줄)
                    if balance_data:
                        # 잔고 정보에서 실제 값 가져오기
                        actual_quantity = int(balance_data.get('hldg_qty', 0))
                        actual_spent_fund = int(float(balance_data.get('pchs_amt', 0)))
                        actual_avg_price = int(float(balance_data.get('pchs_avg_pric', 0)))
                        
                        # 세션 횟수 업데이트
                        count = int(session.get('count', 0)) + 1
                        
                        # DB 업데이트
                        db.save_trading_session_upper(
                            session.get('id'),
                            session.get('start_date'),
                            current_date,
                            session.get('ticker'),
                            session.get('name'),
                            session.get('high_price', 0),
                            session.get('fund'),
                            actual_spent_fund,
                            actual_quantity,
                            actual_avg_price,
                            count
                        )
                        
                        print(f"[DEBUG] 세션 업데이트 완료: 세션ID={session.get('id')}, 보유수량={actual_quantity}, 사용금액={actual_spent_fund}, 평균가={actual_avg_price}, 거래횟수={count}")
                        
                        # 모니터링 시작
                        if actual_quantity > 0:
                            try:
                                loop = getattr(self, "_monitor_loop", None)
                                if loop and not loop.is_closed():
                                    asyncio.run_coroutine_threadsafe(
                                        self.monitor_for_selling_upper([session]),
                                        loop
                                    )
                                    print(f"[DEBUG] 모니터링 시작: 세션ID={session.get('id')}, 종목코드={session.get('ticker')}")
                                else:
                                    self.logger.warning("모니터링 루프를 찾을 수 없습니다.")
                            except RuntimeError:
                                # 테스트 환경에서는 이벤트 루프가 없을 수 있으므로 무시
                                print(f"[DEBUG] 이벤트 루프가 없어 모니터링 시작 안 함: 세션ID={session.get('id')}, 종목코드={session.get('ticker')}")

                    elif not balance_data:
                        print("잔고 정보 조회 실패 (최대 재시도 후에도 실패), 세션 미업데이트")
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="잔고 정보 조회 실패, 세션 미업데이트",
                            context={
                                "세션ID": session.get('id'),
                                "종목코드": session.get('ticker'),
                                "종목명": session.get('name'),
                                "투자금액": session.get('fund'),
                                "사용금액": total_spent_fund,
                                "평균단가": None,
                                "보유수량": total_quantity,
                                "거래횟수": None
                            }
                        )
    
                        # 모니터링 시작
                        if total_quantity > 0:
                            try:
                                loop = getattr(self, "_monitor_loop", None)
                                if loop and not loop.is_closed():
                                    asyncio.run_coroutine_threadsafe(
                                        self.monitor_for_selling_upper([session]),
                                        loop
                                    )
                                    print(f"[DEBUG] 모니터링 시작: 세션ID={session.get('id')}, 종목코드={session.get('ticker')}")
                                else:
                                    self.logger.warning("모니터링 루프를 찾을 수 없습니다.")
                            except RuntimeError:
                                # 테스트 환경에서는 이벤트 루프가 없을 수 있으므로 무시
                                print(f"[DEBUG] 이벤트 루프가 없어 모니터링 시작 안 함: 세션ID={session.get('id')}, 종목코드={session.get('ticker')}")

        except Exception as e:
            error_msg = f"세션 업데이트 중 심각한 오류: {str(e)}"
            print(error_msg)
            self.slack_logger.send_log(
                level="ERROR",
                message="세션 업데이트 실패",
                context={
                    "세션ID": session.get('id'), 
                    "종목코드": session.get('ticker'), 
                    "에러": error_msg
                }
            )


    def validate_db_data(self, session, balance_data=None):
        """실제 보유 정보와 DB를 비교하고 필요시 DB를 업데이트 합니다.
        
        Args:
            session: 검증할 세션 정보
            balance_data: 잔고 데이터 (None일 경우 API로 조회)
            
        Returns:
            bool: 검증/업데이트 성공 여부
        """
        print(f"\n[DEBUG] ====== validate_db_data 진입: 세션ID {session.get('id')} ======")
        
        import time
        MAX_RETRY = 15  # 체결 지연 대응을 위한 재시도 횟수 대폭 증가
        RETRY_DELAY = 2  # 대기 시간도 2초로 증가 (총 최대 30초 이상 대기)
        price_tolerance = 1  # 평균가격 차이 허용 범위
        
        try:
            with DatabaseManager() as db:
                # 1. 실제 잔고 정보 조회 (balance_data가 제공되지 않은 경우)
                if balance_data is None:
                    for retry in range(1, MAX_RETRY+1):
                        try:
                            balance_result = self.kis_api.balance_inquiry()
                            if not balance_result:
                                print(f"잔고 조회 실패: 응답 없음 (재시도 {retry}/{MAX_RETRY})")
                                if retry < MAX_RETRY:
                                    time.sleep(RETRY_DELAY)
                                    continue
                                else:
                                    break
                                    
                            balance_data = next((item for item in balance_result 
                                            if item.get('pdno') == session.get('ticker')), None)
                            
                            if balance_data:
                                break  # 성공적으로 데이터를 얻으면 루프 탈출
                                
                            print(f"종목 잔고 정보 없음: {session.get('ticker')} (재시도 {retry}/{MAX_RETRY})")
                            if retry < MAX_RETRY:
                                time.sleep(RETRY_DELAY)
                                continue
                            
                        except Exception as e:
                                    print(f"잔고 조회 중 오류: {e} (재시도 {retry}/{MAX_RETRY})")
                                    if retry < MAX_RETRY:
                                        time.sleep(RETRY_DELAY)
                                        continue
                                    else:
                                        break
                
                # 잔고 정보가 없는 경우 처리
                if balance_data is None:
                    print(f"최종 잔고 조회 실패: {session.get('ticker')}")
                    self.slack_logger.send_log(
                        level="ERROR",
                        message="잔고 조회 실패로 DB 검증 불가",
                        context={"세션ID": session.get('id'), "종목코드": session.get('ticker')}
                    )
                    return False

                # 2. 실제 보유 정보 파싱
                actual_quantity = int(balance_data.get('hldg_qty', 0))
                actual_spent_fund = int(float(balance_data.get('pchs_amt', 0)))
                actual_avg_price = int(float(balance_data.get('pchs_avg_pric', 0)))
                
                print(f"[DEBUG] 실제 잔고 - 수량: {actual_quantity}, 비용: {actual_spent_fund}, 평균가: {actual_avg_price}")

                # 3. DB에서 세션 정보 조회
                session_data = db.load_trading_session_upper()
                db_session = next((s for s in session_data if s.get('id') == session.get('id')), None)
                
                if db_session is None:
                    print(f"DB 세션 조회 실패: {session.get('id')}")
                    return False
                    
                print(f"[DEBUG] DB 상태 - 수량: {db_session.get('quantity')}, 비용: {db_session.get('spent_fund')}, 평균가: {db_session.get('avr_price')}, 거래횟수: {db_session.get('count')}")

                # 4. DB와 실제 잔고 비교
                is_mismatch = (
                    db_session.get('quantity', 0) != actual_quantity or
                    db_session.get('spent_fund', 0) != actual_spent_fund or
                    abs(db_session.get('avr_price', 0) - actual_avg_price) > price_tolerance
                )
                
                # 거래 횟수 업데이트 필요 여부 확인 (새 주문이 들어왔는지 판단)
                update_count = False
                if actual_quantity > 0 and actual_quantity != db_session.get('quantity', 0):
                    update_count = True
                    
                # 5. 불일치 시 DB 업데이트
                if is_mismatch or update_count:
                    print(f"[DB 업데이트 필요] 세션 ID: {session.get('id')}, 종목: {session.get('name')}")
                    
                    # 업데이트할 횟수 계산
                    count = db_session.get('count', 0)
                    if update_count:
                        count += 1
                        print(f"[DEBUG] 거래 횟수 증가: {db_session.get('count', 0)} -> {count}")
                    
                    # DB 업데이트 실행
                    try:
                        db.save_trading_session_upper(
                            session.get('id'), 
                            db_session.get('start_date'),  # 원본 시작일 유지
                            datetime.now(),  # 현재 시간으로 업데이트 시간 갱신
                            session.get('ticker'), 
                            session.get('name'), 
                            db_session.get('high_price', 0),  # 원본 고가 유지
                            db_session.get('fund'),  # 원본 투자금액 유지
                            actual_spent_fund,  # 실제 사용 금액 갱신
                            actual_quantity,  # 실제 수량 갱신
                            actual_avg_price,  # 실제 평균가 갱신
                            count  # 업데이트된 거래 횟수
                        )
                        print("[DB 업데이트 성공] 세션 데이터 갱신 완료")
                    except Exception as db_error:
                        print(f"[DB 업데이트 실패] 오류: {db_error}")
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="세션 DB 업데이트 실패",
                            context={
                                "세션ID": session.get('id'),
                                "종목코드": session.get('ticker'),
                                "에러": str(db_error)
                            }
                        )
                        return False

                    # 로그 기록
                    self.slack_logger.send_log(
                        level="INFO",
                        message="DB 데이터 일치화 수행",
                        context={
                            "세션ID": session.get('id'),
                            "종목명": session.get('name'),
                            "DB_quantity": db_session.get('quantity'),
                            "DB_spent_fund": db_session.get('spent_fund'),
                            "Actual_quantity": actual_quantity,
                            "Actual_spent_fund": actual_spent_fund,
                            "거래횟수": count
                        }
                    )

                    # 6. 재검증
                    session_data = db.load_trading_session_upper()
                    updated_session = next((s for s in session_data if s.get('id') == session.get('id')), None)
                    
                    if updated_session and (
                        updated_session.get('quantity') == actual_quantity and
                        updated_session.get('spent_fund') == actual_spent_fund and
                        abs(updated_session.get('avr_price', 0) - actual_avg_price) <= price_tolerance
                    ):
                        print("[DB 재검증 성공] 데이터 일치 확인")
                        return True
                    else:
                        print("[DB 재검증 실패] 데이터 불일치 지속")
                        return False
                else:
                    print("[DB 검증 완료] 데이터 일치 - 업데이트 불필요")
                    return True

        except Exception as e:
            error_msg = f"[심각한 오류] validate_db_data 실행 중 예외 발생: {e}"
            print(error_msg)
            
            # 로그 기록: 데이터 검증 오류
            self.logger.log_error("데이터 검증", e, {
                "세션ID": session.get('id'), 
                "종목코드": session.get('ticker'),
                "함수": "validate_db_data"
            })
            
            self.slack_logger.send_log(
                level="ERROR",
                message="데이터 검증/업데이트 실패",
                context={"세션ID": session.get('id'), "종목코드": session.get('ticker'), "에러": str(e)}
            )
            return False


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
                
    def _get_tick_size(self, price: Optional[int]) -> int:
        """
        한국거래소 호가단위 규칙에 따라 1틱 크기를 반환
        
        Args:
            price: 가격 (None이면 1 반환)
            
        Returns:
            int: 호가 단위
        """
        if price is None or price <= 0:
            return 0
        if price < 1000:
            return 1
        elif price < 5000:
            return 5
        elif price < 10000:
            return 10
        elif price < 50000:
            return 50
        elif price < 100000:
            return 100
        elif price < 500000:
            return 500
        else:
            return 1000


    def calculate_funds(self, slot):
        try:
            data = self.kis_api.purchase_availability_inquiry()
            balance = float(data.get('output', {}).get('nrcvb_buy_amt', 0))
            print('calculate_funds - 가용 가능 현금: ', balance)

            rest_fund = 0
            with DatabaseManager() as db:
                sessions = db.load_trading_session_upper()
            
            for session in sessions:
                fund = session.get('fund', 0)
                spent_fund = session.get('spent_fund', 0)
                rest_fund += int(fund) - int(spent_fund)
                print("rest_fund:--", rest_fund)

            available = max(balance - rest_fund, 0)
            if slot <= 0:
                print("calculate_funds - 슬롯이 0 이하입니다.")
                return 0

            if slot == 1:
                allocated_funds = available
                print("slot==1 실행")
            elif slot == 2:
                allocated_funds = available / 2
                print("slot==2 실행")
            else:
                allocated_funds = 0
                print("slot>2 실행")
            
            if allocated_funds <= 0:
                print("calculate_funds - 할당 가능한 자금이 없습니다.")
                return 0

            print("calculate_funds - allocated_funds: ", allocated_funds)
            return int(allocated_funds)

        except Exception as e:
            print(f"Error allocating funds: {e}")
            self.slack_logger.send_log(
                level="ERROR",
                message="자금 할당 실패",
                context={"에러": str(e)}
            )
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
                db.delete_selected_stock_by_no(selected_stock['no'])  # no로 삭제 
                db.close()
                return selected_stock
            else:
                db.close()
                return None
            
        except Exception as e:
            print(f"Error allocating funds: {e}")
            return None


######################################################################################
################################    거래량   ##########################################
######################################################################################

    def get_volume_check(self, ticker):
        volumes = self.kis_api.get_stock_volume(ticker)

        # 거래량 비교
        diff_1_2, diff_2_3 = self.kis_api.compare_volumes(volumes)
        
        if diff_1_2 > -80:
            return True
        else:
            return False
        
        
        
    def check_listing_date(self, ticker):
        result = self.kis_api.get_basic_stock_info(ticker)
        
        scts_date = result.get('output').get('scts_mket_lstg_dt')
        kosdaq_date = result.get('output').get('kosdaq_mket_lstg_dt')
        
        # 유효한 날짜 선택 (빈 문자열이 아닌 것)
        listing_date = scts_date if scts_date and scts_date.strip() else kosdaq_date
        
        if listing_date and listing_date.strip():  # 유효한 날짜 문자열인지 확인
            listing_date_dt = datetime.strptime(listing_date, '%Y%m%d')
            threshold_date = datetime.now() - timedelta(days=300)
            return listing_date_dt < threshold_date
        return False
    
######################################################################################
################################    매수/매도   ##########################################
######################################################################################
    
    def handle_unfilled_order(self, order_result: Dict, ticker: str, quantity: int, order_type: str, wait_time: float) -> List[Dict]:
            """
            미체결 주문을 반복적으로 처리합니다. 재주문 과정에서 동일한 오류로 인해
            무한 루프가 발생하지 않도록 최대 재시도 횟수를 두어 제어합니다.

            Args:
                order_result: 최초 주문 결과
                ticker: 종목 코드
                quantity: 최초 주문 수량(로깅용)
                order_type: 'buy' 또는 'sell'
                wait_time: 주문 후 대기 시간(sec)

            Returns:
                List[Dict]: (재)주문 결과 목록
            """
            order_results: List[Dict] = [order_result]

            # 최초 미체결 수량 확인
            unfilled_qty: int = self.order_complete_check(order_result)
            if unfilled_qty <= 0:
                return order_results

            MAX_TOTAL_ATTEMPT = 5      # 전체 재주문 시도 횟수 제한
            MAX_API_RETRY = 3          # cancel / place API 호출 재시도 횟수
            attempt = 0



            while unfilled_qty > 0 and attempt < MAX_TOTAL_ATTEMPT:
                attempt += 1
                
                ###############################
                # 1) 기존 주문 취소
                ###############################
                cancel_success = False
                for _ in range(MAX_API_RETRY):
                    cancel_result = self.kis_api.cancel_order(order_result.get('output', {}).get('ODNO'))
                    self.logger.debug("주문 취소 시도", {"ticker": ticker, "result": cancel_result})
                    if cancel_result.get('rt_cd') == '0':
                        cancel_success = True
                        break
                    # 초당 거래건수 초과 등 속도 제한 처리
                    time.sleep(1)

                if not cancel_success:
                    # 취소 실패가 지속되면 재주문 로직을 중단하고 종료 (무한 루프 방지)
                    self.slack_logger.send_log(
                        level="ERROR",
                        message="주문 취소 반복 실패 – 미체결 주문 처리 중단",
                        context={"종목코드": ticker, "미체결": unfilled_qty, "order_type": order_type}
                    )
                    break

                ###############################
                # 2) 미체결 수량 재주문 (시장가 → 현재가 + 1틱 지정가)
                ###############################
                re_order_success = False
                new_order_result: Dict = {}
                for _ in range(MAX_API_RETRY):
                    if order_type == 'buy':
                        # 현재가 조회 및 유동성 / 거래정지 체크
                        cur_price, trade_halt = self.kis_api.get_current_price(ticker)
                        if cur_price is None or trade_halt:
                            self.slack_logger.send_log(
                                level="WARNING",
                                message="현재가 조회 실패 또는 거래정지 – 재주문 중단",
                                context={"종목코드": ticker}
                            )
                            return order_results
                        # safe_price = cur_price + _get_tick_size(cur_price)
                        new_order_result = self.kis_api.place_order(
                            ticker,
                            unfilled_qty,
                            order_type=order_type,
                            # price=safe_price
                        )
                    else:
                        # 매도의 경우 기존 로직 유지 (시장가/지정가 호출 시 price None 처리)
                        new_order_result = self.kis_api.place_order(ticker, unfilled_qty, order_type=order_type)

                    # 오류 코드 처리
                    if new_order_result.get('rt_cd') == '1':
                        msg_cd = new_order_result.get('msg_cd')
                        msg1 = new_order_result.get('msg1')
                        # 잔고 부족/없음, 속도 제한 등
                        if msg_cd in {'40240000', '40310000'}:
                            self.slack_logger.send_log(
                                level="WARNING",
                                message="재주문 불가 오류 – 처리 중단",
                                context={"종목코드": ticker, "msg_cd": msg_cd, "msg": msg1}
                            )
                            return order_results
                        time.sleep(1)
                        continue

                    # 정상 접수(rt_cd == '0')
                    re_order_success = True
                    break

                order_results.append(new_order_result)

                # 재주문 성공 여부 확인 후 처리
                if not re_order_success:
                    self.slack_logger.send_log(
                        level="ERROR",
                        message="재주문 반복 실패 – 미체결 주문 처리 중단",
                        context={"종목코드": ticker, "미체결": unfilled_qty, "order_type": order_type}
                    )
                    break

                # 다음 루프를 위한 준비
                order_result = new_order_result
                time.sleep(wait_time)
                unfilled_qty = self.order_complete_check(order_result)

            if unfilled_qty > 0:
                # 최대 시도 후에도 미체결 수량이 남아 있으면 경고
                self.slack_logger.send_log(
                    level="WARNING",
                    message="재주문 시도 종료 – 미체결 수량 잔존",
                    context={"종목코드": ticker, "남은수량": unfilled_qty, "order_type": order_type, "시도횟수": attempt}
                )

            return order_results

    def order_complete_check(self, order_result: Dict) -> int:
        """주문 체결 여부를 확인하고 미체결 수량을 반환합니다."""
        try:
            order_num = order_result.get('output', {}).get('ODNO')
            if not order_num:
                return 0

            conclusion_result = self.kis_api.daily_order_execution_inquiry(order_num)
            unfilled_qty = int(conclusion_result.get('output1', [{}])[0].get('rmn_qty', 0))
            return unfilled_qty
        except Exception as e:
            # 실패 시 0으로 간주하여 무한 루프 방지
            self.logger.error(f"주문 체결 확인 실패 - 주문번호:, {order_result.get('output', {}).get('ODNO')}, 에러:, {str(e)}")
            return 0

    def buy_order(self, name: str, ticker: str, quantity: int, price: Optional[int] = None) -> Optional[Dict]:
        """
        주식 매수 주문을 실행하고, 미체결 주문이 있으면 취소 후 재주문.

        Args:
            ticker: 종목 코드
            quantity: 주문 수량
            price: 지정가 (None이면 시장가)

        Returns:
            List[Dict]: 모든 주문 결과 리스트
        """
        # except possibly unbound 방지용 order_result 변수 선언언
        order_result = None
        
        try:
            with self.api_lock:
                while True:
                    # 주문 실행
                    order_result = self.kis_api.place_order(ticker, quantity, order_type='buy', price=price)
                    print("주문 결과:", order_result)
                    # 로그 기록: 주문 응답 결과
                    self.logger.debug(f"KIS API 매수 주문 응답", {
                        "name": name,
                        "ticker": ticker,
                        "rt_cd": order_result.get('rt_cd'),
                        "msg": order_result.get('msg1')
                    })
                    
                    # 응답 결과에 따른 처리
                    ## 초당 거래 건수 초과 시 재시도
                    if order_result['msg1'] == '초당 거래건수를 초과하였습니다.':
                        self.logger.warning("초당 거래건수 초과로 재시도", {"name": name,"ticker": ticker})
                        time.sleep(1)
                        continue
                    
                    ## 주문 실패 시 반환
                    if order_result.get('rt_cd') == '1':
                        self.logger.error(f"매수 주문 실패", order_result)
                        self.logger.warning("매매불가 종목으로 세션 생성 후 재시도", {"name": name,"ticker": ticker})
                        return order_result
                    
                    # 주문번호가 존재하면 매수 루프 종료
                    if order_result.get('output', {}).get('ODNO') is not None:
                        break
            
            time.sleep(BUY_WAIT)
            
            # 주문 완료 체크
            unfilled_qty = self.order_complete_check(order_result)
            self.logger.info(f"매수 주문 미체결 수량 확인", {"name": name,"ticker": ticker, "unfilled": unfilled_qty})

            ## 매수 성공. 매수 로직 종료
            if unfilled_qty == 0:
                self.logger.info(f"매수 주문 전체 체결 완료", {"name": name,"ticker": ticker})
                return order_result

            TRY_COUNT = 0
            ## 미체결 시 주문 수정
            while unfilled_qty:
                    self.logger.info(f"매수 미체결 주문 처리 시작", {"name": name,"ticker": ticker, "unfilled": unfilled_qty})
                    new_price, _ = self.kis_api.get_current_price(ticker)
                    tick_size = self._get_tick_size(new_price)
                    revised_price = new_price + (tick_size * 2)  # 두 틱 위로 설정
                    
                    # 주문 수정 실행
                    revised_result = self.kis_api.revise_order(order_result.get('output', {}).get('ODNO'), unfilled_qty, revised_price)
                    self.logger.info('revised_result: ',revised_result)
                    ### 수정된 금액으로도 미체결 시 재수정을 위한 준비
                    unfilled_qty = self.order_complete_check(order_result)
                    TRY_COUNT += 1
                    time.sleep(BUY_WAIT)

                    if TRY_COUNT > 5:
                        error_msg = f"미체결 주문 반복 실패: {name}({ticker}), {TRY_COUNT}회 재시도"
                        self.logger.error(error_msg, {"unfilled": unfilled_qty})
                        raise Exception(error_msg)  # 명시적으로 예외 발생
                    
            # 최종 주문 결과
            ## 앱 로그
            success_context = {
                "종목이름": name,
                "종목코드": ticker, 
                "주문번호": order_result['output']['ODNO'],
                "메시지": order_result['msg1']
            }
            self.logger.info(f"매수 주문 최종 결과: 성공", success_context)

            ## 슬랙 로그
            self.slack_logger.send_log(
                level="INFO",
                message="매수 주문 결과",
                context={
                    "종목이름": name,
                    "종목코드": ticker,
                    "주문번호": order_result['output']['ODNO'],
                    "상태": "성공",
                    "메시지": order_result['msg1']
                }
            )
            return order_result

        except Exception as e:
            # 예외 발생 로깅
            error_msg = f"buy_order 중 에러 발생 : {e}"
            print(error_msg)
            self.logger.log_error("매수 주문", e, {"name": name,"ticker": ticker, "quantity": quantity})
            self.slack_logger.send_log(
                level="ERROR",
                message="매수 주문 실패",
                context={"종목이름": name, "종목코드": ticker, "에러": str(e)}
            )
            return order_result


    def sell_order(self, session_id: int, ticker: str, price: Optional[int] = None) -> Optional[Dict]:            
            """
            주식 매도 주문을 실행하고, 미체결 주문이 있으면 주문 수정을 통해 체결 시도.

            Args:
                session_id (int): 세션 ID
                ticker (str): 종목 코드
                price (Optional[int], optional): 매도 호가. 미입력 시 시장가

            Returns:
                Optional[Dict]: 주문 결과 딕셔너리 또는 실패 시 None
            """
            order_result = None  # 예외 발생 시에도 참조 가능하도록 사전 초기화
            try:
                # 로그 기록: 매도 주문 시작
                self.logger.log_order("매도", ticker, price, context={
                    "세션ID": session_id,
                    "주문타입": "지정가" if price is not None else "시장가"
                })
                
                # 매도 주문 전 잔고 확인
                balance_result = None
                with self.api_lock:
                    # balance_result: List
                    balance_result = self.kis_api.balance_inquiry()
                
                # 보유 종목 확인
                balance_data = {}
                for stock in balance_result:
                    if stock.get('pdno') == ticker:
                        balance_data = stock

                # 보유 수량 안전하게 추출
                hold_qty = 0
                if balance_data and isinstance(balance_data, dict):
                    try:
                        hold_qty = int(balance_data.get('hldg_qty', 0))
                    except (ValueError, TypeError):
                        hold_qty = 0
                
                # 잔고가 없으면 세션 삭제하고 종료
                if hold_qty <= 0:
                    # 로그 기록: 잔고 없음으로 세션 삭제               
                    self.delete_finished_session(session_id)

                    self.logger.info("잔고 없음 - 세션 삭제 완료", {
                        "세션ID": session_id,
                        "종목이름": balance_data.get('prdt_name'),
                        "종목코드": ticker
                    })
                    
                    self.slack_logger.send_log(
                        level="INFO",
                        message="잔고 없음으로 세션 삭제",
                        context={
                            "세션ID": session_id,
                            "종목코드": ticker,
                        }
                    )
                    return 
                
                # 실제 보유 수량 확인
                quantity = hold_qty
                self.logger.info("매도 수량 확인", {
                    "세션ID": session_id,
                    "종목코드": ticker,
                    "실제보유": quantity
                })

                # 주문 실행
                with self.api_lock:
                    while True:
                        # 주문 실행
                        order_result = self.kis_api.place_order(ticker, quantity, order_type='sell', price=price)
                        
                        # 로그 기록: 주문 응답 결과
                        self.logger.debug(f"KIS API 매도 주문 응답", {
                            "세션ID": session_id,
                            "ticker": ticker,
                            "rt_cd": order_result.get('rt_cd'),
                            "msg": order_result.get('msg1')
                        })
                        
                        # 응답 결과에 따른 처리
                        ## 초당 거래 건수 초과 시 재시도
                        if order_result.get('msg1') == '초당 거래건수를 초과하였습니다.':
                            self.logger.warning("초당 거래건수 초과로 재시도", {"세션ID": session_id, "ticker": ticker})
                            time.sleep(1)
                            continue
                        
                        ## 주문 실패 시 반환
                        if order_result.get('rt_cd') == '1':
                            self.logger.error(f"매도 주문 실패", order_result)
                            self.slack_logger.send_log(
                                level="ERROR",
                                message="매도 주문 실패",
                                context={
                                    "세션ID": session_id,
                                    "종목코드": ticker,
                                    "주문번호": order_result.get('output', {}).get('ODNO'),
                                    "메시지": order_result.get('msg1')
                                }
                            )
                            return None
                        
                        # 주문번호가 존재하면 매도 루프 종료
                        if order_result.get('output', {}).get('ODNO') is not None:
                            break
                
                # 주문 완료 후 대기
                time.sleep(SELL_WAIT)

                # 주문 완료 체크
                unfilled_qty = self.order_complete_check(order_result)
                self.logger.info(f"매도 주문 미체결 수량 확인", {"세션ID": session_id, "ticker": ticker, "unfilled": unfilled_qty})

                ## 매도 성공. 매도 로직 종료
                if unfilled_qty == 0:
                    self.logger.info(f"매도 주문 전체 체결 완료", {"세션ID": session_id, "ticker": ticker})
                    return order_result

                ## 미체결 시 주문 수정
                TRY_COUNT = 0
                while unfilled_qty > 0:
                    self.logger.info(f"매도 미체결 주문 처리 시작", {"세션ID": session_id, "ticker": ticker, "unfilled": unfilled_qty})
                    new_price, _ = self.kis_api.get_current_price(ticker)
                    
                    # 매도는 가격을 낮출수록 체결 확률 증가
                    tick_size = self._get_tick_size(new_price)
                    revised_price = new_price - (tick_size * 2)  # 두 틱 아래로 설정
                    
                    # 주문 수정 실행
                    revised_result = self.kis_api.revise_order(order_result.get('output', {}).get('ODNO'), unfilled_qty, revised_price)
                    self.logger.info('revised_result: ',revised_result)
                    # 수정된 금액으로도 미체결 시 재수정을 위한 준비
                    unfilled_qty = self.order_complete_check(order_result)
                    TRY_COUNT += 1
                    time.sleep(SELL_WAIT)

                    if TRY_COUNT > 5:
                        error_msg = f"미체결 매도 주문 반복 실패: {ticker}, {TRY_COUNT}회 재시도"
                        self.logger.error(error_msg, {"세션ID": session_id, "unfilled": unfilled_qty})
                        raise Exception(error_msg)  # 명시적으로 예외 발생

                # === 매도 완료 후 trade_history 저장 ===
                MAX_RETRY = 5
                RETRY_DELAY = 2  # 초
                remaining_qty = None
                for retry in range(1, MAX_RETRY + 1):
                    balance_result = self.kis_api.balance_inquiry()
                    if not balance_result:
                        self.logger.warning("잔고 조회 결과 없음(재시도 단계)", {
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "retry": retry
                        })
                        balance_result = []
                    
                    balance_data = next((item for item in balance_result if item.get('pdno') == ticker), None)
                    remaining_qty = int(balance_data.get('hldg_qty', 0)) if balance_data else 0

                    if remaining_qty == 0:
                        # 전체 매도 완료 → 세션 삭제
                        self.delete_finished_session(session_id)
                        self.logger.info("매도 주문 완료 및 세션 삭제",{"세션ID": session_id,"종목코드": ticker,"주문수량": quantity,"재시도": retry,"상태": "성공"})
                        break
                    else:
                        # 잔고가 남아있음 – 잔고 반영 지연 가능성 고려
                        if retry < MAX_RETRY:
                            time.sleep(RETRY_DELAY)
                            continue

                        # 최대 재시도 후에도 잔고가 남아있으면 부분 매도로 간주하고 세션 업데이트
                        try:
                            with DatabaseManager() as db:
                                session_info = db.get_session_by_id(session_id)
                                if session_info:
                                    # 값 보정: 음수/이상치 방지
                                    original_qty = max(0, int(session_info.get('quantity', 0)))
                                    remaining_qty = max(0, remaining_qty)
                                    avr_price = max(0, int(float(balance_data.get('pchs_avg_pric', 0))))
                                    new_spent_fund = max(0, remaining_qty * avr_price)

                                    # DB와 실제 잔고 불일치 시 동기화
                                    db.save_trading_session_upper(
                                        session_id,
                                        session_info.get('start_date'),
                                        datetime.now(),
                                        session_info.get('ticker'),
                                        session_info.get('name'),
                                        session_info.get('high_price'),
                                        session_info.get('fund'),
                                        new_spent_fund,
                                        remaining_qty,
                                        avr_price,
                                        session_info.get('count', 0)
                                    )
                                    self.slack_logger.send_log(
                                        level="WARNING",
                                        message="매도 후 세션 DB-실잔고 불일치 → 동기화",
                                        context={
                                            "세션ID": session_id,
                                            "종목코드": ticker,
                                            "DB수량": original_qty,
                                            "실제잔고": remaining_qty,
                                            "DB평균단가": session_info.get('avr_price', 0),
                                            "실제평균단가": avr_price,
                                            "DB투자금액": session_info.get('spent_fund', 0),
                                            "실제투자금액": new_spent_fund
                                        }
                                    )

                        except Exception as e:
                            print(f"[ERROR] update_session 예외: {e}")
                            self.slack_logger.send_log(
                                level="ERROR",
                                message="매도 후 세션 업데이트 실패",
                                context={
                                    "세션ID": session_id,
                                    "종목코드": ticker,
                                    "에러": str(e)
                                }
                            )
                    
                    break

            except Exception as e:
                # 로그 기록: 매도 주문 중 예외 발생
                self.logger.error(f"매도 주문 예외 발생: {e}", {
                    "세션ID": session_id,
                    "종목코드": ticker,
                    "가격": price
                })
                return None  # 생성된 주문 정보 반환 또는 None 반환


    def delete_finished_session(self, session_id):
        with DatabaseManager() as db:
            db.delete_session_one_row(session_id)
        print(session_id, " 세션을 삭제했습니다.")

    
######################################################################################
###############################    모니터링 메서드   ####################################
######################################################################################

    async def monitor_for_selling_upper(self, sessions_info):
        try:
            # KISWebSocket 인스턴스 재사용 (중복 연결 방지)
            if self.kis_websocket is None:
                self.kis_websocket = KISWebSocket(self.sell_order)
            complete = await self.kis_websocket.real_time_monitoring(sessions_info)
            print("콜백함수 실행함.")
            if complete:
                print("모니터링이 정상적으로 종료되었습니다.")
        except Exception as e:
            print(f"모니터링 오류: {e}")
            self.slack_logger.send_log(
                level="ERROR",
                message="웹소켓 모니터링 실패",
                context={"세션정보": sessions_info, "에러": str(e)}
            )
            

    def get_session_info_upper(self):
        """
        매도 모니터링에 필요한 세션 정보 받아오기
        """
        db = DatabaseManager()
        sessions = db.load_trading_session_upper()
        db.close()
        
        sessions_info = []
        for session in sessions:
            #강제 매도 일자
            target_date = self.date_utils.get_target_date(date.fromisoformat(str(session.get('start_date')).split()[0]), DAYS_LATER_UPPER)
            info_list = session.get('id'), session.get('ticker'), session.get('name'), session.get('quantity'), session.get('avr_price'), session.get('start_date'), target_date
            sessions_info.append(info_list)
            
        return sessions_info
        
    
    ###### 삭제 예정 ######
    # async def start_monitoring_for_session(self, session):
    #     ticker = session.get('ticker')
    #     name = session.get('name')
    #     quantity = session.get('quantity')
    #     avr_price = session.get('avr_price')
    #     start_date = session.get('start_date')
    #     target_date = self.date_utils.get_target_date(start_date, DAYS_LATER_UPPER)
        
    #     session_info = [(session.get('id'), ticker, name, quantity, avr_price, start_date, target_date)]
        
    #     if self.kis_websocket is None:
    #         self.kis_websocket = KISWebSocket(self.sell_order)
    #     await self.kis_websocket.real_time_monitoring(session_info)
        

    # def _run_monitoring_for_session(self, session):
    #     # 이미 모니터링 중인 세션인지 확인
    #     ticker = session.get('ticker')
    #     if ticker in getattr(self, '_monitoring_tickers', set()):
    #         print(f"[경고] {ticker} 이미 모니터링 중임")
    #         return
        
    #     # 모니터링 중인 종목 추적
    #     if not hasattr(self, '_monitoring_tickers'):
    #         self._monitoring_tickers = set()
    #     self._monitoring_tickers.add(ticker)
        
    #     # 새로운 이벤트 루프 생성
    #     loop = asyncio.new_event_loop()
    #     asyncio.set_event_loop(loop)
        
    #     try:
    #         print(f"[디버그] {ticker} 모니터링 시작")
    #         loop.run_until_complete(self.start_monitoring_for_session(session))
    #     except Exception as e:
    #         print(f"[오류] {ticker} 모니터링 중 오류 발생: {e}")
    #     finally:
    #         try:
    #             # 실행 중인 태스크를 정리
    #             pending = asyncio.all_tasks(loop)
    #             if pending:
    #                 print(f"[디버그] {ticker} 모니터링 태스크 정리 중: {len(pending)} 태스크")
    #                 for task in pending:
    #                     task.cancel()
                    
    #                 # 취소된 태스크 정리 대기
    #                 loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                
    #             # 웹소켓 연결 정리 시도
    #             if self.kis_websocket:
    #                 try:
    #                     loop.run_until_complete(self.kis_websocket.close())
    #                     print(f"[디버그] {ticker} 웹소켓 안전 종료 완료")
    #                 except Exception as e:
    #                     print(f"[오류] 웹소켓 종료 중 오류: {e}")
    #         except Exception as e:
    #             print(f"[오류] 이벤트 루프 정리 중 오류: {e}")
    #         finally:
    #             # 루프 종료
    #             try:
    #                 loop.close()
    #             except Exception as e:
    #                 print(f"[오류] 이벤트 루프 닫기 실패: {e}")
                
    #             # 모니터링 종목 정보에서 제거
    #             if ticker in self._monitoring_tickers:
    #                 self._monitoring_tickers.remove(ticker)
    #             print(f"[디버그] {ticker} 모니터링 종료")



######################################################################################
###############################    유틸리티티   ####################################
######################################################################################