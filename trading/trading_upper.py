"""
api와 db 모듈을 사용해서 상승 눌림목매매 로직 모듈을 개발
"""

import random
import time
import asyncio
import pandas as pd
from datetime import datetime, timedelta, date
from api import kis_api
from database.db_manager_upper import DatabaseManager
from utils.date_utils import DateUtils
from utils.decorators import business_day_only
from utils.slack_logger import SlackLogger
from utils.trading_logger import TradingLogger
from api.kis_api import KISApi
from api.krx_api import KRXApi
from api.kis_websocket import KISWebSocket
from config.condition import DAYS_LATER_UPPER, BUY_PERCENT_UPPER, BUY_WAIT, SELL_WAIT, COUNT_UPPER, SLOT_UPPER, UPPER_DAY_AGO_CHECK, BUY_DAY_AGO_UPPER, PRICE_BUFFER
# from .trading_session import TradingSession
from concurrent.futures import ThreadPoolExecutor
import threading
from typing import List, Dict, Optional
from threading import Lock


class TradingUpper():
    """
    트레이딩과 관련된 로직들. main에는 TradinLogic 클래스만 있어야 함.
    """
    def __init__(self):
        self.kis_api = KISApi()
        self.krx_api = KRXApi()
        self.date_utils = DateUtils()
        self.slack_logger = SlackLogger()
        self.logger = TradingLogger()  # 파일 로깅을 위한 TradingLogger 추가
        self.kis_websocket = None
        self.session_lock = Lock()  # 세션 업데이트용 락
        self.api_lock = Lock()  # API 호출용 락

######################################################################################
#########################    상승 종목 받아오기 / 저장   ###################################
######################################################################################

    @business_day_only()
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

    @business_day_only(default=[])
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
        db = DatabaseManager()
        session_info = self.check_trading_session()
        
        # 새로운 세션을 DB에 저장
        # slot이 2라면, i=2,1 순으로 파라미터 전달. i=0일 때 for문 종료.
        for i in range(int(session_info['slot']), 0, -1):
            self.add_new_trading_session(i)
        
        try:
            #SLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACK
            # 세션 시작 로그
            self.slack_logger.send_log(
                level="INFO",
                message="상승 추세매매 트레이딩 세션 시작",
                context={
                    "세션수": session_info['session'],
                    "가용슬롯": session_info['slot']
                }
            )
            
            # 거래 세션을 조회 및 검증
            sessions = db.load_trading_session_upper()
            db.close()
            if not sessions:
                print("start_trading_session - 진행 중인 거래 세션이 없습니다.")
                return
            
            # 주문 결과 리스트로 저장
            order_list = []

            for session in sessions:
                # 2번 거래한 종목은 더이상 매수하지 않고 대기
                if session.get("count") == COUNT_UPPER:
                    print(session.get('name'),"은 2번의 거래를 진행해 넘어갔습니다.")
                    continue
                
                # 세션 정보로 주식 주문
                order_result = self.place_order_session_upper(session)
                if order_result:
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
        sessions = db.load_trading_session_upper()
        slot_count = SLOT_UPPER - len(sessions)
        print({'session': len(sessions), 'slot': slot_count})
        db.close()
        
        return {'session': len(sessions), 'slot': slot_count}


    def add_new_trading_session(self, slot):
        with DatabaseManager() as db:
            fund = self.calculate_funds(slot)

            # 기존 세션 ID 조회
            sessions = db.load_trading_session_upper()
            exclude_num = [session.get('id') for session in sessions]

            random_id = self.generate_random_id(exclude=exclude_num)
            today = datetime.now()
            count = 0
            spent_fund = 0
            quantity = 0
            avr_price = 0
            high_price = 0

            max_attempts = 5  # 최대 시도 횟수
            for _ in range(max_attempts):
                stock = self.allocate_stock()
                if stock is None:
                    print("선택된 종목이 없습니다.")
                    return None

                result = self.kis_api.get_stock_price(stock['ticker'])
                time.sleep(1)
                if result.get('output').get('trht_yn') != 'N':
                    print(f"{stock['name']} - 매수가 불가능하여 다시 받아옵니다.")
                    continue

                db.save_trading_session_upper(random_id, today, today, stock['ticker'], stock['name'], high_price, fund, spent_fund, quantity, avr_price, count)
                return random_id

            print(f"최대 시도 횟수({max_attempts}) 초과: 매수가 가능한 종목을 찾지 못했습니다.")
            return None


    def place_order_session_upper(self, session: Dict) -> Optional[List[Dict]]:
        """
        세션 정보를 바탕으로 분할 매수 주문을 실행합니다.
        - COUNT_UPPER 회차로 자금을 분할하여 주문
        - 첫 주문 실패 시 세션 삭제
        """
        order_results = None
        
        # 1) DB 연결
        db = DatabaseManager()

        try:
            time.sleep(0.9)  # API 호출 속도 제한


            # 2) 현재가 조회 (None, None 반환 가능)
            price, trht_yn = self.kis_api.get_current_price(session.get('ticker'))
            #    -- api/kis_api.py: 실패 시 (None, None) 반환
            if price is None:
                print(f"현재가 조회 실패로 건너뛰기: {session.get('ticker')}")
                db.close()  # DB 세션 정리
                return None

            # 거래정지 여부 확인
            if trht_yn != 'N':
                print(f"거래 정지 종목으로 건너뛰기: {session.get('ticker')}")
                db.close()  # DB 세션 정리
                return None

            # 과열 종목 여부 확인
            stock_info = self.kis_api.get_stock_price(session.get('ticker'))
            if stock_info.get('output', {}).get('short_over_yn') == 'Y':
                print(f"과열 종목으로 건너뛰기: {session.get('ticker')}")
                db.close()  # DB 세션 정리
                return None

            # 3) 분할 매수 비율 계산
            ratio = 1 / COUNT_UPPER
            fund_per_order = int(float(session.get('fund', 0)) * ratio)

            # 4) 슬리피지 버퍼 적용
            effective_price = price * (1 + PRICE_BUFFER)

            # 5) 회차(count)에 따라 주문 수량 산정
            if session.get('count', 0) < COUNT_UPPER - 1:
                # 중간 회차: 균등 분할
                quantity = int(fund_per_order / effective_price)
            else:
                # 마지막 회차: 남은 자금 전부 사용
                remaining_fund = float(session.get('fund', 0)) - session.get('spent_fund', 0)
                quantity = int(remaining_fund / effective_price)

            # 6) 오버바잉 방지: 계산된 수량이 실제 잔금보다 많으면 조정
            if quantity * price > (float(session.get('fund', 0)) - session.get('spent_fund', 0)):
                quantity = max(
                    int((float(session.get('fund', 0)) - session.get('spent_fund',0 )) / effective_price),
                    0
                )

            quantity = int(quantity)
            if quantity <= 0:
                # 수량이 0 이면 주문 불필요
                print("⚠ 주문수량 0, 세션 건너뜀")
                self.slack_logger.send_log(
                    level="WARNING",
                    message="주문 수량 0",
                    context={"세션ID": session.get('id'), "종목코드": session.get('ticker')}
                )
                db.close()
                return None

            # 7) 실제 매수 주문 실행 (api 호출)
            if session.get('count', 0) < COUNT_UPPER:
                ticker = session.get('ticker')
                if not isinstance(ticker, str):
                    print(f"잘못된 티커: {ticker}")
                    db.close()
                    return None
                order_results = self.buy_order(ticker, quantity)
                # 주문 실패(‘rt_cd’가 '1') 시 처리
                if (not order_results) or (isinstance(order_results, list)
                                        and any(r.get('rt_cd') == '1' for r in order_results)):
                    if session.get('count') == 0:
                        # 첫 주문 실패: 세션 자체 삭제
                        print("첫 주문 실패 시 세션 삭제", session)
                        db.delete_session_one_row(session.get('id'))
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="첫 주문 실패로 세션 삭제",
                            context={
                                "세션ID": session.get('id'),
                                "종목 이름": session.get('name'),
                                "종목 코드": session.get('ticker')
                            }
                        )
                    db.close()
                    return order_results

            # 8) 정상 흐름 시 DB 세션은 업데이트 로직(=load_and_update_trading_session)에서 처리
            db.close()
            return order_results

        except Exception as e:
            # 예외 발생 시 로깅 및 세션 정리
            print(f"place_order_session_upper 에러: {e}")
            self.slack_logger.send_log(
                level="ERROR",
                message="매수 주문 실행 실패",
                context={
                    "세션ID": session.get('id'),
                    "종목코드": session.get('ticker'),
                    "에러": str(e)
                }
            )
            db.close()
            return None


    def load_and_update_trading_session(self, order_list):
        db = DatabaseManager()
        try:
            sessions = db.load_trading_session_upper()
            if not sessions:
                print("load_and_update_trading_session - 진행 중인 거래 세션이 없습니다.")
                return

            for session, order_results in zip(sessions, order_list):
                if order_results:
                    self.update_session(session, order_results)
                else:
                    print(f"ticker {session.get('ticker')}에 대한 주문 결과가 없습니다.")

            db.close()
        except Exception as e:
            print("Error in update_trading_session: ", e)
            db.close()
            
    def update_session(self, session, order_results):
        print("\n[DEBUG] ====== update_session 진입 ======")
        print(f"[DEBUG] 세션ID: {session.get('id')}, 주문 결과(order_results): {order_results}")

        import time
        MAX_RETRY = 3
        RETRY_DELAY = 1  # 초
        try:
            with self.session_lock:
                with DatabaseManager() as db:
                    # 최신 세션 정보 다시 조회하여 count 동기화
                    db_session = db.get_session_by_id(session.get('id'))
                    if db_session:
                        count = db_session.get('count', 0)
                    else:
                        count = session.get('count', 0)

                    # 주문 결과 유효성 검사
                    if not order_results or not isinstance(order_results, (list, tuple)):
                        error_msg = f"유효하지 않은 주문 결과: {order_results}"
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
                    if any(result.get('rt_cd') != '0' for result in order_results):
                        error_msg = next(
                            (f"주문 실패: {result.get('msg1')}" 
                             for result in order_results 
                             if result.get('rt_cd') != '0'),
                            "알 수 없는 주문 오류"
                        )
                        print(error_msg)
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="주문 실패",
                            context={
                                "세션ID": session.get('id'),
                                "종목코드": session.get('ticker'),
                                "에러": error_msg
                    try:
                        with self.session_lock:
                            with DatabaseManager() as db:
                                # 최신 세션 정보 다시 조회하여 count 동기화
                                db_session = db.get_session_by_id(session.get('id'))
                                if db_session:
                                    count = db_session.get('count', 0)
                                else:
                                    count = session.get('count', 0)

                                # 주문 결과 유효성 검사
                                if not order_results or not isinstance(order_results, (list, tuple)):
                                    error_msg = f"유효하지 않은 주문 결과: {order_results}"
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
                                if any(result.get('rt_cd') != '0' for result in order_results):
                                    error_msg = next(
                                        (f"주문 실패: {result.get('msg1')}" 
                                         for result in order_results 
                                         if result.get('rt_cd') != '0'),
                                        "알 수 없는 주문 오류"
                                    )
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

                                for idx, order_result in enumerate(order_results):
                                    odno = order_result.get('output', {}).get('ODNO')
                                    print(f"[DEBUG] 주문[{idx}] 주문번호: {odno}")

                                    odno = order_result.get('output', {}).get('ODNO')
                                    if not odno:
                                        print(f"주문 번호 누락: {order_result.get('msg1')}")
                                        continue

                                    # 체결 정보 재시도 루프
                                    for retry in range(1, MAX_RETRY+1):
                                        try:
                                            result = self.kis_api.daily_order_execution_inquiry(odno)
                                            if not result or 'output1' not in result or not result['output1']:
                                                print(f"주문 체결 정보 없음: {odno} (재시도 {retry}/{MAX_RETRY})")
                                                if retry < MAX_RETRY:
                                                    time.sleep(RETRY_DELAY)
                                                    continue
                                                else:
                                                    break

                                            real_spent_fund = int(result['output1'][0].get('tot_ccld_amt', 0))
                                            real_quantity = int(result['output1'][0].get('tot_ccld_qty', 0))
                                            
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
                                    return

                                # 매도 주문 후 남은 수량 확인 및 세션 삭제
                                db_session = db.get_session_by_id(session.get('id'))
                                if db_session and db_session.get('quantity', 0) <= 0:
                                    print(f"[INFO] 매도 후 세션 수량 0: 세션 삭제 (세션ID: {session.get('id')})")
                                    self.delete_finished_session(session.get('id'))
                                    self.slack_logger.send_log(
                                        level="INFO",
                                        message="매도 후 세션 삭제",
                                        context={"세션ID": session.get('id'), "종목코드": session.get('ticker')}
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
                                        monitoring_thread = threading.Thread(
                                            target=self._run_monitoring_for_session,
                                            args=(session,)
                                        )
                                        monitoring_thread.start()
                                        print(f"[DEBUG] 모니터링 시작: 세션ID={session.get('id')}, 종목코드={session.get('ticker')}")
                                
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
                                        monitoring_thread = threading.Thread(
                                            target=self._run_monitoring_for_session,
                                            args=(session,)
                                        )
                                        monitoring_thread.start()
        
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


    def sell_order(self, session_id: int, ticker: str, quantity: int, price: Optional[int] = None) -> List[Dict]:
            """
            주식 매도 주문을 실행하고, 미체결 주문이 있으면 취소 후 재주문.

            Args:
                session_id: 세션 ID
                ticker: 종목 코드
                quantity: 주문 수량
                price: 지정가 (None이면 시장가)

            Returns:
                List[Dict]: 모든 주문 결과 리스트
            """
            order_results: List[Dict] = []  # 예외 발생 시에도 참조 가능하도록 사전 초기화
            current_price: Optional[int] = None  # 현재가 기본값 설정
            try:
                # 로그 기록: 매도 주문 시작
                self.logger.log_order("매도", ticker, quantity, price, context={
                    "세션ID": session_id,
                    "주문타입": "지정가" if price is not None else "시장가"
                })
                
                # 매도 주문 전 잔고 확인
                # 매도 주문 전 잔고 확인 (직렬화 + 재시도)
                MAX_BAL_RETRY = 3
                RETRY_BAL_DELAY = 1  # 초
                balance_result = None
                for attempt in range(1, MAX_BAL_RETRY + 1):
                    with self.api_lock:
                        balance_result = self.kis_api.balance_inquiry()
                    if balance_result:
                        break
                    # 재시도 로그
                    self.logger.warning("잔고 조회 실패 - 재시도", {
                        "세션ID": session_id,
                        "종목코드": ticker,
                        "retry": attempt
                    })
                    if attempt < MAX_BAL_RETRY:
                        time.sleep(RETRY_BAL_DELAY)
                
                if not balance_result:
                    error_msg = f"잔고 조회 실패: {ticker}"
                    print(error_msg)
                    self.logger.error("매도 전 잔고 조회 실패", {
                        "세션ID": session_id,
                        "종목코드": ticker
                    })
                    self.slack_logger.send_log(
                        level="ERROR",
                        message="매도 전 잔고 조회 실패",
                        context={
                            "세션ID": session_id,
                            "종목코드": ticker
                        }
                    )
                    return []

                balance_data = next((item for item in balance_result if item.get('pdno') == ticker), None)
                self.logger.debug("매도 전 잔고 최종 확인", {
                    "종목코드": ticker, 
                    "세션ID": session_id,
                    "잔고정보": balance_data is not None
                })
                
                # 잔고가 없으면 세션 삭제하고 종료
                if not balance_data or int(balance_data.get('hldg_qty', 0)) == 0:
                    info_msg = f"잔고 없음 - 세션 삭제: {ticker}"
                    print(info_msg)
                    
                    # 로그 기록: 잔고 없음으로 세션 삭제
                    self.logger.info("잔고 없음 - 세션 삭제 시작", {
                        "세션ID": session_id,
                        "종목코드": ticker
                    })
                    
                    self.delete_finished_session(session_id)
                    
                    self.logger.info("잔고 없음 - 세션 삭제 완료", {
                        "세션ID": session_id,
                        "종목코드": ticker
                    })
                    
                    self.slack_logger.send_log(
                        level="INFO",
                        message="잔고 없음으로 세션 삭제",
                        context={
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "요청수량": quantity
                        }
                    )
                    return []
                
                # 실제 보유 수량 확인 및 방어적 동기화
                actual_quantity = int(balance_data.get('hldg_qty', 0))
                self.logger.info("매도 수량 확인", {
                    "세션ID": session_id,
                    "종목코드": ticker,
                    "요청수량": quantity,
                    "실제보유": actual_quantity
                })

                # DB 세션 동기화: 실제 잔고와 DB 세션 수량이 다르면 DB를 동기화
                with self.session_lock:
                    with DatabaseManager() as db:
                        db_session = db.get_session_by_id(session_id)
                        if db_session and db_session.get('quantity', 0) != actual_quantity:
                            db.save_trading_session_upper(
                                session_id,
                                db_session.get('start_date'),
                                datetime.now(),
                                ticker,
                                db_session.get('name'),
                                db_session.get('high_price', 0),
                                db_session.get('fund'),
                                db_session.get('spent_fund', 0),
                                actual_quantity,
                                db_session.get('avr_price', 0),
                                db_session.get('count', 0)
                            )
                            self.logger.info("DB 세션 수량 동기화", {
                                "세션ID": session_id,
                                "종목코드": ticker,
                                "DB수량": db_session.get('quantity', 0),
                                "실제수량": actual_quantity
                            })

                # 매도 주문 전 수량 0 이하 방어
                if actual_quantity <= 0 or quantity is None or quantity <= 0:
                    self.delete_finished_session(session_id)
                    self.slack_logger.send_log(
                        level="INFO",
                        message="매도 주문수량 0: 세션 삭제",
                        context={"세션ID": session_id, "종목코드": ticker, "요청수량": quantity, "실제보유": actual_quantity}
                    )
                    return []

                if actual_quantity < quantity:
                    warning_msg = f"보유수량 부족 - 요청: {quantity}, 보유: {actual_quantity}"
                    print(warning_msg)
                    # 수량 조정 로깅
                    self.logger.warning("매도 수량 자동 조정", {
                        "세션ID": session_id,
                        "종목코드": ticker,
                        "원래수량": quantity,
                        "실제보유": actual_quantity,
                        "조정후": actual_quantity
                    })
                    quantity = actual_quantity  # 실제 보유 수량으로 조정
                    self.slack_logger.send_log(
                        level="WARNING",
                        message="매도 수량 조정",
                        context={
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "요청수량": quantity,
                            "실제보유": actual_quantity,
                            "조정수량": quantity
                        }
                    )

                # 지정가 주문의 체결 가능성 검증
                if price is not None:
                    self.logger.debug("지정가 주문 가격 검증 시작", {
                        "세션ID": session_id,
                        "종목코드": ticker,
                        "원본가격": price,
                        "타입": type(price).__name__
                    })
                    
                    # price를 정수로 변환 (문자열일 수 있음)
                    try:
                        price = int(price)
                        self.logger.debug("가격 변환 성공", {"가격": price, "종목코드": ticker})
                    except (ValueError, TypeError) as e:
                        error_msg = f"매도 가격 변환 실패: {price}, 에러: {e}"
                        print(error_msg)
                        
                        # 로그 기록: 가격 변환 실패
                        self.logger.log_error("매도 가격 변환", e, {
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "원본가격": str(price),
                            "타입": type(price).__name__
                        })
                        
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="매도 가격 변환 실패",
                            context={
                                "세션ID": session_id,
                                "종목코드": ticker,
                                "원본가격": price,
                                "에러": str(e)
                            }
                        )
                        return []
                    
                    # 현재가 조회
                    self.logger.debug("매도를 위한 현재가 조회 시도", {"종목코드": ticker})
                    current_price_result = self.kis_api.get_current_price(ticker)
                    
                    if current_price_result[0] is None:
                        error_msg = f"현재가 조회 실패: {ticker}"
                        print(error_msg)
                        
                        # 로그 기록: 현재가 조회 실패
                        self.logger.error("매도 현재가 조회 실패", {
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "API응답": str(current_price_result)
                        })
                        
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="현재가 조회 실패",
                            context={
                                "세션ID": session_id,
                                "종목코드": ticker,
                                "API응답": current_price_result
                            }
                        )
                        return []
                    
                    current_price = current_price_result[0]  # 이미 int로 변환됨
                    self.logger.info("매도 지정가 체결 가능성 검증", {
                        "세션ID": session_id,
                        "종목코드": ticker,
                        "지정가": price,
                        "현재가": current_price,
                        "하한": int(current_price * 0.95),
                        "상한": int(current_price * 1.05)
                    })
                    
                    if price < current_price * 0.95 or price > current_price * 1.05:  # 5% 이내 가격 확인
                        # 로그 기록: 지정가 비현실적
                        self.logger.warning("매도 지정가 비현실적 - 매도 취소", {
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "지정가": price,
                            "현재가": current_price,
                            "차이률": round((price / current_price - 1) * 100, 2)
                        })
                        
                        self.slack_logger.send_log(
                            level="WARNING",
                            message="매도 지정가 비현실적",
                            context={
                                "세션ID": session_id,
                                "종목코드": ticker,
                                "지정가": price,
                                "현재가": current_price
                            }
                        )
                        return []
                    
                with DatabaseManager() as db:
                    # 세션 정보 조회
                    session_info = db.get_session_by_id(session_id)
                    time.sleep(2)  # 서버 반영 대기
                    # 잔고·평균단가 재조회(매도 직후 시점)
                    balance_result = self.kis_api.balance_inquiry()
                    if not balance_result:
                        self.logger.warning("잔고 조회 결과 없음(재시도 단계)", {
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "retry": 1
                        })
                        balance_result = []
                    
                    balance_data = next((item for item in balance_result if item.get('pdno') == ticker), None)
                    remaining_assets = 0
                    if balance_result:
                        # 현금+주식 평가금 합계
                        remaining_assets = int(float(balance_result[-1].get('prsm_tlex_smtl', 0))) if isinstance(balance_result, list) else 0
                    buy_avg_price = session_info.get('avr_price', 0) if session_info else 0
                    sell_price_final = price if price else current_price if 'current_price' in locals() else 0
                    profit_amount = (sell_price_final - buy_avg_price) * quantity
                    profit_rate = round((sell_price_final / buy_avg_price - 1) * 100, 2) if buy_avg_price else 0
                    db.save_trade_history(
                        trade_date=datetime.now().date(),
                        trade_time=datetime.now().time(),
                        ticker=ticker,
                        name=session_info.get('name') if session_info else '',
                        buy_avg_price=buy_avg_price,
                        sell_price=sell_price_final,
                        quantity=quantity,
                        profit_amount=profit_amount,
                        profit_rate=profit_rate,
                        remaining_assets=remaining_assets
                    )
                with self.api_lock:
                    while True:
                        order_result = self.kis_api.place_order(ticker, quantity, order_type='sell', price=price)
                        print("sell_order:- ", ticker, quantity, price, order_result)
                        if order_result['msg1'] == '초당 거래건수를 초과하였습니다.':
                            time.sleep(1)
                            continue
                        order_results.append(order_result)
                        break

                if order_result.get('rt_cd') == '1':
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
                    return order_results

                time.sleep(SELL_WAIT)
                unfilled_qty = self.order_complete_check(order_result)
                if unfilled_qty > 0:
                    order_results.extend(self.handle_unfilled_order(order_result, ticker, quantity, 'sell', SELL_WAIT))

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
                        self.slack_logger.send_log(
                            level="INFO",
                            message="매도 주문 완료 및 세션 삭제",
                            context={
                                "세션ID": session_id,
                                "종목코드": ticker,
                                "주문수량": quantity,
                                "재시도": retry,
                                "상태": "성공"
                            }
                        )
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
                                    original_qty = int(session_info.get('quantity', 0))
                                    sold_qty = original_qty - remaining_qty
                                    avr_price = int(float(balance_data.get('pchs_avg_pric', 0)))
                                    new_spent_fund = remaining_qty * avr_price

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
                                        level="INFO",
                                        message="매도 후 세션 수량 업데이트",
                                        context={
                                            "세션ID": session_id,
                                            "종목코드": ticker,
                                            "원래수량": original_qty,
                                            "매도수량": sold_qty,
                                            "남은수량": remaining_qty,
                                            "업데이트된평균단가": avr_price
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

                return order_results

            except Exception as e:
                print("sell_order 중 에러 발생 : ", e)
                self.slack_logger.send_log(
                    level="ERROR",
                    message="매도 주문 실패",
                    context={
                        "세션ID": session_id,
                        "종목코드": ticker,
                        "에러": str(e)
                    }
                )
                return order_results

    def delete_finished_session(self, session_id):
        db = DatabaseManager()
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
        # 이미 모니터링 중인 세션인지 확인
        ticker = session.get('ticker')
        if ticker in getattr(self, '_monitoring_tickers', set()):
            print(f"[경고] {ticker} 이미 모니터링 중임")
            return
        
        # 모니터링 중인 종목 추적
        if not hasattr(self, '_monitoring_tickers'):
            self._monitoring_tickers = set()
        self._monitoring_tickers.add(ticker)
        
        # 새로운 이벤트 루프 생성
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            print(f"[디버그] {ticker} 모니터링 시작")
            loop.run_until_complete(self.start_monitoring_for_session(session))
        except Exception as e:
            print(f"[오류] {ticker} 모니터링 중 오류 발생: {e}")
        finally:
            try:
                # 실행 중인 태스크를 정리
                pending = asyncio.all_tasks(loop)
                if pending:
                    print(f"[디버그] {ticker} 모니터링 태스크 정리 중: {len(pending)} 태스크")
                    for task in pending:
                        task.cancel()
                    
                    # 취소된 태스크 정리 대기
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                
                # 웹소켓 연결 정리 시도
                if self.kis_websocket:
                    try:
                        loop.run_until_complete(self.kis_websocket.close())
                        print(f"[디버그] {ticker} 웹소켓 안전 종료 완료")
                    except Exception as e:
                        print(f"[오류] 웹소켓 종료 중 오류: {e}")
            except Exception as e:
                print(f"[오류] 이벤트 루프 정리 중 오류: {e}")
            finally:
                # 루프 종료
                try:
                    loop.close()
                except Exception as e:
                    print(f"[오류] 이벤트 루프 닫기 실패: {e}")
                
                # 모니터링 종목 정보에서 제거
                if ticker in self._monitoring_tickers:
                    self._monitoring_tickers.remove(ticker)
                print(f"[디버그] {ticker} 모니터링 종료")



######################################################################################
###############################    유틸리티티   ####################################
######################################################################################

    def add_upper_stocks(self, add_date, stocks):
        """
        상한가 종목 추가
        fetch_and_save_previous_upper_stocks 메서드 못돌렸을 때 사용
        """
        db = DatabaseManager()
        try:
            db.save_upper_stocks(add_date, stocks)
            print(f"Successfully added stocks for date: {add_date}")
        except Exception as e:
            print(f"Error adding stocks: {e}")
        finally:
            db.close()