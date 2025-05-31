"""
api와 db 모듈을 사용해서 상승 눌림목매매 로직 모듈을 개발
"""

import random
import time
import asyncio
import pandas as pd
from datetime import datetime, timedelta, date
from database.db_manager_upper import DatabaseManager
from utils.date_utils import DateUtils
from utils.slack_logger import SlackLogger
from api.kis_api import KISApi
from api.krx_api import KRXApi
from api.kis_websocket import KISWebSocket
from config.condition import DAYS_LATER_UPPER, BUY_PERCENT_UPPER, BUY_WAIT, SELL_WAIT, COUNT_UPPER, SLOT_UPPER, UPPER_DAY_AGO_CHECK
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
        self.kis_websocket = None
        self.session_lock = Lock()  # 세션 업데이트용 락
        self.api_lock = Lock()  # API 호출용 락

######################################################################################
#########################    상승 종목 받아오기 / 저장   ###################################
######################################################################################

    def fetch_and_save_previous_upper_stocks(self):
        upper_stocks = self.kis_api.get_upAndDown_rank()
        if not upper_stocks or not isinstance(upper_stocks.get('output'), list):
            print("상승 종목 데이터가 유효하지 않습니다.")
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
                print(f"상승 종목 데이터 누락: {e}")
                continue

        today = datetime.now().date()
        current_day = self.date_utils.is_business_day(today)
        
        db = DatabaseManager()
        if stocks_info:
            db.save_upper_stocks(current_day.strftime('%Y-%m-%d'), stocks_info)
            print(current_day.strftime('%Y-%m-%d'), stocks_info)
        else:
            print("상승 종목이 없습니다.")
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
        tickers_with_prices = db.get_upper_stocks_days_ago()  # N일 전 상승 종목 가져오기
        print('tickers_with_prices:  ',tickers_with_prices)
        for stock in tickers_with_prices:
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
            result_df = pd.DataFrame(percentage_diff, index=filtered_df.index[:-1], columns=['등락률'])
              # 등락률이 20% 이상인 값이 있으면 False, 없으면 True를 리턴
            result_high_price = not (result_df['등락률'] >= 20).any()
            
            
            ### 조건2: 상승일 고가 - 매수일 현재가 -7.5% 체크 -> 매수하면서 체크
            last_high_price = df['고가'].iloc[-2]
            result_decline = False
            current_price, trht_yn = self.kis_api.get_current_price(stock.get('ticker'))
            if int(current_price) > (int(last_high_price) * BUY_PERCENT_UPPER):
                result_decline = True
            
                
            ### 조건3: 상승일 거래량 대비 다음날 거래량 20% 이상인지 체크
            result_volume = self.get_volume_check(stock.get('ticker'))
            
            
            ### 조건4: 상장일 이후 1년 체크
            result_lstg = self.check_listing_date(stock.get('ticker'))
            
            
            print(stock.get('name'))
            # print('조건1: result_high_price:',result_high_price)
            # print('조건2: result_decline:',result_decline)
            # print('조건3: result_volume:',result_volume)
            # print('조건4: result_lstg:',result_lstg)
            print('매매 확인을 위해 임시로 모든 조건 통과')

            # if result_high_price and result_decline and result_volume and result_lstg:
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
        today = datetime.now().date() # 현재 날짜와 시간 가져오기
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
###############################    트레이딩 세션   ###################################
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
            세션 정보를 바탕으로 매수 주문 실행.

            Args:
                session: 세션 정보

            Returns:
                Optional[List[Dict]]: 주문 결과 리스트 또는 None
            """
            order_results = None  # 초기값 설정
            try:
                time.sleep(0.9)
                db = DatabaseManager()
                result = self.kis_api.get_current_price(session.get('ticker'))
                # 현재가 조회 실패 시 건너뛰기
                if result[0] is None:
                    print(f"현재가 조회 실패로 건너뛰기: {session.get('ticker')}")

                    
                price = result[0]  # 이미 int로 변환됨
                
                ratio = 1 / COUNT_UPPER
                fund_per_order = int(float(session.get('fund')) * ratio)
                if session.get('count') < COUNT_UPPER - 1:
                    quantity = int(fund_per_order / price)
                else:
                    remaining_fund = float(session.get('fund')) - session.get('spent_fund')
                    quantity = int(remaining_fund / price)

                quantity = int(quantity)
                if quantity <= 0:
                    print("⚠ 주문수량 0, 세션 건너뜀")
                    self.slack_logger.send_log(
                        level="WARNING",
                        message="주문 수량 0",
                        context={"세션ID": session.get('id'), "종목코드": session.get('ticker')}
                    )
                    db.close()
                    return None

                if session.get('count') < COUNT_UPPER:
                    order_results = self.buy_order(session.get('ticker'), quantity)
                    if not order_results or (isinstance(order_results, list) and any(r.get('rt_cd') == '1' for r in order_results)):
                        if session.get('count') == 0:
                            print("첫 주문 실패 시 세션 삭제", session)
                            db.delete_session_one_row(session.get('id'))
                            self.slack_logger.send_log(
                                level="ERROR",
                                message="첫 주문 실패로 세션 삭제",
                                context={"세션ID": session.get('id'), "종목코드": session.get('ticker')}
                            )
                        db.close()
                        return order_results

                    self.update_session(session, order_results)

                db.close()
                return order_results

            except Exception as e:
                print(f"place_order_session_upper 에러: {e}")
                self.slack_logger.send_log(
                    level="ERROR",
                    message="매수 주문 실행 실패",
                    context={"세션ID": session.get('id'), "종목코드": session.get('ticker'), "에러": str(e)}
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
        try:
            with self.session_lock:
                with DatabaseManager() as db:
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
    
                    for order_result in order_results:
                        odno = order_result.get('output', {}).get('ODNO')
                        if not odno:
                            print(f"주문 번호 누락: {order_result.get('msg1')}")
                            continue
    
                        try:
                            # 주문 체결 조회
                            result = self.kis_api.daily_order_execution_inquiry(odno)
                            if not result or 'output1' not in result or not result['output1']:
                                print(f"주문 체결 정보 없음: {odno}")
                                continue
    
                            real_spent_fund = int(result['output1'][0].get('tot_ccld_amt', 0))
                            real_quantity = int(result['output1'][0].get('tot_ccld_qty', 0))
                            
                            if real_quantity <= 0 or real_spent_fund <= 0:
                                print(f"유효하지 않은 체결 수량 또는 금액: 수량={real_quantity}, 금액={real_spent_fund}")
                                continue
    
                            total_spent_fund += real_spent_fund
                            total_quantity += real_quantity
                            updated = True
    
                        except Exception as e:
                            print(f"주문 체결 조회 중 오류: {e}")
                            continue
    
                    if not updated:
                        print("유효한 주문 체결 정보가 없습니다.")
                        return
    
                    # 잔고 조회
                    try:
                        balance_result = self.kis_api.balance_inquiry()
                        if not balance_result:
                            print("잔고 조회 실패: 응답 없음")
                            return
    
                        balance_data = next(
                            (item for item in balance_result if item.get('pdno') == session.get('ticker')),
                            None
                        )
                        
                        if not balance_data:
                            print(f"종목 잔고 정보 없음: {session.get('ticker')}")
                            return
    
                        avr_price = int(float(balance_data.get('pchs_avg_pric', 0)))
                        count = session.get('count', 0) + 1
    
                        # DB 업데이트
                        db.save_trading_session_upper(
                            session.get('id'), 
                            session.get('start_date'), 
                            current_date,
                            session.get('ticker'), 
                            session.get('name'), 
                            session.get('high_price'), 
                            session.get('fund'),
                            total_spent_fund, 
                            total_quantity, 
                            avr_price, 
                            count
                        )
    
                        # 데이터 검증
                        self.validate_db_data(session, balance_data)
                        print('데이터 정합성 검증 완료')
    
                        # 슬랙 알림
                        self.slack_logger.send_log(
                            level="INFO",
                            message="상승 추세매매 세션 업데이트 완료",
                            context={
                                "세션ID": session.get('id'),
                                "종목명": session.get('name'),
                                "투자금액": session.get('fund'),
                                "사용금액": total_spent_fund,
                                "평균단가": avr_price,
                                "보유수량": total_quantity,
                                "거래횟수": count
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
                        print(f"잔고 조회 또는 DB 업데이트 중 오류: {e}")
    
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
        try:
            with DatabaseManager() as db:
                if balance_data is None:
                    balance_result = self.kis_api.balance_inquiry()
                    balance_data = next((item for item in balance_result if item.get('pdno') == session.get('ticker')), None)
                    if balance_data is None:
                        print(f"잔고 조회 실패: {session.get('ticker')}")
                        self.slack_logger.send_log(
                            level="ERROR",
                            message="잔고 조회 실패",
                            context={"세션ID": session.get('id'), "종목코드": session.get('ticker')}
                        )
                        return False

                actual_quantity = int(balance_data.get('hldg_qty', 0))
                actual_spent_fund = int(float(balance_data.get('pchs_amt', 0)))
                actual_avg_price = int(float(balance_data.get('pchs_avg_pric', 0)))

                session_data = db.load_trading_session_upper()
                db_session = next((s for s in session_data if s.get('id') == session.get('id')), None)
                if db_session is None:
                    print(f"DB 세션 조회 실패: {session.get('id')}")
                    return False

                price_tolerance = 1
                if (db_session.get('quantity', 0) != actual_quantity or
                    db_session.get('spent_fund', 0) != actual_spent_fund or
                    abs(db_session.get('avr_price', 0) - actual_avg_price) > price_tolerance):
                    print(f"DB 데이터 불일치 감지: {session.get('ticker')}")
                    print(f"DB - quantity: {db_session.get('quantity')}, spent_fund: {db_session.get('spent_fund')}, avr_price: {db_session.get('avr_price')}")
                    print(f"Actual - quantity: {actual_quantity}, spent_fund: {actual_spent_fund}, avr_price: {actual_avg_price}")

                    db.save_trading_session_upper(
                        session.get('id'), session.get('start_date'), datetime.now(),
                        session.get('ticker'), session.get('name'), session.get('high_price'), session.get('fund'),
                        actual_spent_fund, actual_quantity, actual_avg_price, session.get('count')
                    )

                    self.slack_logger.send_log(
                        level="WARNING",
                        message="DB 데이터 불일치 수정",
                        context={
                            "세션ID": session.get('id'),
                            "종목명": session.get('name'),
                            "DB_quantity": db_session.get('quantity'),
                            "DB_spent_fund": db_session.get('spent_fund'),
                            "Actual_quantity": actual_quantity,
                            "Actual_spent_fund": actual_spent_fund
                        }
                    )

                    # 재검증
                    session_data = db.load_trading_session_upper()
                    db_session = next((s for s in session_data if s.get('id') == session.get('id')), None)
                    if db_session and (db_session.get('quantity') == actual_quantity and
                                    db_session.get('spent_fund') == actual_spent_fund and
                                    abs(db_session.get('avr_price', 0) - actual_avg_price) <= price_tolerance):
                        print("재검증 성공: 데이터 일치")
                        return True
                    else:
                        print("재검증 실패: 데이터 불일치 지속")
                        return False

                return True

        except Exception as e:
            print(f"validate_db_data 중 에러 발생: {e}")
            self.slack_logger.send_log(
                level="ERROR",
                message="데이터 검증 실패",
                context={"세션ID": session.get('id'), "종목코드": session.get('ticker'), "에러": str(e)}
            )
            return False

    # def validate_db_data(self, session, balance_data=None):
    #     try:
    #         db = DatabaseManager()
            
    #         if balance_data is None:
    #             balance_result = self.kis_api.balance_inquiry()
    #             balance_data = next((item for item in balance_result if item.get('pdno') == session.get('ticker')), None)
    #             if balance_data is None:
    #                 print(f"잔고 조회 실패: {session.get('ticker')}")
    #                 db.close()
    #                 return False

    #         actual_quantity = int(balance_data.get('hldg_qty'))
    #         actual_spent_fund = int(float(balance_data.get('pchs_amt')))
    #         actual_avg_price = int(float(balance_data.get('pchs_avg_pric')))

    #         session_data = db.load_trading_session_upper()
    #         db_session = next((s for s in session_data if s.get('id') == session.get('id')), None)
    #         if db_session is None:
    #             print(f"DB 세션 조회 실패: {session.get('id')}")
    #             db.close()
    #             return False

    #         # db_session이 딕셔너리 타입인지 확인
    #         if not isinstance(db_session, dict):
    #             print(f"DB 세션이 올바른 형식이 아닙니다: {type(db_session)}")
    #             db.close()
    #             return False

    #         # avr_price 비교 시 허용 오차 추가
    #         price_tolerance = 1  # 1원 이내 차이는 무시
    #         if (db_session.get('quantity', 0) != actual_quantity or 
    #             db_session['spent_fund'] != actual_spent_fund or
    #             abs(db_session['avr_price'] - actual_avg_price) > price_tolerance):
    #             print(f"DB 데이터 불일치 감지: {session.get('ticker')}")
    #             print(f"DB - quantity: {db_session['quantity']}, spent_fund: {db_session['spent_fund']}, avr_price: {db_session['avr_price']}")
    #             print(f"Actual - quantity: {actual_quantity}, spent_fund: {actual_spent_fund}, avr_price: {actual_avg_price}")

    #             db.save_trading_session_upper(
    #                 session.get('id'), session.get('start_date'), datetime.now(),
    #                 session.get('ticker'), session.get('name'), session.get('high_price'), session.get('fund'),
    #                 actual_spent_fund, actual_quantity, actual_avg_price, session.get('count')
    #             )

    #             self.slack_logger.send_log(
    #                 level="WARNING",
    #                 message="DB 데이터 불일치 수정",
    #                 context={
    #                     "세션ID": session.get('id'),
    #                     "종목명": session.get('name'),
    #                     "DB_quantity": db_session['quantity'],
    #                     "DB_spent_fund": db_session['spent_fund'],
    #                     "Actual_quantity": actual_quantity,
    #                     "Actual_spent_fund": actual_spent_fund
    #                 }
    #             )

    #         db.close()
    #         return True

    #     except Exception as e:
    #         print(f"validate_db_data 중 에러 발생: {e}")
    #         db.close()
    #         return False


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
            미체결 주문을 처리하여 모든 주문 결과를 반환.

            Args:
                order_result: 초기 주문 결과
                ticker: 종목 코드
                quantity: 초기 주문 수량
                order_type: 주문 타입 ('buy' 또는 'sell')
                wait_time: 주문 후 대기 시간

            Returns:
                List[Dict]: 모든 주문 결과 리스트
            """
            order_results = [order_result]
            unfilled_qty = self.order_complete_check(order_result)

            new_order_result = order_result
            while unfilled_qty > 0:
                with self.api_lock:
                    cancel_result = self.kis_api.cancel_order(new_order_result.get('output', {}).get('ODNO'))
                    print(f"cancel_result ({order_type}): - ", cancel_result)
                    time.sleep(1)

                    while True:
                        new_order_result = self.kis_api.place_order(ticker, unfilled_qty, order_type=order_type)
                        print(f"새로운 {order_type} 결과 new_order_result", new_order_result)
                        if new_order_result.get('rt_cd') == '1':
                            time.sleep(1)
                            continue
                        break

                    order_results.append(new_order_result)

                    conclusion_result = self.kis_api.daily_order_execution_inquiry(new_order_result.get('output', {}).get('ODNO'))
                    real_quantity = int(conclusion_result.get('output1', [{}])[0].get('tot_ccld_qty', 0))
                    real_spent_fund = int(conclusion_result.get('output1', [{}])[0].get('tot_ccld_amt', 0))
                    self.slack_logger.send_log(
                        level="INFO",
                        message=f"{order_type.capitalize()} 재주문 체결",
                        context={
                            "종목코드": ticker,
                            "주문번호": new_order_result.get('output', {}).get('ODNO'),
                            "체결수량": real_quantity,
                            "체결금액": real_spent_fund
                        }
                    )

                time.sleep(wait_time)
                unfilled_qty = self.order_complete_check(new_order_result)

            return order_results

    def buy_order(self, ticker: str, quantity: int) -> List[Dict]:
        """
        주식 매수 주문을 실행하고, 미체결 주문이 있으면 취소 후 재주문.

        Args:
            ticker: 종목 코드
            quantity: 주문 수량

        Returns:
            List[Dict]: 모든 주문 결과 리스트
        """
        order_results = []
        try:
            self.slack_logger.send_log(
                level="INFO",
                message="매수 주문 시작",
                context={"종목코드": ticker, "주문수량": quantity, "주문타입": "매수"}
            )

            with self.api_lock:
                while True:
                    order_result = self.kis_api.place_order(ticker, quantity, order_type='buy')
                    print("place_order_session: 주문 실행", ticker, order_result)
                    if order_result['msg1'] == '초당 거래건수를 초과하였습니다.':
                        time.sleep(1)
                        continue
                    if order_result['msg1'] == '모의투자 주문처리가 안되었습니다(매매불가 종목)':
                        db = DatabaseManager()
                        self.add_new_trading_session(1)
                        sessions = db.load_trading_session_upper()
                        session = sessions[-1]
                        order_result = self.place_order_session_upper(session)
                        db.close()
                    order_results.append(order_result)
                    break

            if order_result.get('rt_cd') == '1':
                self.slack_logger.send_log(
                    level="ERROR",
                    message="매수 주문 결과",
                    context={
                        "종목코드": ticker,
                        "주문번호": order_result.get('output', {}).get('ODNO'),
                        "상태": "실패",
                        "메시지": order_result.get('msg1')
                    }
                )
                print("buy_order - ERROR order_results 값: ", order_results)
                return order_results

            time.sleep(BUY_WAIT)
            if self.order_complete_check(order_result) == 0:
                return order_results

            order_results.extend(self.handle_unfilled_order(order_result, ticker, quantity, 'buy', BUY_WAIT))

            self.slack_logger.send_log(
                level="INFO",
                message="매수 주문 결과",
                context={
                    "종목코드": ticker,
                    "주문번호": order_results[-1].get('output', {}).get('ODNO'),
                    "상태": "성공",
                    "메시지": order_results[-1].get('msg1')
                }
            )

            return order_results

        except Exception as e:
            print("buy_order 중 에러 발생 : ", e)
            self.slack_logger.send_log(
                level="ERROR",
                message="매수 주문 실패",
                context={"종목코드": ticker, "에러": str(e)}
            )
            return order_results


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
            try:
                order_results = []

                # 지정가 주문의 체결 가능성 검증
                if price is not None:
                    # price를 정수로 변환 (문자열일 수 있음)
                    try:
                        price = int(price)
                    except (ValueError, TypeError) as e:
                        print(f"매도 가격 변환 실패: {price}, 에러: {e}")
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
                    
                    current_price_result = self.kis_api.get_current_price(ticker)
                    if current_price_result[0] is None:
                        print(f"현재가 조회 실패: {ticker}")
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
                    
                    if price < current_price * 0.95 or price > current_price * 1.05:  # 5% 이내 가격 확인
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

                # 잔고 확인으로 세션 삭제 여부 판단
                balance_result = self.kis_api.balance_inquiry()
                balance_data = next((item for item in balance_result if item.get('pdno') == ticker), None)
                
                if balance_data and int(balance_data.get('hldg_qty', 0)) == 0:
                    self.delete_finished_session(session_id)
                    self.slack_logger.send_log(
                        level="INFO",
                        message="매도 주문 완료 및 세션 삭제",
                        context={
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "주문수량": quantity,
                            "상태": "성공"
                        }
                    )
                else:
                    # 매도 후 잔고가 남아있는 경우 세션 수량 업데이트
                    remaining_qty = int(balance_data.get('hldg_qty', 0)) if balance_data else 0
                    if remaining_qty > 0:
                        try:
                            # 현재 세션 정보 조회
                            with DatabaseManager() as db:
                                session_info = db.get_session_by_id(session_id)
                                if session_info:
                                    # 매도된 수량 계산
                                    original_qty = int(session_info.get('quantity', 0))
                                    sold_qty = original_qty - remaining_qty
                                    
                                    # 평균단가와 투자금액 재계산
                                    avr_price = int(float(balance_data.get('pchs_avg_pric', 0)))
                                    new_spent_fund = remaining_qty * avr_price
                                    
                                    # 세션 업데이트
                                    db.save_trading_session_upper(
                                        session_id,
                                        session_info.get('start_date'),
                                        datetime.now(),
                                        ticker,
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
                            print(f"매도 후 세션 업데이트 실패: {e}")
                            self.slack_logger.send_log(
                                level="ERROR",
                                message="매도 후 세션 업데이트 실패",
                                context={
                                    "세션ID": session_id,
                                    "종목코드": ticker,
                                    "에러": str(e)
                                }
                            )
                    
                    self.slack_logger.send_log(
                        level="WARNING",
                        message="매도 후 잔고가 남아 세션 유지",
                        context={
                            "세션ID": session_id,
                            "종목코드": ticker,
                            "잔고수량": remaining_qty
                        }
                    )
                
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

    def order_complete_check(self, order_result: Dict) -> int:
            """
            주문 체결 여부를 확인하고 미체결 수량 반환.

            Args:
                order_result: 주문 결과

            Returns:
                int: 미체결 수량
            """
            print("order_complete_check 실행")
            try:
                conclusion_result = self.kis_api.daily_order_execution_inquiry(order_result.get('output', {}).get('ODNO'))
                print("일별체결 결과: ", conclusion_result)
                unfilled_qty = int(conclusion_result.get('output1', [{}])[0].get('rmn_qty', 0))
                print("주문확인 order_complete_check: - ", unfilled_qty)
                return unfilled_qty
            except (KeyError, IndexError, TypeError) as e:
                print(f"order_complete_check 에러: {e}")
                self.slack_logger.send_log(
                    level="ERROR",
                    message="주문 체결 확인 실패",
                    context={"주문번호": order_result.get('output', {}).get('ODNO'), "에러": str(e)}
                )
                return 0  # 에러 시 미체결 수량 0으로 가정


    def delete_finished_session(self, session_id):
        db = DatabaseManager()
        db.delete_session_one_row(session_id)
        print(session_id, " 세션을 삭제했습니다.")

    
######################################################################################
###############################    모니터링 메서드   ####################################
######################################################################################

    async def monitor_for_selling_upper(self, sessions_info):
        try:
            kis_websocket = KISWebSocket(self.sell_order)
            complete = await kis_websocket.real_time_monitoring(sessions_info)
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
        # 새로운 이벤트 루프 생성
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            loop.run_until_complete(self.start_monitoring_for_session(session))
        finally:
            loop.close()


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