import json
import requests
import logging
import time
import asyncio
import websockets
from requests.exceptions import RequestException
from websockets.exceptions import ConnectionClosed
from config.config import R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET
from config.condition import SELLING_POINT_UPPER, RISK_MGMT_UPPER, KRX_TRADING_START, KRX_TRADING_END
from utils.slack_logger import SlackLogger
from datetime import datetime, timedelta, time
from database.db_manager import DatabaseManager
from api.kis_api import KISApi


class KISWebSocket:
    def __init__(self, callback=None):
        self.db_manager = DatabaseManager()
        self.real_approval = None
        self.mock_approval = None
        self.real_approval_expires_at = None
        self.mock_approval_expires_at = None
        self.hashkey = None
        self.upper_limit_stocks = {}
        self.callback = callback
        self.websocket = None
        self.subscribed_tickers = set()
        self.ticker_queues = {}
        self.message_queue = asyncio.Queue()
        self.is_connected = False
        self.approval_key = None
        self.connect_headers = {
            "approval_key": self.approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        }
        self.active_tasks = {}
        self.slack_logger = SlackLogger()
        # 수신 전용 태스크 (웹소켓당 하나만 유지)
        self.receiver_task = None
        self.locks = {}  # 종목별 락 관리
        self.LOCK_TIMEOUT = 10
        self._loop = None  # 이벤트 루프 저장용
        # recv 동시 호출 방지를 위한 락
        self.recv_lock = asyncio.Lock()
        # 잔고 확인을 위한 API 인스턴스
        self.kis_api = KISApi()
        # API 호출 동시성 제어 락
        self.api_lock = asyncio.Lock()

######################################################################################
##################################    매도 로직   #####################################
######################################################################################

    async def sell_condition(self, recvvalue, session_id, ticker, name, quantity, avr_price, target_date):
        if len(recvvalue) == 1 and "SUBSCRIBE SUCCESS" in recvvalue[0]:
            return False
            
        ### 매도 전 전처리 ###
        # 현재 호가(매도 1호가)를 2틱 낮춰 안전가 계산
        try:
            raw_price = int(recvvalue[15])
        except Exception as e:
            print("sell_condition - recvvalue[15]: ", e)
            return False

        # 한국거래소 호가단위에 맞춰 2틱 아래 가격 계산
        def _get_tick(p: int) -> int:
            """가격대별 호가단위를 반환 (코스닥·코스피 동일 기준)."""
            if p < 1000:
                return 1
            elif p < 5000:
                return 5
            elif p < 10000:
                return 10
            elif p < 50000:
                return 50
            elif p < 100000:
                return 100
            else:
                return 1000

        tick = _get_tick(raw_price)
        target_price = max(raw_price - tick * 2, tick)  # 2틱 아래, 음수가 되지 않도록 보정

        # 종목 락 설정
        if ticker not in self.locks:
            self.locks[ticker] = asyncio.Lock()

        # 현재 날짜와 시간 가져오기
        now = datetime.now()
        today = now.date()
        current_time = now.time()
        
        # 매도 시간 범위 설정 (한국거래소 기준)
        trading_start_time = KRX_TRADING_START
        trading_end_time = KRX_TRADING_END
        
        # 매도 가능 시간(9시-20시)이 아니면 바로 종료
        # 불필요한 매도 시도 반복을 방지하기 위해 15분 간격으로만 로깅
        if current_time < trading_start_time or current_time > trading_end_time:
            # 10분 단위일 때만 로그 기록 (예: 8:00, 8:10, 8:20...)
            if current_time.minute % 10 == 0 and current_time.second < 10:
                print(f"[{now.strftime('%H:%M:%S')}] 거래 시간 외 ({trading_start_time}-{trading_end_time}) - 매도 모니터링만 진행")
            return False
            
        # 매도 시간 설정 (15시 10분)
        sell_time = time(15, 10)
        
        # 매도 사유와 조건을 먼저 확인
        sell_reason = None
        
        # target_date가 boolean인지 확인하고 처리
        if isinstance(target_date, bool):
            # 타겟데이트가 boolean이면, 우선 매도 조건2, 3으로만 진행하고 조건1은 무시
            print(f"[경고] {ticker} 타겟데이트가 boolean({target_date})입니다. 기간만료 체크를 건너뜁니다.")
            # 로그 남기기
            self.slack_logger.send_log(
                level="WARNING",
                message="타겟데이트 형식 오류",
                context={
                    "종목코드": ticker,
                    "타겟데이트타입": str(type(target_date)),
                    "값": str(target_date)
                }
            )
        else:
            # 조건1: 보유기간 만료로 매도
            try:
                if today > target_date and current_time >= sell_time:
                    sell_reason = {
                        "매도사유": "기간만료",
                        "매도목표일": target_date.strftime('%Y-%m-%d') if hasattr(target_date, 'strftime') else str(target_date)
                    }
            except TypeError as e:
                print(f"[오류] 날짜 비교 실패: {e}, 타겟데이트: {type(target_date)}, 값: {target_date}")
        
        # 조건2: 주가 상승으로 익절
        if target_price > (avr_price * SELLING_POINT_UPPER):
            sell_reason = {
                "매도사유": "주가 상승: 목표가 도달",
                "매도가": target_price,
                "매도조건가": avr_price * SELLING_POINT_UPPER
            }
        
        # 조건3: 리스크 관리차 매도
        elif target_price < (avr_price * RISK_MGMT_UPPER):
            sell_reason = {
                "매도사유": "주가 하락: 리스크 관리차 매도",
                "매도가": target_price,
                "매도조건가": avr_price * RISK_MGMT_UPPER
            }
        
        try:
            # 타임아웃과 함께 락 획득 시도
            async with asyncio.timeout(self.LOCK_TIMEOUT):
                async with self.locks[ticker]:
                    # 매도 조건 충족 시에만 잔고 확인 및 매도 실행
                    if sell_reason:
                        try:
                            # 매도 실행 전 잔고 확인 (최대 3회 재시도)
                            max_retry = 3
                            balance_result = None
                            for attempt in range(1, max_retry + 1):
                                try:
                                    # balance inquiry must not block event loop; run in thread
                                    async with self.api_lock:
                                        balance_result = await asyncio.to_thread(self.kis_api.balance_inquiry)
                                    if balance_result:
                                        break  # 성공적으로 조회됨
                                except Exception as e:
                                    # 예외 발생 시 로깅 후 재시도
                                    self.slack_logger.send_log(
                                        level="ERROR",
                                        message="잔고 조회 예외 발생",
                                        context={
                                            "세션ID": session_id,
                                            "종목코드": ticker,
                                            "attempt": attempt,
                                            "에러": str(e)
                                        }
                                    )
                                # 조회 실패 → 다음 재시도 전 잠시 대기
                                if attempt < max_retry:
                                    await asyncio.sleep(1)
                            
                            if not balance_result:
                                print(f"잔고 조회 실패(재시도 후): {ticker}")
                                self.slack_logger.send_log(
                                    level="ERROR",
                                    message="매도 전 잔고 조회 실패(재시도 후)",
                                    context={
                                        "세션ID": session_id,
                                        "종목코드": ticker
                                    }
                                )
                                # 잔고 조회 실패 시 모니터링 중단
                                await self.stop_monitoring(ticker)
                                return True
                            
                            balance_data = next((item for item in balance_result if item.get('pdno') == ticker), None)
                            
                            # 잔고가 없으면 모니터링 중단
                            if not balance_data or int(balance_data.get('hldg_qty', 0)) == 0:
                                print(f"잔고 없음 - 모니터링 중단: {ticker}")
                                # 세션 DB에서도 삭제하여 데이터 정합성 유지
                                try:
                                    from database.db_manager_upper import DatabaseManager
                                    with DatabaseManager() as db:
                                        db.delete_session_one_row(session_id)
                                except Exception as db_err:
                                    print(f"[ERROR] 세션 삭제 실패: {db_err}")
                                    self.slack_logger.send_log(
                                        level="ERROR",
                                        message="잔고 없음 세션 삭제 실패",
                                        context={
                                            "세션ID": session_id,
                                            "종목코드": ticker,
                                            "에러": str(db_err)
                                        }
                                    )
                                self.slack_logger.send_log(
                                    level="INFO",
                                    message="잔고 없음으로 모니터링 중단",
                                    context={
                                        "세션ID": session_id,
                                        "종목코드": ticker,
                                        "종목명": name
                                    }
                                )
                                # 구독 해제 및 모니터링 중단
                                await self.unsubscribe_ticker(ticker)
                                await self.stop_monitoring(ticker)
                                return True
                            
                            # 실제 보유 수량 확인
                            actual_quantity = int(balance_data.get('hldg_qty', 0))
                            if actual_quantity < quantity:
                                print(f"보유수량 조정 - 요청: {quantity}, 보유: {actual_quantity}")
                                quantity = actual_quantity  # 실제 보유 수량으로 조정
                                self.slack_logger.send_log(
                                    level="WARNING",
                                    message="매도 수량 조정",
                                    context={
                                        "세션ID": session_id,
                                        "종목코드": ticker,
                                        "원래수량": quantity,
                                        "실제보유": actual_quantity,
                                        "조정수량": quantity
                                    }
                                )
                            
                            # 기존 미체결 매도 주문이 있으면 모두 취소
                            try:
                                today_orders = self.kis_api.daily_order_execution_inquiry("")
                                for od in today_orders.get("output1", []):
                                    if od.get("pdno") == ticker and od.get("sll_buy_dvsn_cd") == "01":  # 매도 주문
                                        if int(od.get("rmn_qty", 0)) > 0:
                                            self.kis_api.cancel_order(od.get("odno"))
                                            await asyncio.sleep(0.5)
                            except Exception as cancel_err:
                                self.slack_logger.send_log(
                                    level="ERROR",
                                    message="기존 주문 취소 실패",
                                    context={
                                        "종목코드": ticker,
                                        "에러": str(cancel_err)
                                    }
                                )

                            # 1. 매도 주문 실행 (2틱 아래 안전가 주문)
                            if self.callback is not None:
                                sell_completed = self.callback(session_id, ticker, quantity, target_price)
                            else:
                                # 콜백이 없을 때 처리 (예: 로그 남기기, 기본값 할당 등)
                                sell_completed = False
                            
                            # 2. 즉시 구독 해제
                            await self.unsubscribe_ticker(ticker)
                            
                            if sell_completed:
                                # 3. 매도 실행 로그
                                self.slack_logger.send_log(
                                    level="WARNING",
                                    message="매도 조건 충족",
                                    context={
                                        "종목코드": ticker,
                                        "종목이름": name,
                                        **sell_reason
                                    }
                                )
                                # 3-1. 매도 후 잔고 재확인
                                try:
                                    # balance inquiry must not block event loop; run in thread
                                    async with self.api_lock:
                                        balance_list = await asyncio.to_thread(self.kis_api.balance_inquiry)
                                    remaining = next((item for item in balance_list if item.get("pdno") == ticker), None)
                                    if remaining:
                                        self.slack_logger.send_log(
                                            level="CRITICAL",
                                            message="매도 후 잔고가 남았습니다",
                                            context={
                                                "종목코드": ticker,
                                                "잔여수량": remaining.get("hldg_qty"),
                                            }
                                        )
                                    else:
                                        self.slack_logger.send_log(
                                            level="INFO",
                                            message="매도 완료 및 잔고 없음 확인",
                                            context={"종목코드": ticker}
                                        )
                                except Exception as bal_err:
                                    self.slack_logger.send_log(
                                        level="ERROR",
                                        message="매도 후 잔고 확인 실패",
                                        context={"종목코드": ticker, "에러": str(bal_err)}
                                    )
                                 
                                 # 4. 모니터링 완전 종료
                                await self.stop_monitoring(ticker)
                                return True
                                
                        except Exception as e:
                            # 매도 실패 시 구독 복구
                            try:
                                await self.subscribe_ticker(ticker)
                                self.slack_logger.send_log(
                                    level="ERROR",
                                    message=f"매도 실패 - 구독 복구됨: {str(e)}",
                                    context={"종목코드": ticker}
                                )
                            except Exception as sub_error:
                                self.slack_logger.send_log(
                                    level="CRITICAL",
                                    message="구독 복구 실패",
                                    context={
                                        "종목코드": ticker,
                                        "에러": str(sub_error)
                                    }
                                )
                            raise e
                            
        except TimeoutError:
            self.slack_logger.send_log(
                level="WARNING",
                message="락 획득 타임아웃",
                context={"종목코드": ticker}
            )
            return False
            
        finally:
            # 락 객체 정리
            if ticker in self.locks:
                if not self.locks[ticker].locked():
                    del self.locks[ticker]
                    
############################  코드 백업  ##############################
    # async def sell_condition(self, recvvalue, session_id, ticker, quantity, avr_price, target_date): # 실제 거래 시간에 값이 받아와지는지 확인 필요

    #     # print("sell_condition: ",recvvalue)
        
    #     # 구독 성공 메시지 체크
    #     if len(recvvalue) == 1 and "SUBSCRIBE SUCCESS" in recvvalue[0]:
    #         # print("sell_condition - Subscription successful")
    #         return False
        
    #     try:
    #         # 매도 목표가 정하기
    #         target_price = int(recvvalue[15])
                       
    #     except Exception as e:
    #         # print("recvvalue 데이터 없음", e)
    #         return False
        
    #     today = datetime.now().date()
             
             
    #     # 조건1: 보유기간 만료로 매도
    #     if today > target_date:
    #         sell_completed = await self.callback(session_id, ticker, quantity, target_price)
            
    #         # 매도 실행 로그
    #         self.slack_logger.send_log(
    #             level="WARNING",
    #             message="매도 조건 충족",
    #             context={
    #                 "종목코드": ticker,
    #                 "매도목표일": target_date,
    #                 "매도사유": "기간만료"
    #             }
    #         )
            
    #         # 매도 발생 시
    #         await self.stop_monitoring(ticker)  # 해당 종목만 모니터링 중단
            
    #     # 조건2: 주가 상승으로 익절
    #     if target_price > (avr_price * SELLING_POINT_UPPER):
    #         sell_completed = await self.callback(session_id, ticker, quantity, target_price)

    #         # 매도 실행 로그
    #         self.slack_logger.send_log(
    #             level="WARNING",
    #             message="매도 조건 충족",
    #             context={
    #                 "종목코드": ticker,
    #                 "매도가": target_price,
    #                 "매도조건가": avr_price * SELLING_POINT_UPPER,
    #                 "매도사유": "주가 상승: 목표가 도달"
    #             }
    #         )

    #         # 매도 발생 시
    #         await self.stop_monitoring(ticker)  # 해당 종목만 모니터링 중단
            
    #     # 조건3: 리스크 관리차 매도
    #     if target_price < (avr_price * RISK_MGMT_UPPER):
    #         sell_completed = await self.callback(session_id, ticker, quantity, target_price)

    #         # 매도 실행 로그
    #         self.slack_logger.send_log(
    #             level="WARNING",
    #             message="매도 조건 충족",
    #             context={
    #                 "종목코드": ticker,
    #                 "매도가": target_price,
    #                 "매도조건가": avr_price * RISK_MGMT_UPPER,
    #                 "매도사유": "주가 하락: 리스크 관리차 매도"
    #             }
    #         )
            
    #         # 매도 발생 시
    #         await self.stop_monitoring(ticker)  # 해당 종목만 모니터링 중단
            
    #         # print("매도 : ", sell_completed)
    #         return True
    #     else:
    #         # print("매도 조건 불일치")
    #         return False


######################################################################################
##############################    인증 관련 메서드   #####################################
######################################################################################

    async def _get_approval(self, app_key, app_secret, approval_type, max_retries=3, retry_delay=5):
        """
        웹소켓 인증키 발급
        """
    
        cached_approval, cached_expires_at = self.db_manager.get_approval(approval_type)
        if cached_approval and cached_expires_at > datetime.utcnow():
            logging.info("Using cached %s approval", approval_type)
            return cached_approval, cached_expires_at

        url = "https://openapi.koreainvestment.com:9443/oauth2/Approval"
        headers = {
            "content-type": "application/json; utf-8"
            }
        body = {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "secretkey": app_secret
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.post(url, headers=headers, json=body, timeout=10)
                response.raise_for_status()
                approval_data = response.json()
                
                if "approval_key" in approval_data:
                    self.approval_key = approval_data["approval_key"]

                    expires_at = datetime.utcnow() + timedelta(seconds=86400)                    

                    # Save the new approval_key to the database
                    self.db_manager.save_approval(approval_type, self.approval_key, expires_at)
                    
                    logging.info("Successfully obtained and cached %s approval_key on attempt %d", approval_type, attempt + 1)
                    return self.approval_key, expires_at
                else:
                    logging.warning("Unexpected response format on attempt %d: %s", attempt + 1, approval_data)
            except RequestException as e:
                logging.error("An error occurred while fetching the %s approval_key on attempt %d: %s", approval_type, attempt + 1, e)
                if attempt < max_retries - 1:
                    logging.info("Retrying in %d seconds...", retry_delay)
                    time.sleep(retry_delay)
                else:
                    logging.error("Max retries reached. Unable to obtain %s approval_key.", approval_type)

    async def _ensure_approval(self, is_mock):
        """
        유효한 웹소켓 인증키가 있는지 확인하고, 필요한 경우 새 인증키를 가져옵니다.

        Args:
            is_mock (bool): 모의 거래 여부

        Returns:
            str: 유효한 액세스 인증키
        """
        # print("##########is_mock: ", is_mock)
        now = datetime.now()
        if is_mock:
            if not self.mock_approval or now >= self.mock_approval_expires_at:
                self.mock_approval, self.mock_approval_expires_at = await self._get_approval(M_APP_KEY, M_APP_SECRET, "mock")
            return self.mock_approval
        else:
            if not self.real_approval or now >= self.real_approval_expires_at:
                self.real_approval, self.real_approval_expires_at = await self._get_approval(R_APP_KEY, R_APP_SECRET, "real")
            return self.real_approval


######################################################################################
##############################    웹소켓 연결   #######################################
######################################################################################

    async def real_time_monitoring(self, sessions_info):
        """실시간 모니터링 시작"""
        try:
            
            # 실시간호가 모니터링 웹소켓 연결
            if not self.is_connected:
                await self.connect_websocket()

            # background_tasks를 클래스 속성으로 변경
            self.background_tasks = set()

            # 종목 구독
            for session_id, ticker, name, qty, price, start_date, target_date in sessions_info:
                await self.add_new_stock_to_monitoring(session_id, ticker, name, qty, price, start_date, target_date)
            
            # 단일 웹소켓 수신 처리 시작 (중복 생성 방지)
            if self.receiver_task is None or self.receiver_task.done():
                self.receiver_task = asyncio.create_task(self._message_receiver())
            
            # 모든 태스크가 완료될 때까지 대기
            while self.background_tasks:
                done, _ = await asyncio.wait(
                    self.background_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    try:
                        await task
                    except Exception as e:
                        print(f"태스크에러: {e}")

            # 모든 모니터링 태스크 완료 대기
            results = await asyncio.gather(*self.background_tasks, return_exceptions=True)
            print('real_time_monitoring 반환값:',results)
            return results

        except Exception as e:
            print(f"모니터링 중 오류 발생: {e}")


    async def stop_monitoring(self, ticker):
        """특정 종목의 모니터링만 중단"""
        try:
            # 1. 구독 해제
            if ticker in self.subscribed_tickers:
                await self.unsubscribe_ticker(ticker)
            
            # 2. 태스크 취소
            if ticker in self.active_tasks:
                task = self.active_tasks[ticker]
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                del self.active_tasks[ticker]
                
            # 3. 큐 정리
            if ticker in self.ticker_queues:
                while not self.ticker_queues[ticker].empty():
                    try:
                        self.ticker_queues[ticker].get_nowait()
                    except asyncio.QueueEmpty:
                        break
                del self.ticker_queues[ticker]
                
            # 4. 락 정리
            if hasattr(self, 'locks') and ticker in self.locks:
                if not self.locks[ticker].locked():
                    del self.locks[ticker]
                    
            self.slack_logger.send_log(
                level="INFO",
                message=f"{ticker} 모니터링 중단 완료",
                context={"종목코드": ticker}
            )
                
        except Exception as e:
            self.slack_logger.send_log(
                level="ERROR",
                message="모니터링 중단 중 오류",
                context={
                    "종목코드": ticker,
                    "에러": str(e)
                }
            )

########### 백업 코드 #############
    # async def stop_monitoring(self, ticker):
    #     """특정 종목의 모니터링만 중단"""
    #     if ticker in self.active_tasks:
    #         task = self.active_tasks[ticker]
    #         task.cancel()
    #         try:
    #             await task
    #         except asyncio.CancelledError:
    #             pass
    #         del self.active_tasks[ticker]
    #         print(f"{ticker} 모니터링 중단")




    async def _message_receiver(self):
        """웹소켓 메시지 수신 전담 코루틴"""
        while True:
            try:
                # 웹소켓 연결 상태 확인
                if not self.is_connected or self.websocket is None:
                    try:
                        print("웹소켓 재연결 시도...")
                        await self.connect_websocket()
                        if not self.is_connected or self.websocket is None:
                            print("웹소켓 재연결 실패, 5초 후 재시도")
                            await asyncio.sleep(5)
                            continue
                        
                        # 재연결 후 종목 재구독
                        if self.subscribed_tickers:
                            to_resub = list(self.subscribed_tickers)
                            self.subscribed_tickers.clear()  # 집합을 비워 실제 재구독 실행
                            print(f"재구독 시도: {to_resub}")
                            for ticker in to_resub:
                                try:
                                    await self.subscribe_ticker(ticker)
                                except Exception as sub_e:
                                    print(f"재구독 실패 {ticker}: {sub_e}")
                    except Exception as e:
                        print(f"재연결 실패: {e}")
                        await asyncio.sleep(5)
                        continue
                
                # 웹소켓이 닫혔는지 추가 확인
                if self.websocket.closed:
                    print("웹소켓이 닫혀있음, 재연결 필요")
                    self.is_connected = False
                    self.websocket = None
                    continue
                
                # 웹소켓 응답 수신 (동시 호출 방지)
                async with self.recv_lock:
                    data = await self.websocket.recv()
                # print(f"수신된 원본 데이터: {data}")  # 디버깅용
            
                #웹소켓 연결상태 체크
                if '"tr_id":"PINGPONG"' in data:
                    try:
                        await self.websocket.send(data)  # 서버 요구에 따라 그대로 반송
                    except Exception as e:
                        print(f"PINGPONG 응답 실패: {e}")
                    continue

                # 구독 성공 메시지 체크
                if "SUBSCRIBE SUCCESS" in data:
                    # JSON 파싱 시도
                    data_dict = json.loads(data)
                    ticker = data_dict['header']['tr_key']
                    continue
                
                # 실시간 호가 데이터일 경우
                recvvalue = data.split('^')

                if len(recvvalue) > 1:  # 실제 호가 데이터인 경우
                    ticker = recvvalue[0].split('|')[-1]
                    if ticker in self.subscribed_tickers:
                        await self.ticker_queues[ticker].put((recvvalue))

            except ConnectionClosed:
                print("웹소켓 연결이 끊어졌습니다. 재연결을 시도합니다.")
                self.is_connected = False
                self.websocket = None
                await asyncio.sleep(1)
                continue

            except AttributeError as e:
                if "'NoneType' object has no attribute 'recv'" in str(e):
                    print("웹소켓이 None 상태입니다. 재연결이 필요합니다.")
                else:
                    print(f"속성 에러: {e}")
                self.is_connected = False
                self.websocket = None
                await asyncio.sleep(2)
                continue

            except Exception as e:
                print(f"수신 에러: {e}")
                print(f"에러 발생 데이터: {data if 'data' in locals() else 'No data'}")
                self.is_connected = False
                self.websocket = None
                await asyncio.sleep(1)
                continue

    async def connect_websocket(self):
        """웹소켓 연결 설정"""
        if self.websocket and not self.websocket.closed:
            print("웹소켓이 이미 연결되어 있어 connect_websocket 메서드 종료")
            return

        try:
            # 기존 웹소켓 정리
            if self.websocket:
                try:
                    await self.websocket.close()
                except:
                    pass
                self.websocket = None
            
            url = 'ws://ops.koreainvestment.com:31000/tryitout/H0STASP0'           
            self.connect_headers = {
                "approval_key": await self._ensure_approval(is_mock=True),
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8"
            }
            self._loop = asyncio.get_running_loop()  # 현재 이벤트 루프 저장
            
            # 타임아웃 설정하여 연결 시도
            self.websocket = await asyncio.wait_for(
                websockets.connect(url, extra_headers=self.connect_headers),
                timeout=10.0
            )
            self.is_connected = True
            print('이벤트 루프 저장 및 웹소켓 연결 성공')

        except asyncio.TimeoutError:
            print("WebSocket 연결 타임아웃")
            self.is_connected = False
            self.websocket = None
        except OSError as e:
            if e.errno == 11001:  # getaddrinfo failed
                print(f"DNS 해석 실패 - 네트워크 연결을 확인하세요: {e}")
            else:
                print(f"네트워크 연결 실패: {e}")
            self.is_connected = False
            self.websocket = None
        except Exception as e:
            print(f"WebSocket 연결 실패: {e}")
            self.is_connected = False
            self.websocket = None

    async def close(self):
        """웹소켓 연결 종료"""
        self.is_connected = False
        
        # 모든 활성 태스크 취소
        for ticker, task in list(self.active_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception) as e:
                    pass
        self.active_tasks.clear()
        
        # 메시지 수신 태스크 취소
        if self.receiver_task and not self.receiver_task.done():
            self.receiver_task.cancel()
            try:
                await self.receiver_task
            except asyncio.CancelledError:
                pass
        self.receiver_task = None
        
        # 웹소켓 연결 종료
        if self.websocket and not self.websocket.closed:
            try:
                await self.websocket.close()
                await asyncio.sleep(0.1)  # 연결 종료 대기
            except Exception as e:
                print(f"웹소켓 종료 중 오류: {e}")
        
        # 리소스 정리
        self.websocket = None
        self.subscribed_tickers.clear()
        self.ticker_queues.clear()
        print("WebSocket 연결이 완전히 종료되었습니다.")
            
######################################################################################
##############################    구독 관리   #######################################
######################################################################################

    async def subscribe_ticker(self, ticker):
        """종목 구독"""
        if ticker in self.subscribed_tickers:
            print(f"이미 구독 중인 종목입니다: {ticker}")
            return
        self.connect_headers['tr_type'] = "1"
        request_data = {
            "header": self.connect_headers,
            "body": {
                "input": {
                    "tr_id": "H0STASP0",  # 실시간 호가 TR ID
                    "tr_key": ticker
                }
            }
        }
        
        try:
            await self.websocket.send(json.dumps(request_data))
            self.subscribed_tickers.add(ticker)
            # print(f"종목 구독 성공: {ticker}")
        except Exception as e:
            print(f"종목 구독 실패: {ticker}, 에러: {e}")

            
    async def unsubscribe_ticker(self, ticker):
        """종목 구독 취소"""
        if ticker not in self.subscribed_tickers:
            print(f"구독하지 않은 종목입니다: {ticker}")
            return
        
        # 웹소켓 연결 상태 확인
        if self.websocket is None or not self.is_connected or self.websocket.closed:
            print(f"웹소켓이 연결되어 있지 않아 구독을 취소할 수 없습니다: {ticker}")
            self.subscribed_tickers.discard(ticker)  # 구독 목록에서 제거
            return
        
        self.connect_headers['tr_type'] = "2"
        request_data = {
            "header": self.connect_headers,
            "body": {
                "input": {
                    "tr_id": "H0STASP0",  # 실시간 호가 TR ID
                    "tr_key": ticker
                }
            }
        }
        
        try:
            await self.websocket.send(json.dumps(request_data))
            self.subscribed_tickers.discard(ticker)  # remove 대신 discard 사용
            print(f"종목 구독 취소 성공: {ticker}")
        except websockets.exceptions.ConnectionClosed:
            print(f"웹소켓 연결이 닫혀 있어 구독 취소에 실패했습니다: {ticker}")
            self.is_connected = False
            self.websocket = None
        except Exception as e:
            print(f"종목 구독 취소 중 오류 발생 ({ticker}): {e}")
            # 오류 발생 시에도 구독 목록에서 제거
            self.subscribed_tickers.discard(ticker)


    async def add_new_stock_to_monitoring(self, session_id, ticker, name, qty, price, start_date, target_date):
        # 기존 모니터링이 있으면 우선 중단해 중복 태스크 방지
        if ticker in self.active_tasks:
            await self.stop_monitoring(ticker)

        # 새 종목 구독
        await self.subscribe_ticker(ticker)
        
        # 이벤트 루프별 큐 정합성 보장: 항상 현재 루프에 바인딩된 새 큐 생성
        # 기존 큐가 다른 루프에 바인딩되어 있을 경우 "다른 event loop" 오류가 발생하므로 새로 생성한다.
        self.ticker_queues[ticker] = asyncio.Queue()
            
        # 새 종목에 대한 모니터링 태스크 생성
        task = asyncio.create_task(
            self._monitor_ticker(session_id, ticker, name, qty, price, target_date)
        )
        task.add_done_callback(self.background_tasks.discard)
        self.background_tasks.add(task)
        self.active_tasks[ticker] = task
        
        print(f"{ticker} 모니터링 태스크 추가됨")

######################################################################################
##############################    모니터링 메서드   #######################################
######################################################################################

    async def _monitor_ticker(self, session_id, ticker, name, quantity, avr_price, target_date):
        """개별 종목 모니터링 및 매도 처리"""
        # 큐가 없거나 다른 루프에 바인딩되어 있으면 새로 생성
        if (
            ticker not in self.ticker_queues or
            getattr(self.ticker_queues[ticker], "_loop", None) is not asyncio.get_running_loop()
        ):
            self.ticker_queues[ticker] = asyncio.Queue()

        #SLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACK
        # 모니터링 시작 로그

        self.slack_logger.send_log(
            level="INFO",
            message="모니터링 시작",
            context={
                "종목코드": ticker,
                "평균단가": avr_price,
                "목표일": target_date,
                "보유수량": quantity
            }
        )
        # 모니터링 프로세스 시작
        while True:
            # 거래시간 체크: 거래시간 외면 모니터링 중단
            if not self._is_market_open():
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 거래시간 외 - {ticker} 모니터링 중단")
                self.slack_logger.send_log(
                    level="INFO",
                    message="거래시간 외 모니터링 중단",
                    context={"종목코드": ticker}
                )
                break
            # 구독이 해제되면 모니터링 종료
            if ticker not in self.subscribed_tickers:
                break
            try:
                try:
                # 타임아웃을 설정하여 데이터 대기
                    recvvalue = await asyncio.wait_for(self.ticker_queues[ticker].get(), timeout=5.0)
                    # 매도 조건 확인
                    sell_completed = await self.sell_condition(
                        recvvalue, session_id, ticker, name, quantity, avr_price, target_date
                    )
                    
                    if sell_completed:
                        await self.unsubscribe_ticker(ticker)
                        print(f"{ticker} 매도 완료")
                        return True
                    
                    # 큐 태스크 종료
                    self.ticker_queues[ticker].task_done()

                except asyncio.TimeoutError:
                    # 타임아웃은 데이터 미수신 상태를 나타내며, 네트워크 재연결 대기 동안 자주 발생할 수 있다.
                    # 웹소켓 연결 여부와 관계없이 계속 루프를 유지하여 재연결을 기다린다.
                    continue
            except asyncio.CancelledError:
                print(f"{ticker} 모니터링 취소됨")
                return False
                
            except Exception as e:
                print(f"{ticker} 모니터링 에러: {e}")
                continue

        return False
    
    def _is_market_open(self):
        """장 운영 시간 체크"""
        from config.condition import KRX_TRADING_START, KRX_TRADING_END
        now = datetime.now()
        market_start = now.replace(hour=KRX_TRADING_START.hour, minute=KRX_TRADING_START.minute, second=0)
        market_end = now.replace(hour=KRX_TRADING_END.hour, minute=KRX_TRADING_END.minute, second=0)
        
        # 주말 체크
        if now.weekday() >= 5:  # 5: 토요일, 6: 일요일
            return False
            
        # 장 운영 시간 체크
        return market_start <= now <= market_end
