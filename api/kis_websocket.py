import json
import requests
import logging
from requests.exceptions import RequestException
from config.config import R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET
from config.condition import SELLING_POINT, RISK_MGMT
from utils.slack_logger import SlackLogger
from datetime import datetime, timedelta
from database.db_manager import DatabaseManager
import time
import asyncio
import websockets
import logging


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
        

######################################################################################
##################################    매도 로직   #####################################
######################################################################################

    async def sell_condition(self, recvvalue, session_id, name, ticker, quantity, avr_price, target_date):
        if len(recvvalue) == 1 and "SUBSCRIBE SUCCESS" in recvvalue[0]:
            return False
        
        try:
            target_price = int(recvvalue[15])
        except Exception as e:
            return False
        
        today = datetime.now().date()
        
        # 매도 사유와 조건을 먼저 확인
        sell_reason = None
        
        # 조건1: 보유기간 만료로 매도
        if today > target_date:
            sell_reason = {
                "매도사유": "기간만료",
                "매도목표일": target_date
            }
        
        # 조건2: 주가 상승으로 익절
        elif target_price > (avr_price * SELLING_POINT):
            sell_reason = {
                "매도사유": "주가 상승: 목표가 도달",
                "매도가": target_price,
                "매도조건가": avr_price * SELLING_POINT
            }
        
        # 조건3: 리스크 관리차 매도
        elif target_price < (avr_price * RISK_MGMT):
            sell_reason = {
                "매도사유": "주가 하락: 리스크 관리차 매도",
                "매도가": target_price,
                "매도조건가": avr_price * RISK_MGMT
            }
        
        # 매도 조건이 충족되면 실행
        if sell_reason:
            # 매도 실행
            sell_completed = await self.callback(session_id, ticker, quantity, target_price)
            
            if sell_completed:
                # 매도 실행 로그
                self.slack_logger.send_log(
                    level="WARNING",
                    message="매도 조건 충족",
                    context={
                        "종목코드": ticker,
                        "종목이름":  name,
                        **sell_reason
                    }
                )
                
                # 매도 발생 시 모니터링 중단
                await self.stop_monitoring(ticker)
                return True
                
        return False

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
    #     if target_price > (avr_price * SELLING_POINT):
    #         sell_completed = await self.callback(session_id, ticker, quantity, target_price)

    #         # 매도 실행 로그
    #         self.slack_logger.send_log(
    #             level="WARNING",
    #             message="매도 조건 충족",
    #             context={
    #                 "종목코드": ticker,
    #                 "매도가": target_price,
    #                 "매도조건가": avr_price * SELLING_POINT,
    #                 "매도사유": "주가 상승: 목표가 도달"
    #             }
    #         )

    #         # 매도 발생 시
    #         await self.stop_monitoring(ticker)  # 해당 종목만 모니터링 중단
            
    #     # 조건3: 리스크 관리차 매도
    #     if target_price < (avr_price * RISK_MGMT):
    #         sell_completed = await self.callback(session_id, ticker, quantity, target_price)

    #         # 매도 실행 로그
    #         self.slack_logger.send_log(
    #             level="WARNING",
    #             message="매도 조건 충족",
    #             context={
    #                 "종목코드": ticker,
    #                 "매도가": target_price,
    #                 "매도조건가": avr_price * RISK_MGMT,
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

    async def upper_limit_monitoring(self, sessions_info):
        """상한가 눌림목매매 모니터링 시작"""
        try:
            # 실시간호가 모니터링 웹소켓 연결
            await self.connect_websocket()
            
            
            # 종목 구독
            for session_id, ticker, name, qty, price, start_date, target_date in sessions_info:
                await self.subscribe_ticker(ticker)        

            # 단일 웹소켓 수신 처리 시작
            asyncio.create_task(self._message_receiver())
            
            # 각 종목별 모니터링 태스크 생성
            background_tasks = set()
            for session_id, ticker, name, qty, price, start_date, target_date in sessions_info:
                task = asyncio.create_task(
                    self._monitor_ticker(session_id, ticker, name, qty, price, target_date)
                )
                task.add_done_callback(background_tasks.discard)
                background_tasks.add(task)
                self.active_tasks[ticker] = task
                # print(f"{ticker} 모니터링 태스크 생성")
                

        except Exception as e:
            print(f"모니터링 중 오류 발생: {e}")
            
        # 모든 태스크가 완료될 때까지 대기
        while background_tasks:
            done, _ = await asyncio.wait(
                background_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    await task
                except Exception as e:
                    print(f"태스크에러: {e}")
                    # 에러 발생한 태스크 제거하고 나머지 태스크 실행
                    background_tasks.discard(task)
        
        # 모든 모니터링 태스크 완료 대기
        results = await asyncio.gather(*background_tasks, return_exceptions=True)
        return results


    async def breakout_monitoring(self, stocks_info):
        """호가 돌파매매 모니터링 시작"""
        try:
            # 실시간호가 모니터링 웹소켓 연결
            await self.connect_websocket()
            
            
            # 종목 구독
            for session_id, ticker, qty, price, start_date, target_date in stocks_info:
                await self.subscribe_ticker(ticker)        

            # 단일 웹소켓 수신 처리 시작
            asyncio.create_task(self._message_receiver())

            # 각 종목별 모니터링 태스크 생성
            background_tasks = set()
            for session_id, ticker, qty, price, start_date, target_date in stocks_info:
                task = asyncio.create_task(
                    self._monitor_ticker(session_id, ticker, qty, price, target_date)
                )
                task.add_done_callback(background_tasks.discard)
                background_tasks.add(task)
                self.active_tasks[ticker] = task
                # print(f"{ticker} 모니터링 태스크 생성")


        except Exception as e:
            print(f"모니터링 중 오류 발생: {e}")
            
        # 모든 태스크가 완료될 때까지 대기
        while background_tasks:
            done, _ = await asyncio.wait(
                background_tasks,
                return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                try:
                    await task
                except Exception as e:
                    print(f"태스크에러: {e}")
                    # 에러 발생한 태스크 제거하고 나머지 태스크 실행
                    background_tasks.discard(task)

        # 모든 모니터링 태스크 완료 대기
        results = await asyncio.gather(*background_tasks, return_exceptions=True)
        return results        

    async def stop_monitoring(self, ticker):
        """특정 종목의 모니터링만 중단"""
        if ticker in self.active_tasks:
            task = self.active_tasks[ticker]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self.active_tasks[ticker]
            print(f"{ticker} 모니터링 중단")


    async def _message_receiver(self):
        """웹소켓 메시지 수신 전담 코루틴"""
        while True:
            try:
                # 혹시나 웹소켓 연결이 끊어진다면
                if not self. is_connected:
                    try:
                        await self.connect_websocket()
                        # 재연결 후 종목 재구독
                        for ticker in self.subscribed_tickers:
                            await self.subscribe_ticker(ticker)
                    except Exception as e:
                        print(f"재연결 실패: {e}")
                        await asyncio.sleep(5)
                        continue
                
                # 웹소켓 응답 수신
                data = await self.websocket.recv()
                # print(f"수신된 원본 데이터: {data}")  # 디버깅용
            
                #웹소켓 연결상태 체크
                if '"tr_id":"PINGPONG"' in data:
                    await self.websocket.pong(data)
                    continue


                # 구독 성공 메시지 체크
                if "SUBSCRIBE SUCCESS" in data:
                    # JSON 파싱 시도
                    data_dict = json.loads(data)
                    # print("data_dict:",data_dict)
                    ticker = data_dict['header']['tr_key']
                    continue
                
                # 실시간 호가 데이터일 경우
                recvvalue = data.split('^')
                # print("가공데이터",recvvalue)
                if len(recvvalue) > 1:  # 실제 호가 데이터인 경우
                    ticker = recvvalue[0].split('|')[-1]
                    if ticker in self.subscribed_tickers:
                        await self.ticker_queues[ticker].put((recvvalue))
                            
            except websockets.exceptions.ConnectionClosed:
                print("웹소켓 연결이 끊어졌습니다. 재연결을 시도합니다.")
                self.is_connected = False
                continue
                
            except Exception as e:
                print(f"수신 에러: {e}")
                print(f"에러 발생 데이터: {data}")  # 디버깅용
                self.is_connected = False
                await asyncio.sleep(1)
                continue

    async def connect_websocket(self):
        """웹소켓 연결 설정"""
        try:
            # websocket 속성 존재 여부 먼저 확인
            if hasattr(self, 'websocket'):
                if self.websocket is not None:
                    try:
                        if not self.websocket.closed:
                            print("이미 웹소켓이 연결되어 있습니다")
                            return
                    except Exception as e:
                        print(f"웹소켓 상태 확인 중 에러: {e}")
            else:
                print("websocket 속성이 아직 없습니다")
                
            self.approval_key = await self._ensure_approval(is_mock=True)
            url = 'ws://ops.koreainvestment.com:31000/tryitout/H0STASP0'
            
            self.connect_headers = {
                "approval_key": self.approval_key,
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8"
            }
            
            self.websocket = await websockets.connect(url, extra_headers=self.connect_headers)
            self.is_connected = True
            # print("WebSocket 연결 성공")

        except Exception as e:
            print(f"WebSocket 연결 실패: {e}")
            self.is_connected = False

    async def close(self):
        """웹소켓 연결 종료"""
        if self.websocket:
            await self.websocket.close()
            self.is_connected = False
            self.subscribed_tickers.clear()
            print("WebSocket 연결이 종료되었습니다.")
            
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
            self.subscribed_tickers.remove(ticker)
            print(f"종목 구독 취소 성공: {ticker}")
        except Exception as e:
            print(f"종목 구독 취소 실패: {ticker}, 에러: {e}")



######################################################################################
##############################    모니터링 메서드   #######################################
######################################################################################

    async def _monitor_ticker(self, session_id, ticker, name, quantity, avr_price, target_date):
        """개별 종목 모니터링 및 매도 처리"""
        # 해당 종목의 전용 큐 생성
        if ticker not in self.ticker_queues:
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
        while self.is_connected and ticker in self.subscribed_tickers:
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
        now = datetime.now()
        market_start = now.replace(hour=9, minute=0, second=0)
        market_end = now.replace(hour=15, minute=30, second=0)
        
        # 주말 체크
        if now.weekday() >= 5:  # 5: 토요일, 6: 일요일
            return False
            
        # 장 운영 시간 체크
        return market_start <= now <= market_end
    

######################################################################################
##############################    레거시 메서드   #######################################
######################################################################################

    async def realtime_quote_subscribe(self, session_id, ticker, quantity, avr_price, target_date):
        """실시간 호가 구독"""
        try:
            # WebSocket 연결
            await self.connect_websocket()
            
            if not self.is_connected:
                print("웹소켓 연결 실패")
                return False

            # 실시간 호가 요청 데이터 구성
            request_data = {
                "header": self.connect_headers,
                "body": {
                    "input":{
                        "tr_id": "H0STASP0",  # 실시간 호가 TR ID
                        "tr_key": ticker
                    }
                }
            }

            # 구독 요청
            await self.websocket.send(json.dumps(request_data))
            
            print(f"Subscribed to real-time quotes for {ticker}, {quantity}, {avr_price}")
            print("Subscribe request:", json.dumps(request_data))

            while self.is_connected:
                try:
                    data = await self.websocket.recv()
                    recvvalue = data.split('^')
                    
                    # PINGPONG 처리
                    if '"tr_id":"PINGPONG"' in data:
                        await self.websocket.pong(data) 
                        continue
                    
                    # 실시간 데이터 처리
                    sell_completed = await self.sell_condition(recvvalue, session_id, ticker, quantity, avr_price, target_date)
                    if sell_completed is True:
                        print("호가 감시를 종료합니다.")
                        return True
                        
                except websockets.ConnectionClosed:
                    print("WebSocket connection closed")
                    self.is_connected = False
                    return False
                    
                except Exception as e:
                    print(f"데이터 처리 중 에러 발생: {e}")
                    return False
                    
        except Exception as e:
            print(f"실시간 호가 구독 중 에러 발생: {e}")
            return False
