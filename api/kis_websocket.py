import json
import requests
import logging
from requests.exceptions import RequestException
from config.config import R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET
from config.condition import SELLING_POINT
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
        self.message_queue = asyncio.Queue()
        self.is_connected = False
        self.approval_key = None
        self.connect_headers = {
            "approval_key": self.approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        }
        

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
                print("#########response: ", approval_data)
                
                print("###############실패#############")

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

    async def start_monitoring(self, sessions_info):
        """여러 종목 동시 모니터링 시작"""
        await self.connect_websocket()
        # 종목 구독
        for session_id, ticker, qty, price, date in sessions_info:
            await self.subscribe_ticker(ticker)        

        # 단일 웹소켓 수신 처리 시작
        asyncio.create_task(self._message_receiver())
        
        # 각 종목별 모니터링 태스크 생성
        monitoring_tasks = []
        for session_id, ticker, qty, price, date in sessions_info:
            task = asyncio.create_task(
                self._monitor_ticker(session_id, ticker, qty, price, date)
            )
            monitoring_tasks.append(task)
            print(f"{ticker} 모니터링 태스크 생성")  # 디버깅용
            
        # 모든 모니터링 태스크 완료 대기
        results = await asyncio.gather(*monitoring_tasks, return_exceptions=True)
        return results
        
    async def _message_receiver(self):
        """웹소켓 메시지 수신 전담 코루틴"""
        while self.is_connected:
            try:
                data = await self.websocket.recv()
                # print(f"웹소켓 수신 데이터: {data}")  # 디버깅용
                
                if '"tr_id":"PINGPONG"' in data:
                    await self.websocket.pong(data)
                    continue
                
                # 구독 성공 메시지 체크
                if "SUBSCRIBE SUCCESS" in data:
                    print("Subscription successful")
                    data_dict = json.loads(data)
                    ticker = data_dict['header']['tr_key']
                    print("ticker: ",ticker)

                recvvalue = data.split('^')
                # 구분자로 분리된 데이터인 경우
                if '|' in recvvalue:
                    ticker = data.split('|')[-1]

                
                if ticker in self.subscribed_tickers:
                    print(f"{ticker} 큐에 추가")  # 디버깅용
                    await self.message_queue.put((ticker, recvvalue))
                    print(f"현재 큐 크기: {self.message_queue.qsize()}")  # 디버깅용
                    
            except Exception as e:
                print(f"수신 에러: {e}")
                self.is_connected = False
                break
                

    async def connect_websocket(self):
        """웹소켓 연결 설정"""
        if self.is_connected:
            return
        
        self.approval_key = await self._ensure_approval(is_mock=True)
        url = 'ws://ops.koreainvestment.com:31000/tryitout/H0STASP0'
        
        self.connect_headers = {
            "approval_key": self.approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        }
        
        try:
            self.websocket = await websockets.connect(url, extra_headers=self.connect_headers)
            self.is_connected = True
            print("WebSocket 연결 성공")

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
            print(f"종목 구독 성공: {ticker}")
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
##############################    핑퐁 처리   #######################################
######################################################################################

    # async def _handle_pingpong(self):
    #     """PINGPONG 처리"""
    #     while self.is_connected:
    #         try:
    #             data = await self.websocket.recv()
    #             if '"tr_id":"PINGPONG"' in data:
    #                 await self.websocket.pong(data)
    #         except Exception as e:
    #             print(f"PINGPONG 처리 중 에러: {e}")
    #             self.is_connected = False
    #             break


######################################################################################
##############################    모니터링 메서드   #######################################
######################################################################################

    async def _monitor_ticker(self, session_id, ticker, quantity, avr_price, target_date):
        """개별 종목 모니터링 및 매도 처리"""
        while self.is_connected and ticker in self.subscribed_tickers:

            try:
                # 큐에서 해당 종목의 데이터 가져오기
                data = await self.message_queue.get()
                recv_ticker, recvvalue = data  # 튜플 언패킹
                
                print(f"받은 데이터: ticker={recv_ticker}, value={recvvalue}")  # 디버깅용
                
                if recv_ticker == ticker:
                    print("_monitor_ticker, recv_ticker 실행된거냐")

                    # 매도 조건 확인
                    sell_completed = await self.monitoring_for_selling(
                        recvvalue, session_id, ticker, quantity, avr_price, target_date
                    )
                    
                    if sell_completed:
                        await self.unsubscribe_ticker(ticker)
                        print(f"{ticker} 매도 완료")
                        return True

            except Exception as e:
                print(f"{ticker} 모니터링 에러: {e}")
                return False
                

    # async def _monitor_ticker(self, ticker, quantity, avr_price, target_date):
    #     """개별 종목 모니터링"""
    #     while ticker in self.subscribed_tickers:
    #         try:
    #             data = await self.websocket.recv()
    #             recvvalue = data.split('^')
                
    #             if '"tr_id":"PINGPONG"' in data:
    #                 continue
    #             sell_completed = await self.monitoring_for_selling(recvvalue, ticker, quantity, avr_price, target_date)
    #             if sell_completed:
    #                 await self.unsubscribe_ticker(ticker)
    #                 print(f"{ticker} 호가 감시를 종료합니다.")
    #                 break
                    
    #         except Exception as e:
    #             print(f"{ticker} 모니터링 중 에러: {e}")
    #             break

######################################################################################
##############################    레거시 메서드   #######################################
######################################################################################
            
    async def realtime_quote_subscribe(self, ticker, quantity, avr_price, target_date):
        """실시간 호가 구독"""
        approval_key = await self._ensure_approval(is_mock=True)
        
        # WebSocket 연결 설정
        url = 'ws://ops.koreainvestment.com:31000/tryitout/H0STASP0'  # 모의투자 웹소켓 URL
        
        connect_headers = {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8"
        }
        try:
            async with websockets.connect(url, extra_headers=connect_headers) as websocket:
                # 실시간 호가 요청 데이터 구성
                print("WebSocket 연결 성공")
                request_data = {
                    "header": connect_headers,
                    "body": {
                        "input":{
                            "tr_id": "H0STASP0",  # 실시간 호가 TR ID
                            "tr_key": ticker
                    }
                    }
                }

                # 구독 요청
                await websocket.send(json.dumps(request_data))
                
                print("Subscribed to real-time quotes for %s", ticker, quantity, avr_price)
                print("Subscribed to real-",json.dumps(request_data))

                while True:
                    try:
                        data = await websocket.recv()
                        recvvalue = data.split('^')
                        # PINGPONG 처리
                        if '"tr_id":"PINGPONG"' in data:
                            await websocket.pong(data) 
                            continue
                        
                        # 실시간 데이터 처리
                        sell_completed = await self.monitoring_for_selling(recvvalue, ticker, quantity, avr_price, target_date)
                        if sell_completed is True:
                            print("호가 감시를 종료합니다.")
                            return True
                    except websockets.ConnectionClosed:
                        print("WebSocket connection closed")
                        return False
                    except Exception as e:
                        print("Error processing data: %s", e)
                        return False
        except Exception as e:
            print(f"WebSocket 연결 실패: {e}")
            return False


    async def monitoring_for_selling(self, recvvalue, session_id, ticker, quantity, avr_price, target_date): # 실제 거래 시간에 값이 받아와지는지 확인 필요

        print("monitoring_for_selling: ",recvvalue)
        
        # 구독 성공 메시지 체크
        if len(recvvalue) == 1 and "SUBSCRIBE SUCCESS" in recvvalue[0]:
            print("Subscription successful")
            return False
        
        try:
            target_price = int(recvvalue[15])
        except Exception as e:
            print("recvvalue 데이터 없음", e)
            return False
        today = datetime.now().date()
        
        # print("monitoring_for_selling- ",today)
        # print("monitoring_for_selling - ",target_date)
        # print("monitoring_for_selling - target_price",target_price)
        # print("값 비교: ",today > target_date) 
        print("recvvalue[14]: ", target_price)
        
        if recvvalue: #today > target_date or target_price > (avr_price * SELLING_POINT):
            sell_completed = self.callback(session_id, ticker, quantity, recvvalue[15])
            print("매도 :   ", sell_completed)
            return True
        else:
            print("값이 없습니다.")
            return False