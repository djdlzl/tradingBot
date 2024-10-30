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
        self.headers = {"content-type": "utf-8"}
        self.db_manager = DatabaseManager()
        self.real_approval = None
        self.mock_approval = None
        self.real_approval_expires_at = None
        self.mock_approval_expires_at = None
        self.hashkey = None
        self.upper_limit_stocks = {}
        self.callback = callback

######################################################################################
#########################    인증 관련 메서드   #######################################
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
                    approval_key = approval_data["approval_key"]

                    expires_at = datetime.utcnow() + timedelta(seconds=86400)                    

                    # Save the new approval_key to the database
                    self.db_manager.save_approval(approval_type, approval_key, expires_at)
                    
                    logging.info("Successfully obtained and cached %s approval_key on attempt %d", approval_type, attempt + 1)
                    return approval_key, expires_at
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


    async def _set_headers(self, is_mock=False, tr_id=None):
        """
        API 요청에 필요한 헤더를 설정합니다.

        Args:
            is_mock (bool): 모의 거래 여부
            tr_id (str, optional): 거래 ID
        """
        approval_key = self._ensure_approval(is_mock)
        self.mock_approval = approval_key
        self.headers["appkey"] = M_APP_KEY if is_mock else R_APP_KEY
        self.headers["appsecret"] = M_APP_SECRET if is_mock else R_APP_SECRET
        if tr_id:
            self.headers["tr_id"] = tr_id
        self.headers["tr_type"] = "1"
        self.headers["custtype"] = "P"


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


    async def monitoring_for_selling(self, recvvalue, ticker, quantity, avr_price, target_date): # 실제 거래 시간에 값이 받아와지는지 확인 필요

        print("monitoring_for_selling: ",recvvalue)
        
        # 구독 성공 메시지 체크
        if len(recvvalue) == 1 and "SUBSCRIBE SUCCESS" in recvvalue[0]:
            print("Subscription successful")
            return False
        
        try:
            target_price = int(recvvalue[14])
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
            sell_completed = self.callback(ticker, quantity, recvvalue[14])
            print("매도 :   ", sell_completed)
            return True
        else:
            print("값이 없습니다.")
            return False