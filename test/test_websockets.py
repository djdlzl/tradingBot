import websockets
import json
import time

class KISWebSocket:
    def __init__(self, callback=None):
        self.headers = {"content-type": "utf-8"}
        self.callback = callback

    async def realtime_quote_subscribe(self, i):
        """실시간 호가 구독"""
        
        # WebSocket 연결 설정
        url = 'ws://ops.koreainvestment.com:31000/tryitout/H0STASP0'  # 모의투자 웹소켓 URL
        
        connect_headers = {
            "approval_key": "def6f782-c076-492e-a026-c5bbcb82c120",
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
                            "tr_key": "187420"
                    }
                    }
                }

                # 구독 요청
                await websocket.send(json.dumps(request_data))
                
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
                        sell_completed = await self.monitoring_for_selling(recvvalue)
                        if sell_completed is True:
                            print("호가 감시를 종료합니다.", i)
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


    async def monitoring_for_selling(self, recvvalue): # 실제 거래 시간에 값이 받아와지는지 확인 필요

        print("monitoring_for_selling: ",recvvalue)
        
        # 구독 성공 메시지 체크
        if len(recvvalue) == 1 and "SUBSCRIBE SUCCESS" in recvvalue[0]:
            print("Subscription successful")
            time.sleep(2)
            return False
        else:
            return True