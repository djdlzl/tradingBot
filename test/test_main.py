from test_websockets import KISWebSocket
import asyncio
import threading

class MainProcess:
    def __init__(self):
        self.stop_event = threading.Event()
        self.db_lock = threading.Lock()
        self.threads = {}


    def start_all(self):
        """모든 스레드 시작"""

        # 트레이딩 스레드들
        for i in range(3):
            trading_thread = threading.Thread(
                target=self.trading_cycle,
                name="trading_cycle", args=(i,)
                )
            trading_thread.start()
        self.threads['trading'] = trading_thread  # threads 딕셔너리에 추가

    def trading_cycle(self, i):
        
        kis_ws = KISWebSocket()
        res = asyncio.run(kis_ws.realtime_quote_subscribe(i))
        print(res)    


    def stop_all(self):
        """모든 스레드 종료"""
        self.stop_event.set()
        for name, thread in self.threads.items():
            thread.join()
            print(f"{name} 스레드 종료됨")


if __name__ == "__main__":
    main_process =MainProcess()
    main_process.start_all()
    main_process.stop_all()