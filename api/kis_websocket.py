import json
import requests
import logging
import time
import asyncio
import websockets
from requests.exceptions import RequestException
from websockets.exceptions import ConnectionClosed
from config.config import R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET
from config.condition import (
    SELLING_POINT_UPPER,
    RISK_MGMT_UPPER,
    KRX_TRADING_START,
    KRX_TRADING_END,
)
from utils.slack_logger import SlackLogger
from datetime import datetime, timedelta, time
from database.db_manager_upper import DatabaseManager
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
        # sell_order 콜백 (필수)
        if callback is None:
            def _invalid_callback(*args, **kwargs):
                raise RuntimeError("sell callback not set")
            self.callback = _invalid_callback
        else:
            self.callback = callback
        self.websocket = None
        self.subscribed_tickers = set()
        self.ticker_queues = {}
        # 현재 루프 저장 (생성 시점 루프)
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = asyncio.get_event_loop()
        self.message_queue = asyncio.Queue()
        self.is_connected = False
        self.approval_key = None
        self.connect_headers = {
            "approval_key": self.approval_key,
            "custtype": "P",
            "tr_type": "1",
            "content-type": "utf-8",
        }
        self.active_tasks = {}
        self.slack_logger = SlackLogger()
        self.receiver_task = None
        self.locks = {}
        self.LOCK_TIMEOUT = 10
        self.recv_lock = asyncio.Lock()
        self.kis_api = KISApi()
        self.api_lock = asyncio.Lock()
        # 매수 중인 종목 추적 (key: 종목코드, value: 매수 중 상태)
        self.buying_in_progress = {}
        self.buy_status_lock = asyncio.Lock()


    ######################################################################################
    ##################################    매도 로직   #####################################
    ######################################################################################

    async def sell_condition(
        self, recvvalue, session_id, ticker, name, quantity, avr_price, target_date
    ):
        sell_completed = False
        lock_start_time = None
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
        target_price = max(
            raw_price - tick * 2, tick
        )  # 2틱 아래, 음수가 되지 않도록 보정

        # 종목 락 설정 (동일 종목 동시 매도 로직을 방지하기 위한 락)
        if ticker not in self.locks:
            self.locks[ticker] = asyncio.Lock()

        # 이미 다른 코루틴이 해당 종목에 대해 매도 로직을 수행 중이라면
        # LOCK_TIMEOUT 만큼 대기하지 않고 바로 반환하여 불필요한 경고를 줄입니다.
        if self.locks[ticker].locked():
            return False

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
                print(
                    f"[{now.strftime('%H:%M:%S')}] 거래 시간 외 ({trading_start_time}-{trading_end_time}) - 매도 모니터링만 진행"
                )
            return False

        # 매도 시간 설정 (15시 10분)
        sell_time = time(15, 10)

        # 매도 사유와 조건을 먼저 확인
        sell_reason = None
        # target_date가 boolean인지 확인하고 처리
        if isinstance(target_date, bool):
            # 타겟데이트가 boolean이면, 우선 매도 조건2, 3으로만 진행하고 조건1은 무시
            print(
                f"[경고] {ticker} 타겟데이트가 boolean({target_date})입니다. 기간만료 체크를 건너뜁니다."
            )
            # 로그 남기기
            self.slack_logger.send_log(
                level="WARNING",
                message="타겟데이트 형식 오류",
                context={
                    "종목코드": ticker,
                    "타겟데이트타입": str(type(target_date)),
                    "값": str(target_date),
                },
            )
        else:
            # 조건1: 보유기간 만료로 매도
            try:
                if today > target_date and current_time >= sell_time:
                    sell_reason = {
                        "매도사유": "기간만료",
                        "매도목표일": target_date.strftime("%Y-%m-%d")
                        if hasattr(target_date, "strftime")
                        else str(target_date),
                    }
            except TypeError as e:
                print(
                    f"[오류] 날짜 비교 실패: {e}, 타겟데이트: {type(target_date)}, 값: {target_date}"
                )

        # 조건2: 주가 상승으로 익절
        if target_price > (avr_price * SELLING_POINT_UPPER):
            sell_reason = {
                "매도사유": "주가 상승: 목표가 도달",
                "매도가": target_price,
                "매도조건가": avr_price * SELLING_POINT_UPPER,
            }

        # 조건3: 리스크 관리차 매도
        elif target_price < (avr_price * RISK_MGMT_UPPER):
            sell_reason = {
                "매도사유": "주가 하락: 리스크 관리차 매도",
                "매도가": target_price,
                "매도조건가": avr_price * RISK_MGMT_UPPER,
            }

        if sell_reason:
            lock_start_time = datetime.now()
            log_context = {
                "종목코드": ticker,
                "매도이유": sell_reason,
                "시작시간": lock_start_time.strftime("%H:%M:%S.%f")[:-3]
            }
            
            # 락 획득 시도 직전 로깅
            self.slack_logger.send_log(
                level="INFO",
                message=f"{ticker} 락 획득 시도 시작",
                context=log_context
            )
            
            try:
                # 타임아웃과 함께 락 획득 시도
                async with asyncio.timeout(self.LOCK_TIMEOUT):
                    # 락 획득 직전 현재 시간 기록
                    lock_acquire_start = datetime.now()
                    
                    async with self.locks[ticker]:
                        # 락 획득 직후 로깅
                        lock_acquire_time = datetime.now()
                        acquire_ms = (lock_acquire_time - lock_acquire_start).total_seconds() * 1000
                        
                        self.slack_logger.send_log(
                            level="INFO",
                            message=f"{ticker} 락 획득 완료",
                            context={
                                **log_context,
                                "락획득시간": lock_acquire_time.strftime("%H:%M:%S.%f")[:-3],
                                "획득소요ms": f"{acquire_ms:.1f}ms"
                            }
                        )
                        
                        # === 단계별 작업 시간 측정 ===
                        
                        # 1. 잔고 확인 시작
                        balance_start = datetime.now()
                        try:
                            # 매도 실행 전 잔고 확인 (최대 3회 재시도)
                            max_retry = 3
                            balance_result = None
                            for attempt in range(max_retry):
                                balance_result = await self.check_balance_async(ticker)
                                if balance_result:
                                    break
                                await asyncio.sleep(0.5)
                                
                            balance_end = datetime.now()
                            balance_ms = (balance_end - balance_start).total_seconds() * 1000
                            
                            self.slack_logger.send_log(
                                level="INFO",
                                message=f"{ticker} 잔고 확인 완료",
                                context={
                                    **log_context,
                                    "잔고확인소요ms": f"{balance_ms:.1f}ms",
                                    "잔고결과": "성공" if balance_result else "실패"
                                }
                            )
                            
                            # 2. 매도 실행 단계 시간 측정
                            if balance_result:  # 잔고가 있는 경우에만 매도
                                # 실제 보유 수량 확인
                                real_quantity = int(balance_result.get('hldg_qty', 0))
                                if real_quantity <= 0:
                                    self.slack_logger.send_log(
                                        level="WARNING",
                                        message=f"{ticker} 실제 보유량 없음, 모니터링 중단",
                                        context={**log_context}
                                    )
                                    # 중요: 모니터링 완전히 중단하고 구독 해제
                                    await self._stop_monitoring_internal(ticker)
                                    # 구독과 모니터링이 중단되었으므로 세션 데이터도 구독 취소 표시해야 함
                                    sell_completed = True  # 매도 완료로 처리하여 세션 정리되게 함
                                    return sell_completed
                                    
                                sell_start = datetime.now()
                                # 매도 실행 - TradingUpper의 sell_order 메서드 사용 (비동기 실행, 타임아웃 추가)
                                try:
                                    loop = asyncio.get_event_loop()
                                    # 매도 실행 콜백 함수
                                    sell_func = self.callback
                                    sell_results = await asyncio.wait_for(
                                        loop.run_in_executor(
                                            None,
                                            sell_func,
                                            session_id, ticker, real_quantity, target_price
                                        ),
                                        timeout=30.0  # 30초 타임아웃
                                    )
                                except asyncio.TimeoutError:
                                    self.slack_logger.send_log(
                                        level="ERROR",
                                        message=f"{ticker} 매도 주문 타임아웃 (30초)",
                                        context={**log_context}
                                    )
                                    return False
                                
                                sell_end = datetime.now()
                                sell_ms = (sell_end - sell_start).total_seconds() * 1000

                                # None 또는 iterable이 아닐 경우 빈 리스트로 대체
                                if not sell_results or not hasattr(sell_results, '__iter__'):
                                    sell_results = []
                                # 매도 결과 확인
                                sell_completed = len(sell_results) > 0 and any(
                                    result.get('success', False) for result in sell_results
                                )
                                
                                self.slack_logger.send_log(
                                    level="INFO",
                                    message=f"{ticker} 매도 처리 완료",
                                    context={
                                        **log_context,
                                        "매도소요ms": f"{sell_ms:.1f}ms",
                                        "매도결과": "성공" if sell_completed else "실패"
                                    }
                                )
                            else:
                                # 잔고 확인 실패 시 로그
                                self.slack_logger.send_log(
                                    level="WARNING",
                                    message=f"{ticker} 잔고 조회 실패",
                                    context={**log_context}
                                )
                                
                        except Exception as e:
                            self.slack_logger.send_log(
                                level="ERROR",
                                message=f"{ticker} 락 내부 처리 중 오류",
                                context={
                                    **log_context,
                                    "에러": str(e),
                                    "에러위치": "잔고조회" if 'balance_end' not in locals() else "매도처리"
                                }
                            )
                            raise  # 예외 재발생하여 finally 블록 실행 보장
                            
                        # 락 종료 직전 로깅
                        lock_end_time = datetime.now()
                        lock_held_ms = (lock_end_time - lock_acquire_time).total_seconds() * 1000
                        
                        self.slack_logger.send_log(
                            level="INFO",
                            message=f"{ticker} 락 해제 준비",
                            context={
                                **log_context,
                                "락점유시간ms": f"{lock_held_ms:.1f}ms"
                            }
                        )
                        
                    # 락 해제 후 로깅
                    lock_release_time = datetime.now()
                    total_ms = (lock_release_time - lock_start_time).total_seconds() * 1000
                    
                    self.slack_logger.send_log(
                        level="INFO",
                        message=f"{ticker} 락 해제 완료",
                        context={
                            **log_context,
                            "전체소요ms": f"{total_ms:.1f}ms"
                        }
                    )
                    
                    return sell_completed if 'sell_completed' in locals() else False
                    
            except TimeoutError:
                timeout_time = datetime.now()
                total_wait_ms = (timeout_time - lock_start_time).total_seconds() * 1000
                
                self.slack_logger.send_log(
                    level="WARNING",
                    message=f"{ticker} 락 획득 타임아웃",
                    context={
                        **log_context,
                        "대기시간ms": f"{total_wait_ms:.1f}ms"
                    }
                )
                return False
                
            except Exception as e:
                self.slack_logger.send_log(
                    level="ERROR", 
                    message=f"{ticker} 예외 발생",
                    context={
                        **log_context,
                        "에러": str(e)
                    }
                )
                return False
                
            finally:
                # 락 정리
                try:
                    if ticker in self.locks:
                        lock_is_locked = self.locks[ticker].locked()
                        if not lock_is_locked:
                            del self.locks[ticker]
                            self.slack_logger.send_log(
                                level="INFO",
                                message=f"{ticker} 락 정리 완료",
                                context={**log_context}
                            )
                        else:
                            self.slack_logger.send_log(
                                level="WARNING",
                                message=f"{ticker} 여전히 락 상태",
                                context={**log_context}
                            )
                except Exception as cleanup_error:
                    self.slack_logger.send_log(
                        level="ERROR",
                        message=f"{ticker} 락 정리 중 오류",
                        context={
                            **log_context,
                            "정리오류": str(cleanup_error)
                        }
                    )

    ######################################################################################
    ##############################    인증 관련 메서드   #####################################
    ######################################################################################

    async def _get_approval(
        self, app_key, app_secret, approval_type, max_retries=3, retry_delay=5
    ):
        """
        웹소켓 인증키 발급
        """

        cached_approval, cached_expires_at = self.db_manager.get_approval(approval_type)
        if cached_approval and cached_expires_at:
            # cached_expires_at이 문자열이면 datetime 객체로 변환
            if isinstance(cached_expires_at, str):
                try:
                    cached_expires_at = datetime.fromisoformat(cached_expires_at)
                except (ValueError, TypeError) as e:
                    logging.error(f"Failed to parse cached_expires_at: {e}")
                    cached_expires_at = None
            
            # datetime 객체인 경우에만 비교
            if cached_expires_at and hasattr(cached_expires_at, '__gt__'):
                if not isinstance(cached_expires_at, datetime):
                    try:
                        if isinstance(cached_expires_at, str):
                            cached_expires_at = datetime.fromisoformat(cached_expires_at)
                        elif isinstance(cached_expires_at, (int, float)):
                            cached_expires_at = datetime.fromtimestamp(cached_expires_at)
                        # 다른 타입들에 대한 변환 로직 추가 가능
                    except (ValueError, TypeError) as e:
                        logging.error(f"Failed to convert cached_expires_at: {e}")
                        cached_expires_at = None
                
                # datetime 객체인지 다시 확인 후 비교
                if isinstance(cached_expires_at, datetime) and cached_expires_at > datetime.utcnow():
                    logging.info("Using cached %s approval", approval_type)
                    return cached_approval, cached_expires_at

        url = "https://openapi.koreainvestment.com:9443/oauth2/Approval"
        headers = {"content-type": "application/json; utf-8"}
        body = {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "secretkey": app_secret,
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
                    self.db_manager.save_approval(
                        approval_type, self.approval_key, expires_at
                    )

                    logging.info(
                        "Successfully obtained and cached %s approval_key on attempt %d",
                        approval_type,
                        attempt + 1,
                    )
                    return self.approval_key, expires_at
                else:
                    logging.warning(
                        "Unexpected response format on attempt %d: %s",
                        attempt + 1,
                        approval_data,
                    )
            except RequestException as e:
                logging.error(
                    "An error occurred while fetching the %s approval_key on attempt %d: %s",
                    approval_type,
                    attempt + 1,
                    e,
                )
                if attempt < max_retries - 1:
                    logging.info("Retrying in %d seconds...", retry_delay)
                    await asyncio.sleep(retry_delay)
                else:
                    logging.error(
                        "Max retries reached. Unable to obtain %s approval_key.",
                        approval_type,
                    )

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
            if not self.mock_approval or self.mock_approval_expires_at is None or now >= self.mock_approval_expires_at:
                (
                    self.mock_approval,
                    self.mock_approval_expires_at,
                ) = await self._get_approval(M_APP_KEY, M_APP_SECRET, "mock")
            return self.mock_approval
        else:
            if not self.real_approval or self.real_approval_expires_at is None or now >= self.real_approval_expires_at:
                (
                    self.real_approval,
                    self.real_approval_expires_at,
                ) = await self._get_approval(R_APP_KEY, R_APP_SECRET, "real")
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
            for (
                session_id,
                ticker,
                name,
                qty,
                price,
                start_date,
                target_date,
            ) in sessions_info:
                await self.add_new_stock_to_monitoring(
                    session_id, ticker, name, qty, price, start_date, target_date
                )

            # 단일 웹소켓 수신 처리 시작 (중복 생성 방지)
            if self.receiver_task is None or self.receiver_task.done():
                self.receiver_task = asyncio.create_task(self._message_receiver())

            # 모든 태스크가 완료될 때까지 대기
            while self.background_tasks:
                done, _ = await asyncio.wait(
                    self.background_tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    try:
                        await task
                    except Exception as e:
                        print(f"태스크에러: {e}")

            # 모든 모니터링 태스크 완료 대기
            results = await asyncio.gather(
                *self.background_tasks, return_exceptions=True
            )
            print("real_time_monitoring 반환값:", results)
            return results

        except Exception as e:
            print(f"모니터링 중 오류 발생: {e}")

    async def stop_monitoring(self, ticker):
        """특정 종목의 모니터링만 중단 (루프 일치 보장)"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = self._loop
        if loop != self._loop:
            fut = asyncio.run_coroutine_threadsafe(
                self._stop_monitoring_internal(ticker), self._loop
            )
            fut.result()
            return
        await self._stop_monitoring_internal(ticker)

    async def _stop_monitoring_internal(self, ticker):
        try:
            # 먼저 구독 해제 확인
            if ticker not in self.subscribed_tickers:
                # 이미 구독 해제된 경우 로그만 남기고 진행
                self.slack_logger.send_log(
                    level="INFO",
                    message=f"{ticker} 이미 구독 해제됨",
                    context={"종목코드": ticker},
                )
            else:
                # 1. 구독 해제
                await self.unsubscribe_ticker(ticker)
                self.slack_logger.send_log(
                    level="INFO",
                    message=f"{ticker} 구독 해제 완료",
                    context={"종목코드": ticker},
                )
            
            # 2. 태스크 취소
            if ticker in self.active_tasks:
                task = self.active_tasks[ticker]
                try:
                    # Python 3.7+에서 Task 루프 확인
                    task_loop = task.get_loop()
                except AttributeError:
                    task_loop = self._loop  # Fallback
                
                # 현재 실행 중인 루프 확인
                try:
                    current_loop = asyncio.get_running_loop()
                except RuntimeError:
                    # 실행 중인 루프가 없는 경우
                    current_loop = None
                
                if current_loop is not None and task_loop != current_loop:
                    # 다른 루프에서 생성된 태스크는 해당 루프에서 안전하게 취소
                    def cancel_and_await():
                        if not task.done() and not task.cancelled():
                            task.cancel()
                        return asyncio.ensure_future(task)

                    fut = asyncio.run_coroutine_threadsafe(
                        cancel_and_await(), task_loop
                    )
                    try:
                        fut.result(timeout=5)  # 5초 타임아웃 추가
                    except Exception as e:
                        print(f"태스크 취소 중 오류: {e}")
                else:
                    # 같은 루프 또는 루프가 없는 경우
                    if not task.done() and not task.cancelled():
                        task.cancel()
                        try:
                            await asyncio.wait_for(asyncio.shield(task), timeout=5)
                        except (asyncio.CancelledError, asyncio.TimeoutError, Exception) as e:
                            print(f"태스크 취소 처리 중: {type(e).__name__}")
                
                # 타스크 삭제
                del self.active_tasks[ticker]
                self.slack_logger.send_log(
                    level="INFO",
                    message=f"{ticker} 모니터링 태스크 취소됨",
                    context={"종목코드": ticker},
                )
            
            # 3. 큐 정리
            if ticker in self.ticker_queues:
                del self.ticker_queues[ticker]
            
            # 4. 매수 중 플래그 정리
            async with self.buy_status_lock:
                if ticker in self.buying_in_progress:
                    del self.buying_in_progress[ticker]
            
            # 5. 락 정리
            if hasattr(self, "locks") and ticker in self.locks:
                if not self.locks[ticker].locked():
                    del self.locks[ticker]
            
            # 완료 로그
            self.slack_logger.send_log(
                level="INFO",
                message=f"{ticker} 모니터링 중단 및 리소스 정리 완료",
                context={"종목코드": ticker},
            )
        except Exception as e:
            self.slack_logger.send_log(
                level="ERROR",
                message="모니터링 중단 중 오류",
                context={"종목코드": ticker, "에러": str(e)},
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
                # --- 웹소켓 연결 상태 및 속성 체크 ---
                if self.websocket is None or not self.is_connected:
                    print("[WS] self.websocket is None 또는 연결 안 됨. 재연결 시도...")
                    try:
                        await self.connect_websocket()
                    except Exception as e:
                        print(f"[WS] connect_websocket 예외: {e}")
                        await asyncio.sleep(5)
                        continue
                    if self.websocket is None or not self.is_connected:
                        print("[WS] 재연결 실패, 5초 후 재시도")
                        await asyncio.sleep(5)
                        continue
                    # 재연결 후 종목 재구독
                    if self.subscribed_tickers:
                        to_resub = list(self.subscribed_tickers)
                        self.subscribed_tickers.clear()
                        print(f"[WS] 재구독 시도: {to_resub}")
                        for ticker in to_resub:
                            try:
                                await self.subscribe_ticker(ticker)
                            except Exception as sub_e:
                                print(f"[WS] 재구독 실패 {ticker}: {sub_e}")

                # 닫힌 소켓 체크
                if self.websocket is None or getattr(self.websocket, "closed", True):
                    print("[WS] 웹소켓이 None이거나 닫혀 있음. 재연결 필요")
                    self.is_connected = False
                    self.websocket = None
                    await asyncio.sleep(2)
                    continue

                # --- recv() 호출 및 예외 처리 ---
                try:
                    async with self.recv_lock:
                        data = await self.websocket.recv()
                except (KeyboardInterrupt, asyncio.CancelledError):
                    print(
                        "[WS] 수신 루프가 사용자 요청(ctrl+c) 또는 태스크 취소로 종료됩니다."
                    )
                    break
                except AttributeError as e:
                    print(f"[WS] AttributeError: {e}. self.websocket={self.websocket}")
                    self.is_connected = False
                    self.websocket = None
                    await asyncio.sleep(2)
                    continue
                except OSError as e:
                    if getattr(e, "errno", None) == 11001:
                        print(f"[WS] DNS 에러(getaddrinfo failed): {e}")
                    else:
                        print(f"[WS] OSError: {e}")
                    self.is_connected = False
                    self.websocket = None
                    await asyncio.sleep(5)
                    continue
                except Exception as e:
                    print(f"[WS] recv 예외: {e}")
                    self.is_connected = False
                    self.websocket = None
                    await asyncio.sleep(2)
                    continue
                # print(f"수신된 원본 데이터: {data}")  # 디버깅용

                # 웹소켓 연결상태 체크
                data_str = data.decode('utf-8') if isinstance(data, bytes) else data
                
                if '"tr_id":"PINGPONG"' in data_str:
                    try:
                        await self.websocket.send(data)  # 서버 요구에 따라 그대로 반송
                    except Exception as e:
                        print(f"PINGPONG 응답 실패: {e}")
                    continue

                # 구독 성공 메시지 체크
                if "SUBSCRIBE SUCCESS" in data_str:
                    # JSON 파싱 시도
                    data_dict = json.loads(data_str)
                    ticker = data_dict["header"]["tr_key"]
                    continue

                # # 실시간 호가 데이터일 경우
                data = data.encode('utf-8') if isinstance(data, str) else data
                recvvalue = data.split(b"^")

                # 실시간 호가 데이터일 경우
                # recvvalue = data.split("^")

                
                if len(recvvalue) > 1:  # 실제 호가 데이터인 경우
                    try:
                        # ticker = recvvalue[0].split("|")[-1]
                        ticker = recvvalue[0].split(b"|")[-1]
                        ticker_str = ticker.decode('utf-8') if isinstance(ticker, bytes) else ticker
                        
                        if ticker_str in self.subscribed_tickers:
                            await self.ticker_queues[ticker_str].put((recvvalue))
                        
                        # if ticker in self.subscribed_tickers:
                        #     await self.ticker_queues[ticker].put((recvvalue))
                        else:
                            print(f"종목코드 {ticker}는 구독 목록에 없음 (구독목록: {self.subscribed_tickers})")
                    except Exception as extract_e:
                        print(f"종목코드 추출 또는 큐 저장 중 오류: {extract_e}")

            except (KeyboardInterrupt, asyncio.CancelledError):
                print(
                    "[WS] 수신 루프가 사용자 요청(ctrl+c) 또는 태스크 취소로 종료됩니다."
                )
                break
            except ConnectionClosed:
                print("웹소켓 연결이 끊어졌습니다. 재연결을 시도합니다.")
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
                    print("기존 웹소켓 연결 종료")
                except Exception as e:
                    print(f"기존 웹소켓 종료 중 오류: {str(e)}")
                self.websocket = None

            url = "ws://ops.koreainvestment.com:31000/tryitout/H0STASP0"
            print(f"웹소켓 URL: {url}")
            
            self.connect_headers = {
                "approval_key": await self._ensure_approval(is_mock=True),
                "custtype": "P",
                "tr_type": "1",
                "content-type": "utf-8",
            }
            print(f"웹소켓 헤더 준비됨: {self.connect_headers}")
            self._loop = asyncio.get_running_loop()  # 현재 이벤트 루프 저장
            print("이벤트 루프 저장됨")

            # 타임아웃 설정하여 연결 시도
            print("웹소켓 연결 시도 중...")
            self.websocket = await asyncio.wait_for(
                websockets.connect(url, extra_headers=self.connect_headers),
                timeout=10.0,
            )
            self.is_connected = True
            print(f"웹소켓 연결 성공! (ID: {id(self.websocket)})")
            print("이벤트 루프 저장 및 웹소켓 연결 성공")

        except asyncio.TimeoutError:
            print("웹소켓 연결 타임아웃")
            self.is_connected = False
            self.websocket = None
            return False
            
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)
            print(f"웹소켓 연결 실패: {error_type} - {error_msg}")
            self.is_connected = False
            self.websocket = None
            return False
        
        return True

    async def close(self):
        """웹소켓 연결 종료 (루프 일치 보장)"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = self._loop
        if loop != self._loop:
            # 다른 루프에서 호출 시 안전하게 run_coroutine_threadsafe 사용
            fut = asyncio.run_coroutine_threadsafe(self._close_internal(), self._loop)
            fut.result()  # 동기화 보장(필요시)
            return
        await self._close_internal()

    async def _close_internal(self):
        self.is_connected = False

        # 모든 활성 태스크 취소
        for ticker, task in list(self.active_tasks.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
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
        # 종목 구독 전에 웹소켓 연결 상태 확인
        if not await self.ensure_websocket_connected():
            self.slack_logger.send_log(
                level="ERROR",
                message="웹소켓 연결 없이 구독 시도 실패",
                context={"종목코드": ticker}
            )
            return False
            
        if ticker in self.subscribed_tickers:
            print(f"이미 구독 중인 종목입니다: {ticker}")
            return
        self.connect_headers["tr_type"] = "1"
        request_data = {
            "header": self.connect_headers,
            "body": {
                "input": {
                    "tr_id": "H0STASP0",  # 실시간 호가 TR ID
                    "tr_key": ticker,
                }
            },
        }

        # 웹소켓 연결 상태 확인 및 재연결 시도
        if self.websocket is None or self.websocket.closed:
            await self.ensure_websocket_connected()
            
            # 재연결 후에도 웹소켓이 None이면 에러 처리
            if self.websocket is None:
                self.slack_logger.send_log(
                    level="ERROR",
                    message=f"웹소켓 연결 실패: {ticker} 구독 불가",
                    context={"ticker": ticker}
                )
                return

        try:
            await self.websocket.send(json.dumps(request_data))
            self.subscribed_tickers.add(ticker)
            # print(f"종목 구독 성공: {ticker}")
        except Exception as e:
            error_msg = f"종목 구독 실패: {ticker}, 에러: {str(e)}"
            print(error_msg)
            self.slack_logger.send_log(
                level="ERROR",
                message=error_msg,
                context={"ticker": ticker, "error": str(e)}
            )
            
            # 에러 발생 시 웹소켓 정리
            if self.websocket:
                await self._close_internal()
                self.websocket = None

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

        self.connect_headers["tr_type"] = "2"
        request_data = {
            "header": self.connect_headers,
            "body": {
                "input": {
                    "tr_id": "H0STASP0",  # 실시간 호가 TR ID
                    "tr_key": ticker,
                }
            },
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

    async def add_new_stock_to_monitoring(
        self, session_id, ticker, name, qty, price, start_date, target_date
    ):
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

    async def _monitor_ticker(
        self, session_id, ticker, name, quantity, avr_price, target_date
    ):
        """개별 종목 모니터링 및 매도 처리"""
        # 큐가 없거나 다른 루프에 바인딩되어 있으면 새로 생성
        if (
            ticker not in self.ticker_queues
            or getattr(self.ticker_queues[ticker], "_loop", None)
            is not asyncio.get_running_loop()
        ):
            self.ticker_queues[ticker] = asyncio.Queue()

        # SLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACK
        # 모니터링 시작 로그

        self.slack_logger.send_log(
            level="INFO",
            message="모니터링 시작",
            context={
                "종목코드": ticker,
                "평균단가": avr_price,
                "목표일": target_date,
                "보유수량": quantity,
            },
        )
        # 모니터링 프로세스 시작
        while True:
            # 거래시간 체크: 거래시간 외면 모니터링 중단
            if not self._is_market_open():
                print(
                    f"[{datetime.now().strftime('%H:%M:%S')}] 거래시간 외 - {ticker} 모니터링 중단"
                )
                self.slack_logger.send_log(
                    level="INFO",
                    message="거래시간 외 모니터링 중단",
                    context={"종목코드": ticker},
                )
                break
            # 구독이 해제되면 모니터링 종료
            if ticker not in self.subscribed_tickers:
                break
            try:
                try:
                    print('타임아웃 대기 시작')
                    # 타임아웃을 설정하여 데이터 대기
                    recvvalue = await asyncio.wait_for(
                        self.ticker_queues[ticker].get(), timeout=5.0
                    )
                    
                    # 매수 중인 경우 모니터링 건너뛰기
                    is_buying = await self.is_buying_in_progress(ticker)
                    if is_buying:
                        print(f'{ticker} - 매수 중인 종목이므로 모니터링 건너뜀')
                        self.ticker_queues[ticker].task_done()
                        continue
                        
                    # 매도 조건 확인
                    print('데이터 수신 성공:', ticker)
                    sell_completed = await self.sell_condition(
                        recvvalue,
                        session_id,
                        ticker,
                        name,
                        quantity,
                        avr_price,
                        target_date,
                    )

                    if sell_completed:
                        # 매도 완료 또는 잔고 없음으로 인한 세션 종료 처리
                        if ticker in self.subscribed_tickers:
                            await self.unsubscribe_ticker(ticker)
                        print(f"{ticker} 매도 완료 또는 세션 종료")
                        # 세션이 종료되었으므로 모니터링 루프 종료
                        return True

                    # 큐 태스크 종료
                    self.ticker_queues[ticker].task_done()

                except asyncio.TimeoutError:
                    # 타임아웃은 데이터 미수신 상태를 나타내며, 네트워크 재연결 대기 동안 자주 발생할 수 있다.
                    # 웹소켓 연결 여부와 관계없이 계속 루프를 유지하여 재연결을 기다린다.
                    print(f'타임아웃 발생: {ticker} - 5초 동안 데이터 없음')
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
        market_start = now.replace(
            hour=KRX_TRADING_START.hour, minute=KRX_TRADING_START.minute, second=0
        )
        market_end = now.replace(
            hour=KRX_TRADING_END.hour, minute=KRX_TRADING_END.minute, second=0
        )

        # 주말 체크
        if now.weekday() >= 5:  # 5: 토요일, 6: 일요일
            return False

        # 장 운영 시간 체크
        return market_start <= now <= market_end

    async def check_balance_async(self, ticker):
        """비동기적으로 종목의 잔고를 확인합니다."""
        try:
            # API 락 획득 후 잔고 조회
            async with self.api_lock:
                balance_list = await asyncio.to_thread(self.kis_api.balance_inquiry)
                if not balance_list:
                    return None
                
                # 해당 종목 찾기
                balance_data = next(
                    (item for item in balance_list if item.get("pdno") == ticker),
                    None
                )
                return balance_data
        except Exception as e:
            self.slack_logger.send_log(
                level="ERROR",
                message="비동기 잔고 조회 실패",
                context={"종목코드": ticker, "에러": str(e)}
            )
            return None
            
    async def set_buying_in_progress(self, ticker, status=True):
        """매수 중인 종목 상태 설정 (매수 중: True, 매수 완료: False)"""
        async with self.buy_status_lock:
            self.buying_in_progress[ticker] = status
            print(f"{ticker} 매수 상태 설정: {'매수 중' if status else '매수 완료'}")
            message = f"{ticker} 모니터링 {'일시 중지' if status else '재개'}"
            self.slack_logger.send_log(
                level="INFO",
                message=message,
                context={"종목코드": ticker, "매수 중": status}
            )
    
    async def is_buying_in_progress(self, ticker):
        """특정 종목이 매수 중인지 확인"""
        async with self.buy_status_lock:
            return self.buying_in_progress.get(ticker, False)

    async def ensure_websocket_connected(self):
        """웹소켓 연결이 없으면 연결을 시도합니다."""
        if self.websocket is None or self.websocket.closed:
            for attempt in range(3):  # 최대 3번 재시도
                try:
                    print(f"웹소켓 재연결 시도 #{attempt+1}")
                    await self.connect_websocket()
                    if self.websocket and not self.websocket.closed:
                        print(f"웹소켓 재연결 성공 (시도 #{attempt+1})")
                        return True
                except Exception as e:
                    print(f"웹소켓 재연결 시도 #{attempt+1} 실패: {str(e)}")
                    await asyncio.sleep(1)
            print("최대 재시도 횟수 초과, 웹소켓 연결 실패")
            return False
        return True