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
    RISK_MGMT_STRONG_MOMENTUM,
    KRX_TRADING_START,
    KRX_TRADING_END,
    TRAILING_STOP_PERCENTAGE
)
from utils.trading_logger import TradingLogger
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
        self._sell_order = callback
        self.websocket = None
        self.subscribed_tickers = set()
        self.ticker_queues = {}
        # 현재 루프 저장 (생성 시점 루프)
        try:
            self._monitor_loop = asyncio.get_running_loop()
        except RuntimeError:
            self._monitor_loop = asyncio.get_event_loop()
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
        # self.locks = {}
        self.selling_in_progress = set()
        self.LOCK_TIMEOUT = 10
        self.recv_lock = asyncio.Lock()
        self.kis_api = KISApi()
        self.api_lock = asyncio.Lock()
        # 매수 중인 종목 추적 (key: 종목코드, value: 매수 중 상태)
        self.buying_in_progress = {}
        self.buy_status_lock = asyncio.Lock()
        self.logger = TradingLogger()
        self.pending_sell = {}
        # 전역 매도 세마포어 - 동시 매도 제한 (값이 1이면 한 번에 하나의 매도만 처리)
        self.global_sell_semaphore = asyncio.Semaphore(1)
        # 티커별 매도 락 (key: 종목코드, value: 락 객체)
        self.ticker_sell_locks = {}


    ######################################################################################
    ##################################    매도 로직   #####################################
    ######################################################################################

    async def sell_condition(self, recv_value, session_id, ticker, name, quantity, avg_price, target_date, trade_condition):
        """
        매도 조건을 판단하고, 조건이 충족되면 매도 처리를 진행합니다.
        """
        # ------------------------------------------------------------------
        # 새 로직: 매도 조건만 판단하여 결과를 반환 (_monitor_ticker에서 매도 실행)
        # ------------------------------------------------------------------
        try:
            if not avg_price:
                self.logger.error(f"{ticker} avg_price=0, 매도 조건 판단 건너뜀")
                return False
                
            # 1) 수신 데이터 유효성 체크
            if len(recv_value) == 1 and "SUBSCRIBE SUCCESS" in recv_value[0]:
                return False

            current_price = int(recv_value[15])
            tick_interval = self.get_tick(current_price)
            target_price = max(current_price - tick_interval * 2, tick_interval)

            now = datetime.now()
            current_date = now.date()
            current_time = now.time()

            # 2) 거래시간이 아니면 조건 미충족
            if current_time < KRX_TRADING_START or current_time > KRX_TRADING_END:
                return False

            # 3) 매도 사유 판단
            sell_reason = {"매도가": target_price}
            sell_reason_text = None

            # (1) 보유기간 만료
            sell_time_threshold = time(15, 10)  # 15:10 이후
            if current_date > target_date and current_time >= sell_time_threshold:
                sell_reason_text = "기간만료"
                sell_reason["매도목표일"] = (
                    target_date.strftime("%Y-%m-%d") if hasattr(target_date, "strftime") else str(target_date)
                )

            # (2) 리스크 관리(손절)
            if sell_reason_text is None:
                if trade_condition == "strong_momentum":
                    risk_threshold = RISK_MGMT_STRONG_MOMENTUM
                    reason = "주가 하락: 강력 모멘텀 리스크 관리"
                else:
                    risk_threshold = RISK_MGMT_UPPER
                    reason = "주가 하락: 리스크 관리차 매도"

                if target_price < (avg_price * risk_threshold):
                    sell_reason_text = reason
                    sell_reason["매도조건가"] = int(avg_price * risk_threshold)

            # (3) 트레일링스탑(익절 보호)
            if sell_reason_text is None:
                current_profit_ratio = target_price / avg_price
                if not hasattr(self, "ticker_high_ratio"):
                    self.ticker_high_ratio = {}
                if ticker not in self.ticker_high_ratio or current_profit_ratio > self.ticker_high_ratio[ticker]:
                    self.ticker_high_ratio[ticker] = current_profit_ratio

                high_ratio = self.ticker_high_ratio[ticker]
                selling_threshold = SELLING_POINT_UPPER  # 예: 1.08 (8%)
                trailing_threshold = 1 - TRAILING_STOP_PERCENTAGE  # 예: 0.96 (4%)

                if selling_threshold < high_ratio < (selling_threshold + TRAILING_STOP_PERCENTAGE):
                    if current_profit_ratio < selling_threshold:
                        sell_reason_text = "트레일링스탑: 수익률 8% 미만으로 하락"
                elif high_ratio >= (selling_threshold + TRAILING_STOP_PERCENTAGE):
                    if current_profit_ratio <= (high_ratio * trailing_threshold):
                        sell_reason_text = "트레일링스탑: 고점 대비 4% 하락"

            # 4) 매도 조건 최종 판단
            if sell_reason_text is None:
                return False  # 조건 미충족

            sell_reason["매도사유"] = sell_reason_text

            session_exists = self.db_manager.get_session_by_id(session_id) is not None

            return {
                "sell_decision": True,
                "target_price": target_price,
                "sell_reason": sell_reason,
                "session_exists": session_exists,
            }

        except Exception as e:
            self.slack_logger.send_log(
                level="ERROR",
                message=f"{ticker} 매도 조건 판단 중 예외 발생",
                context={"종목코드": ticker, "에러": str(e)},
            )
            return False
    # ------------------------------------------------------------------
    # 기존 매도 실행 로직은 더 이상 사용되지 않음 (legacy) -------
    # ------------------------------------------------------------------

    async def sell_condition_legacy_disabled(self, *args, **kwargs):
        """(Legacy) Disabled after sell refactor. Always returns False."""
        return False

    # -------- 이하 legacy 코드(사용되지 않음) --------
    async def sell_condition_legacy(self, recv_value, session_id, ticker, name, quantity, avg_price, target_date, trade_condition):
        """
        매도 조건을 판단하고, 조건이 충족되면 매도 처리를 진행합니다.
        """
        if ticker in self.selling_in_progress:
            # 이미 다른 비동기 작업에서 매도 처리가 진행 중인 경우, 현재 작업은 중복이므로 무시합니다.
            return False
            
        # 세마포어 획득 시도 (비강제: 이미 다른 매도가 진행 중이면 바로 반환)
        if self.global_sell_semaphore.locked():
            self.logger.info(f"다른 종목 매도 중 - {ticker} 매도 연기", {"ticker": ticker})
            return False
            
        # 현재 종목을 매도 중인 종목으로 표시
        self.selling_in_progress.add(ticker)

        try:
            # 세마포어 획득 - 다른 종목의 매도 처리가 끝날 때까지 대기
            await self.global_sell_semaphore.acquire()
            
            # 세마포어 획득 후 매도 처리 시작
            # self.logger.info(f"{ticker} 매도 처리 시작", {"ticker": ticker})
            
            if len(recv_value) == 1 and "SUBSCRIBE SUCCESS" in recv_value[0]:
                return False

            current_price = int(recv_value[15])
            tick_interval = self.get_tick(current_price)
            target_price = max(current_price - tick_interval * 2, tick_interval)

            now = datetime.now()
            current_date = now.date()
            current_time = now.time()

            # 매도 시간 범위 설정 (한국거래소 기준)
            if current_time < KRX_TRADING_START or current_time > KRX_TRADING_END:
                if current_time.minute % 10 == 0 and current_time.second < 10:
                    print("장 시간이 아닙니다. 현재 시간:", current_time)
                return False

            # 매도 사유 판단 및 기록 (조건식 기반)
            sell_reason = {"매도가": target_price}
            sell_reason_text = None

            # 조건1: 보유기간 만료로 매도
            sell_time_threshold = time(15, 10)  # 오후 3시 10분 이후 매도
            try:
                if current_date > target_date and current_time >= sell_time_threshold:
                    sell_reason_text = "기간만료"
                    sell_reason["매도목표일"] = target_date.strftime("%Y-%m-%d") if hasattr(target_date, "strftime") else str(target_date)
            except TypeError as e:
                print(f"[오류] 날짜 비교 실패: {e}, 타겟데이트: {type(target_date)}, 값: {target_date}")

            # 조건2: 주가 상승으로 익절
            # if sell_reason_text is None and target_price > (avg_price * SELLING_POINT_UPPER):
            #     sell_reason_text = "주가 상승: 목표가 도달"
            #     sell_reason["매도조건가"] = int(avg_price * SELLING_POINT_UPPER)  # 정수로 변환

            # 조건3: 리스크 관리차 매도
            if sell_reason_text is None:
                # trade_condition에 따라 다른 손절 라인 적용
                if trade_condition == "strong_momentum":
                    risk_threshold = RISK_MGMT_STRONG_MOMENTUM
                    reason = "주가 하락: 강력 모멘텀 리스크 관리"
                else:
                    risk_threshold = RISK_MGMT_UPPER
                    reason = "주가 하락: 리스크 관리차 매도"

                if target_price < (avg_price * risk_threshold):
                    sell_reason_text = reason
                    sell_reason["매도조건가"] = int(avg_price * risk_threshold)
        
            # 조건2: 주가 상승으로 익절
            # 트레일링스탑 로직 구현
            if sell_reason_text is None:
                # 현재 수익률 계산
                current_profit_ratio = target_price / avg_price
                
                # 종목별 최고 수익률 추적 (딕셔너리에 저장하여 관리)
                if not hasattr(self, 'ticker_high_ratio'):
                    self.ticker_high_ratio = {}
                
                # 최고 수익률 업데이트
                if ticker not in self.ticker_high_ratio or current_profit_ratio > self.ticker_high_ratio[ticker]:
                    self.ticker_high_ratio[ticker] = current_profit_ratio
                
                high_ratio = self.ticker_high_ratio[ticker]
                selling_threshold = SELLING_POINT_UPPER  # 기준 수익률 (8%)
                trailing_threshold = 1 - (TRAILING_STOP_PERCENTAGE)  # 예: 4%면 0.96
                
                # 케이스 1: 수익률이 8% 이하일 때는 트레일링스탑 미적용 (기존 로직대로 작동)
                if current_profit_ratio <= selling_threshold:
                    pass
                    
                # 케이스 2: 수익률이 8~12% 사이일 때, 8% 미만으로 떨어지면 매도
                elif selling_threshold < high_ratio < (selling_threshold + TRAILING_STOP_PERCENTAGE):
                    if current_profit_ratio < selling_threshold:
                        sell_reason_text = "트레일링스탑: 수익률 8% 미만으로 하락"
                        sell_reason["최고수익률"] = round((high_ratio - 1) * 100, 2)
                        sell_reason["현재수익률"] = round((current_profit_ratio - 1) * 100, 2)
                        sell_reason["하락비율"] = round(((high_ratio - current_profit_ratio) / high_ratio) * 100, 2)
                
                # 케이스 3: 수익률이 12%를 초과한 경우, 고점 대비 4% 하락 시 매도
                elif high_ratio >= (selling_threshold + TRAILING_STOP_PERCENTAGE):
                    if current_profit_ratio <= (high_ratio * trailing_threshold):
                        sell_reason_text = f"트레일링스탑: 고점 대비 {(TRAILING_STOP_PERCENTAGE*100)-100}% 하락"
                        sell_reason["최고수익률"] = round((high_ratio - 1) * 100, 2)
                        sell_reason["현재수익률"] = round((current_profit_ratio - 1) * 100, 2)
                        sell_reason["하락비율"] = round(((high_ratio - current_profit_ratio) / high_ratio) * 100, 2)
                
                # 매도 사유가 없으면 종료
                if sell_reason_text is None:
                    return False

            # 최종 매도 사유 저장
            sell_reason["매도사유"] = sell_reason_text

            # 매도 실행 시작 시간 기록
            start_time = datetime.now()
            log_context = {
                "종목코드": ticker,
                "매도이유": sell_reason,
                "시작시간": start_time.strftime("%H:%M:%S.%f")[:-3]
            }

            # 세션 확인
            session = self.db_manager.get_session_by_id(session_id)
            if not session:
                # 세션이 없으면 모니터링 중단 처리
                try:
                    await self._stop_monitoring_internal(ticker)
                except asyncio.CancelledError:
                    # 정상적인 취소 흐름
                    pass
                except Exception as e:
                    self.logger.error(f"{ticker} - 세션 없음 - 모니터링 중단 중 오류: {str(e)}")
                return True  # True 반환하여 모니터링 루프가 종료되도록 함

            # 매도 실행
            try:
                loop = asyncio.get_event_loop()
                sell_results = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        self._sell_order,
                        session_id, ticker, target_price
                    ),
                        timeout=30.0  # 30초 타임아웃
                    )
            except asyncio.TimeoutError:
                self.logger.error(f"{ticker} 매도 주문 타임아웃 (30초)")
                return False

            # 매도 결과 처리
            sell_duration_ms = (datetime.now() - start_time).total_seconds() * 1000

            if not sell_results or not isinstance(sell_results, dict):
                sell_results = {}

            # sell_order의 결과(dict)가 None이 아니고, 'rt_cd'가 '0'인지 확인
            is_sell_completed = (
                sell_results is not None and
                sell_results.get('rt_cd') == '0'
            )

            self.slack_logger.send_log(
                level="INFO",
                message=f"{ticker} 매도 처리 완료",
                context={
                    **log_context,
                    "매도소요ms": f"{sell_duration_ms:.1f}ms",
                    "매도결과": "성공" if is_sell_completed else "실패"
                }
            )

            if is_sell_completed:
                # 작업 취소 예외를 처리하여 안전하게 모니터링을 중단합니다.
                try:
                    await self._stop_monitoring_internal(ticker)
                except asyncio.CancelledError:
                    # 스스로를 취소하는 과정에서 발생하는 예외이므로 정상적인 흐름입니다.
                    pass

            return is_sell_completed

        except Exception as e:
            self.slack_logger.send_log(
                level="ERROR",
                message=f"{ticker} 매도 처리 중 예외 발생",
                context={
                    "종목코드": ticker,
                    "에러": str(e)
                }
            )
            return False

        finally:
            # 전역 세마포어 해제 (다른 매도 작업이 진행될 수 있도록)
            if self.global_sell_semaphore.locked():
                self.global_sell_semaphore.release()
                # self.logger.info(f"{ticker} 매도 작업 완료 - 다음 매도 가능", {"ticker": ticker})
                
            # 개별 종목 매도 진행 중 상태 해제
            if ticker in self.selling_in_progress:
                self.selling_in_progress.remove(ticker)
                
            # 티커별 락 해제 - _monitor_ticker에서 획득한 락을 여기서 해제해야 함
            if ticker in self.ticker_sell_locks and self.ticker_sell_locks[ticker].locked():
                self.ticker_sell_locks[ticker].release()
                # self.logger.info(f"{ticker} 티커 락 해제 완료", {"ticker": ticker})


    def get_tick(self, price: int) -> int:
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

            # 세션별 모니터링 시작
            for session in sessions_info:
                session_id, ticker, name, quantity, avg_price, start_date, target_date, trade_condition = session
                await self.start_monitoring_ticker(session_id, ticker, name, quantity, avg_price, start_date, target_date, trade_condition)

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

            # 모든 태스크가 완료되면 그냥 리턴
            return True

        except Exception as e:
            print(f"모니터링 중 오류 발생: {e}")
            return False

    async def stop_monitoring(self, ticker):
        """특정 종목의 모니터링만 중단 (루프 일치 보장)"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = self._monitor_loop
        if loop != self._monitor_loop:
            fut = asyncio.run_coroutine_threadsafe(
                self._stop_monitoring_internal(ticker), self._monitor_loop
            )
            fut.result()
            return
        await self._stop_monitoring_internal(ticker)

    async def _stop_monitoring_internal(self, ticker):
        try:
            # 먼저 구독 해제 확인
            if ticker not in self.subscribed_tickers:
                # 이미 구독 해제된 경우 로그만 남기고 진행
                self.logger.info(f"{ticker} 이미 구독 해제됨")
            else:
                # 1. 구독 해제
                await self.unsubscribe_ticker(ticker)
                self.logger.info(f"{ticker} 구독 해제 완료")
            
            # 2. 태스크 취소
            if ticker in self.active_tasks:
                task = self.active_tasks[ticker]
                try:
                    # Python 3.7+에서 Task 루프 확인
                    task_loop = task.get_loop()
                except AttributeError:
                    task_loop = self._monitor_loop  # Fallback
                
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
                
                # 태스크 삭제
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
                            # print(ticker, type(ticker))
                            # print(ticker_str, type(ticker_str))
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
            loop = self._monitor_loop
        if loop != self._monitor_loop:
            # 다른 루프에서 호출 시 안전하게 run_coroutine_threadsafe 사용
            fut = asyncio.run_coroutine_threadsafe(self._close_internal(), self._monitor_loop)
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

    async def start_monitoring_ticker(self, session_id, ticker, name, quantity, avg_price, start_date, target_date, trade_condition):
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
            self._monitor_ticker(session_id, ticker, name, quantity, avg_price, target_date, trade_condition)
        )
        task.add_done_callback(self.background_tasks.discard)
        self.background_tasks.add(task)
        self.active_tasks[ticker] = task

        print(f"{ticker} 모니터링 태스크 추가됨")

    ######################################################################################
    ##############################    모니터링 메서드   #######################################
    ######################################################################################

    async def _monitor_ticker(
        self, session_id, ticker, name, quantity, avr_price, target_date, trade_condition
    ):
        """개별 종목 모니터링 및 매도 처리"""
        # 큐가 없거나 다른 루프에 바인딩되어 있으면 새로 생성
        if (
            ticker not in self.ticker_queues
            or getattr(self.ticker_queues[ticker], "_loop", None) is not asyncio.get_running_loop()
        ):
            self.ticker_queues[ticker] = asyncio.Queue()

        # SLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACKSLACK
        # 모니터링 시작 로그

        self.slack_logger.send_log(
            level="INFO",
            message="모니터링 시작",
            context={
                    "종목코드": ticker,
                    "종목이름": name,
                    "평균단가": avr_price,
                    "목표일": target_date,
                    "보유수량": quantity,
            },
        )

        # ---------- 계좌-DB 정합성 동기화 ----------
        try:
            quantity, avr_price, closed = await self.sync_session_with_balance(
                session_id, ticker, quantity, avr_price
            )
            if closed:
                # 잔고가 없어 세션이 종료되었으므로 모니터링도 중단
                if ticker in self.subscribed_tickers:
                    await self.unsubscribe_ticker(ticker)
                return True
        except Exception as e:
            self.logger.error(
                "초기 정합성 동기화 실패",
                {"context": {"종목코드": ticker, "에러": str(e)}},
            )
        # 세션 확인 카운터 초기화
        session_check_counter = 0
        
        # 모니터링 프로세스 시작
        while True:
            # 거래시간 체크: 거래시간 외면 모니터링 중단
            if not self._is_market_open():
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 거래시간 외 - {ticker} 모니터링 중단")
                self.logger.info(
                    "거래시간 외 모니터링 중단",
                    {
                        "context": {
                            "종목코드": ticker,
                        }
                    },
                )
                break
                
            # 구독 상태 확인
            if ticker not in self.subscribed_tickers:
                self.logger.info(
                    f"{ticker} 구독 취소됨 - 모니터링 종료",
                    {"context": {"종목코드": ticker}}
                )
                break
                
            # 주기적으로 세션 존재 여부 확인 (약 1분마다)
            session_check_counter += 1
            if session_check_counter >= 2:  # 30초 타임아웃 * 2 = 약 60초
                # 매수 중인 종목인지 확인
                is_buying = await self.is_buying_in_progress(ticker)
                if not is_buying:  # 매수 중이 아닌 경우에만 세션 검증 수행
                    session = self.db_manager.get_session_by_id(session_id)
                    if not session:
                        self.slack_logger.send_log(
                            level="INFO", 
                            message=f"{ticker} 세션이 존재하지 않음 - 모니터링 종료", 
                            context={"종목코드": ticker, "세션ID": session_id}
                        )
                        if ticker in self.subscribed_tickers:
                            await self.unsubscribe_ticker(ticker)
                        return True
                else:
                    self.logger.info(f"매수 중인 종목이므로 세션 검증 건너뜀: {ticker}")
                session_check_counter = 0
                
            try:
                try:
                    # 타임아웃을 설정하여 데이터 대기
                    recvvalue = await asyncio.wait_for(
                        self.ticker_queues[ticker].get(), timeout=30.0
                    )
                    
                    # 매수 중인 경우 모니터링 건너뛰기
                    is_buying = await self.is_buying_in_progress(ticker)
                    if is_buying:
                        print(f'{ticker} - 매수 중인 종목이므로 모니터링 건너뜀')
                        self.ticker_queues[ticker].task_done()
                        continue
                    
                    # 티커별 매도 락 확인
                    if ticker in self.ticker_sell_locks and self.ticker_sell_locks[ticker].locked():
                        self.logger.info(f"{ticker} - 이미 매도 진행 중이므로 건너뜀", {"ticker": ticker})
                        self.ticker_queues[ticker].task_done()
                        continue

                    # 티커에 락이 없으면 생성
                    if ticker not in self.ticker_sell_locks:
                        self.ticker_sell_locks[ticker] = asyncio.Lock()

                    # --- 매도 조건 확인 및 필요 시 매도 실행 ---
                    await self.ticker_sell_locks[ticker].acquire()
                    try:
                        # 1) 매도 조건만 판단 (실제 매도 실행 X)
                        sell_signal = await self.sell_condition(
                            recvvalue,
                            session_id,
                            ticker,
                            name,
                            quantity,
                            avr_price,
                            target_date,
                            trade_condition,
                        )

                        # 조건 충족 여부 확인
                        if not (isinstance(sell_signal, dict) and sell_signal.get("sell_decision")):
                            # 조건 미충족 → 다음 루프로
                            self.ticker_queues[ticker].task_done()
                            continue

                        # 세션이 이미 종료된 경우 → 모니터링 중단
                        if not sell_signal.get("session_exists", True):
                            if ticker in self.subscribed_tickers:
                                await self.unsubscribe_ticker(ticker)
                            return True

                        target_price = sell_signal.get("target_price")
                        sell_reason = sell_signal.get("sell_reason", {})

                        # 2) 전역 세마포어로 동시 매도 제한
                        await self.global_sell_semaphore.acquire()
                        try:
                            start_time = datetime.now()
                            log_context = {
                                "종목코드": ticker,
                                "매도이유": sell_reason,
                                "시작시간": start_time.strftime("%H:%M:%S.%f")[:-3],
                            }

                            # 3) sell_order 콜백 실행 (동기 함수를 스레드 풀에서 실행)
                            loop = asyncio.get_event_loop()
                            sell_results = await asyncio.wait_for(
                                loop.run_in_executor(
                                    None,
                                    self._sell_order,
                                    session_id,
                                    ticker,
                                    target_price,
                                ),
                                timeout=30.0,
                            )

                            sell_duration_ms = (datetime.now() - start_time).total_seconds() * 1000

                            if not sell_results or not isinstance(sell_results, dict):
                                sell_results = {}

                            is_sell_completed = sell_results.get("rt_cd") == "0"

                            self.slack_logger.send_log(
                                level="INFO",
                                message=f"{ticker} 매도 처리 완료",
                                context={
                                    **log_context,
                                    "매도소요ms": f"{sell_duration_ms:.1f}ms",
                                    "매도결과": "성공" if is_sell_completed else "실패",
                                },
                            )

                            # 매도 성공 후 잔고/세션 동기화
                            if is_sell_completed:
                                quantity, avr_price, closed = await self.sync_session_with_balance(
                                    session_id, ticker, quantity, avr_price
                                )
                                if closed:
                                    if ticker in self.subscribed_tickers:
                                        await self.unsubscribe_ticker(ticker)
                                    return True  # 모니터링 종료 (잔고 없음)
                        except asyncio.TimeoutError:
                            self.slack_logger.send_log(
                                level="ERROR",
                                message=f"{ticker} 매도 주문 타임아웃 (30초)",
                                context={"종목코드": ticker},
                            )
                        finally:
                            if self.global_sell_semaphore.locked():
                                self.global_sell_semaphore.release()
                    finally:
                        # 티커 락 해제
                        if self.ticker_sell_locks[ticker].locked():
                            self.ticker_sell_locks[ticker].release()

                    # 큐 태스크 종료
                    self.ticker_queues[ticker].task_done()

                except asyncio.TimeoutError:
                    # 타임아웃은 데이터 미수신 상태를 나타내며, 네트워크 재연결 대기 동안 자주 발생할 수 있다.
                    # 웹소켓 연결 여부와 관계없이 계속 루프를 유지하여 재연결을 기다린다.
                    self.logger.info(f"{ticker}: 데이터 수신 대기 중")
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
            self.logger.error(
                "비동기 잔고 조회 실패",
                {
                    "context": {
                        "종목코드": ticker,
                        "에러": str(e)
                    }
                }
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

    #############################################################
    #    DB-계좌 정합성 동기화 메서드
    #############################################################
    async def sync_session_with_balance(self, session_id, ticker, current_qty, current_avr_price):
        """모니터링 시작 시 DB 세션과 실제 계좌 잔고를 동기화합니다.

        반환:
            updated_qty, updated_avr_price, closed
            closed가 True이면 잔고가 없어 세션이 종료되어야 함
        """
        # 매수 중인 종목인지 확인
        is_buying = await self.is_buying_in_progress(ticker)
        if is_buying:
            # 매수 중인 경우 DB 값 유지하고 세션 유지
            self.logger.info(
                f"매수 중인 종목이므로 잔고 동기화 건너뜀: {ticker}",
                {
                    "context": {
                        "종목코드": ticker,
                        "세션ID": session_id,
                    }
                },
            )
            return current_qty, current_avr_price, False
            
        try:
            balance_data = await self.check_balance_async(ticker)
        except Exception as e:
            print(f"잔고 확인 실패: {e}")
            # 에러 발생 시 DB 값 그대로 사용
            return current_qty, current_avr_price, False

        if balance_data is None:
            # 조회 실패: 기존 값 유지
            return current_qty, current_avr_price, False

        try:
            actual_qty = int(balance_data.get("hldg_qty", 0))
            avr_price = int(float(balance_data.get("pchs_avg_pric", 0)))
        except (TypeError, ValueError):
            # 파싱 실패 시 기존 값 유지
            actual_qty = current_qty
            avr_price = current_avr_price

        # 잔고가 없으면 세션 종료 필요
        if actual_qty == 0:
            await asyncio.to_thread(self.db_manager.delete_session_one_row, session_id)
            self.logger.info(
                "실잔고 0 → 세션 삭제",
                {
                    "context": {
                        "종목코드": ticker,
                        "세션ID": session_id,
                    }
                },
            )
            return 0, avr_price, True

        # DB 값이 실제 잔고와 다르면 업데이트
        if actual_qty != current_qty or avr_price != current_avr_price:
            def _update_db():
                try:
                    session = self.db_manager.get_session_by_id(session_id)
                    if session:
                        spent_fund = actual_qty * avr_price
                        self.db_manager.save_trading_session_upper(
                            session_id,
                            session.get("start_date"),
                            datetime.now(),
                            ticker,
                            session.get("name"),
                            session.get("high_price", 0),
                            session.get("fund", 0),
                            spent_fund,
                            actual_qty,
                            avr_price,
                            session.get("count", 0),
                        )
                except Exception as e:
                    print(f"[SYNC ERROR] DB 업데이트 실패: {e}")

            await asyncio.to_thread(_update_db)
            self.logger.info(
                "DB 세션 ↔ 실잔고 동기화",
                {
                    "context": {
                        "종목코드": ticker,
                        "기존수량": current_qty,
                        "실제수량": actual_qty,
                        "기존평균가": current_avr_price,
                        "실제평균가": avr_price,
                    }
                },
            )
            return actual_qty, avr_price, False

        # 변화 없음
        return current_qty, current_avr_price, False