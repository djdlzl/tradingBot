import json
import requests
import logging
from requests.exceptions import RequestException
from utils.string_utils import unicode_to_korean
from config.config import R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET, M_ACCOUNT_NUMBER
from config.condition import BUY_DAY_AGO
from datetime import datetime, timedelta
from database.db_manager_upper import DatabaseManager
import time
from threading import Lock



class KISApi:
    """한국투자증권 API와 상호작용하기 위한 클래스입니다."""
    _global_api_lock = Lock()  # 전역 API 호출 락

    def __init__(self):
        """KISApi 클래스의 인스턴스를 초기화합니다."""
        self.headers = {"content-type": "application/json; charset=utf-8"}
        self.w_headers = {"content-type": "utf-8"}
        self.real_token = None
        self.mock_token = None
        self.real_token_expires_at = None
        self.mock_token_expires_at = None
        self.real_approval = None
        self.mock_approval = None
        self.real_approval_expires_at = None
        self.mock_approval_expires_at = None
        self.hashkey = None
        self.upper_limit_stocks = {}
        self.watchlist = set()

######################################################################################
#########################    인증 관련 메서드   #######################################
######################################################################################

    def _get_token(self, app_key, app_secret, token_type, max_retries=3, retry_delay=5):
        """
        지정된 토큰 유형에 대한 액세스 토큰을 가져옵니다.

        Args:
            app_key (str): 애플리케이션 키
            app_secret (str): 애플리케이션 시크릿
            token_type (str): 토큰 유형 ('real' 또는 'mock')
            max_retries (int): 최대 재시도 횟수
            retry_delay (int): 재시도 간 대기 시간(초)

        Returns:
            tuple: (액세스 토큰, 만료 시간) 또는 실패 시 (None, None)
        """
        db_manager = DatabaseManager()
        
        # Check if we have a valid cached token
        cached_token, cached_expires_at = db_manager.get_token(token_type)
        if cached_token and cached_expires_at > datetime.utcnow():
            logging.info("Using cached %s token", token_type)
            return cached_token, cached_expires_at

        url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        body = {
            "grant_type": "client_credentials",
            "appkey": app_key,
            "appsecret": app_secret
        }

        for attempt in range(max_retries):
            try:
                response = requests.post(url, headers=headers, json=body, timeout=10)
                response.raise_for_status()
                token_data = response.json()
                
                if "access_token" in token_data:
                    access_token = token_data["access_token"]
                    expires_at = datetime.utcnow() + timedelta(seconds=token_data.get("expires_in", 86400))
                    
                    # Save the new token to the database
                    db_manager.save_token(token_type, access_token, expires_at)
                    db_manager.close()
                    
                    logging.info("Successfully obtained and cached %s token on attempt %d", token_type, attempt + 1)
                    return access_token, expires_at
                else:
                    logging.warning("Unexpected response format on attempt %d: %s", attempt + 1, token_data)
            except RequestException as e:
                logging.error("An error occurred while fetching the %s token on attempt %d: %s", token_type, attempt + 1, e)
                if attempt < max_retries - 1:
                    logging.info("Retrying in %d seconds...", retry_delay)
                    time.sleep(retry_delay)
                else:
                    logging.error("Max retries reached. Unable to obtain %s token.", token_type)
        db_manager.close()
        return None, None

    def _ensure_token(self, is_mock):
        """
        유효한 토큰이 있는지 확인하고, 필요한 경우 새 토큰을 가져옵니다.

        Args:
            is_mock (bool): 모의 거래 여부

        Returns:
            str: 유효한 액세스 토큰
        """
        now = datetime.now()
        if is_mock:
            if not self.mock_token or now >= self.mock_token_expires_at:
                self.mock_token, self.mock_token_expires_at = self._get_token(M_APP_KEY, M_APP_SECRET, "mock")
            return self.mock_token
        else:
            if not self.real_token or now >= self.real_token_expires_at:
                self.real_token, self.real_token_expires_at = self._get_token(R_APP_KEY, R_APP_SECRET, "real")
            return self.real_token

######################################################################################
###############################    헤더와 해쉬   ########################################
######################################################################################

    def _set_headers(self, is_mock=False, tr_id=None):
        """
        API 요청에 필요한 헤더를 설정합니다.

        Args:
            is_mock (bool): 모의 거래 여부
            tr_id (str, optional): 거래 ID
        """
        token = self._ensure_token(is_mock)
        self.headers["authorization"] = f"Bearer {token}"
        self.headers["appkey"] = M_APP_KEY if is_mock else R_APP_KEY
        self.headers["appsecret"] = M_APP_SECRET if is_mock else R_APP_SECRET
        if tr_id:
            self.headers["tr_id"] = tr_id
        self.headers["tr_cont"] = ""
        self.headers["custtype"] = "P"

    def _get_hashkey(self, body, is_mock=False):
        """
        주어진 요청 본문에 대한 해시 키를 생성합니다.

        Args:
            body (dict): 요청 본문

        Returns:
            str: 생성된 해시 키
        """
        if is_mock:
            url = "https://openapivts.koreainvestment.com:29443/uapi/hashkey"
            self._set_headers(is_mock=True)

        else:
            url = "https://openapi.koreainvestment.com:9443/uapi/hashkey"
            self._set_headers(is_mock=False)


        
        try:
            response = requests.post(url=url, headers=self.headers, data=json.dumps(body), timeout=10)
            response.raise_for_status()
            tmp = response.json()
            self.hashkey = tmp['HASH']
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the hash key: {e}")

######################################################################################
#########################    상한가 관련 메서드   #######################################
######################################################################################

    def get_stock_price(self, ticker):
        """
        지정된 종목의 현재 주가 정보를 가져옵니다.

        Args:
            ticker (str): 종목 코드

        Returns:
            dict: 주가 정보를 포함한 딕셔너리
        """
        self._set_headers(is_mock=False, tr_id="FHPST01010000")
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price-2"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker
        }
        response = requests.get(url=url, params=params, headers=self.headers, timeout=10)
        json_response = response.json()
        # print(json.dumps(json_response,indent=2))

        return json_response


    def get_upper_limit_stocks(self):
        """
        상한가 종목 목록을 가져옵니다.

        Returns:
            dict: 상한가 종목 정보를 포함한 딕셔너리
        """
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/capture-uplowprice"
        body = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "11300",
            "FID_PRC_CLS_CODE": "0",
            "FID_DIV_CLS_CODE": "0",
            "FID_INPUT_ISCD": "0000",
            "FID_TRGT_CLS_CODE": "",
            "FID_TRGT_EXLS_CLS_CODE": "",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": ""
        }
        self._get_hashkey(body, is_mock=False)
        self._set_headers(is_mock=False, tr_id="FHKST130000C0")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        
        upper_limit_stocks = response.json()
        return upper_limit_stocks


    def get_upAndDown_rank(self):
        """
        상승/하락 순위 정보를 가져옵니다.

        Returns:
            dict: 상승/하락 순위 정보를 포함한 딕셔너리
        """
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/ranking/fluctuation"
        body = {
            "fid_cond_mrkt_div_code":"J",
            "fid_cond_scr_div_code":"20170",
            "fid_input_iscd":"0000",
            "fid_rank_sort_cls_code":"0",
            "fid_input_cnt_1":"0",
            "fid_prc_cls_code":"0",
            "fid_input_price_1":"",
            "fid_input_price_2":"",
            "fid_vol_cnt":"",
            "fid_trgt_cls_code":"0",
            "fid_trgt_exls_cls_code":"0",
            "fid_div_cls_code":"0",
            "fid_rsfl_rate1":"18",
            "fid_rsfl_rate2":"29.5",
            "fid_input_date_1": "20240314",
            "fid_input_date_2": "20241124"
        }
        
        self._get_hashkey(body, is_mock=False)
        self._set_headers(is_mock=False, tr_id="FHPST01700000")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        
        updown = response.json()
        # print('상승 종목: ',json.dumps(updown, indent=2, ensure_ascii=False))
        return updown


    def print_korean_response(self, response):
        """
        API 응답의 한글 내용을 정리된 JSON 형식으로 출력합니다.

        Args:
            response (dict): API 응답 딕셔너리
        """
        def unicode_to_korean_converter(obj):
            return unicode_to_korean(obj) if isinstance(obj, str) else obj

        formatted_response = json.loads(json.dumps(response, default=unicode_to_korean_converter))
        print(json.dumps(formatted_response, ensure_ascii=False, indent=2))

    def get_current_price(self, ticker: str) -> tuple[int, str]:
        """
        지정된 종목의 현재 주가 정보를 가져옵니다.

        Args:
            ticker (str): 종목 코드

        Returns:
            tuple: (현재가(int), 거래정지여부(str)) 또는 (None, None) if error
        """
        try:
            stock_price_info = self.get_stock_price(ticker)
            
            if not stock_price_info or 'output' not in stock_price_info:
                print(f"주가 정보 조회 실패: {ticker}")
                return 0, "0"
            
            output = stock_price_info.get('output', {})
            price_str = output.get('stck_prpr')
            trht_yn = output.get('trht_yn')
            
            # 가격을 정수로 변환
            if price_str is not None:
                try:
                    price = int(price_str)
                    return price, trht_yn
                except (ValueError, TypeError) as e:
                    print(f"가격 변환 실패: {price_str}, 에러: {e}")
                    return 0, "0"
            else:
                print(f"가격 정보 없음: {ticker}")
                return 0, "0"
                
        except Exception as e:
            print(f"get_current_price 에러: {ticker}, {e}")
            return 0, "0"

    # def get_balance(self):
    #     """
    #     계좌 예수금 확인 및 return
    #     """
        
    #     # url="https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    #     url="https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/inquire-balance"
    #     body = {
    #         "CANO": M_ACCOUNT_NUMBER,
    #         "ACNT_PRDT_CD": "01",
    #         "AFHR_FLPR_YN": "N",
    #         "OFL_YN": "",
    #         "INQR_DVSN": "02",
    #         "UNPR_DVSN": "01",
    #         "FUND_STTL_ICLD_YN": "N",
    #         "FNCG_AMT_AUTO_RDPT_YN": "N",
    #         "PRCS_DVSN": "01",
    #         "CTX_AREA_FK100": "",
    #         "CTX_AREA_NK100": ""
    #     }
                
    #     self._get_hashkey(body, is_mock=True)
    #     self._set_headers(is_mock=True, tr_id="VTTC8434R")
    #     self.headers["hashkey"] = self.hashkey
        
    #     response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
    #     json_response = response.json()
    #     # print(json.dumps(json_response, indent=2))
    #     # print(json.dumps(json_response.get('output2')[0].get('dnca_tot_amt'), indent=2))
    #     return json_response

######################################################################################
################################    주문 메서드   ###################################
######################################################################################

    def place_order(self, ticker, quantity, order_type=None, price=None):
        """
        주식 주문을 실행합니다.

        Args:
            ticker (str): 종목 코드
            order_type (str): 주문 유형
            quantity (int): 주문 수량

            Returns:
            dict: 주문 실행 결과를 포함한 딕셔너리
        """
        
        # --- 기본 파라미터 검증 ---
        if not ticker or quantity is None:
            logging.error("[place_order] 잘못된 주문 파라미터: ticker=%s, quantity=%s", ticker, quantity)
            return {"rt_cd": "1", "msg_cd": "00010000", "msg1": "잘못된 주문 파라미터"}

        quantity = int(quantity)
        if quantity <= 0:
            logging.warning("[place_order] 주문 수량이 0 이하입니다. ticker=%s", ticker)
            return {"rt_cd": "1", "msg_cd": "00010001", "msg1": "주문 수량이 0 이하입니다."}

        # --- 거래 ID 설정 ---
        tr_id_code = "VTTC0802U" if order_type == 'buy' else "VTTC0801U"

        # --- 시장·지정가 구분 및 가격 선정 ---
        # price 가 None 이면 시장가, 지정가면 입력값 사용
        if price is None:
            price_to_use, _ = self.get_current_price(ticker)
        else:
            price_to_use = price

        # --- 매수 시 주문가능금액 기반 수량 조정 ---
        if order_type == 'buy':
            try:
                available_cash = self.get_available_cash()
                if available_cash <= 0:
                    logging.warning("[place_order] 주문가능금액 없음 – 주문 건너뜀")
                    return {"rt_cd": "1", "msg_cd": "40250000", "msg1": "모의투자 주문가능금액이 부족합니다."}

                if price_to_use and price_to_use > 0:
                    max_qty = int((available_cash * 0.99) // price_to_use)  # 1% 여유 확보
                    if max_qty <= 0:
                        return {"rt_cd": "1", "msg_cd": "40250000", "msg1": "모의투자 주문가능금액이 부족합니다."}
                    if quantity > max_qty:
                        logging.info("[place_order] 주문 수량 %s → %s (available_cash=%s)", quantity, max_qty, available_cash)
                        quantity = max_qty
            except Exception as e:
                logging.error("[place_order] available cash check 실패: %s", e)

        # --- (선택적) 매도 시 보유 수량 초과 방지는 호출 측에서 수행한다. ---


        data = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "PDNO": ticker,
            "ORD_DVSN": "01" if price is None else "00",  # 01: 시장가, 00: 지정가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0" if price is None else str(price),
        }
        # hashkey 생성 및 헤더 설정
        self._get_hashkey(data, is_mock=True)
        self._set_headers(is_mock=True, tr_id=tr_id_code)
        self.headers["hashkey"] = self.hashkey
        url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash"

        for attempt in range(1, 4):
            try:
                with KISApi._global_api_lock:
                    response = requests.post(url=url, data=json.dumps(data), headers=self.headers, timeout=10)
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                logging.error("[place_order] API 호출 실패(%s/3): %s", attempt, e)
                if attempt < 3:
                    time.sleep(1)
                    continue
                return {"rt_cd": "1", "msg_cd": "50000000", "msg1": f"API 호출 실패: {e}"}


    def sell_order(self, ticker, quantity, price=None):
        """
        주식 매도 주문을 실행합니다.

        Args:
            ticker (str): 종목 코드
            quantity (int): 매도 수량
            price (float, optional): 매도 희망 가격. None이면 시장가 주문

        Returns:
            dict: 주문 실행 결과를 포함한 딕셔너리
        """
        data = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "PDNO": ticker,
            "ORD_DVSN": "01" if price is None else "00",  # 01: 시장가, 00: 지정가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0" if price is None else str(price),
        }
        # hashkey 생성 및 헤더 설정
        self._get_hashkey(data, is_mock=True)
        self._set_headers(is_mock=True, tr_id="VTTC0801U")  # 매도 거래 ID
        self.headers["hashkey"] = self.hashkey
        url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash"

        for attempt in range(1, 4):
            try:
                with KISApi._global_api_lock:
                    response = requests.post(url=url, data=json.dumps(data), headers=self.headers, timeout=10)
                response.raise_for_status()
                return response.json()
            except RequestException as e:
                logging.error("[sell_order] API 호출 실패(%s/3): %s", attempt, e)
                if attempt < 3:
                    time.sleep(1)
                    continue
                return {"rt_cd": "1", "msg_cd": "50000000", "msg1": f"API 호출 실패: {e}"}


    def cancel_order(self, order_num):
        """
        주문취소 API
        """
        
        # 주문번호는 8자리로 맞춰야 함
        order_num = str(order_num).zfill(8)
        
        url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-rvsecncl"
        body = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "KRX_FWDG_ORD_ORGNO": "",
            "ORGN_ODNO": order_num,
            "ORD_DVSN": "01",
            "RVSE_CNCL_DVSN_CD": "02", # 취소주문
            "ORD_QTY": "0",
            "ORD_UNPR": "0",
            "QTY_ALL_ORD_YN": "Y"
        }

        self._get_hashkey(body, is_mock=True)
        self._set_headers(is_mock=True, tr_id="VTTC0803U")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.post(url=url, headers=self.headers, json=body, timeout=10)
        json_response = response.json()
        
        return json_response


    def revise_order(self, order_num, order_price, quantity):
        """
        주문취소 API
        """
        print("revise_order:- ",order_num, "주문정정 실행")
        
        # 주문번호는 8자리로 맞춰야 함
        order_num = str(order_num).zfill(8)
        
        url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-rvsecncl"
        body = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "KRX_FWDG_ORD_ORGNO": "00950",
            "ORGN_ODNO": order_num,
            "ORD_DVSN": "01",
            "RVSE_CNCL_DVSN_CD": "01", # 01:정정, 02:취소
            "ORD_QTY": quantity,
            "ORD_UNPR": order_price,
            "QTY_ALL_ORD_YN": "Y",
            "ALGO_NO": ""
        }
        self._get_hashkey(body, is_mock=True)
        self._set_headers(is_mock=True, tr_id="VTTC0803U")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.post(url=url, headers=self.headers, json=body, timeout=10)
        json_response = response.json()
        
        return json_response


    def purchase_availability_inquiry(self, ticker=None):
        """
        주문가능조회
        """
        url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/inquire-psbl-order"
        body = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "PDNO": "" if ticker is None else ticker,
            "ORD_UNPR": "",
            "ORD_DVSN": "01",
            "CMA_EVLU_AMT_ICLD_YN": "N",
            "OVRS_ICLD_YN": "N"
        }
        
        self._get_hashkey(body, is_mock=True)
        self._set_headers(is_mock=True, tr_id="VTTC8908R")
        self.headers["hashkey"] = self.hashkey

        response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        json_response = response.json()
        
        return json_response
    
    def get_available_cash(self):
        """현재 주문가능 현금(모의투자 서버 기준)을 정수로 반환합니다. 실패 시 0."""
        try:
            resp = self.purchase_availability_inquiry()
            if not resp:
                return 0
            target = None
            if 'output' in resp and resp['output']:
                target = resp['output'][0] if isinstance(resp['output'], list) else resp['output']
            elif 'output1' in resp and resp['output1']:
                target = resp['output1'][0] if isinstance(resp['output1'], list) else resp['output1']
            if target:
                cash_str = target.get('ord_psbl_cash') or target.get('ord_psbl_cash_amt') or '0'
                return int(str(cash_str).replace(',', ''))
        except Exception as e:
            logging.error("get_available_cash error: %s", e)
        return 0

######################################################################################
################################    잔고 메서드   ###################################
######################################################################################

    def daily_order_execution_inquiry(self, order_num):
        """
        주식일별주문체결조회
        """
        today = datetime.now()
        formatted_date = today.strftime('%Y%m%d')
        # url="https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        url="https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        body = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "INQR_STRT_DT": formatted_date,
            "INQR_END_DT": formatted_date,
            "UNPR_DVSN": "01",
            "SLL_BUY_DVSN_CD": "00",
            "INQR_DVSN": "00",
            "PDNO": "", # 상품번호 ticker, 공란 - 전체조회
            "CCLD_DVSN": "00",
            "ORD_GNO_BRNO": "",
            "ODNO": order_num,
            "INQR_DVSN_3": "00",
            "INQR_DVSN_1": "",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        self._get_hashkey(body, is_mock=True)
        self._set_headers(is_mock=True, tr_id="VTTC8001R")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        response_json = response.json()        
        # print(json.dumps(response_json, indent=2, ensure_ascii=False))

        return response_json





    def balance_inquiry(self, max_retries: int = 3, retry_delay: float = 1.0):

        # url="https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-daily-ccld"
        url="https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/inquire-balance"
        body = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N", 
            "PRCS_DVSN": "00",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
                
        # 전역 락을 사용하여 동시 호출 방지
        with KISApi._global_api_lock:
            self._get_hashkey(body, is_mock=True)
            self._set_headers(is_mock=True, tr_id="VTTC8434R")
            local_headers = self.headers.copy()
            local_headers["hashkey"] = self.hashkey
            response = requests.get(url=url, headers=local_headers, params=body, timeout=10)
        json_response = response.json()
        output1 = json_response.get("output1")

        # List[Dict] -> Dict 반환
        return output1

    

######################################################################################
################################    종목 조회   ###################################
######################################################################################

    def get_volume_rank(self):
        """ 거래량 상위 종목 조회 """
        self._set_headers(tr_id="FHPST01710000")
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/volume-rank"
        body = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_COND_SCR_DIV_CODE": "20171",
            "FID_INPUT_ISCD": "0000",
            "FID_DIV_CLS_CODE": "0",
            "FID_BLNG_CLS_CODE": "0",
            "FID_TRGT_CLS_CODE": "111111111",
            "FID_TRGT_EXLS_CLS_CODE": "000000",
            "FID_INPUT_PRICE_1": "",
            "FID_INPUT_PRICE_2": "",
            "FID_VOL_CNT": "",
            "FID_INPUT_DATE_1": ""
        }

        response = requests.get(url=url, params=body, headers=self.headers, timeout=10)
        response.raise_for_status()
        response_json = response.json()
        # print(json.dumps(response_json, indent=2, ensure_ascii=False))
        
        return response_json

    
    
    def get_stock_volume(self, ticker, days=3):
        """
        지정된 종목의 최근 n일간의 거래량을 가져옵니다.
        
        Args:
            ticker (str): 종목 코드
            days (int): 조회할 일 수 (기본값: 3)
        
        Returns:
            list: 최근 n일간의 거래량 리스트 (최신 날짜부터 과거 순으로)
        """
        
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-daily-price"
        body = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "0",
            # "ST_DATE": start_date,
            # "END_DATE": end_date
        }
        self._get_hashkey(body, is_mock=False)
        self._set_headers(is_mock=False, tr_id="FHKST01010400")
        
        response = requests.get(url=url, params=body, headers=self.headers, timeout=10)
        json_response = response.json()
        
        # print(json.dumps(json_response, indent=2, e_ascii=False))
        
        volumes = []
        for item in json_response.get('output', []):
            volumes.append(int(item.get('acml_vol', '0')))
        
        return volumes[:days]  # 최근 n일간의 거래량만 반환
    
    def compare_volumes(self, volumes):
        """
        3일간의 거래량을 비교하고 차이를 백분율로 계산합니다.
        
        Args:
            volumes (list): 3일간의 거래량 리스트 (최신 날짜부터 과거 순으로)
        
        Returns:
            tuple: (1일 전과 2일 전의 차이 백분율, 2일 전과 3일 전의 차이 백분율)
        """
        if len(volumes) != 3:
            raise ValueError("3일간의 거래량 데이터가 필요합니다.")
        
        day1, day2, day3 = volumes
        
        diff_1_2 = ((day1 - day2) / day2) * 100 if day2 != 0 else float('inf')
        diff_2_3 = ((day2 - day3) / day3) * 100 if day3 != 0 else float('inf')
        
        return round(diff_1_2, 2), round(diff_2_3, 2)
    
    def get_basic_stock_info(self, ticker):
        self._set_headers(tr_id="FHPST01710000")
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/search-stock-info"
        body = {
            "PRDT_TYPE_CD": "300",
            "PDNO": ticker
        }

        self._get_hashkey(body, is_mock=False)
        self._set_headers(is_mock=False, tr_id="CTPF1002R")
        self.headers["hashkey"] = self.hashkey

        response = requests.get(url=url, params=body, headers=self.headers, timeout=10)
        response.raise_for_status()
        response_json = response.json()
        # print(json.dumps(response_json, indent=2, ensure_ascii=False))
        
        return response_json
