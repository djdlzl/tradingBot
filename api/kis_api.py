import json
import requests
import logging
from requests.exceptions import RequestException
from utils.string_utils import unicode_to_korean
from config.config import R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET, M_ACCOUNT_NUMBER
from datetime import datetime, timedelta
from database.db_manager import DatabaseManager
import time


class KISApi:
    """한국투자증권 API와 상호작용하기 위한 클래스입니다."""

    def __init__(self):
        """KISApi 클래스의 인스턴스를 초기화합니다."""
        self.headers = {"content-type": "application/json; charset=utf-8"}
        self.w_headers = {"content-type": "utf-8"}
        self.db_manager = DatabaseManager()
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
        # Check if we have a valid cached token
        cached_token, cached_expires_at = self.db_manager.get_token(token_type)
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
                    self.db_manager.save_token(token_type, access_token, expires_at)
                    
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
        
    def _get_approval(self, app_key, app_secret, approval_type, max_retries=3, retry_delay=5):
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

    def _ensure_approval(self, is_mock):
        """
        유효한 웹소켓 인증키가 있는지 확인하고, 필요한 경우 새 인증키를 가져옵니다.

        Args:
            is_mock (bool): 모의 거래 여부

        Returns:
            str: 유효한 액세스 인증키
        """
        print("##########is_mock: ", is_mock)
        now = datetime.now()
        if is_mock:
            if not self.mock_approval or now >= self.mock_approval_expires_at:
                self.mock_approval, self.mock_approval_expires_at = self._get_approval(M_APP_KEY, M_APP_SECRET, "mock")
            return self.mock_approval
        else:
            if not self.real_approval or now >= self.real_approval_expires_at:
                self.real_approval, self.real_approval_expires_at = self._get_approval(R_APP_KEY, R_APP_SECRET, "real")
            return self.real_approval

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
        else:
            url = "https://openapi.koreainvestment.com:9443/uapi/hashkey"
            # 모의 거래와 실제 거래에 따라 헤더 설정
        self._set_headers(is_mock=True, tr_id="VTTC8908R")

        
        try:
            response = requests.post(url=url, headers=self.headers, data=json.dumps(body), timeout=10)
            response.raise_for_status()
            tmp = response.json()
            self.hashkey = tmp['HASH']
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the hash key: {e}")

    def _set_w_headers(self, is_mock=False, tr_id=None):
        """
        API 요청에 필요한 헤더를 설정합니다.

        Args:
            is_mock (bool): 모의 거래 여부
            tr_id (str, optional): 거래 ID
        """
        approval_key = self._ensure_approval(is_mock)
        self.w_headers["approval_key"] = approval_key
        self.w_headers["appkey"] = M_APP_KEY if is_mock else R_APP_KEY
        self.w_headers["appsecret"] = M_APP_SECRET if is_mock else R_APP_SECRET
        if tr_id:
            self.w_headers["tr_id"] = tr_id
        self.w_headers["tr_type"] = "1"
        self.w_headers["custtype"] = "P"

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
        self._set_headers(is_mock=False, tr_id="FHKST01010100")
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/inquire-price"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker
        }
        response = requests.get(url=url, params=params, headers=self.headers, timeout=10)
        json_response = response.json()
        
        for key, value in json_response.items():
            if isinstance(value, str):
                json_response[key] = unicode_to_korean(value)
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
            "fid_rsfl_rate1":"",
            "fid_rsfl_rate2":""
        }
        
        self._get_hashkey(body, is_mock=False)
        self._set_headers(is_mock=False, tr_id="FHPST01700000")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        print("response status_code:", response.status_code )
        print("response headers:", response.headers )
        
        updown = response.json()
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

    def get_current_price(self, ticker):
        """
        지정된 종목의 현재 주가 정보를 가져옵니다.

        Args:
            ticker (str): 종목 코드

        Returns:
            float: 현재 주가
        """
        stock_price_info = self.get_stock_price(ticker)

        return stock_price_info['output']['stck_prpr'], stock_price_info['output']['temp_stop_yn']  # 현재가 반환, 기본값은 0
    
    # def get_my_cash(self):
    #     """
    #     계좌 잔고 현금 확인 및 return
    #     """
        
    #     # url="https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    #     url="https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/inquire-psbl-order"
    #     body = {
    #         "CANO": M_ACCOUNT_NUMBER,
    #         "ACNT_PRDT_CD": "01",
    #         "PDNO": "",
    #         "ORD_UNPR": "",
    #         "ORD_DVSN": "01",
    #         "CMA_EVLU_AMT_ICLD_YN": "N",
    #         "OVRS_ICLD_YN": "N"
    #     }
                
    #     self._get_hashkey(body, is_mock=True)
    #     self._set_headers(is_mock=True, tr_id="VTTC8908R")
    #     self.headers["hashkey"] = self.hashkey
        
    #     response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
    #     json_response = response.json()
    #     # print(json.dumps(json_response, indent=2))
    #     return json_response.get('')
        
    def get_balance(self):
        """
        계좌 예수금 확인 및 return
        """
        
        # url="https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/trading/inquire-psbl-order"
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
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": ""
        }
                
        self._get_hashkey(body, is_mock=True)
        self._set_headers(is_mock=True, tr_id="VTTC8434R")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        json_response = response.json()
        # print(json.dumps(json_response, indent=2))
        # print(json.dumps(json_response.get('output2')[0].get('dnca_tot_amt'), indent=2))
        return json_response

######################################################################################
################################    주문 메서드   ###################################
######################################################################################

    def place_order(self, ticker, quantity):
        """
        주식 주문을 실행합니다.

        Args:
            ticker (str): 종목 코드
            order_type (str): 주문 유형
            quantity (int): 주문 수량

            Returns:
            dict: 주문 실행 결과를 포함한 딕셔너리
        """
        self._set_headers(is_mock=True, tr_id="VTTC0802U")
        url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash"
        data = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "PDNO": ticker,
            "ORD_DVSN": "01",
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0",
        }
        self.headers["hashkey"] = None

        response = requests.post(url=url, data=json.dumps(data), headers=self.headers, timeout=10)
        json_response = response.json()

        return json_response

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
        self._set_headers(is_mock=True, tr_id="VTTC0801U")  # 매도 거래 ID
        url = "https://openapivts.koreainvestment.com:29443/uapi/domestic-stock/v1/trading/order-cash"
        
        data = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "PDNO": ticker,
            "ORD_DVSN": "01" if price is None else "00",  # 01: 시장가, 00: 지정가
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0" if price is None else str(price),
        }
        self.headers["hashkey"] = None

        response = requests.post(url=url, data=json.dumps(data), headers=self.headers, timeout=10)
        json_response = response.json()

        return json_response


######################################################################################
################################    잔고 메서드   ###################################
######################################################################################

    def daily_order_execution_inquiry(self, order_num):
        """
        사용한 자금 조회
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
            "SLL_BUY_DVSN_CD": "02",
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
        json_response = response.json()
        
        # print("##########select_spent_fund:  ",json.dumps(json_response, indent=2))
        print(json_response.get('output1')[0].get('tot_ccld_amt'))
        return json_response
    
    def balance_inquiry(self):

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
                
        self._get_hashkey(body, is_mock=True)
        self._set_headers(is_mock=True, tr_id="VTTC8434R")
        self.headers["hashkey"] = self.hashkey
        
        response = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        json_response = response.json()
        
        # print("##########select_spent_fund:  ",json.dumps(json_response, indent=2))
        print(json.dumps(json_response.get("output1"), indent=2))
        index_of_odno = next((index for index, d in enumerate(json_response.get("output1")) if d.get('pdno') == "095700"), -1)
        avr_price = json_response.get("output1")[index_of_odno].get("pchs_avg_pric")
        print("update_session - avr_price", avr_price)
        return json_response.get("output1")
    