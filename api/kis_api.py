import json
import requests
import logging
from requests.exceptions import RequestException
from utils.string_utils import unicode_to_korean
from config.config import R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET, M_ACCOUNT_NUMBER, TICKER_URL, TRADE_URL
from datetime import datetime, timedelta, date
from database.db_manager import DatabaseManager
import time
from utils.date_utils import DateUtils


class KISApi:
    """한국투자증권 API와 상호작용하기 위한 클래스입니다."""

    def __init__(self):
        """KISApi 클래스의 인스턴스를 초기화합니다."""
        self.headers = {"content-type": "application/json; charset=utf-8"}
        self.db_manager = DatabaseManager()
        self.real_token = None
        self.mock_token = None
        self.real_token_expires_at = None
        self.mock_token_expires_at = None
        self.hashkey = None
        self.upper_limit_stocks = {}
        self.watchlist = set()

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
                print(self.real_token_expires_at)
            return self.real_token

    def set_headers(self, is_mock=False, tr_id=None):
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

    def get_hashkey(self, body):
        """
        주어진 요청 본문에 대한 해시 키를 생성합니다.

        Args:
            body (dict): 요청 본문

        Returns:
            str: 생성된 해시 키
        """
        url = "https://openapi.koreainvestment.com:9443/uapi/hashkey"
        try:
            res = requests.post(url=url, headers=self.headers, data=json.dumps(body), timeout=10)
            res.raise_for_status()
            tmp = res.json()
            self.hashkey = tmp['HASH']
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the hash key: {e}")

    def get_stock_price(self, ticker):
        """
        지정된 종목의 현재 주가 정보를 가져옵니다.

        Args:
            ticker (str): 종목 코드

        Returns:
            dict: 주가 정보를 포함한 딕셔너리
        """
        self.set_headers(is_mock=False)
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker
        }
        response = requests.get(TICKER_URL, params=params, headers=self.headers, timeout=10)
        json_response = response.json()
        
        for key, value in json_response.items():
            if isinstance(value, str):
                json_response[key] = unicode_to_korean(value)
        
        return json_response

    def place_order(self, ticker, order_type, quantity):
        """
        주식 주문을 실행합니다.

        Args:
            ticker (str): 종목 코드
            order_type (str): 주문 유형
            quantity (int): 주문 수량

        Returns:
            dict: 주문 실행 결과를 포함한 딕셔너리
        """
        self.set_headers(is_mock=True)
        data = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "PDNO": ticker,
            "ORD_DVSN": order_type,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0",
        }
        response = requests.post(TRADE_URL, data=json.dumps(data), headers=self.headers, timeout=10)
        json_response = response.json()
        
        for key, value in json_response.items():
            if isinstance(value, str):
                json_response[key] = unicode_to_korean(value)
        
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
        self.get_hashkey(body)
        self.set_headers(is_mock=False, tr_id="FHKST130000C0")
        self.headers["hashkey"] = self.hashkey
        
        res = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        
        upper_limit_stocks = res.json()
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
        
        self.get_hashkey(body)
        self.set_headers(is_mock=False, tr_id="FHPST01700000")
        self.headers["hashkey"] = self.hashkey
        
        res = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        print("res status_code:", res.status_code )
        print("res headers:", res.headers )
        
        updown = res.json()
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
        
    def fetch_and_save_previous_upper_limit_stocks(self):
        upper_limit_stocks = self.get_upper_limit_stocks()
        if upper_limit_stocks:
            print("Upper Limit Stocks:")
            self.print_korean_response(upper_limit_stocks)
            
            # 상한가 종목 정보 추출
            stocks_info = [(stock['mksc_shrn_iscd'], stock['hts_kor_isnm'], stock['stck_prpr'], stock['prdy_ctrt']) 
                        for stock in upper_limit_stocks['output']]
            
            # 영업일 기준 날짜 가져오기
            today = date.today()
            if not DateUtils.is_business_day(today):
                today = DateUtils.get_previous_business_day(today)
            # 데이터베이스에 저장
            db = DatabaseManager()
            db.save_upper_limit_stocks(today.isoformat(), stocks_info)
            db.close()