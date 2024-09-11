import json
import requests
import logging
from utils.string_utils import unicode_to_korean
from config.config import *
from datetime import datetime, timedelta
from database.db_manager import DatabaseManager

class KISApi:
    def __init__(self):
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
        # Check if we have a valid cached token
        cached_token, cached_expires_at = self.db_manager.get_token(token_type)
        if cached_token and cached_expires_at > datetime.utcnow():
            logging.info(f"Using cached {token_type} token")
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
                    
                    logging.info(f"Successfully obtained and cached {token_type} token on attempt {attempt + 1}")
                    return access_token, expires_at
                else:
                    logging.warning(f"Unexpected response format on attempt {attempt + 1}: {token_data}")
            except RequestException as e:
                logging.error(f"An error occurred while fetching the {token_type} token on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    logging.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logging.error(f"Max retries reached. Unable to obtain {token_type} token.")
        
        return None, None

    def _ensure_token(self, is_mock):
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

    def _set_headers(self, is_mock=False, tr_id=None):
        token = self._ensure_token(is_mock)
        self.headers["authorization"] = f"Bearer {token}"
        self.headers["appkey"] = M_APP_KEY if is_mock else R_APP_KEY
        self.headers["appsecret"] = M_APP_SECRET if is_mock else R_APP_SECRET
        if tr_id:
            self.headers["tr_id"] = tr_id
        self.headers["tr_cont"] = ""
        self.headers["custtype"] = "P"

    def get_hashkey(self, body):
        url = "https://openapi.koreainvestment.com:9443/uapi/hashkey"
        try:
            res = requests.post(url=url, headers=self.headers, data=json.dumps(body), timeout=10)
            res.raise_for_status()
            tmp = res.json()
            self.hashkey = tmp['HASH']
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the hash key: {e}")

    def get_stock_price(self, ticker):
        self._set_headers(is_mock=False)
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": ticker
        }
        json_response = response.json()
        response = requests.get(TICKER_URL, params=params, headers=self.headers)
        
        for key, value in json_response.items():
            if isinstance(value, str):
                json_response[key] = unicode_to_korean(value)
        
        return json_response

    def place_order(self, ticker, order_type, quantity):
        self._set_headers(is_mock=True)
        data = {
            "CANO": M_ACCOUNT_NUMBER,
            "ACNT_PRDT_CD": "01",
            "PDNO": ticker,
            "ORD_DVSN": order_type,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": "0",
        }
        response = requests.post(TRADE_URL, data=json.dumps(data), headers=self.headers)
        json_response = response.json()
        
        for key, value in json_response.items():
            if isinstance(value, str):
                json_response[key] = unicode_to_korean(value)
        
        return json_response

    def get_upper_limit_stocks(self):
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
        self._set_headers(is_mock=False, tr_id="FHKST130000C0")
        self.headers["hashkey"] = self.hashkey
        
        res = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        
        upper_limit_stocks = res.json()
        return upper_limit_stocks

    def get_upAndDown_rank(self):
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
        self._set_headers(is_mock=False, tr_id="FHPST01700000")
        self.headers["hashkey"] = self.hashkey
        
        res = requests.get(url=url, headers=self.headers, params=body, timeout=10)
        print("res status_code:", res.status_code )
        print("res headers:", res.headers )
        
        updown = res.json()
        return updown

    def print_korean_response(self, response):
        for key, value in response.items():
            if isinstance(value, str):
                print(f"{key}: {unicode_to_korean(value)}")
            else:
                print(f"{key}: {value}")