"""
Trading Bot Main Module

This module retrieves an access token from the API using app key and secret.
"""
import json
import requests
from config import M_APP_KEY, M_APP_SECRET, R_APP_KEY, R_APP_SECRET, ACCOUNT_NUMBER
from datetime import datetime, timedelta

class TradingBot:
    def __init__(self, r_api_key, r_api_secret, m_api_key, m_api_secret,account_number):
        self.r_api_key = r_api_key
        self.r_api_secret = r_api_secret
        self.m_api_key = m_api_key
        self.m_api_secret = m_api_secret
        self.hashkey = None
        self.account_number = account_number
        self.access_token = None
        self.websocket_key = None
        self.upper_limit_stocks = {}
        self.watchlist = set()
        
    def get_access_token(self):
        """
        Retrieve an access token from the API using app key and secret.
        """
        
        url = "https://openapi.koreainvestment.com:9443/oauth2/tokenP"
        headers = {"Content-Type": "application/json"}

        body = {
            "grant_type": "client_credentials",
            "appkey": R_APP_KEY,
            "appsecret": R_APP_SECRET
        }
        try:
            response = requests.post(url, headers=headers, data=json.dumps(body), timeout=10)
            response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
            token_data = response.json()
            
            if "access_token" in token_data:
                self.access_token = token_data["access_token"]
                return None
            else:
                print(f"Unexpected response format: {token_data}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the access token: {e}")
            return None
    
    def get_hashkey(self, body):
        url = "https://openapi.koreainvestment.com:9443/uapi/hashkey"
        headers = {
            "content-type": "application/json; charset=utf-8",
            "appkey": R_APP_KEY,
            "appsecret": R_APP_SECRET
        }
        
        try:
            res = requests.post(url=url, headers=headers, data=json.dumps(body), timeout=10)
            res.raise_for_status()
            tmp = res.json()
            self.hashkey = tmp['HASH']
            return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the hash key: {e}")
            return None
        
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
        print('여기서부터 해시키', self.hashkey)
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.r_api_key,
            "appsecret": self.r_api_secret,
            "tr_id": "FHKST130000C0",
            "tr_cont": "",
            "custtype": "P",
            "hashkey": self.hashkey
        }
        
        res = requests.get(url=url, headers=headers, params=body, timeout=10)  # data를 params로 변경
        
        print(f"Response status code: {res.status_code}")
        print(f"Response headers: {res.headers}")
        
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
        headers = {
            "content-type": "application/json; charset=utf-8",
            "authorization": f"Bearer {self.access_token}",
            "appkey": self.r_api_key,
            "appsecret": self.r_api_secret,
            "tr_id": "FHPST01700000",
            "tr_cont": "",
            "custtype": "P",
            "hashkey": self.hashkey
        }
        
        res = requests.get(url=url, headers=headers, params=body, timeout=10)
        print("res status_code:", res.status_code )
        print("res headers:", res.headers )

        
        updown = res.json()
        return updown
    # def get_upper_limit_stocks(self, days_ago=0):
        
    #     url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/capture-uplowprice"
        
    #     target_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
        
    #     body = {
    #         "FID_COND_MRKT_DIV_CODE": "J",
    #         "FID_COND_SCR_DIV_CODE": "11300",
    #         "FID_PRC_CLS_CODE": "0",
    #         "FID_DIV_CLS_CODE": "3",
    #         "FID_INPUT_ISCD": "0000",
    #         "FID_INPUT_DATE_1": target_date,
    #         "FID_INPUT_DATE_2": target_date,  # 종료 날짜 추가
    #         "FID_VOL_CNT": "0",  # 거래량 조건 제거
    #         "FID_INPUT_PRICE_1": "0",  # 가격 조건 제거
    #         "FID_INPUT_PRICE_2": "0",  # 가격 조건 제거
    #     }
    #     self.get_hashkey(body)
        
    #     headers = {
    #         "content-type": "application/json",
    #         "authorization": f"Bearer {self.access_token}",
    #         "appkey": self.r_api_key,
    #         "appsecret": self.r_api_secret,
    #         "tr_id": "FHKST130000C0",  # TR ID 변경
    #         "custtype": "P",
    #         "hashkey": self.hashkey
    #     }
        
    #     print(f"Request URL: {url}")
    #     print(f"Request Headers: {json.dumps(headers, indent=2)}")
    #     print(f"Request Body: {json.dumps(body, indent=2)}")
        
    #     try:
    #         res = requests.get(url=url, headers=headers, params=body, timeout=10)
    #         print(f"Response Status Code: {res.status_code}")
    #         print(f"Response Headers: {json.dumps(dict(res.headers), indent=2)}")
    #         print(f"Response Content: {res.text[:1000]}...")  # 응답 내용 더 많이 출력
            
    #         res.raise_for_status()
            
    #         if res.text:
    #             return res.json()
    #         else:
    #             print("The response is empty.")
    #             return None
    #     except requests.exceptions.RequestException as e:
    #         print(f"An error occurred while fetching the upper limit stocks: {e}")
    #         if hasattr(e, 'response'):
    #             print(f"Error Response Status Code: {e.response.status_code}")
    #             print(f"Error Response Content: {e.response.text[:1000]}...")
    #         return None

if __name__=="__main__":
    tradingBot = TradingBot(R_APP_KEY, R_APP_SECRET, M_APP_KEY, M_APP_SECRET, ACCOUNT_NUMBER)
    tradingBot.get_access_token()
    # 3일 전 상한가 조회
    # stocks = tradingBot.get_upper_limit_stocks(days_ago=3)
    stocks = tradingBot.get_upper_limit_stocks()
    if stocks:
        print(f"Upper Limit Stocks: {json.dumps(stocks, indent=2)}")
        
    # test = tradingBot.get_upAndDown_rank()
    # print(test)
    # print(f"TEST: {json.dumps(test, indent=2)}")