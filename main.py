"""
Trading Bot Main Module

This module retrieves an access token from the API using app key and secret.
"""
import json
import requests
from config import M_APP_KEY, M_APP_SECRET, R_APP_KEY, R_APP_SECRET, ACCOUNT_NUMBER
from datetime import datetime, timedelta

class TradingBot:
    def __init__(self, api_key, api_secret, account_number):
        self.api_key = api_key
        self.api_secret = api_secret
        self.account_number = account_number
        self.access_token = None
        self.websocket_key = None
        self.upper_limit_stocks = {}
        self.watchlist = set()

    def get_access_token(self):
        """
        Retrieve an access token from the API using app key and secret.
        """
        
        url = "https://openapivts.koreainvestment.com:29443/oauth2/tokenP"
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
                return token_data["access_token"]
            else:
                print(f"Unexpected response format: {token_data}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching the access token: {e}")
            return None

    def get_upper_limit_stocks():
        url = "https://openapi.koreainvestment.com:9443/uapi/domestic-stock/v1/quotations/psearch-result"
        headers = {
            "content-type": "application/json; charset=utf-8"
        }


if __name__=="__main__":
    access_token = get_access_token()
    print(access_token)
    