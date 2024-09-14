"""
전체 설정 파일
"""
import os
from dotenv import load_dotenv

load_dotenv()

# API Keys
R_APP_KEY = os.getenv('R_APP_KEY')
R_APP_SECRET = os.getenv('R_APP_SECRET')
M_APP_KEY = os.getenv('M_APP_KEY')
M_APP_SECRET = os.getenv('M_APP_SECRET')

# Account Numbers
R_ACCOUNT_NUMBER = os.getenv('R_ACCOUNT_NUMBER')
M_ACCOUNT_NUMBER = os.getenv('M_ACCOUNT_NUMBER')

# API URLs
BASE_URL = "https://openapi.koreainvestment.com:9443"
TICKER_URL = f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price"
TRADE_URL = f"{BASE_URL}/uapi/domestic-stock/v1/trading/order-cash"

# Database
DB_NAME = "quant_trading.db"

# Trading parameters
MAX_PRICE_DROP_PERCENT = -8
