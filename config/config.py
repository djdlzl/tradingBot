"""
중요데이터 설정 파일
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

# Database - sqlite3
DB_NAME = "quant_trading.db"
# Database - mariadb
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),      # 데이터베이스 서버 주소
    'user': os.getenv('DB_USER'),         # 데이터베이스 사용자 이름
    'password': os.getenv('DB_PASS'),  # 데이터베이스 비밀번호
    'database': os.getenv('DB_NAME'),  # 사용할 데이터베이스 이름
    'port': os.getenv('DB_PORT'),            # MariaDB 기본 포트
    'auth_plugin': 'mysql_native_password',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_general_ci'
}


# Slack
SLACK_TOKEN = os.getenv('SLACK_TOKEN')