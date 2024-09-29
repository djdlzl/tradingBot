"""
데이터베이스 관리를 위한 DatabaseManager 클래스를 정의합니다.

이 모듈은 SQLite 데이터베이스를 사용하여 트레이딩 봇의 데이터를 저장하고 관리합니다.
주요 기능으로는 토큰 정보 저장 및 조회, 상한가 주식 정보 저장 및 조회 등이 있습니다.

클래스:
    DatabaseManager: 데이터베이스 연결 및 쿼리 실행을 관리합니다.

의존성:
    - sqlite3: SQLite 데이터베이스 연동
    - logging: 로그 기록
    - datetime: 날짜 및 시간 처리
    - config.config: 데이터베이스 설정 정보

사용 예:
    db = DatabaseManager()
    db.save_token("access", "token123", datetime.now())
    db.insert_upper_limit_stock("2023-05-01", "005930", "삼성전자", 70000)
    db.close()
"""

import sqlite3
import logging
from datetime import datetime
from config.config import DB_NAME
from utils.date_utils import DateUtils

class DatabaseManager:
    """
    Database 관리용 클래스
    """
    def __init__(self):
        """
        DatabaseManager 클래스의 생성자.
        데이터베이스 연결을 초기화하고 필요한 테이블을 생성합니다.
        """
        self.conn = sqlite3.connect(DB_NAME)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        """
        필요한 데이터베이스 테이블을 생성합니다.
        upper_limit_stocks와 tokens 테이블을 만듭니다.
        """
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS upper_limit_stocks (
                date TEXT,
                ticker TEXT,
                name TEXT,
                price REAL,
                upper_rate REAL,
                PRIMARY KEY (date, ticker)
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                token_type TEXT PRIMARY KEY,
                access_token TEXT,
                expires_at TEXT
            )
        ''')
        
        ### 테이블 삭제
        # self.cursor.execute('''
        # DROP TABLE IF EXISTS upper_limit_history
        # ''')
        
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trading_session (
                date TEXT,
                ticker TEXT,
                name TEXT,
                name TEXT,
                PRIMARY KEY (date, name)
            )
        ''')
        
        self.conn.commit()


    def save_token(self, token_type, access_token, expires_at):
        """
        토큰 정보를 데이터베이스에 저장합니다.

        :param token_type: 토큰 유형
        :param access_token: 액세스 토큰
        :param expires_at: 토큰 만료 시간
        """
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO tokens (token_type, access_token, expires_at)
            VALUES (?, ?, ?)
            ''', (token_type, access_token, expires_at.isoformat()))
            self.conn.commit()
            logging.info("Token saved successfully: %s", token_type)
        except sqlite3.Error as e:
            logging.error("Error saving token: %s", e)
            raise

    def get_token(self, token_type):
        """
        지정된 유형의 토큰 정보를 데이터베이스에서 조회합니다.

        :param token_type: 조회할 토큰 유형
        :return: (액세스 토큰, 만료 시간) 튜플 또는 (None, None)
        """
        try:
            self.cursor.execute('SELECT access_token, expires_at FROM tokens WHERE token_type = ?', (token_type,))
            result = self.cursor.fetchone()
            if result:
                access_token, expires_at_str = result
                expires_at = datetime.fromisoformat(expires_at_str)
                return access_token, expires_at
            return None, None
        except sqlite3.Error as e:
            logging.error("Error retrieving token: %s", e)
            raise

    def get_upper_limit_stocks(self, start_date, end_date):
        """
        지정된 기간 동안의 상한가 주식 정보를 조회합니다.

        :param start_date: 시작 날짜 (문자열 형식: 'YYYY-MM-DD')
        :param end_date: 종료 날짜 (문자열 형식: 'YYYY-MM-DD')
        :return: 상한가 주식 정보 리스트 [(date, ticker, name, price), ...]
        """
        try:
            self.cursor.execute('''
            SELECT date, ticker, name, price FROM upper_limit_stocks
            WHERE date BETWEEN ? AND ?
            ORDER BY date, name
            ''', (start_date, end_date))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logging.error("Error retrieving upper limit stocks: %s", e)
            raise

    def save_upper_limit_stocks(self, date, stocks):
        """
        상한가 주식 정보를 데이터베이스에 저장합니다.

        :param date: 상한가 날짜 (문자열 형식: 'YYYY-MM-DD')
        :param stocks: 상한가 주식 정보 리스트 [(ticker, name, price), ...]
        """
        try:
            print(date, stocks)
            for ticker, name, price, upper_rate in stocks:
                self.cursor.execute('''
                INSERT OR REPLACE INTO upper_limit_stocks (date, ticker, name, price, upper_rate)
                VALUES (?, ?, ?, ?, ?)
                ''', (date, ticker, name, float(price), float(upper_rate)))
            self.conn.commit()
            logging.info("Saved upper limit stocks for date: %s", date)
        except sqlite3.Error as e:
            logging.error("Error saving upper limit stocks: %s", e)
            raise

    def get_upper_limit_history(self, start_date, end_date):
        """
        지정된 기간 동안의 상한가 히스토리를 조회합니다.

        :param start_date: 시작 날짜 (문자열 형식: 'YYYY-MM-DD')
        :param end_date: 종료 날짜 (문자열 형식: 'YYYY-MM-DD')
        :return: 상한가 히스토리 리스트 [(날짜, 종목명), ...]
        """
        try:
            self.cursor.execute('''
            SELECT * FROM upper_limit_history
            WHERE date BETWEEN ? AND ?
            ORDER BY date, name
            ''', (start_date, end_date))
            return self.cursor.fetchall()
        except sqlite3.Error as e:
            logging.error("Error retrieving upper limit history: %s", e)
            raise

    def close(self):
        """
        데이터베이스 연결을 종료합니다.
        """
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")
            
    def delete_upper_limit_stocks(self, date):
        """
        특정 날짜의 상한가 주식 정보를 삭제합니다.

        :param date: 삭제할 날짜 (문자열 형식: 'YYYY-MM-DD')
        """
        try:
            self.cursor.execute('''
            DELETE FROM upper_limit_stocks WHERE date = ?
            ''', (date,))
            self.conn.commit()
            logging.info("Deleted upper limit stocks for date: %s", date)
        except sqlite3.Error as e:
            logging.error("Error deleting upper limit stocks: %s", e)
            raise
        
    def get_ticker(self, stock_name):
        """
        종목코드 조회
        """
        try:
            self.cursor.execute('''
            SELECT * FROM upper_limit_stocks WHERE name = ?
            ''', (stock_name,))
            result = self.cursor.fetchone()
            return result
        except sqlite3.Error as e:
            logging.error("Error deleting upper limit stocks: %s", e)
            raise
        
    def get_upper_limit_stocks_two_days_ago(self):
        """
        영업일 기준으로 3일 전에 상한가를 기록한 종목의 ticker와 price를 반환합니다.

        :return: 상한가 종목 ticker와 price 리스트
        """
        today = datetime.now()
        three_days_ago = DateUtils.get_previous_business_day(today, 2)  # 2 영업일 전 날짜 계산
        three_days_ago_str = three_days_ago.strftime('%Y-%m-%d')  # 문자열 형식으로 변환

        # 3 영업일 전의 상한가 종목 조회
        self.cursor.execute('''
            SELECT ticker, name, price FROM upper_limit_stocks WHERE date = ?
        ''', (three_days_ago_str,))
        # print(self.cursor.fetchall())
        return self.cursor.fetchall()  # ticker와 price 리스트 반환

    def create_trade_session(self):
        """
        새로운 거래 세션을 생성하고 ID를 반환합니다.
        """
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock TEXT,
                shares INTEGER,
                price REAL,
                status TEXT,
                day_count INTEGER DEFAULT 1,  -- 거래 일수 추가
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        self.conn.commit()
        return self.cursor.lastrowid

    def save_trade_session(self, session_id, stock, shares, price, status):
        """
        거래 세션 정보를 데이터베이스에 저장합니다.

        :param session_id: 거래 세션 ID
        :param stock: 종목 코드
        :param shares: 매수 수량
        :param price: 매수 가격
        :param status: 거래 상태
        """
        self.cursor.execute('''
            INSERT INTO trade_sessions (id, stock, shares, price, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (session_id, stock, shares, price, status))
        self.conn.commit()

    def get_active_trade_sessions(self):
        """
        현재 활성화된 거래 세션을 조회합니다.

        :return: 활성화된 거래 세션 리스트
        """
        self.cursor.execute('SELECT * FROM trade_sessions WHERE status = "active"')
        return self.cursor.fetchall()

    def update_trade_session_status(self, session_id, status):
        """
        거래 세션의 상태를 업데이트합니다.

        :param session_id: 거래 세션 ID
        :param status: 새로운 상태
        """
        self.cursor.execute('''
            UPDATE trade_sessions SET status = ? WHERE id = ?
        ''', (status, session_id))
        self.conn.commit()

    def update_trade_session_day_count(self, session_id, day_count):
        """
        거래 세션의 day_count를 업데이트합니다.

        :param session_id: 거래 세션 ID
        :param day_count: 새로운 거래 일수
        """
        self.cursor.execute('''
            UPDATE trade_sessions SET day_count = ? WHERE id = ?
        ''', (day_count, session_id))
        self.conn.commit()