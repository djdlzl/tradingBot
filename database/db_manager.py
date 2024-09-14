import sqlite3
import logging
from datetime import datetime
from config.config import DB_NAME


class DatabaseManager:
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




    def insert_upper_limit_stock(self, date, ticker, name, price):
        """
        상한가 주식 정보를 데이터베이스에 삽입합니다.

        :param date: 날짜
        :param ticker: 종목 코드
        :param name: 종목명
        :param price: 가격
        """
        self.cursor.execute('''
            INSERT OR REPLACE INTO upper_limit_stocks (date, ticker, name, price)
            VALUES (?, ?, ?, ?)
        ''', (date, ticker, name, price))
        self.conn.commit()

    def get_upper_limit_stocks(self, date):
        """
        지정된 날짜의 상한가 주식 정보를 조회합니다.

        :param date: 조회할 날짜
        :return: 상한가 주식 정보 리스트
        """
        self.cursor.execute('''
            SELECT * FROM upper_limit_stocks WHERE date = ?
        ''', (date,))
        return self.cursor.fetchall()

    def close(self):
        """
        데이터베이스 연결을 종료합니다.
        """
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")