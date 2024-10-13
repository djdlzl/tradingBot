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
        """

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                token_type TEXT PRIMARY KEY,
                access_token TEXT,
                expires_at TEXT
            )
        ''')

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
            CREATE TABLE IF NOT EXISTS selected_stocks (
                no INTEGER PRIMARY KEY,
                date TEXT,
                ticker TEXT,
                name TEXT,
                price REAL
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trading_session (
                id INTEGER,
                start_date TEXT,
                current_date TEXT,
                ticker TEXT,
                name TEXT,
                fund INTEGER,
                count INTEGER,
                PRIMARY KEY (id, name)
            )
        ''')
        
                
        ### 테이블 삭제
        # self.cursor.execute('''
        # DROP TABLE IF EXISTS trading_session
        # ''')
        
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

    # def get_upper_limit_history(self, start_date, end_date):
    #     """
    #     지정된 기간 동안의 상한가 히스토리를 조회합니다.

    #     :param start_date: 시작 날짜 (문자열 형식: 'YYYY-MM-DD')
    #     :param end_date: 종료 날짜 (문자열 형식: 'YYYY-MM-DD')
    #     :return: 상한가 히스토리 리스트 [(날짜, 종목명), ...]
    #     """
    #     try:
    #         self.cursor.execute('''
    #         SELECT * FROM upper_limit_history
    #         WHERE date BETWEEN ? AND ?
    #         ORDER BY date, name
    #         ''', (start_date, end_date))
    #         return self.cursor.fetchall()
    #     except sqlite3.Error as e:
    #         logging.error("Error retrieving upper limit history: %s", e)
    #         raise

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

    def delete_old_stocks(self, date):
        """
        upper_limit_stocks 테이블에서 2개월이 지난 상한가 종목을 삭제합니다.
        """
        try:
            self.cursor.execute('''
                DELETE FROM upper_limit_stocks WHERE date < ?
                ''', (date,))
            self.conn.commit()
            logging.info("Deleted upper limit stocks before date: %s", date)
        except sqlite3.Error as e:
            logging.error("Error deleting upper limit stocks: %s", e)
            raise
        
    # def get_ticker(self, stock_name):
    #     """
    #     종목코드 조회
    #     """
    #     try:
    #         self.cursor.execute('''
    #         SELECT * FROM upper_limit_stocks WHERE name = ?
    #         ''', (stock_name,))
    #         result = self.cursor.fetchone()
    #         return result
    #     except sqlite3.Error as e:
    #         logging.error("Error deleting upper limit stocks: %s", e)
    #         raise

######################################################################################
#########################    선택된 종목 관련 메서드   ####################################
######################################################################################

    def get_selected_stocks(self):
        """
        selected_stocks 테이블에서 첫 번째 종목을 조회합니다.

        :return: (no, date, ticker, name, price, upper_rate) 튜플 또는 None
        """
        try:
            self.cursor.execute('SELECT * FROM selected_stocks ORDER BY no LIMIT 1')
            result = self.cursor.fetchone()
            print(result)
            return result  # 첫 번째 종목 반환
        except sqlite3.Error as e:
            logging.error("Error retrieving first selected stock: %s", e)
            raise

    def get_upper_limit_stocks_two_days_ago(self):
        """
        영업일 기준으로 2일 전에 상한가를 기록한 종목의 ticker와 price를 반환합니다.

        :return: 상한가 종목 ticker와 price 리스트
        """
        today = datetime.now()
        two_days_ago = DateUtils.get_previous_business_day(today, 2)  # 2 영업일 전 날짜 계산
        print("two_days_ago: ",two_days_ago)
        two_days_ago_str = two_days_ago.strftime('%Y-%m-%d')  # 문자열 형식으로 변환

        # 3 영업일 전의 상한가 종목 조회
        self.cursor.execute('''
            SELECT ticker, name, price FROM upper_limit_stocks WHERE date = ?
        ''', (two_days_ago_str,))
        return self.cursor.fetchall()  # ticker와 price 리스트 반환

    def save_selected_stocks(self, selected_stocks):
        """
        선택된 종목 정보를 데이터베이스에 저장합니다.

        :param selected_stocks: 선택된 종목 리스트 [(ticker, name, price, upper_rate), ...]
        """
        try:
            # selected_stocks 테이블 초기화
            self.delete_selected_stocks()  # 테이블 초기화

            # no 갱신
            self.cursor.execute('SELECT MAX(no) FROM selected_stocks')
            max_no = self.cursor.fetchone()[0]

            # no 값을 결정
            if max_no is None or max_no < 100:
                no = (max_no + 1) if max_no is not None else 1
            else:
                no = 1  # 100에 도달하면 1로 리셋

            today = DateUtils.get_previous_business_day(datetime.now(), 2)  # 영업일 기준으로 오늘 날짜 가져오기

            for ticker, name, price in selected_stocks:  # 수정된 부분
                self.cursor.execute('''
                INSERT INTO selected_stocks (no, date, ticker, name, price)
                VALUES (?, ?, ?, ?, ?)
                ''', (no, today.strftime('%Y-%m-%d'), ticker, name, float(price))) 
                
                no += 1
                
            self.conn.commit()
            logging.info("Saved selected stocks successfully.")
        except sqlite3.Error as e:
            logging.error("Error saving selected stocks: %s", e)
            raise

    def delete_selected_stocks(self):
        """
        selected_stocks 테이블의 모든 데이터를 삭제합니다.
        """
        try:
            self.cursor.execute('DELETE FROM selected_stocks')
            self.conn.commit()
            logging.info("Deleted all records from selected_stocks table.")
        except sqlite3.Error as e:
            logging.error("Error deleting selected stocks: %s", e)
            raise

    def delete_selected_stock_by_no(self, no):
        """
        특정 no에 해당하는 종목을 selected_stocks 테이블에서 삭제합니다.

        :param no: 삭제할 종목의 no
        """
        try:
            self.cursor.execute('DELETE FROM selected_stocks WHERE no = ?', (no,))
            self.conn.commit()
            logging.info("Deleted stock with no: %d", no)
            
            # 삭제 후 no 값을 재정렬
            self.reorder_selected_stocks()
        except sqlite3.Error as e:
            logging.error("Error deleting selected stock: %s", e)
            raise

    def reorder_selected_stocks(self):
        """
        selected_stocks 테이블의 no 값을 1부터 다시 정렬합니다.
        """
        try:
            # no 값을 1부터 재정렬
            self.cursor.execute('''
            UPDATE selected_stocks SET no = NULL
            ''')
            self.cursor.execute('''
            DELETE FROM sqlite_sequence WHERE name='selected_stocks'
            ''')  # AUTOINCREMENT를 초기화
            self.cursor.execute('''
            SELECT rowid, * FROM selected_stocks
            ''')
            rows = self.cursor.fetchall()
            for index, row in enumerate(rows, start=1):
                self.cursor.execute('''
                UPDATE selected_stocks SET no = ? WHERE rowid = ?
                ''', (index, row[0]))  # rowid를 사용하여 no 업데이트
            self.conn.commit()
            logging.info("Reordered selected stocks successfully.")
        except sqlite3.Error as e:
            logging.error("Error reordering selected stocks: %s", e)
            raise


######################################################################################
#########################    트레이딩 세션 관련 메서드   ###################################
######################################################################################

    def save_trading_session(self, random_id, start_date, current_date, ticker, name, fund, count):
        """
        거래 세션 정보를 데이터베이스에 저장합니다.

        :param start_date: 거래 시작 날짜
        :param current_date: 현재 날짜
        :param ticker: 종목 코드
        :param name: 종목명
        :param fund: 투자 금액
        :param count: 매수 수량
        """
        try:
            self.cursor.execute('''
            INSERT INTO trading_session (id, start_date, current_date, ticker, name, fund, count)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                start_date = excluded.start_date,
                current_date = excluded.current_date,
                ticker = excluded.ticker,
                name = excluded.name,
                fund = excluded.fund,
                count = excluded.count
            ''', (random_id, start_date, current_date, ticker, name, fund, count))
            self.conn.commit()
            logging.info("Trading session saved/updated successfully for ticker: %s", ticker)
        except sqlite3.Error as e:
            logging.error("Error saving trading session: %s", e)
            raise

    def load_trading_session(self):
        """
        trading_session 테이블에서 모든 정보를 조회합니다.

        :return: 거래 세션 정보 리스트
        """
        try:
            self.cursor.execute('SELECT * FROM trading_session')
            session = self.cursor.fetchall()
            return session
        except sqlite3.Error as e:
            logging.error("Error loading trading session: %s", e)
            raise