import mysql.connector
from mysql.connector.cursor import MySQLCursor, MySQLCursorDict
import logging
from datetime import datetime
from config.config import DB_CONFIG
from config.condition import BUY_DAY_AGO_UPPER
from utils.date_utils import DateUtils
from typing import Any, Dict, List, Optional, Sequence, cast
from zoneinfo import ZoneInfo
KST = ZoneInfo("Asia/Seoul")


class DatabaseManager:
    cursor: MySQLCursorDict

    def __init__(self):
        """
        DatabaseManager 클래스의 생성자
        DB_CONFIG에는 다음 정보가 필요합니다:
        - host: 데이터베이스 호스트
        - user: 사용자명
        - password: 비밀번호
        - database: 데이터베이스명
        """
        self.conn = mysql.connector.connect(**DB_CONFIG)
        self.cursor = cast(MySQLCursorDict, self.conn.cursor(buffered=True, dictionary=True))
        self._create_tables()

    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                # 연결 종료 전 커밋
                if self.conn.is_connected():
                    self.conn.commit()
                    self.conn.close()
        except mysql.connector.Error as e:
            logging.error(f"데이터베이스 종료 중 오류: {e}")
        finally:
            # 명시적으로 None으로 설정하여 참조 제거
            pass
            
            

    def _reset_cursor(self):
        """
        커서를 안전하게 재설정하는 메서드
        """
        try:
            # 연결이 없거나 끊겼으면 재연결
            if self.conn is None or not self.conn.is_connected():
                self.conn = mysql.connector.connect(**DB_CONFIG)
            if self.cursor:
                self.cursor.close()
            self.cursor = cast(MySQLCursorDict, self.conn.cursor(buffered=True, dictionary=True))
        except mysql.connector.Error as e:
            logging.error(f"커서 재설정 오류: {e}")
            raise

    def _create_tables(self):
        """필요한 데이터베이스 테이블을 생성합니다."""
        # 커서가 None이거나 연결이 끊겼으면 재설정
        if self.cursor is None or not self.conn.is_connected():
            self._reset_cursor()
        assert self.cursor is not None
        assert self.cursor is not None
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS tokens (
                token_type VARCHAR(50) PRIMARY KEY,
                access_token TEXT,
                expires_at DATETIME
            ) ENGINE=InnoDB
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS approvals (
                approval_type VARCHAR(50) PRIMARY KEY,
                approval_key TEXT,
                expires_at DATETIME
            ) ENGINE=InnoDB
        ''')


        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trading_session_upper (
                id INT PRIMARY KEY,
                start_date DATE,
                `current_date` DATE,
                ticker VARCHAR(20),
                name VARCHAR(100),
                high_price INT,
                fund INT,
                spent_fund INT,
                quantity INT,
                avr_price INT,
                count INT
            ) ENGINE=InnoDB
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS upper_stocks (
                `date` DATE,
                ticker VARCHAR(20),
                name VARCHAR(100),
                closing_price DECIMAL(10,2),
                upper_rate DECIMAL(5,2),
                PRIMARY KEY (`date`, ticker)
            ) ENGINE=InnoDB
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS selected_upper_stocks (
                no INT AUTO_INCREMENT PRIMARY KEY,
                `date` DATE,
                ticker VARCHAR(20),
                name VARCHAR(100),
                closing_price DECIMAL(10,2)
            ) ENGINE=InnoDB
        ''')

        # 거래 내역 저장용 테이블
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trade_history (
                id INT AUTO_INCREMENT PRIMARY KEY,
                trade_date DATE,
                trade_time TIME,
                ticker VARCHAR(20),
                name VARCHAR(100),
                buy_avg_price INT,
                sell_price INT,
                quantity INT,
                profit_amount INT,
                profit_rate DECIMAL(7,2),
                remaining_assets INT
            ) ENGINE=InnoDB
        ''')

        self.conn.commit()

    def save_token(self, token_type, access_token, expires_at):
        try:
            self.cursor.execute('''
                INSERT INTO tokens (token_type, access_token, expires_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    access_token = VALUES(access_token),
                    expires_at = VALUES(expires_at)
            ''', (token_type, access_token, expires_at))
            self.conn.commit()
        except mysql.connector.Error as e:
            logging.error("Error saving token: %s", e)
            raise

#####################################################################################
#####################################################################################
#####################################################################################

    def get_token(self, token_type):
        try:
            self.cursor.execute(
                'SELECT access_token, expires_at FROM tokens WHERE token_type = %s',
                (token_type,)
            )
            result = self.cursor.fetchone()
            if result:
                access_token = result.get('access_token')
                expires_at = result.get('expires_at')
                if expires_at and expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=KST)
                return access_token, expires_at
            return None, None
        except mysql.connector.Error as e:
            logging.error("Error retrieving token: %s", e)
            raise

    def save_approval(self, approval_type, approval_key, expires_at):
        try:
            self.cursor.execute('''
                INSERT INTO approvals (approval_type, approval_key, expires_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    approval_key = VALUES(approval_key),
                    expires_at = VALUES(expires_at)
            ''', (approval_type, approval_key, expires_at))
            self.conn.commit()
            logging.info("Approval saved successfully: %s", approval_type)
        except mysql.connector.Error as e:
            logging.error("Error saving approval: %s", e)
            raise

    def get_approval(self, approval_type):
        try:
            self.cursor.execute(
                'SELECT approval_key, expires_at FROM approvals WHERE approval_type = %s',
                (approval_type,)
            )
            result = self.cursor.fetchone()
            if result:
                approval_key = result.get('access_token')
                expires_at = result.get('expires_at')
                return approval_key, expires_at
            return None, None
        except mysql.connector.Error as e:
            logging.error("Error retrieving approval: %s", e)
            raise

#####################################################################################
#####################################################################################
#####################################################################################

    def get_upper_stocks(self, start_date, end_date):
        try:
            self.cursor.execute('''
                SELECT date, ticker, name, closing_price 
                FROM upper_stocks 
                WHERE date BETWEEN %s AND %s
                ORDER BY date, name
            ''', (start_date, end_date))
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logging.error("Error retrieving upper stocks: %s", e)
            raise

    def save_upper_stocks(self, date, stocks):
        try:
            for ticker, name, closing_price, upper_rate in stocks:
                self.cursor.execute('''
                    INSERT INTO upper_stocks 
                    (date, ticker, name, closing_price, upper_rate)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        closing_price = VALUES(closing_price),
                        upper_rate = VALUES(upper_rate)
                ''', (date, ticker, name, float(closing_price), float(upper_rate)))
            self.conn.commit()
            logging.info("Saved upper stocks for date: %s", date)
        except mysql.connector.Error as e:
            logging.error("Error saving upper stocks: %s", e)
            raise

    def save_upper_limit_stocks(self, date, stocks):
        try:
            for ticker, name, closing_price, upper_rate in stocks:
                self.cursor.execute('''
                    INSERT INTO upper_stocks 
                    (date, ticker, name, closing_price, upper_rate)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        closing_price = VALUES(closing_price),
                        upper_rate = VALUES(upper_rate)
                ''', (date, ticker, name, float(closing_price), float(upper_rate)))
            self.conn.commit()
            logging.info("Saved upper limit stocks for date: %s", date)
        except mysql.connector.Error as e:
            logging.error("Error saving upper limit stocks: %s", e)
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

    def delete_upper_stocks(self, date):
        try:
            self.cursor.execute(
                'DELETE FROM upper_stocks WHERE date = %s',
                (date,)
            )
            self.conn.commit()
            logging.info("Deleted upper stocks for date: %s", date)
        except mysql.connector.Error as e:
            logging.error("Error deleting upper stocks: %s", e)
            raise

    def delete_old_stocks(self, date):
        try:
            self.cursor.execute(
                'DELETE FROM upper_stocks WHERE date < %s',
                (date,)
            )
            self.conn.commit()
            logging.info("Deleted upper stocks before date: %s", date)
        except mysql.connector.Error as e:
            logging.error("Error deleting old stocks: %s", e)
            raise

    def get_selected_stocks(self):
        try:
            # 커서 재설정
            self._reset_cursor()
            
            self.cursor.execute('''
                SELECT * FROM selected_upper_stocks 
                ORDER BY no 
                LIMIT 1
            ''')
            result = self.cursor.fetchone()
            if result:
                return {
                    'no': int(result.get('no')),
                    'date': result.get('date'),
                    'ticker': result.get('ticker'),
                    'name': result.get('name'),
                    'closing_price': result.get('closing_price')
                }
            return None
        except mysql.connector.Error as e:
            logging.error("Error retrieving selected stocks: %s", e)
            return None

    def get_upper_stocks_days_ago(self, day_ago):
        try:
            # selected_upper_stocks 테이블 초기화
            self.delete_selected_upper_stocks()
            
            today = datetime.now()
            days_ago = DateUtils.get_previous_business_day(today, day_ago)
            days_ago_str = days_ago.strftime('%Y-%m-%d')
            
            self.cursor.execute('''
                SELECT ticker, name, upper_rate, closing_price 
                FROM upper_stocks 
                WHERE date = %s
            ''', (days_ago_str,))
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logging.error("Error retrieving stocks from days ago: %s", e)
            raise

    def save_selected_stocks(self, selected_upper_stocks):
        try:
            # no 갱신
            self.cursor.execute('SELECT MAX(no) FROM selected_upper_stocks')
            print('selected_upper_stocks:--',selected_upper_stocks)
            # upper_rate를 기준으로 내림차순 정렬
            sorted_stocks = sorted(selected_upper_stocks, key=lambda x: float(x.get('upper_rate', 0)), reverse=True)
            print('sorted_stocks:--',sorted_stocks)
            
            today = DateUtils.get_previous_business_day(datetime.now(), 1)
            
            for index, stock in enumerate(sorted_stocks, start=1):
                self.cursor.execute('''
                    INSERT INTO selected_upper_stocks 
                    (no, date, ticker, name, closing_price)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (index, today.strftime('%Y-%m-%d'), stock.get('ticker'), stock.get('name'), 
                    float(stock.get('closing_price')), ))
            print("선별 종목 저장 완료")
            self.conn.commit()
            logging.info("Saved selected stocks successfully.")
        except mysql.connector.Error as e:
            logging.error("Error saving selected stocks: %s", e)
            raise

    def delete_selected_upper_stocks(self):
        try:
            self.cursor.execute('DELETE FROM selected_upper_stocks')
            self.conn.commit()
            logging.info("Deleted all records from selected_upper_stocks table.")
        except mysql.connector.Error as e:
            logging.error("Error deleting selected stocks: %s", e)
            raise

    def delete_selected_stock_by_no(self, no):
        try:
            # 커서 재설정
            self._reset_cursor()
            
            self.cursor.execute('DELETE FROM selected_upper_stocks WHERE no = %s', (no,))
            self.conn.commit()
            logging.info("Deleted stock with no: %d", no)
            self.reorder_selected_upper_stocks()
        except mysql.connector.Error as e:
            logging.error("Error deleting selected stock: %s", e)
            raise

    def reorder_selected_upper_stocks(self):
        try:
            # 커서 재설정
            self._reset_cursor()
            
            self.cursor.execute('SET @count = 0')
            self.cursor.execute('''
                UPDATE selected_upper_stocks 
                SET no = (@count:=@count+1) 
                ORDER BY no
            ''')
            
            self.conn.commit()
            logging.info("Reordered selected stocks successfully.")
        except mysql.connector.Error as e:
            self.conn.rollback()
            logging.error("Error reordering selected stocks: %s", e)
            raise

    def save_trading_session_upper(self, random_id, start_date, current_date, ticker, name, high_price, fund, spent_fund, quantity, avr_price, count):
        try:
            # 파라미터 유효성 검사
            if not all([random_id, ticker, name]):
                raise ValueError("필수 파라미터가 누락되었습니다.")
                
            if not isinstance(quantity, int) or quantity < 0:
                raise ValueError(f"유효하지 않은 수량: {quantity}")
                
            if not isinstance(avr_price, (int, float)) or avr_price < 0:
                raise ValueError(f"유효하지 않은 평균가: {avr_price}")
    
            # SQL 쿼리 실행
            self.cursor.execute('''
                INSERT INTO trading_session_upper 
                (id, start_date, `current_date`, ticker, name, high_price, fund, spent_fund, quantity, avr_price, count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    `current_date` = VALUES(`current_date`),
                    spent_fund = VALUES(spent_fund),
                    quantity = VALUES(quantity),
                    avr_price = CASE 
                        WHEN VALUES(quantity) > 0 THEN VALUES(avr_price)
                        ELSE avr_price
                    END,
                    count = VALUES(count)
            ''', (
                random_id, 
                start_date, 
                current_date, 
                ticker, 
                name, 
                high_price, 
                fund, 
                spent_fund, 
                quantity, 
                avr_price, 
                count
            ))
            
            self.conn.commit()
            logging.info(
                "Trading session saved/updated - ID: %s, Ticker: %s, Quantity: %s, AvgPrice: %s",
                random_id, ticker, quantity, avr_price
            )
            
        except mysql.connector.Error as e:
            self.conn.rollback()
            error_msg = f"DB 오류: {str(e)}"
            logging.error(error_msg)
            raise RuntimeError(error_msg) from e
        except Exception as e:
            self.conn.rollback()
            error_msg = f"세션 저장 중 오류 발생: {str(e)}"
            logging.error(error_msg)
            raise

    def load_trading_session_upper(self, random_id: Optional[int] = None) -> Sequence[Dict[str, Any]]:
        try:
            # 커서 재설정
            self._reset_cursor()
            
            if random_id is not None:
                self.cursor.execute('''
                    SELECT * FROM trading_session_upper 
                    WHERE id = %s
                ''', (random_id,))
            else:
                self.cursor.execute('SELECT * FROM trading_session_upper')
                
            return self.cursor.fetchall()
        
        except mysql.connector.Error as e:
            logging.error("Error loading trading session: %s", e)
            raise

    def delete_session_one_row(self, session_id):
        try:
            # 커서 재설정
            self._reset_cursor()

            self.cursor.execute('DELETE FROM trading_session_upper WHERE id = %s', (session_id,))
            self.conn.commit()
            logging.info("Session row deleted successfully")
        except mysql.connector.Error as e:
            logging.error("Error deleting session row: %s", e)
            self.conn.rollback()
            raise

    def get_session_by_id(self, session_id):
        """특정 세션 ID로 세션 정보 조회"""
        try:
            # 커서 재설정
            self._reset_cursor()
            
            self.cursor.execute('''
                SELECT * FROM trading_session_upper 
                WHERE id = %s
            ''', (session_id,))
            
            result = self.cursor.fetchone()
            if result:
                if isinstance(result, dict):
                    return {
                        'no': int(result.get('no', 0)) if 'no' in result else None,
                        'date': result.get('date'),
                        'ticker': result.get('ticker'),
                        'name': result.get('name'),
                        'closing_price': result.get('closing_price')
                    }
                else:
                    logging.error("Result is not a dictionary in get_session_by_id: %s", result)
                    return None
            else:
                return None
            
        except mysql.connector.Error as e:
            logging.error("Error getting session by ID: %s", e)
            raise

    #####################################################################################
    #                                  Trade History                                   #
    #####################################################################################

    def save_trade_history(self, trade_date, trade_time, ticker, name, buy_avg_price,
                            sell_price, quantity, profit_amount, profit_rate, remaining_assets):
        """거래 내역을 trade_history 테이블에 저장"""
        try:
            self.cursor.execute('''
                INSERT INTO trade_history (
                    trade_date, trade_time, ticker, name, buy_avg_price, sell_price,
                    quantity, profit_amount, profit_rate, remaining_assets)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (
                trade_date, trade_time, ticker, name, buy_avg_price, sell_price,
                quantity, profit_amount, profit_rate, remaining_assets
            ))
            self.conn.commit()
        except mysql.connector.Error as e:
            logging.error("Error saving trade history: %s", e)
            self.conn.rollback()
            raise

    ################## Utility ###################################
    def delete_upper_limit_stocks(self, date):
        try:
            self.cursor.execute(
                'DELETE FROM upper_limit_stocks WHERE date = %s',
                (date,)
            )
            self.conn.commit()
            logging.info("Deleted upper limit stocks for date: %s", date)
        except mysql.connector.Error as e:
            logging.error("Error deleting upper limit stocks: %s", e)
            raise

    def delete_selected_stocks(self):
        try:
            self.cursor.execute('DELETE FROM selected_stocks')
            self.conn.commit()
            logging.info("Deleted all records from selected_stocks table.")
        except mysql.connector.Error as e:
            logging.error("Error deleting selected stocks: %s", e)
            raise