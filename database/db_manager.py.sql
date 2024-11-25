import mysql.connector
import logging
from datetime import datetime
from config.config import DB_CONFIG
from config.condition import BUY_DAY_AGO
from utils.date_utils import DateUtils

class DatabaseManager:
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
        self.cursor = self.conn.cursor(buffered=True)
        self._create_tables()

    def _create_tables(self):
        """필요한 데이터베이스 테이블을 생성합니다."""
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
            CREATE TABLE IF NOT EXISTS upper_limit_stocks (
                `date` DATE,
                ticker VARCHAR(20),
                name VARCHAR(100),
                price DECIMAL(10,2),
                upper_rate DECIMAL(5,2),
                PRIMARY KEY (`date`, ticker)
            ) ENGINE=InnoDB
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS selected_stocks (
                no INT AUTO_INCREMENT PRIMARY KEY,
                `date` DATE,
                ticker VARCHAR(20),
                name VARCHAR(100),
                price DECIMAL(10,2)
            ) ENGINE=InnoDB
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS trading_session (
                id INT PRIMARY KEY,
                start_date DATE,
                `current_date` DATE,
                ticker VARCHAR(20),
                name VARCHAR(100),
                fund INT,
                spent_fund INT,
                quantity INT,
                avr_price INT,
                count INT
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


    def get_token(self, token_type):
        try:
            self.cursor.execute(
                'SELECT access_token, expires_at FROM tokens WHERE token_type = %s',
                (token_type,)
            )
            result = self.cursor.fetchone()
            if result:
                access_token, expires_at_str = result
                access_token = str(access_token)
                expires_at_str = str(expires_at_str)
                expires_at = datetime.fromisoformat(expires_at_str)
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
                approval_key, expires_at_str = result
                approval_key = str(approval_key)
                expires_at_str = str(expires_at_str)
                expires_at = datetime.fromisoformat(expires_at_str)
                return approval_key, expires_at
            return None, None
        except mysql.connector.Error as e:
            logging.error("Error retrieving approval: %s", e)
            raise

    def get_upper_limit_stocks(self, start_date, end_date):
        try:
            self.cursor.execute('''
                SELECT date, ticker, name, price 
                FROM upper_limit_stocks 
                WHERE date BETWEEN %s AND %s
                ORDER BY date, name
            ''', (start_date, end_date))
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logging.error("Error retrieving upper limit stocks: %s", e)
            raise

    def save_upper_limit_stocks(self, date, stocks):
        try:
            for ticker, name, price, upper_rate in stocks:
                self.cursor.execute('''
                    INSERT INTO upper_limit_stocks 
                    (date, ticker, name, price, upper_rate)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name),
                        price = VALUES(price),
                        upper_rate = VALUES(upper_rate)
                ''', (date, ticker, name, float(price), float(upper_rate)))
            self.conn.commit()
            logging.info("Saved upper limit stocks for date: %s", date)
        except mysql.connector.Error as e:
            logging.error("Error saving upper limit stocks: %s", e)
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")

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

    def delete_old_stocks(self, date):
        try:
            self.cursor.execute(
                'DELETE FROM upper_limit_stocks WHERE date < %s',
                (date,)
            )
            self.conn.commit()
            logging.info("Deleted upper limit stocks before date: %s", date)
        except mysql.connector.Error as e:
            logging.error("Error deleting old stocks: %s", e)
            raise

    def get_selected_stocks(self):
        try:
            self.cursor.execute('''
                SELECT * FROM selected_stocks 
                ORDER BY no 
                LIMIT 1
            ''')
            result = self.cursor.fetchone()
            if result:
                return {
                    'no': int(result[0]),
                    'date': result[1],
                    'ticker': result[2],
                    'name': result[3],
                    'price': result[4]
                }
            return None
        except mysql.connector.Error as e:
            logging.error("Error retrieving selected stocks: %s", e)
            return None
        
    def get_upper_limit_stocks_days_ago(self):
        try:
            # selected_stocks 테이블 초기화
            self.delete_selected_stocks()
            
            today = datetime.now()
            days_ago = DateUtils.get_previous_business_day(today, BUY_DAY_AGO)
            days_ago_str = days_ago.strftime('%Y-%m-%d')
            
            self.cursor.execute('''
                SELECT ticker, name, price 
                FROM upper_limit_stocks 
                WHERE date = %s
            ''', (days_ago_str,))
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logging.error("Error retrieving stocks from days ago: %s", e)
            raise

    def save_selected_stocks(self, selected_stocks):
        try:
            # no 갱신
            self.cursor.execute('SELECT MAX(no) FROM selected_stocks')
            max_no = self.cursor.fetchone()[0]
            
            # no 값 결정
            if max_no is None or max_no < 100:
                no = (max_no + 1) if max_no is not None else 1
            else:
                no = 1  # 100에 도달하면 1로 리셋
                
            today = DateUtils.get_previous_business_day(datetime.now(), 2)
            
            for ticker, name, price in selected_stocks:
                self.cursor.execute('''
                    INSERT INTO selected_stocks 
                    (no, date, ticker, name, price)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (no, today.strftime('%Y-%m-%d'), ticker, name, float(price)))
                no += 1
                
            self.conn.commit()
            logging.info("Saved selected stocks successfully.")
        except mysql.connector.Error as e:
            logging.error("Error saving selected stocks: %s", e)
            raise

    def delete_selected_stocks(self):
        try:
            self.cursor.execute('DELETE FROM selected_stocks')
            self.conn.commit()
            logging.info("Deleted all records from selected_stocks table.")
        except mysql.connector.Error as e:
            logging.error("Error deleting selected stocks: %s", e)
            raise

    def delete_selected_stock_by_no(self, no):
        try:
            self.cursor.execute('DELETE FROM selected_stocks WHERE no = %s', (no,))
            self.conn.commit()
            logging.info("Deleted stock with no: %d", no)
            self.reorder_selected_stocks()
        except mysql.connector.Error as e:
            logging.error("Error deleting selected stock: %s", e)
            raise

    def reorder_selected_stocks(self):
        try:
            self.cursor.execute('''
                SET @count = 0;
                UPDATE selected_stocks 
                SET no = (@count:=@count+1) 
                ORDER BY no;
            ''')
            self.conn.commit()
            logging.info("Reordered selected stocks successfully.")
        except mysql.connector.Error as e:
            self.conn.rollback()
            logging.error("Error reordering selected stocks: %s", e)
            raise

    def save_trading_session(self, random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count):
        try:
            self.cursor.execute('''
                INSERT INTO trading_session 
                (id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    start_date = VALUES(start_date),
                    current_date = VALUES(current_date),
                    ticker = VALUES(ticker),
                    name = VALUES(name),
                    fund = VALUES(fund),
                    spent_fund = VALUES(spent_fund),
                    quantity = VALUES(quantity),
                    avr_price = VALUES(avr_price),
                    count = VALUES(count)
            ''', (random_id, start_date, current_date, ticker, name, fund, spent_fund, quantity, avr_price, count))
            self.conn.commit()
            logging.info("Trading session saved/updated successfully for ticker: %s", ticker)
        except mysql.connector.Error as e:
            logging.error("Error saving trading session: %s", e)
            raise

    def load_trading_session(self, random_id=None):
        try:
            if random_id is not None:
                self.cursor.execute('''
                    SELECT * FROM trading_session 
                    WHERE id = %s
                ''', (random_id,))
            else:
                self.cursor.execute('SELECT * FROM trading_session')
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            logging.error("Error loading trading session: %s", e)
            raise

    def delete_session_one_row(self, session_id):
        try:
            self.cursor.execute('DELETE FROM trading_session WHERE id = %s', (session_id,))
            self.conn.commit()
            logging.info("Session row deleted successfully")
        except mysql.connector.Error as e:
            logging.error("Error deleting session row: %s", e)
            self.conn.rollback()
            raise
        
