import sqlite3
import logging
from datetime import datetime
from config.config import DB_NAME


class DatabaseManager:
    def __init__(self):
        self.conn = sqlite3.connect(DB_NAME)
        self.cursor = self.conn.cursor()
        self._create_tables()

    def _create_tables(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS upper_limit_stocks (
                date TEXT,
                ticker TEXT,
                name TEXT,
                price REAL,
                PRIMARY KEY (date, ticker)
            )
        ''')
        self.conn.commit()


    def save_token(self, token_type, access_token, expires_at):
        try:
            self.cursor.execute('''
            INSERT OR REPLACE INTO tokens (token_type, access_token, expires_at)
            VALUES (?, ?, ?)
            ''', (token_type, access_token, expires_at.isoformat()))
            self.conn.commit()
            logging.info(f"Token saved successfully: {token_type}")
        except sqlite3.Error as e:
            logging.error(f"Error saving token: {e}")
            raise

    def get_token(self, token_type):
        try:
            self.cursor.execute('SELECT access_token, expires_at FROM tokens WHERE token_type = ?', (token_type,))
            result = self.cursor.fetchone()
            if result:
                access_token, expires_at_str = result
                expires_at = datetime.fromisoformat(expires_at_str)
                return access_token, expires_at
            return None, None
        except sqlite3.Error as e:
            logging.error(f"Error retrieving token: {e}")
            raise




    def insert_upper_limit_stock(self, date, ticker, name, price):
        self.cursor.execute('''
            INSERT OR REPLACE INTO upper_limit_stocks (date, ticker, name, price)
            VALUES (?, ?, ?, ?)
        ''', (date, ticker, name, price))
        self.conn.commit()

    def get_upper_limit_stocks(self, date):
        self.cursor.execute('''
            SELECT * FROM upper_limit_stocks WHERE date = ?
        ''', (date,))
        return self.cursor.fetchall()

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed")