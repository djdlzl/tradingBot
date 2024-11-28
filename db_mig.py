import sqlite3
import mysql.connector
from datetime import datetime

def migrate_data():
    # SQLite 연결
    sqlite_conn = sqlite3.connect('quant_trading.db')
    sqlite_cursor = sqlite_conn.cursor()
    
    # MariaDB 연결
    mariadb_conn = mysql.connector.connect(
        host='localhost',
        user='root',
        password='547723',
        database='test_maria',
        port=3307,
        charset='utf8mb4',
        collation='utf8mb4_general_ci'
    )
    mariadb_cursor = mariadb_conn.cursor()
    
    try:
        # tokens 테이블 마이그레이션
        sqlite_cursor.execute('SELECT * FROM tokens')
        tokens_data = sqlite_cursor.fetchall()
        for row in tokens_data:
            mariadb_cursor.execute('''
                INSERT INTO tokens (token_type, access_token, expires_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    access_token = VALUES(access_token),
                    expires_at = VALUES(expires_at)
            ''', row)
            
        # approvals 테이블 마이그레이션
        sqlite_cursor.execute('SELECT * FROM approvals')
        approvals_data = sqlite_cursor.fetchall()
        for row in approvals_data:
            mariadb_cursor.execute('''
                INSERT INTO approvals (approval_type, approval_key, expires_at)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    approval_key = VALUES(approval_key),
                    expires_at = VALUES(expires_at)
            ''', row)
            
        # upper_limit_stocks 테이블 마이그레이션
        sqlite_cursor.execute('SELECT * FROM upper_limit_stocks')
        stocks_data = sqlite_cursor.fetchall()
        for row in stocks_data:
            mariadb_cursor.execute('''
                INSERT INTO upper_limit_stocks 
                (`date`, ticker, name, price, upper_rate)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    price = VALUES(price),
                    upper_rate = VALUES(upper_rate)
            ''', row)
            
        # selected_stocks 테이블 마이그레이션
        sqlite_cursor.execute('SELECT * FROM selected_stocks')
        selected_data = sqlite_cursor.fetchall()
        for row in selected_data:
            mariadb_cursor.execute('''
                INSERT INTO selected_stocks 
                (no, `date`, ticker, name, price)
                VALUES (%s, %s, %s, %s, %s)
            ''', row)
            
        # trading_session 테이블 마이그레이션
        sqlite_cursor.execute('SELECT * FROM trading_session')
        session_data = sqlite_cursor.fetchall()
        for row in session_data:
            mariadb_cursor.execute('''
                INSERT INTO trading_session 
                (id, start_date, `current_date`, ticker, name, 
                fund, spent_fund, quantity, avr_price, count)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', row)
        
        mariadb_conn.commit()
        print("데이터 마이그레이션이 성공적으로 완료되었습니다.")
        
    except Exception as e:
        print(f"마이그레이션 중 오류 발생: {e}")
        mariadb_conn.rollback()
        
    finally:
        sqlite_cursor.close()
        sqlite_conn.close()
        mariadb_cursor.close()
        mariadb_conn.close()

if __name__ == "__main__":
    migrate_data()