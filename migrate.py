import sqlite3
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import os
from turso import TursoDBManager 

# Load environment variables for MySQL
load_dotenv()

# MySQL Connection Information
mysql_host = os.getenv("MYSQL_HOST", "localhost")
mysql_port = os.getenv("MYSQL_PORT", "3306")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_database = os.getenv("MYSQL_DATABASE")


def migrate_table(mysql_conn, table_name):
    db_manager = TursoDBManager()
    
    # Define column names for each table explicitly
    if table_name == 'e_bern_raw':
        rows = db_manager.get_all_rows_e_bern_raw()
        column_names = ["ID", "tsd", "file_name", "datum", "forderung", "signatur", "source", "file_path", "pdf_url", "checksum", "case_number", "scrapy_job", "fetch_time_utc"]
    elif table_name == 'e_bern_parsed':
        rows = db_manager.get_all_rows_e_bern_parsed()
        # Replace the following line with the actual column names for the 'e_bern_parsed' table
        column_names = ["ID", "tsd", "file_name", "file_path", "pdf_text"]
    
    # Generate placeholders for the INSERT INTO statement based on column names
    columns = ', '.join(column_names)
    placeholders = ', '.join(['%s'] * len(column_names))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    # Insert each row
    for row in rows:
        try:
            mysql_cursor = mysql_conn.cursor()
            mysql_cursor.execute(insert_query, row)
        except Exception as e:  # Using Exception to catch broader range of errors
            print(f"Error inserting row: {e}")
            mysql_conn.rollback()  # Rollback in case of error
    
    mysql_conn.commit()
    print(f"Migrated {len(rows)} rows to MySQL '{table_name}' table.")

def main():
    try:
        # Connect to MySQL
        mysql_conn = mysql.connector.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        
        # Migrate 'e_bern_raw' and 'e_bern_parsed' tables
        #migrate_table( mysql_conn, 'e_bern_raw')
        migrate_table( mysql_conn, 'e_bern_parsed')
        
    except Error as e:
        print(f"MySQL Error: {e}")
    finally:
        if (mysql_conn.is_connected()):
            mysql_conn.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    main()
