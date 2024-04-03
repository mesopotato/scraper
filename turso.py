import os
import libsql_experimental as libsql
from dotenv import load_dotenv

class TursoDBManager:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Retrieve database credentials from environment variables
        self.url = os.getenv("TURSO_DATABASE_URL")
        self.auth_token = os.getenv("TURSO_AUTH_TOKEN")
        self.conn = None

    def connect(self):
        """Connect to the Turso database."""
        if not self.conn:
            self.conn = libsql.connect(database=self.url, auth_token=self.auth_token)
            print(f"Connected to {self.url}")

    def create_table_e_bern_raw(self):
        """Create the table with updated attributes."""
        self.connect()
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS e_bern_raw (
                ID INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name TEXT UNIQUE, 
                datum TEXT, 
                forderung TEXT,
                signatur TEXT,
                source TEXT,
                file_path TEXT,
                pdf_url TEXT,
                checksum TEXT,
                case_number TEXT,
                scrapy_job TEXT,
                fetch_time_utc TEXT,
                pdf BLOB,
                tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.commit()
        print("Table e_bern_raw created or already exists.")
    

    def drop_table(self, table_name):
        """Drops (deletes) a table from the database."""
        self.connect()
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.commit()
            print(f"Table '{table_name}' dropped successfully.")
        except Exception as e:
            print(f"Error dropping table '{table_name}': {e}")

    def get_all_rows(self):
        """Retrieve all rows from the e_bern_raw table."""
        self.connect()
        cursor = self.conn.execute("SELECT * FROM e_bern_raw")
        rows = cursor.fetchall()
        return rows
    
    def insert_or_update_row_with_data(self, data, pdf_blob=None):
        self.connect()
        # Check if the row exists
        cursor = self.conn.execute("SELECT ID FROM e_bern_raw WHERE file_name = ?", (data['file_name'],))
        result = cursor.fetchone()
        print(f"Result: {result}")
        if pdf_blob is not None:
            data['pdf'] = pdf_blob
        else :
            data['pdf'] = ''    

        if result is None:
            # Directly constructing SQL query string (Beware of SQL Injection risks!)
            query = f"""INSERT INTO e_bern_raw (
                        file_name, datum, forderung, signatur, source, file_path, pdf_url, 
                        checksum, case_number, scrapy_job, fetch_time_utc, pdf
                    ) VALUES (
                        '{data['file_name']}', '{data['datum']}', '{data['forderung']}', '{data['signatur']}', 
                        '{data['source']}', '{data['file_path']}', '{data['pdf_url']}', 
                        '{data['checksum']}', '{data['case_number']}', '{data['scrapy_job']}', 
                        '{data['fetch_time_utc']}', '{data['pdf']}')"""
            # Using parameterized query only for BLOB due to limitations
            self.conn.execute(query)
            print(f"Inserted new row for {data['file_name']}")
        else:
            # Update logic, adjusting for direct interpolation with caution
            update_fields = [f"{key} = '{data[key]}'" for key in data if key != 'file_name' and data[key] is not None]  # Directly interpolating values; ensure they are sanitized
            update_sql = f"UPDATE e_bern_raw SET {', '.join(update_fields)} WHERE file_name = '{data['file_name']}'"
            print(f"Update SQL: {update_sql}")
            self.conn.execute(update_sql)
            print(f"Updated row for {data['file_name']}")
        self.conn.commit()

# Example usage
if __name__ == "__main__":
    db_manager = TursoDBManager()
   # db_manager.drop_table('e_bern_raw')

    db_manager.create_table_e_bern_raw()

    # Insert a row - replace 'your_url_here' and 'your_blob_data_here' with actual values
    # db_manager.insert_row('your_url_here', b'your_blob_data_here')
    # Retrieve and print all rows
    #rows = db_manager.get_all_rows()
    #for row in rows:
    #    print(row)
