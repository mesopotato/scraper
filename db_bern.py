import os
from mysql.connector import connect, Error
from dotenv import load_dotenv

class MySQLDBManager:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Retrieve database credentials from environment variables
        self.host = os.getenv("MYSQL_HOST", "localhost")
        self.port = os.getenv("MYSQL_PORT", "3306")  # Default MySQL port
        self.user = os.getenv("MYSQL_USER")
        self.password = os.getenv("MYSQL_PASSWORD")
        self.database = os.getenv("MYSQL_DATABASE")  # Name of the database to connect to
        self.conn = None

    def connect(self):
        """Connect to the MySQL database."""
        if not self.conn:
            try:
                self.conn = connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database
                )
                print(f"Connected to MySQL database at {self.host}:{self.port}")
            except Error as e:
                print(f"Error connecting to MySQL database: {e}")

    def reconnect(self):
        """Reconnect to the MySQL database."""
        if self.conn:
            self.conn.close()
            self.conn = None
        self.connect()

    def create_table_e_bern_raw(self):
        """Create the e_bern_raw table with updated attributes."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS e_bern_raw (
                ID INT AUTO_INCREMENT,
                tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_name VARCHAR(255) UNIQUE,
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
                PRIMARY KEY (ID)
                )
            """)
            self.conn.commit()
            print("Table e_bern_raw created or already exists.")
        except Error as e:
            print(f"Error creating table 'e_bern_raw': {e}")

    def drop_table(self, table_name):
        """Drops (deletes) a table from the database."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.commit()
            print(f"Table '{table_name}' dropped successfully.")
        except Error as e:
            print(f"Error dropping table '{table_name}': {e}")

    def get_all_rows_e_bern_raw(self):
        """Retrieve all rows from the e_bern_raw table."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM e_bern_raw")
            rows = cursor.fetchall()
            return rows
        except Error as e:
            print(f"Error retrieving rows: {e}")
            return []

    def dataChanged(self, current_data, new_data):
        columns = ["file_name", "datum", "forderung", "signatur", "source", "file_path", "pdf_url", "checksum", "case_number", "scrapy_job", "fetch_time_utc"]
        
        changes = {}
        has_changes = False  # Flag to track if there are any changes
        
        for col in columns:
            current_value = current_data.get(col)
            #print(f"Current value for {col}: {current_value}")
            new_value = new_data.get(col, current_value)
            #print(f"New value for {col}: {new_value}")

            if new_value != current_value and new_value is not None and new_value != '':
                changes[col] = new_value
                #print(f"Change detected for {col}: {current_value} -> {new_value}")
                has_changes = True  # Set flag to True if there's a change
        
        return changes, has_changes

    def insert_or_update_row_with_data(self, new_data):
        self.connect()
        cursor = self.conn.cursor()  # Use a cursor object for operations

        # Check if the row exists
        cursor.execute("SELECT * FROM e_bern_raw WHERE file_name = %s", (new_data['file_name'],))
        existing_row = cursor.fetchone()

        if existing_row is None:
            columns = ', '.join(new_data.keys())
            placeholders = ', '.join(['%s'] * len(new_data))  # Use %s as the placeholder
            query = f"INSERT INTO e_bern_raw ({columns}) VALUES ({placeholders})"
            cursor.execute(query, list(new_data.values()))
            print(f"Inserted new row with for {new_data['file_name']}")
        else:
            # If the row exists, update it
            column_names = ["ID", "tsd", "file_name", "datum", "forderung", "signatur", "source", "file_path", "pdf_url", "checksum", "case_number", "scrapy_job", "fetch_time_utc"]
            current_data = dict(zip(column_names, existing_row))
            changes, has_changes = self.dataChanged(current_data, new_data)

            if has_changes:
                updates = ', '.join([f"{col} = %s" for col in changes])
                values = list(changes.values())
                values.append(current_data['ID'])  # Append the ID to the values for the WHERE clause
                update_query = f"UPDATE e_bern_raw SET {updates} WHERE ID = %s"
                cursor.execute(update_query, values)
                print(f"Updated row for {new_data['file_name']}")
            else:
                print(f"No changes detected for {new_data['file_name']}")

        self.conn.commit()

    def create_table_e_bern_parsed(self):
        """Create the table with updated attributes."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS e_bern_parsed (
                ID INT AUTO_INCREMENT,
                tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_name VARCHAR(255) UNIQUE, 
                file_path TEXT, 
                pdf_text LONGTEXT,
                PRIMARY KEY (ID)
                )
            """)
            self.conn.commit()
            print("Table e_bern_parsed created or already exists.")
        except Error as e:
            print(f"Error creating table 'e_bern_parsed': {e}")

    def insert_row_if_not_exists_e_bern_parsed(self, file_name, file_path, pdf_text):
        """Insert a row into the e_bern_parsed table if it doesn't already exist."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM e_bern_parsed WHERE file_name = %s", (file_name,))
            if cursor.fetchone() is None:
                cursor.execute("INSERT INTO e_bern_parsed (file_name, file_path, pdf_text) VALUES (%s, %s, %s)", (file_name, file_path, pdf_text))
                self.conn.commit()
                print(f"Inserted new row for {file_name}")
            else:
                print(f"Row for {file_name} already exists")
        except Error as e:
            print(f"Error inserting row into 'e_bern_parsed': {e}")

    def get_row_by_file_name_e_bern_parsed(self, file_name):
        """Retrieve a row from the e_bern_parsed table by file name."""
        self.connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM e_bern_parsed WHERE file_name = %s", (file_name,))
            row = cursor.fetchone()
            return row
        except Error as e:
            print(f"Error retrieving row by file name: {e}")
            return None


# Example usage
if __name__ == "__main__":
    db_manager = MySQLDBManager()
    #db_manager.create_table_e_bern_raw()
    db_manager.create_table_e_bern_parsed()
    #db_manager.drop_table('e_bern_parsed')
    # Continue with other operations as needed
