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
            ID INTEGER,
            tsd TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
            PRIMARY KEY (ID, tsd)
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
        cursor = self.conn.execute("SELECT * FROM e_bern_raw WHERE file_name = ?", (new_data['file_name'],))
        existing_row = cursor.fetchone()
        
        if existing_row is None:
            # Manually manage ID for new insert
            cursor = self.conn.execute("SELECT MAX(ID) FROM e_bern_raw")
            max_id_row = cursor.fetchone()
            max_id = max_id_row[0] if max_id_row[0] is not None else 0
            new_id = max_id + 1

            new_data['ID'] = new_id  # Set the new ID
            columns = ', '.join(new_data.keys())
            placeholders = ', '.join(['?'] * len(new_data))
            query = f"INSERT INTO e_bern_raw ({columns}) VALUES ({placeholders})"
            self.conn.execute(query, tuple(new_data.values()))
            print(f"Inserted new row with ID {new_id} for {new_data['file_name']}")
        else:
            #column_names = [desc[0] for desc in cursor.description]
            #print(f" cursor.description: {cursor.description}") -> seems to be empty ..

            column_names = ["ID", "tsd", "file_name", "datum", "forderung", "signatur", "source", "file_path", "pdf_url", "checksum", "case_number", "scrapy_job", "fetch_time_utc"]
            current_data = dict(zip(column_names, existing_row))
            
            #print(f"colum names: {column_names}")
            #print(f"current data: {current_data}")
            #print(f"existing row: {existing_row}")

            changes, has_changes = self.dataChanged(current_data, new_data)

            if has_changes:
                changes['ID'] = current_data['ID']  # Keep the original ID
                columns = ', '.join(changes.keys())
                placeholders = ', '.join(['?'] * len(changes))
                insert_query = f"INSERT INTO e_bern_raw ({columns}) VALUES ({placeholders})"
                #print(f"Query: {insert_query}")
                self.conn.execute(insert_query, tuple(changes.values()))
                print(f"Inserted updated row for {new_data['file_name']}")

            else:
                print(f"No changes detected for {new_data['file_name']}")
        self.conn.commit()


# Example usage
if __name__ == "__main__":
    db_manager = TursoDBManager()
    #db_manager.drop_table('e_bern_raw_new')

    #db_manager.create_table_e_bern_raw()

    # Insert a row - replace 'your_url_here' and 'your_blob_data_here' with actual values
    # db_manager.insert_row('your_url_here', b'your_blob_data_here')
    # Retrieve and print all rows
    #rows = db_manager.get_all_rows()
    #for row in rows:
    #    print(row)
