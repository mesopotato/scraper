import os
import libsql_experimental as libsql
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Retrieve database credentials from environment variables
url = os.getenv("TURSO_DATABASE_URL")
auth_token = os.getenv("TURSO_AUTH_TOKEN")

# Connect to the Turso database
conn = libsql.connect("hello.db", sync_url=url, auth_token=auth_token)

def write_data():
    """Write data to the Turso database."""
    conn.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT);")
    conn.execute("INSERT INTO users (id, name) VALUES (1, 'John Doe');")
    conn.commit()
    conn.sync()  # Sync changes with the primary database

def read_data():
    """Read data from the Turso database."""
    results = conn.execute("SELECT * FROM users").fetchall()
    for row in results:
        print(row)

# Example usage
write_data()
read_data()

# Close the connection
conn.close()
