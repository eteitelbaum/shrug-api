import duckdb
import os
from shared.utils import get_data_path

# Get the absolute path to the project root (parent of 'apis' and 'data' folders)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

# Define paths
DB_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "census.db")

def get_db_connection():
    # Ensure the database directory exists
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    return duckdb.connect(DB_FILE)

def init_db():
    conn = get_db_connection()
    
    # Define the file names for each census year
    census_files = {
        1991: "pc91_pca_clean_shrid.parquet",
        2001: "pc01_pca_clean_shrid.parquet",
        2011: "pc11_pca_clean_shrid.parquet"
    }
    
    # Create tables for each census year
    for year, filename in census_files.items():
        table_name = f"population_{str(year)[-2:]}"
        file_path = get_data_path(filename, api_name='pop-census')
        
        try:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS
                SELECT * FROM parquet_scan('{file_path}')
            """)
            print(f"Table {table_name} created successfully.")
        except Exception as e:
            print(f"Error creating table {table_name}: {str(e)}")
    
    conn.close()

def ensure_db_initialized():
    conn = get_db_connection()
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'population_%'").fetchall()
    conn.close()
    
    if not tables:
        init_db()

if __name__ == "__main__":
    ensure_db_initialized()
    print("Database initialized successfully.")