import json
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
import duckdb
from typing import List, Dict
from ...shared.utils import get_data_path

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
DB_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "census.db")
CONFIG_FILE = os.path.join(PROJECT_ROOT, "apis", "pop-census", "data-config.json")

class AsyncDuckDB:
    def __init__(self, db_path):
        self.db_path = db_path
        self.executor = ThreadPoolExecutor()

    async def execute(self, query, params=None):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.executor, self._execute, query, params)

    def _execute(self, query, params):
        with duckdb.connect(self.db_path) as conn:
            return conn.execute(query, params).fetchall()

db = AsyncDuckDB(DB_FILE)

def load_config():
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

async def init_db():
    print(f"Initializing database at {DB_FILE}")
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    config = load_config()
    
    for file_info in config['data_files']:
        print(f"Processing data type: {file_info['type']}")
        for year in file_info['years']:
            year_str = str(year)
            year_short = year_str[-2:]
            available_levels = file_info['available_levels'][year_str]
            print(f"Processing year: {year}, available levels: {available_levels}")
            for level in available_levels:
                filename = file_info['aggregation_levels'][level].format(year_short=year_short)
                file_path = os.path.join(PROJECT_ROOT, file_info['path_template'].format(
                    type=file_info['type'],
                    year=year,
                    year_short=year_short,
                    filename=filename
                ))
                
                table_name = os.path.splitext(filename)[0]
                
                print(f"Attempting to create table: {table_name}")
                print(f"File path: {file_path}")
                
                if not os.path.exists(file_path):
                    print(f"Error: Parquet file not found at {file_path}")
                    continue
                
                try:
                    query = f"""
                        CREATE TABLE IF NOT EXISTS {table_name} AS
                        SELECT * FROM parquet_scan('{file_path}')
                    """
                    print(f"Executing query: {query}")
                    await db.execute(query)
                    print(f"Table {table_name} created successfully.")
                except Exception as e:
                    print(f"Error creating table {table_name}: {str(e)}")

    tables = await db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    print("Tables in the database:")
    for table in tables:
        print(table[0])

async def ensure_db_initialized():
    print("Ensuring database is initialized...")
    if os.path.exists(DB_FILE):
        print(f"Existing database found at {DB_FILE}. Removing it to reinitialize.")
        os.remove(DB_FILE)
    await init_db()
    print("Database initialization complete.")

def process_column_name(full_name):
    parts = full_name.split('_')
    if len(parts) <= 3 and any(id_type in full_name for id_type in ['_id', 'state', 'district', 'subdistrict']):
        return full_name  # Keep ID columns intact
    return '_'.join(parts[2:])  # For other columns, remove the first two parts

def get_id_columns(year: int, aggregation_level: str) -> List[str]:
    year_short = str(year)[-2:]
    if aggregation_level == 'shrid':
        return ['shrid2']
    elif aggregation_level == 'constituency_pre_2008':
        return ['ac07_id']
    elif aggregation_level == 'constituency_post_2008':
        return ['ac08_id']
    elif aggregation_level == 'district':
        return [f'pc{year_short}_state_id', f'pc{year_short}_district_id']
    elif aggregation_level == 'subdistrict' and year in [2001, 2011]:
        return [f'pc{year_short}_state_id', f'pc{year_short}_district_id', f'pc{year_short}_subdistrict_id']
    else:
        raise ValueError(f"Unsupported aggregation level: {aggregation_level} for year {year}")

async def query_data(data_type: str, year: int, aggregation_level: str, variables: List[str], limit: int, offset: int):
    year_short = str(year)[-2:]
    config = load_config()
    file_info = next(file for file in config['data_files'] if file['type'] == data_type)
    filename = file_info['aggregation_levels'][aggregation_level].format(year_short=year_short)
    table_name = os.path.splitext(filename)[0]
    
    print(f"Querying table: {table_name}")
    print(f"Variables: {variables}")
    
    # Get the appropriate ID columns
    id_columns = get_id_columns(year, aggregation_level)
    
    # Get the actual column names from the table
    columns_query = f"PRAGMA table_info({table_name})"
    columns_info = await db.execute(columns_query)
    
    # Create a mapping of variable names to full column names
    column_mapping = {}
    for col in columns_info:
        full_name = col[1]
        var_name = process_column_name(full_name)
        column_mapping[var_name] = full_name
    
    # Construct the SELECT statement
    select_clauses = []
    if len(id_columns) == 1:
        select_clauses.append(f"{id_columns[0]} as id")
    else:
        concat_cols = " || '_' || ".join(id_columns)
        select_clauses.append(f"({concat_cols}) as id")
    
    for var in variables:
        if var in column_mapping:
            select_clauses.append(f'"{column_mapping[var]}" as "{var}"')
        else:
            print(f"Warning: Column '{var}' not found in table. Skipping.")
    
    select_statement = ", ".join(select_clauses)
    
    query = f"SELECT {select_statement} FROM {table_name} LIMIT ? OFFSET ?"
    print(f"Executing query: {query}")
    
    try:
        result = await db.execute(query, [limit, offset])
        columns = ["id"] + [var for var in variables if var in column_mapping]
        return [dict(zip(columns, row)) for row in result]
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise

async def get_variable_list(data_type: str, year: int, aggregation_level: str):
    year_short = str(year)[-2:]
    config = load_config()
    file_info = next(file for file in config['data_files'] if file['type'] == data_type)
    filename = file_info['aggregation_levels'][aggregation_level].format(year_short=year_short)
    table_name = os.path.splitext(filename)[0]
    
    query = f"PRAGMA table_info({table_name})"
    result = await db.execute(query)
    
    id_columns = get_id_columns(year, aggregation_level)
    
    variables = []
    for col in result:
        full_name = col[1]
        if full_name not in id_columns:
            var_name = process_column_name(full_name)
            if var_name and var_name not in variables:  # Avoid empty strings and duplicates
                variables.append(var_name)
    
    return variables

if __name__ == "__main__":
    asyncio.run(ensure_db_initialized())
    print("Database initialized successfully.")