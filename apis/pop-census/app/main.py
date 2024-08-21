from fastapi import FastAPI, HTTPException, Query
from app.database import get_db_connection, ensure_db_initialized
from typing import List, Optional
import duckdb
from shared.utils import get_data_path, read_parquet_file

app = FastAPI()

@app.on_event("startup")
async def startup_event():
    ensure_db_initialized()

VALID_YEARS = [1991, 2001, 2011]

def validate_year(year: int):
    if year not in VALID_YEARS:
        raise HTTPException(status_code=400, detail=f"Invalid year. Choose from {VALID_YEARS}.")

@app.get("/census/{year}/variables")
def get_variables(year: int):
    validate_year(year)
    
    conn = get_db_connection()
    table_name = f"census_{str(year)[-2:]}"
    try:
        # Get the column names directly from the table schema
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        return [column[0] for column in result]  # Return just the column names
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=f"Error querying database: {str(e)}")
    finally:
        conn.close()

@app.get("/census/{year}/query")
def query_census(year: int, variable: str, limit: Optional[int] = 10):
    validate_year(year)
    
    conn = get_db_connection()
    table_name = f"census_{str(year)[-2:]}"
    year_prefix = f"pc{str(year)[-2:]}"
    full_variable = f"{year_prefix}_{variable}"
    
    try:
        result = conn.execute(f"SELECT shrid2 as shrid, \"{full_variable}\" as \"{variable}\" FROM {table_name} LIMIT {limit}").fetchall()
        columns = ["shrid", variable]
        return [dict(zip(columns, row)) for row in result]
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=f"Error querying database: {str(e)}")
    finally:
        conn.close()

@app.get("/census/compare")
def compare_census(variable: str, years: List[int] = Query(default=VALID_YEARS), limit: Optional[int] = 10):
    valid_years = [year for year in years if year in VALID_YEARS]
    if not valid_years:
        raise HTTPException(status_code=400, detail="No valid years provided. Choose from 1991, 2001, and 2011.")
    
    conn = get_db_connection()
    results = []
    
    try:
        for year in valid_years:
            table_name = f"census_{str(year)[-2:]}"
            year_prefix = f"pc{str(year)[-2:]}"
            full_variable = f"{year_prefix}_{variable}"
            
            result = conn.execute(f"SELECT shrid2 as shrid, \"{full_variable}\" as \"{variable}\" FROM {table_name} LIMIT {limit}").fetchall()
            columns = ["shrid", variable]
            results.append({
                "year": year,
                "data": [dict(zip(columns, row)) for row in result]
            })
        return results
    except duckdb.Error as e:
        raise HTTPException(status_code=400, detail=f"Error querying database: {str(e)}")
    finally:
        conn.close()