from fastapi import FastAPI, HTTPException, Query, Depends
from typing import List, Optional
import duckdb
from app.database import get_db_connection, ensure_db_initialized
from shared.utils import get_data_path
import logging

app = FastAPI()

VALID_YEARS = [1991, 2001, 2011]
MAX_VARIABLES = 50
DEFAULT_LIMIT = 1000
MAX_LIMIT = 100000

@app.on_event("startup")
async def startup_event():
    ensure_db_initialized()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_variables(
    variables: List[str] = Query(
        default=None,
        description="List of variables to query (without 'pca_' prefix)",
        example=["tot_p", "tot_m"]
    ),
    list_variables: bool = Query(False, description="If true, returns the list of available variables for the specified years")
):
    if list_variables and variables:
        raise HTTPException(status_code=400, detail="Cannot specify both 'list_variables=true' and provide specific variables. Please choose one operation.")
    if not list_variables and not variables:
        raise HTTPException(status_code=400, detail="Either 'variables' must be provided or 'list_variables' must be set to true.")
    return variables

def create_census_endpoint(data_type: str):
    @app.get(f"/census/{data_type}")
    def query_census(
        years: List[int] = Query(default=VALID_YEARS, example=[2001, 2011]),
        variables: List[str] = Depends(get_variables),
        limit: Optional[int] = Query(DEFAULT_LIMIT, le=MAX_LIMIT, example=1000),
        offset: Optional[int] = Query(0, ge=0, example=0),
        list_variables: bool = Query(False, description="If true, returns the list of available variables for the specified years")
    ):
        valid_years = [year for year in years if year in VALID_YEARS]
        if not valid_years:
            raise HTTPException(status_code=400, detail=f"No valid years provided. Choose from {VALID_YEARS}.")
        
        conn = get_db_connection()
        
        try:
            if list_variables:
                return get_variable_list(conn, data_type, valid_years)
            
            if len(variables) > MAX_VARIABLES:
                raise HTTPException(status_code=400, detail=f"Maximum of {MAX_VARIABLES} variables allowed per query")
            
            return query_data(conn, data_type, valid_years, variables, limit, offset)
        
        except duckdb.Error as e:
            raise HTTPException(status_code=400, detail=f"Error querying database: {str(e)}")
        finally:
            conn.close()

    return query_census

def get_variable_list(conn, data_type: str, years: List[int]):
    variable_lists = {}
    for year in years:
        table_name = f"{data_type}_{str(year)[-2:]}"
        year_prefix = f"pc{str(year)[-2:]}"
        
        table_exists = conn.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'").fetchone()
        if not table_exists:
            logger.warning(f"Table {table_name} does not exist")
            variable_lists[year] = []
            continue
        
        result = conn.execute(f"DESCRIBE {table_name}").fetchall()
        variable_lists[year] = [
            column[0].split(f'{year_prefix}_pca_', 1)[-1]
            for column in result
            if column[0] != 'shrid2' and column[0].startswith(f'{year_prefix}_pca_')
        ]
    return variable_lists

def query_data(conn, data_type: str, years: List[int], variables: List[str], limit: int, offset: int):
    results = []
    for year in years:
        table_name = f"{data_type}_{str(year)[-2:]}"
        year_prefix = f"pc{str(year)[-2:]}"
        
        select_clauses = ["shrid2 as shrid"] + [f"\"{year_prefix}_pca_{var}\" as \"{var}\"" for var in variables]
        select_statement = ", ".join(select_clauses)
        
        result = conn.execute(f"SELECT {select_statement} FROM {table_name} LIMIT {limit} OFFSET {offset}").fetchall()
        columns = ["shrid"] + variables
        year_data = [dict(zip(columns, row)) for row in result]
        
        results.append({
            "year": year,
            "data": year_data
        })
    
    return results[0]["data"] if len(years) == 1 else results

# Create endpoints for each data type
population_endpoint = create_census_endpoint("population")
village_endpoint = create_census_endpoint("village")
constituency_endpoint = create_census_endpoint("constituency")
district_endpoint = create_census_endpoint("district")