import asyncio
from concurrent.futures import ThreadPoolExecutor
from fastapi import FastAPI, HTTPException, Query, Depends
from typing import List, Optional
from enum import Enum
import duckdb
from .database import ensure_db_initialized, get_variable_list, query_data, load_config

app = FastAPI()

class DatasetType(str, Enum):
    PCA = "pca"
    VD = "vd"
    TD = "td"

class AggregationLevel(str, Enum):
    SHRID = "shrid"
    CONSTITUENCY_PRE_2008 = "constituency_pre_2008"
    CONSTITUENCY_POST_2008 = "constituency_post_2008"
    DISTRICT = "district"
    SUBDISTRICT = "subdistrict"

VALID_YEARS = [1991, 2001, 2011]
MAX_VARIABLES = 50
DEFAULT_LIMIT = 1000
MAX_LIMIT = 100000

def get_available_levels(dataset_type: str, year: int):
    config = load_config()
    file_config = next((file for file in config['data_files'] if file['type'] == dataset_type), None)
    if not file_config:
        raise ValueError(f"Configuration for data type {dataset_type} not found")
    return file_config['available_levels'][str(year)]

def get_variables(
    variables: List[str] = Query(
        default=None,
        description="List of variables to query",
        example=["tot_p", "tot_m"]
    ),
    list_variables: bool = Query(False, description="If true, returns the list of available variables for the specified year")
):
    if list_variables and variables:
        raise HTTPException(status_code=400, detail="Cannot specify both 'list_variables=true' and provide specific variables. Please choose one operation.")
    if not list_variables and not variables:
        raise HTTPException(status_code=400, detail="Either 'variables' must be provided or 'list_variables' must be set to true.")
    return variables

@app.on_event("startup")
async def startup_event():
    await ensure_db_initialized()

@app.get("/api/census/{dataset_type}")
async def query_census(
    dataset_type: DatasetType,
    year: int = Query(..., description="Year to query"),
    aggregation_level: AggregationLevel = Query(default=AggregationLevel.SHRID, description="Level of aggregation"),
    variables: List[str] = Depends(get_variables),
    limit: Optional[int] = Query(DEFAULT_LIMIT, le=MAX_LIMIT, description="Maximum number of records to return"),
    offset: Optional[int] = Query(0, ge=0, description="Number of records to skip"),
    list_variables: bool = Query(False, description="If true, returns the list of available variables for the specified year")
):
    if year not in VALID_YEARS:
        raise HTTPException(status_code=400, detail=f"Invalid year. Choose from {VALID_YEARS}.")
    
    available_levels = get_available_levels(dataset_type.value, year)
    if aggregation_level.value not in available_levels:
        raise HTTPException(status_code=400, detail=f"Aggregation level {aggregation_level.value} not available for {dataset_type.value} in {year}. Available levels: {', '.join(available_levels)}")
    
    try:
        if list_variables:
            return await get_variable_list(dataset_type.value, year, aggregation_level.value)
        
        if len(variables) > MAX_VARIABLES:
            raise HTTPException(status_code=400, detail=f"Maximum of {MAX_VARIABLES} variables allowed per query")
        
        return await query_data(
            dataset_type.value,
            year,
            aggregation_level.value,
            variables,
            limit,
            offset
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)