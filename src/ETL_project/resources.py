

import dagster as dg
from dagster_duckdb import DuckDBResource


dbb_resource = DuckDBResource(database= "src/ETL_project/data/raw_duckdb.duckdb")

db_resource = {
        "duckdb": dbb_resource
    }
