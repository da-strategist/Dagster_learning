

import dagster as dg
from dagster_duckdb import DuckDBResource


database_resource = DuckDBResource(database= "src/ETL_project/data/raw_duckdb.duckdb")

@dg.definitions
def db_resource():
    return dg.Definitions(
        resources={
            "database": database_resource
        }
    )