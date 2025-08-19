import dagster as dg
import duckdb
from dagster_duckdb import DuckDBResource
import filelock
import os


"""first we define a function to handle ingestion of files one at a time , such that the next file is load 
only after the previous one has finished"""

def serialize_ingestion(duckdb_path: str, sql: str): 
    """the function takes in a duckdb_path and a sql query string, and executes the query on the specified duckdb database
    """
    lock_path = f"{duckdb_path}.lock" #this creates a lock file used to preserve access while the query is being executed
    with filelock.FileLock(lock_path):
        #this opens when the db is not in use or the previous query has finished
        conn = duckdb.connect(duckdb_path) #opens connection to the duckdb database
        try:
            conn.execute(sql)
        finally:
            conn.close()



#now we create our dynamic ingestion function taht will be called eachtime we want to ingest a new file

#def dynamic_ingest(url, duckdb_path, table_name):
#   query = f"""
   # create or replace table {table_name} as (
     #   select * from read_csv_auto('{url}')
     #   )

   # """
   # serialize_ingestion(duckdb_path, query)



def url_import(url, duckdb: DuckDBResource, table_name):
    with duckdb.get_connection() as conn:
        row_count = conn.execute(
         query = f"""
         create or replace table {table_name} as (
        select * from read_csv_auto('{url}')
        )
    """).fetchone()
        assert row_count is not None
        row_count = row_count[0]
   


#now we shall write the syntax for our ingestion pipeline.

"""
we will be ingesting three tables stored in a cloud storage i.e. github for our current process

"""

#@dg.asset(kinds = {"duckdb"}, key=["target", "main", "raw_customer_data"])

@dg.asset(kinds = {"duckdb"}, op_tags= {"target": "target", "main": "main", "raw_customer_data": "raw_customer_data"})
def raw_customer_data(duckdb: DuckDBResource) -> None:
       url_import(
        url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv",
        duckdb_path = "src/ETL_project/data/raw_duckdb.duckdb",
        table_name = "main.raw_customer_data"
        )


@dg.asset(kinds={"duckdb"}, op_tags={"target": "target", "main": "main", "raw_orders": "raw_orders"})
def raw_orders(duckdb: DuckDBResource) -> None:
    url_import(
        url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_orders.csv",
        duckdb_path = "src/ETL_project/data/raw_duckdb.duckdb",
        table_name = "main.raw_orders"
    )


@dg.asset(kinds={"duckdb"}, op_tags={"target": "target", "main": "main", "raw_payments": "raw_payments"})
def raw_payments(duckdb: DuckDBResource) -> None:
    url_import(
        url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_payments.csv",
        duckdb_path = "src/ETL_project/data/raw_duckdb.duckdb",
        table_name = "main.raw_payments"
    )









