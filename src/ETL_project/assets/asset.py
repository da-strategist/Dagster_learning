import dagster as dg
import duckdb
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

def dynamic_ingest(url, duckdb_path, table_name):
    query = f"""
    create or replace table {table_name} as (
        select * from read_csv_auto('{url})
        )

    """
    serialize_ingestion(duckdb_path, query)


#now we shall write the syntax for our ingestion pipeline.

"""
we will be ingesting three tables stored in a cloud storage i.e. github for our current process

"""

@dg.asset(kinds = {"duckdb"}, keys= ["target", "main", "raw_customer_data"])
def raw_customer_data() -> None:
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv"
    duckdb_path = "data/duckdb/customer_data.duckdb"
    table_name = "raw_customer_data"
    dynamic_ingest(url, duckdb_path, table_name)



@dg.asset(kinds={"duckbd"}, keys =["target", "main", "raw_orders"])
def raw_orders() -> None:
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_orders.csv"
    duckdb_path = "data/duckdb/raw_orders.duckdb"
    table_name = "raw_orders"

    dynamic_ingest(url, duckdb_path, table_name)


@dg.asset(kinds={"duckdb"}, keys= ["target", "main", "raw_payments"])
def raw_payments() -> None:
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_payments.csv"
    duckdb_path = "data/duckdb/raw_payments.duckdb"
    table_name = "raw_payments"

    dynamic_ingest(url, duckdb_path, table_name)











