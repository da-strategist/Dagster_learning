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
        conn = duckdb.connect(duckdb_path)
        try:
            conn.execute(sql)
        finally:
            conn.close()


