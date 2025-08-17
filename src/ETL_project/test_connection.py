import dagster as dagster
import duckdb
import os



#this file is created for running tests


#testing connection to duckdb


conn = duckdb.connect(database=':memory:')
print("connection successful")
conn.close()
