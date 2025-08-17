import dagster as dagster
import duckdb
import os



#this file is created for running tests


#testing connection to duckdb


#conn = duckdb.connect(database=':memory:')
#print("connection successful")
#conn.close()


#import duckdb

# connect to your db file
conn = duckdb.connect("src/ETL_project/data/raw_duckdb.duckdb")

# list all tables
print(conn.execute("SHOW TABLES").fetchall())

# peek at the raw_orders table
print(conn.execute("SELECT * FROM main.raw_orders LIMIT 5").fetchdf())
print(conn.execute("select * from main.raw_payments LIMIT 5").fetchdf())
print(conn.execute("select * from main.raw_customer_data LIMIT 5").fetchdf())
