import psycopg2
import time
import os
import json

# Opening JSON file
with open(os.path.expanduser('~')+'/conf/bq-benchmark/db_conf.json') as json_file:
    db_conf = json.load(json_file)

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=db_conf['db'],
    user=db_conf['user'],
    password=db_conf['password'],
    host=db_conf['host'],
    port=db_conf['port']
)
cursor = conn.cursor()

# Define your query
query1 = (
    "select * from t_empty;"
)

# Run the query and measure the time
start_time = time.time()
cursor.execute(query1)
end_time = time.time()
query_time = end_time - start_time

# Print the number of rows in the returned data
row_count = 0
for row in cursor.fetchall():
    row_count += 1
print("Number of rows in returned data:", row_count)

# Print the query time
print("PG Query1 time - empty table (us): ", query_time, "seconds")

# Define your query
query2 = (
    "select * from t_user_stat limit 1000; "
)

# Run the query and measure the time
start_time = time.time()
cursor.execute(query2)
end_time = time.time()
query_time = end_time - start_time

# Print the number of rows in the returned data
row_count = 0
for row in cursor.fetchall():
    row_count += 1
print("Number of rows in returned data:", row_count)

# Print the query time
print("PG Query2 time - 1000 rows table: ", query_time, "seconds")

# Define your query
query3 = (
    "select * from t_user_stat limit 10000; "
)

# Run the query and measure the time
start_time = time.time()
cursor.execute(query3)
end_time = time.time()
query_time = end_time - start_time

# Print the number of rows in the returned data
row_count = 0
for row in cursor.fetchall():
    row_count += 1
print("Number of rows in returned data:", row_count)

# Print the query time
print("PG Query3 time - 10000 rows table: ", query_time, "seconds")

# Close the connection
cursor.close()
conn.close()

