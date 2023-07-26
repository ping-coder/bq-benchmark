from google.cloud import bigquery
import time

# Initialize a BigQuery client with specific configuration file and connection pool size
# client = bigquery.Client.from_service_account_json('~/keys/tencent-sharkmob.json')
client = bigquery.Client()

# Define your query
query1 = (
    "select * from peace-demo.us_benchmark.t_empty;"
)

# Run the query and measure the time
start_time = time.time()
query_job = client.query(query1)
end_time = time.time()
query_time = end_time - start_time

# Print the number of rows in the returned data
row_count = 0
for row in query_job:
    row_count += 1
print("Number of rows in returned data:", row_count)

# Print the query time
print("BQ Query1 time - empty table (us): ", query_time, "seconds")

# Define your query
query2 = (
    "select * from peace-demo.us_benchmark.t_user_stat limit 1000; "
)

# Run the query and measure the time
start_time = time.time()
query_job = client.query(query2)
end_time = time.time()
query_time = end_time - start_time

# Print the number of rows in the returned data

row_count = 0
for row in query_job:
    row_count += 1
print("Number of rows in returned data:", row_count)

# Print the query time
print("BQ Query2 time - 1000 rows table: ", query_time, "seconds")

