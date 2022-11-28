from datetime import datetime, timedelta
from datetime import datetime
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import datetime
import pandas as pd

token = "xDfhQrrKUo7vZAZgGEk3lZCLCJcQ2oC8l7GAxHBw6kw-Ghh8ai_D-s8hgJkdL7TH9uakMKRi2U0jJw8oQOzWuQ=="
org = "gt"
bucket = "stock_bucket"
url="http://localhost:8086"

client = InfluxDBClient(
    url=url,
    token=token,
    org=org
)
write_api = client.write_api(write_options=WriteOptions(SYNCHRONOUS, batch_size=10_000, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000, max_retries=5, max_retry_delay=30_000, exponential_base=2))

def load_data():
  df = pd.read_csv("../stock_csv/ALLE.csv")
  # print(df)                            
  start_time = datetime.now()
  write_api.write(bucket=bucket, org=org, record=df, data_frame_measurement_name='Symbol', data_frame_tag_columns=['Symbol'])
  end_time = datetime.now()
  print("Load Time: ")
  print(end_time-start_time)


def query_data(query):
  query_api = client.query_api()
  start_time = datetime.now()
  result = query_api.query(org=org, query=query)
  results = []
  for table in result:
      for record in table.records:
          results.append((record.get_value(), record.get_field()))
  print(results)
  end_time = datetime.now()
  print("Query Time: ")
  print(end_time-start_time)
  return results 



def main():
  load_data()

  query1 = 'from(bucket: "stock_bucket")\
  |> range(start: 1633124983)\
  |> filter(fn: (r) => r._field == "High")'
  query_data(query1)


if __name__ == "__main__":
  main()
