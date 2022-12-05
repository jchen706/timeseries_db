from datetime import datetime, timedelta
from datetime import datetime
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import datetime
import pandas as pd
# import matplotlib.pyplot as plt

token = "xDfhQrrKUo7vZAZgGEk3lZCLCJcQ2oC8l7GAxHBw6kw-Ghh8ai_D-s8hgJkdL7TH9uakMKRi2U0jJw8oQOzWuQ=="
org = "gt"
url="http://localhost:8086"

metrics = {'total_values': 0, 'total_rows': 0}
table = "stocks"
stock_paths = []
single_stock = ['ALLE']
five_stocks = ['A', 'ALLE', 'AAL','AAP','AAPL']
stock_list = [
  'A',
  'ALLE',
  'AAL',
  'AAP',
  'ABBV',
  'ABC',
  'ACN',
  'ADBE',
  'ADI',
  'ADM'
]
# create dataframes for stats

global_dataframe = pd.DataFrame({
  'Workload': pd.Series(dtype='int'),
  'QueryNum': pd.Series(dtype='int'),
  'Latency': pd.Series(dtype='float'),
})

load_dataframe = pd.DataFrame({ 
  'Batch_Size': pd.Series(dtype='int'),
  'TotalMetrics': pd.Series(dtype='float'),
  'MetricsPerSec': pd.Series(dtype='float'),
  'RowsPerSec': pd.Series(dtype='float'),
  'TotalRows': pd.Series(dtype='float'),
})

mini_dataframe = pd.DataFrame({ 
  'Batch_Size': pd.Series(dtype='int'),
  'Latency': pd.Series(dtype='float'),
})


client = InfluxDBClient(
    url=url,
    token=token,
    org=org,
    debug=True
)
write_api = client.write_api(write_options=SYNCHRONOUS, batch_size=10_000, flush_interval=10_000, jitter_interval=2_000, retry_interval=5_000, max_retries=5, max_retry_delay=30_000, exponential_base=2)

def load_data(stock_list, bucket):
  for each in stock_list:
   stock_paths.append('../stock_data/' + each + '.csv')
  df = pd.DataFrame()
  for file in stock_paths:
    temp_df = pd.read_csv(file)
    df = df.append(temp_df)
  
  rows = df.size
  df['Date'] = pd.to_datetime(df['Date'])
  df['day_of_week'] =df["Date"].dt.day_name()        
  df['_time'] = df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

  start_time = datetime.now()
  write_api.write(bucket=bucket, org=org, record=df, data_frame_measurement_name='Symbol') #, data_frame_timestamp_column="_time")
  end_time = datetime.now()
  latency = (end_time-start_time).total_seconds()
  metrics = rows * 8

  load_dataframe.loc[len(load_dataframe)] = [10000, metrics, metrics/latency, rows/latency, rows]

def mini_load(num_rows):
  bucket = "mini" + str(num_rows)
  df = pd.read_csv('../stock_data/ALLE.csv')
  df = df.head(num_rows)
  df['Date'] = pd.to_datetime(df['Date'])
  df['day_of_week'] =df["Date"].dt.day_name()        
  df['_time'] = df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

  start_time = datetime.now()
  write_api.write(bucket=bucket, org=org, record=df, data_frame_measurement_name='Symbol') #, data_frame_timestamp_column="_time")
  end_time = datetime.now()
  # latency = (end_time-start_time).total_seconds()

def query_data(query, query_num):
  query_api = client.query_api()
  start_time = datetime.now()
  result = query_api.query(org=org, query=query)
  results = []
  for table in result:
      for record in table.records:
          results.append((record.get_value(), record.get_field()))
  print(results)
  end_time = datetime.now()
  latency = (end_time-start_time).total_seconds()
  global_dataframe.loc[len(global_dataframe)] = [2, query_num, latency]
  return results 

def work_4_query(query, num_rows):
  query_api = client.query_api()
  start_time = datetime.now()
  result = query_api.query(org=org, query=query)
  results = []
  for table in result:
      for record in table.records:
          results.append((record.get_value(), record.get_field()))
  print(results)
  end_time = datetime.now()
  latency = (end_time-start_time).total_seconds()
  mini_dataframe.loc[len(mini_dataframe)] = [num_rows, latency]

  return results 

def main():
  # 1. Simple aggregate max for a stock, every week for 1 month
  agg_max_stock_week_one_month = '''from(bucket:"one_stock")
      |> range(start: -1mo)
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''

  # 2. Simple aggregate max for a stock, every week for 1 year
  agg_max_stock_week_one_year = '''from(bucket:"one_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''

  # 3. Simple aggregate max for five stock, every week for 1 month
  agg_max_stock_week_one_month_5 = '''from(bucket:"five_stock")
      |> range(start: -1mo)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''

  # 4. Simple aggregate max for five stock, every week for 1 year
  agg_max_stock_week_one_year_5 = '''from(bucket:"five_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''

  # 5. Aggregate max across all stocks per week over 1 week
  agg_max_stock_week_one_year_all = '''from(bucket:"all_stock")
      |> range(start: -1w)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''

  # 6. Compute the average of a stock per week for 1 month
  agg_avg_stock_week_one_stock = '''from(bucket:"one_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: mean)'''

  # 7. Compute the average of all stocks per week for 1 month
  agg_avg_stock_week_all_stocks = '''from(bucket:"all_stock")
      |> range(start: -1mo)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: mean)'''

  # gains
  gains_all_stocks = '''from(bucket:"all_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r["_measurement"] == "Symbol")
      |> filter(fn: (r) => r["_field"] == "Open" or r["_field"] == "Close")
      |>  map(fn: (r) => ({ r with gain: if r.Open < r.Close then true else false}))'''

  # losses
  losses_all_stocks = '''from(bucket:"all_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r["_measurement"] == "Symbol")
      |> filter(fn: (r) => r["_field"] == "Open" or r["_field"] == "Close")
      |>  map(fn: (r) => ({r with gain: if r.Open > r.Close then true else false}))'''

  #bucket: one_stock
  load_data(single_stock, "one_stock")
  
  query_data(agg_max_stock_week_one_month, 1)
  query_data(agg_max_stock_week_one_year, 2)
  
  #bucket: five_stock
  load_data(five_stocks, "five_stock")

  query_data(agg_max_stock_week_one_month_5, 3)
  query_data(agg_max_stock_week_one_year_5, 4)

  #bucket: all_stock
  load_data(stock_list, "all_stock")    
  
  query_data(agg_max_stock_week_one_year_all, 5)
  query_data(agg_avg_stock_week_one_stock, 6)
  query_data(agg_avg_stock_week_all_stocks, 7)

  #mini1000
  mini_load(1000)
  work_4_query(gains_all_stocks, 1000)
  work_4_query(losses_all_stocks, 1000)

  #mini5000
  mini_load(5000)
  work_4_query(gains_all_stocks, 5000)
  work_4_query(losses_all_stocks, 5000)

  #mini10000
  mini_load(10000)
  work_4_query(gains_all_stocks, 10000)
  work_4_query(losses_all_stocks, 10000)

  global_dataframe.to_csv('influxDB_queryStats.csv', index=False)
  load_dataframe.to_csv('influxDB_loadStats.csv', index=False)
  mini_dataframe.to_csv('influxDB_workload4Stats.csv', index=False)

  client.close()
  

if __name__ == "__main__":
  main()
