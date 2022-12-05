from datetime import datetime, timedelta
from datetime import datetime
from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import datetime
import pandas as pd
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
# create global dataframe for stats

global_dataframe = pd.DataFrame({
  'Workload': pd.Series(dtype='int'),
  'QueryNum': pd.Series(dtype='int'),
  'NumWorkers':pd.Series(dtype='int'),
  'MinLatency': pd.Series(dtype='float'),
  'MaxLatency': pd.Series(dtype='float'),
  'MedianLatency': pd.Series(dtype='float'),
  'MeanLatency': pd.Series(dtype='float'),
  'StdLatency': pd.Series(dtype='float'),
})

load_dataframe = pd.DataFrame({ 
  'NumWorkers':pd.Series(dtype='int'),
  'Batch_Size': pd.Series(dtype='int'),
  'TotalMetrics': pd.Series(dtype='float'),
  'MetricsPerSec': pd.Series(dtype='float'),
  'RowsPerSec': pd.Series(dtype='float'),
  'TotalRows': pd.Series(dtype='float'),
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

  # get moving averages


  df['Date'] = pd.to_datetime(df['Date'])
  df['day_of_week'] =df["Date"].dt.day_name()        
  df['_time'] = df['Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
  # return
  start_time = datetime.now()
  write_api.write(bucket=bucket, org=org, record=df, data_frame_measurement_name='Symbol') #, data_frame_timestamp_column="_time")
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
  # load_data()

  load_data(single_stock, one_stock)
  # 1. Simple aggregate max for a stock, every week for 1 month
  agg_max_stock_week_one_month = '''from(bucket:"one_stock")
      |> range(start: -1mo)
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''
  query_data(agg_max_stock_week_one_month)

  # 2. Simple aggregate max for a stock, every week for 1 year
  agg_max_stock_week_one_year = '''from(bucket:"one_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''
  query_data(agg_max_stock_week_one_year)

  load_data(five_stocks, five_stock)
  # 3. Simple aggregate max for five stock, every week for 1 month
  agg_max_stock_week_one_month_5 = '''from(bucket:"five_stock")
      |> range(start: -1mo)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''
  query_data(agg_max_stock_week_one_month_5)

  # 4. Simple aggregate max for five stock, every week for 1 year
  agg_max_stock_week_one_year_5 = '''from(bucket:"five_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''
  query_data(agg_max_stock_week_one_year_5)

  load_data(stock_list, all_stock)    
  # 5. Aggregate max across all stocks per week over 1 week
  agg_max_stock_week_one_year_all = '''from(bucket:"all_stock")
      |> range(start: -1w)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: max)'''
  query_data(agg_max_stock_week_one_year_all)

  # 6. Compute the average of a stock per week for 1 month
  agg_avg_stock_week_one_stock = '''from(bucket:"one_stock")
      |> range(start: -1y)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: avg)'''
  query_data(agg_avg_stock_week_one_stock)

  # 7. Compute the average of all stocks per week for 1 month
  agg_avg_stock_week_all_stocks = '''from(bucket:"all_stock")
      |> range(start: -1mo)
      |> filter(fn: (r) => r._measurement == "Symbol")
      |> filter(fn: (r) => r._field == "Close")  
      |> aggregateWindow(
      every: 7d,
      fn: avg)'''
  query_data(agg_avg_stock_week_all_stocks)


  # compute moving average  
  # moving_average_query = '''from(bucket:"all_stock)
  #     |> range(start:-1yr)
  #     |> filter(fn: (r) => r._field == "Close")  
  #     |> movingAverage(n:10)'''
      
  # query_api = client.query_api()
  # moving_average = query_api.query(org=org, query=moving_average_query)

  # # greater than moving average
  # greater_average_query = '''from(bucket:"all_stock)
  #     |> range(start:-1yr)
  #     |> filter(fn: (r) => r._field == "Close")'''
  # greater_than_moving = query_api.query(org=org, query=greater_average_query)

  

  client.close()
  

if __name__ == "__main__":
  main()
