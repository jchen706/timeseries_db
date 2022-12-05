"""
Connect to timescale db
"""
# from distutils import command
import atexit
from datetime import datetime
from glob import glob
from sqlite3 import Timestamp
#from turtle import back
from typing_extensions import runtime
from unittest import result
import psycopg2
from psycopg2.extras import RealDictCursor
from concurrent.futures import ThreadPoolExecutor, as_completed, ALL_COMPLETED
import concurrent
import pandas as pd
import sys 
import os
pd.options.display.max_rows = 10
import time
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import LoggingConnection
import logging
import statistics
import threading



logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

metrics = {'total_values': 0, 'total_rows': 0}
table = "stocks"
stock_paths = []
single_stock = 'ALLE'
five_stocks = "('A', 'ALLE', 'AAL','AAP','ABBV')"
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

for each in stock_list:
   stock_paths.append('../stock_data/' + each + '.csv')

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
  'TimeStamp': pd.Series(dtype=object)
})

load_dataframe = pd.DataFrame({ 
  'NumWorkers':pd.Series(dtype='int'),
  'Batch_Size': pd.Series(dtype='int'),
  'TotalMetrics': pd.Series(dtype='float'),
  'MetricsPerSec': pd.Series(dtype='float'),
  'RowsPerSec': pd.Series(dtype='float'),
  'TotalRows': pd.Series(dtype='float'),
  'TimeStamp': pd.Series(dtype=object),
  "Try":pd.Series(dtype='int'),
  'Latency': pd.Series(dtype='float'),
})



def create_tables(cursor):
  # create regular postresql table
  commands = (
    """
    CREATE TABLE IF NOT EXISTS {} (
      time DATE NOT NULL,
      symbol TEXT NOT NULL,
      open DOUBLE PRECISION NULL,
      high DOUBLE PRECISION NULL,
      low DOUBLE PRECISION NULL,
      close DOUBLE PRECISION NULL,
      adj_close DOUBLE PRECISION NULL,
      volume INT NULL
    )
    """.format(table)
  )
  # print(commands)
  cursor.execute(commands)
  create_ext = "CREATE EXTENSION IF NOT EXISTS timescaledb;"
  cursor.execute(create_ext)
  
  # create hypertable
  query_create__hypertable = "SELECT create_hypertable( '{}', 'time', if_not_exists => true);".format(table)
  # print(query_create__hypertable)
  cursor.execute(query_create__hypertable)

  print(" Table {} Created".format(table))

# local
# CONNECTION = "postgres://postgres:123@localhost:5432/postgres"

#EC2 Connection
CONNECTION = "postgres://postgres:123@34.201.251.248:5432/postgres"

conn = None
def connect():  
    global conn

    # single thread connection

    # conn = psycopg2.connect(
    #   # CONNECTION
    #   dbname="postgres",
    #   user="postgres",
    #   password="123",
    #   host="localhost",
    #   port="5432",
    #   keepalives=1,
    #   keepalives_idle=30,
    #   keepalives_interval=10,
    #   keepalives_count=5
    #   )

    # 34.201.251.248
    # threadpool connection
    tcp = ThreadedConnectionPool(1, 10, dbname="postgres",
      user="postgres",
      password="123",
      host="3.83.121.47",
      port="5432",
      keepalives=1,
      keepalives_idle=30,
      keepalives_interval=10,
      keepalives_count=5 )
    conn = tcp.getconn()
    cursor = conn.cursor()

    # use the cursor to interact with your database 
    # test connection
    cursor.execute("SELECT 'hello world'")
    print(cursor.fetchone())

def inject_thread(rowlist, conn):
  # print(rowlist)
  ins = "INSERT INTO " + table + " (time, open, high, low, close, adj_close, volume, symbol) VALUES (%s,%s,%s,%s,%s,%s,%s, %s)"
  cursor = conn.cursor()
  # print("here")
  try:
    # s = time.time()
    # print('excute')
    cursor.executemany(ins, rowlist)
    conn.commit()
    # print(" executemany time:", time.time() - s)
  except Exception as e:
    print(e)
    conn.rollback()


def inject_many(stock_path):
  global conn
  print('injecting data with many on ' + stock_path)
  #insert data into the table
  df = pd.read_csv(stock_path)
  print(df)
  print(len(df))
  itemBank = list(zip(*map(df.get, df)))
  # print(itemBank)
  # insert at once with python library
  ins = "INSERT INTO '{}' (time, open, high, low, close, adj_close, volume, symbol) VALUES (%s,%s,%s,%s,%s,%s,%s, %s)".format(table)
  cursor = conn.cursor() 
  try:
    s = time.time()
    cursor.executemany(ins, itemBank)
    conn.commit()
    print(" executemany time:", time.time() - s)
  except Exception as e:
    print(e)
    conn.rollback()

"""
Write Into DB with threads
"""
# def inject(conn):
#   print('injecting data on path ')

#   #insert data into the table
#   df = pd.read_csv('../stock_csv/ALLE.csv')
#   print(df)
#   print(len(df))
#   itemBank = list(zip(*map(df.get, df)))

#   print("Running threaded:")
#   threaded_start = time.time()
#   futures = []
#   # queue
#   # popping of the futures
#   with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
#       for i in range(0,len(df), 5):
#           # 5 item at a time
#           print(i,i+5)
#           if (i+5 > len(df)):
#             futures.append(executor.submit(inject_thread,rowlist=itemBank[i:len(df)],conn=conn))
#           else:
#             futures.append(executor.submit(inject_thread,rowlist=itemBank[i:i+5],conn=conn))
#       # for future in concurrent.futures.as_completed(futures):
#       #     pass
#           # print(future.result())
#   concurrent.futures.wait(futures,return_when=ALL_COMPLETED)
#   print("Threaded time:", time.time() - threaded_start)

"""

"""


"""
Write Into DB with threads
"""
def load_threadpool(list_of_stock_paths, batch_size, worker_number, attempt):
  global conn
  print('Threadpool Injection of Batch size: ' + str(batch_size) + " workers: " + str(worker_number))

  #insert data into the table
  df = pd.read_csv(list_of_stock_paths[0])
  for i in range(1,len(list_of_stock_paths)):
    temp_df = pd.read_csv(list_of_stock_paths[i])
    df = pd.concat([df, temp_df])
  

  # df = pd.read_csv('../stock_csv/ALLE.csv')
  print(df)
  print(len(df))

  if (batch_size==0):
    batch_size = len(df)

  itemBank = list(zip(*map(df.get, df)))
  total_rows = len(itemBank)

  print("Running thread pool for load:")
  threaded_start = time.time()
  futures = []
  # queue
  # popping of the futures
  with concurrent.futures.ThreadPoolExecutor(max_workers=worker_number) as executor:
      for i in range(0,len(df), batch_size):
          # 5 item at a time
          print(i,i+batch_size)
          if (i+batch_size > len(df)):
            futures.append(executor.submit(inject_thread,rowlist=itemBank[i:len(df)],conn=conn))
          else:
            futures.append(executor.submit(inject_thread,rowlist=itemBank[i:i+batch_size],conn=conn))
  
  concurrent.futures.wait(futures,return_when=ALL_COMPLETED)
  run_time = time.time() - threaded_start
  total_metrics = total_rows * 8
  print("Load Data by Thread Pool time:", run_time)
  print('Rows written per second: {0}'.format(total_rows / run_time))
  print('Total rows written: {0}'.format(total_rows))
  print('Values written per second: {0}'.format(total_rows * 8 / run_time))
  print('Total values written: {0}'.format(total_rows * 8))
  print('Total Metrics ' + str(total_metrics))
  # ['NumWorkers','Batch_Size','TimeStamp','TotalMetrics','MetricsPerSec','RowsPerSec','TotalRows']
  global load_dataframe
  load_dataframe.loc[len(load_dataframe)] = [worker_number, batch_size, total_metrics, total_metrics/run_time, total_rows/run_time, total_rows, pd.Timestamp(datetime.now()), attempt, run_time]

  # return run_time


# 1. Simple aggregate max for a stock, every week for 1 month
agg_max_stock_week_one_month = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where (time >= '2014-01-01') and (time < '2014-02-01') and symbol = '{}'
  group by bucket, symbol
  order by bucket, symbol
  """.format(table,single_stock)

# 2. Simple aggregate max for a stock, every week for 1 year
agg_max_stock_week_one_year = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where  (time >= '2014-01-01') and (time < '2015-01-01') and symbol = '{}'
  group by bucket, symbol
  order by bucket, symbol
  """.format(table,single_stock)

# 3. Simple aggregate max for five stock, every week for 1 month
agg_max_stock_week_five_one_month = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where (time >= '2014-01-01') and (time < '2014-02-01') and symbol IN {}
  group by bucket, symbol
  order by bucket, symbol
  """.format(table, five_stocks)

# 4. Simple aggregate max for five stock, every week for 1 year
agg_max_stock_week_five_one_year = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from {}
  where (time >= '2014-01-01') and (time < '2015-01-01') and symbol IN {}
  group by bucket, symbol
  order by bucket, symbol
  """.format(table, five_stocks)

# Aggregate max across all stocks per week over 1 week
agg_max_stock_week_all_one_year = """
select time_bucket('7 day', time) as bucket, max(close), symbol
from stocks
where (time >= '2022-05-09') and (time <= '2022-05-13' )
group by bucket, symbol
order by bucket, symbol
""".format(table)


# Compute the average of a stock per week for 1 month
agg_avg_stock_week_one_month = """
select time_bucket('7 day', time) as bucket, avg(close),symbol
from {}
where (time >= '2014-01-01') and (time < '2014-02-01') and symbol = '{}'
group by bucket, symbol
order by bucket, symbol
""".format(table,single_stock)

# Compute the average of all stocks per week for 1 month
agg_avg_stock_week_all_one_month = """
select time_bucket('7 day', time) as bucket, avg(close),symbol
from {}
where (time >= '2014-01-01') and (time < '2014-02-01')
group by bucket, symbol
order by bucket, symbol
""".format(table)


# price greater than the ten day moving average
workload_three_query = """
Select s.time, s.symbol
from (
 SELECT time, AVG(close) OVER(ORDER BY time
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
    AS ten_day_avg_close, symbol
  FROM stocks
  ORDER BY time DESC
) as t, stocks as s
where s.close - t.ten_day_avg_close > 0 and s.symbol = t.symbol and s.time = t.time
order by s.time DESC
""".format(table)


# moving average
"""
SELECT time, AVG(close) OVER(ORDER BY time
    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
  AS ten_day_avg_close
FROM stocks3
WHERE time > NOW() - INTERVAL '1 month'
ORDER BY time DESC
"""

# subtract the open price of today from the close price of previous row
# get the gains or losses of those,then take the days that are greater than 0
workload_four_query = """
select d.time, d.close, d.change_of_price, d.symbol
from  (
SELECT t.time, t.close, t.close - LAG(t.close, 1, t.close) OVER(partition  by t.symbol ORDER BY t.symbol)
AS change_of_price, t.symbol
FROM (
select symbol, time, close 
from stocks 
order by symbol, time asc) 
as t 
) as d 
where change_of_price > 0
order by d.time desc
""".format(table)


query_total_records = "select count(*)  from {}".format(table)

def thread_helper(query):
  global conn
  cursor = conn.cursor() 
  cursor.execute(query) 
  conn.commit()
  result = cursor.fetchall()
  # print(result)

  
"""
Query Data From data timescale db
"""
def run_query(query, num_workers, workload_num, query_num, attempts = 5):
  global conn
  cursor = conn.cursor() 

  try:
    cursor.execute(query_total_records)
    latencies = []

    if num_workers == 1:
      for i in range(attempts):
        # print(query)
        start = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        # print(result[0])
        end = time.time()
        latencies.append(end - start)


    elif num_workers == 2:
       for i in range(attempts):
          
          t1 = threading.Thread(target=thread_helper, args=(query,))
          t2 = threading.Thread(target=thread_helper, args=(query,))
          start = time.time()
          t1.start()
          t2.start()
          t1.join()
          t2.join()
          end = time.time()
          latencies.append(end - start)
    elif num_workers == 3:
        for i in range(attempts):
          t1 = threading.Thread(target=thread_helper, args=(query,))
          t2 = threading.Thread(target=thread_helper, args=(query,))
          t3 = threading.Thread(target=thread_helper, args=(query,))

          start = time.time()
          t1.start()
          t2.start()
          t3.start()
          t1.join()
          t2.join()
          t3.join()
          end = time.time()
          latencies.append(end - start)

    elif num_workers == 4:
       for i in range(attempts):
          t1 = threading.Thread(target=thread_helper, args=(query,))
          t2 = threading.Thread(target=thread_helper, args=(query,))
          t3 = threading.Thread(target=thread_helper, args=(query,))
          t4 = threading.Thread(target=thread_helper, args=(query,))

          start = time.time()
          t1.start()
          t2.start()
          t3.start()
          t4.start()
          t1.join()
          t2.join()
          t3.join()
          t4.join()
          end = time.time()
          latencies.append(end - start)


    print("Min latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, min(latencies)))
    print("Max latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, max(latencies)))
    print("Median latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, statistics.median(latencies)))
    print("Mean latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, statistics.mean(latencies)))
    print("Standard Deviation latency for Workload {0}, Query {1}: {2}".format(workload_num, query_num, statistics.stdev(latencies)))
 
    # ['Workload', 'QueryNum', 'NumWorkers, 'MinLatency', 'MaxLatency', 'MedianLatency', 'MedianLatency','MeanLatency', 'StdLatency']
    global global_dataframe
    global_dataframe.loc[len(global_dataframe)] = [workload_num, query_num, num_workers, min(latencies), max(latencies), statistics.median(latencies), statistics.mean(latencies), statistics.stdev(latencies), pd.Timestamp(datetime.now())]
    # global_dataframe = pd.concat([global_dataframe, pd.Series([workload_num, query_num, num_workers, min(latencies), max(latencies), statistics.median(latencies), statistics.mean(latencies), statistics.stdev(latencies)])], axis=1)
  except KeyboardInterrupt:
    try:
            sys.exit(0)
    except SystemExit:
            os._exit(0)
  except Exception as e:
    logger.warning("{}".format(e))
    conn.rollback()

 

def drop_tables(table_name):
  global conn
  drop = "DROP TABLE " + table_name
  print('Attempt Drop Table ' + table_name)
  try: 
    cursor = conn.cursor() 
    cursor.execute(drop)
    conn.commit()
  except Exception as e:
    logger.warning("{}".format(e))
    conn.rollback()


def get_consecutive_days(list_of_stock_paths, total_size):
  global conn 

  rows_per_df = total_size // len(list_of_stock_paths)
  print("Rows per stock to be inserted: " + str(rows_per_df))
  ins = "INSERT INTO {} (time, open, high, low, close, adj_close, volume, symbol) VALUES (%s,%s,%s,%s,%s,%s,%s, %s)".format(table)

  for each in list_of_stock_paths:
    df = pd.read_csv(each)
    itemBank = list(zip(*map(df.get, df)))
    # print(itemBank[:rows_per_df])
    itemBank =  itemBank[:rows_per_df]
    #insert at once with python library
    cursor = conn.cursor() 
    try:
      # s = time.time()
      cursor.executemany(ins, itemBank)
      conn.commit()
      # print(" executemany time:", time.time() - s)
    except Exception as e:
      print(e)
      conn.rollback()
    
# '../stock_csv/ALLE.csv'
# '../stock_csv/A.csv'
# '../stock_csv/AAL.csv'
# '../stock_csv/AAP.csv'
# '../stock_csv/AAPL.csv'

# inject_many('../stock_csv/ALLE.csv')
# inject_many('../stock_csv/A.csv')
# inject_many('../stock_csv/AAL.csv')
# inject_many('../stock_csv/AAP.csv')
# inject_many('../stock_csv/AAPL.csv')

if __name__ == "__main__":
  print('Starting TimescaleDB Tests')
 
  print('Stock Paths: ' + ', '.join(stock_paths))
  
  # connect to Database
  connect()

  # # Try Drop Table Before Start
  # drop_tables('{}'.format(table))

  # # # # create table if not exist
  # cursor = conn.cursor()
  # create_tables(cursor)
  # conn.commit()
  
  # # # Load Data into Table using Threadpool with batch size and 4
  # load_threadpool(stock_paths,0,4,1)

  # 
  print("======== Workload 1 ======== \n")
  load_size = [1000,5000,10000]
  num_workers = [1,5,10,20]
  # load_size = [10000]
  # num_workers = [5]
  try:


    # 1 stock
    # 5 stock
    # 10 stock
    

    s = [['../stock_data/ALLE.csv'], [
      '../stock_data/A.csv',
      '../stock_data/ALLE.csv',
      '../stock_data/AAL.csv',
      '../stock_data/AAP.csv',
      '../stock_data/ABBV.csv'
    ], stock_paths]

    # s = [['../stock_data/ALLE.csv']]

    for i in range(1,6):
      for each in s:
        for eachWorkerSize in num_workers:
          # drop table before start
          drop_tables('{}'.format(table))
          #create table
          cursor = conn.cursor()
          create_tables(cursor)
          conn.commit()
          # test load
          load_threadpool(each,0,eachWorkerSize,i)

    # for eachLoadSize in load_size:
    #   for eachWorkerSize in num_workers:
    #     # drop table before start
    #     drop_tables('{}'.format(table))
    #     #create table
    #     cursor = conn.cursor()
    #     create_tables(cursor)
    #     conn.commit()
    #     # test load
    #     load_threadpool(stock_paths,eachLoadSize,eachWorkerSize)
    
    print("======== Workload 2 ======== \n")
    # Workload 2: Each thread or client executes the same query 


    # for i in range(1,5):
    #   print("+++======== {} Worker ========+++ \n".format(i))

    #   print("==== Query 1 ==== \n")
    #   run_query(agg_max_stock_week_one_month,i,2,1)
    #   print(' ')

    #   print("==== Query 2 ==== \n")
    #   run_query(agg_max_stock_week_one_year,i,2,2)
    #   print(' ')

    #   print("==== Query 3 ==== \n")
    #   run_query(agg_max_stock_week_five_one_month,i,2,3)
    #   print(' ')

    #   print("==== Query 4 ==== \n")
    #   run_query(agg_max_stock_week_five_one_year,i,2,4)
    #   print(' ')

    #   print("==== Query 5 ==== \n")
    #   run_query(agg_max_stock_week_all_one_year ,i,2,5)
    #   print(' ')

    #   print("==== Query 6 ==== \n")
    #   run_query(agg_avg_stock_week_one_month,i,2,6)
    #   print(' ')

    #   print("==== Query 7 ==== \n")
    #   run_query(agg_avg_stock_week_all_one_month,i,2,7)
    #   print(' ')
      


    # print("==== Workload 3 ====")
    # for i in range(1,5):
    #   print("+++======== {} Worker ========+++ \n".format(i))
    #   run_query(workload_three_query, i, 3, 1)
      
      
    # print("==== Workload 4 ==== \n")

    # # test query with 1000, 5000, 100000 data points
    # # 1000 / len(stocks_path) = rows per stock added to the table

    # num_data_points_list = [1000,5000,10000]
    # for each in num_data_points_list:
    #   print("==== TEST {} DATA POINTS ==== \n".format(each))

    #   drop_tables('{}'.format(table))
    #   # # create table if not exist
    #   cursor = conn.cursor()
    #   create_tables(cursor)
    #   conn.commit()
    #   get_consecutive_days(stock_paths, each)
    #   cursor.execute(query_total_records)
    #   total_records_count = int(cursor.fetchall()[0][0])
    #   print("Loaded: Total Data Points: " + str(total_records_count))

    #   for i in range(1,5):
    #     print("+++======== {} Worker ========+++ \n".format(i))
    #     run_query(workload_four_query, i, 4, 1)
    #     # break
    
    # # close connection
    # conn.close()

    # # export global dataframe to csv 
    # global_dataframe.to_csv('timescaleDB_queryStats.csv', index=False)
    load_dataframe.to_csv('timescaleDB_loadStats.csv', index=False)
  except KeyboardInterrupt:
      try:
              sys.exit(0)
      except SystemExit:
              os._exit(0)

