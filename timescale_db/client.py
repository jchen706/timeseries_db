"""
Connect to timescale db
"""
# from distutils import command
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

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def create_tables(cursor):
  # create regular postresql table
  commands = (
    """
    CREATE TABLE IF NOT EXISTS stocks3 (
      time DATE NOT NULL,
      symbol TEXT NOT NULL,
      open DOUBLE PRECISION NULL,
      high DOUBLE PRECISION NULL,
      low DOUBLE PRECISION NULL,
      close DOUBLE PRECISION NULL,
      adj_close DOUBLE PRECISION NULL,
      volume INT NULL
    )
    """
  )
  cursor.execute(commands)
  create_ext = "CREATE EXTENSION IF NOT EXISTS timescaledb;"
  cursor.execute(create_ext)
  
  # create hypertable
  query_create__hypertable = "SELECT create_hypertable('stocks3', 'time', if_not_exists => true);"
  cursor.execute(query_create__hypertable)
 
CONNECTION = "postgres://postgres:123@localhost:5432/postgres"
def connect():  
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
    tcp = ThreadedConnectionPool(1, 10, dbname="postgres",
      user="postgres",
      password="123",
      host="localhost",
      port="5432",
      keepalives=1,
      keepalives_idle=30,
      keepalives_interval=10,
      keepalives_count=5 )
    conn = tcp.getconn()
    cursor = conn.cursor()


    # use the cursor to interact with your database
    cursor.execute("SELECT 'hello world'")
    print(cursor.fetchone())

    create_tables(cursor)
    conn.commit()

    # insert data
    # inject(conn)

    #query data
    query(conn)

    conn.close()

def inject_thread(rowlist, conn):
  # print(rowlist)
  ins = "INSERT INTO stocks3 (time, open, high, low, close, adj_close, volume, symbol) VALUES (%s,%s,%s,%s,%s,%s,%s, %s)"
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


def inject_many(conn):
  print('injecting data with many')
  #insert data into the table
  df = pd.read_csv('../stock_csv/ALLE.csv')
  print(df)
  print(len(df))
  itemBank = list(zip(*map(df.get, df)))
  print(itemBank)
  # insert at once with python library
  ins = "INSERT INTO stocks (time, open, high, low, close, adj_close, volume, symbol) VALUES (%s,%s,%s,%s,%s,%s,%s, %s)"
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
def inject(conn):
  print('injecting data')

  #insert data into the table
  df = pd.read_csv('../stock_csv/ALLE.csv')
  print(df)
  print(len(df))
  itemBank = list(zip(*map(df.get, df)))

  print("Running threaded:")
  threaded_start = time.time()
  futures = []
  with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
      
      for i in range(0,len(df), 5):
          # 5 item at a time
          print(i,i+5)
          if (i+5 > len(df)):
            futures.append(executor.submit(inject_thread,rowlist=itemBank[i:len(df)],conn=conn))
          else:
            futures.append(executor.submit(inject_thread,rowlist=itemBank[i:i+5],conn=conn))
      # for future in concurrent.futures.as_completed(futures):
      #     pass
          # print(future.result())
  concurrent.futures.wait(futures,return_when=ALL_COMPLETED)
  print("Threaded time:", time.time() - threaded_start)

"""
Query Data From data timescale db
"""
def query(conn):
  print('query')
  cursor = conn.cursor() 

  # Simple aggregate max for a stock, every week for 1 month
  agg_max_stock_week_one_month = """
  select time_bucket('7 day', time) as bucket, max(close),symbol
  from stocks3
  where time > now() - Interval '1 month'
  group by bucket, symbol
  order by bucket, symbol
  """
  s = time.time()
  cursor.execute(agg_max_stock_week_one_month)
  print(" agg time:", time.time() - s)

  results = cursor.fetchall()
  print(results)

  # Simple aggregate max for a stock, every week for 1 year

  # Simple aggregate max on 5 stocks, every week for 1 month

  # Simple aggregate max on 5 stocks, every week for 1 year

  # Aggregate max across all stocks per week over 1 week

  # Compute the average of a stock per week for 1 month

  # Compute the average of all stocks per week for 1 month


  # moving average

  """
  SELECT time, AVG(close) OVER(ORDER BY time
      ROWS BETWEEN 9 PRECEDING AND CURRENT ROW)
    AS ten_day_avg_close
  FROM stocks3
  WHERE time > NOW() - INTERVAL '1 month'
  ORDER BY time DES
  """


  # automatic refresh policy



  

if __name__ == "__main__":
  print('connect')
  connect()