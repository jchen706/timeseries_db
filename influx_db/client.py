from datetime import datetime
import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS, ASYNCHRONOUS
from datetime import datetime
import csv
import pandas as pd
import concurrent
import _thread

token = "xDfhQrrKUo7vZAZgGEk3lZCLCJcQ2oC8l7GAxHBw6kw-Ghh8ai_D-s8hgJkdL7TH9uakMKRi2U0jJw8oQOzWuQ=="
org = "gt"
bucket = "new1"

class InfluxClient:
    def __init__(self,token,org,bucket): 
        self._org=org 
        self._bucket = bucket
        self._client = InfluxDBClient(url="http://localhost:8086", token=token)
        # self.write_api = self.client.write_api(
        #     write_options=WriteOptions(write_type=WriteType.batching, batch_size=50_000, flush_interval=10_000))

    def write_data(self,data,write_option=ASYNCHRONOUS):
      write_api = self._client.write_api(write_option)
      start_time = datetime.now()
      write_api.write(self._bucket, self._org, data, data_frame_measurement_name='Symbol')
      total_time = datetime.now() - start_time
      print(total_time)

    def query_data(self,query):
      query_api = self._client.query_api()
      result = query_api.query(org=self._org, query=query)
      results = []
      for table in result:
        for record in table.records:
          results.append((record.get_value(), record.get_field()))
      return results 
    

def multithread(df, client):
  client.write_data(df)
  
def write(data, client):
  client.write(data)

def main():
  # IC = InfluxClient(token,org,bucket)
  
  # IC2 = InfluxClient(token,org,bucket)
  _now = datetime.utcnow()
  df = pd.read_csv('../stock_csv/ALLE.csv', header=0)
  df = df.set_index("Date")

  client = InfluxDBClient(url="http://localhost:8086", token=token)
  _thread.start_new_thread(write, (df, client))
  

  
  
  # IC.write_data(df)
  # IC2.write_data(df)
  # num_clients = 2
  # multithread(df, num_clients)
  # _thread.start_new_thread(IC.write_data, (df,))
  # _thread.start_new_thread(IC2.write_data, (df,))
  # load_data(df, num_clients)

  # multi_thread(IC, csv_reader, csv_rows, 5)


if __name__ == "__main__":
  main()  

