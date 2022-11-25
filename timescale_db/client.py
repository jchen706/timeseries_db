"""
Connect to timescale db
"""
# from distutils import command
import psycopg2
from psycopg2.extras import RealDictCursor

def create_tables(cursor):
  # create regular postresql table
  commands = (
    """
    CREATE TABLE IF NOT EXISTS stocks (
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
  create_ext = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;"
  cursor.execute(create_ext)
  

  # create hypertable
  query_create__hypertable = "SELECT create_hypertable('stock_data', 'time', if_not_exists => true);"
  cursor.execute(query_create__hypertable)

  
  # create table company
  # query_create_company_table = """CREATE TABLE company (
  #                                          symbol TEXT NOT NULL,
  #                                          name text NOT NULL,
  #                                          );"""

# postgres
# docker exec -u root -i -t timescaledb /bin/bash  
CONNECTION = "postgres://postgres:123@localhost:5432/postgres"
def connect():  
    conn = psycopg2.connect(CONNECTION)
    cursor = conn.cursor()
    # use the cursor to interact with your database
    cursor.execute("SELECT 'hello world'")
    print(cursor.fetchone())

    # create_tables(cursor)
    # conn.commit()


"""
Hypertable

Chunks: child tables of hypertable 

chunks are broken up by time

chunk time interval 

partitioning by time

faster when query or update data by time

each junk has only 1 day interval period

Inject data timescale db
"""
def inject():
  print('injecting data')

  # time
  # multithreaded inject data







"""
Query Data From data timescale db
"""
def query():
  print('query')

if __name__ == "__main__":
  print('connect')
  connect()