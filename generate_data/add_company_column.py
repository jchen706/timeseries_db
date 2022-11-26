import pandas as pd
import sys 
import os
pd.options.display.max_rows = 10

import os

arr = os.listdir('../stock_data')

def add_company():
  df = pd.read_csv('../stock_csv/ALLE.csv')
  print(df) 
  df['Symbol'] = "ALLE"
  df.to_csv('ALLE.csv', encoding='utf-8', index=False)

def add_company_folder():
  for each in arr:
    name = each.split('.')[0]
    df = pd.read_csv('../stock_data/'+each)
    df['Symbol'] = name
    df.to_csv('../stock_csv/'+each, encoding='utf-8', index=False)


if __name__ == "__main__":
  # getData()
  try:
    add_company_folder()
    print("Start Data Collection")
  except KeyboardInterrupt:
    try:
            sys.exit(0)
    except SystemExit:
            os._exit(0)
