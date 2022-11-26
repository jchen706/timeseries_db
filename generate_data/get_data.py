"""
downloads 2000 tickers of csv data from yahoo.finance local using selenium and panda csv parsing


"""
import os 
import selenium 
from selenium import webdriver
import sys
import traceback
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import csv
import pandas
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
import time

#intialize drivers
from selenium.webdriver.firefox.options import Options

options = Options()
# options.binary_location = r'C:\Program Files\Mozilla Firefox\firefox.exe'
options.add_argument("--headless")
# set Download Location
options.set_preference("browser.download.folderList", 2)

options.set_preference("browser.download.dir", r"D:\a_timeseries")

# prefs = {"profile.default_content_settings.popups": 0,    
#         "download.default_directory":r"D:\a_timeseries", ### Set the path accordingly
#         "download.prompt_for_download": False, ## change the downpath accordingly
#         "download.directory_upgrade": True}
# options.add_experimental_option("prefs", prefs)
driver = webdriver.Firefox(service=Service(executable_path=GeckoDriverManager().install()), options=options)

# driver = webdriver.Firefox(executable_path=r"C:\Users\jchen\Downloads\geckodriver-v0.32.0-win32\geckodriver.exe", options=options)
driver.implicitly_wait(15)

static_url = 'https://finance.yahoo.com/quote/'

end_url = '/history?p='

# https://datahub.io/core/s-and-p-500-companies#data
def getData(ticker = ''):
  try:
    # print('here')
    # sample url https://finance.yahoo.com/quote/WOLF/history?p=WOLF
    if ticker == '':
      exit(1)
    driver.get(static_url+ticker+end_url+ticker)
    driver.implicitly_wait(5)
    time.sleep(2)
    try:
      WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, '/html/body/div[1]/div/div/div[1]/div/div[4]/div/div/div[1]/div/div/div/div/div/section/button[1]')))
      sign_x = driver.find_element('xpath','/html/body/div[1]/div/div/div[1]/div/div[4]/div/div/div[1]/div/div/div/div/div/section/button[1]')
      sign_x.click()
    except:
      print('pass signx')
      pass
    # click the date range  11-22- .....
    # /html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/div[1]/div/div/div/span
    date_link = driver.find_element('xpath','/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/div[1]/div/div/div/span')
    date_link.click()

    max_button = driver.find_element('xpath','/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/div[1]/div/div/div[2]/div/ul[2]/li[4]/button')
    max_button.click()

    driver.implicitly_wait(5)

    try:
      max_button = driver.find_element('xpath','/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/div[1]/div/div/div[2]/div/ul[2]/li[4]/button')
      max_button.click()
    except:
      pass

    # done_date = driver.find_element('xpath','/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/div[1]/div/div/div[2]/div/div[3]/button[1]')
    # done_date.click()

    # apply_btn = driver.find_element('xpath','/html/body/div[1]/div/div/div[1]/div/div[3]/div[1]/div/div[2]/div/div/section/div[1]/div[1]/button')
    # apply_btn.click()

    driver.implicitly_wait(5)
    time.sleep(1)
    wait = WebDriverWait(driver, 3)
    element = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, 'Download')))
    download = driver.find_element('xpath',"//*[contains(text(),'Download')]")
    download.click()
    time.sleep(2)
                   
  except KeyboardInterrupt:
    try:
            sys.exit(0)
    except SystemExit:
            os._exit(0)

  except:
      ex_type, ex_value, ex_traceback = sys.exc_info()

      # Extract unformatter stack traces as tuples
      trace_back = traceback.extract_tb(ex_traceback)

      # Format stacktrace
      stack_trace = list()

      for trace in trace_back:
          stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))

      print("Exception type : %s " % ex_type.__name__)
      print("Exception message : %s" %ex_value)
      print("Stack trace : %s" %stack_trace)
      return None

def write_done(done_set):
  with open('done.csv', mode='a', newline='') as csv_file:
    fieldnames = ['Symbol']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

    # writer.writeheader()
    for each in done_set:
     writer.writerow({'Symbol': each})

# BLL ANTM

def loopThroughCompanies():
  print('loop')
  done_set = set()
  added_set = set()
  try:
    with open('done.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                line_count += 1
                done_set.add(row[0])
  except KeyboardInterrupt:
    try:
            sys.exit(0)
    except SystemExit:
            os._exit(0)

  try:
    with open('constituents_csv.csv') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                print(f'Column names are {", ".join(row)}')
                line_count += 1
            else:
                print(f'\t{row[0]}  {row[1]}  {row[2]}.')
                line_count += 1
                if row[0] in done_set:
                  print("done")
                else:
                  getData(row[0])
                  added_set.add(row[0])
            
                if(line_count % 5):
                  write_done(added_set)
                  added_set.clear()
        

        print(f'Processed {line_count} lines.')
       
  except KeyboardInterrupt:
    try:
            write_done(added_set)
            sys.exit(0)
    except SystemExit:
            os._exit(0)

  # write done set to done csv


   





if __name__ == "__main__":
  # getData()
  try:
    loopThroughCompanies()
    print("Start Data Collection")
  except KeyboardInterrupt:
    try:
            sys.exit(0)
    except SystemExit:
            os._exit(0)






