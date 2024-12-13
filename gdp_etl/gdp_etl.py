# -*- coding: utf-8 -*-
"""
Created on Fri Dec  6 05:54:21 2024

@author: ekazu
"""
# Importing the required libraries

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime

# initialize entities

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['country', 'gdp_usd_millions']
db_name = 'world_economies.db'
table_name = 'countries_by_gdp'
csv_path = './countries_by_gdp.csv'
log_file = 'gdp_etl_log.txt'
query = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"

# Code for ETL operations on Country-GDP data

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    
    # initialize df
    df = pd.DataFrame(columns=table_attribs)
    # load webpage
    html_page = requests.get(url).text
    html_data = BeautifulSoup(html_page, 'html.parser')
    # select gdp table
    tables = html_data.find_all('tbody')
    rows = tables[2].find_all('tr')
    # iterate over rows
    for row in rows:
        # select all cells
        cols = row.find_all('td')
        # ensure cell isn't empty and contains a hyperlink
        if (len(cols) != 0) and ('—' not in cols[2]) and (cols[0].find_all('a') is not None):
            # select link in first column
            links = cols[0].find_all('a')
            for item in links:
                # extract country name and gdp from row, load into df
                gdp_dict = {'country': item.text,
                            'gdp_usd_millions' : cols[2].text}
                df = pd.concat([df, pd.DataFrame(gdp_dict, index=[0])], ignore_index=True)
    
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    
    # convert strings to floats
    df['gdp_usd_millions'] = df['gdp_usd_millions'].str.replace(',', '').astype(float)
    # convert to billions of dollars and change column name 
    df['gdp_usd_millions'] = round(df['gdp_usd_millions'] / 1000, 2)
    df.rename(columns={'gdp_usd_millions': 'gdp_usd_billions'}, inplace=True)
    
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    
    df.to_csv(csv_path)
    
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    
    df.to_sql(table_name,
              sql_connection,
              if_exists='replace',
              index=False)
    
def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    print(query_statement)
    print(pd.read_sql(query_statement, sql_connection))
    
def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. 
    Function returns nothing'''
    
    dt_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(dt_format)
    
    with open(log_file, "a") as file:
        file.write(timestamp + " : " + message + '\n')
    
''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# start ETL
log_progress('ETL Job Started')

# extraction
log_progress('Data Extraction Underway...')
extracted_data = extract(url, table_attribs)
log_progress('Data Extraction Complete!')

# transformation
log_progress('Data Transformation Underway...')
transformed_data = transform(extracted_data)
log_progress('Transformation Complete!')

# loading
log_progress('Data Loading Underway...')
# load to CSV
load_to_csv(transformed_data, csv_path)
log_progress('Data Loaded to CSV File!')
# load to db
conn = sqlite3.connect(db_name)
load_to_db(transformed_data, conn, table_name)
log_progress('Data Loaded to DB!')

# querying
log_progress('Querying DB...')
run_query(query, conn)
conn.close()
log_progress('Query Executed!')

# finish job
print("Finalized File: {}".format(csv_path))

log_progress("ETL Job Ended Successfully")