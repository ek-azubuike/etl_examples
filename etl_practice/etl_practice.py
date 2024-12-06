import glob
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET

# create target file and log file
log_file = "log_file.txt"
target_file = "transformed_data.csv"

# extract

def extract_csv(file):
    df = pd.read_csv(file)
    return df

def extract_json(file):
    df = pd.read_json(file, lines = True)
    return df

def extract_xml(file):
    df = pd.DataFrame(columns = ["car_model",
                                 "year_of_manufacture",
                                 "price",
                                 "fuel"])
    tree = ET.parse(file)
    root = tree.getroot()
    
    for car in root:
        model = car.find("car_model").text
        year = car.find("year_of_manufacture").text
        price = float(car.find("price").text)
        fuel = car.find("fuel").text
        pd.concat([df, 
                  pd.DataFrame([{"car_model" : model,
                                "year_of_manufacture" : year,
                                "price" : price,
                                "fuel" : fuel}])],
                  ignore_index = True)
    return df
 
def extract():
    # create df to hold extracted data
    extracted_data = pd.DataFrame(columns = ["car_model",
                                             "year_of_manufacture",
                                             "price",
                                             "fuel"])
    # identify appropropriate extraction function
    for csv in glob.glob("*.csv"):
        df = extract_csv(csv)
        pd.concat([extracted_data, 
                  df], 
                  ignore_index = True)
    
    for json in glob.glob("*.json"):
        df = extract_json(json)
        extracted_data = pd.concat([extracted_data,
                                   df],
                                   ignore_index = True)
    
    for xml in glob.glob("*.xml"):
        df = extract_xml(xml)
        extracted_data = pd.concat([extracted_data,
                                   df],
                                   ignore_index = True)
        
    return extracted_data

def transform(df):
    
    df["price"] = round(df["price"], 2)
    
    return df

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index = False)
    
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as file:
        file.write(timestamp + "," + message + "\n")
        
# test and log ETL process
log_progress("ETL Job Started")

# extraction
log_progress("Beginning Extraction")
extracted_data = extract()
log_progress("End Extraction")
print("Extracted Data")
print(extracted_data)

# transformation
log_progress("Beginning Transformation")
transformed_data = transform(extracted_data)
log_progress("End Transformation")
print("Transformed Data")
print(transformed_data)

# loading
log_progress("Beginning Loading")
load_data(target_file, transformed_data)
log_progress("Loaded Data")
print("Finalized File: {}".format(target_file))

log_progress("ETL Job Ended Successfully")