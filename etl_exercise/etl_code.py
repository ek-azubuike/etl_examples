import glob
import xml.etree.ElementTree as ET 
import pandas as pd
from datetime import datetime

# create file paths for log file and transformed data
log_file = "log_file.txt"
target_file = "transformed_data.csv"

# extraction
def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df

def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df

def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns = ["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        df = pd.concat(objs = [df, 
                               pd.DataFrame([{"name" : name,
                                              "height" : height, 
                                              "weight" : weight}])],
                       ignore_index = True)
        return df

# identify the appropriate function to call on each file
def extract():
    # create empty df to hold extracted data
    extracted_data = pd.DataFrame(columns = ["name", "height", "weight"])
    
    # process csv files
    for csv_file in glob.glob("*.csv"):
        extracted_data = pd.concat(objs = [extracted_data,
                                           extract_from_csv(csv_file)],
                                   ignore_index = True)
    
    # process json files
    for json_file in glob.glob("*.json"):
        extracted_data = pd.concat(objs = [extracted_data,
                                           extract_from_json(json_file)],
                                   ignore_index = True)
    
    # process xml files
    for xml_file in glob.glob("*.xml"):
        extracted_data = pd.concat(objs = [extracted_data,
                                           extract_from_xml(xml_file)],
                                   ignore_index = True)
    
    return extracted_data

# transform
def transform(df):
    # convert inches to centimeters
    df['height'] = round(df['height'] * 0.0254, 2)
    
    # convert pounds to kilos
    df['weight'] = round(df['weight'] * 0.45359237, 2)
    
    return df

# load data
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index = False)
    
# logging to record progress
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
