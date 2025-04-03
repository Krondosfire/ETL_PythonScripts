# Description: This script extracts data from CSV, JSON, and XML files, transforms it into a DataFrame, and saves it as a CSV file.
import glob
import pandas as pd
import xml.etree.ElementTree as ET

def extract_from_csv(file):
    return pd.read_csv(file)

def extract_from_json(file):
    return pd.read_json(file, lines=True)

def extract_from_xml(file):
    tree = ET.parse(file)
    root = tree.getroot()
    
    rows = []
    for record in root:
        row = {
            "car_model": record.find("car_model").text,
            "year_of_manufacture": int(record.find("year_of_manufacture").text),
            "price": float(record.find("price").text),
            "fuel": record.find("fuel").text,
        }
        rows.append(row)
    
    return pd.DataFrame(rows)

def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    
    for file in glob.glob("data/*.csv"):
        extracted_data = extracted_data.append(extract_from_csv(file), ignore_index=True)
    
    for file in glob.glob("data/*.json"):
        extracted_data = extracted_data.append(extract_from_json(file), ignore_index=True)
    
    for file in glob.glob("data/*.xml"):
        extracted_data = extracted_data.append(extract_from_xml(file), ignore_index=True)
    
    return extracted_data

extracted_data = extract()
extracted_data.to_csv("transformed_data.csv", index=False)
