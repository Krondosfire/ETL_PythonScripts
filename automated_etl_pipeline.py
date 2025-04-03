# An example of automating an ETL pipeline that extracts data from an API, transforms it, and loads it into a database
import requests
import pandas as pd
from sqlalchemy import create_engine

def extract_data():
    response = requests.get('https://api.example.com/data')
    return response.json()

def transform_data(raw_data):
    df = pd.DataFrame(raw_data)
    df['date'] = pd.to_datetime(df['date'])
    df['value'] = df['value'] * 1000  # Example transformation
    return df

def load_data(transformed_data):
    engine = create_engine('postgresql://user:password@localhost/dbname')
    transformed_data.to_sql('target_table', engine, if_exists='replace', index=False)

def run_etl_pipeline():
    raw_data = extract_data()
    transformed_data = transform_data(raw_data)
    load_data(transformed_data)

# Schedule the pipeline to run daily using cron or time module
run_etl_pipeline()
