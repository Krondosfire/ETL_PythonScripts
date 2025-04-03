# Description: This script extracts data from a CSV file, transforms it, and loads it into a MongoDB database.
import pandas as pd
from pymongo import MongoClient

# Extract
data = pd.read_csv('source_file.csv')

# Transform
data['price'] = data['price'].apply(lambda x: round(x * 1.1))  # Example transformation

# Load
client = MongoClient('mongodb://localhost:27017/')
db = client['etl_database']
collection = db['transformed_data']

collection.insert_many(data.to_dict('records'))
print("Data loaded into MongoDB")
