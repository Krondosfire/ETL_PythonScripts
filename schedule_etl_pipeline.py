# Description: This script schedules the ETL pipeline to run daily using the schedule library.
import requests
import pandas as pd
from sqlalchemy import create_engine

# Extract logs from API
response = requests.get('https://api.spotify.com/v1/logs/yesterday')
logs = response.json()

# Transform logs into aggregated play counts per artist
df = pd.DataFrame(logs)
aggregated_df = df.groupby('artist').sum().reset_index()

# Load into PostgreSQL database
engine = create_engine('postgresql://user:password@localhost/dbname')
aggregated_df.to_sql('daily_play_counts', engine, if_exists='replace', index=False)
print("ETL process completed")
