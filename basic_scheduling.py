# Basic Scheduling with Python
import time

def run_etl_pipeline():
    # Your ETL steps here
    pass

# Run every 24 hours
while True:
    run_etl_pipeline()
    time.sleep(86400)  # Wait for one day before running again
