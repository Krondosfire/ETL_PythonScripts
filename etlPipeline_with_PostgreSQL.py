import csv
import pandas as pd
from sqlalchemy import create_engine

class ETLPipeline:
    @staticmethod
    def preprocess_csv(file_path):
        """Preprocess CSV to handle missing values."""
        df = pd.read_csv(file_path)
        df['age'] = df['age'].fillna(-1)  # Replace missing values with -1
        df.to_csv(file_path, index=False)
        print("CSV file preprocessed successfully.")

    @staticmethod
    def get_engine():
        """Create an SQLAlchemy engine."""
        user = "postgres"
        password = "admin"  # Replace with your PostgreSQL password
        host = "localhost"
        port = "5432"
        database = "etl_pipeline"
        return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    @staticmethod
    def load_data_to_table(engine, file_path, table_name):
        """Load data from a CSV file into PostgreSQL."""
        try:
            df = pd.read_csv(file_path)
            df.to_sql(table_name, con=engine, if_exists='replace', index=False)
            print(f"Data loaded successfully into '{table_name}' table.")
        except Exception as e:
            print(f"Error loading data: {str(e)}")

    @staticmethod
    def extract_data(engine, query):
        """Extract data from PostgreSQL using Pandas."""
        try:
            df = pd.read_sql_query(query, con=engine)
            print("Data extracted successfully.")
            return df
        except Exception as e:
            print(f"Error during extraction: {str(e)}")
            return None

# Main script execution
if __name__ == "__main__":
    csv_file = "sample_data.csv"
    
    # Step 1: Preprocess CSV file
    ETLPipeline.preprocess_csv(csv_file)

    # Step 2: Create database connection and load data
    engine = ETLPipeline.get_engine()
    
    table_name = "etl_data"
    
    # Load preprocessed data into PostgreSQL table
    ETLPipeline.load_data_to_table(engine, csv_file, table_name)

    # Step 3: Extract data from PostgreSQL for verification (optional)
    query = f"SELECT * FROM {table_name};"
    extracted_data = ETLPipeline.extract_data(engine, query)

    if extracted_data is not None:
        print(extracted_data.head())
