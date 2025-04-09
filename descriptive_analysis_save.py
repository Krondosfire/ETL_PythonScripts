import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

class FinanceDataAnalysis:
    @staticmethod
    def connect_to_database():
        """
        Create a connection to the PostgreSQL database using SQLAlchemy.
        :return: SQLAlchemy engine object.
        """
        user = "postgres"
        password = "admin"
        host = "localhost"
        port = "5432"
        database = "etl_pipeline"
        
        try:
            engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
            print("Connected to the database successfully.")
            return engine
        except Exception as e:
            print(f"Error connecting to the database: {str(e)}")
            return None

    @staticmethod
    def load_data(engine, table_name):
        """
        Load data from the specified SQL table into a Pandas DataFrame.
        :param engine: SQLAlchemy engine object.
        :param table_name: Name of the table to query.
        :return: Pandas DataFrame containing the table data.
        """
        try:
            query = f"SELECT * FROM {table_name};"
            df = pd.read_sql_query(query, con=engine)
            print(f"Data loaded successfully from '{table_name}' table.")
            return df
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return None

    @staticmethod
    def perform_descriptive_analysis(df):
        """
        Perform descriptive analysis on the DataFrame and return results as a string.
        :param df: Pandas DataFrame containing transaction data.
        :return: String containing descriptive analysis results.
        """
        try:
            results = []
            
            # Basic statistics
            results.append("\n--- Basic Statistics ---\n")
            results.append(str(df.describe()))
            
            # Transaction type distribution
            results.append("\n--- Transaction Type Distribution ---\n")
            transaction_type_counts = df['transaction_type'].value_counts()
            results.append(str(transaction_type_counts))
            
            # Trend analysis (amount over time)
            df['date'] = pd.to_datetime(df['date'])  # Convert date column to datetime
            trend_data = df.groupby('date')['amount'].sum()
            
            results.append("\n--- Trend Analysis (Amount Over Time) ---\n")
            results.append(str(trend_data))
            
            return "\n".join(results)
        
        except Exception as e:
            print(f"Error during descriptive analysis: {str(e)}")
            return None

    @staticmethod
    def save_results_to_file(results):
        """
        Save descriptive analysis results to a file with a timestamped filename.
        :param results: String containing descriptive analysis results.
        """
        try:
            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"descriptive_analysis_{timestamp}.txt"
            
            # Save results to file
            with open(filename, "w") as file:
                file.write(results)
            
            print(f"Results saved successfully to {filename}")
        
        except Exception as e:
            print(f"Error saving results to file: {str(e)}")

# Main script execution
if __name__ == "__main__":
    table_name = "finance_transactions"
    
    # Step 1: Connect to the database
    engine = FinanceDataAnalysis.connect_to_database()
    
    if engine is not None:
        # Step 2: Load data from the table
        data = FinanceDataAnalysis.load_data(engine, table_name)
        
        if data is not None:
            # Step 3: Perform descriptive analysis
            analysis_results = FinanceDataAnalysis.perform_descriptive_analysis(data)
            
            if analysis_results is not None:
                # Step 4: Save results to a timestamped file
                FinanceDataAnalysis.save_results_to_file(analysis_results)
