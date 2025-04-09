import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt

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
        Perform descriptive analysis on the DataFrame.
        :param df: Pandas DataFrame containing transaction data.
        """
        try:
            print("\n--- Descriptive Analysis ---")
            
            # Basic statistics
            print("\nBasic Statistics:")
            print(df.describe())
            
            # Transaction type distribution
            print("\nTransaction Type Distribution:")
            transaction_type_counts = df['transaction_type'].value_counts()
            print(transaction_type_counts)
            
            # Trend analysis (amount over time)
            print("\nTrend Analysis (Amount Over Time):")
            df['date'] = pd.to_datetime(df['date'])  # Convert date column to datetime
            trend_data = df.groupby('date')['amount'].sum()
            print(trend_data)

            # Visualization
            FinanceDataAnalysis.visualize_data(transaction_type_counts, trend_data)
        
        except Exception as e:
            print(f"Error during descriptive analysis: {str(e)}")

    @staticmethod
    def visualize_data(transaction_type_counts, trend_data):
        """
        Visualize transaction type distribution and trends over time.
        :param transaction_type_counts: Series containing transaction type counts.
        :param trend_data: Series containing amount trends over time.
        """
        
        try:
            # Transaction type distribution (bar chart)
            plt.figure(figsize=(8, 5))
            transaction_type_counts.plot(kind='bar', color=['blue', 'orange'])
            plt.title("Transaction Type Distribution")
            plt.xlabel("Transaction Type")
            plt.ylabel("Count")
            plt.xticks(rotation=0)
            plt.tight_layout()
            
            # Save and show plot
            plt.savefig("transaction_type_distribution.png")
            plt.show()

            # Trend analysis (line chart)
            plt.figure(figsize=(10, 6))
            trend_data.plot(kind='line', color='green')
            plt.title("Transaction Amount Trend Over Time")
            plt.xlabel("Date")
            plt.ylabel("Total Amount ($)")
            
            # Save and show plot
            plt.savefig("trend_analysis.png")
            plt.show()
        
        except Exception as e:
            print(f"Error during visualization: {str(e)}")

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
            FinanceDataAnalysis.perform_descriptive_analysis(data)
