import csv
import random
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, Date, Numeric, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FinanceTransaction(Base):
    __tablename__ = "finance_transactions"
    
    transaction_id = Column(Integer, primary_key=True)
    date = Column(Date)
    account_id = Column(Integer)
    amount = Column(Numeric(10, 2))
    transaction_type = Column(String(10))

class FinanceDataGenerator:
    @staticmethod
    def generate_csv(file_path, num_rows=100):
        """
        Generate a sample finance dataset and save it as a CSV file.
        :param file_path: Path to save the CSV file.
        :param num_rows: Number of rows to generate.
        """
        headers = ["transaction_id", "date", "account_id", "amount", "transaction_type"]
        transaction_types = ["credit", "debit"]

        with open(file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(headers)

            for i in range(1, num_rows + 1):
                transaction_id = i
                date = f"2023-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
                account_id = random.randint(1000, 9999)
                amount = round(random.uniform(10.0, 1000.0), 2)
                transaction_type = random.choice(transaction_types)
                writer.writerow([transaction_id, date, account_id, amount, transaction_type])

        print(f"Sample finance data CSV file created at {file_path}")

class PostgreSQLLoader:
    @staticmethod
    def get_engine():
        """
        Create an SQLAlchemy engine for PostgreSQL.
        :return: SQLAlchemy engine object.
        """
        user = "postgres"
        password = "admin"  # Replace with your PostgreSQL password
        host = "localhost"
        port = "5432"
        database = "etl_pipeline"
        return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

    @staticmethod
    def create_table(engine):
        """
        Create a new table in the PostgreSQL database using ORM.
        :param engine: SQLAlchemy engine object.
        """
        Base.metadata.create_all(engine)  # Automatically creates tables defined in ORM classes
        print("Table 'finance_transactions' created successfully.")

class ETLPipeline:
    @staticmethod
    def load_data_to_table(engine, file_path):
        """
        Load data from a CSV file into the PostgreSQL table.
        :param engine: SQLAlchemy engine object.
        :param file_path: Path to the CSV file.
        """
        try:
            df = pd.read_csv(file_path)
            df.to_sql(FinanceTransaction.__tablename__, con=engine, if_exists='replace', index=False)
            print(f"Data loaded successfully into '{FinanceTransaction.__tablename__}' table.")
        except Exception as e:
            print(f"Error loading data: {str(e)}")

# Main script execution
if __name__ == "__main__":
    # Step 1: Generate Sample Finance Data CSV File
    csv_file = "finance_data.csv"
    FinanceDataGenerator.generate_csv(csv_file)

    # Step 2: Connect to PostgreSQL Database and Create Table
    engine = PostgreSQLLoader.get_engine()
    PostgreSQLLoader.create_table(engine)

    # Step 3: Load Data into PostgreSQL Table
    ETLPipeline.load_data_to_table(engine, csv_file)
