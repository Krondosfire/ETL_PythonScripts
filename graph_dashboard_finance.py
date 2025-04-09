import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go

class FinanceDashboard:
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
    def generate_charts(df):
        """
        Generate charts to visualize trends and KPIs.
        :param df: Pandas DataFrame containing transaction data.
        """
        try:
            # Convert 'date' column to datetime format
            df['date'] = pd.to_datetime(df['date'])

            # Transaction Type Distribution (Bar Chart)
            transaction_type_counts = df['transaction_type'].value_counts()
            plt.figure(figsize=(8, 5))
            transaction_type_counts.plot(kind='bar', color=['blue', 'orange'])
            plt.title("Transaction Type Distribution")
            plt.xlabel("Transaction Type")
            plt.ylabel("Count")
            plt.xticks(rotation=0)
            plt.tight_layout()
            plt.savefig("transaction_type_distribution.png")
            plt.show()

            # Trend Analysis (Line Chart)
            trend_data = df.groupby('date')['amount'].sum()
            plt.figure(figsize=(10, 6))
            trend_data.plot(kind='line', color='green')
            plt.title("Transaction Amount Trend Over Time")
            plt.xlabel("Date")
            plt.ylabel("Total Amount ($)")
            plt.tight_layout()
            plt.savefig("trend_analysis.png")
            plt.show()

            # Heatmap for Correlation Analysis
            correlation_data = df[['transaction_id', 'account_id', 'amount']].corr()
            plt.figure(figsize=(8, 6))
            sns.heatmap(correlation_data, annot=True, cmap="coolwarm", fmt=".2f")
            plt.title("Correlation Heatmap")
            plt.tight_layout()
            plt.savefig("correlation_heatmap.png")
            plt.show()

        except Exception as e:
            print(f"Error generating charts: {str(e)}")

    @staticmethod
    def generate_dashboard(df):
        """
        Generate an interactive dashboard using Plotly.
        :param df: Pandas DataFrame containing transaction data.
        """
        try:
            # Calculate KPIs
            total_transactions = len(df)
            total_credit = df[df['transaction_type'] == 'credit']['amount'].sum()
            total_debit = df[df['transaction_type'] == 'debit']['amount'].sum()

            # Create a Plotly dashboard
            fig = go.Figure()

            # Add KPI cards
            fig.add_trace(go.Indicator(
                mode="number",
                value=total_transactions,
                title={"text": "Total Transactions"},
                domain={'x': [0, 0.3], 'y': [0.5, 1]}
            ))

            fig.add_trace(go.Indicator(
                mode="number",
                value=total_credit,
                title={"text": "Total Credit ($)"},
                domain={'x': [0.35, 0.65], 'y': [0.5, 1]}
            ))

            fig.add_trace(go.Indicator(
                mode="number",
                value=total_debit,
                title={"text": "Total Debit ($)"},
                domain={'x': [0.7, 1], 'y': [0.5, 1]}
            ))

            # Add a line chart for trends over time
            trend_data = df.groupby('date')['amount'].sum().reset_index()
            
            fig.add_trace(go.Scatter(
                x=trend_data['date'],
                y=trend_data['amount'],
                mode='lines',
                name='Transaction Trends'
            ))

            # Update layout and save dashboard as HTML
            fig.update_layout(
                title="Finance Dashboard",
                template="plotly_dark",
                height=600,
                width=1000
            )
            
            fig.write_html("finance_dashboard.html")
            
            print("Dashboard saved as finance_dashboard.html")

        except Exception as e:
            print(f"Error generating dashboard: {str(e)}")

# Main script execution
if __name__ == "__main__":
    table_name = "finance_transactions"
    
    # Step 1: Connect to the database
    engine = FinanceDashboard.connect_to_database()
    
    if engine is not None:
        # Step 2: Load data from the table
        data = FinanceDashboard.load_data(engine, table_name)
        
        if data is not None:
            # Step 3: Generate charts
            FinanceDashboard.generate_charts(data)

            # Step 4: Generate interactive dashboard
            FinanceDashboard.generate_dashboard(data)
