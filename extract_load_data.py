# This script extracts and loads data from multiple databases into a centralized warehouse. 
# It supports automation and scalability for enterprise-level workflows
import mysql.connector
import pyodbc
import fdb

def etl(query, source_cnx, target_cnx):
    # Extract data from source database
    source_cursor = source_cnx.cursor()
    source_cursor.execute(query.extract_query)
    data = source_cursor.fetchall()
    source_cursor.close()
    
    # Load data into target database
    if data:
        target_cursor = target_cnx.cursor()
        target_cursor.executemany(query.load_query, data)
        print('Data loaded to warehouse')
        target_cursor.close()
    else:
        print('No data found')

def etl_process(queries, target_cnx, source_db_config, db_platform):
    # Connect to source database
    if db_platform == 'mysql':
        source_cnx = mysql.connector.connect(**source_db_config)
    elif db_platform == 'sqlserver':
        source_cnx = pyodbc.connect(**source_db_config)
    elif db_platform == 'firebird':
        source_cnx = fdb.connect(**source_db_config)
    else:
        raise ValueError('Unsupported database platform')
    
    # Loop through queries and perform ETL
    for query in queries:
        etl(query, source_cnx, target_cnx)
    
    # Close connection
    source_cnx.close()
