import pandas as pd  # type: ignore
from sqlalchemy import create_engine, text  # type: ignore
import time
from sqlalchemy.exc import OperationalError  # type: ignore
import datetime
import sys
import os

# Get the parent directory and append it to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import MongoDBSingleton

def handle_memoryview(data_frame):
    '''
    Replaces any memoryview objects in a DataFrame with byte representations.
    
    Iterates through each column in the DataFrame, checking for any memoryview objects.
    If a memoryview object is found, it is converted to bytes using `.tobytes()`.
    
    Parameters:
        data_frame (pd.DataFrame): The DataFrame potentially containing memoryview objects.

    Returns:
        pd.DataFrame: The modified DataFrame with memoryview objects replaced by bytes.
    '''
    for column in data_frame.columns:
        # Replace memoryview objects
        data_frame[column] = data_frame[column].apply(
            lambda x: x.tobytes() if isinstance(x, memoryview) else x
        )
    return data_frame

def handle_nat_values(data_frame):
    '''
    Replaces NaT (Not-a-Time) values in datetime columns with a default date.
    
    Identifies all columns with datetime data types and replaces any NaT values
    with a pre-defined standard date. This ensures consistency in date values.
    
    Parameters:
        data_frame (pd.DataFrame): The DataFrame with potential NaT values in datetime columns.

    Returns:
        pd.DataFrame: The DataFrame with NaT values replaced by the standard date.
    '''
    standard_date = pd.Timestamp('2005-05-24 00:00:00')
    for column in data_frame.select_dtypes(include=['datetime']):
        # Replace all NaT values in datetime columns with the default value
        data_frame[column] = data_frame[column].fillna(standard_date)
    return data_frame

def convert_dates(data_frame):
    '''
    Converts date objects in columns of type 'object' to datetime with minimum time.
    
    Checks for date objects in columns of type 'object' and converts them to datetime
    with a time of 00:00:00. This standardizes date formats for consistency.
    
    Parameters:
        data_frame (pd.DataFrame): The DataFrame potentially containing date objects.

    Returns:
        pd.DataFrame: The DataFrame with date objects converted to datetime.
    '''
    for column in data_frame.select_dtypes(include=['object']):
        data_frame[column] = data_frame[column].apply(lambda x: datetime.datetime.combine(x, datetime.datetime.min.time()) if isinstance(x, datetime.date) else x)
    return data_frame

def creatSQLconnection():
    '''
    Establishes a connection to a PostgreSQL database, retrying on failure.
    
    Uses a while loop to continuously attempt to connect to the PostgreSQL database
    using SQLAlchemy. If the connection fails, it waits 5 seconds and retries.
    
    Returns:
        sqlalchemy.engine.base.Engine: An SQLAlchemy engine connected to PostgreSQL.
    '''
    while True:
        try:
            # Create the SQLAlchemy engine for PostgreSQL
            engine = create_engine('postgresql://postgres:1234@postgresdb:5432/dvdrental')
            # Test the connection
            with engine.connect() as connection:
                print("Connected to PostgreSQL")
            break  # Connection successful, exit loop
        except OperationalError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            print("Waiting for PostgreSQL to start...")
            time.sleep(5)  # Retry after 5 seconds
    return engine

def retrieve_tables(engine):
    '''
    Retrieves a list of all tables in the PostgreSQL public schema.
    
    Connects to the database using the provided engine and executes an SQL query to
    fetch all table names from the 'public' schema. Outputs debug information to aid
    in tracing issues if any arise.
    
    Parameters:
        engine (sqlalchemy.engine.base.Engine): The SQLAlchemy engine for database connection.

    Returns:
        list: A list of tuples, where each tuple contains a table name.
    '''
    with engine.connect() as connection:
        result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"))  # text() to execute SQL
        tables = result.fetchall()  # Retrieve list of table names

        # Output debug information
        print("Executed query: SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
        print(f"Raw tables result: {tables}")  # Added to check the return of fetchall()

        # Print number of tables found
        print(f"Found {len(tables)} tables.")
    
    return tables

def migrate_tables(tables):
    '''
    Migrates each table from PostgreSQL to MongoDB.
    
    Iterates through the list of tables and, for each table:
    - Retrieves all data from the PostgreSQL table.
    - Processes data to handle memoryview objects, NaT values, and date conversions.
    - Inserts data into a MongoDB collection with the same name as the table.
    
    Parameters:
        tables (list): A list of tuples, each containing a table name from PostgreSQL.
    '''
    for table in tables:
        table_name = table[0]  # Table name
        print(f"Migrating table: {table_name}")

        # Retrieve data from the PostgreSQL table
        sql_query = text(f"SELECT * FROM {table_name};")  # text() to execute SQL
        data_frame = pd.read_sql(sql_query, engine)
        data_frame = handle_memoryview(data_frame)  # Convert memoryview objects
        data_frame = handle_nat_values(data_frame)  # Overwrite NaT values with the specified date
        data_frame = convert_dates(data_frame=data_frame)

        if not data_frame.empty:  # Ensure the table is not empty
            # Insert data into MongoDB collection (collection = table name)
            mongo_collection = mongo_db[table_name]  # MongoDB collection has the same name as the table
            mongo_collection.insert_many(data_frame.to_dict('records'))
            print(f"Inserted {len(data_frame)} records into MongoDB collection: {table_name} with command: mongo_db[{table_name}].insert_many(data_frame.to_dict('records'))")
        else:
            print(f"No data found in table: {table_name}")

# Create SQLAlchemy connection
engine = creatSQLconnection()

# Retrieve a list of all public tables in PostgreSQL
tables = retrieve_tables(engine=engine)

# Connect to MongoDB using the singleton instance
mongo_db = MongoDBSingleton.get_instance()

# Inform about the beginning of the migration process
print("-" * 10 + " MIGRATE " + "-" * 10)
print()

# Migrate the tables from PostgreSQL to MongoDB
migrate_tables(tables=tables)