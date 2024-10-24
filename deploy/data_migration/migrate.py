import pandas as pd
import psycopg2
import time
from psycopg2 import OperationalError
from pymongo import MongoClient


# PostgreSQL-Verbindung
while True:
    try:
        pg_conn = psycopg2.connect(
            host="postgresdb",
            database="dvdrental",
            user="postgres",
            password="1234",
            port="5432"
        )
        print("Connected to PostgreSQL")
        break  # Connection successful, break out of the loop
    except OperationalError:
        print("Waiting for PostgreSQL to start...")
        time.sleep(5)  # Retry after 5 seconds

# MongoDB-Verbindung
mongo_client = MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['dvdrental']  # MongoDB-Datenbank 'dvdrental'

# Tabellenliste aus PostgreSQL abrufen
pg_cursor = pg_conn.cursor()
pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
tables = pg_cursor.fetchall()  # Liste der Tabellennamen abrufen

# Tabellen migrieren
for table in tables:
    table_name = table[0]  # Tabellenname
    print(f"Migrating table: {table_name}")

    # Daten aus der PostgreSQL-Tabelle abrufen
    sql_query = f"SELECT * FROM {table_name};"
    data_frame = pd.read_sql(sql_query, pg_conn)

    if not data_frame.empty:  # Sicherstellen, dass die Tabelle nicht leer ist
        # Daten in MongoDB-Collection einfügen (Collection = Tabellenname)
        mongo_collection = mongo_db[table_name]  # MongoDB-Collection hat denselben Namen wie die Tabelle
        mongo_collection.insert_many(data_frame.to_dict('records'))
        print(f"Inserted {len(data_frame)} records into MongoDB collection: {table_name}")
    else:
        print(f"No data found in table: {table_name}")

# Verbindungen schließen
pg_conn.close()
mongo_client.close()
