import pandas as pd # type: ignore
import psycopg2 # type: ignore
import time
from psycopg2 import OperationalError # type: ignore
from pymongo import MongoClient # type: ignore
import datetime

def handle_memoryview(data_frame):
    for column in data_frame.columns:
        # Ersetzen von memoryview-Objekten
        data_frame[column] = data_frame[column].apply(
            lambda x: x.tobytes() if isinstance(x, memoryview) else x
        )
    return data_frame

def handle_nat_values(data_frame):
    # Festgelegter Standardwert für NaT
    standard_date = pd.Timestamp('2005-05-24 00:00:00')
    for column in data_frame.select_dtypes(include=['datetime']):
        # Ersetzt alle NaT-Werte in datetime-Spalten durch den Standardwert
        data_frame[column] = data_frame[column].fillna(standard_date)
    return data_frame

def convert_dates(data_frame):
    for column in data_frame.select_dtypes(include=['object']):
        data_frame[column] = data_frame[column].apply(lambda x: datetime.datetime.combine(x, datetime.datetime.min.time()) if isinstance(x, datetime.date) else x)
    return data_frame

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
    except OperationalError as e:
        print(f"Error connecting to PostgreSQL: {e}")
        print("Waiting for PostgreSQL to start...")
        time.sleep(5)  # Retry after 5 seconds

# MongoDB-Verbindung
mongo_client = MongoClient('mongodb://mongodb:27017/')
mongo_db = mongo_client['dvdrental']  # MongoDB-Datenbank 'dvdrental'

# Tabellenliste aus PostgreSQL abrufen
pg_cursor = pg_conn.cursor()
try:
    pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = pg_cursor.fetchall()  # Liste der Tabellennamen abrufen

    # Debug-Informationen ausgeben
    print("Executed query: SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    print(f"Raw tables result: {tables}")  # Fügt dies hinzu, um die Rückgabe von fetchall() zu überprüfen

    # Alle Tabellennamen printen
    print(f"Found {len(tables)} tables.")
    for table in tables:
        print(table[0])  # table ist ein Tupel, daher verwenden wir table[0] für den Tabellennamen
except Exception as e:
    print(f"Error fetching tables: {e}")

# Tabellen migrieren
for table in tables:
    table_name = table[0]  # Tabellenname
    print(f"Migrating table: {table_name}")

    # Daten aus der PostgreSQL-Tabelle abrufen
    sql_query = f"SELECT * FROM {table_name};"
    data_frame = pd.read_sql(sql_query, pg_conn)
    data_frame = handle_memoryview(data_frame)  # memoryview-Objekte konvertieren
    data_frame = handle_nat_values(data_frame)  # NaT-Werte mit festgelegtem Datum überschreiben
    data_frame = convert_dates(data_frame=data_frame)

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