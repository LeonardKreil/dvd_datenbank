import pandas as pd  # type: ignore
from sqlalchemy import create_engine, text # type: ignore
import time
from sqlalchemy.exc import OperationalError  # type: ignore
import datetime
import sys
import os

# Get the parent directory and append it to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import MongoDBSingleton

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

# SQLAlchemy-Verbindung erstellen
while True:
    try:
        # Erstelle die SQLAlchemy-Engine für PostgreSQL
        engine = create_engine('postgresql://postgres:1234@postgresdb:5432/dvdrental')
        # Teste die Verbindung
        with engine.connect() as connection:
            print("Connected to PostgreSQL")
        break  # Verbindung erfolgreich, Schleife verlassen
    except OperationalError as e:
        print(f"Error connecting to PostgreSQL: {e}")
        print("Waiting for PostgreSQL to start...")
        time.sleep(5)  # Erneuter Versuch nach 5 Sekunden

# MongoDB-Verbindung
mongo_db = MongoDBSingleton.get_instance()

# Tabellenliste aus PostgreSQL abrufen
with engine.connect() as connection:
    result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"))  # text() um SQL auszuführen
    tables = result.fetchall()  # Liste der Tabellennamen abrufen

    # Debug-Informationen ausgeben
    print("Executed query: SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    print(f"Raw tables result: {tables}")  # Fügt dies hinzu, um die Rückgabe von fetchall() zu überprüfen

    # Alle Tabellennamen printen
    print(f"Found {len(tables)} tables.")
    # for table in tables:
    #    print(table[0])  # table ist ein Tupel, daher verwenden wir table[0] für den Tabellennamen


# Tabellen migrieren
for table in tables:
    table_name = table[0]  # Tabellenname
    print(f"Migrating table: {table_name}")

    # Daten aus der PostgreSQL-Tabelle abrufen
    sql_query = text(f"SELECT * FROM {table_name};")  # text() um SQL auszuführen
    data_frame = pd.read_sql(sql_query, engine)
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
# pg_conn.close()