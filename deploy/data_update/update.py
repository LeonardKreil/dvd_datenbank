import random
import string
import hashlib
import sys
import os

# Get the parent directory and append it to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import MongoDBSingleton  

def generate_secure_password(length=12):
    """
    Generiert ein zufälliges, sicheres Passwort.
    """
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

def update_staff_passwords():
    """
    Vergibt allen Mitarbeitern ein neues, sicheres Passwort und aktualisiert die `staff`-Sammlung.
    """
    staff_members = mongo_db.staff.find()
    
    for staff in staff_members:
        new_password = generate_secure_password()
        
        # Optional: Passwort verschlüsseln, z. B. mit SHA-256
        hashed_password = hashlib.sha256(new_password.encode()).hexdigest()
        
        # Passwort in der Datenbank aktualisieren
        mongo_db.staff.update_one(
            {"staff_id": staff["staff_id"]},
            {"$set": {"password": hashed_password}}
        )
        print(f"Passwort für Mitarbeiter {staff['first_name']} {staff['last_name']} wurde auf {hashed_password} aktualisiert.")

def add_new_store_with_inventory_transfer():
    """
    Fügt einen neuen Standort hinzu und verlegt das Inventar der bisherigen Standorte dorthin.
    """
    # 1. Erstellen des neuen Standorts
    new_store = {
        "store_id": mongo_db.store.count_documents({}) + 1,  # Neuer store_id basierend auf aktueller Anzahl
        "address": "123 New Location Street",
        "city": "Fictional City",
        "country": "Fictionland",
        "phone": "123-456-7890"
    }
    mongo_db.store.insert_one(new_store)
    new_store_id = new_store["store_id"]

    # 2. Aktualisieren des Inventars, um auf den neuen Standort zu verweisen
    mongo_db.inventory.update_many(
        {},  # Aktualisiert alle Dokumente
        {"$set": {"store_id": new_store_id}}
    )
    print(f"Neuer Standort {new_store_id} erstellt und Inventar verlegt.")

def verify_staff_passwords():
    """
    Überprüft die aktualisierten Passwörter der Mitarbeiter.
    """
    staff_members = mongo_db.staff.find()
    for staff in staff_members:
        if 'password' in staff:
            # Optional: Um zu überprüfen, dass die Passwortaktualisierung erfolgreich war,
            # könnte man hier auch versuchen, das Passwort zu entschlüsseln.
            print(f"Überprüfen des Passworts für Mitarbeiter {staff['first_name']} {staff['last_name']}: {staff['password']}")
        else:
            print(f"Keine Passwortdaten für Mitarbeiter {staff['first_name']} {staff['last_name']} gefunden.")

def verify_new_store_and_inventory():
    """
    Überprüft, ob der neue Standort erstellt wurde und das Inventar korrekt aktualisiert wurde.
    """
    new_store = mongo_db.store.find_one({"address": "123 New Location Street"})
    if new_store:
        print(f"Neuer Standort erfolgreich erstellt: {new_store['store_id']} - {new_store['address']}")
        
        # Überprüfen, ob das Inventar auf den neuen Standort verweist
        inventory_items = mongo_db.inventory.find({"store_id": new_store['store_id']})
        item_count = mongo_db.inventory.count_documents({"store_id": new_store['store_id']})
        
        if item_count > 0:
            print(f"Inventar erfolgreich auf den neuen Standort {new_store['store_id']} verlegt. Anzahl der Artikel: {item_count}")
        else:
            print(f"Keine Artikel im Inventar für den neuen Standort {new_store['store_id']} gefunden.")
    else:
        print("Neuer Standort nicht gefunden.")

# Establish a connection to the NoSQL database
mongo_db = MongoDBSingleton.get_instance()

print()
print("-" * 10 + " UPDATE " + "-" * 10)
print()

# Funktionen aufrufen
verify_staff_passwords()
update_staff_passwords()
verify_staff_passwords()

add_new_store_with_inventory_transfer()
verify_new_store_and_inventory()

