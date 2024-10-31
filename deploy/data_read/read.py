from pymongo import MongoClient # type: ignore
from prettytable import PrettyTable # type: ignore

# a. Gesamtanzahl der verfügbaren Filme
def total_films():
    """
    Berechnet die Gesamtanzahl der verfügbaren Filme.
    
    Rückgabe:
        int: Gesamtanzahl der Filme
    """
    count = mongo_db.film.count_documents({})
    print(f"Gesamtanzahl der verfügbaren Filme: {count}")
    return count

# b. Anzahl der unterschiedlichen Filme je Standort
def films_per_location():
    """
    Berechnet die Anzahl der unterschiedlichen Filme pro Standort.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit Standort-IDs und jeweiliger Filmanzahl
    """
    pipeline = [
        {"$group": {"_id": "$store_id", "film_count": {"$sum": 1}}}  # Gruppiert nach Standort-ID und zählt die Filme
    ]
    result = list(mongo_db.inventory.aggregate(pipeline))
    
    print("Anzahl der unterschiedlichen Filme je Standort:")
    for entry in result:
        print(f"Standort {entry['_id']}: {entry['film_count']} Filme")
    return result

# c. Die 10 Schauspieler mit den meisten Filmen, absteigend sortiert
def top_actors():
    """
    Findet die 10 Schauspieler mit den meisten Filmen und gibt ihre Namen und die Filmanzahl zurück,
    absteigend sortiert.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit Schauspieler-IDs und Filmanzahl
    """
    pipeline = [
        {"$group": {"_id": "$actor_id", "film_count": {"$sum": 1}}},  # Gruppiert nach Schauspieler-ID und zählt die Filme
        {"$sort": {"film_count": -1}},                                # Sortiert absteigend nach Filmanzahl
        {"$limit": 10}                                                # Beschränkt auf die Top 10 Schauspieler
    ]
    result = list(mongo_db.film_actor.aggregate(pipeline))
    
    print("Top 10 Schauspieler nach Filmanzahl:")
    for actor in result:
        actor_details = mongo_db.actor.find_one({"actor_id": actor["_id"]})  # Findet den Namen des Schauspielers
        print(f"{actor_details['first_name'].ljust(10)} {actor_details['last_name'].ljust(10)}: {actor['film_count']} Filme")
    return result

# d. Die Erlöse je Mitarbeiter
def revenue_per_staff():
    """
    Berechnet die Erlöse (Umsatz) pro Mitarbeiter und gibt diese in absteigender Reihenfolge zurück.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit Mitarbeiter-IDs und jeweiligen Umsätzen
    """
    pipeline = [
        {"$group": {"_id": "$staff_id", "total_revenue": {"$sum": "$amount"}}},  # Gruppiert nach Mitarbeiter-ID und summiert die Beträge
        {"$sort": {"total_revenue": -1}}                                         # Sortiert absteigend nach Umsatz
    ]
    result = list(mongo_db.payment.aggregate(pipeline))
    
    print("Erlöse je Mitarbeiter:")
    for staff in result:
        staff_details = mongo_db.staff.find_one({"staff_id": staff["_id"]})
        print(f"Mitarbeiter {staff_details['first_name'].ljust(10)} {staff_details['last_name'].ljust(10)}: ${staff['total_revenue']:.2f}")
    return result

# e. Die IDs der 10 Kunden mit den meisten Entleihungen
def top_customers_by_rentals():
    """
    Findet die 10 Kunden mit den meisten Entleihungen und gibt ihre IDs sowie die Anzahl der Entleihungen zurück.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit Kunden-IDs und jeweiliger Entleihungsanzahl
    """
    pipeline = [
        {"$group": {"_id": "$customer_id", "rental_count": {"$sum": 1}}},  # Gruppiert nach Kunden-ID und zählt die Entleihungen
        {"$sort": {"rental_count": -1}},                                   # Sortiert absteigend nach Anzahl der Entleihungen
        {"$limit": 10}                                                     # Beschränkt auf die Top 10 Kunden
    ]
    result = list(mongo_db.rental.aggregate(pipeline))
    
    print("Top 10 Kunden nach Entleihungsanzahl:")
    for customer in result:
        customer_details = mongo_db.customer.find_one({"customer_id": customer["_id"]})
        print(f"Kunde {customer['_id']} {customer_details['first_name'].ljust(10)} {customer_details['last_name'].ljust(10)}: {customer['rental_count']} Entleihungen")
    return result

# f. Die 10 Kunden, die das meiste Geld ausgegeben haben
def top_customers_by_spending():
    """
    Findet die 10 Kunden, die am meisten ausgegeben haben und gibt ihre Namen und Ausgaben zurück.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit Kunden-IDs und deren Gesamtausgaben
    """
    pipeline = [
        {"$group": {"_id": "$customer_id", "total_spent": {"$sum": "$amount"}}},  # Gruppiert nach Kunden-ID und summiert die Beträge
        {"$sort": {"total_spent": -1}},                                           # Sortiert absteigend nach Ausgaben
        {"$limit": 10}                                                            # Beschränkt auf die Top 10 Kunden
    ]
    result = list(mongo_db.payment.aggregate(pipeline))
    
    print("Top 10 Kunden nach Ausgaben:")
    for customer in result:
        customer_details = mongo_db.customer.find_one({"customer_id": customer["_id"]})
        print(f"{customer_details['first_name'].ljust(10)} {customer_details['last_name'].ljust(10)}: ${customer['total_spent']:.2f}")
    return result

# g. Die 10 meistgesehenen Filme nach Titel, absteigend sortiert
def most_watched_movies():
    """
    Findet die 10 meistgesehenen Filme und gibt deren Titel sowie die Anzahl der Entleihungen zurück.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit Film-IDs und Anzahl der Entleihungen
    """
    pipeline = [
        # Schritt 1: Gruppiert nach Inventar-ID und zählt die Entleihungen
        {"$group": {"_id": "$inventory_id", "rental_count": {"$sum": 1}}},

        # Schritt 2: Verknüpft die `inventory`-Sammlung, um die `film_id` für jede `inventory_id` abzurufen
        {"$lookup": {
            "from": "inventory",
            "localField": "_id",
            "foreignField": "inventory_id",
            "as": "inventory_details"
        }},
        {"$unwind": "$inventory_details"},  # Entpackt die Daten der verknüpften Dokumente

        # Schritt 3: Verknüpft die `film`-Sammlung, um Filmtitel anhand der `film_id` abzurufen
        {"$lookup": {
            "from": "film",
            "localField": "inventory_details.film_id",
            "foreignField": "film_id",
            "as": "film_details"
        }},
        {"$unwind": "$film_details"},  # Entpackt die Daten der verknüpften Filmtitel
        
        # Schritt 4: Gruppiert nach `film_id` und `title` und summiert die Entleihungen für denselben Film
        {"$group": {"_id": {"film_id": "$inventory_details.film_id", "title": "$film_details.title"}, "total_rentals": {"$sum": "$rental_count"}}},

        # Schritt 5: Sortiert nach Entleihungsanzahl und beschränkt auf die Top 10 Filme
        {"$sort": {"total_rentals": -1}},
        {"$limit": 10}
    ]
    result = list(mongo_db.rental.aggregate(pipeline))
    
    print("Top 10 meistgesehenen Filme:")
    for film in result:
        title = film["_id"]["title"]
        rentals = film["total_rentals"]
        print(f"{title.ljust(25)}: {rentals} Entleihungen")
    return result

# h. Die 3 meistgesehenen Filmkategorien
def top_categories():
    """
    Findet die 3 meistgesehenen Filmkategorien und gibt deren Namen sowie die Anzahl der Entleihungen zurück.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit Kategorienamen und deren Entleihungsanzahl
    """
    pipeline = [
        # Schritt 1: Gruppieren der Entleihungen nach `inventory_id` und Zählen der Entleihungen
        {"$group": {"_id": "$inventory_id", "rental_count": {"$sum": 1}}},

        # Schritt 2: Verknüpfung mit `inventory`, um die `film_id` für jede `inventory_id` zu erhalten
        {"$lookup": {
            "from": "inventory",
            "localField": "_id",
            "foreignField": "inventory_id",
            "as": "inventory_details"
        }},
        {"$unwind": "$inventory_details"},  # Entpackt die Inventardetails

        # Schritt 4: Verknüpfung mit `category`, um den Kategorienamen anhand der `film_id` zu erhalten
        {"$lookup": {
            "from": "category",
            "localField": "inventory_details.film_id",
            "foreignField": "category_id",
            "as": "category_details"
        }},
        {"$unwind": "$category_details"},  # Entpackt die Kategorienamen

        # Schritt 5: Gruppieren nach Kategorie und Summieren der Entleihungen pro Kategorie
        {"$group": {"_id": "$category_details.name", "total_rentals": {"$sum": "$rental_count"}}},

        # Schritt 6: Sortieren nach Entleihungsanzahl und Begrenzung auf die Top 3 Kategorien
        {"$sort": {"total_rentals": -1}},
        {"$limit": 3}
    ]

    result = list(mongo_db.rental.aggregate(pipeline))

    print("Top 3 meistgesehenen Filmkategorien:")
    for category in result:
        print(f"Kategorie {category['_id'].ljust(10)}: {category['total_rentals']} Entleihungen")
    return result

#i. Eine Sicht auf die Kunden mit allen relevanten Informationen wie im View customer_l
def customer_view():
    """
    Erstellt eine Sicht der Kunden mit allen relevanten Informationen, ähnlich dem View „customer_list“,
    und gibt die Daten in tabellarischer Form mit einer Überschriftenzeile aus.
    
    Rückgabe:
        list: Eine Liste von Dokumenten mit vollständigen Kundeninformationen.
    """
    pipeline = [
        # Schritt 1: Verknüpfung mit der "address" Sammlung, um Adressdetails hinzuzufügen
        {"$lookup": {
            "from": "address",
            "localField": "address_id",
            "foreignField": "address_id",
            "as": "address_details"
        }},
        {"$unwind": "$address_details"},  # Entpackt die Adressdetails in einzelne Dokumente
        
        # Schritt 2: Verknüpfung mit der "city" Sammlung, um Stadtdetails hinzuzufügen
        {"$lookup": {
            "from": "city",
            "localField": "address_details.city_id",
            "foreignField": "city_id",
            "as": "city_details"
        }},
        {"$unwind": "$city_details"},  # Entpackt die Stadtdetails
        
        # Schritt 3: Verknüpfung mit der "country" Sammlung, um Länderdetails hinzuzufügen
        {"$lookup": {
            "from": "country",
            "localField": "city_details.country_id",
            "foreignField": "country_id",
            "as": "country_details"
        }},
        {"$unwind": "$country_details"},  # Entpackt die Länderdetails
        
        # Schritt 4: Auswahl der relevanten Felder, um das endgültige Ergebnis zu formen
        {"$project": {
            "customer_id": "$customer_id",  # Kunden-ID aus der aktuellen Sammlung
            "name": {"$concat": ["$first_name", " ", "$last_name"]},  # Vor- und Nachname kombiniert
            "address": "$address_details.address",  # Adresse
            "postal_code": "$address_details.postal_code",  # PLZ
            "phone": "$address_details.phone",               # Telefonnummer
            "city": "$city_details.city",    # Stadt
            "country": "$country_details.country",  # Land
            #"notes": "$activebool",               # Notizen
            "active": "$active",             # Status des Kunden
        }}
    ]

    result = list(mongo_db.customer.aggregate(pipeline))
    
    # Tabelle erstellen und Überschriftenzeile definieren
    table = PrettyTable()
    table.field_names = ["Customer ID", "Name", "Address", "Postal Code", "Phone", "City", "Country", "SID"] # Nodes fehlt
    
    # Daten in die Tabelle einfügen
    for customer in result[:10]:
        table.add_row([
            customer["customer_id"],
            customer["name"],
            customer["address"],
            customer["postal_code"],
            customer["phone"],
            customer["city"],
            customer["country"],
            # customer["nodes"],
            customer["active"]
        ])
    
    # Tabelle ausgeben
    print(table)
    return result



# Verbindung zur NoSQL-Datenbank herstellen
client = MongoClient("mongodb://mongodb:27017/")
mongo_db = client['dvdrental']

print("A: ")
number_of_films = total_films()
print("B: ")
number_of_films_per_location = films_per_location()
print("C: ")
top_ten_actors = top_actors()
print("D: ")
revenue_of_stuff = revenue_per_staff()
print("E: ")
top_customer_rentals = top_customers_by_rentals()
print("F: ")
top_ten_customer_spending = top_customers_by_spending()
print("G: ")
top_ten_movies = most_watched_movies()
print("H: ")
top_three_categories = top_categories()
print("I: ")
top_ten_cusomer_view = customer_view()

