import sys
import os

# Get the parent directory and append it to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import MongoDBSingleton

def count_films_and_rentals():
    """
    Count the total number of films and rentals in the database.

    Returns:
        tuple: (total_films, total_rentals)
    """
    total_films = mongo_db.film.count_documents({})
    total_rentals = mongo_db.rental.count_documents({})
    return total_films, total_rentals

def delete_short_films_and_rentals():
    """
    Deletes all films with a duration of less than 60 minutes, their associated rentals,
    inventory items, and related entries in multiple collections (e.g., film_actor, film_category).
    """
    # Count and print the number of films and rentals before deletion
    print("Before deletion:")
    before_counts = count_films_and_rentals()
    print(f"Total Films: {before_counts[0]}")
    print(f"Total Rentals: {before_counts[1]}")

    # Find short films (duration < 60 minutes)
    short_films = list(mongo_db.film.find({"length": {"$lt": 60}}))
    print('Command: list(mongo_db.film.find({"length": {"$lt": 60}})) #Find short films (duration < 60 minutes)')
    
    if not short_films:
        print("No short films to delete.")
        return

    # Collect film IDs to find related inventory IDs
    film_ids_to_delete = [film["film_id"] for film in short_films if "film_id" in film]

    # Fetch inventory IDs related to the films
    inventory_docs = mongo_db.inventory.find({"film_id": {"$in": film_ids_to_delete}})
    inventory_ids_to_delete = [doc["inventory_id"] for doc in inventory_docs if "inventory_id" in doc]

    # If there are inventory IDs, delete associated rentals
    if inventory_ids_to_delete:
        try:
            mongo_db.rental.delete_many({"inventory_id": {"$in": inventory_ids_to_delete}})
            print('Command: inventory_docs = mongo_db.inventory.find({"film_id": {"$in": film_ids_to_delete}})')
            print('Command: inventory_ids_to_delete = [doc["inventory_id"] for doc in inventory_docs if "inventory_id" in doc]')
            print('Command: mongo_db.rental.delete_many({"inventory_id": {"$in": inventory_ids_to_delete}})')
            print(f"Deleted {len(inventory_ids_to_delete)} inventory_ids in rentals associated with short films.")
        except Exception as e:
            print(f"An error occurred during deletion of rentals: {e}")

    # If there are film IDs, delete the short films and their related data
    if film_ids_to_delete:
        try:
            # Finally, delete the short films themselves
            mongo_db.film.delete_many({"film_id": {"$in": film_ids_to_delete}})
            print('Command: film_ids_to_delete = [film["film_id"] for film in short_films if "film_id" in film]')
            print('Command: mongo_db.film.delete_many({"film_id": {"$in": film_ids_to_delete}})')
            print(f"Deleted {len(film_ids_to_delete)} short films.")
        except Exception as e:
            print(f"An error occurred during deletion of films: {e}")
    else:
        print("No short films to delete.")

    # Count and print the number of films and rentals after deletion
    print("After deletion:")
    after_counts = count_films_and_rentals()
    print(f"Total Films: {after_counts[0]}")
    print(f"Total Rentals: {after_counts[1]}")

print()
print("-" * 10 + " DELETE " + "-" * 10)
print()
# Establish a connection to the NoSQL database
mongo_db = MongoDBSingleton.get_instance()

# Execute the deletion function
delete_short_films_and_rentals()
