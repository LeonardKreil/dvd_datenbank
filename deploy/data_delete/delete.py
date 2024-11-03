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
    Deletes all films with a duration of less than 60 minutes and their associated rentals,
    while printing the counts of films and rentals before and after the deletion.
    """
    # Count and print the number of films and rentals before deletion
    print("Before deletion:")
    before_counts = count_films_and_rentals()
    print(f"Total Films: {before_counts[0]}")
    print(f"Total Rentals: {before_counts[1]}")

    # Find films to delete (duration < 60 minutes)
    short_films = mongo_db.film.find({"length": {"$lt": 60}})

    # Collect the film IDs to delete
    film_ids_to_delete = [film["film_id"] for film in short_films]

    if film_ids_to_delete:
        # Delete the associated rentals
        mongo_db.rental.delete_many({"film_id": {"$in": film_ids_to_delete}})
        
        # Delete the short films
        mongo_db.film.delete_many({"film_id": {"$in": film_ids_to_delete}})
    
        print(f"Deleted {len(film_ids_to_delete)} short films and their associated rentals.")
    else:
        print("No short films to delete.")

    # Count and print the number of films and rentals after deletion
    print("After deletion:")
    after_counts = count_films_and_rentals()
    print(f"Total Films: {after_counts[0]}")
    print(f"Total Rentals: {after_counts[1]}")

# Establish a connection to the NoSQL database
mongo_db = MongoDBSingleton.get_instance()

print('hello from delete')
# Execute the deletion function
delete_short_films_and_rentals()
