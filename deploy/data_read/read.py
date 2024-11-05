from pymongo import MongoClient # type: ignore
from prettytable import PrettyTable # type: ignore
import sys
import os

# Get the parent directory and append it to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import MongoDBSingleton

# a. Total number of available films
def total_films():
    """
    Calculates the total number of available films.
    
    Returns:
        int: Total number of films.
    """

    count = mongo_db.film.count_documents({})

    # Output
    print("A: ")
    command = "mongo_db.film.count_documents({})"
    print(f"Command: {command}")
    print(f"Total number of available films: {count}")
    print()
    return count

# b. Number of unique films per location
def films_per_location():
    """
    Calculates the number of unique film titles per location in MongoDB.
    
    Returns:
        list: A list of documents with store IDs and the count of unique film titles per store.
    """
    pipeline = [
        # Step 1: Join the `inventory` collection with `film` to get the title for each `film_id`
        {"$lookup": {
            "from": "film",
            "localField": "film_id",
            "foreignField": "film_id",
            "as": "film_details"
        }},
        {"$unwind": "$film_details"},  # Unwind `film_details` to access the title directly
        
        # Step 2: Group by store and title so each title is counted only once per store
        {"$group": {"_id": {"store_id": "$store_id", "title": "$film_details.title"}}},
        
        # Step 3: Group by store and count unique titles per store
        {"$group": {"_id": "$_id.store_id", "films_per_store": {"$sum": 1}}}
    ]

    result = list(mongo_db.inventory.aggregate(pipeline))

    # Output
    print("B: ")
    command = f"mongo_db.inventory.aggregate({pipeline})"
    print(f"Command: {command}")
    print("Number of unique films per location:")
    for entry in result:
        print(f"Store {entry['_id']}: {entry['films_per_store']} films")
    print()
    
    return result

# c. The top 10 actors with the most films, sorted in descending order
def top_actors():
    """
    Finds the top 10 actors with the most films and returns their names and film counts,
    sorted in descending order.
    
    Returns:
        list: A list of documents with actor IDs and film counts.
    """
    pipeline = [
        {"$group": {"_id": "$actor_id", "film_count": {"$sum": 1}}},  # Groups by actor ID and counts the films
        {"$sort": {"film_count": -1}},                                # Sorts in descending order by film count
        {"$limit": 10}                                                # Limits to the top 10 actors
    ]
    result = list(mongo_db.film_actor.aggregate(pipeline))
    
    # Output
    print("C: Top 10 actors by number of films:")
    command = f"mongo_db.film_actor.aggregate({pipeline})"
    print(f"Command: {command}")
    for actor in result:
        actor_details = mongo_db.actor.find_one({"actor_id": actor["_id"]})  # Finds the actor's name
        print(f"{actor_details['first_name'].ljust(10)} {actor_details['last_name'].ljust(10)}: {actor['film_count']} films")

    print()
    return result

# d. Revenue per staff member
def revenue_per_staff():
    """
    Calculates revenue (total amount) per staff member and returns it in descending order.
    
    Returns:
        list: A list of documents with staff IDs and corresponding revenues.
    """
    pipeline = [
        {"$group": {"_id": "$staff_id", "total_revenue": {"$sum": "$amount"}}},  # Groups by staff ID and sums the amounts
        {"$sort": {"total_revenue": -1}}                                         # Sorts in descending order by revenue
    ]
    result = list(mongo_db.payment.aggregate(pipeline))
    
    # Output
    print("D: Revenue per staff member:")
    command = f"mongo_db.payment.aggregate({pipeline})"
    print(f"Command: {command}")
    for staff in result:
        staff_details = mongo_db.staff.find_one({"staff_id": staff["_id"]})
        print(f"Staff {staff_details['first_name'].ljust(10)} {staff_details['last_name'].ljust(10)}: ${staff['total_revenue']:.2f}")
    
    print()
    return result

# e. The IDs of the top 10 customers with the most rentals
def top_customers_by_rentals():
    """
    Finds the top 10 customers with the most rentals and returns their IDs along with the number of rentals.
    
    Returns:
        list: A list of documents with customer IDs and rental counts.
    """
    pipeline = [
        {"$group": {"_id": "$customer_id", "rental_count": {"$sum": 1}}},  # Groups by customer ID and counts the rentals
        {"$sort": {"rental_count": -1}},                                   # Sorts in descending order by rental count
        {"$limit": 10}                                                     # Limits to the top 10 customers
    ]
    result = list(mongo_db.rental.aggregate(pipeline))
    
    # Output
    print("E: Top 10 customers by number of rentals:")
    command = f"mongo_db.rental.aggregate({pipeline})"
    print(f"Command: {command}")
    for customer in result:
        customer_details = mongo_db.customer.find_one({"customer_id": customer["_id"]})
        print(f"Customer {customer['_id']} {customer_details['first_name'].ljust(10)} {customer_details['last_name'].ljust(10)}: {customer['rental_count']} rentals")
    
    print()
    return result

# f. The top 10 customers who spent the most money
def top_customers_by_spending():
    """
    Finds the top 10 customers who spent the most money and returns their names, total spending, and store ID.
    
    Returns:
        list: A list of documents with customer names, total spending, and store IDs.
    """
    pipeline = [
        {"$group": {"_id": "$customer_id", "total_spent": {"$sum": "$amount"}}},  # Groups by customer ID and sums the amounts
        {"$sort": {"total_spent": -1}},                                           # Sorts in descending order by total spending
        {"$limit": 10},                                                           # Limits to the top 10 customers
        {
            "$lookup": {                                                          # Joins with the customer collection
                "from": "customer",
                "localField": "_id",
                "foreignField": "customer_id",
                "as": "customer_info"
            }
        },
        {"$unwind": "$customer_info"},                                            # Unwinds the joined customer information
        {
            "$project": {                                                         # Projects relevant fields
                "customer_id": "$_id",
                "total_spent": 1,
                "first_name": "$customer_info.first_name",
                "last_name": "$customer_info.last_name",
                "store_id": "$customer_info.store_id"                             # Includes store ID
            }
        }
    ]
    
    result = list(mongo_db.payment.aggregate(pipeline))
    
    # Output
    print("Top 10 customers by total spending:")
    command = f"mongo_db.payment.aggregate({pipeline})"
    print(f"Command: {command}")
    for customer in result:
        print(f"{customer['first_name'].ljust(10)} {customer['last_name'].ljust(10)} | Store ID: {customer['store_id']} | Total Spent: ${customer['total_spent']:.2f}")

    print()
    return result

# g. The top 10 most-watched movies by title, sorted in descending order
def most_watched_movies():
    """
    Finds the top 10 most-watched movies and returns their titles and number of rentals.
    
    Returns:
        list: A list of documents with movie IDs and rental counts.
    """
    pipeline = [
        # Step 1: Group by inventory ID and count the rentals
        {"$group": {"_id": "$inventory_id", "rental_count": {"$sum": 1}}},

        # Step 2: Lookup the `inventory` collection to get `film_id` for each `inventory_id`
        {"$lookup": {
            "from": "inventory",
            "localField": "_id",
            "foreignField": "inventory_id",
            "as": "inventory_details"
        }},
        {"$unwind": "$inventory_details"},  # Unwind the joined inventory data

        # Step 3: Lookup the `film` collection to get the film title based on `film_id`
        {"$lookup": {
            "from": "film",
            "localField": "inventory_details.film_id",
            "foreignField": "film_id",
            "as": "film_details"
        }},
        {"$unwind": "$film_details"},  # Unwind the film details to access the title
        
        # Step 4: Group by `film_id` and `title` and sum the rentals for the same film
        {"$group": {"_id": {"film_id": "$inventory_details.film_id", "title": "$film_details.title"}, "total_rentals": {"$sum": "$rental_count"}}},

        # Step 5: Sort by rental count and limit to the top 10 films
        {"$sort": {"total_rentals": -1}},
        {"$limit": 10}
    ]
    result = list(mongo_db.rental.aggregate(pipeline))
    
    # Output
    print("G: Top 10 most-watched movies:")
    command = f"mongo_db.rental.aggregate({pipeline})"
    print(f"Command: {command}")
    for film in result:
        title = film["_id"]["title"]
        rentals = film["total_rentals"]
        print(f"{title.ljust(25)}: {rentals} rentals")

    print()
    return result

# h. The top 3 most-watched movie categories
def top_categories():
    """
    Finds the top 3 most-watched movie categories and returns their names and rental counts.
    
    Returns:
        list: A list of documents with category names and their rental counts.
    """
    pipeline = [
        # Step 1: Join the `rental` collection with `inventory` to get `film_id` for each rental
        {"$lookup": {
            "from": "inventory",
            "localField": "inventory_id",
            "foreignField": "inventory_id",
            "as": "inventory_details"
        }},
        {"$unwind": "$inventory_details"},  # Unwind the inventory details

        # Step 2: Join with `film` to find the film categories for each rental
        {"$lookup": {
            "from": "film",
            "localField": "inventory_details.film_id",
            "foreignField": "film_id",
            "as": "film_details"
        }},
        {"$unwind": "$film_details"},  # Unwind the film details

        # Step 3: Join with `film_category` to find the category for each film
        {"$lookup": {
            "from": "film_category",
            "localField": "film_details.film_id",
            "foreignField": "film_id",
            "as": "film_category_details"
        }},
        {"$unwind": "$film_category_details"},  # Unwind the film category details
        
        {"$lookup": {
            "from": "category",
            "localField": "film_category_details.category_id",
            "foreignField": "category_id",
            "as": "category_details"
        }},
        {"$unwind": "$category_details"},  # Unwind the category names

        # Step 4: Group by category and sum the rentals per category
        {"$group": {"_id": "$category_details.name", "total_rentals": {"$sum": 1}}},

        # Step 5: Sort by rental count and limit to the top 3 categories
        {"$sort": {"total_rentals": -1}},
        {"$limit": 3}
    ]

    result = list(mongo_db.rental.aggregate(pipeline))

    # Output
    print("H: Top 3 most-watched movie categories:")
    command = f"mongo_db.rental.aggregate({pipeline})"
    print(f"Command: {command}")

    for category in result:
        print(f"Category {category['_id'].ljust(10)}: {category['total_rentals']} rentals")
    
    print()
    return result

# i. A view of customers with all relevant information as in the customer_list view
def customer_view():
    """
    Creates a view of customers with all relevant information, similar to the view `customer_list`,
    and outputs the data in tabular form with a header row.
    
    Returns:
        list: A list of documents with complete customer information.
    """
    # This pipeline is not used because the direct  
    pipeline = [
        # Step 1: Join with the "address" collection to add address details
        {"$lookup": {
            "from": "address",
            "localField": "address_id",
            "foreignField": "address_id",
            "as": "address_details"
        }},
        {"$unwind": "$address_details"},  # Unwind the address details into individual documents
        
        # Step 2: Join with the "city" collection to add city details
        {"$lookup": {
            "from": "city",
            "localField": "address_details.city_id",
            "foreignField": "city_id",
            "as": "city_details"
        }},
        {"$unwind": "$city_details"},  # Unwind the city details
        
        # Step 3: Join with the "country" collection to add country details
        {"$lookup": {
            "from": "country",
            "localField": "city_details.country_id",
            "foreignField": "country_id",
            "as": "country_details"
        }},
        {"$unwind": "$country_details"},  # Unwind the country details
        
        # Step 4: Select the relevant fields to shape the final result
        {"$project": {
            "customer_id": "$customer_id",  # Customer ID from the current collection
            "name": {"$concat": ["$first_name", " ", "$last_name"]},  # Combined first and last name
            "address": "$address_details.address",  # Address
            "postal_code": "$address_details.postal_code",  # Postal code
            "phone": "$address_details.phone",               # Phone number
            "city": "$city_details.city",    # City
            "country": "$country_details.country",  # Country
            "active": "$active",             # Customer status
        }}
    ]

    # result = list(mongo_db.customer.aggregate(pipeline))

    # Access the MongoDB view "customer_list"
    customer_view = mongo_db.customer_list  # Replace 'db' with the actual MongoDB database name

    # Execute query, optionally with a limit
    result = customer_view.find().limit(15)  # Limit to 15 documents for a preview

    print("I: A view of customer list:")
    # Create a table and define header row
    table = PrettyTable()
    table.field_names = ["Customer ID", "Name", "Address", "Postal Code", "Phone", "City", "Country", "Active", "SID"]

    # Insert data into the table
    for customer in result[1:11]:
        table.add_row([
            customer["id"],
            customer["name"],
            customer["address"],
            customer["zip code"],
            customer["phone"],
            customer["city"],
            customer["country"],
            customer["notes"],
            customer["sid"]
        ])
    
    # Output the table
    print(table)
    return result

# Establish a connection to the NoSQL database
mongo_db = MongoDBSingleton.get_instance()

print()
print("-" * 10 + " READ " + "-" * 10)
print()

# A:
number_of_films = total_films()
# B:
number_of_films_per_location = films_per_location()
# C:
top_ten_actors = top_actors()
# D:
revenue_of_stuff = revenue_per_staff()
# E: 
top_customer_rentals = top_customers_by_rentals()
# F:
top_ten_customer_spending = top_customers_by_spending()
# G:
top_ten_movies = most_watched_movies()
# H:
top_three_categories = top_categories()
# I:
top_ten_cusomer_view = customer_view()

