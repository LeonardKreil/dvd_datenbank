import random
import string
import hashlib
import sys
import os

# Retrieve the parent directory and add it to sys.path to enable `db_connection` import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_connection import MongoDBSingleton  

def generate_secure_password(length=12):
    """
    Generates a random, secure password of the specified length.
    Uses uppercase, lowercase letters, digits, and special characters.
    """
    characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(characters) for _ in range(length))

def update_staff_passwords():
    """
    This function generates a new, secure password for each staff member
    and updates their entries in the `staff` database.
    """
    staff_members = mongo_db.staff.find()  # Retrieve all staff members from the DB

    print('Command: mongo_db.staff.update_one({"staff_id": staff["staff_id"]},{"$set": {"password": hashed_password}})')
    
    for staff in staff_members:
        # Generate a new secure password
        new_password = generate_secure_password()
        
        # Optional: Encrypt the password using SHA-256 for secure storage
        hashed_password = hashlib.sha256(new_password.encode()).hexdigest()
        
        # Update the password in the database
        mongo_db.staff.update_one(
            {"staff_id": staff["staff_id"]},
            {"$set": {"password": hashed_password}}
        )
        
        # Print confirmation that the password has been updated
        print(f"Password for staff member {staff['first_name']} {staff['last_name']} has been updated to {hashed_password}.")

def add_new_store_with_inventory_transfer():
    """
    Adds a new location to the database and updates all inventory entries 
    to reference this new location.
    """
    # 1. Create a new location with address and basic information
    new_store = {
        "store_id": mongo_db.store.count_documents({}) + 1,  # Create a new store_id based on current count
        "address": "123 New Location Street",
        "city": "Fictional City",
        "country": "Fictionland",
        "phone": "123-456-7890"
    }
    
    # Save the new location to the database
    mongo_db.store.insert_one(new_store)
    new_store_id = new_store["store_id"]

    # 2. Update inventory to reference the new location for all items
    mongo_db.inventory.update_many(
        {},  # Update all inventory entries
        {"$set": {"store_id": new_store_id}}
    )

    print(f'Command: mongo_db.store.insert_one({new_store})')
    print('Command: mongo_db.inventory.update_many({},{"$set": {"store_id": new_store_id}})')
    
    # Confirm that the new location was created and inventory transferred
    print(f"New location {new_store_id} created and inventory transferred.")

def verify_staff_passwords():
    """
    This function verifies if staff members' passwords have been updated.
    """
    staff_members = mongo_db.staff.find()
    
    for staff in staff_members:
        if 'password' in staff:
            # Optional: To verify successful password storage,
            # decryption or further validation could be implemented here if needed
            print(f"Verifying password for staff member {staff['first_name']} {staff['last_name']}: {staff['password']}")
        else:
            # Output if no password is found for the staff member
            print(f"No password data found for staff member {staff['first_name']} {staff['last_name']}.")

def verify_new_store_and_inventory():
    """
    Verifies if the new location was successfully created and if inventory 
    entries have been correctly updated to reference the new location.
    """
    # Search for the new location in the database using its address
    new_store = mongo_db.store.find_one({"address": "123 New Location Street"})
    
    if new_store:
        # Confirm that the new location was successfully created
        print(f"New location successfully created: {new_store['store_id']} - {new_store['address']}")
        
        # Check if inventory references the new location
        inventory_items = mongo_db.inventory.find({"store_id": new_store['store_id']})
        item_count = mongo_db.inventory.count_documents({"store_id": new_store['store_id']})
        
        if item_count > 0:
            # Confirm that the inventory was successfully transferred
            print(f"Inventory successfully transferred to new location {new_store['store_id']}. Number of items: {item_count}")
        else:
            # Warning if no items are found for the new location
            print(f"No items found in inventory for the new location {new_store['store_id']}.")
    else:
        # Output if the new location was not found
        print("New location not found.")

# Establish a connection to the NoSQL database
mongo_db = MongoDBSingleton.get_instance()

print()
print("-" * 10 + " UPDATE " + "-" * 10)
print()

# Call functions to perform operations and validations
print('A: Assign a new secure password to all staff members')
verify_staff_passwords()  # Pre-check of existing passwords
print()
update_staff_passwords()  # Update staff members' passwords
print()
verify_staff_passwords()  # Post-check of updated passwords

print()

print('B: Create new location and transfer inventory')
add_new_store_with_inventory_transfer()  # Add new location and transfer inventory
print()
verify_new_store_and_inventory()  # Verify location and inventory update
