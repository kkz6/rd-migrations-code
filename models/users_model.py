from datetime import datetime
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    BooleanField,
    AutoField,
    DateTimeField,
    IntegrityError
)
import sys
from source_db import source_db
from dest_db import dest_db
import pymysql
pymysql.install_as_MySQLdb()
import pytz
import re
import json

MAPPING_FILE_PATH = "user_mappings.json"


# Source Model
class User(Model):
    id = BigIntegerField(primary_key=True)
    username = TextField(null=True)
    password = TextField(null=True)
    full_name = TextField()
    company = TextField(null=True)
    activstate = IntegerField(default=1)
    email = CharField(max_length=255, unique=True)
    mobile = TextField(null=True)
    usertype = TextField(default="Installer")
    country = TextField(default="India")
    add_date = DateTimeField()
    added_by_user_id = IntegerField()
    forgotpassword = IntegerField(default=0)
    access_privilege_array = TextField()
    company_local = TextField()
    full_name_local = TextField()
    cms_support_email = CharField(max_length=100, null=True)
    cms_support_mobileno = CharField(max_length=20, null=True)
    is_cms_admin = BooleanField(null=True)

    class Meta:
        database = source_db
        table_name = "users"


# Destination Model
class DestinationUser(Model):
    id = BigIntegerField(primary_key=True)  # Use the ID from the source
    name = CharField(max_length=255, null=False)
    email = CharField(max_length=255, null=False)
    parent_id = BigIntegerField(null=True)
    email_verified_at = DateTimeField(null=True)
    password = CharField(max_length=255, null=False)
    username = CharField(max_length=255, null=True, unique=True)
    company = CharField(max_length=255, null=True)
    status = CharField(
        max_length=10, default="active", choices=["active", "blocked"], null=False
    )
    phone = CharField(max_length=255, null=True)
    mobile = CharField(max_length=255, null=True)
    emirates = CharField(max_length=255, null=True)
    timezone = CharField(max_length=255, null=True)
    country = CharField(max_length=255, null=True)
    state = CharField(max_length=255, null=True)
    remember_token = CharField(max_length=100, null=True)
    created_at = DateTimeField(null=True)
    updated_at = DateTimeField(null=True)


    class Meta:
        database = dest_db
        table_name = "users"


class DealerMaster(Model):
    id = AutoField()
    company = CharField(max_length=300)
    email = CharField(max_length=200, unique=True)
    phone = TextField()
    mobile = TextField()
    emirate = CharField(max_length=20, default="NA")
    status = CharField(max_length=20, default="AFC")
    salesuser = CharField(max_length=20)
    add_date = TimestampField(default=datetime.now)
    added_by = IntegerField()

    class Meta:
        database = source_db
        table_name = "dealer_master"

def process_user_status_and_email(user):
    """
    Helper function to process a user's email and status based on:
    - Email's trailing hyphen
    - User's activstate
    
    Returns the processed email and status.
    """
    # Process email (remove trailing '-' if present)
    email = user.email.rstrip("-").strip().lower()

    # Check if the email ends with '-' and set status as blocked
    status = "blocked" if user.email.endswith("-") else "active"
    
    # Update the status based on activstate
    status = "active" if user.activstate == 1 else "blocked"
    
    # Return processed email and status
    return email, status

def generate_unique_username(email):
    """
    Generate a unique username based on the email's local part (before @).
    Ensures that the username is unique in the destination database by adding a numeric suffix if needed.
    
    Parameters:
    email (str): The email address to generate the username from.

    Returns:
    str: A unique username.
    """

    # Step 1: Extract the local part of the email (before '@')
    local_part = email.split('@')[0]  # Local part before '@'

    # Step 2: Clean the local part:
    # - Convert to lowercase
    # - Remove special characters (except alphanumeric and underscores)
    username = local_part.lower().strip()
    username = re.sub(r"[^a-z0-9_]", "", username)  # Remove special characters

    # Step 3: Check if the username exists in the destination database
    base_username = username
    suffix = 1
    while DestinationUser.select().where(DestinationUser.username == username).exists():
        username = f"{base_username}{suffix}"  # Add numeric suffix to the username
        suffix += 1

    # Return the unique username
    return username

def load_mappings():
    try:
        with open(MAPPING_FILE_PATH, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}
    
def save_mappings(mappings):
    with open(MAPPING_FILE_PATH, "w") as file:
        json.dump(mappings, file, indent=4)

def run_migration():
    """
    Migrate dealers interactively by asking for user confirmation and selecting user IDs.
    """
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    # Load existing mappings
    mappings = load_mappings()

    try:
        print("Select how you want to start the migration:")
        print("1. Start from the first dealer.")
        print("2. Migrate a specific dealer by ID.")
        print("3. Start from a specific dealer ID.")
        
        choice = input("Enter your choice (1/2/3): ").strip()

        if choice == "1":
            # Option 1: Start from the first dealer
            print("Starting migration from the first dealer...")
            dealers = DealerMaster.select().order_by(DealerMaster.id.desc())  # Sorting in descending order
            start_dealer = None
        
        elif choice == "2":
            # Option 2: Migrate specific dealer by ID
            dealer_id = input("Enter the dealer ID to migrate: ").strip()
            try:
                start_dealer = DealerMaster.get(DealerMaster.id == int(dealer_id))
                print(f"Starting migration for dealer: {start_dealer.company} (ID: {start_dealer.id})")
                dealers = [start_dealer]
            
            except DealerMaster.DoesNotExist:
                print(f"No dealer found with ID {dealer_id}. Please try again.")
                return
        
        elif choice == "3":
            # Option 3: Start from a specific dealer ID (for example, the user enters an ID)
            start_dealer_id = input("Enter the dealer ID to start from: ").strip()
            try:
                dealers = DealerMaster.select().where(DealerMaster.id >= int(start_dealer_id)).order_by(DealerMaster.id.desc())
                print(f"Starting migration from dealer ID {start_dealer_id} onward.")
                start_dealer = None
                
            except ValueError:
                print("Invalid dealer ID format. Please enter a valid numeric ID.")
                return
        
        else:
            print("Invalid choice. Exiting.")
            return
        
        migrated_user_ids = []  # Track migrated users to prevent showing them again
        first_migrated_user = None  # Track the first migrated user for sub-user parent ID updates

        # Step 1: Loop through the DealerMaster table
        for dealer in dealers:
            print(f"\nMigrate {dealer.company}?")
            response = input("Type 'yes' to migrate or 'skip' to skip: ").strip().lower()

            if response == "yes":
                # Step 2: Search for users from the User table matching the company name (with minor variations)
                users_in_company = User.select().where(User.company.contains(dealer.company))  # Match with minor variations
                filtered_users = [user for user in users_in_company if user.id not in migrated_user_ids]

                if filtered_users:
                    print("\nUsers found for this dealer (excluding migrated users):")
                    for user in filtered_users:
                        print(f"ID: {user.id} | Name: {user.full_name} | Email: {user.email}")

                    print("\nOptions:")
                    print("1. Enter the ID of the user to migrate.")
                    print("2. Create a new user from dealer data.")
                    user_choice = input("Enter your choice (1/2): ").strip()

                    if user_choice == "1":
                        while True:
                            # Step 3: Ask user to input the ID of the user to migrate
                            user_id = input("\nEnter the ID of the user to migrate (or type 'skip' to skip this dealer): ").strip()

                            if user_id.lower() == "skip":
                                break  # Skip this dealer, move on to the next one

                            try:
                                user_id = int(user_id)  # Ensure the input is an integer

                                # Step 4: Fetch the selected user
                                selected_user = User.get_by_id(user_id)

                                # Show the selected user details for confirmation
                                print(f"\nYou selected the following user:\nID: {selected_user.id} | Name: {selected_user.full_name} | Email: {selected_user.email}")

                                # Step 5: Check if the user is already migrated
                                if str(selected_user.id) in mappings:
                                    print(f"Warning: User with ID {selected_user.id} is already migrated!")
                                    proceed = input("Do you want to proceed with the migration? (yes/no): ").strip().lower()
                                    if proceed != "yes":
                                        continue  # Skip this user and ask for another ID

                                # Step 6: Ask if they want to migrate this user
                                action = input("Type 'migrate' to migrate this user, 'retry' to select a different user, or 'skip' to skip this user: ").strip().lower()

                                if action == "migrate":
                                    # Perform migration for the selected user
                                    print(f"\nMigrating user {selected_user.full_name}...")
                                    current_time = datetime.now(pytz.utc)

                                    # Migrate user to destination user table
                                    # Call your migration logic here to create a user record in the destination table
                                    add_date = selected_user.add_date
                                    email, status = process_user_status_and_email(selected_user)
                                    try:
                                        new_user = DestinationUser.create(
                                            name=selected_user.full_name,
                                            email=email,
                                            email_verified_at=current_time,
                                            password="$2y$10$4sCgBDych20ZjQ8EY/z4SOKNRObHjl6LWe02OmI3Ht4cktxPHNAmC",  # Pre-defined password hash
                                            username=selected_user.username,
                                            company=selected_user.company,
                                            status=status,
                                            phone=selected_user.mobile,
                                            mobile=selected_user.mobile,
                                            timezone="UTC",
                                            country=selected_user.country,
                                            created_at=add_date,
                                            updated_at=add_date,
                                        )

                                        print(f"User {selected_user.full_name} migrated successfully!")

                                        # Update parent_id for any sub-users
                                        if first_migrated_user is not None:
                                            # Set the parent_id of sub-users to the first migrated user's ID
                                            print(f"Updating parent_id for sub-user {new_user.name} (ID: {new_user.id})")
                                            new_user.parent_id = first_migrated_user.id
                                            new_user.save()

                                        # Add this migrated user's ID to the list
                                        migrated_user_ids.append(new_user.id)

                                        # Store the first migrated user for setting parent_id for sub-users
                                        if first_migrated_user is None:
                                            first_migrated_user = new_user

                                        # Step 7: Update the mappings
                                        mappings[str(new_user.id)] = {
                                            "old_user_id": selected_user.id,
                                            "dealer_id": dealer.id
                                        }
                                        save_mappings(mappings)

                                        # Step 8: Ask if they want to migrate more sub-users for the same dealer
                                        migrate_more = input("Do you want to migrate more users from the same company? (yes/skip): ").strip().lower()
                                        if migrate_more != "yes":
                                            first_migrated_user = None
                                            break  # Exit the loop to proceed with the next dealer
                                    except IntegrityError as e:
                                        print(f"Error: A user with the email '{email}' or username '{selected_user.username}' could not be created. Might exist already.")
                                        retry = input("Do you want to retry with different data? (yes/retry) or skip (skip): ").strip().lower()
                                        
                                        if retry == "yes" or retry == "retry":
                                            print("Please provide new data for the user.")
                                            continue  # Continue to retry user creation logic
                                        elif retry == "skip":
                                            print(f"Skipping migration of user {selected_user.full_name}...")
                                            continue  # Skip to the next user
                                        else:
                                            print("Invalid choice. Skipping to next dealer.")
                                            continue
                                elif action == "retry":
                                    first_migrated_user = None
                                    continue  # Retry by asking for the user ID again
                                elif action == "skip":
                                    first_migrated_user = None
                                    break  # Skip this user and move on to the next one

                            except ValueError:
                                print("Invalid input! Please enter a valid user ID.")
                            except User.DoesNotExist:
                                print(f"No user found with ID {user_id}. Please try again.")
                    elif user_choice == "2":
                        # Create a new user from dealer data
                        print(f"\nCreating a new user for company: {dealer.company}")
                        new_user_data = {
                            "name": dealer.company,  # Default name based on the dealer company
                            "email": dealer.email,  # Default email
                            "company": dealer.company,
                            "mobile": dealer.phone,  # Default phone
                            "country": "UAE",  # Default country based on dealer info,
                            "username": generate_unique_username(dealer.email)
                        }

                        # Ask for input or use default values
                        print("\nDefault data for the new user:")
                        for field, default_value in new_user_data.items():
                            user_input = input(f"{field.capitalize()} (Default: {default_value}): ").strip()
                            if user_input:
                                new_user_data[field] = user_input

                        # Create the new user record in the destination table
                        current_time = datetime.now(pytz.utc)
                        try:
                            new_user = DestinationUser.create(
                                name=new_user_data["name"],
                                email=new_user_data["email"],
                                email_verified_at=current_time,
                                password="$2y$10$4sCgBDych20ZjQ8EY/z4SOKNRObHjl6LWe02OmI3Ht4cktxPHNAmC",  # Pre-defined password hash
                                username=new_user_data["username"],
                                company=new_user_data["company"],
                                status="active",
                                phone=new_user_data["mobile"],
                                mobile=new_user_data["mobile"],
                                timezone="UTC",
                                country=new_user_data["country"],
                                created_at=current_time,
                                updated_at=current_time,
                            )

                            print(f"New user created successfully for {dealer.company}!")

                            # Add the newly created user's ID to the migrated list
                            migrated_user_ids.append(new_user.id)

                            # Store the first migrated user for setting parent_id for sub-users
                            if first_migrated_user is None:
                                first_migrated_user = new_user

                            # Step 9: Update the mappings
                            mappings[str(new_user.id)] = {
                                "old_user_id": None,  # Since this is a new user, old_user_id is None
                                "dealer_id": dealer.id
                            }
                            save_mappings(mappings)

                            # Step 10: Ask if they want to migrate more sub-users for the same dealer
                            migrate_more = input("Do you want to migrate more users from the same company? (yes/skip): ").strip().lower()
                            if migrate_more != "yes":
                                first_migrated_user = None
                                continue  # Move on to the next dealer

                        except IntegrityError as e:
                            # Handle duplicate entry error
                            print(f"Error: A user with the email '{new_user_data['email']}' or username '{new_user_data['username']}' could not be created. Might exist already.")
                            retry = input("Do you want to retry with different data? (yes/retry) or skip (skip): ").strip().lower()
                            
                            if retry == "yes" or retry == "retry":
                                print("Please provide new data for the user.")
                                continue  # Retry the user creation
                            elif retry == "skip":
                                print(f"Skipping creation of user for {dealer.company}...")
                                continue  # Skip to the next step or dealer

                            else:
                                print("Invalid choice. Skipping to next dealer.")
                                continue
                    else:
                        print("Invalid choice. Skipping this dealer.")
                        continue
                else:
                    print(f"No users found for dealer {dealer.company}.")

                    # Step 11: Ask the user to create a new user if no users are found for this company
                    print(f"\nCreating a new user for company: {dealer.company}")
                    new_user_data = {
                        "name": dealer.company,  # Default name based on the dealer company
                        "email": dealer.email,  # Default email
                        "company": dealer.company,
                        "mobile": dealer.phone,  # Default phone
                        "country": "UAE",  # Default country based on dealer info,
                        "username": generate_unique_username(dealer.email)
                    }

                    # Ask for input or use default values
                    print("\nDefault data for the new user:")
                    for field, default_value in new_user_data.items():
                        user_input = input(f"{field.capitalize()} (Default: {default_value}): ").strip()
                        if user_input:
                            new_user_data[field] = user_input

                    # Create the new user record in the destination table
                    current_time = datetime.now(pytz.utc)
                    try:
                        new_user = DestinationUser.create(
                            name=new_user_data["name"],
                            email=new_user_data["email"],
                            email_verified_at=current_time,
                            password="$2y$10$4sCgBDych20ZjQ8EY/z4SOKNRObHjl6LWe02OmI3Ht4cktxPHNAmC",  # Pre-defined password hash
                            username=new_user_data["username"],
                            company=new_user_data["company"],
                            status="active",
                            phone=new_user_data["mobile"],
                            mobile=new_user_data["mobile"],
                            timezone="UTC",
                            country=new_user_data["country"],
                            created_at=current_time,
                            updated_at=current_time,
                        )

                        print(f"New user created successfully for {dealer.company}!")

                        # Add the newly created user's ID to the migrated list
                        migrated_user_ids.append(new_user.id)

                        # Store the first migrated user for setting parent_id for sub-users
                        if first_migrated_user is None:
                            first_migrated_user = new_user

                        # Step 12: Update the mappings
                        mappings[str(new_user.id)] = {
                            "old_user_id": None,  # Since this is a new user, old_user_id is None
                            "dealer_id": dealer.id
                        }
                        save_mappings(mappings)

                        # Step 13: Ask if they want to migrate more sub-users for the same dealer
                        migrate_more = input("Do you want to migrate more users from the same company? (yes/skip): ").strip().lower()
                        if migrate_more != "yes":
                            first_migrated_user = None
                            continue  # Move on to the next dealer
                    except IntegrityError as e:
                        # Handle duplicate entry error
                        print(f"Error: A user with the email '{new_user_data['email']}' or username '{new_user_data['username']}'  could not be created. Might exist already.")
                        retry = input("Do you want to retry with different data? (yes/retry) or skip (skip): ").strip().lower()
                        
                        if retry == "yes" or retry == "retry":
                            print("Please provide new data for the user.")
                            continue  # Retry the user creation
                        elif retry == "skip":
                            print(f"Skipping creation of user for {dealer.company}...")
                            continue  # Skip to the next step or dealer

                        else:
                            print("Invalid choice. Skipping to next dealer.")
                            continue
            elif response == "skip":
                first_migrated_user = None
                continue  # Skip this dealer and move on to the next one

        print("Migration process complete.")
    except KeyboardInterrupt:
        print("\nMigration process interrupted. Exiting gracefully...")
        # Close any open database connections
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
        sys.exit(0)  # Exit gracefully

    except Exception as e:
        print(f"Error during migration: {str(e)}")

    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()