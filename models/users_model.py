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
import questionary

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

def safe_ask(prompt_func, *args, **kwargs):
    """
    Calls a questionary prompt function and exits gracefully if None is returned.
    """
    answer = prompt_func(*args, **kwargs).ask()
    if answer is None:
        print("\nUser cancelled the prompt. Exiting gracefully.")
        sys.exit(0)
    return answer


def choose_start_option():
    """Prompt the user to choose how to start the migration."""
    return safe_ask(
        questionary.select,
        "Select how you want to start the migration:",
        choices=[
            "Start from the first dealer",
            "Migrate a specific dealer by ID",
            "Start from a specific dealer ID",
        ],
    )


def get_dealers(start_option):
    """Based on the chosen start option, return the list of dealers to process."""
    if start_option == "Start from the first dealer":
        print("Starting migration from the first dealer...")
        dealers = DealerMaster.select().order_by(DealerMaster.id.desc())
        return dealers
    elif start_option == "Migrate a specific dealer by ID":
        dealer_id = safe_ask(questionary.text, "Enter the dealer ID to migrate:")
        try:
            dealer = DealerMaster.get(DealerMaster.id == int(dealer_id))
            print(f"Starting migration for dealer: {dealer.company} (ID: {dealer.id})")
            return [dealer]
        except DealerMaster.DoesNotExist:
            print(f"No dealer found with ID {dealer_id}.")
            sys.exit(1)
    elif start_option == "Start from a specific dealer ID":
        dealer_id = safe_ask(questionary.text, "Enter the dealer ID to start from:")
        try:
            dealers = DealerMaster.select().where(DealerMaster.id >= int(dealer_id)).order_by(DealerMaster.id.desc())
            print(f"Starting migration from dealer ID {dealer_id} onward.")
            return dealers
        except ValueError:
            print("Invalid dealer ID format. Please enter a valid numeric ID.")
            sys.exit(1)
    else:
        print("Invalid option selected.")
        sys.exit(1)


def list_unmigrated_users(dealer, mappings):
    """Return a list of unmigrated users for the given dealer."""
    # Get all users that match the dealer’s company.
    users = User.select().where(User.company.contains(dealer.company))
    # Build a set of already migrated old user IDs.
    migrated_ids = {mapping["old_user_id"] for mapping in mappings.values() if mapping.get("old_user_id") is not None}
    # Filter out migrated users.
    return [user for user in users if user.id not in migrated_ids]


def migrate_user(selected_user, dealer, first_migrated_user, mappings):
    """Migrate the given user and update mappings and parent assignment."""
    print(f"\nMigrating user {selected_user.full_name}...")
    current_time = datetime.now(pytz.utc)
    add_date = selected_user.add_date
    email, status = process_user_status_and_email(selected_user)
    try:
        new_user = DestinationUser.create(
            name=selected_user.full_name,
            email=email,
            email_verified_at=current_time,
            password="$2y$10$4sCgBDych20ZjQ8EY/z4SOKNRObHjl6LWe02OmI3Ht4cktxPHNAmC",
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
        if first_migrated_user is not None:
            print(f"Updating parent_id for sub-user {new_user.name} (ID: {new_user.id})")
            new_user.parent_id = first_migrated_user.id
            new_user.save()
        return new_user
    except IntegrityError as e:
        print(f"Error: {e}")
        return None


def run_migration():
    """Main migration function with an improved CLI UI using Questionary."""
    # Open database connections if needed.
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    mappings = load_mappings()
    start_option = choose_start_option()
    dealers = get_dealers(start_option)
    migrated_user_ids = []  # Track IDs of new migrated users (if needed)
    first_migrated_user = None  # Primary user for current dealer

    try:
        for dealer in dealers:
            response = safe_ask(
                questionary.select,
                f"Migrate {dealer.company}?",
                choices=["yes", "skip"],
            )
            if response != "yes":
                first_migrated_user = None
                continue

            skip_dealer = False

            # Process each dealer in an inner loop until the user chooses to stop.
            while not skip_dealer:
                unmigrated_users = list_unmigrated_users(dealer, mappings)
                if unmigrated_users:
                    print("\nAvailable unmigrated users for this dealer:")
                    for user in unmigrated_users:
                        print(f"ID: {user.id} | Name: {user.full_name} | Email: {user.email}")

                    # If no primary user exists, let the user choose between selecting an existing user or creating a new one.
                    if first_migrated_user is None:
                        user_choice = safe_ask(
                            questionary.select,
                            "Choose an option:",
                            choices=[
                                "Select a user to migrate",
                                "Create a new user from dealer data",
                            ],
                        )
                    else:
                        # Primary user exists; default to selecting an existing user.
                        user_choice = "Select a user to migrate"
                else:
                    print("\nNo unmigrated users found for this dealer.")
                    user_choice = safe_ask(
                        questionary.select,
                        "Choose an option:",
                        choices=[
                            "Create a new user from dealer data",
                            "Skip this dealer",
                        ],
                    )
                    if user_choice == "Skip this dealer":
                        break

                # Handle the choice.
                if user_choice == "Select a user to migrate":
                    # Use a select prompt to choose from the available users.
                    choices = [
                        questionary.Choice(
                            title=f"{user.id} - {user.full_name} - {user.email}", value=user.id
                        )
                        for user in unmigrated_users
                    ]
                    choices.append(questionary.Choice(title="Skip this dealer", value="skip"))
                    selected_value = safe_ask(
                        questionary.select,
                        "Select a user to migrate:",
                        choices=choices,
                    )
                    if selected_value == "skip":
                        skip_dealer = True
                        break
                    try:
                        selected_user = User.get_by_id(selected_value)
                        print(f"\nYou selected:\nID: {selected_user.id} | Name: {selected_user.full_name} | Email: {selected_user.email}")
                        if str(selected_user.id) in mappings:
                            proceed = safe_ask(
                                questionary.confirm,
                                "This user is already migrated. Proceed anyway?",
                                default=False,
                            )
                            if not proceed:
                                continue
                        action = safe_ask(
                            questionary.select,
                            "Choose an action:",
                            choices=["migrate", "retry", "skip"],
                        )
                        if action == "migrate":
                            new_user = migrate_user(selected_user, dealer, first_migrated_user, mappings)
                            if new_user:
                                if first_migrated_user is None:
                                    first_migrated_user = new_user
                                migrated_user_ids.append(new_user.id)
                                mappings[str(new_user.id)] = {"old_user_id": selected_user.id, "dealer_id": dealer.id}
                                save_mappings(mappings)
                                more = safe_ask(
                                    questionary.confirm,
                                    "Do you want to migrate more users from the same company?",
                                    default=True,
                                )
                                if not more:
                                    first_migrated_user = None
                                    skip_dealer = True
                            break
                        elif action == "retry":
                            continue
                        elif action == "skip":
                            first_migrated_user = None
                            skip_dealer = True
                            break
                    except ValueError:
                        print("Invalid input! Please select a valid user.")
                    except User.DoesNotExist:
                        print("No user found with that selection. Please try again.")
                    if skip_dealer:
                        break

                elif user_choice == "Create a new user from dealer data":
                    new_user_data = {
                        "name": dealer.company,
                        "email": dealer.email,
                        "company": dealer.company,
                        "mobile": dealer.phone,
                        "country": "UAE",
                        "username": generate_unique_username(dealer.email),
                    }
                    print("\nDefault data for the new user:")
                    for field, default_value in new_user_data.items():
                        user_val = safe_ask(
                            questionary.text,
                            f"{field.capitalize()} (Default: {default_value}):"
                        )
                        if user_val:
                            new_user_data[field] = user_val
                    current_time = datetime.now(pytz.utc)
                    try:
                        new_user = DestinationUser.create(
                            name=new_user_data["name"],
                            email=new_user_data["email"],
                            email_verified_at=current_time,
                            password="$2y$10$4sCgBDych20ZjQ8EY/z4SOKNRObHjl6LWe02OmI3Ht4cktxPHNAmC",
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
                        migrated_user_ids.append(new_user.id)
                        if first_migrated_user is None:
                            first_migrated_user = new_user
                        mappings[str(new_user.id)] = {"old_user_id": None, "dealer_id": dealer.id}
                        save_mappings(mappings)
                        more = safe_ask(
                            questionary.confirm,
                            "Do you want to migrate more users from the same company?",
                            default=True,
                        )
                        if not more:
                            first_migrated_user = None
                            skip_dealer = True
                    except IntegrityError as e:
                        retry = safe_ask(
                            questionary.select,
                            f"Error: {e}\nRetry with different data or skip?",
                            choices=["Retry", "Skip"],
                        )
                        if retry == "Skip":
                            skip_dealer = True
                            break
                        else:
                            continue

                else:
                    skip_dealer = True
                    break

        print("Migration process complete.")

    except KeyboardInterrupt:
        print("\nMigration process interrupted. Exiting gracefully...")
        sys.exit(0)
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()