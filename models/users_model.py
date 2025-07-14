from datetime import datetime
import json
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import pytz
import questionary
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TextField,
    IntegerField,
    BooleanField,
    AutoField,
    DateTimeField,
    IntegrityError,
)
import pymysql

# Ensure MySQL compatibility
pymysql.install_as_MySQLdb()

from source_db import source_db
from dest_db import dest_db

# Constants
MAPPING_FILE_PATH = "user_mappings.json"
DEFAULT_PASSWORD_HASH = "$2y$12$dBmyHYZEBS72MrZ2704RF.dw3X2RFaE49y0X6FXR1mO47ehjggIqu"
DEFAULT_TIMEZONE = "UTC"


# ------------- MODELS ------------- #
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
    country_id = IntegerField(default=231)
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
    add_date = DateTimeField(default=datetime.now)
    added_by = IntegerField()

    class Meta:
        database = source_db
        table_name = "dealer_master"


# ------------- HELPER FUNCTIONS ------------- #
def process_user_status_and_email(user: User) -> Tuple[str, str]:
    """
    Process the user's email and determine status.
    Always returns a lowercase, stripped email.
    """
    email = user.email.rstrip("-").strip().lower()
    status = "active" if user.activstate == 1 else "blocked"
    return email, status


def generate_unique_username(email: str) -> str:
    """
    Generate a unique username based on the email's local part.
    If needed, a numeric suffix is added until uniqueness is achieved.
    """
    local_part = email.split("@")[0].lower().strip()
    username = re.sub(r"[^a-z0-9_]", "", local_part)
    base_username = username
    suffix = 1
    while DestinationUser.select().where(DestinationUser.username == username).exists():
        username = f"{base_username}{suffix}"
        suffix += 1
    return username


def load_mappings() -> Dict[str, Any]:
    """
    Load existing mappings from the JSON file.
    """
    try:
        with open(MAPPING_FILE_PATH, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def save_mappings(mappings: Dict[str, Any]) -> None:
    """
    Save mappings to the JSON file.
    """
    with open(MAPPING_FILE_PATH, "w") as file:
        json.dump(mappings, file, indent=4)


def safe_ask(prompt_func: Any, *args, **kwargs) -> Any:
    """
    Wrapper for questionary prompts.
    Exits gracefully if the user cancels.
    """
    answer = prompt_func(*args, **kwargs).ask()
    if answer is None:
        print("\nUser cancelled the prompt. Exiting gracefully.")
        sys.exit(0)
    return answer


def choose_start_option() -> str:
    """
    Prompt the user for how to start the migration.
    """
    return safe_ask(
        questionary.select,
        "Select how you want to start the migration:",
        choices=[
            "Start from the first dealer",
            "Migrate a specific dealer by ID",
            "Start from a specific dealer ID",
        ],
    )


def get_dealers(start_option: str) -> List[DealerMaster]:
    """
    Retrieve dealers based on the chosen start option.
    """
    if start_option == "Start from the first dealer":
        print("Starting migration from the first dealer...")
        return list(DealerMaster.select().order_by(DealerMaster.id.desc()))
    elif start_option == "Migrate a specific dealer by ID":
        dealer_id_input = safe_ask(questionary.text, "Enter the dealer ID to migrate:")
        try:
            dealer_id = int(dealer_id_input)
            dealer = DealerMaster.get(DealerMaster.id == dealer_id)
            print(f"Starting migration for dealer: {dealer.company} (ID: {dealer.id})")
            return [dealer]
        except (ValueError, DealerMaster.DoesNotExist):
            print(f"Invalid dealer ID or no dealer found with ID {dealer_id_input}.")
            sys.exit(1)
    elif start_option == "Start from a specific dealer ID":
        dealer_id_input = safe_ask(questionary.text, "Enter the dealer ID to start from:")
        try:
            dealer_id = int(dealer_id_input)
            dealers = DealerMaster.select().where(DealerMaster.id >= dealer_id).order_by(DealerMaster.id.desc())
            print(f"Starting migration from dealer ID {dealer_id} onward.")
            return list(dealers)
        except ValueError:
            print("Invalid dealer ID format. Please enter a valid numeric ID.")
            sys.exit(1)
    else:
        print("Invalid option selected.")
        sys.exit(1)


def list_unmigrated_users(dealer: DealerMaster, mappings: Dict[str, Any]) -> List[User]:
    """
    Retrieve users from the source database for a given dealer that have not yet been migrated.
    """
    users = User.select().where(User.company.contains(dealer.company))
    migrated_ids = {mapping.get("old_user_id") for mapping in mappings.values() if mapping.get("old_user_id") is not None}
    return [user for user in users if user.id not in migrated_ids]


def migrate_user(
    selected_user: User,
    dealer: DealerMaster,
    first_migrated_user: Optional[DestinationUser],
    mappings: Dict[str, Any],
) -> Optional[DestinationUser]:
    """
    Migrate a selected user to the destination database.
    If a first migrated user exists, assign its ID as parent_id.
    """
    print(f"\nMigrating user {selected_user.full_name}...")
    current_time = datetime.now(pytz.utc)
    add_date = selected_user.add_date
    email, status = process_user_status_and_email(selected_user)

    try:
        new_user = DestinationUser.create(
            name=selected_user.full_name,
            email=email,
            email_verified_at=current_time,
            password=DEFAULT_PASSWORD_HASH,
            username=selected_user.username or generate_unique_username(email),
            company=selected_user.company,
            status=status,
            phone=selected_user.mobile,
            mobile=selected_user.mobile,
            timezone=DEFAULT_TIMEZONE,
            country_id=231,
            created_at=add_date,
            updated_at=add_date,
        )
        # If a first migrated user exists, assign its ID as the parent_id.
        if first_migrated_user:
            print(f"Assigning parent_id: {first_migrated_user.id} to user {new_user.id}")
            new_user.parent_id = first_migrated_user.id
            new_user.save()
        print(f"User {selected_user.full_name} migrated successfully!")
        return new_user
    except IntegrityError as e:
        print(f"Error migrating user {selected_user.full_name}: {e}")
        return None


# ------------- MAIN MIGRATION FUNCTION ------------- #
def run_migration() -> None:
    """
    Main migration function with an interactive CLI.
    For each dealer, once the first user is migrated or created,
    that user's ID will be used as the parent_id for all further users.
    Yes/no prompts have been replaced with select-based (radio button) UIs.
    """
    mappings = load_mappings()
    start_option = choose_start_option()
    dealers = get_dealers(start_option)
    migrated_user_ids: List[int] = []
    first_migrated_user: Optional[DestinationUser] = None

    try:
        if source_db.is_closed():
            source_db.connect()
        if dest_db.is_closed():
            dest_db.connect()

        for dealer in dealers:
            dealer_response = safe_ask(
                questionary.select,
                f"Migrate {dealer.id} {dealer.company}?",
                choices=["Yes", "Skip"],
            )
            if dealer_response != "Yes":
                first_migrated_user = None  # Reset for new dealer.
                continue

            skip_dealer = False

            while not skip_dealer:
                unmigrated_users = list_unmigrated_users(dealer, mappings)
                if unmigrated_users:
                    print("\nAvailable unmigrated users for this dealer:")
                    for user in unmigrated_users:
                        print(f"ID: {user.id} | Name: {user.full_name} | Email: {user.email}")

                    # Offer options based on whether we have a first migrated user.
                    if not first_migrated_user:
                        user_choice = safe_ask(
                            questionary.select,
                            "Choose an option:",
                            choices=[
                                "Enter the ID of the user to migrate",
                                "Create a new user from dealer data",
                                "Skip this dealer",
                            ],
                        )
                    else:
                        user_choice = safe_ask(
                            questionary.select,
                            "Choose an option:",
                            choices=[
                                "Enter the ID of the user to migrate",
                                "Skip this dealer",
                            ],
                        )
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

                if user_choice == "Skip this dealer":
                    first_migrated_user = None
                    skip_dealer = True
                    break

                if user_choice == "Enter the ID of the user to migrate":
                    while True:
                        user_input = safe_ask(
                            questionary.text,
                            "Enter the ID of the user to migrate (or type 'skip' to skip this dealer):",
                        )
                        if user_input.lower() == "skip":
                            skip_dealer = True
                            break
                        try:
                            user_id = int(user_input)
                            selected_user = User.get_by_id(user_id)
                            print(f"\nYou selected:\nID: {selected_user.id} | Name: {selected_user.full_name} | Email: {selected_user.email}")
                            if str(selected_user.id) in mappings:
                                proceed = safe_ask(
                                    questionary.select,
                                    "This user is already migrated. Proceed anyway?",
                                    choices=["Yes", "No"],
                                )
                                if proceed == "No":
                                    continue
                            action = safe_ask(
                                questionary.select,
                                "Choose an action:",
                                choices=["migrate", "retry", "skip"],
                            )
                            if action == "migrate":
                                new_user = migrate_user(selected_user, dealer, first_migrated_user, mappings)
                                if new_user:
                                    # If no first migrated user exists, assign it.
                                    if not first_migrated_user:
                                        first_migrated_user = new_user
                                    else:
                                        if new_user.parent_id is None:
                                            new_user.parent_id = first_migrated_user.id
                                            new_user.save()
                                    migrated_user_ids.append(new_user.id)
                                    mappings[str(new_user.id)] = {"old_user_id": selected_user.id, "dealer_id": dealer.id}
                                    save_mappings(mappings)
                                    more = safe_ask(
                                        questionary.select,
                                        "Do you want to migrate more users from the same company?",
                                        choices=["Yes", "No"],
                                    )
                                    if more == "No":
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
                            print("Invalid input! Please enter a valid numeric user ID.")
                        except User.DoesNotExist:
                            print("No user found with that ID. Please try again.")
                    if skip_dealer:
                        break

                elif user_choice == "Create a new user from dealer data":
                    new_user_data = {
                        "name": dealer.company,
                        "email": dealer.email,
                        "company": dealer.company,
                        "mobile": dealer.phone,
                        "country_id": 231,
                        "username": generate_unique_username(dealer.email),
                    }
                    print("\nDefault data for the new user:")
                    for field, default_value in new_user_data.items():
                        user_val = safe_ask(
                            questionary.text,
                            f"{field.capitalize()} (Default: {default_value}):",
                        )
                        if user_val:
                            new_user_data[field] = user_val

                    current_time = datetime.now(pytz.utc)
                    try:
                        new_user = DestinationUser.create(
                            name=new_user_data["name"],
                            email=new_user_data["email"],
                            email_verified_at=current_time,
                            password=DEFAULT_PASSWORD_HASH,
                            username=new_user_data["username"],
                            company=new_user_data["company"],
                            status="active",
                            phone=new_user_data["mobile"],
                            mobile=new_user_data["mobile"],
                            timezone=DEFAULT_TIMEZONE,
                            country_id=231,
                            created_at=dealer.add_date,
                            updated_at=dealer.add_date,
                        )
                        # If a first migrated user already exists, update the parent_id.
                        if first_migrated_user:
                            print(f"Assigning parent_id: {first_migrated_user.id} to user {new_user.id}")
                            new_user.parent_id = first_migrated_user.id
                            new_user.save()
                        else:
                            first_migrated_user = new_user
                        print(f"New user created successfully for {dealer.company}!")
                        migrated_user_ids.append(new_user.id)
                        mappings[str(new_user.id)] = {"old_user_id": None, "dealer_id": dealer.id}
                        save_mappings(mappings)
                        more = safe_ask(
                            questionary.select,
                            "Do you want to migrate more users from the same company?",
                            choices=["Yes", "No"],
                        )
                        if more == "No":
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