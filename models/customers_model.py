import json
import questionary
from openpyxl import Workbook
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    CompositeKey,
    DateTimeField,
    IntegrityError,
)
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser

# Constants
CUSTOMER_MAPPING_FILE = "customer_mappings.json"
USER_MAPPING_FILE = "user_mappings.json"
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Replace with admin email
EXCEL_FILE_NAME = "customer_migration_report.xlsx"


# Source Model
class CustomerMaster(Model):
    id = IntegerField(primary_key=True)
    company = TextField()
    email = TextField()
    o_address = TextField()
    o_contactphone = TextField()
    add_date = DateTimeField()  # Changed to DateTimeField
    user_id = IntegerField()
    company_local = TextField()

    class Meta:
        database = source_db
        table_name = "customer_master"


# Destination Models
class Customer(Model):
    id = BigIntegerField(primary_key=True)
    email = CharField(max_length=255)
    name = CharField(max_length=255)
    name_local = CharField(max_length=255, null=True)  # Added name_local
    address = TextField()
    contact_number = CharField(max_length=255)
    user_id = BigIntegerField()
    created_at = DateTimeField()  # Added created_at
    updated_at = DateTimeField()  # Added updated_at

    class Meta:
        database = dest_db
        table_name = "customers"


class CustomerDealer(Model):
    customer_id = BigIntegerField()
    dealer_id = BigIntegerField()

    class Meta:
        database = dest_db
        table_name = "customer_dealer"
        primary_key = CompositeKey("customer_id", "dealer_id")


# Helper Functions
def load_user_mappings():
    """Load user mappings from the JSON file."""
    try:
        with open(USER_MAPPING_FILE, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print("User mappings file not found. Ensure user_mappings.json exists.")
        return {}


def load_customer_mappings():
    """Load customer mappings from the JSON file."""
    try:
        with open(CUSTOMER_MAPPING_FILE, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"{CUSTOMER_MAPPING_FILE} not found. Creating a new one.")
        return {}


def save_customer_mappings(customer_mappings):
    """Save customer mappings to the JSON file."""
    with open(CUSTOMER_MAPPING_FILE, "w") as file:
        json.dump(customer_mappings, file, indent=4)


def get_new_user_id_from_mapping(old_user_id, user_mappings):
    """
    Get the new user_id from the user_mappings.json file based on the old user_id.
    """
    for new_user_id, mapping in user_mappings.items():
        if mapping.get("old_user_id") == old_user_id:
            return int(new_user_id)  # Return the new user ID as an integer
    return None  # Return None if no mapping is found


def get_default_user():
    """Get the default user for the 'created_by' field."""
    try:
        return DestinationUser.get(DestinationUser.email == DEFAULT_USER_EMAIL)
    except Exception as e:
        raise Exception(f"Default user with email {DEFAULT_USER_EMAIL} not found.") from e


def generate_excel_report(migrated_data, unmigrated_data):
    """Generate an Excel file with migrated and unmigrated customer data."""
    workbook = Workbook()
    migrated_sheet = workbook.active
    migrated_sheet.title = "Migrated Customers"
    unmigrated_sheet = workbook.create_sheet(title="Unmigrated Customers")

    # Headers for Migrated Customers
    migrated_sheet.append(
        [
            "Customer ID",
            "Customer Name",
            "Customer Name Local",
            "Email",
            "Address",
            "Contact Number",
            "New Dealer ID",
            "Old User ID",
        ]
    )

    for record in migrated_data:
        migrated_sheet.append(
            [
                record["customer_id"],
                record["customer_name"],
                record["customer_name_local"],
                record["email"],
                record["address"],
                record["contact_number"],
                record["new_dealer_id"],
                record["old_user_id"],
            ]
        )
        # Sub-row showing old user and new user details
        migrated_sheet.append(
            [
                "",
                "Old User",
                record["old_user_email"],
                "",
                "",
                "",
                "New User",
                record["new_user_email"],
            ]
        )

    # Headers for Unmigrated Customers
    unmigrated_sheet.append(
        [
            "Customer ID",
            "Customer Name",
            "Email",
            "Address",
            "Contact Number",
            "Reason",
        ]
    )

    for record in unmigrated_data:
        unmigrated_sheet.append(
            [
                record["id"],
                record["name"],
                record["email"],
                record["address"],
                record["contact_number"],
                record["reason"],
            ]
        )

    workbook.save(EXCEL_FILE_NAME)
    print(f"\nMigration report saved as {EXCEL_FILE_NAME}")


def migrate_customers(automated=False):
    """Migrate all customers and populate the customer_dealer pivot table."""
    migrated_data = []
    unmigrated_data = []
    total_records = CustomerMaster.select().count()
    migrated_count = 0
    skipped_count = 0

    # Establish database connections if not already connected
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    # Load user and customer mappings
    user_mappings = load_user_mappings()
    customer_mappings = load_customer_mappings()

    try:
        for record in CustomerMaster.select():
            try:
                # Check if the customer is already migrated
                if str(record.id) in customer_mappings:
                    print(
                        f"Customer {record.company} (ID: {record.id}) already migrated. Skipping..."
                    )
                    skipped_count += 1
                    continue

                # Get new dealer_id from the user mappings
                new_dealer_id = get_new_user_id_from_mapping(
                    record.user_id, user_mappings
                )
                if not new_dealer_id:
                    print(
                        f"Skipping Customer {record.company} (ID: {record.id}) - No mapped dealer found."
                    )
                    unmigrated_data.append(
                        {
                            "id": record.id,
                            "name": record.company,
                            "email": record.email,
                            "address": record.o_address,
                            "contact_number": record.o_contactphone,
                            "reason": "No mapped dealer found",
                        }
                    )
                    skipped_count += 1
                    continue

                # Insert or update the customer record
                new_customer_id = migrate_single_customer(record)

                # Populate the customer_dealer table
                CustomerDealer.create(
                    customer_id=new_customer_id, dealer_id=new_dealer_id
                )

                # Save the mapping
                customer_mappings[new_customer_id] = record.id
                save_customer_mappings(customer_mappings)

                # Add to migrated data
                migrated_data.append(
                    {
                        "customer_id": new_customer_id,
                        "customer_name": record.company,
                        "customer_name_local": record.company_local,
                        "email": record.email,
                        "address": record.o_address,
                        "contact_number": record.o_contactphone,
                        "new_dealer_id": new_dealer_id,
                        "old_user_id": record.user_id,
                        "old_user_email": "Old_User_Email",  # Replace with actual old user email if available
                        "new_user_email": DestinationUser.get_by_id(new_dealer_id).email,
                        "created_at": record.add_date,
                        "updated_at": record.add_date,
                    }
                )

                if automated:
                    migrated_count += 1
                else:
                    proceed = questionary.confirm(
                        f"Do you want to migrate Customer: {record.company} (Email: {record.email})?"
                    ).ask()
                    if proceed:
                        migrated_count += 1
                    else:
                        skipped_count += 1

            except KeyboardInterrupt:
                print("\nMigration interrupted. Exiting gracefully...")
                raise
            except Exception as e:
                print(
                    f"Error migrating Customer {record.company} ({record.email}): {e}"
                )
                unmigrated_data.append(
                    {
                        "id": record.id,
                        "name": record.company,
                        "email": record.email,
                        "address": record.o_address,
                        "contact_number": record.o_contactphone,
                        "reason": str(e),
                    }
                )
                skipped_count += 1

    finally:
        # Generate Excel report in automated mode
        if automated:
            generate_excel_report(migrated_data, unmigrated_data)

        # Close connections
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Migration Summary
    print(f"\nMigration Summary:")
    print(f"Total records: {total_records}")
    print(f"Successfully migrated: {migrated_count}")
    print(f"Skipped/Failed: {skipped_count}")


def migrate_single_customer(record):
    """
    Migrate a single customer to the destination database.
    Returns the new customer ID.
    """
    try:
        customer = Customer.create(
            id=record.id,
            email=record.email,
            name=record.company,
            name_local=record.company_local,
            address=record.o_address,
            contact_number=record.o_contactphone,
            user_id=record.user_id,
            created_at=record.add_date,
            updated_at=record.add_date,
        )
        print(f"Migrated Customer: {record.company} (New ID: {customer.id})")
        return customer.id
    except IntegrityError as e:
        if "Duplicate entry" in str(e):
            print(
                f"Duplicate entry for {record.email}. Attempting to update existing record..."
            )
            Customer.update(
                {
                    "name": record.company,
                    "name_local": record.company_local,
                    "address": record.o_address,
                    "contact_number": record.o_contactphone,
                    "created_at": record.add_date,
                    "updated_at": record.add_date,
                }
            ).where(Customer.email == record.email).execute()
            updated_customer = Customer.get(Customer.email == record.email)
            print(f"Updated existing Customer: {record.company}")
            return updated_customer.id
        else:
            raise


def run_migration():
    """Main function to run the customer migration."""
    try:
        # Select migration mode
        mode = questionary.select(
            "How would you like to perform the migration?",
            choices=[
                "Run Fully Automated",
                "Migrate Customers One by One",
                "Migrate a Single Customer by ID",
            ],
        ).ask()

        if mode == "Run Fully Automated":
            print("Running Fully Automated Migration...")
            migrate_customers(automated=True)

        elif mode == "Migrate Customers One by One":
            print("Running Migration One by One...")
            migrate_customers(automated=False)

        elif mode == "Migrate a Single Customer by ID":
            customer_id = questionary.text(
                "Enter the Customer ID to migrate:"
            ).ask()
            if not customer_id.isdigit():
                print("Invalid Customer ID. Please enter a numeric value.")
                return
            record = CustomerMaster.get_by_id(int(customer_id))
            migrate_single_customer(record)
        else:
            print("Invalid choice. Exiting...")
            return

    except KeyboardInterrupt:
        print("\nMigration interrupted. Exiting gracefully...")
    except Exception as e:
        print(f"Error during customer migration: {e}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()