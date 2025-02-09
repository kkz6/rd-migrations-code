import json
import questionary
from openpyxl import Workbook
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TextField,
    IntegerField,
    ForeignKeyField,
    DateTimeField,
    IntegrityError,
    DoesNotExist,
)
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser

# Constants
TECHNICIANS_MAPPING_FILE = "technicians_mapping.json"
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Replace with the known admin email
EXCEL_FILE_NAME = "technician_migration_report.xlsx"


# Source Model
class TechnicianMaster(Model):
    id = IntegerField(primary_key=True)
    technician_name = TextField()
    technician_phone = TextField()
    technician_email = TextField()
    add_date = DateTimeField()
    user_id = IntegerField()

    class Meta:
        database = source_db
        table_name = "technician_master"


# Destination Model
class Technician(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    email = CharField(max_length=255)
    phone = CharField(max_length=255)
    user_id = BigIntegerField()
    created_by = ForeignKeyField(
        DestinationUser, field="id", null=True, column_name="created_by"
    )
    created_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        database = dest_db
        table_name = "technicians"


# Helper Functions
def load_user_mappings():
    """Load user mappings from the JSON file."""
    try:
        with open("user_mappings.json", "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print("User mappings file not found. Ensure user_mappings.json exists.")
        return {}


def load_technicians_mappings():
    """Load technicians mappings from the JSON file."""
    try:
        with open(TECHNICIANS_MAPPING_FILE, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        print(f"{TECHNICIANS_MAPPING_FILE} not found. Creating a new one.")
        return {}


def save_technicians_mappings(technicians_mappings):
    """Save technicians mappings to the JSON file."""
    with open(TECHNICIANS_MAPPING_FILE, "w") as file:
        json.dump(technicians_mappings, file, indent=4)


def clean_destination_table():
    """
    Clean up the destination technicians table before migration.
    Returns the number of records deleted.
    """
    try:
        if dest_db.is_closed():
            dest_db.connect()

        with dest_db.atomic():
            # Get the count before deletion
            count_before = Technician.select().count()

            Technician.delete().execute()

            count_after = Technician.select().count()
            deleted_count = count_before - count_after

            print(f"Cleanup Summary:")
            print(f"Records before cleanup: {count_before}")
            print(f"Records after cleanup: {count_after}")
            print(f"Total records deleted: {deleted_count}")

            return deleted_count

    except Exception as e:
        print(f"Error during cleanup: {str(e)}")
        raise
    finally:
        if not dest_db.is_closed():
            dest_db.close()


def get_default_user():
    """Get the default user for the 'created_by' field."""
    try:
        return DestinationUser.get(DestinationUser.email == DEFAULT_USER_EMAIL)
    except DoesNotExist:
        raise Exception(f"Default user with email {DEFAULT_USER_EMAIL} not found.")


def get_new_user_id_from_mapping(old_user_id, user_mappings):
    """
    Get the new user_id from the user_mappings.json file based on the old user_id.
    """
    for new_user_id, mapping in user_mappings.items():
        if mapping.get("old_user_id") == old_user_id:
            return int(new_user_id)
    return None


def generate_excel_report(migrated_data, unmigrated_data):
    """Generate an Excel file with migrated and unmigrated technician data."""
    workbook = Workbook()
    migrated_sheet = workbook.active
    migrated_sheet.title = "Migrated Technicians"
    unmigrated_sheet = workbook.create_sheet(title="Unmigrated Technicians")

    # Headers for Migrated Technicians
    migrated_sheet.append(["Technician ID", "Name", "Email", "Phone", "User ID", "Created At", "Updated At"])
    for record in migrated_data:
        migrated_sheet.append(
            [
                record["id"],
                record["name"],
                record["email"],
                record["phone"],
                record["user_id"],
                record["created_at"],
                record["updated_at"],
            ]
        )

    # Headers for Unmigrated Technicians
    unmigrated_sheet.append(["Technician ID", "Name", "Email", "Phone", "Reason"])
    for record in unmigrated_data:
        unmigrated_sheet.append(
            [
                record["id"],
                record["name"],
                record["email"],
                record["phone"],
                record["reason"],
            ]
        )

    workbook.save(EXCEL_FILE_NAME)
    print(f"\nMigration report saved as {EXCEL_FILE_NAME}")


def migrate_single_technician_data(record, new_user_id, default_user):
    """
    Migrate a single technician record to the destination database.
    Returns the new technician ID.
    """
    try:
        technician = Technician.create(
            id=record.id,
            name=record.technician_name,
            email=record.technician_email,
            phone=record.technician_phone,
            user_id=new_user_id,
            created_by=default_user.id,
            created_at=record.add_date,
            updated_at=record.add_date,
        )
        print(
            f"Migrated Technician: {record.technician_name} (New ID: {technician.id})"
        )
        return technician.id
    except IntegrityError as e:
        if "Duplicate entry" in str(e):
            print(
                f"Duplicate entry for {record.technician_email}. Attempting to update..."
            )
            Technician.update(
                {
                    "name": record.technician_name,
                    "phone": record.technician_phone,
                    "user_id": new_user_id,
                    "created_by": default_user.id,
                }
            ).where(Technician.email == record.technician_email).execute()
            print(f"Updated existing technician: {record.technician_name}")
            return (
                Technician.get(Technician.email == record.technician_email).id
            )  # Return the ID of the updated record
        else:
            raise


def migrate_technicians(automated=False):
    """
    Migrate all technicians from the source database to the destination database.
    Handles automated and one-by-one migration modes.
    """
    ignored_rows = []
    migrated_data = []
    unmigrated_data = []
    total_records = TechnicianMaster.select().count()
    migrated_count = 0
    skipped_count = 0

    # Establish connections if not already connected
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    # Get default "created_by" user
    default_user = get_default_user()
    print(f"Default 'created_by' user: {default_user.email} (ID: {default_user.id})")

    # Load user mappings and technicians mappings
    user_mappings = load_user_mappings()
    technicians_mappings = load_technicians_mappings()

    for record in TechnicianMaster.select():
        try:
            # Check if this technician is already migrated
            if any(
                mapping.get("old_technician_id") == record.id
                for mapping in technicians_mappings.values()
            ):
                print(
                    f"Technician {record.technician_name} (ID: {record.id}) already migrated. Skipping..."
                )
                skipped_count += 1
                continue

            # Get new user_id from the mapping table
            new_user_id = get_new_user_id_from_mapping(record.user_id, user_mappings)
            if not new_user_id:
                print(
                    f"Skipping Technician {record.technician_name} (ID: {record.id}) - No mapped user found."
                )
                unmigrated_data.append(
                    {
                        "id": record.id,
                        "name": record.technician_name,
                        "email": record.technician_email,
                        "phone": record.technician_phone,
                        "reason": "No mapped user found",
                    }
                )
                skipped_count += 1
                continue

            # Automated mode: Perform migration directly
            if automated:
                new_technician_id = migrate_single_technician_data(
                    record, new_user_id, default_user
                )
                technicians_mappings[new_technician_id] = {
                    "old_technician_id": record.id,
                    "user_id": new_user_id,
                }
                save_technicians_mappings(technicians_mappings)

                migrated_data.append(
                    {
                        "id": new_technician_id,
                        "name": record.technician_name,
                        "email": record.technician_email,
                        "phone": record.technician_phone,
                        "user_id": new_user_id,
                        "created_at": record.add_date,
                        "updated_at": record.add_date,
                    }
                )
                migrated_count += 1
            else:
                # One-by-one mode: Ask for confirmation
                proceed = questionary.confirm(
                    f"Do you want to migrate Technician: {record.technician_name}?"
                ).ask()
                if proceed:
                    new_technician_id = migrate_single_technician_data(
                        record, new_user_id, default_user
                    )
                    technicians_mappings[new_technician_id] = {
                        "old_technician_id": record.id,
                        "user_id": new_user_id,
                    }
                    save_technicians_mappings(technicians_mappings)

                    migrated_data.append(
                        {
                            "id": new_technician_id,
                            "name": record.technician_name,
                            "email": record.technician_email,
                            "phone": record.technician_phone,
                            "user_id": new_user_id,
                            "created_at": record.add_date,
                            "updated_at": record.add_date,
                        }
                    )
                    migrated_count += 1
                else:
                    skipped_count += 1
        except KeyboardInterrupt:
            # Stop entire migration on keyboard interrupt
            print("\nMigration interrupted. Exiting gracefully...")
            raise
        except Exception as e:
            print(
                f"Error migrating Technician {record.technician_name} ({record.technician_email}): {e}"
            )
            unmigrated_data.append(
                {
                    "id": record.id,
                    "name": record.technician_name,
                    "email": record.technician_email,
                    "phone": record.technician_phone,
                    "reason": str(e),
                }
            )
            skipped_count += 1

    # Generate Excel report in automated mode
    if automated:
        generate_excel_report(migrated_data, unmigrated_data)

    # Summary of migration results
    print(f"\nMigration Summary:")
    print(f"Total records: {total_records}")
    print(f"Successfully migrated: {migrated_count}")
    print(f"Skipped/Failed: {skipped_count}")


def run_migration():
    """
    Main function to run the technician migration.
    Allows users to select:
      1. Fully automated migration.
      2. Technician-by-technician migration.
      3. Single technician migration by ID.
    """
    try:
        # Ask for migration mode using questionary
        mode = questionary.select(
            "How would you like to perform the technician migration?",
            choices=[
                "Run Fully Automated",
                "Migrate Technicians One by One",
                "Migrate a Single Technician by ID",
            ],
        ).ask()

        if mode == "Run Fully Automated":
            print("Running Fully Automated Migration...")
            clean_destination_table()
            migrate_technicians(automated=True)

        elif mode == "Migrate Technicians One by One":
            print("Running Migration One by One...")
            clean_destination_table()
            migrate_technicians(automated=False)

        elif mode == "Migrate a Single Technician by ID":
            technician_id = questionary.text(
                "Enter the Technician ID to migrate:"
            ).ask()
            if not technician_id.isdigit():
                print("Invalid Technician ID. Please enter a numeric value.")
                return

            # Fetch technician from the source database and migrate it
            record = TechnicianMaster.get_by_id(int(technician_id))
            user_mappings = load_user_mappings()
            default_user = get_default_user()
            new_user_id = get_new_user_id_from_mapping(record.user_id, user_mappings)

            if not new_user_id:
                print(
                    f"No mapping found for Technician: {record.technician_name} (ID: {record.id})."
                )
                return

            migrate_single_technician_data(record, new_user_id, default_user)
            print(f"Successfully migrated Technician: {record.technician_name}")

        else:
            print("Invalid choice. Exiting...")
            return

    except KeyboardInterrupt:
        print("\nMigration interrupted by user. Exiting gracefully...")
    except Exception as e:
        print(f"Error during migration process: {e}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()