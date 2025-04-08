import json
import questionary
from datetime import datetime
from openpyxl import Workbook
from peewee import *
from peewee import fn  # For case-insensitive queries
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser
from tqdm import tqdm  # For the progress bar

# Constants
TECHNICIANS_MAPPING_FILE = "technicians_mapping.json"

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

class TechnicianUser(Model):
    technician_id = BigIntegerField()
    user_id = BigIntegerField()

    class Meta:
        primary_key = False
        database = dest_db
        table_name = "technician_user"


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

        count_before = Technician.select().count()
        Technician.delete().execute()
        count_after = Technician.select().count()
        deleted_count = count_before - count_after

        print("Cleanup Summary:")
        print(f"  Records before cleanup: {count_before}")
        print(f"  Records after cleanup: {count_after}")
        print(f"  Total records deleted: {deleted_count}")

        return deleted_count

    except Exception as e:
        print(f"Error during cleanup: {str(e)}")
        raise
    finally:
        if not dest_db.is_closed():
            dest_db.close()

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
    migrated_sheet.append(
        ["Technician ID", "Name", "Email", "Phone", "User ID", "Created At", "Updated At"]
    )
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
    try:
        workbook.save("technician_migration_report.xlsx")
        print(f"\nMigration report saved as technician_migration_report.xlsx")
    except KeyboardInterrupt:
        workbook.save("technician_migration_report.xlsx")
        print(f"\nExport interrupted but partial report saved as technician_migration_report.xlsx")

def migrate_single_technician_data(record, new_user_id):
    """
    Migrate a single technician record to the destination database.
    Checks if a technician already exists in the destination (matching name, phone, and email regardless of case).
    If exists, returns the existing technician's id; if not, creates a new record.
    """
    try:
        # Clean and prepare the fields
        technician_name = record.technician_name.strip()
        technician_email = record.technician_email.strip().lower()
        technician_phone = record.technician_phone.strip()

        # Check if a matching technician already exists (case-insensitive match for name and email)
        existing = Technician.select().where(
            fn.Lower(Technician.name) == technician_name.lower(),
            Technician.email == technician_email,
            Technician.phone == technician_phone
        ).first()
        if existing:
            print(f"Duplicate found. Using existing technician: {technician_name} (ID: {existing.id})")
            return existing.id

        # Get destination user details
        dest_user = DestinationUser.get_by_id(new_user_id)
        if dest_user.parent_id is not None:
            user_id_field = dest_user.parent_id
            created_by_field = dest_user.id
        else:
            user_id_field = dest_user.id
            created_by_field = dest_user.id

        # Create a new technician record
        technician = Technician.create(
            id=record.id,
            name=technician_name,
            email=technician_email,
            phone=technician_phone,
            user_id=user_id_field,
            created_by=created_by_field,
            created_at=record.add_date,
            updated_at=record.add_date,
        )
        print(f"Created technician: {technician_name}")

        # Create TechnicianUser relationship
        try:
            TechnicianUser.get_or_create(
                technician_id=technician.id,
                user_id=new_user_id
            )
        except Exception as e:
            print(f"Warning: Failed to create TechnicianUser relationship: {str(e)}")

        return technician.id

    except IntegrityError as e:
        print(f"IntegrityError for {technician_name}: {e}")
        raise

def migrate_technicians(automated=False):
    """
    Migrate all technicians from the source database to the destination database one by one.
    Checks for duplicates in the destination database using a case-insensitive match on name and email,
    and an exact match on phone. If a duplicate is found, the existing record is used.
    In one-by-one mode, user confirmation is requested; in automated mode, it processes silently.
    """
    migrated_data = []
    unmigrated_data = []
    total_records = TechnicianMaster.select().count()
    migrated_count = 0
    skipped_count = 0

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    # Load mappings
    user_mappings = load_user_mappings()
    technicians_mappings = load_technicians_mappings()

    try:
        if automated:
            print("Starting Fully Automated Migration (One-by-One without batch insert)")
            progress_bar = tqdm(total=total_records, desc="Migrating Technicians", ncols=100, colour="green")
            for record in TechnicianMaster.select():
                progress_bar.update(1)

                # Skip if the record was already migrated (based on mapping)
                if any(mapping.get("old_technician_id") == record.id for mapping in technicians_mappings.values()):
                    print(f"Technician {record.technician_name} (ID: {record.id}) already migrated. Skipping...")
                    skipped_count += 1
                    continue

                new_user_id = get_new_user_id_from_mapping(record.user_id, user_mappings)
                if not new_user_id:
                    print(f"Skipping Technician {record.technician_name} (ID: {record.id}) - No mapped user found.")
                    unmigrated_data.append({
                        "id": record.id,
                        "name": record.technician_name.strip(),
                        "email": record.technician_email.strip().lower(),
                        "phone": record.technician_phone.strip(),
                        "reason": "No mapped user found",
                    })
                    skipped_count += 1
                    continue

                try:
                    new_id = migrate_single_technician_data(record, new_user_id)
                    # Update mapping only if not already present
                    if str(new_id) not in technicians_mappings:
                        technicians_mappings[str(new_id)] = {"old_technician_id": record.id, "user_id": new_user_id}
                        save_technicians_mappings(technicians_mappings)
                    migrated_data.append({
                        "id": new_id,
                        "name": record.technician_name.strip(),
                        "email": record.technician_email.strip().lower(),
                        "phone": record.technician_phone.strip(),
                        "user_id": new_user_id,
                        "created_at": record.add_date,
                        "updated_at": record.add_date,
                    })
                    migrated_count += 1
                except Exception as ex:
                    print(f"Error inserting technician {record.technician_name}: {ex}")
                    unmigrated_data.append({
                        "id": record.id,
                        "name": record.technician_name.strip(),
                        "email": record.technician_email.strip().lower(),
                        "phone": record.technician_phone.strip(),
                        "reason": str(ex)
                    })
                    skipped_count += 1
            progress_bar.close()
        else:
            print("Starting One-by-One Migration (Interactive)")
            processed_count = 0
            for record in TechnicianMaster.select():
                processed_count += 1
                print(f"\nProcessing Technician {processed_count} of {total_records} (Name: {record.technician_name})")
                # Skip if already migrated
                if any(mapping.get("old_technician_id") == record.id for mapping in technicians_mappings.values()):
                    print(f"Technician {record.technician_name} (ID: {record.id}) already migrated. Skipping...")
                    skipped_count += 1
                    print(f"Progress: {processed_count}/{total_records} | Migrated: {migrated_count} | Skipped/Failed: {skipped_count}")
                    continue

                new_user_id = get_new_user_id_from_mapping(record.user_id, user_mappings)
                if not new_user_id:
                    print(f"Skipping Technician {record.technician_name} (ID: {record.id}) - No mapped user found.")
                    unmigrated_data.append({
                        "id": record.id,
                        "name": record.technician_name.strip(),
                        "email": record.technician_email.strip().lower(),
                        "phone": record.technician_phone.strip(),
                        "reason": "No mapped user found",
                    })
                    skipped_count += 1
                    print(f"Progress: {processed_count}/{total_records} | Migrated: {migrated_count} | Skipped/Failed: {skipped_count}")
                    continue

                proceed = questionary.confirm(
                    f"Do you want to migrate Technician: {record.technician_name}?"
                ).ask()
                if proceed:
                    try:
                        new_id = migrate_single_technician_data(record, new_user_id)
                        if str(new_id) not in technicians_mappings:
                            technicians_mappings[str(new_id)] = {"old_technician_id": record.id, "user_id": new_user_id}
                            save_technicians_mappings(technicians_mappings)
                        migrated_data.append({
                            "id": new_id,
                            "name": record.technician_name.strip(),
                            "email": record.technician_email.strip().lower(),
                            "phone": record.technician_phone.strip(),
                            "user_id": new_user_id,
                            "created_at": record.add_date,
                            "updated_at": record.add_date,
                        })
                        migrated_count += 1
                        print("Technician migrated successfully!")
                    except Exception as e:
                        print(f"Error migrating Technician {record.technician_name}: {e}")
                        unmigrated_data.append({
                            "id": record.id,
                            "name": record.technician_name.strip(),
                            "email": record.technician_email.strip().lower(),
                            "phone": record.technician_phone.strip(),
                            "reason": str(e),
                        })
                        skipped_count += 1
                else:
                    skipped_count += 1
                print(f"Progress: {processed_count}/{total_records} | Migrated: {migrated_count} | Skipped/Failed: {skipped_count}")

    except KeyboardInterrupt:
        print("\nMigration interrupted by user. Proceeding to export report...")
    finally:
        generate_excel_report(migrated_data, unmigrated_data)
        print("\nMigration Summary:")
        print(f"  Total records: {total_records}")
        print(f"  Successfully migrated: {migrated_count}")
        print(f"  Skipped/Failed: {skipped_count}")

def run_migration():
    """
    Main function to run the technician migration.
    Offers three options:
      1. Fully automated migration (processes records one-by-one without confirmation).
      2. Interactive migration (prompts for each technician).
      3. Single technician migration by ID.
    """
    try:
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
            technician_id = questionary.text("Enter the Technician ID to migrate:").ask()
            if not technician_id.isdigit():
                print("Invalid Technician ID. Please enter a numeric value.")
                return

            record = TechnicianMaster.get_by_id(int(technician_id))
            user_mappings = load_user_mappings()
            new_user_id = get_new_user_id_from_mapping(record.user_id, user_mappings)

            if not new_user_id:
                print(
                    f"No mapping found for Technician: {record.technician_name} (ID: {record.id})."
                )
                return

            try:
                new_id = migrate_single_technician_data(record, new_user_id)
                technicians_mappings = load_technicians_mappings()
                technicians_mappings[str(new_id)] = {"old_technician_id": record.id, "user_id": new_user_id}
                save_technicians_mappings(technicians_mappings)
                print(f"Successfully migrated Technician: {record.technician_name}")
            except Exception as e:
                print(f"Error migrating Technician {record.technician_name}: {e}")

        else:
            print("Invalid choice. Exiting...")
            return

    except KeyboardInterrupt:
        print("\nMigration interrupted by user. Exporting report and exiting gracefully...")
    except Exception as e:
        print(f"Error during migration process: {e}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

if __name__ == "__main__":
    run_migration()
