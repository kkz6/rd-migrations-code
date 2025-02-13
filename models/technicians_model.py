import json
import questionary
from datetime import datetime
from openpyxl import Workbook
from peewee import *
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser
from tqdm import tqdm  # Added for progress bar

# Constants
TECHNICIANS_MAPPING_FILE = "technicians_mapping.json"
EXCEL_FILE_NAME = "technician_migration_report.xlsx"
BATCH_SIZE = 100  # Adjust batch size as needed

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
    country_id = BigIntegerField()
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
        workbook.save(EXCEL_FILE_NAME)
        print(f"\nMigration report saved as {EXCEL_FILE_NAME}")
    except KeyboardInterrupt:
        # Catching interrupt during export – data is still saved.
        workbook.save(EXCEL_FILE_NAME)
        print(f"\nExport interrupted but partial report saved as {EXCEL_FILE_NAME}")


def migrate_single_technician_data(record, new_user_id):
    """
    Migrate a single technician record to the destination database.
    Uses the fetched destination user record to determine the proper user_id and created_by fields.
    Returns the new technician ID.
    """
    try:
        dest_user = DestinationUser.get_by_id(new_user_id)
        if dest_user.parent_id is not None:
            user_id_field = dest_user.parent_id
            created_by_field = dest_user.id
        else:
            user_id_field = dest_user.id
            created_by_field = dest_user.id

        technician = Technician.create(
            id=record.id,
            name=record.technician_name,
            email=record.technician_email,
            phone=record.technician_phone,
            user_id=user_id_field,
            country_id=231,  # Set country_id as 231
            created_by=created_by_field,
            created_at=record.add_date,
            updated_at=record.add_date,
        )
        print(f"Migrated Technician: {record.technician_name} (New ID: {technician.id})")
        return technician.id
    except IntegrityError as e:
        if "Duplicate entry" in str(e):
            print(f"Duplicate entry for {record.technician_email}. Attempting to update...")
            dest_user = DestinationUser.get_by_id(new_user_id)
            if dest_user.parent_id is not None:
                user_id_field = dest_user.parent_id
                created_by_field = dest_user.id
            else:
                user_id_field = dest_user.id
                created_by_field = dest_user.id

            Technician.update(
                {
                    "name": record.technician_name,
                    "phone": record.technician_phone,
                    "user_id": user_id_field,
                    "country_id": 231,  # Also update country_id here
                    "created_by": created_by_field,
                }
            ).where(Technician.email == record.technician_email).execute()
            print(f"Updated existing technician: {record.technician_name}")
            return Technician.get(Technician.email == record.technician_email).id
        else:
            raise


def migrate_technicians(automated=False):
    """
    Migrate all technicians from the source database to the destination database.
    In fully automated mode, batch insertion is used with a progress bar.
    In one-by-one mode, the CLI displays progress information.
    In case of a KeyboardInterrupt, the migration stops gracefully and an Excel report is generated.
    """
    migrated_data = []
    unmigrated_data = []
    total_records = TechnicianMaster.select().count()
    migrated_count = 0
    skipped_count = 0

    # Connect to databases if not already connected
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    # Load mappings
    user_mappings = load_user_mappings()
    technicians_mappings = load_technicians_mappings()

    try:
        if automated:
            print("Starting Fully Automated Migration with Batch Insertion")
            progress_bar = tqdm(total=total_records, desc="Migrating Technicians", ncols=100, colour="green")
            batch_list = []
            for record in TechnicianMaster.select():
                # Check if already migrated
                if any(mapping.get("old_technician_id") == record.id for mapping in technicians_mappings.values()):
                    print(f"Technician {record.technician_name} (ID: {record.id}) already migrated. Skipping...")
                    skipped_count += 1
                    progress_bar.update(1)
                    continue

                new_user_id = get_new_user_id_from_mapping(record.user_id, user_mappings)
                if not new_user_id:
                    print(f"Skipping Technician {record.technician_name} (ID: {record.id}) - No mapped user found.")
                    unmigrated_data.append({
                        "id": record.id,
                        "name": record.technician_name,
                        "email": record.technician_email,
                        "phone": record.technician_phone,
                        "reason": "No mapped user found",
                    })
                    skipped_count += 1
                    progress_bar.update(1)
                    continue

                dest_user = DestinationUser.get_by_id(new_user_id)
                if dest_user.parent_id is not None:
                    user_id_field = dest_user.parent_id
                    created_by_field = dest_user.id
                else:
                    user_id_field = dest_user.id
                    created_by_field = dest_user.id

                data = {
                    "id": record.id,
                    "name": record.technician_name,
                    "email": record.technician_email,
                    "phone": record.technician_phone,
                    "user_id": user_id_field,
                    "country_id": 231,
                    "created_by": created_by_field,
                    "created_at": record.add_date,
                    "updated_at": record.add_date,
                }
                batch_list.append((record, data))

                if len(batch_list) >= BATCH_SIZE:
                    try:
                        data_to_insert = [d for (_, d) in batch_list]
                        Technician.insert_many(data_to_insert).execute()
                        for (r, d) in batch_list:
                            new_id = d["id"]
                            technicians_mappings[new_id] = {"old_technician_id": r.id, "user_id": d["user_id"]}
                            migrated_data.append({
                                "id": new_id,
                                "name": r.technician_name,
                                "email": r.technician_email,
                                "phone": r.technician_phone,
                                "user_id": d["user_id"],
                                "created_at": r.add_date,
                                "updated_at": r.add_date,
                            })
                            migrated_count += 1
                        save_technicians_mappings(technicians_mappings)
                        batch_list = []
                    except IntegrityError:
                        print("Batch insert failed, falling back to individual insertion for this batch.")
                        for (r, d) in batch_list:
                            try:
                                new_id = migrate_single_technician_data(r, new_user_id)
                                technicians_mappings[new_id] = {"old_technician_id": r.id, "user_id": d["user_id"]}
                                migrated_data.append({
                                    "id": new_id,
                                    "name": r.technician_name,
                                    "email": r.technician_email,
                                    "phone": r.technician_phone,
                                    "user_id": d["user_id"],
                                    "created_at": r.add_date,
                                    "updated_at": r.add_date,
                                })
                                migrated_count += 1
                                save_technicians_mappings(technicians_mappings)
                            except Exception as ex:
                                print(f"Error inserting technician {r.technician_name}: {ex}")
                                unmigrated_data.append({
                                    "id": r.id,
                                    "name": r.technician_name,
                                    "email": r.technician_email,
                                    "phone": r.technician_phone,
                                    "reason": str(ex)
                                })
                                skipped_count += 1
                        batch_list = []
                    finally:
                        progress_bar.update(1)
                else:
                    progress_bar.update(1)
            # Process any remaining records in the batch
            if batch_list:
                try:
                    data_to_insert = [d for (_, d) in batch_list]
                    Technician.insert_many(data_to_insert).execute()
                    for (r, d) in batch_list:
                        new_id = d["id"]
                        technicians_mappings[new_id] = {"old_technician_id": r.id, "user_id": d["user_id"]}
                        migrated_data.append({
                            "id": new_id,
                            "name": r.technician_name,
                            "email": r.technician_email,
                            "phone": r.technician_phone,
                            "user_id": d["user_id"],
                            "created_at": r.add_date,
                            "updated_at": r.add_date,
                        })
                        migrated_count += 1
                    save_technicians_mappings(technicians_mappings)
                except IntegrityError:
                    for (r, d) in batch_list:
                        try:
                            new_id = migrate_single_technician_data(r, new_user_id)
                            technicians_mappings[new_id] = {"old_technician_id": r.id, "user_id": d["user_id"]}
                            migrated_data.append({
                                "id": new_id,
                                "name": r.technician_name,
                                "email": r.technician_email,
                                "phone": r.technician_phone,
                                "user_id": d["user_id"],
                                "created_at": r.add_date,
                                "updated_at": r.add_date,
                            })
                            migrated_count += 1
                            save_technicians_mappings(technicians_mappings)
                        except Exception as ex:
                            print(f"Error inserting technician {r.technician_name}: {ex}")
                            unmigrated_data.append({
                                "id": r.id,
                                "name": r.technician_name,
                                "email": r.technician_email,
                                "phone": r.technician_phone,
                                "reason": str(ex)
                            })
                            skipped_count += 1
                    batch_list = []
            progress_bar.close()
        else:
            print("Starting One-by-One Migration")
            processed_count = 0
            for record in TechnicianMaster.select():
                processed_count += 1
                print(f"\nProcessing Technician {processed_count} of {total_records} (Name: {record.technician_name})")
                # Check if already migrated
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
                        "name": record.technician_name,
                        "email": record.technician_email,
                        "phone": record.technician_phone,
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
                        technicians_mappings[new_id] = {"old_technician_id": record.id, "user_id": new_user_id}
                        save_technicians_mappings(technicians_mappings)
                        migrated_data.append({
                            "id": new_id,
                            "name": record.technician_name,
                            "email": record.technician_email,
                            "phone": record.technician_phone,
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
                            "name": record.technician_name,
                            "email": record.technician_email,
                            "phone": record.technician_phone,
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

        # Summary of migration results
        print("\nMigration Summary:")
        print(f"  Total records: {total_records}")
        print(f"  Successfully migrated: {migrated_count}")
        print(f"  Skipped/Failed: {skipped_count}")


def run_migration():
    """
    Main function to run the technician migration.
    Provides options for:
      1. Fully automated (batch) migration.
      2. Technician-by-technician migration.
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

            migrate_single_technician_data(record, new_user_id)
            print(f"Successfully migrated Technician: {record.technician_name}")

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