import json
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
from models.users_model import DestinationUser, User

# Constants
TECHNICIANS_MAPPING_FILE = "technicians_mapping.json"
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Replace with the known admin email

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
    """Get the new user_id from the user_mappings.json file."""
    return user_mappings.get(str(old_user_id), {}).get("new_user_id")


def migrate_technicians(automated=False):
    """
    Migrate all technicians from the source database to the destination database.
    """
    ignored_rows = []
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

    try:
        with dest_db.atomic():  # Begin transaction
            for record in TechnicianMaster.select():
                try:
                    # Check if this technician is already migrated
                    if str(record.id) in technicians_mappings:
                        print(
                            f"Technician {record.technician_name} (ID: {record.id}) already migrated. Skipping..."
                        )
                        skipped_count += 1
                        continue

                    # Get new user_id from the mapping table
                    new_user_id = get_new_user_id_from_mapping(
                        record.user_id, user_mappings
                    )
                    if not new_user_id:
                        print(
                            f"Skipping Technician {record.technician_name} (ID: {record.id}) - No mapped user found."
                        )
                        ignored_rows.append(
                            (record, "No mapped user found for old user_id.")
                        )
                        skipped_count += 1
                        continue

                    # Insert or update the technician record
                    new_technician_id = migrate_single_technician_data(
                        record, new_user_id, default_user
                    )

                    # Save the mapping
                    technicians_mappings[new_technician_id] = record.id
                    save_technicians_mappings(technicians_mappings)

                    if automated:
                        migrated_count += 1
                    else:
                        print(
                            f"Technician Name: {record.technician_name}, Email: {record.technician_email}"
                        )
                        proceed = input("Do you want to migrate this record? (yes/no): ")
                        if proceed.lower() == "yes":
                            migrated_count += 1
                        else:
                            skipped_count += 1

                except Exception as e:
                    print(
                        f"Error migrating Technician {record.technician_name} ({record.technician_email}): {e}"
                    )
                    ignored_rows.append((record, str(e)))
                    skipped_count += 1

    finally:
        # Close connections
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Summary of migration results
    print(f"\nMigration Summary:")
    print(f"Total records: {total_records}")
    print(f"Successfully migrated: {migrated_count}")
    print(f"Skipped/Failed: {skipped_count}")

    if ignored_rows:
        print("\nDetailed error log:")
        for technician, reason in ignored_rows:
            print(
                f"- {technician.technician_name} ({technician.technician_email}): {reason}"
            )


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


def run_migration():
    """Main function to run the migration."""
    try:
        # Ask for migration mode
        mode = input(
            "How would you like to perform the migration?\n"
            "1. Run Fully Automated\n"
            "2. Migrate Technicians One by One\n"
            "3. Migrate a Single Technician by ID\n"
            "Enter your choice (1/2/3): "
        )

        if mode == "1":
            print("Running Fully Automated Migration...")
            clean_destination_table()
            migrate_technicians(automated=True)

        elif mode == "2":
            print("Running Migration One by One...")
            clean_destination_table()
            migrate_technicians(automated=False)

        elif mode == "3":
            technician_id = input("Enter the Technician ID to migrate: ")
            if not technician_id.isdigit():
                print("Invalid Technician ID. Please enter a numeric value.")
                return
            clean_destination_table()
            migrate_single_technician_data(
                TechnicianMaster.get_by_id(int(technician_id)),
                get_default_user().id,
                load_user_mappings(),
            )

        else:
            print("Invalid choice. Exiting...")
            return

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
