from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    ForeignKeyField,
    DoesNotExist,
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError
from models.users_model import DestinationUser, User

# Source Model
class TechnicianMaster(Model):
    id = IntegerField(primary_key=True)
    technician_name = TextField()
    technician_phone = TextField()
    technician_email = TextField()
    add_date = TimestampField()
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

    class Meta:
        database = dest_db
        table_name = "technicians"


def clean_destination_table():
    """
    Clean up the destination table before migration.
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


def check_user_exists(user_id):
    """Check if a user exists in the destination database"""
    try:
        sourceUser = User.get(User.id == user_id)
        return DestinationUser.get(DestinationUser.email == sourceUser.email)
    except DoesNotExist:
        return None


def get_default_user():
    """Get or create a default user for cases where the original user doesn't exist"""

    return DestinationUser.get(
        DestinationUser.email == "linoj@resloute-dynamics.com"
    )  # Replace with a known admin email


def migrate_technicians():
    ignored_rows = []
    total_records = TechnicianMaster.select().count()
    migrated_count = 0
    skipped_count = 0

    # Establish connections if not already connected
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    # Get default user for fallback
    default_user = get_default_user()
    print(default_user)

    try:
        with dest_db.atomic():  # Begin transaction
            for record in TechnicianMaster.select():
                try:
                    # Check if the user_id exists in destination database
                    user = check_user_exists(record.user_id)

                    if not user:
                        if default_user:
                            print(
                                f"User ID {record.user_id} not found, using default user for {record.technician_name}"
                            )
                            created_by_id = default_user.id
                        else:
                            print(
                                f"Skipping {record.technician_name} - no valid user_id and no default user"
                            )
                            ignored_rows.append(
                                (record, "No valid user_id and no default user")
                            )
                            skipped_count += 1
                            continue

                    # Attempt to insert into the destination table
                    Technician.insert(
                        {
                            "name": record.technician_name,
                            "email": record.technician_email,
                            "phone": record.technician_phone,
                            "user_id": user.id,
                            "created_by": user.id,
                        }
                    ).execute()

                    print(
                        f"Migrated Technician: {record.technician_name}, {record.technician_email}"
                    )
                    migrated_count += 1

                except IntegrityError as e:
                    # Handle duplicate entries
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Duplicate entry for {record.technician_email}. Attempting to update..."
                            )
                            Technician.update(
                                {
                                    "name": record.technician_name,
                                    "phone": record.technician_phone,
                                    "user_id": user.id,
                                    "created_by": user.id,
                                }
                            ).where(
                                Technician.email == record.technician_email
                            ).execute()
                            print(
                                f"Updated existing technician: {record.technician_name}, {record.technician_email}"
                            )
                            migrated_count += 1

                        except Exception as update_error:
                            print(
                                f"Error updating {record.technician_name} ({record.technician_email}): {update_error}"
                            )
                            ignored_rows.append((record, str(update_error)))
                            skipped_count += 1
                    else:
                        print(
                            f"IntegrityError for {record.technician_name} ({record.technician_email}): {e}"
                        )
                        ignored_rows.append((record, str(e)))
                        skipped_count += 1

                except Exception as e:
                    print(
                        f"Error migrating {record.technician_name} ({record.technician_email}): {e}"
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


def run_migration():
    """Main function to run the cleanup and migration"""
    try:
        # Ask for confirmation before cleanup
        response = input(
            "This will delete all existing records in the destination table. Are you sure? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return

        # Step 1: Clean the destination table
        print("\nStep 1: Cleaning destination table...")
        clean_destination_table()

        # Step 2: Perform the migration
        print("\nStep 2: Starting migration...")
        migrate_technicians()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
