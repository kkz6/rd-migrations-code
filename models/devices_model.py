from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    ForeignKeyField,
    BooleanField,
    DoesNotExist,
    AutoField,  # Added for explicit primary key
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError
from models.users_model import DestinationUser
from datetime import datetime


# Source Model
class EcuMaster(Model):
    ecu = CharField(max_length=50, unique=True)
    lock = IntegerField(default=0)
    dealer_id = IntegerField()
    add_date_timestamp = TimestampField(default=datetime.now)
    ecu_added_by = IntegerField()
    remarks = CharField(max_length=500, null=True)

    class Meta:
        database = source_db
        table_name = "ecu_master"
        primary_key = False  # Disable automatic id field


# Destination Model
class Device(Model):
    id = AutoField()
    ecu_number = CharField(max_length=255, unique=True)
    device_type_id = BigIntegerField()
    device_model_id = BigIntegerField()
    device_variant_id = BigIntegerField()
    remarks = TextField(null=True)
    lock = BooleanField(default=False)
    dealer_id = BigIntegerField(null=True)
    user_id = BigIntegerField()
    blocked = BooleanField(default=False)
    blocked_description = TextField(null=True)  # Added missing field
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)
    deleted_at = TimestampField(null=True)

    class Meta:
        database = dest_db
        table_name = "devices"
        primary_key = False  # Disable automatic id field


def clean_destination_table():
    """
    Clean up the destination table before migration.
    Returns the number of records deleted.
    """
    try:
        if dest_db.is_closed():
            dest_db.connect()

        with dest_db.atomic():
            count_before = Device.select().count()
            # Changed to use raw SQL to avoid id field reference
            Device.raw("DELETE FROM devices").execute()
            count_after = Device.select().count()
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
        return DestinationUser.get(DestinationUser.id == user_id)
    except DoesNotExist:
        return None


def migrate_devices():
    ignored_rows = []
    total_records = EcuMaster.select().count()
    migrated_count = 0
    skipped_count = 0
    current_time = datetime.now()

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        with dest_db.atomic():
            for record in EcuMaster.select():
                print('tesseract')
                try:
                    print(f"Migrating ECU {record.ecu}")
                    # Check if user exists
                    user = check_user_exists(record.ecu_added_by)
                    if not user:
                        print(
                            f"Skipping ECU {record.ecu} - user_id {record.ecu_added_by} not found"
                        )
                        ignored_rows.append((record, "User ID not found"))
                        skipped_count += 1
                        continue

                    # Convert lock value from int to boolean
                    lock_value = bool(record.lock)

                    # Attempt to insert into the destination table
                    Device.insert(
                        {
                            "ecu_number": record.ecu,
                            "device_type_id": 1,
                            "device_model_id": 1,
                            "device_variant_id": 1,
                            "remarks": record.remarks,
                            "lock": lock_value,
                            "dealer_id": record.dealer_id,
                            "user_id": record.ecu_added_by,
                            "blocked": False,
                            "blocked_description": None,
                            "created_at": record.add_date_timestamp,
                            "updated_at": current_time,
                        }
                    ).execute()

                    print(f"Migrated Device: ECU {record.ecu}")
                    migrated_count += 1

                except IntegrityError as e:
                    # Handle duplicate entries
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Duplicate entry for ECU {record.ecu}. Attempting to update..."
                            )
                            Device.update(
                                {
                                    "device_type_id": 1,
                                    "device_model_id": 1,
                                    "device_variant_id": 1,
                                    "remarks": record.remarks,
                                    "lock": lock_value,
                                    "dealer_id": record.dealer_id,
                                    "user_id": record.ecu_added_by,
                                    "updated_at": current_time,
                                }
                            ).where(Device.ecu_number == record.ecu).execute()
                            print(f"Updated existing device: ECU {record.ecu}")
                            migrated_count += 1

                        except Exception as update_error:
                            print(f"Error updating ECU {record.ecu}: {update_error}")
                            ignored_rows.append((record, str(update_error)))
                            skipped_count += 1
                    else:
                        print(f"IntegrityError for ECU {record.ecu}: {e}")
                        ignored_rows.append((record, str(e)))
                        skipped_count += 1

                except Exception as e:
                    print(f"Error migrating ECU {record.ecu}: {e}")
                    ignored_rows.append((record, str(e)))
                    skipped_count += 1

    finally:
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
        for device, reason in ignored_rows:
            print(f"- ECU {device.ecu}: {reason}")


def run_migration():
    """Main function to run the cleanup and migration"""
    try:
        response = input(
            "This will delete all existing records in the destination table. Are you sure? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return

        print("\nStep 1: Cleaning destination table...")
        clean_destination_table()

        print("\nStep 2: Starting migration...")
        migrate_devices()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
