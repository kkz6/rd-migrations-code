from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    BooleanField,
    DoesNotExist,
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError


# Source Model
class Fleet(Model):
    fleet_id = CharField(primary_key=True, max_length=20)
    fleet_veh_no = TextField()
    fleet_veh_model = TextField()
    brand = TextField()
    fleet_chassis = TextField()

    class Meta:
        database = source_db
        table_name = "fleet"


# Destination Model
class Vehicle(Model):
    id = BigIntegerField(primary_key=True)
    brand = CharField(max_length=255)
    model = CharField(max_length=255)
    vehicle_no = CharField(max_length=255, null=True)
    vehicle_chassis_no = CharField(max_length=255)
    new_registration = BooleanField(default=False)
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)
    certificate_id = BigIntegerField(null=True)

    class Meta:
        database = dest_db
        table_name = "vehicles"


def clean_destination_table():
    """
    Clean up the destination table before migration.
    Returns the number of records deleted.
    """
    try:
        if dest_db.is_closed():
            dest_db.connect()

        with dest_db.atomic():
            count_before = Vehicle.select().count()
            Vehicle.delete().execute()
            count_after = Vehicle.select().count()
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


def migrate_vehicles():
    ignored_rows = []
    total_records = Fleet.select().count()
    migrated_count = 0
    skipped_count = 0

    # Establish connections if not already connected
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        with dest_db.atomic():  # Begin transaction
            for record in Fleet.select():
                try:
                    # Attempt to insert into the destination table
                    Vehicle.insert(
                        {
                            "brand": record.brand,
                            "model": record.fleet_veh_model,
                            "vehicle_no": record.fleet_veh_no,
                            "vehicle_chassis_no": record.fleet_chassis,
                            "new_registration": False,  # Default value as per schema
                        }
                    ).execute()

                    print(
                        f"Migrated Vehicle: {record.brand} {record.fleet_veh_model}, Chassis: {record.fleet_chassis}"
                    )
                    migrated_count += 1

                except IntegrityError as e:
                    # Handle duplicate entries
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Duplicate entry for chassis {record.fleet_chassis}. Attempting to update..."
                            )
                            Vehicle.update(
                                {
                                    "brand": record.brand,
                                    "model": record.fleet_veh_model,
                                    "vehicle_no": record.fleet_veh_no,
                                }
                            ).where(
                                Vehicle.vehicle_chassis_no == record.fleet_chassis
                            ).execute()
                            print(
                                f"Updated existing vehicle: {record.brand} {record.fleet_veh_model}"
                            )
                            migrated_count += 1

                        except Exception as update_error:
                            print(
                                f"Error updating vehicle with chassis {record.fleet_chassis}: {update_error}"
                            )
                            ignored_rows.append((record, str(update_error)))
                            skipped_count += 1
                    else:
                        print(
                            f"IntegrityError for vehicle with chassis {record.fleet_chassis}: {e}"
                        )
                        ignored_rows.append((record, str(e)))
                        skipped_count += 1

                except Exception as e:
                    print(
                        f"Error migrating vehicle with chassis {record.fleet_chassis}: {e}"
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
        for vehicle, reason in ignored_rows:
            print(
                f"- {vehicle.brand} {vehicle.fleet_veh_model} (Chassis: {vehicle.fleet_chassis}): {reason}"
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
        migrate_vehicles()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
