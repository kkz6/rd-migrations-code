import tempfile
from typing import Optional
from peewee import *
from datetime import datetime
from models.users_model import DestinationUser
from models.technicians_model import Technician
from models.customers_model import Customer
from models.devices_model import Device
from models.vehicles_model import Vehicle
from source_db import source_db
from dest_db import dest_db
import bcrypt
from id_mapping import dealer_id_mapper, customer_id_mapper


# Source Model
class CertificateRecord(Model):
    id = AutoField()
    serialno = IntegerField(null=True, unique=True)
    ecu = CharField(max_length=50, null=False)
    customer_id = IntegerField(null=False)
    installer_user_id = IntegerField(null=False)
    caliberater_user_id = IntegerField(null=False)
    installer_technician_id = IntegerField(null=False)
    caliberater_technician_id = IntegerField(null=False)
    fleet_id = CharField(max_length=20, default="0", null=False)
    vehicle_type = TextField(null=False)
    vehicle_registration = TextField(null=False)
    vehicle_chassis = TextField(null=False)
    speed = TextField(null=False)
    kilometer = IntegerField(null=True)
    date_actual_installation = TimestampField(null=True)
    date_installation = TimestampField(null=True)
    date_calibrate = TimestampField(null=True)
    date_expiry = TimestampField(null=True)
    renewal_count = IntegerField(default=0, null=False)
    dealer_id = IntegerField(null=False)
    print_count = IntegerField(default=0, null=False)
    activstate = IntegerField(default=1, null=False)
    description = CharField(max_length=500, null=True)
    date_cancelation = TimestampField(null=True)
    updated_by_user_id = IntegerField(null=False)

    class Meta:
        database = source_db
        table_name = "certificate_record"


# Destination Model
class Certificate(Model):
    id = AutoField()
    serial_number = BigIntegerField(null=True, unique=True)
    status = CharField(
        max_length=255,
        choices=("cancelled", "renewed", "blocked", "active", "nullified"),
        default="active",
    )
    device_id = ForeignKeyField(Device, null=True, on_delete="SET NULL")
    installation_date = DateTimeField(null=False)
    calibration_date = DateTimeField(null=False)
    expiry_date = DateTimeField(null=False)
    notified = BooleanField(default=False)
    requested_for_cancellation = BooleanField(default=False)
    cancellation_date = DateTimeField(null=True)
    cancelled = BooleanField(default=False)
    previous_certificate_id = ForeignKeyField("self", null=True, on_delete="SET NULL")
    cancelled_by_id = ForeignKeyField(DestinationUser, null=True, on_delete="SET NULL")
    installed_by_id = ForeignKeyField(Technician, null=False)
    installed_for_id = ForeignKeyField(Customer, null=True, on_delete="SET NULL")
    vehicle_id = ForeignKeyField(Vehicle, null=True, on_delete="SET NULL")
    km_reading = IntegerField(null=False)
    speed_limit = IntegerField(null=False)
    print_count = IntegerField(default=0, null=False)
    renewal_count = IntegerField(default=0, null=False)
    description = TextField(null=True)
    country = CharField(max_length=255, null=True)
    dealer_id = ForeignKeyField(DestinationUser, null=False)
    user_id = ForeignKeyField(DestinationUser, null=False)
    enable_renewal = BooleanField(default=False)
    cancellation_requested_by_id = ForeignKeyField(
        DestinationUser, null=True, on_delete="SET NULL"
    )

    class Meta:
        database = dest_db
        table_name = "certificates"


def clean_destination_table():
    """
    Clean up the destination table before migration.
    Returns the number of records deleted.
    """
    try:
        if dest_db.is_closed():
            dest_db.connect()

        with dest_db.atomic():
            count_before = Certificate.select().count()
            Certificate.delete().execute()
            count_after = Certificate.select().count()
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


def get_dealer_mapping(source_dealer_id: str) -> Optional[str]:
    """
    Get destination dealer ID for a given source dealer ID.
    Returns None if mapping doesn't exist.
    """
    return dealer_id_mapper.get_dest_id(str(source_dealer_id))


def get_user_mapping(source_user_id: str) -> Optional[str]:
    """
    Get destination dealer ID for a given source dealer ID.
    Returns None if mapping doesn't exist.
    """
    return dealer_id_mapper.get_dest_id(str(source_user_id))


def migrate_certificates(log_file):
    """
    Migrate certificates from source to destination database and update vehicle references.
    """

    ignored_rows = []
    total_records = CertificateRecord.select().count()
    migrated_count = 0
    skipped_count = 0
    current_time = datetime.now()

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        with dest_db.atomic():
            for record in CertificateRecord.select():
                try:
                    print(f"Processing ECU {record.ecu}")
                    # Get mapped dealer ID
                    dest_dealer_id = get_dealer_mapping(str(record.dealer_id))
                    dest_user_id = get_user_mapping(str(record.installer_user_id))
                    if not dest_dealer_id:
                        error_msg = f"No matching destination dealer found for source dealer ID: {record.dealer_id}"
                        print(f"Skipping ECU {record.ecu} - {error_msg}")
                        ignored_rows.append((record, error_msg))
                        skipped_count += 1
                        continue

                    # Create vehicle record if it doesn't exist
                    try:
                        Vehicle.insert(
                            {
                                "brand": record.vehicle_type,
                                "vehicle_no": record.vehicle_registration,
                                "vehicle_chassis_no": record.vehicle_chassis,
                                "new_registration": False,
                                "model": record.vehicle_type,  # added vehicle_type here since no model in source
                            }
                        ).execute()
                        print(f"Created new vehicle record for ECU {record.ecu}")
                    except IntegrityError:
                        print(f"Vehicle already exists for ECU {record.ecu}")

                    # mapped_calibrater_user = get_user_mapping(
                    #     str(record.caliberater_user_id)
                    # )
                    # caliberater_user = DestinationUser.get_or_none(
                    #     DestinationUser.id == mapped_calibrater_user.id
                    # )
                    # if not caliberater_user:
                    #     caliberater_user = DestinationUser.get_by_id(1)

                    mapped_installed_for_id = customer_id_mapper.get_dest_id(
                        str(record.customer_id)
                    )

                    installed_for = Customer.get_or_none(
                        Customer.id == mapped_installed_for_id
                    )
                    device = Device.get_or_none(Device.ecu_number == record.ecu)
                    vehicle = Vehicle.get_or_none(
                        Vehicle.vehicle_chassis_no == record.vehicle_chassis
                    )

                    # Convert speed value by removing any non-numeric characters
                    speed_value = "".join(filter(str.isdigit, record.speed))
                    speed_limit = int(speed_value) if speed_value else 0

                    if not all([device, vehicle]):
                        missing_entities = []
                        if not device:
                            missing_entities.append("device")
                        if not vehicle:
                            missing_entities.append("vehicle")

                        error_msg = (
                            f"Missing related entities: {', '.join(missing_entities)}"
                        )
                        print(f"Skipping ECU {record.ecu} - {error_msg}")
                        ignored_rows.append((record, error_msg))
                        skipped_count += 1
                        continue

                    # Create or update certificate
                    try:
                        certificate = Certificate.create(
                            serial_number=record.serialno,
                            status="active",
                            device_id=device.id,
                            installation_date=record.date_actual_installation,
                            calibration_date=record.date_calibrate,
                            expiry_date=record.date_expiry,
                            km_reading=record.kilometer or 0,
                            speed_limit=speed_limit,
                            print_count=record.print_count,
                            renewal_count=record.renewal_count,
                            description=record.description,
                            dealer_id=dest_dealer_id,
                            user_id=dest_user_id or dest_dealer_id,
                            installed_by_id=1,  # Default technician ID
                            installed_for_id=(
                                installed_for.id if installed_for else None
                            ),
                            vehicle_id=vehicle.id,
                        )

                        # Update the vehicle with the certificate ID
                        Vehicle.update(certificate_id=certificate.id).where(
                            Vehicle.id == vehicle.id
                        ).execute()

                        print(
                            f"Successfully migrated Certificate: ECU {record.ecu} and updated vehicle reference"
                        )
                        migrated_count += 1

                    except IntegrityError as e:
                        if "Duplicate entry" in str(e):
                            try:
                                # Get existing certificate
                                existing_certificate = Certificate.get(
                                    Certificate.serial_number == record.serialno
                                )

                                # Update existing certificate
                                Certificate.update(
                                    {
                                        "device_id": device.id,
                                        "installation_date": record.date_actual_installation,
                                        "calibration_date": record.date_calibrate,
                                        "expiry_date": record.date_expiry,
                                        "km_reading": record.kilometer or 0,
                                        "speed_limit": speed_limit,
                                        "print_count": record.print_count,
                                        "renewal_count": record.renewal_count,
                                        "description": record.description,
                                        "dealer_id": dest_dealer_id,
                                        "user_id": dest_user_id or dest_dealer_id,
                                        "installed_by_id": 1,
                                        "installed_for_id": (
                                            installed_for.id if installed_for else None
                                        ),
                                        "vehicle_id": vehicle.id,
                                    }
                                ).where(
                                    Certificate.id == existing_certificate.id
                                ).execute()

                                # Update the vehicle with the certificate ID
                                Vehicle.update(
                                    certificate_id=existing_certificate.id,
                                ).where(Vehicle.id == vehicle.id).execute()

                                print(
                                    f"Updated existing certificate: ECU {record.ecu} and vehicle reference"
                                )
                                migrated_count += 1

                            except Exception as update_error:
                                error_msg = f"Error updating: {str(update_error)}"
                                print(f"Error updating ECU {record.ecu}: {error_msg}")
                                ignored_rows.append((record, error_msg))
                                skipped_count += 1
                        else:
                            error_msg = f"IntegrityError: {str(e)}"
                            print(f"IntegrityError for ECU {record.ecu}: {error_msg}")
                            ignored_rows.append((record, error_msg))
                            skipped_count += 1

                except Exception as e:
                    error_msg = f"Unexpected error: {str(e)}"
                    print(f"Error migrating ECU {record.ecu}: {error_msg}")
                    ignored_rows.append((record, error_msg))
                    skipped_count += 1

    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Print migration summary
    print(f"\n Migration from certificates to certificates table ", file=log_file)
    print(f"\n Summary:", file=log_file)
    print(f"Total records processed: {total_records}", file=log_file)
    print(f"Successfully migrated: {migrated_count}", file=log_file)
    print(f"Skipped/Failed: {skipped_count}", file=log_file)
    print(f"Success rate: {(migrated_count/total_records)*100:.2f}%", file=log_file)

    if ignored_rows:
        print("\nDetailed error log:", file=log_file)
        for record, reason in ignored_rows:
            print(f"- ECU {record.ecu}: {reason}", file=log_file)


def run_migration():
    """Main function to run the complete migration process"""

    try:
        # Ask for confirmation before cleanup
        response = input(
            "This will delete all existing records in the destination table. Are you sure? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return

        with tempfile.NamedTemporaryFile(mode="w", delete=False) as log_file:
            print(
                "\nStarting Migration for Certificates table. Logs written to:",
                log_file.name,
            )

            print("\nStep 1: Cleaning destination table...")
            clean_destination_table()

            print("\nStep 2: Migrating certificates...")
            migrate_certificates(log_file)

            print("\nMigration complete. Logs written to:", log_file.name)

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
