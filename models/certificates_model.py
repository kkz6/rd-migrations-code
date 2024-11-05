from peewee import *
from datetime import datetime
from models.users_model import User
from models.technicians_model import Technician
from models.customers_model import Customer
from models.devices_model import Device
from models.vehicles_model import Vehicle
from source_db import source_db
from dest_db import dest_db


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
    cancelled_by_id = ForeignKeyField(User, null=True, on_delete="SET NULL")
    installed_by_id = ForeignKeyField(Technician, null=False)
    installed_for_id = ForeignKeyField(Customer, null=True, on_delete="SET NULL")
    vehicle_id = ForeignKeyField(Vehicle, null=True, on_delete="SET NULL")
    km_reading = IntegerField(null=False)
    speed_limit = IntegerField(null=False)
    print_count = IntegerField(default=0, null=False)
    renewal_count = IntegerField(default=0, null=False)
    description = TextField(null=True)
    country = CharField(max_length=255, null=True)
    dealer_id = ForeignKeyField(User, null=False)
    user_id = ForeignKeyField(User, null=False)
    enable_renewal = BooleanField(default=False)
    cancellation_requested_by_id = ForeignKeyField(
        User, null=True, on_delete="SET NULL"
    )

    class Meta:
        database = dest_db
        table_name = "certificates"


def migrate_certificates():
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

                    Vehicle.insert(
                        {
                            "brand": record.vehicle_type,
                            "vehicle_no": record.vehicle_registration,
                            "vehicle_chassis_no": record.vehicle_chassis,
                            "new_registration": False,  # Default value as per schema
                        }
                    ).execute()

                    print(f"Migrating ECU {record.ecu}")
                    # Check if related entities exist in the destination database
                    installer_user = User.get_or_none(
                        User.id == record.installer_user_id
                    )

                    caliberater_user = User.get_or_none(
                        User.id == record.caliberater_user_id
                    )
                    # installed_by = Technician.get_or_none(
                    #     Technician.id == record.installer_technician_id
                    # )
                    # print("installed_by", installed_by)

                    installed_for = Customer.get_or_none(
                        Customer.id == record.customer_id
                    )

                    # dealer = User.get_or_none(User.id == record.dealer_id)
                    device = Device.get_or_none(Device.ecu_number == record.ecu)
                    vehicle = Vehicle.get_or_none(
                        Vehicle.vehicle_chassis_no == record.vehicle_chassis
                    )

                    if (
                        not installer_user
                        or not caliberater_user
                        # or not installed_by
                        # or not dealer
                        or not device
                        or not vehicle
                    ):
                        print(f"Skipping ECU {record.ecu} - related entity not found")
                        ignored_rows.append((record, "Related entity not found"))
                        skipped_count += 1
                        continue

                    # Attempt to insert into the destination table
                    Certificate.create(
                        serial_number=record.serialno,
                        status="active",
                        device_id=device.id,
                        installation_date=record.date_actual_installation,
                        calibration_date=record.date_calibrate,
                        expiry_date=record.date_expiry,
                        km_reading=0,
                        speed_limit=int(record.speed),
                        print_count=record.print_count,
                        renewal_count=record.renewal_count,
                        description=record.description,
                        dealer_id=1,
                        user_id=1,
                        created_at=current_time,
                        updated_at=current_time,
                        installed_by_id=1,
                        installed_for_id=installed_for.id,
                        vehicle_id=vehicle.id,
                    )

                    print(f"Migrated Certificate: ECU {record.ecu}")
                    migrated_count += 1

                except IntegrityError as e:
                    # Handle duplicate entries
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Duplicate entry for ECU {record.ecu}. Attempting to update..."
                            )
                            Certificate.update(
                                {
                                    "device_id": device.id,
                                    "installation_date": record.date_actual_installation,
                                    "calibration_date": record.date_calibrate,
                                    "expiry_date": record.date_expiry,
                                    "km_reading": 0,
                                    "speed_limit": int(record.speed),
                                    "print_count": record.print_count,
                                    "renewal_count": record.renewal_count,
                                    "description": record.description,
                                    "dealer_id": 1,
                                    "user_id": record.updated_by_user_id,
                                    "updated_at": current_time,
                                    "installed_by_id": 1,
                                    "installed_for_id": installed_for.id,
                                    "vehicle_id": vehicle.id,
                                }
                            ).where(
                                Certificate.serial_number == record.serialno
                            ).execute()
                            print(f"Updated existing certificate: ECU {record.ecu}")
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
    """Main function to run the migration"""
    try:
        print("\nStarting migration...")
        migrate_certificates()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
