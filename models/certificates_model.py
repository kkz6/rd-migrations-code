import tempfile
from typing import Optional
from peewee import *
from datetime import datetime
from models.users_model import DestinationUser, User
from models.technicians_model import Technician
from models.customers_model import Customer,CustomerMaster,CustomerDealer
from models.devices_model import Device
from models.vehicles_model import Vehicle
from source_db import source_db
from dest_db import dest_db
import bcrypt

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
        database = source_db
        table_name = "certificates"

class Technician(Model):
    id = AutoField()
    technician_name = TextField(null=False)
    technician_phone = TextField(null=False)
    technician_email = TextField(null=False)
    user_id = IntegerField()
    class Meta:
        database = dest_db
        table_name = "technician_master"

class DestTechnician(Model):
    name = TextField(null=False)
    email = TextField(null=False)
    phone = TextField(null=False)
    user_id = IntegerField()
    created_by = IntegerField()
    class Meta:
        database = source_db
        table_name = "technicians"


def get_dealer_mapping(source_dealer_id) -> Optional[str]:
    """
    Get destination dealer ID for a given source dealer ID.
    Returns None if mapping doesn't exist.
    """
    sourceUser = User.get(User.id == source_dealer_id);
    return DestinationUser.get(DestinationUser.email == sourceUser.email)


def get_user_mapping(source_user_id: str) -> Optional[str]:
    """
    Get destination dealer ID for a given source dealer ID.
    Returns None if mapping doesn't exist.
    """
    sourceUser = User.get(User.id == source_user_id);
    return DestinationUser.get(DestinationUser.email == sourceUser.email)

def create_and_assign_customer(customer_id,dealer_id):
    sourceCustomer = CustomerMaster.get(Customer.id == customer_id)
    sourceUser = User.get(User.id == sourceCustomer.user_id);
    user = DestinationUser.get(DestinationUser.email == sourceUser.email)

    dealer = User.get(User.id == dealer_id);
    destDealer = DestinationUser.get(DestinationUser.email == dealer.email)
    customer,created = Customer.get_or_create(
        Customeremail=sourceCustomer.email,
        defaults={
            "name":sourceCustomer.name,
            "address":sourceCustomer.o_address,
            "contact_number":sourceCustomer.o_contactphone,
            "user_id":user.id
        }
    )
    return CustomerDealer.get_or_create(customer_id = customer.id, dealer_id=destDealer.id)


def create_assign_technician(certificate):
    created_by = DestinationUser.get(DestinationUser.email=='linoj@resloute-dynamics.com')

    if certificate.installer_technician_id == 0:
        technician = User.get(User.id == certificate.installer_user_id)
        return DestTechnician.get_or_create(name = technician.full_name, phone=technician.mobile, email=technician.email, user_id = technician.id,created_by = created_by.id)
    else: 
        technician = Technician.get(Technician.id == certificate.caliberater_technician_id)
        sourceUser = User.get(User.id == technician.user_id)
        user = DestinationUser.get(DestinationUser.email == sourceUser.email)
        return DestTechnician.get_or_create(name = technician.technician_name, phone=technician.technician_phone, email=technician.technician_email, user_id = user.id,created_by = created_by.id)
        
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

    for record in CertificateRecord.select():
        print(f"Processing ECU {record.ecu}")
        dealer = get_dealer_mapping(str(record.dealer_id))
        user = get_user_mapping(str(record.installer_user_id))

        if not dealer:
            error_msg = f"No matching destination dealer found for source dealer ID: {record.dealer_id}"
            print(f"Skipping ECU {record.ecu} - {error_msg}")
            ignored_rows.append((record, error_msg))
            skipped_count += 1
            continue

        try:
            vehicle, created = Vehicle.get_or_create(
                brand= record.vehicle_type,
                vehicle_no=record.vehicle_registration,
                vehicle_chassis_no=record.vehicle_chassis,
                new_registration=False,
                model=record.vehicle_type,
            ).execute()
            print(f"Created new vehicle record for ECU {record.ecu}")
        except IntegrityError:
            print(f"Vehicle already exists for ECU {record.ecu}")

        customer = create_and_assign_customer(record.customer_id,record.dealer_id)

        technician = create_assign_technician(record)

        device = Device.get_or_none(Device.ecu_number == record.ecu)

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

        certificate, created = Certificate.create(
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
            dealer_id=dealer.id,
            user_id=user.id or dealer.id,
            installed_by_id=technician.id,
            installed_for_id=customer.id,
            vehicle_id=vehicle.id,
        )

        print(
            f"Successfully migrated Certificate: ECU {record.ecu} and updated vehicle reference"
        )
        migrated_count += 1

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

            print("\nMigrating certificates...")
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
