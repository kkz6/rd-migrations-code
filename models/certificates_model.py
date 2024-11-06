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


class DealerMaster(Model):
    id = AutoField()
    company = CharField(max_length=300)
    email = CharField(max_length=200, unique=True)
    phone = TextField()
    mobile = TextField()
    emirate = CharField(max_length=20, default="NA")
    status = CharField(max_length=20, default="AFC")
    salesuser = CharField(max_length=20)
    add_date = TimestampField(default=datetime.now)
    added_by = IntegerField()

    class Meta:
        database = source_db
        table_name = "dealer_master"


# Destination role models for Spatie
class Role(Model):
    id = AutoField()
    name = CharField()
    guard_name = CharField()
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)

    class Meta:
        database = dest_db
        table_name = "roles"


class ModelHasRole(Model):
    role_id = IntegerField()
    model_type = CharField()
    model_id = IntegerField()

    class Meta:
        database = dest_db
        table_name = "model_has_roles"
        indexes = ((("model_id", "model_type", "role_id"), True),)


class MigrationState:
    def __init__(self):
        self.dealer_id_mapping = {}

    def get_mapping(self):
        return self.dealer_id_mapping

    def set_mapping(self, mapping):
        self.dealer_id_mapping = mapping


def migrate_dealers(migration_state):
    """
    Migrate dealers from source to destination database and assign roles.
    Updates the migration_state with dealer ID mappings.
    """
    current_time = datetime.now()

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        # Get or create dealer role
        dealer_role, created = Role.get_or_create(
            name="dealer",
            guard_name="web",
            defaults={"created_at": current_time, "updated_at": current_time},
        )

        print(f"Using dealer role ID: {dealer_role.id}")

        # Migrate each dealer
        for dealer in DealerMaster.select():
            try:
                # Generate a secure password for the dealer
                temp_password = bcrypt.hashpw(
                    f"temp_{dealer.id}_{current_time.timestamp()}".encode(),
                    bcrypt.gensalt(),
                ).decode()

                # Create user record for dealer
                new_user = DestinationUser.create(
                    name=dealer.company,
                    email=dealer.email,
                    password=temp_password,
                    created_at=current_time,
                    updated_at=current_time,
                )

                # Assign dealer role
                ModelHasRole.create(
                    role_id=dealer_role.id,
                    model_type="App\\Models\\User",
                    model_id=new_user.id,
                )

                migration_state.dealer_id_mapping[dealer.id] = new_user.id
                print(
                    f"Migrated dealer {dealer.company} (ID: {dealer.id}) to user ID: {new_user.id}"
                )

            except IntegrityError as e:
                # Handle case where user might already exist
                if "Duplicate entry" in str(e):
                    existing_user = DestinationUser.get(
                        DestinationUser.email == dealer.email
                    )
                    migration_state.dealer_id_mapping[dealer.id] = existing_user.id
                    print(
                        f"Dealer {dealer.company} already exists as user ID: {existing_user.id}"
                    )
                else:
                    print(f"Error migrating dealer {dealer.company}: {str(e)}")
            except Exception as e:
                print(f"Unexpected error migrating dealer {dealer.company}: {str(e)}")

    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()


# To test the dealer mapping, you can add this debug function:
def print_dealer_mapping(migration_state):
    """
    Print the current dealer ID mapping for debugging purposes.
    """
    mapping = migration_state.get_mapping()
    print("\nDealer ID Mapping:")
    print("Source ID -> Destination ID")
    print("-" * 30)
    for source_id, dest_id in mapping.items():
        print(f"{source_id} -> {dest_id}")


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


def migrate_certificates(migration_state):
    """
    Migrate certificates from source to destination database and update vehicle references.
    """

    dealer_id_mapping = migration_state.get_mapping()
    if not dealer_id_mapping:
        raise ValueError("Dealer ID mapping is empty. Run dealer migration first.")

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
                    dest_dealer_id = dealer_id_mapping.get(record.dealer_id)
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
                                "created_at": current_time,
                                "updated_at": current_time,
                                "model": record.vehicle_type,
                            }
                        ).execute()
                        print(f"Created new vehicle record for ECU {record.ecu}")
                    except IntegrityError:
                        print(f"Vehicle already exists for ECU {record.ecu}")

                    # Check if related entities exist
                    installer_user = DestinationUser.get_or_none(
                        DestinationUser.id == record.installer_user_id
                    )
                    caliberater_user = DestinationUser.get_or_none(
                        DestinationUser.id == record.caliberater_user_id
                    )
                    installed_for = Customer.get_or_none(
                        Customer.id == record.customer_id
                    )
                    device = Device.get_or_none(Device.ecu_number == record.ecu)
                    vehicle = Vehicle.get_or_none(
                        Vehicle.vehicle_chassis_no == record.vehicle_chassis
                    )

                    # Convert speed value by removing any non-numeric characters
                    speed_value = "".join(filter(str.isdigit, record.speed))
                    speed_limit = int(speed_value) if speed_value else 0

                    if not all([installer_user, caliberater_user, device, vehicle]):
                        missing_entities = []
                        if not installer_user:
                            missing_entities.append("installer_user")
                        if not caliberater_user:
                            missing_entities.append("caliberater_user")
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
                            user_id=dest_dealer_id,
                            created_at=current_time,
                            updated_at=current_time,
                            installed_by_id=1,  # Default technician ID
                            installed_for_id=(
                                installed_for.id if installed_for else None
                            ),
                            vehicle_id=vehicle.id,
                        )

                        # Update the vehicle with the certificate ID
                        Vehicle.update(
                            certificate_id=certificate.id, updated_at=current_time
                        ).where(Vehicle.id == vehicle.id).execute()

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
                                        "dealer_id": 1,
                                        "user_id": record.updated_by_user_id,
                                        "updated_at": current_time,
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
                                    updated_at=current_time,
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
    print(f"\nMigration Summary:")
    print(f"Total records processed: {total_records}")
    print(f"Successfully migrated: {migrated_count}")
    print(f"Skipped/Failed: {skipped_count}")
    print(f"Success rate: {(migrated_count/total_records)*100:.2f}%")

    if ignored_rows:
        print("\nDetailed error log:")
        for record, reason in ignored_rows:
            print(f"- ECU {record.ecu}: {reason}")


def run_migration():
    """Main function to run the complete migration process"""
    migration_state = MigrationState()

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

        # Step 2: Migrate dealers and get ID mapping
        print("\nStep 2: Migrating dealers...")
        migrate_dealers(migration_state)
        print_dealer_mapping(migration_state)

        # Step 3: Perform the certificate migration with dealer mapping
        print("\nStep 3: Migrating certificates...")
        migrate_certificates(migration_state)

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
