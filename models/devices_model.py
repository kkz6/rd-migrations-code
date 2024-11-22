from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    BooleanField,
    DoesNotExist,
    AutoField,
    BlobField,
    ForeignKeyField,
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError
from models.users_model import DestinationUser, User, DealerMaster
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


class DeviceType(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    enabled = IntegerField(default=1)  # TINYINT(1) maps to IntegerField (0 or 1)
    user_id = BigIntegerField()
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)
    deleted_at = TimestampField(null=True)

    class Meta:
        database = dest_db
        table_name = "device_types"


class DeviceModel(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    enabled = IntegerField(default=1)  # tinyint(1) mapped to IntegerField
    user_id = BigIntegerField()
    device_type_id = ForeignKeyField(
        DeviceType, backref="device_models", on_delete="CASCADE"
    )  # Foreign key constraint
    approval_code = CharField(max_length=255, default="0000")
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)
    deleted_at = TimestampField(null=True)

    class Meta:
        database = dest_db
        table_name = "device_models"
        indexes = ((("device_type_id",), False),)


class DeviceVariant(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    description = CharField(max_length=255)
    enabled = IntegerField(default=1)  # TINYINT(1) for enabled/disabled status
    device_model_id = ForeignKeyField(
        DeviceModel, backref="variants", on_delete="CASCADE"
    )  # Foreign key to DeviceModel
    user_id = BigIntegerField()
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)
    deleted_at = TimestampField(null=True)

    class Meta:
        database = dest_db  # Set the database connection
        table_name = "device_variants"  # Explicitly set the table name
        indexes = ((("name",), True),)  # Unique constraint for the 'name' field


# Destination Model


class Device(Model):
    id = BigIntegerField(primary_key=True)
    ecu_number = CharField(max_length=255, unique=True)
    device_type_id = ForeignKeyField(DeviceType, backref="devices", on_delete="CASCADE")
    device_model_id = ForeignKeyField(
        DeviceModel, backref="devices", on_delete="CASCADE"
    )
    device_variant_id = ForeignKeyField(
        DeviceVariant, backref="devices", on_delete="CASCADE"
    )
    remarks = TextField(null=True)
    lock = IntegerField(default=0)
    dealer_id = BigIntegerField(null=True)
    user_id = BigIntegerField()
    blocked = IntegerField(default=0)
    blocked_description = BlobField(null=True)
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)
    deleted_at = TimestampField(null=True)

    class Meta:
        database = dest_db  # Specifies which database to use
        table_name = "devices"  # Table name for this model
        indexes = ((("ecu_number",), True),)  # Unique constraint for ecu_number


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


def check_dealer_exists(dealer_id):
    """Check if a dealer exists in the destination database"""
    dealer = DealerMaster.get(DealerMaster.id == dealer_id)
    try:
        return DestinationUser.get(DestinationUser.email == dealer.email)
    except DoesNotExist:
        return None


def migrate_devices():
    ignored_rows = []
    total_records = EcuMaster.select().count()
    migrated_count = 0
    skipped_count = 0

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        with dest_db.atomic():
            user = DestinationUser.get(
                DestinationUser.email == "linoj@resloute-dynamics.com"
            )
            for record in EcuMaster.select():
                try:
                    print(f"Migrating ECU {record.ecu}")
                    get_device_data_by_ecu(record, user)

                    print(f"Migrated Device: ECU {record.ecu}")
                    migrated_count += 1

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


current_time = datetime.now()
ecm_mapping = {
    "S100": {
        "device_type": "Electronic Type Speed Limiter",
        "device_model": "Autograde Safedrive",
        "device_variant": "",
        "approval_code": "24-01-22785/Q24-01-048935/NB0002",
    },
    "DBW": {
        "device_type": "Electronic Type Speed Limiter",
        "device_model": "Fleetmax DBW",
        "device_variant": "",
        "approval_code": "24-01-22784/Q24-01-048943/NB0002",
    },
    "DBV": {
        "device_type": "Fuel Type Speed Limiter",
        "device_model": "Fleetmax DBV",
        "device_variant": "",
        "approval_code": "24-01-22784/Q24-01-048943/NB0002",
    },
    "ESL": {
        "device_type": "Electronic Type Speed Limiter Limiter",
        "device_model": "Resolute Dynamics ESL",
        "device_variant": "",
        "approval_code": "24-01-22783/Q24-01-048944/NB0002",
    },
    "FSL": {
        "device_type": "Fuel Type Speed Limiter Limiter",
        "device_model": "Resolute Dynamics FSL",
        "device_variant": "",
        "approval_code": "24-01-22783/Q24-01-048944/NB0002",
    },
    "ETM": {
        "device_type": "Engine Temperature Monitor",
        "device_model": "Resolute Dynamics ThermoPro",
        "device_variant": "",
        "approval_code": "24-01-22783/Q24-01-048944/NB0002",
    },
    "BAS": {
        "device_type": "Brake Alert System",
        "device_model": "Resolute Dynamics TailSafe",
        "device_variant": "",
        "approval_code": "24-01-22783/Q24-01-048944/NB0002",
    },
    "SAS": {
        "device_type": "Speed Alert System",
        "device_model": "Resolute Dynamics SAS",
        "device_variant": "",
        "approval_code": "24-01-22783/Q24-01-048944/NB0002",
    },
}


# Function to create or get the related data (DeviceType, DeviceModel, and DeviceVariant)
def get_or_create_device_type(name, user):
    device_type, created = DeviceType.get_or_create(
        name=name,
        defaults={
            "user_id": user.id,
            "created_at": current_time,
            "updated_at": current_time,
        },
    )
    return device_type


def get_or_create_device_model(name, device_type, approval_code, user):
    device_model, created = DeviceModel.get_or_create(
        name=name,
        device_type_id=device_type.id,
        approval_code=approval_code,
        defaults={
            "user_id": user.id,
            "created_at": current_time,
            "updated_at": current_time,
        },
    )
    return device_model


def get_or_create_device_variant(name, device_model, user):
    if not name or not name.strip():
        # Use model name as fallback, converted to snake_case
        variant_name = device_model.name.lower().replace(" ", "_")
    else:
        variant_name = name
    device_variant, created = DeviceVariant.get_or_create(
        name=variant_name,
        device_model_id=device_model.id,
        defaults={
            "user_id": user.id,
            "created_at": current_time,
            "updated_at": current_time,
        },
    )
    return device_variant


# Function to match ECU number and fetch or create data
def get_device_data_by_ecu(ecu_record, user):
    for prefix, mapping in ecm_mapping.items():
        if ecu_record.ecu.startswith(prefix):
            print(mapping["device_variant"])
            device_type = get_or_create_device_type(mapping["device_type"], user)
            device_model = get_or_create_device_model(
                mapping["device_model"], device_type, mapping["approval_code"], user
            )
            device_variant = get_or_create_device_variant(
                mapping["device_variant"]
                or mapping["device_model"].lower().replace(" ", "_"),
                device_model,
                user,
            )

            device, created = Device.get_or_create(
                ecu_number=ecu_record.ecu,
                remarks=ecu_record.remarks,
                lock=ecu_record.lock,
                defaults={
                    "device_type_id": device_type.id,
                    "device_model_id": device_model.id,
                    "device_variant_id": device_variant.id if device_variant else None,
                    "user_id": user.id,
                },
            )

            if created:
                print(
                    f"Device created: {device.ecu_number}, Model: {device.device_model_id.name}, Type: {device.device_type_id.name}"
                )
            else:
                print(f"Device with ECU number {ecu_record.ecu} already exists.")

            return device

    return None  # Return None if no matching ECU prefix found


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
