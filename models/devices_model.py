import json
import os
from datetime import datetime
import questionary
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    DoesNotExist,
    BlobField,
    ForeignKeyField,
    IntegrityError,
    DateTimeField,
)
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser, DealerMaster  # Assuming these are defined

# ----------------- SOURCE MODELS ----------------- #
class EcuMaster(Model):
    ecu = CharField(max_length=50, unique=True)
    lock = IntegerField(default=0)
    dealer_id = IntegerField()
    add_date_timestamp = DateTimeField(default=datetime.now)
    ecu_added_by = IntegerField()
    remarks = CharField(max_length=500, null=True)

    class Meta:
        database = source_db
        table_name = "ecu_master"
        primary_key = False  # We use ecu as unique identifier


# ----------------- DESTINATION MODELS ----------------- #
class DeviceType(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    enabled = IntegerField(default=1)
    user_id = BigIntegerField()
    created_at = DateTimeField(null=True)
    updated_at = DateTimeField(null=True)

    class Meta:
        database = dest_db
        table_name = "device_types"


class DeviceModel(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    enabled = IntegerField(default=1)
    user_id = BigIntegerField()
    device_type_id = ForeignKeyField(DeviceType, backref="device_models", on_delete="CASCADE")
    approval_code = CharField(max_length=255, default="0000")
    created_at = DateTimeField(null=True)
    updated_at = DateTimeField(null=True)

    class Meta:
        database = dest_db
        table_name = "device_models"
        indexes = ((("device_type_id",), False),)


class DeviceVariant(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    description = CharField(max_length=255)
    enabled = IntegerField(default=1)
    device_model_id = ForeignKeyField(DeviceModel, backref="variants", on_delete="CASCADE")
    user_id = BigIntegerField()
    created_at = DateTimeField(null=True)
    updated_at = DateTimeField(null=True)

    class Meta:
        database = dest_db
        table_name = "device_variants"
        indexes = ((("name",), True),)


class Device(Model):
    id = BigIntegerField(primary_key=True)
    ecu_number = CharField(max_length=255, unique=True)
    device_type_id = ForeignKeyField(DeviceType, backref="devices", on_delete="CASCADE")
    device_model_id = ForeignKeyField(DeviceModel, backref="devices", on_delete="CASCADE")
    device_variant_id = ForeignKeyField(DeviceVariant, backref="devices", on_delete="CASCADE")
    remarks = TextField(null=True)
    lock = IntegerField(default=0)
    dealer_id = BigIntegerField(null=True)  # Stores the new (destination) dealer id
    user_id = BigIntegerField()             # The DestinationUser performing the migration
    blocked = IntegerField(default=0)
    blocked_description = BlobField(null=True)
    created_at = DateTimeField(null=True)
    updated_at = DateTimeField(null=True)

    class Meta:
        database = dest_db
        table_name = "devices"
        indexes = ((("ecu_number",), True),)


# ----------------- GLOBALS AND MAPPINGS ----------------- #
current_time = datetime.now()

# ECU mapping (prefix-based). Adjust these as necessary.
ecm_mapping = {
    "D1005E": {
        "device_type": "Fuel Type Speed Limiter",
        "device_model": "AutoGrade Dass86",
        "device_variant": "",
        "approval_code": "24-01-22784/Q24-01-048943/NB0002",
    },
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


# ----------------- HELPER FUNCTIONS ----------------- #
def load_json_mapping(file_path: str) -> dict:
    if os.path.exists(file_path):
        try:
            with open(file_path, "r") as file:
                return json.load(file)
        except Exception as e:
            print(f"Error loading {file_path}: {e}")
            return {}
    return {}


def save_json_mapping(file_path: str, mapping: dict) -> None:
    with open(file_path, "w") as file:
        json.dump(mapping, file, indent=4)


def load_user_mappings() -> dict:
    return load_json_mapping("user_mappings.json")


def save_user_mappings(mapping: dict) -> None:
    save_json_mapping("user_mappings.json", mapping)


def load_device_mappings() -> dict:
    return load_json_mapping("device_mappings.json")


def save_device_mappings(mapping: dict) -> None:
    save_json_mapping("device_mappings.json", mapping)


def get_or_create_device_type(name: str, user: DestinationUser):
    device_type, created = DeviceType.get_or_create(
        name=name,
        defaults={
            "user_id": user.id,
            "created_at": current_time,
            "updated_at": current_time,
        },
    )
    return device_type


def get_or_create_device_model(name: str, device_type: DeviceType, approval_code: str, user: DestinationUser):
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


def get_or_create_device_variant(name: str, device_model: DeviceModel, user: DestinationUser):
    # Use the provided variant name or fallback to the device model name in snake_case.
    variant_name = name if name and name.strip() else device_model.name.lower().replace(" ", "_")
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


def get_device_data_by_ecu(ecu_record: EcuMaster, user: DestinationUser, new_dealer_id: int):
    """
    For a given ECU record, match the ECU prefix to the mapping, create or get
    the device type, model, and variant, and then create the Device record.
    Uses the add_date_timestamp from the ECU record for both created_at and updated_at.
    """
    for prefix, mapping in ecm_mapping.items():
        if ecu_record.ecu.startswith(prefix):
            device_type = get_or_create_device_type(mapping["device_type"], user)
            device_model = get_or_create_device_model(mapping["device_model"], device_type, mapping["approval_code"], user)
            device_variant = get_or_create_device_variant(mapping["device_variant"], device_model, user)

            device, created = Device.get_or_create(
                ecu_number=ecu_record.ecu,
                defaults={
                    "remarks": ecu_record.remarks,
                    "lock": ecu_record.lock,
                    "device_type_id": device_type.id,
                    "device_model_id": device_model.id,
                    "device_variant_id": device_variant.id if device_variant else None,
                    "user_id": user.id,
                    "dealer_id": new_dealer_id,
                    "created_at": ecu_record.add_date_timestamp,
                    "updated_at": ecu_record.add_date_timestamp,
                },
            )
            if created:
                print(f"Device created: ECU {device.ecu_number} (Model: {device_model.name}, Type: {device_type.name})")
            else:
                print(f"Device with ECU {ecu_record.ecu} already exists.")
            return device

    print(f"No matching mapping found for ECU {ecu_record.ecu}")
    return None


def list_unmigrated_devices(dealer_source_id: int, device_mappings: dict) -> list:
    """
    Returns a list of EcuMaster records for the given source dealer id that have not yet been migrated,
    by checking if the ECU (unique value) exists in device_mappings.
    """
    # Build a list of ECU numbers that have already been mapped.
    migrated_ecus = [v[0] for v in device_mappings.values()]  # v[0] holds the ecu_number.
    query = EcuMaster.select().where(EcuMaster.dealer_id == dealer_source_id)
    return [record for record in query if record.ecu not in migrated_ecus]


# ----------------- MIGRATION LOGIC ----------------- #
def migrate_devices_for_user(user: DestinationUser, source_dealer_id: int, device_mappings: dict) -> dict:
    """
    For the given DestinationUser (dealer), check for unmigrated devices based on the
    source dealer id. Ask for confirmation (with a Skip option) and migrate each device.
    Update the device mappings (new_device_id => [ecu_number, dealer_id]) for later use.
    Returns the updated device mappings.
    """
    unmigrated_devices = list_unmigrated_devices(source_dealer_id, device_mappings)
    device_count = len(unmigrated_devices)

    if device_count == 0:
        print(f"No unmigrated devices found for dealer with source ID {source_dealer_id}.")
        return device_mappings

    print(f"\nUser {user.email} has {device_count} unmigrated device(s) from source dealer ID {source_dealer_id}.")
    
    # Single confirmation prompt with "Migrate" and "Skip" options.
    user_choice = questionary.select(
        "Choose an action:",
        choices=["Migrate", "Skip"],
    ).ask()

    if user_choice != "Migrate":
        print("Skipping migration for this user.")
        return device_mappings

    for ecu_record in unmigrated_devices:
        try:
            print(f"\nMigrating ECU {ecu_record.ecu}...")
            device = get_device_data_by_ecu(ecu_record, user, new_dealer_id=user.id)
            if device:
                # Save mapping as: new_device_id => [ecu_number, dealer_id]
                device_mappings[str(device.id)] = [ecu_record.ecu, ecu_record.dealer_id]
                save_device_mappings(device_mappings)
        except IntegrityError as e:
            print(f"Integrity error while migrating ECU {ecu_record.ecu}: {e}")
        except Exception as e:
            print(f"Error while migrating ECU {ecu_record.ecu}: {e}")

    return device_mappings


def run_migration():
    """
    Main function that:
      1. Loads user and device mappings.
      2. Iterates through each DestinationUser (dealer) that has a mapping in user_mappings.json.
      3. For each such user, shows the count of unmigrated devices and asks once if they want to migrate or skip.
      4. Performs the migration if chosen.
      5. Handles keyboard interrupts gracefully.
    """
    # Load mappings
    user_mappings = load_user_mappings()  # Keys: destination user IDs, values include "dealer_id"
    device_mappings = load_device_mappings()  # Keys: new device ID, values: [ecu_number, dealer_id]

    try:
        if source_db.is_closed():
            source_db.connect()
        if dest_db.is_closed():
            dest_db.connect()

        for user in DestinationUser.select().order_by(DestinationUser.id):
            mapping = user_mappings.get(str(user.id))
            if not mapping or "dealer_id" not in mapping:
                continue  # Skip users that are not mapped to a source dealer

            source_dealer_id = mapping["dealer_id"]
            prompt = questionary.select(
                f"\nMigrate devices for user {user.email} (mapped source dealer ID: {source_dealer_id})?",
                choices=["Migrate", "Skip"],
            ).ask()
            if prompt != "Migrate":
                continue

            unmigrated = list_unmigrated_devices(source_dealer_id, device_mappings)
            count = len(unmigrated)
            print(f"\nUser {user.email} has {count} unmigrated device(s) from source dealer ID {source_dealer_id}.")

            # Single confirmation prompt with a Skip option.
            choice = questionary.select(
                "Do you want to migrate these devices or skip?",
                choices=["Migrate", "Skip"],
            ).ask()
            if choice != "Migrate":
                continue

            device_mappings = migrate_devices_for_user(user, source_dealer_id, device_mappings)
            print(f"Completed device migration for user {user.email}.\n")

    except KeyboardInterrupt:
        print("\nMigration interrupted by user. Exiting gracefully...")
    except Exception as e:
        print(f"Error during migration process: {e}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()


if __name__ == "__main__":
    run_migration()
