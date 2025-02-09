import json
import os
from datetime import datetime
import questionary
from openpyxl import Workbook
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    IntegerField,
    ForeignKeyField,
    DateTimeField,
)
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser

# Constants
DEVICE_MAPPING_FILE = "device_mappings.json"
USER_MAPPING_FILE = "user_mappings.json"
EXCEL_FILE_NAME = "device_migration_report.xlsx"
BATCH_SIZE = 20000  # Number of records per batch

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
    # Add more mappings as necessary...
}

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
        primary_key = False


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


class Device(Model):
    id = BigIntegerField(primary_key=True)
    ecu_number = CharField(max_length=255, unique=True)
    device_type_id = ForeignKeyField(DeviceType, backref="devices", on_delete="CASCADE")
    device_model_id = ForeignKeyField(DeviceModel, backref="devices", on_delete="CASCADE")
    device_variant_id = ForeignKeyField(DeviceVariant, backref="devices", on_delete="CASCADE")
    remarks = CharField(null=True)
    lock = IntegerField(default=0)
    dealer_id = BigIntegerField(null=True)
    user_id = BigIntegerField()
    created_at = DateTimeField(null=True)
    updated_at = DateTimeField(null=True)

    class Meta:
        database = dest_db
        table_name = "devices"


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
    return load_json_mapping(USER_MAPPING_FILE)


def load_device_mappings() -> dict:
    return load_json_mapping(DEVICE_MAPPING_FILE)


def save_device_mappings(mapping: dict) -> None:
    save_json_mapping(DEVICE_MAPPING_FILE, mapping)


def list_unmigrated_devices(dealer_source_id: int, device_mappings: dict) -> list:
    """Get a list of unmigrated devices for a given dealer source ID."""
    migrated_ecus = [v["ecu_number"] for v in device_mappings.values()]
    query = EcuMaster.select().where(EcuMaster.dealer_id == dealer_source_id)
    return [record for record in query if record.ecu not in migrated_ecus]


def generate_excel_report(migrated_data, unmigrated_data):
    """Generate an Excel file with migrated and unmigrated device data."""
    workbook = Workbook()
    migrated_sheet = workbook.active
    migrated_sheet.title = "Migrated Devices"
    unmigrated_sheet = workbook.create_sheet(title="Unmigrated Devices")

    # Headers for Migrated Devices
    migrated_sheet.append(
        [
            "Device ID",
            "ECU Number",
            "Dealer ID",
            "User ID",
            "Created At",
            "Device Type",
            "Device Model",
            "Device Variant",
        ]
    )

    for record in migrated_data:
        migrated_sheet.append(
            [
                record["device_id"],
                record["ecu_number"],
                record["dealer_id"],
                record["user_id"],
                record["created_at"],
                record.get("device_type_name", ""),  # Device Type Name
                record.get("device_model_name", ""),  # Device Model Name
                record.get("device_variant_name", ""),  # Device Variant Name
            ]
        )

    # Headers for Unmigrated Devices
    unmigrated_sheet.append(
        ["ECU Number", "Dealer ID", "Reason"]
    )

    for record in unmigrated_data:
        unmigrated_sheet.append(
            [
                record["ecu_number"],
                record["dealer_id"],
                record["reason"],
            ]
        )

    workbook.save(EXCEL_FILE_NAME)
    print(f"\nMigration report saved as {EXCEL_FILE_NAME}")


def get_or_create_device_type(name: str, user: DestinationUser):
    device_type, created = DeviceType.get_or_create(
        name=name,
        defaults={
            "user_id": user.id,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
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
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        },
    )
    return device_model


def get_or_create_device_variant(name: str, device_model: DeviceModel, user: DestinationUser):
    variant_name = name if name else device_model.name.lower().replace(" ", "_")
    device_variant, created = DeviceVariant.get_or_create(
        name=variant_name,
        device_model_id=device_model.id,
        defaults={
            "user_id": user.id,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        },
    )
    return device_variant


def get_device_data_by_ecu(ecu_record: EcuMaster, user: DestinationUser, new_dealer_id: int):
    """Map ECU record to DeviceType, DeviceModel, and DeviceVariant, then create a Device record."""
    for prefix, mapping in ecm_mapping.items():
        if ecu_record.ecu.startswith(prefix):
            device_type = get_or_create_device_type(mapping["device_type"], user)
            device_model = get_or_create_device_model(mapping["device_model"], device_type, mapping["approval_code"], user)
            device_variant = get_or_create_device_variant(mapping["device_variant"], device_model, user)

            device, created = Device.get_or_create(
                ecu_number=ecu_record.ecu,
                defaults={
                    "device_type_id": device_type.id,
                    "device_model_id": device_model.id,
                    "device_variant_id": device_variant.id if device_variant else None,
                    "dealer_id": new_dealer_id,
                    "user_id": user.id,
                    "lock": ecu_record.lock,
                    "remarks": ecu_record.remarks,
                    "created_at": ecu_record.add_date_timestamp,
                    "updated_at": ecu_record.add_date_timestamp,
                },
            )
            return device

    print(f"No matching mapping found for ECU {ecu_record.ecu}")
    return None


def migrate_devices_in_batches(unmigrated_devices, user, dealer_id, device_mappings):
    """Migrate devices in batches to handle large datasets."""
    total_devices = len(unmigrated_devices)
    migrated_data = []
    unmigrated_data = []

    for batch_start in range(0, total_devices, BATCH_SIZE):
        batch = unmigrated_devices[batch_start:batch_start + BATCH_SIZE]
        print(f"Migrating batch {batch_start + 1} to {batch_start + len(batch)} of {total_devices}")

        for ecu_record in batch:
            try:
                device = get_device_data_by_ecu(ecu_record, user, dealer_id)
                if device:
                    # Fetch related data for DeviceType, DeviceModel, and DeviceVariant
                    device_type = DeviceType.get_by_id(device.device_type_id)
                    device_model = DeviceModel.get_by_id(device.device_model_id)
                    device_variant = (
                        DeviceVariant.get_by_id(device.device_variant_id)
                        if device.device_variant_id
                        else None
                    )

                    # Add to migrated data
                    migrated_data.append(
                        {
                            "device_id": device.id,
                            "ecu_number": ecu_record.ecu,
                            "device_type_name": device_type.name,
                            "device_model_name": device_model.name,
                            "device_variant_name": device_variant.name if device_variant else "",
                            "dealer_id": dealer_id,
                            "user_id": user.id,
                            "created_at": ecu_record.add_date_timestamp,
                        }
                    )

                    # Save mapping
                    device_mappings[str(device.id)] = {
                        "ecu_number": ecu_record.ecu,
                        "dealer_id": dealer_id,
                    }
                    save_device_mappings(device_mappings)

            except Exception as e:
                unmigrated_data.append({"ecu_number": ecu_record.ecu, "dealer_id": ecu_record.dealer_id, "reason": str(e)})

    return migrated_data, unmigrated_data


def run_migration():
    """Main function to run the device migration."""
    user_mappings = load_json_mapping(USER_MAPPING_FILE)
    device_mappings = load_json_mapping(DEVICE_MAPPING_FILE)

    mode = questionary.select(
        "How would you like to perform the migration?",
        choices=["Run Fully Automated", "Migrate Devices One by One", "Migrate Devices for a Specific Dealer by ID"],
    ).ask()

    try:
        if mode == "Run Fully Automated":
            print("Running Fully Automated Migration...")
            for user in DestinationUser.select():
                dealer_id = user_mappings.get(str(user.id), {}).get("dealer_id")
                if dealer_id:
                    unmigrated_devices = list_unmigrated_devices(dealer_id, device_mappings)
                    migrated_data, unmigrated_data = migrate_devices_in_batches(unmigrated_devices, user, dealer_id, device_mappings)
                    generate_excel_report(migrated_data, unmigrated_data)

    except KeyboardInterrupt:
        print("Migration interrupted. Progress saved.")
    except Exception as e:
        print(f"Error during migration: {e}")


if __name__ == "__main__":
    run_migration()