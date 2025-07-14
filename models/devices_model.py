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
    DoesNotExist
)
from tqdm import tqdm
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser

# Constants
DEVICE_MAPPING_FILE = "device_mappings.json"
USER_MAPPING_FILE = "user_mappings.json"
EXCEL_FILE_NAME = "device_migration_report.xlsx"
BATCH_SIZE = 20000  # Number of records per batch
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Replace with the known admin email

# ECU mapping (prefix-based). Adjust these as necessary.
ecm_mapping = {
    "D1005E": {
        "device_type": "Fuel Type Speed Limiter",
        "device_model": "AutoGrade Dass86",
        "device_variant": "",
        "approval_code": "24-01-22784/Q24-01-048943/NB0002",
    },
    "D1005F": {
        "device_type": "Fuel Type Speed Limiter",
        "device_model": "AutoGrade Dass86",
        "device_variant": "",
        "approval_code": "24-01-22784/Q24-01-048943/NB0002",
    },
    "D1007E": {
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
        "device_type": "Electronic Type Speed Limiter",
        "device_model": "Resolute Dynamics ESL",
        "device_variant": "",
        "approval_code": "24-01-22783/Q24-01-048944/NB0002",
    },
    "FSL": {
        "device_type": "Fuel Type Speed Limiter",
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
    "ADS": {
        "device_type": "Adaptive Speed Limiter",
        "device_model": "Resolute Dynamics ASL",
        "device_variant": "",
        "approval_code": "24-01-22783/Q24-01-048944/NB0002",
    },
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
    country_id = IntegerField()
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
    country_id = IntegerField()
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
    country_id = IntegerField()
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
    blocked = IntegerField(default=0)
    dealer_id = BigIntegerField(null=True)
    user_id = BigIntegerField()
    country_id = IntegerField()
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
    """Get a list of unmigrated devices for a given source dealer ID."""
    migrated_ecus = [v["ecu_number"] for v in device_mappings.values()]
    query = EcuMaster.select().where(EcuMaster.dealer_id == dealer_source_id)
    return [record for record in query if record.ecu not in migrated_ecus]

def generate_excel_report(migrated_data, unmigrated_data):
    """Generate an Excel file with migrated and unmigrated device data."""
    workbook = Workbook()
    migrated_sheet = workbook.active
    migrated_sheet.title = "Migrated Devices"
    unmigrated_sheet = workbook.create_sheet(title="Unmigrated Devices")

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
                record.get("device_type_name", ""),
                record.get("device_model_name", ""),
                record.get("device_variant_name", ""),
            ]
        )

    unmigrated_sheet.append(["ECU Number", "Dealer ID", "Reason"])
    for record in unmigrated_data:
        unmigrated_sheet.append(
            [
                record["ecu_number"],
                record["dealer_id"],
                record["reason"],
            ]
        )

    try:
        workbook.save(EXCEL_FILE_NAME)
    except Exception as e:
        print(f"Error saving Excel file: {e}")

def get_or_create_device_type(name: str, user: DestinationUser):
    device_type, _ = DeviceType.get_or_create(
        name=name,
        defaults={
            "user_id": user.id,
            "country_id": 231,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        },
    )
    return device_type

def get_or_create_device_model(name: str, device_type: DeviceType, approval_code: str, user: DestinationUser):
    device_model, _ = DeviceModel.get_or_create(
        name=name,
        device_type_id=device_type.id,
        approval_code=approval_code,
        defaults={
            "user_id": user.id,
            "country_id": 231,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        },
    )
    return device_model

def get_or_create_device_variant(name: str, device_model: DeviceModel, user: DestinationUser):
    variant_name = name if name else device_model.name.lower().replace(" ", "_")
    device_variant, _ = DeviceVariant.get_or_create(
        name=variant_name,
        device_model_id=device_model.id,
        defaults={
            "user_id": user.id,
            "country_id": 231,
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        },
    )
    return device_variant

def migrate_devices_in_batches(unmigrated_devices, default_user, new_dealer_id, device_mappings):
    """
    Migrate devices in batches.
    'new_dealer_id' is the destination user's id.
    A progress bar is displayed showing the current batch and the failed records count.
    """
    total_devices = len(unmigrated_devices)
    migrated_data = []
    unmigrated_data = []
    batch_device_records = []
    fail_count = 0

    total_batches = (total_devices + BATCH_SIZE - 1) // BATCH_SIZE

    with tqdm(total=total_batches, desc="Migrating batches", unit="batch") as pbar:
        for batch_start in range(0, total_devices, BATCH_SIZE):
            batch = unmigrated_devices[batch_start:batch_start + BATCH_SIZE]

            for ecu_record in batch:
                try:
                    device_mapped = False
                    for prefix, mapping in ecm_mapping.items():
                        if ecu_record.ecu.startswith(prefix):
                            device_mapped = True
                            device_type = get_or_create_device_type(mapping["device_type"], default_user)
                            device_model = get_or_create_device_model(mapping["device_model"], device_type, mapping["approval_code"], default_user)
                            device_variant = get_or_create_device_variant(mapping["device_variant"], device_model, default_user)

                            device_record = {
                                "ecu_number": ecu_record.ecu,
                                "device_type_id": device_type.id,
                                "device_model_id": device_model.id,
                                "device_variant_id": device_variant.id if device_variant else None,
                                "dealer_id": new_dealer_id,
                                "user_id": default_user.id,
                                "country_id": 231,
                                "lock": ecu_record.lock,
                                "remarks": ecu_record.remarks,
                                "created_at": ecu_record.add_date_timestamp,
                                "updated_at": ecu_record.add_date_timestamp,
                            }
                            batch_device_records.append(device_record)
                            break
                    if not device_mapped:
                        fail_count += 1
                        unmigrated_data.append({
                            "ecu_number": ecu_record.ecu,
                            "dealer_id": ecu_record.dealer_id,
                            "reason": "No matching prefix in ECM mapping",
                        })
                except Exception as e:
                    fail_count += 1
                    unmigrated_data.append({
                        "ecu_number": ecu_record.ecu,
                        "dealer_id": ecu_record.dealer_id,
                        "reason": str(e),
                    })

            if batch_device_records:
                try:
                    Device.insert_many(batch_device_records).execute()
                    for record in batch_device_records:
                        try:
                            device = Device.get(Device.ecu_number == record["ecu_number"])
                            migrated_data.append({
                                "device_id": device.id,
                                "ecu_number": device.ecu_number,
                                "device_type_name": DeviceType.get_by_id(device.device_type_id).name,
                                "device_model_name": DeviceModel.get_by_id(device.device_model_id).name,
                                "device_variant_name": (DeviceVariant.get_by_id(device.device_variant_id).name
                                                        if device.device_variant_id else ""),
                                "dealer_id": new_dealer_id,
                                "user_id": default_user.id,
                                "created_at": device.created_at,
                            })
                            # Store the mapping using the new device ID as the key
                            device_mappings[str(device.id)] = {
                                "ecu_number": device.ecu_number,
                                "dealer_id": new_dealer_id,
                            }
                        except DoesNotExist:
                            fail_count += 1
                            unmigrated_data.append({
                                "ecu_number": record["ecu_number"],
                                "dealer_id": record["dealer_id"],
                                "reason": "Device not found after insertion",
                            })
                    save_device_mappings(device_mappings)
                    batch_device_records.clear()
                except Exception as e:
                    for record in batch_device_records:
                        fail_count += 1
                        unmigrated_data.append({
                            "ecu_number": record["ecu_number"],
                            "dealer_id": record["dealer_id"],
                            "reason": "Batch insert failed: " + str(e),
                        })
                    batch_device_records.clear()
            pbar.set_postfix({"Failed": fail_count})
            pbar.update(1)

    return migrated_data, unmigrated_data

def get_default_user():
    """Get the default user for assigning user_id in the Device table."""
    try:
        return DestinationUser.get(DestinationUser.email == DEFAULT_USER_EMAIL)
    except DoesNotExist:
        raise Exception(f"Default user '{DEFAULT_USER_EMAIL}' not found.")

def run_migration():
    """
    Main function to run the device migration.
    Uses the destination user as the basis for assigning the new dealer_id.
    """
    user_mappings = load_json_mapping(USER_MAPPING_FILE)
    device_mappings = load_json_mapping(DEVICE_MAPPING_FILE)
    default_user = get_default_user()  # Default user for device creation
    all_migrated_data = []
    all_unmigrated_data = []

    mode = questionary.select(
        "How would you like to perform the migration?",
        choices=["Run Fully Automated", "Migrate Devices One by One", "Migrate Devices for a Specific Destination User by ID"],
    ).ask()

    try:
        if mode == "Run Fully Automated":
            for dest_user in DestinationUser.select():
                source_dealer_id = user_mappings.get(str(dest_user.id), {}).get("dealer_id")
                if source_dealer_id:
                    unmigrated_devices = list_unmigrated_devices(source_dealer_id, device_mappings)
                    migrated_data, unmigrated_data = migrate_devices_in_batches(unmigrated_devices, default_user, dest_user.id, device_mappings)
                    all_migrated_data.extend(migrated_data)
                    all_unmigrated_data.extend(unmigrated_data)
        elif mode == "Migrate Devices One by One":
            for dest_user in DestinationUser.select():
                source_dealer_id = user_mappings.get(str(dest_user.id), {}).get("dealer_id")
                if source_dealer_id:
                    user_choice = questionary.select(
                        f"Migrate devices for user {dest_user.email} (mapped source dealer ID: {source_dealer_id})?",
                        choices=["Migrate", "Skip"],
                    ).ask()
                    if user_choice == "Migrate":
                        unmigrated_devices = list_unmigrated_devices(source_dealer_id, device_mappings)
                        migrated_data, unmigrated_data = migrate_devices_in_batches(unmigrated_devices, default_user, dest_user.id, device_mappings)
                        all_migrated_data.extend(migrated_data)
                        all_unmigrated_data.extend(unmigrated_data)
        elif mode == "Migrate Devices for a Specific Destination User by ID":
            user_id_input = questionary.text("Enter the Destination User ID to migrate:").ask()
            if not user_id_input.isdigit():
                print("Invalid User ID. Please enter a numeric value.")
                return
            dest_user_id = int(user_id_input)
            source_dealer_id = user_mappings.get(str(dest_user_id), {}).get("dealer_id")
            if not source_dealer_id:
                print(f"No mapping found for Destination User ID {dest_user_id}.")
                return
            unmigrated_devices = list_unmigrated_devices(source_dealer_id, device_mappings)
            migrated_data, unmigrated_data = migrate_devices_in_batches(unmigrated_devices, default_user, dest_user_id, device_mappings)
            all_migrated_data.extend(migrated_data)
            all_unmigrated_data.extend(unmigrated_data)
        else:
            print("Invalid choice. Exiting...")
    except KeyboardInterrupt:
        print("Migration interrupted. Progress saved.")
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        generate_excel_report(all_migrated_data, all_unmigrated_data)
        print("Migration process completed and report generated.")

if __name__ == "__main__":
    run_migration()