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
    BlobField,
    ForeignKeyField,
    IntegrityError,
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
class Device(Model):
    id = BigIntegerField(primary_key=True)
    ecu_number = CharField(max_length=255, unique=True)
    device_type_id = BigIntegerField()
    device_model_id = BigIntegerField()
    device_variant_id = BigIntegerField(null=True)
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
            "Device Type ID",
            "Device Model ID",
            "Dealer ID",
            "User ID",
            "Created At",
        ]
    )
    for record in migrated_data:
        migrated_sheet.append(
            [
                record["device_id"],
                record["ecu_number"],
                record["device_type_id"],
                record["device_model_id"],
                record["dealer_id"],
                record["user_id"],
                record["created_at"],
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


# ----------------- MIGRATION LOGIC ----------------- #
def list_unmigrated_devices(dealer_source_id: int, device_mappings: dict) -> list:
    """Get a list of unmigrated devices for a given dealer source ID."""
    migrated_ecus = [v["ecu_number"] for v in device_mappings.values()]
    query = EcuMaster.select().where(EcuMaster.dealer_id == dealer_source_id)
    return [record for record in query if record.ecu not in migrated_ecus]


def migrate_devices_in_batches(unmigrated_devices, user, dealer_id, device_mappings):
    """Migrate devices in batches to handle large datasets."""
    total_devices = len(unmigrated_devices)
    migrated_data = []
    unmigrated_data = []
    print(f"Total devices to migrate: {total_devices}")
    
    for batch_start in range(0, total_devices, BATCH_SIZE):
        batch = unmigrated_devices[batch_start : batch_start + BATCH_SIZE]
        print(f"Migrating batch: {batch_start + 1} to {batch_start + len(batch)}")

        for ecu_record in batch:
            try:
                device, created = Device.get_or_create(
                    ecu_number=ecu_record.ecu,
                    defaults={
                        "lock": ecu_record.lock,
                        "remarks": ecu_record.remarks,
                        "dealer_id": dealer_id,
                        "user_id": user.id,
                        "created_at": ecu_record.add_date_timestamp,
                        "updated_at": ecu_record.add_date_timestamp,
                    },
                )
                if created:
                    migrated_data.append(
                        {
                            "device_id": device.id,
                            "ecu_number": ecu_record.ecu,
                            "device_type_id": None,  # Add if available
                            "device_model_id": None,  # Add if available
                            "dealer_id": dealer_id,
                            "user_id": user.id,
                            "created_at": ecu_record.add_date_timestamp,
                        }
                    )
                    device_mappings[device.id] = {
                        "ecu_number": ecu_record.ecu,
                        "dealer_id": dealer_id,
                    }
                    save_device_mappings(device_mappings)
            except IntegrityError as e:
                print(f"Failed to migrate ECU {ecu_record.ecu}: {e}")
                unmigrated_data.append(
                    {
                        "ecu_number": ecu_record.ecu,
                        "dealer_id": ecu_record.dealer_id,
                        "reason": str(e),
                    }
                )
            except Exception as e:
                print(f"Error migrating ECU {ecu_record.ecu}: {e}")
                unmigrated_data.append(
                    {
                        "ecu_number": ecu_record.ecu,
                        "dealer_id": ecu_record.dealer_id,
                        "reason": str(e),
                    }
                )

    return migrated_data, unmigrated_data


def migrate_devices(user: DestinationUser, source_dealer_id: int, device_mappings):
    """Migrate devices for a given user and source dealer ID."""
    unmigrated_devices = list_unmigrated_devices(source_dealer_id, device_mappings)
    if not unmigrated_devices:
        print(f"No unmigrated devices found for dealer {source_dealer_id}.")
        return device_mappings

    print(f"\nStarting migration for {len(unmigrated_devices)} devices.")
    migrated_data, unmigrated_data = migrate_devices_in_batches(
        unmigrated_devices, user, source_dealer_id, device_mappings
    )
    generate_excel_report(migrated_data, unmigrated_data)
    return device_mappings


def run_migration():
    """Main function to run the device migration."""
    user_mappings = load_user_mappings()
    device_mappings = load_device_mappings()

    mode = questionary.select(
        "How would you like to perform the migration?",
        choices=[
            "Run Fully Automated",
            "Migrate Devices One by One",
            "Migrate Devices for a Specific Dealer by ID",
        ],
    ).ask()

    try:
        if mode == "Run Fully Automated":
            print("Running Fully Automated Migration...")
            for user in DestinationUser.select():
                mapping = user_mappings.get(str(user.id))
                if not mapping or "dealer_id" not in mapping:
                    continue
                dealer_id = mapping["dealer_id"]
                device_mappings = migrate_devices(user, dealer_id, device_mappings)

        elif mode == "Migrate Devices One by One":
            for user in DestinationUser.select():
                mapping = user_mappings.get(str(user.id))
                if not mapping or "dealer_id" not in mapping:
                    continue
                dealer_id = mapping["dealer_id"]
                proceed = questionary.confirm(
                    f"Do you want to migrate devices for {user.email}?"
                ).ask()
                if proceed:
                    device_mappings = migrate_devices(user, dealer_id, device_mappings)

        elif mode == "Migrate Devices for a Specific Dealer by ID":
            dealer_id = int(questionary.text("Enter the dealer ID:").ask())
            user = DestinationUser.get(
                DestinationUser.id == [
                    k for k, v in user_mappings.items() if v["dealer_id"] == dealer_id
                ][0]
            )
            device_mappings = migrate_devices(user, dealer_id, device_mappings)

    except KeyboardInterrupt:
        print("\nMigration interrupted. Progress saved.")
    except Exception as e:
        print(f"Error during migration: {e}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()


if __name__ == "__main__":
    run_migration()