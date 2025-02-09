import json
import questionary
import pandas as pd
from peewee import DoesNotExist, IntegrityError
from typing import List
from threading import Thread
from queue import Queue
from models.certificates_model import CertificateRecord, Certificate
from models.devices_model import Device
from models.customers_model import Customer
from models.technicians_model import Technician
from models.vehicles_model import Vehicle
from models.users_model import DestinationUser

# Configurations
CUSTOMER_MAPPING_FILE = "customer_mappings.json"
USER_MAPPING_FILE = "user_mappings.json"
DEVICE_MAPPING_FILE = "devices_mappings.json"
TECHNICIAN_MAPPING_FILE = "technicians_mappings.json"
CERTIFICATES_MAPPING_FILE = "certificates_mappings.json"
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Replace with admin email
EXCEL_FILE_NAME = "certificate_migration_report.xlsx"
BATCH_SIZE = 10000
THREAD_COUNT = 4

# Helper Functions
def load_mappings(file_path):
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def save_mappings(file_path, mappings):
    with open(file_path, "w") as file:
        json.dump(mappings, file, indent=4)

def save_to_excel(migrated: List[dict], unmigrated: List[dict]):
    with pd.ExcelWriter(EXCEL_FILE_NAME) as writer:
        if migrated:
            pd.DataFrame(migrated).to_excel(writer, index=False, sheet_name="Migrated")
        if unmigrated:
            pd.DataFrame(unmigrated).to_excel(writer, index=False, sheet_name="Unmigrated")
    print(f"Migration report saved to: {EXCEL_FILE_NAME}")

def get_default_user():
    try:
        return DestinationUser.get(DestinationUser.email == DEFAULT_USER_EMAIL)
    except DoesNotExist:
        raise Exception(f"Default user with email {DEFAULT_USER_EMAIL} not found.")

def list_unmigrated_certificates(migrated_ids):
    return CertificateRecord.select().where(CertificateRecord.id.not_in(migrated_ids))

def batch_insert_certificates(certificates):
    if certificates:
        Certificate.insert_many(certificates).execute()

def migrate_certificate(record, mappings, default_user, certificate_mappings):
    print(f"Starting migration for Certificate ID {record.id} (ECU: {record.ecu})")
    errors = []

    # Device Mapping
    device = Device.get_or_none(Device.ecu_number == record.ecu)
    if not device:
        errors.append("Device not found")

    # Customer Mapping
    customer = Customer.get_or_none(Customer.id == mappings.get("customer", {}).get(str(record.customer_id)))
    if not customer:
        errors.append("Customer not found")

    # Technician Mapping
    technician = Technician.get_or_none(Technician.id == mappings.get("technician", {}).get(str(record.installer_technician_id)))
    if not technician:
        errors.append("Technician not found")

    # Vehicle Mapping or Creation
    vehicle = Vehicle.get_or_none(vehicle_chassis_no=record.vehicle_chassis)
    if not vehicle and record.vehicle_type:
        vehicle_brand_model = record.vehicle_type.split(" ", 1)
        brand = vehicle_brand_model[0]
        model = vehicle_brand_model[1] if len(vehicle_brand_model) > 1 else brand
        try:
            vehicle = Vehicle.create(
                brand=brand,
                model=model,
                vehicle_no=record.vehicle_registration,
                vehicle_chassis_no=record.vehicle_chassis,
                new_registration=False
            )
        except IntegrityError:
            errors.append("Vehicle creation failed")

    # Skip if any errors
    if errors:
        print(f"Skipping Certificate ID {record.id} due to errors: {errors}")
        return None, errors

    # Create Certificate
    try:
        status = "renewed" if record.renewal_count > 0 else "active"
        status = "nullified" if record.serialno is None else status
        status = "cancelled" if record.date_cancelation else status
        status = "blocked" if record.activstate == 0 else status

        certificate = {
            "serial_number": record.serialno,
            "status": status,
            "device_id": device.id,
            "installation_date": record.date_installation,
            "calibration_date": record.date_calibrate,
            "expiry_date": record.date_expiry,
            "cancellation_date": record.date_cancelation,
            "cancelled": (record.date_cancelation is not None),
            "installed_by_id": technician.id,
            "installed_for_id": customer.id,
            "vehicle_id": vehicle.id if vehicle else None,
            "km_reading": record.kilometer or 0,
            "speed_limit": int(record.speed) if record.speed else 0,
            "print_count": record.print_count,
            "renewal_count": record.renewal_count,
            "description": record.description,
            "country": "UAE",
            "dealer_id": default_user.id,
            "user_id": default_user.id
        }

        # Update Certificate Mapping
        certificate_mappings[str(record.id)] = {
            "old_certificate_id": record.id,
            "device_id": device.id,
            "customer_id": customer.id,
            "technician_id": technician.id,
            "vehicle_id": vehicle.id if vehicle else None,
            "dealer_id": default_user.id,
        }

        return certificate, None
    except Exception as e:
        errors.append(str(e))
        print(f"Error migrating Certificate ID {record.id}: {errors}")
        return None, errors

# Multithreaded Worker
def worker(queue, migrated, unmigrated, mappings, default_user, certificate_mappings):
    while not queue.empty():
        record = queue.get()
        try:
            certificate, errors = migrate_certificate(record, mappings, default_user, certificate_mappings)
            if certificate:
                migrated.append(certificate)
            else:
                unmigrated.append({"ecu": record.ecu, "errors": errors})
        finally:
            queue.task_done()

# Migration Modes
def run_fully_automated(mappings, default_user, certificate_mappings):
    print("Starting Fully Automated Migration")
    migrated = []
    unmigrated = []
    records = list_unmigrated_certificates(certificate_mappings.keys())

    queue = Queue()
    for record in records:
        queue.put(record)

    threads = []
    for _ in range(THREAD_COUNT):
        thread = Thread(target=worker, args=(queue, migrated, unmigrated, mappings, default_user, certificate_mappings))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    batch_insert_certificates(migrated)
    save_mappings(CERTIFICATES_MAPPING_FILE, certificate_mappings)
    save_to_excel(migrated, unmigrated)

def run_one_by_one(mappings, default_user, certificate_mappings):
    print("Starting One-by-One Migration")
    migrated = []
    unmigrated = []
    records = list_unmigrated_certificates(certificate_mappings.keys())
    try:
        for record in records:
            proceed = questionary.confirm(f"Migrate Certificate for ECU {record.ecu}?").ask()
            if proceed:
                certificate, errors = migrate_certificate(record, mappings, default_user, certificate_mappings)
                if certificate:
                    migrated.append(certificate)
                else:
                    unmigrated.append({"ecu": record.ecu, "errors": errors})
    except KeyboardInterrupt:
        print("\nMigration interrupted by user. Saving progress...")
    finally:
        batch_insert_certificates(migrated)
        save_mappings(CERTIFICATES_MAPPING_FILE, certificate_mappings)
        save_to_excel(migrated, unmigrated)

def run_by_id(mappings, default_user, certificate_mappings):
    try:
        certificate_id = questionary.text("Enter Certificate ID to migrate:").ask()
        record = CertificateRecord.get_or_none(CertificateRecord.id == int(certificate_id))
        if not record:
            print(f"Certificate with ID {certificate_id} not found.")
            return
        certificate, errors = migrate_certificate(record, mappings, default_user, certificate_mappings)
        if certificate:
            batch_insert_certificates([certificate])
            print(f"Successfully migrated Certificate for ECU {record.ecu}")
        else:
            print(f"Failed to migrate Certificate for ECU {record.ecu}: {errors}")
    except KeyboardInterrupt:
        print("\nMigration interrupted by user.")

def run_batch_migration(mappings, default_user, certificate_mappings):
    print("Starting Batch Migration")
    migrated = []
    unmigrated = []
    total_records = CertificateRecord.select().count()
    batches = (total_records // BATCH_SIZE) + 1

    try:
        for batch in range(batches):
            print(f"Processing batch {batch + 1}/{batches}...")
            records = CertificateRecord.select().limit(BATCH_SIZE).offset(batch * BATCH_SIZE)
            queue = Queue()
            for record in records:
                queue.put(record)

            threads = []
            for _ in range(THREAD_COUNT):
                thread = Thread(target=worker, args=(queue, migrated, unmigrated, mappings, default_user, certificate_mappings))
                thread.start()
                threads.append(thread)

            for thread in threads:
                thread.join()

            batch_insert_certificates(migrated)
            migrated.clear()
    except KeyboardInterrupt:
        print("\nBatch migration interrupted by user. Saving progress...")
    finally:
        save_mappings(CERTIFICATES_MAPPING_FILE, certificate_mappings)
        save_to_excel(migrated, unmigrated)

# Main Function
def run_migration():
    print("Loading Mappings")
    mappings = {
        "customer": load_mappings(CUSTOMER_MAPPING_FILE),
        "technician": load_mappings(TECHNICIAN_MAPPING_FILE),
        "device": load_mappings(DEVICE_MAPPING_FILE),
    }
    certificate_mappings = load_mappings(CERTIFICATES_MAPPING_FILE)
    default_user = get_default_user()

    mode = questionary.select(
        "Choose migration mode:",
        choices=[
            "Run Fully Automated",
            "Migrate Certificates One by One",
            "Migrate Certificate by ID",
            "Batch Migration",
        ],
    ).ask()

    try:
        if mode == "Run Fully Automated":
            run_fully_automated(mappings, default_user, certificate_mappings)
        elif mode == "Migrate Certificates One by One":
            run_one_by_one(mappings, default_user, certificate_mappings)
        elif mode == "Migrate Certificate by ID":
            run_by_id(mappings, default_user, certificate_mappings)
        elif mode == "Batch Migration":
            run_batch_migration(mappings, default_user, certificate_mappings)
        else:
            print("Invalid choice. Exiting...")
    except KeyboardInterrupt:
        print("\nMigration process interrupted by user. Exiting...")
