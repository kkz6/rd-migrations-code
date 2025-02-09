import json
import questionary
from openpyxl import Workbook
from typing import List
from threading import Thread
from queue import Queue
from peewee import DoesNotExist, IntegrityError
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
    if not migrated and not unmigrated:
        print("No data to save. Skipping Excel file creation.")
        return

    workbook = Workbook()
    
    # Migrated Sheet
    if migrated:
        migrated_sheet = workbook.active
        migrated_sheet.title = "Migrated"
        headers = migrated[0].keys()
        migrated_sheet.append(headers)  # Add headers
        for row in migrated:
            migrated_sheet.append(row.values())  # Add rows

    # Unmigrated Sheet
    if unmigrated:
        unmigrated_sheet = workbook.create_sheet(title="Unmigrated")
        headers = unmigrated[0].keys()
        unmigrated_sheet.append(headers)  # Add headers
        for row in unmigrated:
            unmigrated_sheet.append(row.values())  # Add rows

    workbook.save(EXCEL_FILE_NAME)
    print(f"Migration report saved to: {EXCEL_FILE_NAME}")

def get_default_user():
    try:
        return DestinationUser.get(DestinationUser.email == DEFAULT_USER_EMAIL)
    except DoesNotExist:
        raise Exception(f"Default user with email {DEFAULT_USER_EMAIL} not found.")

def list_unmigrated_certificates(migrated_ids):
    # Convert dict_keys to a list and ensure it contains valid integers
    migrated_ids = list(map(int, migrated_ids)) if migrated_ids else []  # Convert keys to integers if available
    if migrated_ids:  # Check if migrated_ids is not empty
        return CertificateRecord.select().where(CertificateRecord.id.not_in(migrated_ids))
    else:
        return CertificateRecord.select()  # No migrated IDs, return all certificates

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

        # Insert Certificate Immediately After Successful Migration
        Certificate.insert(certificate).execute()

        # Update Certificate Mapping
        if str(record.id) not in certificate_mappings:
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
def worker(queue, unmigrated, mappings, default_user, certificate_mappings):
    while not queue.empty():
        record = queue.get()
        try:
            _, errors = migrate_certificate(record, mappings, default_user, certificate_mappings)
            if errors:
                unmigrated.append({"ecu": record.ecu, "errors": errors})
        finally:
            queue.task_done()

def run_one_by_one(mappings, default_user, certificate_mappings):
    print("Starting One-by-One Migration")
    unmigrated = []
    records = list_unmigrated_certificates(certificate_mappings.keys())  # Pass dict_keys, handled by the function
    try:
        for record in records:
            proceed = questionary.confirm(f"Migrate Certificate for ECU {record.ecu}?").ask()
            if proceed:
                certificate, errors = migrate_certificate(record, mappings, default_user, certificate_mappings)
                if not certificate:
                    unmigrated.append({"ecu": record.ecu, "errors": errors})
    except KeyboardInterrupt:
        print("\nMigration interrupted by user. Saving progress...")
    finally:
        save_mappings(CERTIFICATES_MAPPING_FILE, certificate_mappings)
        save_to_excel([], unmigrated)

def run_fully_automated(mappings, default_user, certificate_mappings):
    print("Starting Fully Automated Migration")
    unmigrated = []
    records = list_unmigrated_certificates(certificate_mappings.keys())  # Pass dict_keys, handled by the function

    queue = Queue()
    for record in records:
        queue.put(record)

    threads = []
    for _ in range(THREAD_COUNT):
        thread = Thread(target=worker, args=(queue, unmigrated, mappings, default_user, certificate_mappings))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    save_mappings(CERTIFICATES_MAPPING_FILE, certificate_mappings)
    save_to_excel([], unmigrated)

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
        ],
    ).ask()

    try:
        if mode == "Run Fully Automated":
            run_fully_automated(mappings, default_user, certificate_mappings)
        elif mode == "Migrate Certificates One by One":
            run_one_by_one(mappings, default_user, certificate_mappings)
        else:
            print("Invalid choice. Exiting...")
    except KeyboardInterrupt:
        print("\nMigration process interrupted by user. Exiting...")