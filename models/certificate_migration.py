from datetime import datetime
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
TECHNICIAN_MAPPING_FILE = "technicians_mapping.json"
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

def save_technician_mapping(technician_id, old_user_id, new_user_id):
    """
    Append a single technician mapping to the mappings file.
    """
    try:
        # Load the existing mappings
        existing_mappings = load_mappings(TECHNICIAN_MAPPING_FILE)

        # Add the new mapping
        existing_mappings[str(technician_id)] = {
            "old_user_id": old_user_id,
            "user_id": new_user_id,
        }

        # Save the updated mappings
        save_mappings(TECHNICIAN_MAPPING_FILE, existing_mappings)
        print(f"Technician mapping added: {technician_id} -> {old_user_id}")

    except Exception as e:
        print(f"Failed to save technician mapping: {e}")

def get_or_create_technician_for_certificate(
    calibrater_user_id, technician_id, default_user, mappings
):
    """
    Get or create a technician based on calibrater_user_id or technician_id.
    1. Use technician_id if available to find the technician using technician mappings.
    2. If technician_id is not available or 0, find the calibrater_user_id in the user mappings.
    3. Ensure no duplicate technician is created for the same user.
    4. Save the newly created technician in the technician mappings if created.

    Args:
        calibrater_user_id (int): The old user ID (calibrater_user_id) from the source data.
        technician_id (int): The old technician ID from the source data.
        default_user (DestinationUser): The default user object for created_by field.
        mappings (dict): A dictionary containing user and technician mappings.

    Returns:
        Technician: The existing or newly created technician object.
    """
    try:
        technician_mappings = mappings["technician"]

        # Step 1: Check if technician_id is provided and valid
        if technician_id and technician_id != 0:
            for new_technician_id, technician_mapping in technician_mappings.items():
                if technician_mapping.get("old_technician_id") == technician_id:
                    print(
                        f"Technician already exists for old_technician_id {technician_id}. Using Technician ID {new_technician_id}."
                    )
                    return Technician.get_by_id(int(new_technician_id))

        # Step 2: If technician_id is not valid, use calibrater_user_id to find/create a technician
        user_mappings = mappings["user"]
        new_user_id = None
        for user_id, mapping in user_mappings.items():
            if mapping.get("old_user_id") == calibrater_user_id:
                new_user_id = int(user_id)  # The new user ID is the key
                break

        if not new_user_id:
            raise Exception(
                f"No mapping found for calibrater_user_id {calibrater_user_id} in user mappings."
            )

        # Step 3: Check if a technician already exists for the user in the technician table
        calibrater_user = DestinationUser.get_by_id(new_user_id)
        existing_technician = Technician.get_or_none(Technician.email == calibrater_user.email)
        if existing_technician:
            print(
                f"Technician with email {calibrater_user.email} already exists. Using existing technician."
            )
            return existing_technician

        # Step 4: Create a new technician
        technician = Technician.create(
            name=calibrater_user.name,
            email=calibrater_user.email,
            phone=calibrater_user.phone or "0000000000",  # Placeholder phone if empty
            user_id=new_user_id,
            created_by=default_user.id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Step 5: Save the technician mapping
        technician_mappings[str(technician.id)] = {
            "old_technician_id": technician_id or 0,  # Store the old technician ID or 0 if not provided
            "user_id": new_user_id,
        }
        save_mappings("technicians_mappings.json", technician_mappings)

        print(
            f"New Technician created: {technician.name} for calibrater_user_id {calibrater_user_id}."
        )
        return technician

    except Exception as e:
        print(f"Error in creating or fetching technician: {e}")
        return None
    
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

    # Technician Mapping or Creation
    technician = None

    if record.installer_technician_id == 0:
        technician = get_or_create_technician_for_certificate(
            calibrater_user_id=record.caliberater_user_id,
            technician_id=record.installer_technician_id,
            default_user=default_user,
            mappings=mappings
        )
    else:
        technician_id = mappings["technician"].get(str(record.installer_technician_id))
        technician = Technician.get_or_none(id=technician_id)

    if not technician:
        errors.append("Technician not found or could not be created")

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
            answer = questionary.select(
                f"Certificate for ECU {record.ecu}:",
                choices=[
                    "Migrate Certificate",
                    "Skip Certificate",
                    "Exit Migration"
                ]
            ).ask()

            if answer == "Migrate Certificate":
                certificate, errors = migrate_certificate(record, mappings, default_user, certificate_mappings)
                if not certificate:
                    unmigrated.append({"ecu": record.ecu, "errors": errors})
            elif answer == "Skip Certificate":
                print(f"Skipping Certificate for ECU {record.ecu}.")
                continue
            elif answer == "Exit Migration":
                print("Exiting migration as per user request.")
                break

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
        "user": load_mappings(USER_MAPPING_FILE),
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