import re
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

# Global Constants / Configurations
CUSTOMER_MAPPING_FILE = "customer_mappings.json"
USER_MAPPING_FILE = "user_mappings.json"
DEVICE_MAPPING_FILE = "devices_mappings.json"
TECHNICIAN_MAPPING_FILE = "technicians_mapping.json"  # Global constant for technician mapping file
CERTIFICATES_MAPPING_FILE = "certificates_mappings.json"
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Used for technician creation only
EXCEL_FILE_NAME = "certificate_migration_report.xlsx"
THREAD_COUNT = 4

# --- Helper Functions ---

def parse_speed(speed_str):
    """
    Extracts the first integer found in speed_str.
    Returns 0 if no valid number is found.
    """
    if not speed_str:
        return 0
    match = re.search(r'\d+', speed_str)
    if match:
        return int(match.group())
    return 0

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
        headers = list(migrated[0].keys())
        migrated_sheet.append(headers)
        for row in migrated:
            migrated_sheet.append(list(row.values()))

    # Unmigrated Sheet
    if unmigrated:
        unmigrated_sheet = workbook.create_sheet(title="Unmigrated")
        headers = list(unmigrated[0].keys())
        unmigrated_sheet.append(headers)
        for row in unmigrated:
            unmigrated_sheet.append(list(row.values()))

    workbook.save(EXCEL_FILE_NAME)
    print(f"Migration report saved to: {EXCEL_FILE_NAME}")

def get_default_user():
    try:
        return DestinationUser.get(DestinationUser.email == DEFAULT_USER_EMAIL)
    except DoesNotExist:
        raise Exception(f"Default user with email {DEFAULT_USER_EMAIL} not found.")

def list_unmigrated_certificates(migrated_ids):
    # Convert migrated_ids (which may be a dict_keys object) into a list of integers.
    migrated_ids = list(migrated_ids)
    migrated_ids = list(map(int, migrated_ids)) if migrated_ids else []
    if migrated_ids:
        return CertificateRecord.select().where(CertificateRecord.id.not_in(migrated_ids))
    else:
        return CertificateRecord.select()

# --- Technician Mapping Function (using global constant for technician mapping file) ---
def get_or_create_technician_for_certificate(calibrater_user_id, technician_id, default_user, mappings):
    """
    Get or create a technician based on calibrater_user_id or technician_id.
    Uses the global TECHNICIAN_MAPPING_FILE for saving mappings.
    """
    try:
        technician_mappings = mappings["technician"]

        # If technician_id is provided and valid, try to locate an existing mapping.
        if technician_id and technician_id != 0:
            for new_technician_id, technician_mapping in technician_mappings.items():
                if technician_mapping.get("old_technician_id") == technician_id:
                    print(f"Technician already exists for old_technician_id {technician_id}. Using Technician ID {new_technician_id}.")
                    return Technician.get_by_id(int(new_technician_id))

        # Otherwise, use calibrater_user_id to find the new user mapping.
        user_mappings = mappings["user"]
        new_user_id = None
        for user_id, mapping in user_mappings.items():
            if mapping.get("old_user_id") == calibrater_user_id:
                new_user_id = int(user_id)  # New user ID is the mapping key.
                break

        if not new_user_id:
            raise Exception(f"No mapping found for calibrater_user_id {calibrater_user_id} in user mappings.")

        # Check if a technician already exists for the user.
        calibrater_user = DestinationUser.get_by_id(new_user_id)
        existing_technician = Technician.get_or_none(Technician.email == calibrater_user.email)
        if existing_technician:
            print(f"Technician with email {calibrater_user.email} already exists. Using existing technician.")
            return existing_technician

        # Create a new technician.
        technician = Technician.create(
            name=calibrater_user.name,
            email=calibrater_user.email,
            phone=calibrater_user.phone or "0000000000",  # Placeholder if empty
            user_id=new_user_id,
            created_by=default_user.id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        # Save the technician mapping using the global TECHNICIAN_MAPPING_FILE.
        technician_mappings[str(technician.id)] = {
            "old_technician_id": technician_id or 0,
            "user_id": new_user_id,
        }
        save_mappings(TECHNICIAN_MAPPING_FILE, technician_mappings)

        print(f"New Technician created: {technician.name} for calibrater_user_id {calibrater_user_id}.")
        return technician

    except Exception as e:
        print(f"Error in creating or fetching technician: {e}")
        return None

# --- Certificate Migration Function with Extended Export Data ---
# If batch_mode is False (default) the certificate is inserted immediately.
# If batch_mode is True, it only prepares certificate_data and export_data.
def migrate_certificate(record, mappings, default_user, certificate_mappings, batch_mode=False):
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
        tech_id = mappings["technician"].get(str(record.installer_technician_id))
        technician = Technician.get_or_none(id=tech_id)

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

    if errors:
        print(f"Skipping Certificate ID {record.id} due to errors: {errors}")
        return None, errors

    # --- Dealer Mapping (Updated) ---
    dealer_errors = []
    dealer_obj = None
    old_dealer_id = getattr(record, 'dealer_id', None)
    if not old_dealer_id:
        dealer_errors.append("No dealer id found in record")
    else:
        new_dealer_id = None
        # Look for a mapping where the mapping's dealer_id equals the certificate's old dealer id.
        for key, user_map in mappings["user"].items():
            if user_map.get("dealer_id") == old_dealer_id:
                new_dealer_id = int(key)  # The mapping key represents the new dealer id.
                break
        if not new_dealer_id:
            dealer_errors.append(f"No matching dealer mapping found for dealer id {old_dealer_id}")
        else:
            try:
                dealer_obj = DestinationUser.get_by_id(new_dealer_id)
                if dealer_obj.parent_id:
                    dealer_id_val = dealer_obj.parent_id
                    user_id_val = dealer_obj.id
                else:
                    dealer_id_val = dealer_obj.id
                    user_id_val = dealer_obj.id
            except DoesNotExist:
                dealer_errors.append(f"Dealer not found in DestinationUser for new_dealer_id {new_dealer_id}")

    if dealer_errors:
        errors.extend(dealer_errors)
        print(f"Skipping Certificate ID {record.id} due to dealer mapping errors: {dealer_errors}")
        return None, errors

    # Determine certificate status based on record fields.
    status = "renewed" if record.renewal_count > 0 else "active"
    status = "nullified" if record.serialno is None else status
    status = "cancelled" if record.date_cancelation else status
    status = "blocked" if record.activstate == 0 else status

    # Build the certificate data dictionary (for insertion) and export data dictionary.
    certificate_data = {
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
        # Use parse_speed to safely convert the speed value.
        "speed_limit": parse_speed(record.speed) if record.speed else 0,
        "print_count": record.print_count,
        "renewal_count": record.renewal_count,
        "description": record.description,
        "country": "UAE",
        "dealer_id": dealer_id_val,
        "user_id": user_id_val
    }

    export_data = {
        "ecu": record.ecu,
        "old_certificate_id": record.id,
        "new_certificate_id": None,  # To be updated after insertion.
        "certificate_serial": certificate_data.get("serial_number"),
        "status": certificate_data.get("status"),
        "old_technician_id": record.installer_technician_id,
        "new_technician_id": technician.id,
        "dealer_name": dealer_obj.name if dealer_obj else "N/A",
        "installation_date": certificate_data.get("installation_date"),
        "calibration_date": certificate_data.get("calibration_date"),
        "expiry_date": certificate_data.get("expiry_date"),
        "cancellation_date": certificate_data.get("cancellation_date"),
        "device_id": certificate_data.get("device_id"),
        "customer_id": certificate_data.get("installed_for_id"),
        "vehicle_id": certificate_data.get("vehicle_id")
    }

    if not batch_mode:
        try:
            new_cert = Certificate.create(**certificate_data)
            export_data["new_certificate_id"] = new_cert.id
            if str(record.id) not in certificate_mappings:
                certificate_mappings[str(record.id)] = {
                    "old_certificate_id": record.id,
                    "device_id": device.id,
                    "customer_id": customer.id,
                    "technician_id": technician.id,
                    "vehicle_id": vehicle.id if vehicle else None,
                    "dealer_id": dealer_id_val,
                }
            return export_data, None
        except Exception as e:
            errors.append(str(e))
            print(f"Error migrating Certificate ID {record.id}: {errors}")
            return None, errors
    else:
        # In batch mode, return the prepared data; insertion will be done later.
        return (certificate_data, export_data), None

# --- Worker for Fully Automated Batch Mode ---
def worker_batch(queue, batch_results, unmigrated, mappings, default_user, certificate_mappings):
    while not queue.empty():
        record = queue.get()
        try:
            result, errors = migrate_certificate(record, mappings, default_user, certificate_mappings, batch_mode=True)
            if result:
                batch_results.append(result)  # result is (certificate_data, export_data)
            elif errors:
                unmigrated.append({"ecu": record.ecu, "errors": errors})
        finally:
            queue.task_done()

# --- Migration Modes ---

def run_one_by_one(mappings, default_user, certificate_mappings):
    print("Starting One-by-One Migration")
    migrated = []
    unmigrated = []
    records = list_unmigrated_certificates(list(certificate_mappings.keys()))
    try:
        for record in records:
            answer = questionary.select(
                f"Certificate for ECU {record.ecu}:",
                choices=[
                    "Migrate Certificate",
                    "Skip Certificate",
                    "Exit Migration"
                ],
            ).ask()

            if answer == "Migrate Certificate":
                export_data, errors = migrate_certificate(record, mappings, default_user, certificate_mappings, batch_mode=False)
                if export_data:
                    migrated.append(export_data)
                else:
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
        save_to_excel(migrated, unmigrated)

def run_fully_automated(mappings, default_user, certificate_mappings):
    print("Starting Fully Automated Migration (Batch Insert Mode)")
    batch_results = []  # Holds tuples: (certificate_data, export_data)
    unmigrated = []
    records = list_unmigrated_certificates(list(certificate_mappings.keys()))
    queue = Queue()
    for record in records:
        queue.put(record)

    threads = []
    for _ in range(THREAD_COUNT):
        thread = Thread(target=worker_batch, args=(queue, batch_results, unmigrated, mappings, default_user, certificate_mappings))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    export_data_list = []
    if batch_results:
        certificate_data_list = [item[0] for item in batch_results]
        export_data_list = [item[1] for item in batch_results]
        try:
            # Attempt bulk insert with returning().
            query = Certificate.insert_many(certificate_data_list).returning(Certificate.id)
            new_ids = list(query.execute())
        except Exception as e:
            print(f"Batch insert with returning() failed: {e}")
            # Fallback for databases (like MySQL) that don't support returning():
            Certificate.insert_many(certificate_data_list).execute()
            new_ids = [None] * len(certificate_data_list)
        for idx, new_cert_id in enumerate(new_ids):
            export_data_list[idx]["new_certificate_id"] = new_cert_id
            old_cert_id = export_data_list[idx]["old_certificate_id"]
            if str(old_cert_id) not in certificate_mappings:
                certificate_mappings[str(old_cert_id)] = {
                    "old_certificate_id": old_cert_id,
                    "dealer_id": export_data_list[idx].get("device_id"),  # Adjust mapping info as needed.
                }
    else:
        export_data_list = []

    save_mappings(CERTIFICATES_MAPPING_FILE, certificate_mappings)
    save_to_excel(export_data_list, unmigrated)
    
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