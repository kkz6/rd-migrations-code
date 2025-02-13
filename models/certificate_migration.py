import re
from datetime import datetime
import json
import questionary
from openpyxl import Workbook
from typing import List
from threading import Thread
from queue import Queue
from peewee import *
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
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Used only in parts of the code not related to technician creation now
EXCEL_FILE_NAME = "certificate_migration_report.xlsx"
THREAD_COUNT = 10

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

# --- Updated Technician Mapping Function ---
def get_or_create_technician_for_certificate(technician_user_id, technician_id, mappings):
    """
    Get or create a technician based on technician_user_id or technician_id.
    Uses the global TECHNICIAN_MAPPING_FILE for saving mappings.
    The new technician's user_id and created_by fields are determined by the destination user's parent_id:
      - If parent_id is not None: user_id is set to parent_id and created_by is set to the user's id.
      - Otherwise, both are set to the user's id.
    """
    try:
        technician_mappings = mappings["technician"]

        # If technician_id is provided and valid, try to locate an existing mapping.
        if technician_id and technician_id != 0:
            for new_technician_id, technician_mapping in technician_mappings.items():
                if technician_mapping.get("old_technician_id") == technician_id:
                    print(f"Technician already exists for old_technician_id {technician_id}. Using Technician ID {new_technician_id}.")
                    return Technician.get_by_id(int(new_technician_id))

        # Otherwise, use technician_user_id to find the new user mapping.
        user_mappings = mappings["user"]
        new_user_id = None
        for user_id, mapping in user_mappings.items():
            if mapping.get("old_user_id") == technician_user_id:
                new_user_id = int(user_id)
                break

        if not new_user_id:
            raise Exception(f"No mapping found for technician_user_id {technician_user_id} in user mappings.")

        # Fetch the destination user record and determine field values based on parent_id.
        dest_user = DestinationUser.get_by_id(new_user_id)
        if dest_user.parent_id is not None:
            user_id_field = dest_user.parent_id
            created_by_field = dest_user.id
        else:
            user_id_field = dest_user.id
            created_by_field = dest_user.id

        technician = Technician.create(
            name=dest_user.name,
            email=dest_user.email,
            phone=dest_user.phone or "0000000000",  # Placeholder if empty
            user_id=user_id_field,
            created_by=created_by_field,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        technician_mappings[str(technician.id)] = {
            "old_technician_id": technician_id or 0,
            "user_id": new_user_id,
        }
        save_mappings(TECHNICIAN_MAPPING_FILE, technician_mappings)

        print(f"New Technician created: {technician.name} for technician_user_id {technician_user_id}.")
        return technician

    except Exception as e:
        print(f"Error in creating or fetching technician: {e}")
        return None

# --- Certificate Migration Function with Extended Technician Mapping ---

def migrate_certificate(record, mappings, default_user, certificate_mappings, batch_mode=False):
    print(f"Starting migration for Certificate ID {record.id} (ECU: {record.ecu})")
    errors = []

    # Device Mapping
    device = Device.get_or_none(Device.ecu_number == record.ecu)
    if not device:
        errors.append("Device not found")

    # Customer Mapping (modified)
    if record.customer_id is None:
        customer = None
    else:
        mapped_customer_id = mappings.get("customer", {}).get(str(record.customer_id))
        customer = Customer.get_or_none(Customer.id == mapped_customer_id) if mapped_customer_id else None
        if not customer:
            errors.append("Customer not found")

    # --- Updated Technician Mapping for Certificate ---
    # Installer Technician Mapping (for installed_by_id)
    installer_technician = None
    if record.installer_technician_id == 0:
        try:
            installer_technician = get_or_create_technician_for_certificate(
                technician_user_id=record.installer_user_id,  # new field expected in the record
                technician_id=record.installer_technician_id,
                mappings=mappings
            )
        except Exception as e:
            errors.append(f"Installer Technician creation error: {e.__class__.__name__}: {str(e)}")
    else:
        found_new_install_id = None
        for new_tech_id, tech_mapping in mappings["technician"].items():
            if tech_mapping.get("old_technician_id") == record.installer_technician_id:
                found_new_install_id = int(new_tech_id)
                break
        if found_new_install_id:
            installer_technician = Technician.get_or_none(id=found_new_install_id)
        else:
            errors.append(f"Technician mapping for installer_technician_id {record.installer_technician_id} not found")

    # Calibrater Technician Mapping (for calibrated_by_id)
    calibrater_technician = None
    if record.calibrater_technician_id == 0:
        try:
            calibrater_technician = get_or_create_technician_for_certificate(
                technician_user_id=record.calibrater_user_id,  # new field expected in the record
                technician_id=record.calibrater_technician_id,
                mappings=mappings
            )
        except Exception as e:
            errors.append(f"Calibrater Technician creation error: {e.__class__.__name__}: {str(e)}")
    else:
        found_new_calib_id = None
        for new_tech_id, tech_mapping in mappings["technician"].items():
            if tech_mapping.get("old_technician_id") == record.calibrater_technician_id:
                found_new_calib_id = int(new_tech_id)
                break
        if found_new_calib_id:
            calibrater_technician = Technician.get_or_none(id=found_new_calib_id)
        else:
            errors.append(f"Technician mapping for calibrater_technician_id {record.calibrater_technician_id} not found")
    
    if (not installer_technician) or (not calibrater_technician):
        if not any("Technician creation error" in err for err in errors):
            errors.append("Installer and/or Calibrater technician not found or could not be created")

    # Vehicle Mapping or Creation using get_or_create for thread safety
    if record.vehicle_type:
        vehicle_brand_model = record.vehicle_type.split(" ", 1)
        brand = vehicle_brand_model[0]
        model = vehicle_brand_model[1] if len(vehicle_brand_model) > 1 else brand
        try:
            vehicle, created = Vehicle.get_or_create(
                vehicle_chassis_no=record.vehicle_chassis,
                defaults={
                    "brand": brand,
                    "model": model,
                    "vehicle_no": record.vehicle_registration,
                    "new_registration": False
                }
            )
        except IntegrityError as ie:
            # In case of a race condition, try to retrieve the vehicle again.
            try:
                vehicle = Vehicle.get(vehicle_chassis_no=record.vehicle_chassis)
            except Exception as e:
                errors.append(f"Vehicle creation failed: {e.__class__.__name__}: {str(e)}")
    else:
        vehicle = Vehicle.get_or_none(vehicle_chassis_no=record.vehicle_chassis)

    if errors:
        print(f"Skipping Certificate ID {record.id} due to errors: {errors}")
        return None, errors

    # Dealer Mapping
    dealer_errors = []
    dealer_obj = None
    old_dealer_id = getattr(record, 'dealer_id', None)
    if not old_dealer_id:
        dealer_errors.append("No dealer id found in record")
    else:
        new_dealer_id = None
        for key, user_map in mappings["user"].items():
            if user_map.get("dealer_id") == old_dealer_id:
                new_dealer_id = int(key)
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
            except DoesNotExist as dne:
                dealer_errors.append(f"DoesNotExist: {str(dne)}")
    if dealer_errors:
        errors.extend(dealer_errors)
        print(f"Skipping Certificate ID {record.id} due to dealer mapping errors: {dealer_errors}")
        return None, errors

    # Determine certificate status
    max_renewal = CertificateRecord.select(fn.MAX(CertificateRecord.renewal_count))\
                    .where(CertificateRecord.ecu == record.ecu).scalar() or 0
    if record.renewal_count < max_renewal:
        status = "renewed"
    else:
        status = "active"

    status = "nullified" if record.serialno is None else status
    status = "cancelled" if record.date_cancelation else status
    status = "blocked" if record.activstate == 0 else status

    # --- Build Data Dictionaries with the new technician fields ---
    certificate_data = {
        "serial_number": record.serialno,
        "status": status,
        "device_id": device.id,
        "installation_date": record.date_installation,
        "calibration_date": record.date_calibrate,
        "expiry_date": record.date_expiry,
        "cancellation_date": record.date_cancelation,
        "cancelled": (record.date_cancelation is not None),
        "installed_by_id": installer_technician.id if installer_technician else None,
        "calibrated_by_id": calibrater_technician.id if calibrater_technician else None,
        "installed_for_id": customer.id if customer else None,  # Allows null if customer is None
        "vehicle_id": vehicle.id if vehicle else None,
        "km_reading": record.kilometer or 0,
        "speed_limit": parse_speed(record.speed) if record.speed else 0,
        "print_count": record.print_count,
        "renewal_count": record.renewal_count,
        "description": record.description,
        "country_id": 231,
        "dealer_id": dealer_id_val,
        "user_id": user_id_val
    }
    export_data = {
        "ecu": record.ecu,
        "old_certificate_id": record.id,
        "new_certificate_id": None,  # To be updated after insertion.
        "certificate_serial": certificate_data.get("serial_number"),
        "status": certificate_data.get("status"),
        "old_installer_technician_id": record.installer_technician_id,
        "new_installer_technician_id": installer_technician.id if installer_technician else None,
        "old_calibrater_technician_id": record.calibrater_technician_id,
        "new_calibrater_technician_id": calibrater_technician.id if calibrater_technician else None,
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

            # Update the Vehicle Record
            if vehicle:
                vehicle.certificate_id = new_cert.id
                vehicle.save()

            if str(record.id) not in certificate_mappings:
                certificate_mappings[str(record.id)] = {
                    "old_certificate_id": record.id,
                    "device_id": device.id,
                    "customer_id": customer.id if customer else None,
                    "technician_id": {  # Store both technician mappings
                        "installer": installer_technician.id if installer_technician else None,
                        "calibrater": calibrater_technician.id if calibrater_technician else None
                    },
                    "vehicle_id": vehicle.id if vehicle else None,
                    "dealer_id": dealer_id_val,
                }
            return export_data, None
        except Exception as e:
            errors.append(f"{e.__class__.__name__}: {str(e)}")
            print(f"Error migrating Certificate ID {record.id}: {errors}")
            return None, errors
    else:
        # In batch mode, return the prepared data; insertion will be done later.
        return (certificate_data, export_data), None

def worker_batch(queue, batch_results, unmigrated, mappings, default_user, certificate_mappings):
    while not queue.empty():
        record = queue.get()
        try:
            result, errors = migrate_certificate(record, mappings, default_user, certificate_mappings, batch_mode=True)
            if result:
                batch_results.append(result)  # result is (certificate_data, export_data)
            elif errors:
                # Join error messages into a single string for Excel export.
                unmigrated.append({"ecu": record.ecu, "errors": ", ".join(errors)})
        except Exception as e:
            # Capture any unexpected errors in the worker thread.
            unmigrated.append({"ecu": record.ecu, "errors": f"{e.__class__.__name__}: {str(e)}"})
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
                    unmigrated.append({"ecu": record.ecu, "errors": ", ".join(errors)})
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
            print(f"Batch insert with returning() failed: {e.__class__.__name__}: {str(e)}")
            # Fallback for databases (like MySQL) that don't support returning():
            Certificate.insert_many(certificate_data_list).execute()
            new_ids = [None] * len(certificate_data_list)
        for idx, new_cert_id in enumerate(new_ids):
            export_data_list[idx]["new_certificate_id"] = new_cert_id

            # --- Update the corresponding Vehicle record with the new certificate id ---
            vehicle_id = export_data_list[idx].get("vehicle_id")
            if vehicle_id:
                Vehicle.update({Vehicle.certificate_id: new_cert_id}).where(Vehicle.id == vehicle_id).execute()

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