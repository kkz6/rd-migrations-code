import re
from datetime import datetime
import json
import questionary
from openpyxl import Workbook
from typing import List
from threading import Lock, Thread
from queue import Queue
from peewee import *
from models.certificates_model import CertificateRecord, Certificate
from models.devices_model import Device
from models.customers_model import Customer
from models.technicians_model import Technician
from models.vehicles_model import Vehicle
from models.users_model import DestinationUser
from tqdm import *

# Global Constants / Configurations
CUSTOMER_MAPPING_FILE = "customer_mappings.json"
USER_MAPPING_FILE = "user_mappings.json"
DEVICE_MAPPING_FILE = "devices_mappings.json"
TECHNICIAN_MAPPING_FILE = "technicians_mapping.json"  # Global constant for technician mapping file
CERTIFICATES_MAPPING_FILE = "certificates_mappings.json"
EXCEL_FILE_NAME = "certificate_migration_report.xlsx"
THREAD_COUNT = 5
TECHNICIAN_MAPPING_LOCK = Lock()


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

def load_mappings(file_path):
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def save_mappings(file_path, mappings):
    with open(file_path, "w") as file:
        json.dump(mappings, file, indent=4)

def parse_speed(speed_str):
    if not speed_str:
        return 0
    match = re.search(r'\d+', speed_str)
    if match:
        return int(match.group())
    return 0

def get_or_create_technician_for_certificate(calibrater_user_id, calibrater_technician_id, installer_user_id, installer_technician_id, mappings):
    technician_mappings = mappings.get("technician", {})

    # --- Calibration Technician Logic ---
    calibration_technician = None
    try:
        calib_tech_id = int(calibrater_technician_id) if calibrater_technician_id else 0
        if calib_tech_id != 0:
            with TECHNICIAN_MAPPING_LOCK:
                # Iterate over a copy of the items to avoid concurrent modification issues.
                for new_tech_id, tech_mapping in list(technician_mappings.items()):
                    if tech_mapping.get("old_technician_id") == calib_tech_id:
                        print(f"Calibration technician already exists for old_technician_id {calib_tech_id}. Using Technician ID {new_tech_id}.")
                        calibration_technician = Technician.get_by_id(int(new_tech_id))
                        break
        if not calibration_technician:
            user_mappings = mappings.get("user", {})
            new_user_id = None
            for user_id, user_map in user_mappings.items():
                if user_map.get("old_user_id") == int(calibrater_user_id):
                    new_user_id = int(user_id)
                    break
            if not new_user_id:
                raise Exception(f"No mapping found for calibrater_user_id {calibrater_user_id} in user mappings.")
            calibrater_user = DestinationUser.get_by_id(new_user_id)
            calibration_technician = Technician.get_or_none(Technician.email == calibrater_user.email)
            if not calibration_technician:
                if calibrater_user.parent_id:
                    tech_user_id = calibrater_user.parent_id
                    tech_created_by = calibrater_user.id
                else:
                    tech_user_id = calibrater_user.id
                    tech_created_by = calibrater_user.id

                calibration_technician = Technician.create(
                    name=calibrater_user.name,
                    email=calibrater_user.email,
                    phone=calibrater_user.phone or "0000000000",
                    user_id=tech_user_id,
                    created_by=tech_created_by,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            with TECHNICIAN_MAPPING_LOCK:
                technician_mappings[str(calibration_technician.id)] = {
                    "old_technician_id": calib_tech_id,
                    "user_id": new_user_id,
                }
                save_mappings(TECHNICIAN_MAPPING_FILE, technician_mappings)
    except Exception as e:
        print(f"Error in creating or fetching calibration technician: {e}")

    # --- Installation Technician Logic ---
    installation_technician = None
    try:
        inst_tech_id = int(installer_technician_id) if installer_technician_id else 0
        if inst_tech_id != 0:
            with TECHNICIAN_MAPPING_LOCK:
                for new_tech_id, tech_mapping in list(technician_mappings.items()):
                    if tech_mapping.get("old_technician_id") == inst_tech_id:
                        print(f"Installation technician already exists for old_technician_id {inst_tech_id}. Using Technician ID {new_tech_id}.")
                        installation_technician = Technician.get_by_id(int(new_tech_id))
                        break
        if not installation_technician:
            user_mappings = mappings.get("user", {})
            new_user_id = None
            for user_id, user_map in user_mappings.items():
                if user_map.get("old_user_id") == int(installer_user_id):
                    new_user_id = int(user_id)
                    break
            if not new_user_id:
                raise Exception(f"No mapping found for installer_user_id {installer_user_id} in user mappings.")
            installer_user = DestinationUser.get_by_id(new_user_id)
            installation_technician = Technician.get_or_none(Technician.email == installer_user.email)
            if not installation_technician:
                if installer_user.parent_id:
                    tech_user_id = installer_user.parent_id
                    tech_created_by = installer_user.id
                else:
                    tech_user_id = installer_user.id
                    tech_created_by = installer_user.id

                installation_technician = Technician.create(
                    name=installer_user.name,
                    email=installer_user.email,
                    phone=installer_user.phone or "0000000000",
                    user_id=tech_user_id,
                    created_by=tech_created_by,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                )
            with TECHNICIAN_MAPPING_LOCK:
                technician_mappings[str(installation_technician.id)] = {
                    "old_technician_id": inst_tech_id,
                    "user_id": new_user_id,
                }
                save_mappings(TECHNICIAN_MAPPING_FILE, technician_mappings)
    except Exception as e:
        print(f"Error in creating or fetching installation technician: {e}")

    return calibration_technician, installation_technician

def list_unmigrated_certificates(migrated_ids):
    migrated_ids = list(migrated_ids)
    migrated_ids = list(map(int, migrated_ids)) if migrated_ids else []
    if migrated_ids:
        return CertificateRecord.select().where(CertificateRecord.id.not_in(migrated_ids))
    else:
        return CertificateRecord.select()

def migrate_certificate(record, mappings, certificate_mappings, batch_mode=False):
    # Removed verbose "Starting migration for Certificate ID ..." print
    errors = []
    device = Device.get_or_none(Device.ecu_number == record.ecu)
    if not device:
        errors.append("Device not found")

    if record.customer_id is None:
        customer = None
    else:
        mapped_customer_id = mappings.get("customer", {}).get(str(record.customer_id))
        customer = Customer.get_or_none(Customer.id == mapped_customer_id) if mapped_customer_id else None
        if not customer:
            errors.append("Customer not found")

    try:
        calibration_technician, installation_technician = get_or_create_technician_for_certificate(
            calibrater_user_id=record.caliberater_user_id,
            calibrater_technician_id=record.caliberater_technician_id,
            installer_user_id=record.installer_user_id,
            installer_technician_id=record.installer_technician_id,
            mappings=mappings
        )
    except Exception as e:
        errors.append(f"Technician creation error: {e.__class__.__name__}: {str(e)}")
    
    if not calibration_technician:
        errors.append("Calibration technician not found or could not be created")
    if not installation_technician:
        errors.append("Installation technician not found or could not be created")

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
            try:
                vehicle = Vehicle.get(vehicle_chassis_no=record.vehicle_chassis)
            except Exception as e:
                errors.append(f"Vehicle creation failed: {e.__class__.__name__}: {str(e)}")
    else:
        vehicle = Vehicle.get_or_none(vehicle_chassis_no=record.vehicle_chassis)

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

    max_renewal = CertificateRecord.select(fn.MAX(CertificateRecord.renewal_count))\
                    .where(CertificateRecord.ecu == record.ecu).scalar() or 0
    if record.renewal_count < max_renewal:
        status = "renewed"
    else:
        status = "active"
    status = "nullified" if record.serialno is None else status
    status = "cancelled" if record.date_cancelation else status
    status = "blocked" if record.activstate == 0 else status

    certificate_data = {
        "serial_number": record.serialno,
        "status": status,
        "device_id": device.id,
        "installation_date": record.date_installation,
        "calibration_date": record.date_calibrate,
        "expiry_date": record.date_expiry,
        "cancellation_date": record.date_cancelation,
        "cancelled": (record.date_cancelation is not None),
        "calibrated_by_id": calibration_technician.id if calibration_technician else None,
        "installed_by_id": installation_technician.id if installation_technician else None,
        "installed_for_id": customer.id if customer else None,
        "vehicle_id": vehicle.id if vehicle else None,
        "km_reading": record.kilometer or 0,
        "speed_limit": parse_speed(record.speed) if record.speed else 0,
        "print_count": record.print_count,
        "renewal_count": record.renewal_count,
        "description": record.description,
        "dealer_id": dealer_id_val,
        "user_id": user_id_val
    }
    export_data = {
        "ecu": record.ecu,
        "old_certificate_id": record.id,
        "new_certificate_id": None,  # To be updated after insertion.
        "certificate_serial": certificate_data.get("serial_number"),
        "status": certificate_data.get("status"),
        "old_calibration_technician_id": record.caliberater_technician_id,
        "new_calibration_technician_id": calibration_technician.id if calibration_technician else None,
        "old_installation_technician_id": record.installer_technician_id,
        "new_installation_technician_id": installation_technician.id if installation_technician else None,
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
            if vehicle:
                vehicle.certificate_id = new_cert.id
                vehicle.save()
            if str(record.id) not in certificate_mappings:
                certificate_mappings[str(record.id)] = {
                    "old_certificate_id": record.id,
                    "device_id": device.id,
                    "customer_id": customer.id if customer else None,
                    "technician_id": installation_technician.id if installation_technician else None,
                    "vehicle_id": vehicle.id if vehicle else None,
                    "dealer_id": dealer_id_val,
                }
            return export_data, None
        except Exception as e:
            errors.append(f"{e.__class__.__name__}: {str(e)}")
            print(f"Error migrating Certificate ID {record.id}: {errors}")
            return None, errors
    else:
        # In batch mode, we return the prepared data.
        return (certificate_data, export_data), None

def worker_batch(queue, batch_results, unmigrated, mappings, certificate_mappings, progress_bar):
    while not queue.empty():
        record = queue.get()
        try:
            result, errors = migrate_certificate(record, mappings, certificate_mappings, batch_mode=True)
            if result:
                batch_results.append(result)  # result is (certificate_data, export_data)
            elif errors:
                unmigrated.append({"ecu": record.ecu, "errors": ", ".join(errors)})
        except Exception as e:
            unmigrated.append({"ecu": record.ecu, "errors": f"{e.__class__.__name__}: {str(e)}"})
        finally:
            progress_bar.update(1)
            queue.task_done()

# --- Migration Modes ---

def run_one_by_one(mappings, certificate_mappings):
    print("Starting One-by-One Migration")
    migrated = []
    unmigrated = []
    records = list_unmigrated_certificates(list(certificate_mappings.keys()))
    total_records = records.count()
    print(f"Total unmigrated certificates: {total_records}")
    migrated_count = 0
    failed_count = 0
    processed_count = 0

    try:
        for record in records:
            print(f"\nProcessing Certificate {processed_count + 1} of {total_records} (ECU: {record.ecu})")
            answer = questionary.select(
                f"Choose action for Certificate with ECU {record.ecu}:",
                choices=[
                    "Migrate Certificate",
                    "Skip Certificate",
                    "Exit Migration"
                ],
            ).ask()

            if answer == "Migrate Certificate":
                export_data, errors = migrate_certificate(record, mappings, certificate_mappings, batch_mode=False)
                if export_data:
                    migrated.append(export_data)
                    migrated_count += 1
                    print("Certificate migrated successfully!")
                else:
                    unmigrated.append({"ecu": record.ecu, "errors": ", ".join(errors)})
                    failed_count += 1
                    print(f"Migration failed with errors: {errors}")
            elif answer == "Skip Certificate":
                print(f"Skipping Certificate for ECU {record.ecu}.")
            elif answer == "Exit Migration":
                print("Exiting migration as per user request.")
                break
            processed_count += 1
            print(f"Progress: {processed_count}/{total_records} processed | Migrated: {migrated_count} | Failed: {failed_count}")
    except KeyboardInterrupt:
        print("\nMigration interrupted by user. Saving progress...")
    finally:
        save_mappings(CERTIFICATES_MAPPING_FILE, certificate_mappings)
        save_to_excel(migrated, unmigrated)

def run_fully_automated(mappings, certificate_mappings):
    print("Starting Fully Automated Migration (Batch Insert Mode)")
    batch_results = []  # Holds tuples: (certificate_data, export_data)
    unmigrated = []
    records = list_unmigrated_certificates(list(certificate_mappings.keys()))
    total_records = records.count()
    print(f"Total unmigrated certificates: {total_records}")

    queue = Queue()
    for record in records:
        queue.put(record)

    # Initialize progress bar with total count.
    progress_bar = tqdm(total=total_records, desc="Migrating Certificates", ncols=100, colour="green")
    
    threads = []
    for _ in range(THREAD_COUNT):
        thread = Thread(target=worker_batch, args=(queue, batch_results, unmigrated, mappings, certificate_mappings, progress_bar))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    progress_bar.close()

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
            Certificate.insert_many(certificate_data_list).execute()
            new_ids = [None] * len(certificate_data_list)
        for idx, new_cert_id in enumerate(new_ids):
            export_data_list[idx]["new_certificate_id"] = new_cert_id

            # Update the corresponding Vehicle record with the new certificate id.
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

    mode = questionary.select(
        "Choose migration mode:",
        choices=[
            "Run Fully Automated",
            "Migrate Certificates One by One",
        ],
    ).ask()

    try:
        if mode == "Run Fully Automated":
            run_fully_automated(mappings, certificate_mappings)
        elif mode == "Migrate Certificates One by One":
            run_one_by_one(mappings, certificate_mappings)
        else:
            print("Invalid choice. Exiting...")
    except KeyboardInterrupt:
        print("\nMigration process interrupted by user. Exiting...")