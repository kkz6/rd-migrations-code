import json
import questionary
from openpyxl import Workbook
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TextField,
    IntegerField,
    CompositeKey,
    DateTimeField,
    IntegrityError,
)
from source_db import source_db
from dest_db import dest_db
from models.users_model import DestinationUser

# Constants
CUSTOMER_MAPPING_FILE = "customer_mappings.json"
USER_MAPPING_FILE = "user_mappings.json"
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"  # Replace with admin email
EXCEL_FILE_NAME = "customer_migration_report.xlsx"
COUNTRY_ID = 231  # Default country ID


# Source Model
class CustomerMaster(Model):
    id = IntegerField(primary_key=True)
    company = TextField()
    email = TextField()
    o_address = TextField()
    o_contactphone = TextField()
    add_date = DateTimeField()
    user_id = IntegerField()
    company_local = TextField()

    class Meta:
        database = source_db
        table_name = "customer_master"


# Destination Models
class Customer(Model):
    id = BigIntegerField(primary_key=True)
    email = CharField(max_length=255)
    name = CharField(max_length=255)
    name_local = CharField(max_length=255, null=True)
    address = TextField()
    contact_number = CharField(max_length=255)
    user_id = BigIntegerField()
    country_id = IntegerField()
    created_at = DateTimeField()
    updated_at = DateTimeField()

    class Meta:
        database = dest_db
        table_name = "customers"


class CustomerDealer(Model):
    customer_id = BigIntegerField()
    dealer_id = BigIntegerField()

    class Meta:
        database = dest_db
        table_name = "customer_dealer"
        primary_key = CompositeKey("customer_id", "dealer_id")


# Helper Functions
def load_json_mapping(file_path):
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except FileNotFoundError:
        return {}


def save_json_mapping(file_path, mapping):
    with open(file_path, "w") as file:
        json.dump(mapping, file, indent=4)


def load_user_mappings():
    return load_json_mapping(USER_MAPPING_FILE)


def load_customer_mappings():
    return load_json_mapping(CUSTOMER_MAPPING_FILE)


def save_customer_mappings(customer_mappings):
    save_json_mapping(CUSTOMER_MAPPING_FILE, customer_mappings)


def get_new_user_id_from_mapping(old_user_id, user_mappings):
    return next((int(new_user_id) for new_user_id, mapping in user_mappings.items()
                 if mapping.get("old_user_id") == old_user_id), None)


def get_default_user():
    try:
        return DestinationUser.get(DestinationUser.email == DEFAULT_USER_EMAIL)
    except DestinationUser.DoesNotExist:
        raise Exception(f"Default user '{DEFAULT_USER_EMAIL}' not found.")


def generate_excel_report(migrated_data, unmigrated_data):
    workbook = Workbook()
    migrated_sheet = workbook.active
    migrated_sheet.title = "Migrated Customers"
    unmigrated_sheet = workbook.create_sheet(title="Unmigrated Customers")

    # Headers for Migrated Customers
    migrated_sheet.append(["Customer ID", "Customer Name", "Customer Name Local", "Email", "Address",
                           "Contact Number", "New Dealer ID", "Old User ID"])

    for record in migrated_data:
        migrated_sheet.append([record[key] for key in record])

    # Headers for Unmigrated Customers
    unmigrated_sheet.append(["Customer ID", "Customer Name", "Email", "Address", "Contact Number", "Reason"])
    for record in unmigrated_data:
        unmigrated_sheet.append([record[key] for key in record])

    workbook.save(EXCEL_FILE_NAME)
    print(f"Migration report saved as {EXCEL_FILE_NAME}")


def batch_migrate_customers():
    customer_mappings = load_customer_mappings()
    user_mappings = load_user_mappings()
    migrated_data = []
    unmigrated_data = []

    customers = CustomerMaster.select()
    customer_data = []
    customer_dealer_data = []

    for customer in customers:
        if str(customer.id) in customer_mappings:
            continue

        new_dealer_id = get_new_user_id_from_mapping(customer.user_id, user_mappings)
        if not new_dealer_id:
            unmigrated_data.append({
                "id": customer.id, "name": customer.company, "email": customer.email,
                "address": customer.o_address, "contact_number": customer.o_contactphone,
                "reason": "No mapped dealer found"
            })
            continue

        customer_data.append({
            "id": customer.id, "email": customer.email, "name": customer.company,
            "name_local": customer.company_local, "address": customer.o_address,
            "contact_number": customer.o_contactphone, "user_id": new_dealer_id,
            "country_id": COUNTRY_ID, "created_at": customer.add_date,
            "updated_at": customer.add_date,
        })

        customer_dealer_data.append({"customer_id": customer.id, "dealer_id": new_dealer_id})
        customer_mappings[str(customer.id)] = {"dealer_id": new_dealer_id}

        migrated_data.append({
            "customer_id": customer.id, "customer_name": customer.company,
            "customer_name_local": customer.company_local, "email": customer.email,
            "address": customer.o_address, "contact_number": customer.o_contactphone,
            "new_dealer_id": new_dealer_id, "old_user_id": customer.user_id,
        })

    with dest_db.atomic():
        if customer_data:
            Customer.insert_many(customer_data).execute()
        if customer_dealer_data:
            CustomerDealer.insert_many(customer_dealer_data).execute()

    save_customer_mappings(customer_mappings)
    generate_excel_report(migrated_data, unmigrated_data)


def interactive_migrate_customers():
    user_mappings = load_user_mappings()
    customer_mappings = load_customer_mappings()
    migrated_data = []
    unmigrated_data = []

    for customer in CustomerMaster.select():
        if str(customer.id) in customer_mappings:
            continue

        new_dealer_id = get_new_user_id_from_mapping(customer.user_id, user_mappings)
        if not new_dealer_id:
            unmigrated_data.append({
                "id": customer.id, "name": customer.company, "email": customer.email,
                "address": customer.o_address, "contact_number": customer.o_contactphone,
                "reason": "No mapped dealer found"
            })
            continue

        proceed = questionary.confirm(
            f"Migrate Customer {customer.company} (ID: {customer.id})?").ask()
        if not proceed:
            unmigrated_data.append({
                "id": customer.id, "name": customer.company, "email": customer.email,
                "address": customer.o_address, "contact_number": customer.o_contactphone,
                "reason": "Skipped by user"
            })
            continue

        try:
            Customer.create(
                id=customer.id, email=customer.email, name=customer.company,
                name_local=customer.company_local, address=customer.o_address,
                contact_number=customer.o_contactphone, user_id=new_dealer_id,
                country_id=COUNTRY_ID, created_at=customer.add_date,
                updated_at=customer.add_date,
            )
            CustomerDealer.create(customer_id=customer.id, dealer_id=new_dealer_id)
            customer_mappings[str(customer.id)] = {"dealer_id": new_dealer_id}
            migrated_data.append({
                "customer_id": customer.id, "customer_name": customer.company,
                "customer_name_local": customer.company_local, "email": customer.email,
                "address": customer.o_address, "contact_number": customer.o_contactphone,
                "new_dealer_id": new_dealer_id, "old_user_id": customer.user_id,
            })
        except IntegrityError as e:
            unmigrated_data.append({
                "id": customer.id, "name": customer.company, "email": customer.email,
                "address": customer.o_address, "contact_number": customer.o_contactphone,
                "reason": str(e)
            })

    save_customer_mappings(customer_mappings)
    generate_excel_report(migrated_data, unmigrated_data)


def run_migration():
    mode = questionary.select(
        "Select migration mode:",
        choices=["Batch Migration", "Interactive Migration"],
    ).ask()

    if mode == "Batch Migration":
        batch_migrate_customers()
    elif mode == "Interactive Migration":
        interactive_migrate_customers()


if __name__ == "__main__":
    run_migration()