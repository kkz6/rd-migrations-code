import pymysql
pymysql.install_as_MySQLdb()

from source_db import source_db
from dest_db import dest_db

import questionary

# Import migration functions from your model modules.
# Uncomment the ones you plan to use.
from models.users_model import run_migration as run_users_migration
from models.technicians_model import run_migration as run_technician_migration
from models.devices_model import run_migration as run_devices_migration
from models.customers_model import run_migration as run_customer_migration
from models.certificate_migration import run_migration as run_certificates_migration


def migrate_data():
    # Ask the user which migration to run.
    migration_choice = questionary.select(
        "Select which migration you want to run:",
        choices=[
            "Users",
            "Devices",
            "Technicians",
            "Customers",
            "Certificates"
        ]
    ).ask()

    try:
        if migration_choice == "Users":
            run_users_migration()
        elif migration_choice == "Devices":
            run_devices_migration()
        elif migration_choice == "Technicians":
            run_technician_migration()
        elif migration_choice == "Customers":
            run_customer_migration()
        elif migration_choice == "Vehicles":
            print("Vehicles migration is not enabled yet.")
        elif migration_choice == "Certificates":
            run_certificates_migration()
        else:
            print("No valid migration option selected.")

    except Exception as e:
        print(f"Error occurred during migration: {e}")

    finally:
        # Always ensure the database connections are closed.
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()


if __name__ == "__main__":
    migrate_data()
