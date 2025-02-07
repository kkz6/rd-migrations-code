import pymysql
pymysql.install_as_MySQLdb()

from source_db import source_db
from dest_db import dest_db

import questionary

# Import migration functions from your model modules.
# Uncomment the ones you plan to use.
from models.users_model import run_migration as run_users_migration
# from models.vehicles_model import run_migration as run_vehicles_migration
from models.devices_model import run_migration as run_devices_migration
# from models.certificates_model import run_migration as run_certificates_migration


def migrate_data():
    # Ask the user which migration to run.
    migration_choice = questionary.select(
        "Select which migration you want to run:",
        choices=[
            "Users",
            "Devices"
        ]
    ).ask()

    try:
        if migration_choice == "Users":
            run_users_migration()
        elif migration_choice == "Devices":
            # Uncomment if devices migration is available.
            run_devices_migration()
        elif migration_choice == "Certificates":
            # Uncomment if certificates migration is available.
            # run_certificates_migration()
            print("Certificates migration is not enabled yet.")
        elif migration_choice == "Vehicles":
            # Uncomment if vehicles migration is available.
            # run_vehicles_migration()
            print("Vehicles migration is not enabled yet.")
        elif migration_choice == "All":
            run_users_migration()
            # Uncomment the following lines as the migrations become available.
            # run_devices_migration()
            # run_certificates_migration()
            # run_vehicles_migration()
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
