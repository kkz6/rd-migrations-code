# main.py

from source_db import source_db
from dest_db import dest_db
# from models.users_model import run_migration
# from models.technicians_model import run_migration
# from models.customers_model import run_migration
# from models.sales_people import run_migration
# from models.vehicles_model import run_migration
# from models.devices_model import run_migration
from models.certificates_model import run_migration,migrate_certificates


def migrate_data():
    # Connect to the source and destination databases
    source_db.connect()
    dest_db.connect()

    try:
        run_migration()

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        # Close the database connections
        source_db.close()
        dest_db.close()


if __name__ == "__main__":
    migrate_data()
