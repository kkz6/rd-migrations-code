from source_db import source_db
from dest_db import dest_db
from models.users_model import run_migration as run_users_migration
from models.technicians_model import run_migration as run_technicians_migration
from models.customers_model import run_migration as run_customers_migration
from models.sales_people import run_migration as run_salespeople_migration
from models.vehicles_model import run_migration as run_vehicles_migration
from models.devices_model import run_migration as run_devices_migration
from models.certificates_model import run_migration as run_certificates_migration
from id_mapping import dealer_id_mapper,user_id_mapper


def migrate_data():
    # Connect to the source and destination databases
    source_db.connect()
    dest_db.connect()

    try:
        user_id_mapper.clear_mapping()
        dealer_id_mapper.clear_mapping()
        run_users_migration()
        run_technicians_migration()
        run_customers_migration()
        run_salespeople_migration()
        run_vehicles_migration()
        run_devices_migration()
        run_certificates_migration()
        

    except Exception as e:
        print(f"Error occurred: {e}")

    finally:
        # Close the database connections
        source_db.close()
        dest_db.close()


if __name__ == "__main__":
    migrate_data()
