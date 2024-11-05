# main.py

from source_db import source_db
from dest_db import dest_db
from models.users_model import migrate_users
from models.technicians_model import migrate_technicians
from models.customers_model import migrate_customers
from models.sales_people import migrate_sales_people
from models.vehicles_model import migrate_vehicles
from models.devices_model import run_migration, migrate_devices
from models.certificates_model import run_migration

def migrate_data():
    # Connect to the source and destination databases
    source_db.connect()
    dest_db.connect()

    try:
        # migrate_users()
        # migrate_technicians()
        # migrate_customers()
        # migrate_sales_people()
        # migrate_vehicles()
        run_migration()

    
    except Exception as e:
        print(f'Error occurred: {e}')
    
    finally:
        # Close the database connections
        source_db.close()
        dest_db.close()

if __name__ == '__main__':
    migrate_data()
