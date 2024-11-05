# main.py

from source_db import source_db
from dest_db import dest_db
from models.model_a import User
from models.model_b import migrate_model_b

def migrate_data():
    # Connect to the source and destination databases
    source_db.connect()
    dest_db.connect()

    try:
        User()
        migrate_model_b()
    
    except Exception as e:
        print(f'Error occurred: {e}')
    
    finally:
        # Close the database connections
        source_db.close()
        dest_db.close()

if __name__ == '__main__':
    migrate_data()
