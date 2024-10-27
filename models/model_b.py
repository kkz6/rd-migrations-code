# models/model_b.py

from peewee import Model, CharField
from source_db import source_db
from dest_db import dest_db

class ModelB(Model):
    title = CharField()
    description = CharField()

    class Meta:
        database = source_db
        table_name = 'model_b_table'  # Replace with your actual source table name

def migrate_model_b():
    dest_db.create_tables([ModelB], safe=True)
    
    for record in ModelB.select():
        dest_db.create(title=record.title, description=record.description)
        print(f'Migrated ModelB: {record.title}, {record.description}')
