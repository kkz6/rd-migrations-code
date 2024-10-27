# models/model_a.py

from peewee import Model, CharField
from source_db import source_db
from dest_db import dest_db

class ModelA(Model):
    name = CharField()
    email = CharField()

    class Meta:
        database = source_db
        table_name = 'model_a_table'  # Replace with your actual source table name

def migrate_model_a():
    dest_db.create_tables([ModelA], safe=True)
    
    for record in ModelA.select():
        dest_db.create(name=record.name, email=record.email)
        print(f'Migrated ModelA: {record.name}, {record.email}')
