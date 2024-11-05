# models/model_c.py

from peewee import Model, CharField, ForeignKeyField
from source_db import source_db
from dest_db import dest_db
from models.users_model import ModelA
from models.model_b import ModelB

class ModelC(Model):
    model_a = ForeignKeyField(ModelA, backref='model_c_entries')
    model_b = ForeignKeyField(ModelB, backref='model_c_entries')
    combined_data = CharField()

    class Meta:
        database = dest_db
        table_name = 'model_c_table'  # Replace with your actual destination table name

def migrate_model_c():
    dest_db.create_tables([ModelC], safe=True)
    
    for record_a in ModelA.select():
        for record_b in ModelB.select():
            # Example logic to combine data; adjust as needed
            combined = f"{record_a.name} - {record_b.title}"
            ModelC.create(model_a=record_a.id, model_b=record_b.id, combined_data=combined)
            print(f'Migrated ModelC: {combined}')
