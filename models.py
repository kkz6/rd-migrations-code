# model.py

from peewee import Model, CharField

# Source model
class SourceModel(Model):
    name = CharField()
    email = CharField()

    class Meta:
        database = None  # To be set in the database connection files
        table_name = 'source_table'  # Replace with your actual table name


# Destination model
class DestModel(Model):
    name = CharField()
    email = CharField()

    class Meta:
        database = None  # To be set in the database connection files
        table_name = 'destination_table'  # Replace with your actual table name
