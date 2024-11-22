from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
)
from source_db import source_db
from dest_db import dest_db

# Source Model
class CustomerMaster(Model):
    id = IntegerField(primary_key=True)
    company = TextField()
    email = TextField()
    o_address = TextField()
    o_contactphone = TextField()
    add_date = TimestampField()
    user_id = IntegerField()
    company_local = TextField()

    class Meta:
        database = source_db
        table_name = "customer_master"


# Destination Model
class Customer(Model):
    id = BigIntegerField(primary_key=True)
    email = CharField(max_length=255)
    name = CharField(max_length=255)
    address = TextField()
    contact_number = CharField(max_length=255)
    user_id = BigIntegerField()

    class Meta:
        database = dest_db
        table_name = "customers"

class CustomerDealer(Model):
    customer_id=BigIntegerField(),
    dealer_id=BigIntegerField(),

    class Meta:
        database = dest_db
        table_name = "customer_dealer"