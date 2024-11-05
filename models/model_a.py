from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    BooleanField,
)
from source_db import source_db
from dest_db import dest_db


# Source Model
class User(Model):
    id = BigIntegerField(primary_key=True)
    username = TextField(null=True)
    password = TextField(null=True)
    full_name = (
        TextField()
    )  # Assuming 'full_name' maps to 'name' in the destination table
    company = TextField(null=True)
    activstate = IntegerField(default=1)
    email = CharField(max_length=255, unique=True)
    mobile = TextField(null=True)
    usertype = TextField(default="Installer")
    country = TextField(default="India")  # Adjust based on your requirements
    add_date = TimestampField()
    added_by_user_id = IntegerField()
    forgotpassword = IntegerField(default=0)
    access_privilege_array = TextField()
    company_local = TextField()
    full_name_local = TextField()
    cms_support_email = CharField(max_length=100, null=True)
    cms_support_mobileno = CharField(max_length=20, null=True)
    is_cms_admin = BooleanField(null=True)

    class Meta:
        database = source_db
        table_name = "users"  # Your source table name


# Destination Model
class DestinationUser(Model):
    id = BigIntegerField(
        primary_key=True
    )  # Assuming this is auto-incremented by the DB
    name = CharField(max_length=255, null=False)
    email = CharField(max_length=255, null=False, unique=True)
    parent_id = BigIntegerField(null=True)
    email_verified_at = TimestampField(null=True)
    password = CharField(max_length=255, null=False)
    username = CharField(max_length=255, null=True, unique=True)
    company = CharField(max_length=255, null=True)
    status = CharField(
        max_length=10, default="active", choices=["active", "blocked"], null=False
    )  # Use CharField with choices
    phone = CharField(max_length=255, null=True)
    mobile = CharField(max_length=255, null=True)
    emirates = CharField(max_length=255, null=True)
    timezone = CharField(max_length=255, null=True)
    country = CharField(max_length=255, null=True)
    state = CharField(max_length=255, null=True)
    remember_token = CharField(max_length=100, null=True)
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)

    class Meta:
        database = dest_db
        table_name = "users"  # Destination table name


def migrate_users():
    for record in User.select():
        # Insert into the destination table
        DestinationUser.insert(
            {
                "name": record.full_name,  # Assuming full_name maps to name
                "email": record.email,
                "parent_id": None,  # Set as needed
                "email_verified_at": None,  # Set as needed
                "password": record.password,
                "username": record.username,
                "company": record.company,
                "status": "active",  # Default status
                "phone": None,  # Set as needed
                "mobile": record.mobile,
                "emirates": None,  # Set as needed
                "timezone": None,  # Set as needed
                "country": record.country,
                "state": None,  # Set as needed
                "remember_token": None,  # Set as needed
                "created_at": record.add_date,
                "updated_at": record.add_date,
            }
        ).execute()  # Execute the insert statement

        print(f"Migrated User: {record.full_name}, {record.email}")
