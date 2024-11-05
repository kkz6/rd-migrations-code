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
from peewee import IntegrityError


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
    email = CharField(max_length=255, null=False)
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
    ignored_rows = []  # List to keep track of ignored rows or errors
    total_records = User.select().count()  # Get total number of records for reporting

    # Establish connections if not already connected
    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        with dest_db.atomic():  # Begin transaction
            for record in User.select():
                try:
                    # Attempt to insert into the destination table
                    DestinationUser.insert(
                        {
                            "name": record.full_name,
                            "email": record.email,
                            "parent_id": record.added_by_user_id,
                            "email_verified_at": None,
                            "password": record.password,
                            "username": record.username,
                            "company": record.company,
                            "status": "active",
                            "phone": None,
                            "mobile": record.mobile,
                            "emirates": None,
                            "timezone": None,
                            "country": record.country,
                            "state": None,
                            "remember_token": None,
                            "created_at": record.add_date,
                            "updated_at": record.add_date,
                        }
                    ).execute()  # Execute the insert statement

                    print(f"Migrated User: {record.full_name}, {record.email}")

                except IntegrityError as e:
                    # Handle duplicate entries specifically
                    if "Duplicate entry" in str(e):
                        # Attempt to update the existing record instead
                        try:
                            print(
                                f"Duplicate entry for {record.email}. Attempting to update..."
                            )
                            DestinationUser.update(
                                {
                                    "name": record.full_name,
                                    "parent_id": record.added_by_user_id,
                                    "password": record.password,
                                    "username": record.username,
                                    "company": record.company,
                                    "status": "active",
                                    "phone": None,
                                    "mobile": record.mobile,
                                    "country": record.country,
                                    "updated_at": record.add_date,
                                }
                            ).where(
                                DestinationUser.email == record.email
                            ).execute()  # Execute the update statement
                            print(
                                f"Updated existing user: {record.full_name}, {record.email}"
                            )

                        except Exception as update_error:
                            print(
                                f"Error updating {record.full_name} ({record.email}): {update_error}"
                            )
                            ignored_rows.append((record, str(update_error)))

                    else:
                        print(
                            f"IntegrityError for {record.full_name} ({record.email}): {e}"
                        )
                        ignored_rows.append((record, str(e)))

                except Exception as e:
                    print(f"Error migrating {record.full_name} ({record.email}): {e}")
                    ignored_rows.append((record, str(e)))

    finally:
        # Close connections
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Summary of migration results
    print(
        f"Migration completed. Total records: {total_records}, Ignored rows: {len(ignored_rows)}"
    )

    if ignored_rows:
        print("Ignored rows:")
        for user, reason in ignored_rows:
            print(f"- {user.full_name} ({user.email}): {reason}")


# Call the function to start migration
migrate_users()
