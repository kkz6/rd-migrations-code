from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    BooleanField,
    DoesNotExist,
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError


# Source Model
class User(Model):
    id = BigIntegerField(primary_key=True)
    username = TextField(null=True)
    password = TextField(null=True)
    full_name = TextField()
    company = TextField(null=True)
    activstate = IntegerField(default=1)
    email = CharField(max_length=255, unique=True)
    mobile = TextField(null=True)
    usertype = TextField(default="Installer")
    country = TextField(default="India")
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
        table_name = "users"


# Destination Model
class DestinationUser(Model):
    id = BigIntegerField(primary_key=True)  # Use the ID from the source
    name = CharField(max_length=255, null=False)
    email = CharField(max_length=255, null=False)
    parent_id = BigIntegerField(null=True)
    email_verified_at = TimestampField(null=True)
    password = CharField(max_length=255, null=False)
    username = CharField(max_length=255, null=True, unique=True)
    company = CharField(max_length=255, null=True)
    status = CharField(
        max_length=10, default="active", choices=["active", "blocked"], null=False
    )
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
        table_name = "users"


def clean_destination_table():
    """
    Clean up the destination table before migration.
    Returns the number of records deleted.
    """
    try:
        if dest_db.is_closed():
            dest_db.connect()

        with dest_db.atomic():
            # Get the count before deletion
            count_before = DestinationUser.select().count()

            # Option 1: Hard delete (completely remove records)
            DestinationUser.delete().execute()

            count_after = DestinationUser.select().count()
            deleted_count = count_before - count_after

            print(f"Cleanup Summary:")
            print(f"Records before cleanup: {count_before}")
            print(f"Records after cleanup: {count_after}")
            print(f"Total records deleted: {deleted_count}")

            return deleted_count

    except Exception as e:
        print(f"Error during cleanup: {str(e)}")
        raise
    finally:
        if not dest_db.is_closed():
            dest_db.close()


def migrate_users():
    ignored_rows = []
    total_records = User.select().count()
    migrated_count = 0
    skipped_count = 0
    updated_count = 0

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
                            "id": record.id,  # Use the same ID from the source
                            "name": record.full_name,
                            "email": record.email,
                            "parent_id": record.added_by_user_id,
                            "email_verified_at": None,
                            "password": record.password,
                            "username": record.username,
                            "company": record.company,
                            "status": "active" if record.activstate == 1 else "blocked",
                            "phone": None,
                            "mobile": record.mobile,
                            "emirates": None,
                            "timezone": None,
                            "country": record.country,
                            "state": None,
                            "remember_token": None,
                        }
                    ).execute()

                    print(f"Migrated User: {record.full_name}, {record.email}")
                    migrated_count += 1

                except IntegrityError as e:
                    # Handle duplicate entries specifically
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Duplicate entry for {record.email}. Attempting to update..."
                            )
                            DestinationUser.update(
                                {
                                    "id": record.id,  # Use the same ID from the source
                                    "name": record.full_name,
                                    "parent_id": record.added_by_user_id,
                                    "password": record.password,
                                    "username": record.username,
                                    "company": record.company,
                                    "status": (
                                        "active"
                                        if record.activstate == 1
                                        else "blocked"
                                    ),
                                    "phone": None,
                                    "mobile": record.mobile,
                                    "country": record.country,
                                    "updated_at": record.add_date,
                                }
                            ).where(DestinationUser.email == record.email).execute()
                            print(
                                f"Updated existing user: {record.full_name}, {record.email}"
                            )
                            updated_count += 1

                        except Exception as update_error:
                            print(
                                f"Error updating {record.full_name} ({record.email}): {update_error}"
                            )
                            ignored_rows.append((record, str(update_error)))
                            skipped_count += 1

                    else:
                        print(
                            f"IntegrityError for {record.full_name} ({record.email}): {e}"
                        )
                        ignored_rows.append((record, str(e)))
                        skipped_count += 1

                except Exception as e:
                    print(f"Error migrating {record.full_name} ({record.email}): {e}")
                    ignored_rows.append((record, str(e)))
                    skipped_count += 1

    finally:
        # Close connections
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Summary of migration results
    print(f"\nMigration Summary:")
    print(f"Total records processed: {total_records}")
    print(f"Successfully migrated (new): {migrated_count}")
    print(f"Successfully updated: {updated_count}")
    print(f"Skipped/Failed: {skipped_count}")
    print(
        f"Total success rate: {((migrated_count + updated_count) / total_records * 100):.2f}%"
    )

    if ignored_rows:
        print("\nDetailed error log:")
        for user, reason in ignored_rows:
            print(f"- {user.full_name} ({user.email}): {reason}")


def run_migration():
    """Main function to run the cleanup and migration"""
    try:
        # Ask for confirmation before cleanup
        response = input(
            "This will delete all existing records in the destination users table. Are you sure? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return

        # Step 1: Clean the destination table
        print("\nStep 1: Cleaning destination table...")
        clean_destination_table()

        # Step 2: Perform the migration
        print("\nStep 2: Starting migration...")
        migrate_users()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
