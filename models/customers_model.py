from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    ForeignKeyField,
    DoesNotExist,
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError
from models.users_model import DestinationUser
from id_mapping import user_id_mapper, customer_id_mapper


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


def clean_destination_table():
    """
    Clean up the destination table before migration.
    Returns the number of records deleted.
    """
    try:
        if dest_db.is_closed():
            dest_db.connect()

        with dest_db.atomic():
            count_before = Customer.select().count()
            Customer.delete().execute()
            count_after = Customer.select().count()
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


def check_user_exists(user_id):
    """Check if a user exists in the destination database"""
    try:
        return DestinationUser.get(DestinationUser.id == user_id)
    except DoesNotExist:
        return None


def migrate_customers():
    ignored_rows = []
    total_records = CustomerMaster.select().count()
    migrated_count = 0
    skipped_count = 0

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        with dest_db.atomic():
            for record in CustomerMaster.select():
                try:
                    # Check if user exists
                    user = check_user_exists(record.user_id)
                    if not user:
                        print(
                            f"Skipping {record.company} - user_id {record.user_id} not found"
                        )
                        ignored_rows.append((record, "User ID not found"))
                        skipped_count += 1
                        continue

                    mapped_user_id = user_id_mapper.get_dest_id(str(record.user_id))

                    # Attempt to insert into the destination table
                    new_customer = Customer.create(
                        name=record.company,
                        email=record.email,
                        address=record.o_address,
                        contact_number=record.o_contactphone,
                        user_id=mapped_user_id,
                    )

                    # Store the ID mapping
                    customer_id_mapper.add_mapping(str(record.id), str(new_customer.id))

                    print(f"Migrated Customer: {record.company}, {record.email}")
                    migrated_count += 1

                except IntegrityError as e:
                    # Handle duplicate entries
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Duplicate entry for {record.email}. Attempting to update..."
                            )
                            dest_customer = Customer.get(Customer.email == record.email)
                            customer_id_mapper.add_mapping(
                                str(record.id), str(dest_customer.id)
                            )

                            update_data = {
                                "name": record.company,
                                "address": record.o_address,
                                "contact_number": record.o_contactphone,
                                "user_id": mapped_user_id,
                            }

                            Customer.update(update_data).where(
                                Customer.id == dest_customer.id
                            ).execute()

                            print(
                                f"Updated existing customer: {record.company}, {record.email}"
                            )
                            migrated_count += 1

                        except Exception as update_error:
                            print(
                                f"Error updating {record.company} ({record.email}): {update_error}"
                            )
                            ignored_rows.append((record, str(update_error)))
                            skipped_count += 1
                    else:
                        print(
                            f"IntegrityError for {record.company} ({record.email}): {e}"
                        )
                        ignored_rows.append((record, str(e)))
                        skipped_count += 1

                except Exception as e:
                    print(f"Error migrating {record.company} ({record.email}): {e}")
                    ignored_rows.append((record, str(e)))
                    skipped_count += 1

    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Summary of migration results
    print(f"\nMigration Summary:")
    print(f"Total records: {total_records}")
    print(f"Successfully migrated: {migrated_count}")
    print(f"Skipped/Failed: {skipped_count}")

    if ignored_rows:
        print("\nDetailed error log:")
        for customer, reason in ignored_rows:
            print(f"- {customer.company} ({customer.email}): {reason}")


def run_migration():
    """Main function to run the cleanup and migration"""
    try:
        response = input(
            "This will delete all existing records in the destination table. Are you sure? (yes/no): "
        )
        if response.lower() != "yes":
            print("Migration cancelled.")
            return

        print("\nStep 1: Cleaning destination table...")
        clean_destination_table()

        print("\nStep 2: Starting migration...")
        migrate_customers()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
