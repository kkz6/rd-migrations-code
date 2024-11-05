from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    IntegerField,
    ForeignKeyField,
    DoesNotExist,
    fn,
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError
from models.users_model import DestinationUser
from datetime import datetime


# Source Model
class Sales(Model):
    id = IntegerField(primary_key=True)
    user_id = IntegerField()
    dealer_id = IntegerField()
    deal_date = TimestampField()

    class Meta:
        database = source_db
        table_name = "sales"


# Destination Model
class SalesPeople(Model):
    id = BigIntegerField(primary_key=True)
    name = CharField(max_length=255)
    email = CharField(max_length=255)
    phone = CharField(max_length=255)
    user_id = BigIntegerField()
    status = CharField(default="blocked")
    created_at = TimestampField(null=True)
    updated_at = TimestampField(null=True)
    deleted_at = TimestampField(null=True)

    class Meta:
        database = dest_db
        table_name = "sales_people"


def clean_destination_table():
    """Clean up the destination table before migration."""
    try:
        if dest_db.is_closed():
            dest_db.connect()

        with dest_db.atomic():
            count_before = SalesPeople.select().count()
            SalesPeople.delete().execute()
            count_after = SalesPeople.select().count()
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


def get_unique_sales_people():
    """Get unique sales people (user_id) with their earliest deal date"""
    return (
        Sales.select(Sales.user_id, fn.MIN(Sales.deal_date).alias("first_deal_date"))
        .group_by(Sales.user_id)
        .having(Sales.user_id.is_null(False))
    )


def migrate_sales_people():
    ignored_rows = []
    migrated_count = 0
    skipped_count = 0

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        # Get unique user_ids from sales table
        unique_sales_people = get_unique_sales_people()
        total_records = unique_sales_people.count()

        print(f"Found {total_records} unique sales people to migrate")

        with dest_db.atomic():
            for record in unique_sales_people:
                try:
                    # Get user details from destination users table
                    user = check_user_exists(record.user_id)
                    if not user:
                        print(
                            f"Skipping user_id {record.user_id} - not found in destination users"
                        )
                        ignored_rows.append(
                            (record.user_id, "User not found in destination users")
                        )
                        skipped_count += 1
                        continue

                    # Get latest dealer_id for this user_id
                    latest_sale = (
                        Sales.select(Sales.dealer_id)
                        .where(Sales.user_id == record.user_id)
                        .order_by(Sales.deal_date.desc())
                        .first()
                    )

                    # Create new sales person record
                    SalesPeople.create(
                        name=user.name,
                        email=user.email,
                        phone=user.mobile,
                        user_id=latest_sale.dealer_id,  # Map dealer_id to user_id
                        status="active",
                        created_at=record.first_deal_date,
                    )

                    print(f"Created sales person: {user.name} (ID: {record.user_id})")
                    migrated_count += 1

                except IntegrityError as e:
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Updating existing sales person for user_id {record.user_id}"
                            )
                            SalesPeople.update(
                                name=user.name,
                                phone=user.mobile,
                                user_id=latest_sale.dealer_id,
                                updated_at=datetime.now(),
                            ).where(SalesPeople.email == user.email).execute()

                            print(f"Updated existing sales person: {user.name}")
                            migrated_count += 1
                        except Exception as update_error:
                            print(
                                f"Error updating sales person {user.name}: {update_error}"
                            )
                            ignored_rows.append((record.user_id, str(update_error)))
                            skipped_count += 1
                    else:
                        print(f"IntegrityError for user_id {record.user_id}: {e}")
                        ignored_rows.append((record.user_id, str(e)))
                        skipped_count += 1

                except Exception as e:
                    print(f"Error migrating user_id {record.user_id}: {e}")
                    ignored_rows.append((record.user_id, str(e)))
                    skipped_count += 1

    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Summary of migration results
    print(f"\nMigration Summary:")
    print(f"Total unique sales people: {total_records}")
    print(f"Successfully migrated: {migrated_count}")
    print(f"Skipped/Failed: {skipped_count}")

    if ignored_rows:
        print("\nDetailed error log:")
        for user_id, reason in ignored_rows:
            print(f"- User ID {user_id}: {reason}")


def check_user_exists(user_id):
    """Check if a user exists in the destination database"""
    try:
        return DestinationUser.get(DestinationUser.id == user_id)
    except DoesNotExist:
        return None


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
        migrate_sales_people()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
