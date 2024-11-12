from datetime import datetime
import re
import bcrypt
from peewee import (
    Model,
    CharField,
    BigIntegerField,
    TimestampField,
    TextField,
    IntegerField,
    BooleanField,
    AutoField,
)
from source_db import source_db
from dest_db import dest_db
from peewee import IntegrityError
from id_mapping import user_id_mapper, dealer_id_mapper


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

    class Meta:
        database = dest_db
        table_name = "users"


class DealerMaster(Model):
    id = AutoField()
    company = CharField(max_length=300)
    email = CharField(max_length=200, unique=True)
    phone = TextField()
    mobile = TextField()
    emirate = CharField(max_length=20, default="NA")
    status = CharField(max_length=20, default="AFC")
    salesuser = CharField(max_length=20)
    add_date = TimestampField(default=datetime.now)
    added_by = IntegerField()

    class Meta:
        database = source_db
        table_name = "dealer_master"


# Destination role models for Spatie
class Role(Model):
    id = AutoField()
    name = CharField()
    guard_name = CharField()

    class Meta:
        database = dest_db
        table_name = "roles"


class ModelHasRole(Model):
    role_id = IntegerField()
    model_type = CharField()
    model_id = IntegerField()

    class Meta:
        database = dest_db
        table_name = "model_has_roles"
        indexes = ((("model_id", "model_type", "role_id"), True),)


def migrate_dealers():
    """
    Migrate dealers from source to destination database and assign roles.
    Updates the migration_state with dealer ID mappings.
    """
    current_time = datetime.now()

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        # Get or create dealer role
        dealer_role, created = Role.get_or_create(
            name="dealer",
            guard_name="web",
            defaults={"created_at": current_time, "updated_at": current_time},
        )

        print(f"Using dealer role ID: {dealer_role.id}")

        # Migrate each dealer
        for dealer in DealerMaster.select():
            try:
                existing_mapping = dealer_id_mapper.get_dest_id(str(dealer.id))

                if existing_mapping:
                    print(
                        f"Dealer {dealer.company} already migrated with mapping {dealer.id} -> {existing_mapping}"
                    )
                    continue
                # Generate a random salt
                salt = bcrypt.gensalt()

                # Hash the password using the salt
                hashed_password = generate_default_password()

                username = dealer.company.lower().replace(" ", "_")

                # Create user record for dealer
                new_user = DestinationUser.create(
                    name=dealer.company,
                    email=dealer.email,
                    password=hashed_password,
                    username=username,
                    mobile=dealer.mobile,
                    company=dealer.company,
                )

                # Store the ID mapping
                dealer_id_mapper.add_mapping(str(dealer.id), str(new_user.id))

                # Assign dealer role
                ModelHasRole.create(
                    role_id=dealer_role.id,
                    model_type="App\\Models\\User",
                    model_id=new_user.id,
                )

                dealer_id_mapper.add_mapping(str(dealer.id), str(new_user.id))

                print(
                    f"Migrated dealer {dealer.company} (ID: {dealer.id}) to user ID: {new_user.id}"
                )

            except IntegrityError as e:
                # Handle case where user might already exist
                if "Duplicate entry" in str(e):
                    existing_user = DestinationUser.get(
                        DestinationUser.email == dealer.email
                    )
                    dealer_id_mapper.add_mapping(str(dealer.id), str(existing_user.id))
                    print(
                        f"Dealer {dealer.company} already exists as user ID: {existing_user.id}"
                    )
                else:
                    print(f"Error migrating dealer {dealer.company}: {str(e)}")
            except Exception as e:
                print(f"Unexpected error migrating dealer {dealer.company}: {str(e)}")

    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()


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


def generate_username(full_name):
    """
    Generate username from full name:
    - Convert to lowercase
    - Replace spaces with *
    - Remove special characters
    - Handle duplicates by adding number suffix if needed
    """
    # Convert to lowercase and replace spaces with *
    username = full_name.lower().strip()
    # Remove special characters except spaces
    username = re.sub(r"[^a-z0-9\s]", "", username)
    # Replace spaces with *
    username = username.replace(" ", "*")
    return username


def generate_default_password():
    """
    Generate a Laravel-compatible bcrypt hashed password.
    Laravel expects passwords to be hashed with bcrypt and have a specific format:
    $2y$rounds$salt+hash
    """
    default_password = "password"  # Customize as needed
    salt = bcrypt.gensalt(rounds=10)  # Laravel default is 10 rounds
    hashed = bcrypt.hashpw(default_password.encode("utf-8"), salt)

    # Convert the bcrypt hash to Laravel's expected format
    # Replace $2b$ (Python's bcrypt prefix) with $2y$ (Laravel's expected prefix)
    laravel_hash = hashed.decode("utf-8").replace("$2b$", "$2y$")

    return laravel_hash


def migrate_users():
    """
    Migrate users using the global ID mapper for relationship tracking.
    """
    ignored_rows = []
    total_records = User.select().count()
    migrated_count = 0
    skipped_count = 0
    updated_count = 0

    if source_db.is_closed():
        source_db.connect()
    if dest_db.is_closed():
        dest_db.connect()

    try:
        # Get or create roles
        admin_role, created = Role.get_or_create(
            name="super_admin",
            guard_name="web",
        )

        dealer_role, created = Role.get_or_create(
            name="dealer",
            guard_name="web",
        )

        with dest_db.atomic():
            for record in User.select():
                try:
                    # Check if user was already migrated
                    existing_mapping = user_id_mapper.get_dest_id(str(record.id))
                    if existing_mapping:
                        print(
                            f"User {record.full_name} already migrated with mapping {record.id} -> {existing_mapping}"
                        )
                        continue

                    # Generate username and password
                    base_username = record.username or generate_username(
                        record.full_name
                    )
                    suffix = 1
                    username = base_username
                    while (
                        DestinationUser.select()
                        .where(DestinationUser.username == username)
                        .exists()
                    ):
                        username = f"{base_username}{suffix}"
                        suffix += 1

                    password = generate_default_password()
                    email = record.email.rstrip("-").strip().lower()

                    # Create new user
                    new_user = DestinationUser.create(
                        name=record.full_name,
                        email=email,
                        email_verified_at=datetime.now(),
                        password=password,
                        username=username,
                        company=record.company,
                        status="active" if record.activstate == 1 else "blocked",
                        phone=None,
                        mobile=record.mobile,
                        emirates=None,
                        timezone="UTC",
                        country=record.country,
                        state=None,
                        parent_id=None,
                        remember_token=None,
                    )

                    # Store the ID mapping
                    user_id_mapper.add_mapping(str(record.id), str(new_user.id))

                    # Assign role based on usertype
                    role_id = (
                        admin_role.id
                        if record.usertype.lower() == "admin"
                        else dealer_role.id
                    )
                    ModelHasRole.create(
                        role_id=role_id,
                        model_type="App\\Models\\User",
                        model_id=new_user.id,
                    )

                    print(
                        f"Migrated User: {record.full_name} (Old ID: {record.id}, New ID: {new_user.id})"
                    )
                    migrated_count += 1

                except IntegrityError as e:
                    if "Duplicate entry" in str(e):
                        try:
                            print(
                                f"Duplicate entry for {email}. Attempting to update..."
                            )
                            dest_user = DestinationUser.get(
                                DestinationUser.email == email
                            )

                            # Store mapping for existing user
                            user_id_mapper.add_mapping(
                                str(record.id), str(dest_user.id)
                            )

                            update_data = {
                                "name": record.full_name,
                                "username": username,
                                "company": record.company,
                                "status": (
                                    "active" if record.activstate == 1 else "blocked"
                                ),
                                "mobile": record.mobile,
                                "country": record.country,
                            }

                            if not dest_user.password:
                                update_data["password"] = generate_default_password()

                            DestinationUser.update(update_data).where(
                                DestinationUser.id == dest_user.id
                            ).execute()

                            print(f"Updated existing user: {record.full_name}")
                            updated_count += 1

                        except Exception as update_error:
                            print(f"Error updating {record.full_name}: {update_error}")
                            ignored_rows.append((record, str(update_error)))
                            skipped_count += 1
                    else:
                        print(f"IntegrityError for {record.full_name}: {e}")
                        ignored_rows.append((record, str(e)))
                        skipped_count += 1

                except Exception as e:
                    print(f"Error migrating {record.full_name}: {e}")
                    ignored_rows.append((record, str(e)))
                    skipped_count += 1

    finally:
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()

    # Print migration summary
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
        print("\nStep 2: Starting user migration...")
        migrate_users()

        # Step 2: Perform dealer migration
        print("\nStep 2: Starting dealer migration...")
        migrate_dealers()

    except Exception as e:
        print(f"Error during migration process: {str(e)}")
    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
