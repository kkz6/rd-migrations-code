from datetime import datetime
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
import pymysql
pymysql.install_as_MySQLdb()
import pytz

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


def run_migration():
    """
    Migrate dealers interactively by asking for user confirmation and selecting user IDs.
    """
    try:
        migrated_user_ids = []  # Track migrated users to prevent showing them again
        first_migrated_user = None  # Track the first migrated user for sub-user parent ID updates

        # Step 1: Loop through the DealerMaster table
        for dealer in DealerMaster.select().order_by(DealerMaster.id.desc()):
            print(f"\nMigrate {dealer.company}?")
            response = input("Type 'yes' to migrate or 'skip' to skip: ").strip().lower()

            if response == "yes":
                # Step 2: Search for users from the User table matching the company name (with minor variations)
                users_in_company = User.select().where(User.company.contains(dealer.company))  # Match with minor variations
                filtered_users = [user for user in users_in_company if user.id not in migrated_user_ids]

                if filtered_users:
                    print("\nUsers found for this dealer (excluding migrated users):")
                    for user in filtered_users:
                        print(f"ID: {user.id} | Name: {user.full_name} | Email: {user.email}")

                    while True:
                        # Step 3: Ask user to input the ID of the user to migrate
                        user_id = input("\nEnter the ID of the user to migrate (or type 'skip' to skip this dealer): ").strip()

                        if user_id.lower() == "skip":
                            break  # Skip this dealer, move on to the next one

                        try:
                            user_id = int(user_id)  # Ensure the input is an integer

                            # Step 4: Fetch the selected user
                            selected_user = User.get_by_id(user_id)

                            # Show the selected user details for confirmation
                            print(f"\nYou selected the following user:\nID: {selected_user.id} | Name: {selected_user.full_name} | Email: {selected_user.email}")

                            # Step 5: Ask if they want to migrate this user
                            action = input("Type 'migrate' to migrate this user, 'retry' to select a different user, or 'skip' to skip this user: ").strip().lower()

                            if action == "migrate":
                                # Perform migration for the selected user
                                print(f"\nMigrating user {selected_user.full_name}...")
                                current_time = datetime.now(pytz.utc)

                                # Migrate user to destination user table
                                # Call your migration logic here to create a user record in the destination table
                                new_user = DestinationUser.create(
                                    name=selected_user.full_name,
                                    email=selected_user.email,
                                    email_verified_at=current_time,
                                    password="$2y$10$4sCgBDych20ZjQ8EY/z4SOKNRObHjl6LWe02OmI3Ht4cktxPHNAmC",  # Pre-defined password hash
                                    username=selected_user.username,
                                    company=selected_user.company,
                                    status="active" if selected_user.activstate == 1 else "blocked",
                                    phone=selected_user.mobile,
                                    mobile=selected_user.mobile,
                                    timezone="UTC",
                                    country=selected_user.country,
                                    created_at=current_time,
                                    updated_at=current_time,
                                )

                                print(f"User {selected_user.full_name} migrated successfully!")

                                # Update parent_id for any sub-users
                                if first_migrated_user is not None:
                                    # Set the parent_id of sub-users to the first migrated user's ID
                                    for sub_user in User.select().where(User.company == dealer.company, User.added_by_user_id == selected_user.id):
                                        print(f"Updating parent_id for sub-user {sub_user.full_name} (ID: {sub_user.id})")
                                        sub_user.parent_id = new_user.id
                                        sub_user.save()

                                # Add this migrated user's ID to the list
                                migrated_user_ids.append(new_user.id)

                                # Store the first migrated user for setting parent_id for sub-users
                                if first_migrated_user is None:
                                    first_migrated_user = new_user

                                # Step 6: Ask if they want to migrate more sub-users for the same dealer
                                migrate_more = input("Do you want to migrate more users from the same company? (yes/skip): ").strip().lower()
                                if migrate_more != "yes":
                                    break  # Exit the loop to proceed with the next dealer

                            elif action == "retry":
                                continue  # Retry by asking for the user ID again
                            elif action == "skip":
                                break  # Skip this user and move on to the next one

                        except ValueError:
                            print("Invalid input! Please enter a valid user ID.")
                        except User.DoesNotExist:
                            print(f"No user found with ID {user_id}. Please try again.")
                else:
                    print(f"No users found for dealer {dealer.company}.")

                    # Step 7: Ask the user to create a new user if no users are found for this company
                    print(f"\nCreating a new user for company: {dealer.company}")
                    new_user_data = {
                        "name": dealer.company,  # Default name based on the dealer company
                        "email": dealer.email,  # Default email
                        "company": dealer.company,
                        "mobile": dealer.phone,  # Default phone
                        "country": "UAE",  # Default country based on dealer info
                    }

                    # Ask for input or use default values
                    print("\nDefault data for the new user:")
                    for field, default_value in new_user_data.items():
                        user_input = input(f"{field.capitalize()} (Default: {default_value}): ").strip()
                        if user_input:
                            new_user_data[field] = user_input

                    # Create the new user record in the destination table
                    current_time = datetime.now(pytz.utc)
                    new_user = DestinationUser.create(
                        name=new_user_data["name"],
                        email=new_user_data["email"],
                        email_verified_at=current_time,
                        password="$2y$10$4sCgBDych20ZjQ8EY/z4SOKNRObHjl6LWe02OmI3Ht4cktxPHNAmC",  # Pre-defined password hash
                        username=new_user_data["username"],
                        company=new_user_data["company"],
                        status="active",
                        phone=new_user_data["mobile"],
                        mobile=new_user_data["mobile"],
                        timezone="UTC",
                        country=new_user_data["country"],
                        created_at=current_time,
                        updated_at=current_time,
                    )

                    print(f"New user created successfully for {dealer.company}!")

                    # Add the newly created user's ID to the migrated list
                    migrated_user_ids.append(new_user.id)

                    # Store the first migrated user for setting parent_id for sub-users
                    if first_migrated_user is None:
                        first_migrated_user = new_user

                    # Step 8: Ask if they want to migrate more sub-users for the same dealer
                    migrate_more = input("Do you want to migrate more users from the same company? (yes/skip): ").strip().lower()
                    if migrate_more != "yes":
                        continue  # Move on to the next dealer

            elif response == "skip":
                continue  # Skip this dealer and move on to the next one

        print("Migration process complete.")

    except Exception as e:
        print(f"Error during migration: {str(e)}")

    finally:
        # Ensure all database connections are closed
        if not source_db.is_closed():
            source_db.close()
        if not dest_db.is_closed():
            dest_db.close()
