# RD Migrations Code

A comprehensive database migration tool for transferring data from an old RD CMS database to a new migrated database structure.

## Quick Setup

For new machines, use the automated setup script:

```bash
# Make the script executable (if needed)
chmod +x setup.sh

# Run the setup script
./setup.sh

# Verify the setup
./verify_setup.sh

# Run migrations
./run_migration.sh
```

## Manual Setup

If you prefer manual setup or need to troubleshoot:

### Prerequisites

- Python 3.7+
- MySQL Server 5.7+
- Git

### Installation Steps

1. **Install system dependencies**:

   ```bash
   # Ubuntu/Debian
   sudo apt-get install python3 python3-pip python3-venv mysql-server libmysqlclient-dev pkg-config

   # CentOS/RHEL
   sudo yum install python3 python3-pip mysql-server mysql-devel pkgconfig

   # macOS
   brew install python3 mysql pkg-config
   ```

2. **Setup Python environment**:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Setup databases**:

   ```bash
   mysql -u root -p
   CREATE DATABASE IF NOT EXISTS resolutedynam9_cms;
   CREATE DATABASE IF NOT EXISTS rd_cms_migrated;
   EXIT;
   ```

4. **Configure database connections**:
   Update `source_db.py` and `dest_db.py` with your MySQL credentials.

## Migration Types

The application supports migrating the following data types:

1. **Users** - User accounts and authentication data
2. **Devices** - Device/ECU information
3. **Technicians** - Technician records
4. **Customers** - Customer information
5. **Certificates** - Certificate records (should be run last)

## Running Migrations

### Using the Helper Script

```bash
./run_migration.sh
```

### Manual Execution

```bash
source venv/bin/activate
python main.py
deactivate
```

## Configuration

### Database Settings

Update the database connection parameters in:

- `source_db.py` - Source database (old data)
- `dest_db.py` - Destination database (new structure)

### Default User Email

Update the `DEFAULT_USER_EMAIL` in migration files if needed:

- `models/customers_model.py`
- `models/devices_model.py`

## Output Files

The migration process generates:

### Mapping Files

- `customer_mappings.json`
- `user_mappings.json`
- `technicians_mapping.json`
- `certificates_mappings.json`
- `device_mappings.json`

### Excel Reports

- `customer_migration_report.xlsx`
- `device_migration_report.xlsx`
- `certificate_migration_report.xlsx`

## Files and Scripts

- `setup.sh` - Automated setup script for new machines
- `verify_setup.sh` - Verification script to check setup
- `run_migration.sh` - Helper script to run migrations
- `SETUP_GUIDE.md` - Detailed setup instructions
- `main.py` - Main migration application

## Troubleshooting

1. **Check setup**: Run `./verify_setup.sh`
2. **View logs**: Check console output for error messages
3. **Database issues**: Verify MySQL is running and credentials are correct
4. **Permission issues**: Ensure scripts are executable with `chmod +x`

For detailed troubleshooting, see `SETUP_GUIDE.md`.

## Security Notes

- Backup your databases before running migrations
- Never commit database passwords to version control
- Use strong passwords for database accounts
- Limit database user permissions appropriately
