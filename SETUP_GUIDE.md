# RD Migrations Code - Setup Guide

This guide will help you set up the RD migration application on a new machine.

## Quick Start

1. **Clone the repository** (if not already done):

   ```bash
   git clone <repository-url>
   cd rd-migrations-code
   ```

2. **Run the setup script**:

   ```bash
   ./setup.sh
   ```

3. **Follow the prompts** and wait for the setup to complete.

4. **Update configuration** (if needed):

   ```bash
   nano config.py  # Update database passwords if needed
   ```

5. **Run the migration**:
   ```bash
   ./run_migration.sh
   ```

## What the Setup Script Does

The `setup.sh` script automatically:

### 1. **System Dependencies Installation**

- **Linux (Debian/Ubuntu)**: Installs Python3, pip, MySQL server, development headers
- **Linux (RedHat/CentOS)**: Installs Python3, pip, MySQL server, development headers
- **macOS**: Installs Homebrew (if needed), Python3, MySQL via brew

### 2. **MySQL Setup**

- Starts MySQL service
- Enables MySQL to start on boot
- Creates required databases:
  - `resolutedynam9_cms` (source database)
  - `rd_cms_migrated` (destination database)

### 3. **Python Environment**

- Creates a virtual environment (`venv/`)
- Installs all Python dependencies from `requirements.txt`
- Verifies package installation

### 4. **Project Configuration**

- Creates `config.py` template with database settings
- Creates directories for mapping files and reports
- Sets up project structure

### 5. **Helper Scripts**

- Creates `run_migration.sh` for easy execution
- Makes scripts executable

## Manual Setup (Alternative)

If you prefer to set up manually or the script doesn't work for your system:

### Prerequisites

1. **Python 3.7+**
2. **MySQL Server 5.7+**
3. **Git**

### Step-by-Step Manual Setup

1. **Install system dependencies**:

   **Ubuntu/Debian:**

   ```bash
   sudo apt-get update
   sudo apt-get install python3 python3-pip python3-venv mysql-server libmysqlclient-dev pkg-config
   ```

   **CentOS/RHEL:**

   ```bash
   sudo yum install python3 python3-pip mysql-server mysql-devel pkgconfig
   ```

   **macOS:**

   ```bash
   brew install python3 mysql pkg-config
   ```

2. **Setup MySQL**:

   ```bash
   # Start MySQL
   sudo systemctl start mysql  # Linux
   # or
   brew services start mysql   # macOS

   # Create databases
   mysql -u root -p
   CREATE DATABASE IF NOT EXISTS resolutedynam9_cms;
   CREATE DATABASE IF NOT EXISTS rd_cms_migrated;
   EXIT;
   ```

3. **Setup Python environment**:

   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Configure database connections**:
   Update `source_db.py` and `dest_db.py` with your MySQL credentials.

## Configuration

### Database Configuration

Update the database connection settings in:

- **`source_db.py`**: Source database (old data)
- **`dest_db.py`**: Destination database (migrated data)

Example configuration:

```python
from peewee import MySQLDatabase

source_db = MySQLDatabase(
    "resolutedynam9_cms",
    user="root",
    password="your_mysql_password",
    host="127.0.0.1",
    port=3306,
)
```

### Default User Email

Update the `DEFAULT_USER_EMAIL` in migration files if needed:

- `models/customers_model.py`
- `models/devices_model.py`

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

### Available Migration Types

1. **Users Migration**: Migrates user accounts and authentication data
2. **Devices Migration**: Migrates device/ECU information
3. **Technicians Migration**: Migrates technician records
4. **Customers Migration**: Migrates customer information
5. **Certificates Migration**: Migrates certificate records (run last)

### Migration Order (Recommended)

1. Users
2. Devices
3. Technicians
4. Customers
5. Certificates

## Output Files

The migration process creates several files:

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

## Troubleshooting

### Common Issues

1. **MySQL Connection Failed**

   - Check if MySQL is running: `systemctl status mysql`
   - Verify credentials in database configuration files
   - Ensure databases exist

2. **Python Package Installation Failed**

   - Make sure you're in the virtual environment: `source venv/bin/activate`
   - Update pip: `pip install --upgrade pip`
   - Install missing system dependencies

3. **Permission Denied**

   - Make sure scripts are executable: `chmod +x setup.sh run_migration.sh`
   - Check file permissions

4. **Database Creation Failed**
   - Manually create databases using MySQL command line
   - Check MySQL user permissions

### Getting Help

1. Check the log output for specific error messages
2. Verify all prerequisites are installed
3. Ensure database connectivity
4. Check file permissions

## Security Notes

- **Never commit database passwords** to version control
- **Backup your databases** before running migrations
- **Use strong passwords** for database accounts
- **Limit database user permissions** to only what's needed

## System Requirements

### Minimum Requirements

- **RAM**: 2GB
- **Storage**: 5GB free space
- **CPU**: 1 core
- **OS**: Linux, macOS, or Windows with WSL

### Recommended Requirements

- **RAM**: 4GB+
- **Storage**: 10GB+ free space
- **CPU**: 2+ cores
- **Network**: Stable internet connection for package downloads

## Support

For issues or questions:

1. Check this setup guide
2. Review error logs
3. Verify system requirements
4. Contact the development team
