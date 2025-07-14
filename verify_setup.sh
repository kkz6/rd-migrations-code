#!/bin/bash

# =============================================================================
# RD Migrations Code - Setup Verification Script
# =============================================================================
# This script verifies that the setup was completed successfully
# =============================================================================

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

echo "============================================================================="
echo "RD Migrations Code - Setup Verification"
echo "============================================================================="
echo ""

# Check if virtual environment exists
print_status "Checking virtual environment..."
if [ -d "venv" ]; then
    print_success "Virtual environment found"
else
    print_error "Virtual environment not found"
    exit 1
fi

# Activate virtual environment
source venv/bin/activate

# Check Python packages
print_status "Checking Python packages..."
packages=("peewee" "pymysql" "questionary" "openpyxl" "tqdm" "pytz")
all_packages_ok=true

for package in "${packages[@]}"; do
    if python -c "import $package" 2>/dev/null; then
        print_success "$package is installed"
    else
        print_error "$package is NOT installed"
        all_packages_ok=false
    fi
done

if [ "$all_packages_ok" = false ]; then
    print_error "Some Python packages are missing. Run: pip install -r requirements.txt"
fi

# Check if MySQL is running
print_status "Checking MySQL service..."
if pgrep -x "mysqld" > /dev/null; then
    print_success "MySQL is running"
else
    print_warning "MySQL is not running. Start it with: sudo systemctl start mysql"
fi

# Check database connectivity
print_status "Checking database connectivity..."
if mysql -u root -e "SELECT 1;" 2>/dev/null; then
    print_success "MySQL connection successful (no password)"
    MYSQL_CMD="mysql -u root"
elif mysql -u root --password="" -e "SELECT 1;" 2>/dev/null; then
    print_success "MySQL connection successful (empty password)"
    MYSQL_CMD="mysql -u root --password=''"
else
    print_warning "MySQL connection failed. You may need to set a password or check credentials."
    MYSQL_CMD=""
fi

# Check if databases exist (only if we have a working connection)
if [ -n "$MYSQL_CMD" ]; then
    print_status "Checking databases..."
    if $MYSQL_CMD -e "USE resolutedynam9_cms;" 2>/dev/null; then
        print_success "Source database (resolutedynam9_cms) exists"
    else
        print_warning "Source database (resolutedynam9_cms) not found"
    fi

    if $MYSQL_CMD -e "USE rd_cms_migrated;" 2>/dev/null; then
        print_success "Destination database (rd_cms_migrated) exists"
    else
        print_warning "Destination database (rd_cms_migrated) not found"
    fi
else
    print_warning "Skipping database checks due to connection issues"
fi

# Check required files
print_status "Checking required files..."
required_files=("main.py" "requirements.txt" "source_db.py" "dest_db.py")
for file in "${required_files[@]}"; do
    if [ -f "$file" ]; then
        print_success "$file found"
    else
        print_error "$file not found"
    fi
done

# Check if run script exists and is executable
print_status "Checking run script..."
if [ -f "run_migration.sh" ] && [ -x "run_migration.sh" ]; then
    print_success "run_migration.sh is ready"
else
    print_warning "run_migration.sh not found or not executable"
fi

# Test import of main modules
print_status "Testing module imports..."
if python -c "from main import migrate_data" 2>/dev/null; then
    print_success "Main module imports successfully"
else
    print_error "Main module import failed"
fi

deactivate

echo ""
echo "============================================================================="
echo "Verification Complete"
echo "============================================================================="
echo ""
echo "If all checks passed, you can run the migration with:"
echo "./run_migration.sh"
echo ""
echo "If there were warnings or errors, please:"
echo "1. Check the setup guide: SETUP_GUIDE.md"
echo "2. Re-run the setup script: ./setup.sh"
echo "3. Fix any configuration issues"
echo "=============================================================================" 