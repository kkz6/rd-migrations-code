#!/bin/bash

# =============================================================================
# RD Migrations Code - Setup Script
# =============================================================================
# This script sets up the complete environment for running the RD migration
# application on a new machine.
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
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

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to detect OS
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        if [ -f /etc/debian_version ]; then
            echo "debian"
        elif [ -f /etc/redhat-release ]; then
            echo "redhat"
        else
            echo "linux"
        fi
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        echo "macos"
    else
        echo "unknown"
    fi
}

# Function to install system dependencies
install_system_dependencies() {
    local os_type=$(detect_os)
    
    print_status "Installing system dependencies for $os_type..."
    
    case $os_type in
        "debian")
            sudo apt-get update
            sudo apt-get install -y \
                python3 \
                python3-pip \
                python3-venv \
                mysql-server \
                mysql-client \
                libmysqlclient-dev \
                pkg-config \
                git \
                curl \
                wget
            ;;
        "redhat")
            sudo yum update -y
            sudo yum install -y \
                python3 \
                python3-pip \
                mysql-server \
                mysql-devel \
                pkgconfig \
                git \
                curl \
                wget
            ;;
        "macos")
            if ! command_exists brew; then
                print_status "Installing Homebrew..."
                /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
            fi
            
            brew update
            brew install python3 mysql pkg-config git
            ;;
        *)
            print_error "Unsupported operating system: $os_type"
            exit 1
            ;;
    esac
    
    print_success "System dependencies installed successfully"
}

# Function to detect MySQL password setup
detect_mysql_auth() {
    print_status "Detecting MySQL authentication setup..."
    
    if mysql -u root -e "SELECT 1;" 2>/dev/null; then
        print_success "MySQL allows connection without password"
        return 0
    elif mysql -u root --password="" -e "SELECT 1;" 2>/dev/null; then
        print_success "MySQL uses empty password"
        return 1
    else
        print_warning "MySQL requires a password"
        return 2
    fi
}

# Function to setup MySQL
setup_mysql() {
    print_status "Setting up MySQL..."
    
    local os_type=$(detect_os)
    
    # Start MySQL service
    case $os_type in
        "debian")
            sudo systemctl start mysql
            sudo systemctl enable mysql
            ;;
        "redhat")
            sudo systemctl start mysqld
            sudo systemctl enable mysqld
            ;;
        "macos")
            brew services start mysql
            ;;
    esac
    
    # Check if MySQL is running
    if ! pgrep -x "mysqld" > /dev/null; then
        print_error "MySQL failed to start. Please check your MySQL installation."
        exit 1
    fi
    
    print_success "MySQL is running"
    
    # Create databases if they don't exist
    print_status "Creating databases..."
    
    # Function to try MySQL connection with different password options
    try_mysql_connection() {
        local command="$1"
        
        # First try without password
        if mysql -u root -e "$command" 2>/dev/null; then
            return 0
        fi
        
        # If that fails, try with empty password explicitly
        if mysql -u root --password="" -e "$command" 2>/dev/null; then
            return 0
        fi
        
        # If that fails, prompt for password
        print_status "MySQL requires a password. Please enter your MySQL root password:"
        if mysql -u root -p -e "$command" 2>/dev/null; then
            return 0
        fi
        
        return 1
    }
    
    # Try to create source database
    if try_mysql_connection "CREATE DATABASE IF NOT EXISTS resolutedynam9_cms;"; then
        print_success "Source database (resolutedynam9_cms) created/verified"
    else
        print_warning "Could not create source database. You may need to create it manually."
        print_warning "Run: CREATE DATABASE IF NOT EXISTS resolutedynam9_cms;"
    fi
    
    # Try to create destination database
    if try_mysql_connection "CREATE DATABASE IF NOT EXISTS rd_cms_migrated;"; then
        print_success "Destination database (rd_cms_migrated) created/verified"
    else
        print_warning "Could not create destination database. You may need to create it manually."
        print_warning "Run: CREATE DATABASE IF NOT EXISTS rd_cms_migrated;"
    fi
}

# Function to setup Python environment
setup_python_environment() {
    print_status "Setting up Python virtual environment..."
    
    # Create virtual environment
    if [ ! -d "venv" ]; then
        python3 -m venv venv
        print_success "Virtual environment created"
    else
        print_warning "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install Python dependencies
    print_status "Installing Python dependencies..."
    pip install -r requirements.txt
    
    print_success "Python dependencies installed successfully"
}

# Function to setup project configuration
setup_project_config() {
    print_status "Setting up project configuration..."
    
    # Create config template if it doesn't exist
    if [ ! -f "config.py" ]; then
        cat > config.py << 'EOF'
# Database Configuration
# Update these settings according to your MySQL setup

SOURCE_DB_CONFIG = {
    "database": "resolutedynam9_cms",
    "user": "root",
    "password": "",  # Update with your MySQL root password
    "host": "127.0.0.1",
    "port": 3306
}

DEST_DB_CONFIG = {
    "database": "rd_cms_migrated",
    "user": "root",
    "password": "",  # Update with your MySQL root password
    "host": "127.0.0.1",
    "port": 3306
}

# Default user email for migrations
DEFAULT_USER_EMAIL = "linoj@resolute-dynamics.com"
EOF
        print_success "Configuration template created: config.py"
        print_warning "Please update the database passwords in config.py"
    else
        print_warning "config.py already exists"
    fi
    
    # Create directories for mapping files
    mkdir -p mappings
    mkdir -p reports
    
    print_success "Project structure setup complete"
}

# Function to verify installation
verify_installation() {
    print_status "Verifying installation..."
    
    # Check Python packages
    source venv/bin/activate
    
    local packages=("peewee" "pymysql" "questionary" "openpyxl" "tqdm" "pytz")
    for package in "${packages[@]}"; do
        if python -c "import $package" 2>/dev/null; then
            print_success "$package is installed"
        else
            print_error "$package is not installed properly"
            return 1
        fi
    done
    
    # Check MySQL connection
    print_status "Testing MySQL connection..."
    if mysql -u root -e "SELECT 1;" 2>/dev/null; then
        print_success "MySQL connection successful (no password)"
    elif mysql -u root --password="" -e "SELECT 1;" 2>/dev/null; then
        print_success "MySQL connection successful (empty password)"
    else
        print_warning "MySQL connection requires password or failed. Please verify your credentials."
    fi
    
    print_success "Installation verification complete"
}

# Function to create run script
create_run_script() {
    print_status "Creating run script..."
    
    cat > run_migration.sh << 'EOF'
#!/bin/bash

# Activate virtual environment
source venv/bin/activate

# Run the migration application
python main.py

# Deactivate virtual environment
deactivate
EOF
    
    chmod +x run_migration.sh
    print_success "Run script created: run_migration.sh"
}

# Function to display final instructions
display_final_instructions() {
    echo ""
    echo "============================================================================="
    echo -e "${GREEN}Setup Complete!${NC}"
    echo "============================================================================="
    echo ""
    echo "Next steps:"
    echo "1. Update database passwords in source_db.py and dest_db.py (if MySQL requires password)"
    echo "2. Update database passwords in config.py (if needed)"
    echo "3. Ensure your source database (resolutedynam9_cms) has data"
    echo "4. Run the migration using: ./run_migration.sh"
    echo ""
    echo "Available migration options:"
    echo "- Users migration"
    echo "- Devices migration"
    echo "- Technicians migration"
    echo "- Customers migration"
    echo "- Certificates migration"
    echo ""
    echo "Generated files and directories:"
    echo "- venv/                 (Python virtual environment)"
    echo "- config.py            (Configuration file)"
    echo "- mappings/            (Directory for mapping files)"
    echo "- reports/             (Directory for Excel reports)"
    echo "- run_migration.sh     (Script to run migrations)"
    echo ""
    echo "Mapping files that will be created during migration:"
    echo "- customer_mappings.json"
    echo "- user_mappings.json"
    echo "- technicians_mapping.json"
    echo "- certificates_mappings.json"
    echo "- device_mappings.json"
    echo ""
    echo "Report files that will be generated:"
    echo "- customer_migration_report.xlsx"
    echo "- device_migration_report.xlsx"
    echo "- certificate_migration_report.xlsx"
    echo ""
    print_warning "Remember to backup your databases before running migrations!"
    echo "============================================================================="
}

# Main execution
main() {
    echo "============================================================================="
    echo "RD Migrations Code - Setup Script"
    echo "============================================================================="
    echo ""
    
    # Check if running as root (not recommended)
    if [ "$EUID" -eq 0 ]; then
        print_warning "Running as root is not recommended. Consider running as a regular user."
        read -p "Do you want to continue? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Install system dependencies
    install_system_dependencies
    
    # Setup MySQL
    setup_mysql
    
    # Detect MySQL authentication after setup
    detect_mysql_auth
    mysql_auth_result=$?
    
    if [ $mysql_auth_result -eq 2 ]; then
        print_warning "MySQL requires a password. You'll need to update the database configuration files:"
        print_warning "- Update source_db.py with your MySQL password"
        print_warning "- Update dest_db.py with your MySQL password"
        echo ""
    fi
    
    # Setup Python environment
    setup_python_environment
    
    # Setup project configuration
    setup_project_config
    
    # Create run script
    create_run_script
    
    # Verify installation
    verify_installation
    
    # Display final instructions
    display_final_instructions
}

# Handle script interruption
trap 'print_error "Setup interrupted by user"; exit 1' INT

# Run main function
main "$@" 