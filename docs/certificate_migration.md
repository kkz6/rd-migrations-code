# Certificate Migration Process Documentation

## Overview
The certificate migration process is designed to transfer certificate data from a source database to a destination database, handling various related entities like devices, customers, technicians, and vehicles. The migration can be performed in two modes: one-by-one (interactive) or fully automated (batch).

## Key Functions and Workflow

### Main Entry Points
1. **`run_migration()`**
   - Entry point for the certificate migration process
   - Loads all required mappings and caches
   - Prompts user to choose migration mode
   - Handles the overall migration flow

2. **`run_one_by_one()`**
   - Interactive migration mode
   - Processes certificates one at a time
   - Provides user with options for each certificate
   - Maintains real-time progress tracking

3. **`run_fully_automated()`**
   - Batch processing mode
   - Uses multi-threading for parallel processing
   - Implements queue-based processing
   - Handles large-scale migrations efficiently

### Core Processing Functions
1. **`migrate_certificate()`**
   - Core function that handles individual certificate migration
   - Processes all related entities (device, customer, technicians, vehicle)
   - Creates new certificate record
   - Returns migration results and any errors

2. **`get_or_create_technician_for_certificate()`**
   - Handles technician mapping and creation
   - Processes both calibration and installation technicians
   - Maintains technician-user relationships
   - Updates technician mappings

3. **`list_unmigrated_certificates()`**
   - Retrieves certificates that need to be migrated
   - Supports filtering by ECU number
   - Excludes already migrated certificates

### Vehicle Data Processing
The vehicle data creation process is handled within the `migrate_certificate()` function. Here's how it works:

1. **Vehicle Data Extraction**
   ```python
   if record.vehicle_type:
       vehicle_brand_model = record.vehicle_type.split(" ", 1)
       brand = vehicle_brand_model[0]
       model = vehicle_brand_model[1] if len(vehicle_brand_model) > 1 else brand
   ```

2. **Vehicle Creation Logic**
   - Creates a new vehicle record for every certificate
   - Does not check for existing vehicles with the same chassis number
   - Creates vehicle with:
     - Brand (extracted from vehicle_type)
     - Model (extracted from vehicle_type)
     - Vehicle chassis number
     - Vehicle registration number
     - New registration flag (default: False)

3. **Vehicle-Certificate Relationship**
   - Each certificate gets its own unique vehicle record
   - One-to-one relationship between certificates and vehicles
   - Vehicle's `certificate_id` is updated with the new certificate ID
   - Multiple vehicles may have the same chassis number

4. **Error Handling**
   - Handles cases where vehicle data might be missing
   - Catches and logs any errors during vehicle creation
   - Sets vehicle to None if creation fails

5. **Data Flow**
   ```
   Source Certificate
         ↓
   Extract Vehicle Data
         ↓
   Create New Vehicle
         ↓
   Link to Certificate
   ```

### Utility Functions
1. **`convert_uae_to_utc()`**
   - Converts UAE timezone dates to UTC
   - Handles timezone localization
   - Ensures consistent date handling

2. **`parse_speed()`**
   - Extracts numeric speed values from strings
   - Handles various speed format inputs
   - Returns standardized speed values

3. **`save_to_excel()`**
   - Generates migration report in Excel format
   - Creates separate sheets for migrated and unmigrated certificates
   - Formats data for easy review

### Workflow Diagram
```
┌─────────────────┐
│  Start Migration│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Load Mappings   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Preload Caches  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Choose Mode     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐     ┌─────────────────┐
│ One-by-One Mode │     │ Batch Mode      │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│ Process Each    │     │ Queue Processing│
│ Certificate     │     └────────┬────────┘
└────────┬────────┘              │
         │                       ▼
         ▼              ┌─────────────────┐
┌─────────────────┐     │ Multi-threaded │
│ Update Mappings │     │ Processing     │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│ Generate Report │     │ Update Mappings │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│ Save Progress   │     │ Generate Report │
└────────┬────────┘     └────────┬────────┘
         │                       │
         ▼                       ▼
┌─────────────────┐     ┌─────────────────┐
│ End Migration   │     │ Save Progress   │
└─────────────────┘     └────────┬────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │ End Migration   │
                        └─────────────────┘
```

### Function Interactions
1. **Mapping and Cache Management**
   - `load_mappings()` → `preload_data()` → `DEVICE_CACHE`, `CUSTOMER_CACHE`, etc.
   - Used by all processing functions to access required data

2. **Certificate Processing Chain**
   - `list_unmigrated_certificates()` → `migrate_certificate()` → `get_or_create_technician_for_certificate()`
   - Forms the core processing pipeline

3. **Reporting and Progress Tracking**
   - `save_to_excel()` ← `migrate_certificate()`
   - Tracks and reports migration progress

## Table of Contents
1. [Configuration and Setup](#configuration-and-setup)
2. [Migration Modes](#migration-modes)
3. [Data Processing Flow](#data-processing-flow)
4. [Error Handling](#error-handling)
5. [Output and Reporting](#output-and-reporting)
6. [Best Practices](#best-practices)

## Configuration and Setup

### Required Files
- **Mapping Files**:
  - `customer_mappings.json`: Maps old customer IDs to new ones
  - `user_mappings.json`: Maps old user IDs to new ones
  - `technicians_mapping.json`: Maps old technician IDs to new ones
  - `certificates_mappings.json`: Tracks migrated certificates

### Global Caches
The system maintains in-memory caches to optimize performance:
- `DEVICE_CACHE`: Maps ECU numbers to device IDs
- `CUSTOMER_CACHE`: Stores customer information
- `USER_CACHE`: Stores user information
- `TECHNICIAN_CACHE`: Stores technician information

## Migration Modes

### 1. One-by-One Migration
- **Mode**: Interactive
- **Features**:
  - Processes certificates individually
  - User can choose to:
    - Migrate the certificate
    - Skip the certificate
    - Exit the migration
  - Real-time feedback and progress tracking
  - Suitable for careful, controlled migration

### 2. Fully Automated Migration
- **Mode**: Batch processing
- **Features**:
  - Multi-threaded processing (default: 10 threads)
  - Queue-based parallel processing
  - Progress bar and statistics
  - Suitable for large-scale migrations

## Data Processing Flow

### 1. Pre-migration Steps
1. Load mapping files
2. Preload data into caches
3. Initialize progress tracking

### 2. Certificate Processing
For each certificate, the system:
1. **Device Mapping**
   - Maps ECU number to new device ID
   - Updates device block status if needed

2. **Customer Mapping**
   - Maps old customer IDs to new ones
   - Creates new customer records if needed

3. **Technician Processing**
   - Handles both calibration and installation technicians
   - Creates new technician records if needed
   - Maintains technician-user relationships

4. **Vehicle Processing**
   - Creates or updates vehicle records
   - Links vehicles to certificates

5. **Certificate Creation**
   - Creates new certificate records
   - Sets appropriate status (active, renewed, nullified, cancelled, blocked)
   - Handles all related dates and metadata

### 3. Post-migration Steps
1. Update mapping files
2. Generate migration report
3. Save progress

## Error Handling

### Error Types
1. **Mapping Errors**
   - Missing customer mappings
   - Missing user mappings
   - Missing technician mappings

2. **Data Validation Errors**
   - Invalid dates
   - Missing required fields
   - Invalid relationships

3. **Database Errors**
   - Connection issues
   - Constraint violations
   - Transaction failures

### Error Recovery
- Failed certificates are logged
- Migration can be resumed
- Detailed error messages are provided
- Failed certificates can be retried

## Output and Reporting

### Migration Report
- Generated in Excel format (`certificate_migration_report.xlsx`)
- Contains two sheets:
  1. **Migrated**: Successfully migrated certificates
  2. **Unmigrated**: Failed certificates with error details

### Statistics
- Total certificates processed
- Success rate
- Failure count
- Last failed certificate

## Best Practices

### Before Migration
1. Ensure all mapping files are up to date
2. Verify database connections
3. Take database backups
4. Clear caches if needed

### During Migration
1. Monitor progress regularly
2. Check error logs
3. Verify data integrity
4. Keep mapping files updated

### After Migration
1. Verify migration report
2. Check for any failed certificates
3. Update documentation
4. Archive old mapping files

## Troubleshooting

### Common Issues
1. **Missing Mappings**
   - Solution: Run user/customer/technician migrations first

2. **Database Connection Issues**
   - Solution: Check connection strings and network

3. **Memory Issues**
   - Solution: Clear caches or reduce batch size

4. **Data Validation Errors**
   - Solution: Check source data quality

### Support
For issues not covered in this documentation, please contact the development team. 