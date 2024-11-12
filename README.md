# Project Setup

This project requires specific steps to initialize the database with roles and set default values for device information.

## Prerequisites

Before proceeding, make sure the following packages are installed:

- `peewee` - an ORM for Python.
- `mysqlclient` - a MySQL database connector for Python.
- `bcrypt` - a library for hashing passwords.

## Setup Instructions

### Initial Step: Run Migrations and Seed Data

1. Navigate to the project directory in the terminal.
2. Run the following Artisan commands to migrate the database schema and seed initial data:

   ```bash
   php artisan migrate
   php artisan db:seed

### Final Step: Add default data for device-related tables

1. Add default test data for device model, device type and device variant
