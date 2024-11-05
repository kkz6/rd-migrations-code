from peewee import MySQLDatabase

# Define the MySQL database connection
source_db = MySQLDatabase(
    "rd_test_db_old_data", user="root", password="password", host="127.0.0.1", port=3306
)
