from peewee import MySQLDatabase

# Define the MySQL database connection
source_db = MySQLDatabase(
    'source_db_name',
    user='source_user',
    password='source_password',
    host='localhost',
    port=3306
)