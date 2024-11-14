from peewee import MySQLDatabase

# Define the MySQL database connection
dest_db = MySQLDatabase(
    'rd_cms',
    user='root',
    password='',
    host='127.0.0.1',
    port=3306
)