import os
import mysql.connector
from mysql.connector import pooling

db_config = {
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "database": os.getenv('DB_DATABASE'),
    "charset": os.getenv('DB_CHARSET'),
    "pool_name": os.getenv('DB_POOLNAME'),
    "pool_size": int(os.getenv('DB_POOL_SIZE', 30)),  # Adjust pool size as per your application's needs
}

try:
    cnxpool = pooling.MySQLConnectionPool(**db_config)
except mysql.connector.Error as err:
    print(f"Error: {err}")
    cnxpool = None

def get_db_connection():
    """ Connect to MySQL database """
    try:
        if cnxpool:
            return cnxpool.get_connection()
        else:
            return mysql.connector.connect(**db_config)
    except mysql.connector.Error as err:
        print(f"Error getting connection from pool: {err}")
        return None
