import psycopg
from config import DB_PORT, DB_HOST, DB_NAME, DB_PASSWORD, DB_USER
import logging

def get_connection():
    try:
        conn = psycopg.connect(
            host = DB_HOST,
            port = DB_PORT,
            user = DB_USER,
            password = DB_PASSWORD,
            dbname = DB_NAME 
    )
        logging.info('Successful connection to PostgreSQL')
        return conn

    except psycopg.OperationalError as e:
        logging.info(f'Error connecting to database: {e}')
        raise

    
    