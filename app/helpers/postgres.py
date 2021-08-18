import logging
from psycopg2.extras import LoggingConnection
import os
import psycopg2

level = logging.DEBUG if os.environ.get('DEBUG') == 'True' else logging.ERROR
logging.basicConfig(level=level)
logger = logging.getLogger("loggerinformation")

def get_conn():
    conn = psycopg2.connect(
        database=os.getenv('DATABASE'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST', 'localhost'),
        port=os.getenv('PORT', '5432'),
        connect_timeout=os.getenv('CONNECT_TIMEOUT', '5'),
        connection_factory=LoggingConnection
    )
    conn.initialize(logger)
    return conn
