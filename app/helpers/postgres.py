import logging
from psycopg2.extras import LoggingConnection
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

level = logging.DEBUG if os.environ.get('DEBUG_POSTGRES') == 'True' else logging.ERROR
logging.basicConfig(level=level)
logger = logging.getLogger("loggerinformation")

connection = psycopg2.connect(
            database=os.environ.get('DATABASE'),
            user=os.environ.get('USER'),
            password=os.environ.get('PASSWORD'),
            host=os.environ.get('HOST', 'localhost'),
            port=os.environ.get('PORT', '5432'),
            connect_timeout=os.environ.get('CONNECT_TIMEOUT', '5'),
            connection_factory=LoggingConnection
        )
connection.initialize(logger)

