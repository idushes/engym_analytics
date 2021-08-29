import logging
from psycopg2.extras import LoggingConnection
import os
import psycopg2
from dotenv import load_dotenv
from tqdm import tqdm
from pandas import json_normalize

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

def get_table_count(table_name):
    SQL = f'SELECT count(*) from {table_name};'
    cur = connection.cursor()
    cur.execute(SQL)
    result = cur.fetchone()
    return result[0]

def get_events(table_name, chunk_size, offset):
    SQL = f"SELECT json FROM {table_name} WHERE clickhouse is NULL ORDER BY id LIMIT {chunk_size} OFFSET {offset}"
    cur = connection.cursor()
    cur.execute(SQL)
    result = cur.fetchall()
    events = [event[0] for event in result]
    df = json_normalize(events, sep="_")
    df = df.fillna('None')
    dict_array = df.to_dict(orient='records')
    # TODO: Костыли !!!!
    for event in dict_array:
        for key, value in event.items():
            if event[key] == 'None': event[key] = None
            if type(event[key]) == bool: event[key] = int(event[key])
    return dict_array

def mark_events(table_name, chunk_size, offset):
    SQL = f"""UPDATE {table_name}
    SET clickhouse = now()
    WHERE ID IN (
      SELECT id from kids2appevent_new WHERE clickhouse is NULL ORDER BY id LIMIT {chunk_size} OFFSET {offset}
    );"""
    cur = connection.cursor()
    cur.execute(SQL)
    connection.commit()

def get_json_columns(table="kids2appevent_new"):
    chunk_size = 10000
    total = get_table_count(table)
    offset = 1
    columns = {}
    pbar = tqdm(total=total, desc="get_json_columns")
    while offset < total:
        events = get_events(table, chunk_size=chunk_size, offset=offset)
        for event in events:
            for key in event.keys():
                if key not in columns.keys():
                    if type(event[key]) == str and event[key] != "" and event[key] != "None":
                        columns[key] = "Nullable(String)"
                    elif type(event[key]) == int:
                        columns[key] = "Nullable(Float32)"
                    elif type(event[key]) == bool:
                        columns[key] = "Nullable(Float32)"
                    elif type(event[key]) == float:
                        columns[key] = "Nullable(Float32)"
                    elif isinstance(event[key], list):
                        columns[key] = "Array(String)"
        offset += chunk_size
        pbar.update(chunk_size)
    pbar.close()
    return columns