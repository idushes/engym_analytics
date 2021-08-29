from clickhouse_driver import Client, connect
from dotenv import load_dotenv
import os
from pandas import json_normalize
import json
import logging
import copy


load_dotenv()
logger = logging.getLogger("clickhouse")


HOST=os.environ.get('CH_HOST')
USER=os.environ.get('CH_USER')
PASSWORD=os.environ.get('CH_PASSWORD')
DATABASE=os.environ.get('CH_DATABASE')

_connection = None

def get_connection():
    # if _connection is not None and _connection.is
    return connect(host=HOST, user=USER, password=PASSWORD, database=DATABASE, connect_timeout=10)

def prepare_table(columns, table_name="kids2appevent"):
    with get_connection() as conn:
        with conn.cursor() as cursor:
            SQL_CREATE_TABLE = f"""create table IF NOT EXISTS {table_name}
            (
                `event_id` Nullable(Float32),
                `time` DateTime,
                `user_id` Nullable(String),
                `device_id` String,
                `event_type` String,
                `app_version` Nullable(String),
                `platform` Nullable(String),
                `os_name` Nullable(String),
                `os_version` Nullable(String),
                `device_model` Nullable(String),
                `country` Nullable(String),
                `language` Nullable(String),
                `revenue` Nullable(Float32),
                `ip` Nullable(IPv4),
                `insert_id` Nullable(String),
                `session_id` Nullable(Float32)
            )
                engine = ReplacingMergeTree()
                    PARTITION BY toYYYYMM(time)
                    ORDER BY (device_id, time, event_type);
            """
            cursor.execute(SQL_CREATE_TABLE)
            cursor.execute(f"SHOW CREATE {table_name}")
            CREATE_SQL = cursor.fetchall()[0][0]
            for key in columns.keys():
                if key not in CREATE_SQL:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} {columns[key]};\n"
                    logger.info(ADD_COLUMN_SQL)
                    cursor.execute(ADD_COLUMN_SQL)

# def sql_execute(SQL, exception_data=""):
#     client = Client(
#         host=os.environ.get('CH_HOST'),
#         user=os.environ.get('CH_USER'),
#         password=os.environ.get('CH_PASSWORD'),
#         database=os.environ.get('CH_DATABASE')
#     )
#     data = None
#     try:
#         data = client.execute(SQL)
#     except Exception as e:
#         message = "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
#         if exception_data != "":
#             message += exception_data
#             logger.error(message + str(e))
#         else:
#             logger.error(message + str(e))
#     client.disconnect()
#     return copy.deepcopy(data)

# def check_new_columns(events, table_name):
#     connect_ = connect(host=HOST, user=USER, password=PASSWORD, database=DATABASE, connect_timeout=10)
#     with connect_ as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(f"SHOW CREATE {table_name}")
#             CREATE_SQL = cursor.fetchall()[0][0]
#             unique_keys = []
#             for event in events:
#                 for key in event.keys():
#                     if key not in CREATE_SQL and key not in unique_keys:
#                         unique_keys.append(key)
#                         ADD_COLUMN_SQL = None
#                         if type(event[key]) == str and event[key] != "" and event[key] != "None":
#                             ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(String);\n"
#                             print(f"`{key}` Nullable(String),", event[key], type(event[key]))
#                         elif type(event[key]) == int:
#                             ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
#                             print(f"`{key}` Nullable(Float32),", event[key], type(event[key]))
#                         elif type(event[key]) == bool:
#                             ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
#                             print(f"`{key}` Nullable(Float32),", event[key], type(event[key]))
#                         elif type(event[key]) == float:
#                             ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
#                             print(f"`{key}` Nullable(Float32),", event[key], type(event[key]))
#                         elif isinstance(event[key], list):
#                             ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Array(String);\n"
#                             print(f"`{key}` Array(String),", event[key], type(event[key]))
#                         if ADD_COLUMN_SQL is not None:
#                             cursor.execute(ADD_COLUMN_SQL)
#                             cursor.fetchone()

def insert_events(events, table_name="kids2appevent", exception_data=""):
    if len(events) == 0: return
    SQL = f"INSERT INTO {table_name} FORMAT JSONEachRow \n"
    for event in events:
        event['time'] = int(event['time'] / 1000.0)
        json_object = json.dumps(event)
        SQL += f"{json_object}\n"
    SQL += ";"
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(SQL)
            conn.close()
    # sql_execute(SQL, exception_data=exception_data)