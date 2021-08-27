from clickhouse_driver import Client
from dotenv import load_dotenv
import os
from pandas import json_normalize
import json
import logging
import copy


load_dotenv()
logger = logging.getLogger("clickhouse")


__SQL_CREATE_TABLE__ = """create table IF NOT EXISTS engym.kids2appevent
(
    `uuid` UInt64,
    `event_id` Nullable(Float32),
    `time` DateTime,
    `user_id` Nullable(String),
    `device_id` String,
    `event_type` String,
    `app_version` Nullable(String),
    `platform` Nullable(String),
    `os_name` Nullable(String),
    `os_version` Nullable(String),
    `device_brand` Nullable(String),
    `device_model` Nullable(String),
    `country` Nullable(String),
    `city` Nullable(String),
    `region` Nullable(String),
    `dma` Nullable(String),
    `language` Nullable(String),
    `revenue` Nullable(Float32),
    `ip` Nullable(IPv4),
    `insert_id` Nullable(String),
    `session_id` Nullable(Float32),
    `location_lat` Nullable(Float32),
    `location_lng` Nullable(Float32),
    `revenueType` Nullable(String),
    `productId` Nullable(String),
    `quantity` Nullable(Int32),
    `price` Nullable(Float32)
)
    engine = MergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY (device_id, time);
"""



def sql_execute(SQL, exception_data=""):
    client = Client(
        host=os.environ.get('CH_HOST'),
        user=os.environ.get('CH_USER'),
        password=os.environ.get('CH_PASSWORD'),
        database=os.environ.get('CH_DATABASE')
    )
    data = None
    try:
        data = client.execute(SQL)
    except Exception as e:
        message = "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
        if exception_data != "":
            message += exception_data
            logger.error(message + str(e))
        else:
            logger.error(message + str(e))
    client.disconnect()
    return copy.deepcopy(data)

def check_new_columns(events, table_name):
    CREATE_SQL = sql_execute(f"SHOW CREATE {table_name}")[0][0]
    unique_keys = []
    for event in events:
        for key in event.keys():
            if key not in CREATE_SQL and key not in unique_keys:
                unique_keys.append(key)
                ADD_COLUMN_SQL = None
                if type(event[key]) == str and event[key] != "" and event[key] != "None":
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(String);\n"
                    print(f"`{key}` Nullable(String),", event[key], type(event[key]))
                elif type(event[key]) == int:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
                    print(f"`{key}` Nullable(Float32),", event[key], type(event[key]))
                elif type(event[key]) == bool:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
                    print(f"`{key}` Nullable(Float32),", event[key], type(event[key]))
                elif type(event[key]) == float:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
                    print(f"`{key}` Nullable(Float32),", event[key], type(event[key]))
                elif isinstance(event[key], list):
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Array(String);\n"
                    print(f"`{key}` Array(String),", event[key], type(event[key]))
                if ADD_COLUMN_SQL is not None:
                    sql_execute(ADD_COLUMN_SQL, exception_data=ADD_COLUMN_SQL)

def insert_events(events, table_name="kids2appevent", exception_data=""):
    if len(events) == 0: return
    df = json_normalize(events, sep="_")
    df = df.fillna('None')
    dict_array = df.to_dict(orient='records')

    # Костыли !!!!
    for event in dict_array:
        for key, value in event.items():
            if event[key] == 'None': event[key] = None
            if type(event[key]) == bool: event[key] = int(event[key])

    SQL = f"INSERT INTO {table_name} FORMAT JSONEachRow \n"
    for event in dict_array:
        event['time'] = int(event['time'] / 1000.0)
        json_object = json.dumps(event)
        SQL += f"{json_object}\n"
    SQL += ";"
    sql_execute(SQL, exception_data=exception_data)