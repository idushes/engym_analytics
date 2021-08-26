from clickhouse_driver import Client
from dotenv import load_dotenv
import os
from pandas import json_normalize
import json

load_dotenv()

client = Client(
    host = os.environ.get('CH_HOST'),
    user = os.environ.get('CH_USER'),
    password = os.environ.get('CH_PASSWORD'),
    database = os.environ.get('CH_DATABASE')
)

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
    `ip` Nullable(String),
    `insert_id` Nullable(String),
    `session_id` Nullable(Float32),
    `backup_json` Nullable(String),
    `location_lat` Nullable(Float32),
    `location_lng` Nullable(Float32),
    `revenueType` Nullable(String),
    `productId` Nullable(String),
    `quantity` Nullable(Int32),
    `price` Nullable(Float32),
    `user_properties_hide_locale_language` Nullable(String),
    `backend_user_id` Nullable(Float32),
    `user_properties_country` Nullable(String),
    `user_properties_actual_fps` Nullable(String),
    `user_properties_device_type` Nullable(String),
    `user_properties_app_language` Nullable(String),
    `user_properties_gender` Nullable(String),
    `user_properties_balance` Nullable(Float32),
    `user_properties_age_year` Nullable(String),
    `user_properties_parent_type` Nullable(String),
    `user_properties_learned_words` Nullable(Float32),
    `user_properties_session_count` Nullable(Float32),
    `user_properties_all_words_count` Nullable(Float32),
    `event_properties_type` Nullable(String),
    `event_properties_duration` Nullable(Float32),
    `event_properties_name` Nullable(String),
    `event_properties_is_next` Nullable(Float32),
    `event_properties_completed` Nullable(Float32),
    `event_properties_resumed` Nullable(Float32),
    `user_properties_subscription` Nullable(String),
    `event_properties_subscription` Nullable(String),
    `event_properties_subscription_status` Nullable(String),
    `user_properties_last_seen_offer` Nullable(String),
    `user_properties_iad_keyword` Nullable(String),
    `user_properties_iad_campaign` Nullable(String),
    `event_properties_flow` Nullable(String),
    `user_properties_utm_medium` Nullable(String),
    `user_properties_utm_source` Nullable(String),
    `user_properties_utm_campaign` Nullable(String),
    `event_properties_text` Nullable(String),
    `event_properties_solved` Nullable(Float32),
    `event_properties_attempts` Nullable(Float32),
    `event_properties_restarts` Nullable(Float32)
)
    engine = MergeTree()
        PARTITION BY toYYYYMM(time)
        ORDER BY (device_id, time);
"""

client.execute(__SQL_CREATE_TABLE__)


def check_new_columns(events, table_name):
    CREATE_SQL = client.execute(f"SHOW CREATE {table_name}")[0][0]
    unique_keys = []
    for event in events:
        for key in event.keys():
            if key not in CREATE_SQL and key not in unique_keys:
                unique_keys.append(key)
                ADD_COLUMN_SQL = None
                if type(event[key]) == str:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(String);\n"
                elif type(event[key]) == int:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
                elif type(event[key]) == bool:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
                elif type(event[key]) == float:
                    ADD_COLUMN_SQL = f"alter table {table_name}\n add column {key} Nullable(Float32);\n"
                else:
                    print(f"NEW TYPY !!!!! ({key} -> {event[key]})", type(event[key]),)
                if ADD_COLUMN_SQL is not None:
                    client.execute(ADD_COLUMN_SQL)

def insert_events(events, table_name="kids2appevent"):
    if len(events) == 0: return
    df = json_normalize(events, sep="_")
    df = df.fillna('None')
    dict_array = df.to_dict(orient='records')
    check_new_columns(dict_array, table_name=table_name)

    # Костыли !!!!
    for event in dict_array:
        for key, value in event.items():
            if event[key] == 'None': event[key] = None
            if type(event[key]) == bool: event[key] = int(event[key])

        # if 'backend_user_id' in event and type(event['backend_user_id']) == float:
        #     event['backend_user_id'] = int(event['backend_user_id'])

    SQL = f"INSERT INTO {table_name} FORMAT JSONEachRow \n"
    for event in dict_array:
        event['time'] = int(event['time'] / 1000.0)
        json_object = json.dumps(event)
        SQL += f"{json_object}\n"
    SQL += ";"
    client.execute(SQL)