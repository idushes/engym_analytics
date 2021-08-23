from helpers.postgres import connection
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()

EVENT_TABLENAME = os.environ.get('EVENT_TABLENAME', 'kids2appevent_new')

def __get_unique_by_colmn__(colmn_name, allow_none=False, sql_where=None):
    cur = connection.cursor()
    SQL = f"SELECT distinct(json->'{colmn_name}') as user_id FROM {EVENT_TABLENAME} "
    if sql_where is not None: SQL += sql_where
    cur.execute(SQL)
    data = cur.fetchall()
    data = map(lambda id_ : id_[0], data)
    if not allow_none:
        data = filter(lambda user_id: user_id is not None, data)
    return list(data)

def get_unique_devices():
    """ Отдает ID уникальных устройств, может включать в том числе связанные """
    return __get_unique_by_colmn__('device_id')

def __get_events__(sql_where):
    """ Отдает список событий (df) """
    cur = connection.cursor()
    SQL = f"""
    SELECT
       to_timestamp((json->>'time')::bigint / 1000) as time,
       json ->> 'event_type' as event_type,
       json ->> 'user_id' as user_id,
       json ->> 'device_id' as device_id,
       json ->> 'platform' as platform,
       json ->> 'app_version' as app_version,
       json -> 'event_properties' ->> 'name' as event_properties_name,
       json -> 'user_properties' -> 'learned_words' as user_properties_learned_words,
       json -> 'user_properties' -> 'actual_fps' as user_properties_actual_fps,
       json -> 'event_properties' -> 'duration' as event_properties_duration
    FROM {EVENT_TABLENAME}
    {sql_where}
    ORDER BY json-> 'time'"""
    cur.execute(SQL)
    data = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    df = pd.DataFrame(data=data, columns=colnames)
    df.set_index('time', inplace=True)
    df['user_properties_actual_fps'] = df['user_properties_actual_fps'].astype(float)
    return df

def __get_device_events_df__(device_id):
    """ Отдает события по одному девайсу """
    sql_where = f"WHERE (json->>'device_id') = '{device_id}'"
    return __get_events__(sql_where)

def __get_linked_devices__(user_id):
    """ Отдает события по user_id, осторожно не включает """
    sql_where = f"WHERE (json->>'device_id') = '{user_id}'"
    return __get_events__(sql_where)

def __get_user_events_df__(device_id):
    """ Может содержать связанные пользовательские устройства В БУДУЩЕМ может быть"""
    df = __get_device_events_df__(device_id)
    # df_users = df[df['user_id'].notna()]
    # if not df_users.empty:
    #     user_id = df_users['user_id'][0]
        # __get_linked_devices__(user_id)
    return df

def get_user_profile(device_id):
    df = __get_user_events_df__(device_id)
    df_with_user_id = df[df['user_id'].notna()]
    df_game_leave = df.query('event_type == "game_leave" and event_properties_duration > 5')
    df_video_leave = df.query('event_type == "video_leave" and event_properties_duration > 5')
    df_app_close = df.query('event_type == "app_close"')
    df_loader_screen_leave = df.query('event_type == "loader_screen_leave"')
    df_bundle_loader_screen_leave = df.query('event_type == "bundle_loader_screen_leave"')
    df_video_load_screen_leave = df.query('event_type == "video_load_screen_leave"')
    df_internet_connection_popup = df.query('event_type == "internet_connection_popup"')
    return {
        "app_versions": ' '.join(pd.unique(df['app_version'].dropna())),
        "device_id": device_id,
        "user_id": df_with_user_id['user_id'][0] if not df_with_user_id.empty else None,
        "platform": df['platform'][0] if not df.empty else None,
        "video_leave_minutes": round(df_video_leave['event_properties_duration'].sum() / 60, 1),
        "video_leave_count": df_video_leave['event_type'].count(),
        "internet_connection_popup_count": df_internet_connection_popup['event_type'].count(),
        "game_leave_minutes": round(df_game_leave['event_properties_duration'].sum() / 60, 1),
        "game_leave_count": df_game_leave['event_type'].count(),
        "game_leave_unique_count": len(pd.unique(df_game_leave['event_properties_name'])),
        "video_leave_unique_count": len(pd.unique(df_video_leave['event_properties_name'])),
        "app_close_minutes": round(df_app_close['event_properties_duration'].sum() / 60, 1),
        "loader_screen_leave_mean_sec": df_loader_screen_leave['event_properties_duration'].mean().round(1) if not df_loader_screen_leave.empty else None,
        "bundle_loader_screen_leave_mean_sec": df_bundle_loader_screen_leave['event_properties_duration'].mean().round(1) if not df_bundle_loader_screen_leave.empty else None,
        "video_load_screen_leave_mean_sec": df_video_load_screen_leave['event_properties_duration'].mean().round(1) if not df_video_load_screen_leave.empty else None,
        "date_start": df.index.min(),
        "date_end": df.index.max(),
        "learned_words": df['user_properties_learned_words'].max(),
        "actual_fps_mean": df['user_properties_actual_fps'].mean(),
        "welcome_aboard_screen_leave": not df.query('event_type == "wellcome_adboard_screen_leave" or event_type == "premium_welcome_aboard_screen_leave"').empty,
        "lt": df.index.max() - df.index.min()
    }
