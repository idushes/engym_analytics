import pandas as pd
import os
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from helpers.users import get_unique_devices, get_user_profile
from pprint import pprint
from tqdm import tqdm
from datetime import datetime
from helpers.dataset import Dataset
from helpers.postgres import get_events, get_table_count
import matplotlib.pyplot as plt
from helpers.amplitude import send_events
import time
from helpers.clickhouse import insert_events

def current_milli_time():
    return round(time.time() * 1000)

load_dotenv()

def worker1(scheduler: BlockingScheduler):
    # scheduler.add_job(prepare_dataset, 'interval', days=1, next_run_time=datetime.now())
    scheduler.add_job(copy_db_from_postgres_to_clickbase, 'interval', days=1, next_run_time=datetime.now())
    # spend_time_analytics()
    # send_to_amplitude()
    # insert_events([])
    pass

def copy_db_from_postgres_to_clickbase():
    table_name = "kids2appevent_new"
    chunk_size = 10000
    total = get_table_count(table_name)
    offset = 1
    while offset < total:
        events = get_events(table_name, chunk_size=chunk_size, offset=offset)
        events = [event[0] for event in events]
        insert_events(events)
        offset += chunk_size

def prepare_dataset():
    dataset_file_path = os.environ.get('DATASET_EXPORT_PATH')
    dataset = Dataset(path=dataset_file_path, save_chunk=1000)
    devices = get_unique_devices()
    for device_id in devices:
        if dataset.exist(key_name='device_id', key=device_id) is False:
            user_profile = get_user_profile(device_id)
            df = pd.DataFrame.from_dict([user_profile])
            dataset.append(df)
    dataset.save()

def spend_time_analytics():
    dataset_file_path = os.environ.get('DATASET_EXPORT_PATH')
    dataset = Dataset(path=dataset_file_path)
    df = dataset.get_df()
    columns = ['minute', 'video', 'game', 'video_and_game']
    sum_df = pd.DataFrame([], columns=columns)

    for minute in tqdm(range(5, 60)):
        video_devices = 0
        game_devices = 0
        video_and_game_devices = 0
        for index, row in df.iterrows():
            video_leave_minutes = row['video_leave_minutes']
            game_leave_minutes = row['game_leave_minutes']
            if video_leave_minutes > minute: video_devices += 1
            if game_leave_minutes > minute: game_devices += 1
            if (game_leave_minutes + video_leave_minutes) > minute: video_and_game_devices += 1
        new_df = pd.DataFrame([[minute, video_devices, game_devices, video_and_game_devices]], columns=columns)
        sum_df = sum_df.append(new_df)
    file_to_save = os.path.join(dataset_file_path, 'sum_df.csv')
    print(file_to_save)
    sum_df.to_csv(file_to_save, index=False)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def send_to_amplitude():
    dataset_file_path = os.environ.get('DATASET_EXPORT_PATH')
    dataset = Dataset(path=dataset_file_path)
    df = dataset.get_df()
    df = df.query('device_id != ""')
    df['video_and_game_minutes'] = df['video_leave_minutes'] + df['game_leave_minutes']
    df = df.query('video_and_game_minutes >= 15')
    df = df.fillna('')
    events = []
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc='prepare'):
        json = {
            "device_id": row['device_id'],
            "event_type": "played_video_and_games_15_minute",
            "app_version": row['app_versions'],
            "time": current_milli_time(),
            "platform": row['platform'],
            "user_properties": {
                "video_leave_minutes": row['video_leave_minutes'],
                "video_leave_count": row['video_leave_count'],
                "internet_connection_popup_count": row['internet_connection_popup_count'],
                "game_leave_minutes": row['game_leave_minutes'],
                "game_leave_count": row['game_leave_count'],
                "game_leave_unique_count": row['game_leave_unique_count'],
                "video_leave_unique_count": row['video_leave_unique_count'],
                "app_close_minutes": row['app_close_minutes'],
                "loader_screen_leave_mean_sec": row['loader_screen_leave_mean_sec'],
                "bundle_loader_screen_leave_mean_sec": row['bundle_loader_screen_leave_mean_sec'],
                "video_load_screen_leave_mean_sec": row['video_load_screen_leave_mean_sec'],
                "date_start": row['date_start'].isoformat(),
                "date_end": row['date_end'].isoformat(),
                "learned_words": row['learned_words'],
                "actual_fps_mean": row['actual_fps_mean'],
                "welcome_aboard_screen_leave": row['welcome_aboard_screen_leave'],
                "lt_days": row['lt'].days,
                "video_and_game_minutes": row['video_and_game_minutes'],
            }
        }
        if row['user_id'] != "":
            json['user_id'] = row['user_id']
        events.append(json)

    for chunk in tqdm(list(chunks(events, 100)), desc='send'):
        # print(chunk)
        # break
        response = send_events(chunk)
        # time.sleep(.1)











