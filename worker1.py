import pandas as pd
import os
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from users import get_unique_devices, get_user_profile
# from pprint import pprint
# from tqdm import tqdm
from datetime import datetime
from helpers.dataset import Dataset


load_dotenv()

def worker1(scheduler: BlockingScheduler):
    scheduler.add_job(prepare_dataset, 'interval', days=1, next_run_time=datetime.now())
    # spend_time_analytics()


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
    if df is not None:
        print(df.info)

def send_to_amplitude():
    # response = send_events([])
    pass