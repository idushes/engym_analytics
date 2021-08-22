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


def prepare_dataset():
    dataset_filename = os.environ.get('DATASET_EXPORT_FILE')
    dataset = Dataset(filename=dataset_filename, save_chunk=1000)
    devices = get_unique_devices()
    for device_id in devices:
        if dataset.exist(key_name='device_id', key=device_id) is False:
            user_profile = get_user_profile(device_id)
            df = pd.DataFrame.from_dict([user_profile])
            dataset.append(df)
    dataset.save()


def send_to_amplitude():
    # response = send_events([])
    pass