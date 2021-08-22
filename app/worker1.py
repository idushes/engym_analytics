import pandas as pd
import os
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from users import get_unique_devices, get_user_profile
# from pprint import pprint
# from tqdm import tqdm
from datetime import datetime


load_dotenv()

def worker1(scheduler: BlockingScheduler):
    scheduler.add_job(prepare_dataset, 'interval', days=1, next_run_time=datetime.now())


def prepare_dataset():
    profiles = []
    devices = get_unique_devices()
    for device_id in devices:
        user_profile = get_user_profile(device_id)
        profiles.append(user_profile)
    file_path = os.environ.get('DATASET_EXPORT_FILE')
    df = pd.DataFrame.from_dict(profiles)
    df.to_csv(file_path)



def send_to_amplitude():
    # response = send_events([])
    pass