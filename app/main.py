from apscheduler.schedulers.blocking import BlockingScheduler
import logging, os
from worker1 import worker1

DEBUG = os.environ.get('DEBUG', '') == 'True'
if DEBUG:
    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)

job_defaults = {
    'max_instances': 1
}

scheduler = BlockingScheduler(job_defaults=job_defaults)

worker1(scheduler)

print("ENGYM CORP Analytics module has been started!")
scheduler.start()

