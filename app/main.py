from apscheduler.schedulers.blocking import BlockingScheduler
import logging, os
from worker1 import worker1
from dotenv import load_dotenv
from helpers.pretty import bcolors

load_dotenv()

DEBUG = os.environ.get('DEBUG', '') == 'True'
if DEBUG:
    logging.basicConfig()
    logging.getLogger('apscheduler').setLevel(logging.DEBUG)

job_defaults = {
    'max_instances': 1
}

scheduler = BlockingScheduler(job_defaults=job_defaults)
print(bcolors.OKGREEN + "ENGYM CORP Analytics module has been started!" + bcolors.ENDC)

worker1(scheduler)

scheduler.start()

