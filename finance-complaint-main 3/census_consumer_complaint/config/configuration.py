import os
import datetime
from census_consumer_complaint.ml.transform import feature_engineering
from census_consumer_complaint.ml.model import trainer
# from census_consumer_complaint.component.model_training import trainer
from dateutil.tz import gettz
ROOT_DIR = os.path.join(os.getcwd(),"census_consumer_complaint_data")
PIPELINE_NAME = "census_consumer_complaint"
from data_pipeleine.finance_data import FEATURE_STORE_DIR
PIPELINE_ARTIFACT = "artifact"
META_DATA_SQLITE_FILE_DIR = "meta_data"
SQLITE_FILE_NAME = "meta_data.db"
LOG_DIR = 'logs'
SERVING_MODEL_DIR = "saved_models"
DAYS = 1  # scheduled interval day
SCHEDULED_INTERVAL = datetime.timedelta(days=DAYS)
START_DATE = datetime.datetime(2022, 11, 2, 18, 45,tzinfo=gettz('Asia/Kolkata'))
TRANSFORM_MODULE_FILE = feature_engineering.__file__
# os.path.join(WORK_DIR, "census_consumer_complaint_component", "feature_engineering",
#                                  "feature_engineering.py")
TRAINER_MODULE_FILE = trainer.__file__


# os.path.join(WORK_DIR, "census_consumer_complaint_component", "model_training",
#                                "trainer.py")


class CensusConsumerConfiguration:

    def __init__(self):
        self.root_dir = os.path.join(ROOT_DIR)
        self.pipeline_name = PIPELINE_NAME
        self.pipeline_root = os.path.join(ROOT_DIR, PIPELINE_NAME, PIPELINE_ARTIFACT)
        self.metadata_path = os.path.join(ROOT_DIR, META_DATA_SQLITE_FILE_DIR, SQLITE_FILE_NAME)
        self.log_dir = os.path.join(ROOT_DIR, LOG_DIR)
        self.serving_model_dir = os.path.join(ROOT_DIR, SERVING_MODEL_DIR)
        self.scheduled_interval = SCHEDULED_INTERVAL
        self.start_date = START_DATE
        self.transform_module_file_path = TRANSFORM_MODULE_FILE
        self.trainer_module_file_path = TRAINER_MODULE_FILE
        self.feature_store_dir = FEATURE_STORE_DIR

