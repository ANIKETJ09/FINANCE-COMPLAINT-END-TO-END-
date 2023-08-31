import requests
import os
from datetime import datetime
from dataclasses import dataclass
from typing import List
import json
import re
import time
import pandas as pd
import uuid
import logging
import pandas as pd
import dill
log_dir = os.path.join("logs")
os.makedirs("logs", exist_ok=True)
file_path = os.path.join(log_dir, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

BUCKET_NAME = "dl-fc-datapipeline"

DATA_SOURCE_URL = f"https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/" \
                  f"?date_received_max=<todate>&date_received_min=<fromdate>" \
                  f"&field=all&format=json"
DIR_NAME = "census_consumer_complaint_data"
ROOT_DIR = os.path.join(os.getcwd(), DIR_NAME, "data_source")

DOWNLOAD_DIR = os.path.join(ROOT_DIR, "download")
FEATURE_STORE_DIR = os.path.join(ROOT_DIR, "feature_store")
_object_file_path = os.path.join(ROOT_DIR, ".finance_complaint", "finance_complaint_data_obj")
_object_file_dir = os.path.dirname(_object_file_path)
FEATURE_STORE_BUCKET_URI =  f"s3://{BUCKET_NAME}/feature_store"
OBJECT_STATUS_BUCKET_URI =  f"s3://{BUCKET_NAME}/object_status"
logging.basicConfig(filename=file_path, filemode="w",
                    format='[%(asctime)s] \t%(levelname)s \t%(lineno)d \t%(filename)s \t%(funcName)s() \t%(message)s',
                    level=logging.INFO)
def sync_data_from_s3_bucket():
    feature_store_bucket_url =FEATURE_STORE_BUCKET_URI
    logging.info("Started syncing data from s3 bucket")
    command = f"aws s3 sync  {FEATURE_STORE_BUCKET_URI} {FEATURE_STORE_DIR}"
    print(command)
    os.system(command)
    logging.info("Started syncing previous run status")
    object_status_bucket_url =f"s3://{BUCKET_NAME}/object_status"
    command = f"aws s3 sync  {OBJECT_STATUS_BUCKET_URI} {_object_file_dir}"
    logging.info(f"Running command {command}")
    os.system(command)

def sync_data_to_s3_bucket():
    command = f"aws s3 sync {FEATURE_STORE_DIR} {FEATURE_STORE_BUCKET_URI}"
    logging.info(f"Running command {command}")
    os.system(command)
    command = f"aws s3 sync  {_object_file_dir} {OBJECT_STATUS_BUCKET_URI}"
    logging.info(f"Running command {command}")
    os.system(command)


@dataclass
class DownloadUrl:
    url: str
    file_path: str
    n_retry: int
    from_date: str
    to_date: str


def save_object(obj,file_path):
    try:
        logging.info(obj)
        with open(file_path, "wb") as obj_file:
            logging.info(f"Saving data pipeline object at {_object_file_path}")
            dill.dump(obj, obj_file)
    except Exception as e:
        raise e

def load_object(file_path):
    self = None
    print(file_path)
    if os.path.exists(file_path):
        logging.info(f"Previous object found {file_path} hence loading previous object")
        with open(file_path, "rb") as object_file:
            obj = dill.load(object_file)
            logging.info(obj)
            return obj


class FinanceComplaintParquetData:
    data_source_url = DATA_SOURCE_URL

    def __init__(self, download_dir: str = DOWNLOAD_DIR, feature_store_dir: str = FEATURE_STORE_DIR, n_retry: int = 5):
        os.makedirs(os.path.dirname(_object_file_path), exist_ok=True)
        self.n_retry = n_retry
        self.file_name = "finance_complaint"
        self._min_start_date = "2011-12-01"
        self.feature_store_dir = feature_store_dir
        self.feature_store_file_path = os.path.join(self.feature_store_dir, f"{self.file_name}.parquet")
        self.dir_path = os.path.join(download_dir, datetime.now().strftime("%Y%m%d_%H%M%S"))
        self.file_path = os.path.join(self.dir_path, self.file_name)
        previous_self = load_object(file_path=_object_file_path)

        if not hasattr(previous_self, "ingested_to_date"):
            self.to_date = None
        else:
            logging.info(f"Data is ingested till: {previous_self.ingested_to_date} ")
            self.to_date = previous_self.ingested_to_date

    def is_data_pipeline_ran_today(self) -> False:
        try:
            intervals = self.get_required_interval()
            if intervals[0] == intervals[-1]:
                logging.info("Pipeline ran todays hence data ingestion will not start today.")
                return True
            return False
        except Exception as e:
            raise e

    def get_required_interval(self):
        try:
            if self.to_date is not None:
                start_date = self.to_date
            else:
                start_date = self._min_start_date
            start_date = datetime.strptime(start_date, "%Y-%m-%d")

            end_date = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
            # end_date =datetime.strptime("2013-02-01", "%Y-%m-%d")
            logging.info(f"Data from {start_date} to {end_date} will be ingested into feature store.")
            n_diff_days = (end_date - start_date).days
            freq = None
            if n_diff_days > 365:
                freq = "Y"
            elif n_diff_days > 30:
                freq = "M"
            elif n_diff_days > 7:
                freq = "W"
            logging.debug(f"{n_diff_days} hence freq: {freq}")
            if freq is None:
                intervals = pd.date_range(start=start_date,
                                          end=end_date,
                                          periods=2).astype('str').tolist()
            else:

                intervals = pd.date_range(start=start_date,
                                          end=end_date,
                                          freq=freq).astype('str').tolist()
            logging.debug(f"Prepared Interval: {intervals}")
            if end_date not in intervals:
                intervals.append(end_date.strftime("%Y-%m-%d"))
            return intervals
        except Exception as e:
            raise e






    def extract_convert_save(self, n_day_interval_url: int = None):
        try:
            if self.is_data_pipeline_ran_today():
                logging.info("Pipeline ran today hence not going to run today")
                return None
            # created feature store directory
            os.makedirs(self.feature_store_dir, exist_ok=True)
            required_interval = self.get_required_interval()
            logging.info("Started downloading files")
            for index in range(1, len(required_interval)):
                from_date, to_date = required_interval[index - 1], required_interval[index]
                logging.debug(f"Generating data download url between {from_date} and {to_date}")
                datasource_url: str = FinanceComplaintParquetData.data_source_url
                url = datasource_url.replace("<todate>", to_date).replace("<fromdate>", from_date)
                logging.debug(f"Url: {url}")
                file_name = f"{self.file_name}_{from_date}_{to_date}.json"
                file_path = os.path.join(self.dir_path, file_name)
                download_url = DownloadUrl(url=url,
                                           file_path=file_path,
                                           n_retry=self.n_retry,
                                           from_date=from_date,
                                           to_date=to_date)

                # startt extracting data
                self.download_data(download_url=download_url)
                logging.info(f"File download completed")

                file_name = os.path.basename(download_url.file_path).replace("json", "parquet")
                parquet_file_path = os.path.join(self.feature_store_dir, file_name)

                # convert and load
                self.convert_n_save_json_to_parquet(json_file_path=download_url.file_path,
                                                    parquet_file_path=parquet_file_path)
                self.ingested_to_date = download_url.to_date
                save_object(obj=self,file_path=_object_file_path)
        except Exception as e:
            raise e

    def __str__(self):
        return f"Date ingested date: {self.ingested_to_date} completed {self.to_date}"

    def download_data(self, download_url: DownloadUrl):
        try:
            logging.info(f"Starting download operation: {download_url}")
            download_dir = os.path.dirname(download_url.file_path)

            # creating download directory
            os.makedirs(download_dir, exist_ok=True)

            # downloading data
            data = requests.get(download_url.url, params={'User-agent': f'your bot {uuid.uuid4()}'})

            try:
                logging.info(f"Started writing downloaded data into json file: {download_url.file_path}")
                # saving downloaded data into hard disk
                with open(download_url.file_path, "w") as file_obj:
                    finance_complaint_data = list(map(lambda x: x["_source"],
                                                      filter(lambda x: "_source" in x.keys(),
                                                             json.loads(data.content)))
                                                  )

                    json.dump(finance_complaint_data, file_obj)
                logging.info(f"Downloaded data has been written into file: {download_url.file_path}")
            except Exception as e:
                logging.info("Failed to download hence retry again.")
                if os.path.exists(download_url.file_path):
                    os.remove(download_url.file_path)
                self.retry_download_data(data, download_url=download_url)

        except Exception as e:
            logging.info(e)
            raise e

    def retry_download_data(self, data, download_url: DownloadUrl):
        """
        This function help to avoid failure as it help to download failed file again

        data:failed response
        download_url: DownloadUrl
        """
        try:
            # if retry still possible try else return the response
            if download_url.n_retry == 0:
                return

            # to handle throatling requestion and can be slove if we wait for some second.
            content = data.content.decode("utf-8")
            logging.info(f"Failed error message: {content}")
            wait_second = re.findall(r'\d+', content)

            if len(wait_second) > 0:
                time.sleep(int(wait_second[0]) + 2)
            # calling download function again to retry
            download_url.n_retry -= 1
            self.download_data(download_url=download_url)
        except Exception as e:
            raise e

    def convert_n_save_json_to_parquet(self, json_file_path, parquet_file_path):
        try:
            if os.path.exists(parquet_file_path):
                os.remove(parquet_file_path)
            df = pd.read_json(json_file_path)
            if df.shape[0]==0:
                return None
            print(f"Total Number of rows in downloaded file: {df.shape[0]}")
            df = df.loc[df.consumer_disputed!= 'N/A']
            print(f"Remaining Number of rows after dropping row which does has 'N/A' in consumer disputed downloaded file: {df.shape[0]}")

            if df.shape[0]==0:
                return None

            logging.info(f"File has {df.shape[0]} row.")
            logging.info(f"Json file {json_file_path} conversion started to parquet format")
            df["consumer_disputed"] = df["consumer_disputed"].replace({"Yes":'1',"No":"0"})
            df["consumer_disputed"] = df["consumer_disputed"].astype(int)
            df.to_parquet(parquet_file_path)


        except Exception as e:
            raise e


# def start_data_collection():
#     fc = FinanceComplaintParquetData()
#     fc.extract_convert_save()
#
#
# #
# start_data_collection()
