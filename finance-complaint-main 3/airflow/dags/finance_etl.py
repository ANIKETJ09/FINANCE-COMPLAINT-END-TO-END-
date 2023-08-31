#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example DAG demonstrating the usage of the TaskFlow API to execute Python functions natively and within a
virtual environment.
"""
import logging
import shutil
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task

log = logging.getLogger(__name__)

with DAG(
    dag_id='data_collection',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 11, 8, tz="UTC"),
    catchup=False,
    tags=['finance_complaint'],
) as dag:
    # [START howto_operator_python]

    

    @task(task_id="sync_data_from_s3")
    def sync_data_from_s3(ds=None, **kwargs):
        from data_pipeleine.finance_data import sync_data_from_s3_bucket
        sync_data_from_s3_bucket()

    @task(task_id="extarct_conver_save")
    def extract_convert_save(ds=None, **kwargs):
        from data_pipeleine.finance_data import FinanceComplaintParquetData
        FinanceComplaintParquetData().extract_convert_save()

    @task(task_id="sync_data_to_s3")
    def sync_data_to_s3(ds=None, **kwargs):
        from data_pipeleine.finance_data import sync_data_to_s3_bucket
        sync_data_to_s3_bucket()

    sync_data_from_s3  = sync_data_from_s3()
    ingestion = extract_convert_save()
    save_to_s3_bucket = sync_data_to_s3()
    sync_data_from_s3 >> ingestion >> save_to_s3_bucket