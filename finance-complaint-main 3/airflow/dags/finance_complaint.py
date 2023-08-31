from census_consumer_complaint.orchestrator.airflow_orchestrator import get_airflow_dag_pipeline
# from airflow.sensors.external_task import ExternalTaskSensor
# from airflow.operators.bash import BashOperator
# from airflow import DAG
# import airflow
# from datetime import datetime, timedelta
# from dateutil.tz import gettz
#
from census_consumer_complaint.config.configuration import CensusConsumerConfiguration
from data_pipeleine.finance_data import FEATURE_STORE_BUCKET_URI,FEATURE_STORE_DIR
census_config = CensusConsumerConfiguration()
BUCKET_NAME ="dl-fc-ml-artifact"
S3_ARTIFACT_URI = f"s3://{BUCKET_NAME}/census_consumer_complaint/artifact"
S3_SAVED_MODEL_URI = f"s3://{BUCKET_NAME}/saved_models"
ARTIFACT_DIR = census_config.pipeline_root
SAVED_MODELS_DIR = census_config.serving_model_dir
#
DAG = get_airflow_dag_pipeline()
from airflow.operators.bash_operator import BashOperator
from airflow.decorators import task


DAG.get_task("Pusher") >>  BashOperator(task_id="SyncDataToS3Bucket",bash_command=f"aws s3 sync {ARTIFACT_DIR} {S3_ARTIFACT_URI}") >> BashOperator(task_id="SyncModelToS3Bucket",bash_command=f"aws s3 sync {SAVED_MODELS_DIR} {S3_SAVED_MODEL_URI}")
BashOperator(task_id="SyncDataSource",bash_command=f"aws s3 sync {FEATURE_STORE_BUCKET_URI} {FEATURE_STORE_DIR}") >> DAG.get_task("FileBasedExampleGen") 


#
#
# # artifact_dir="/home/avnish/iNeuron_Private_Intelligence_Limited/project_neuron/finance-complaint/census_consumer_complaint_data/census_consumer_complaint"
# # metadata_dir="/home/avnish/iNeuron_Private_Intelligence_Limited/project_neuron/finance-complaint/census_consumer_complaint_data/meta_data"
# # model_dir = "/home/avnish/iNeuron_Private_Intelligence_Limited/project_neuron/finance-complaint/census_consumer_complaint_data/saved_models"
# # sync_metadata_from_s3 = BashOperator(task_id="sync_metadata_from_s3", bash_command=f"aws s3 sync  s3://{BUCKET_NAME}/metadata  {metadata_dir}")
# # sync_data_from_s3 = BashOperator(task_id="sync_data_from_s3", bash_command=f"aws s3 sync s3://{BUCKET_NAME} {artifact_dir}")
# # sync_data_to_s3 = BashOperator(task_id="sync_data_to_s3", bash_command=f"aws s3 sync  {artifact_dir} s3://{BUCKET_NAME}")
# # sync_metadata_to_s3 = BashOperator(task_id="sync_metadata_to_s3", bash_command=f"aws s3 sync  {metadata_dir} s3://{BUCKET_NAME}/metadata")
# # sync_model_to_s3 = BashOperator(task_id="sync_model_to_s3", bash_command=f"aws s3 sync {model_dir} s3://{BUCKET_NAME}/saved_models")
# # # DAG.add_task(sync_metadata_from_s3)
# # # DAG.add_task(sync_data_from_s3)
# # DAG.add_task(sync_data_to_s3)
# # # DAG.add_task(sync_metadata_to_s3)
# # # DAG.add_task(sync_model_to_s3)
# # DAG.get_task("FileBasedExampleGen") >> DAG.get_task('sync_data_to_s3')
# # #sync_data_from_s3 >>sync_metadata_from_s3 >>DAG.get_task("FileBasedExampleGen") >> sync_data_to_s3 >> sync_metadata_to_s3 >> sync_model_to_s3
#
#
#
