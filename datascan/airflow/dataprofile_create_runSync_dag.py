# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Airflow dag to create and run data profile scan"""
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from google.cloud import dataplex_v1
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateOrUpdateDataProfileScanOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexGetDataProfileScanResultOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexRunDataProfileScanOperator
from datetime import timedelta, datetime

"""replace project id and region with user project and the region respectively"""
PROJECT_ID = "test-project"
REGION = "us-central1"
DATA_SCAN_ID = "airflow-data-profile-scan"

EXAMPLE_DATA_SCAN = dataplex_v1.DataScan()

EXAMPLE_DATA_SCAN.data.resource = (
    f"//bigquery.googleapis.com/projects/bigquery-public-data/datasets/austin_bikeshare/tables/bikeshare_stations"
)

EXAMPLE_DATA_SCAN.data_profile_spec = {
    "sampling_percent": 100,
    "row_filter": "station_id > 1000",
}

default_args = {
    'start_date': pendulum.today('UTC').add(days=0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'create_runSync',
    default_args=default_args,
    description='Test custom dataplex data profile scan create->run_sync->evaluateflow  ',
    schedule='10 10 * * *',
    start_date=datetime.now(),
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20)
)

create_data_scan = DataplexCreateOrUpdateDataProfileScanOperator(
        task_id="create_data_scan",
        project_id=PROJECT_ID,
        region=REGION,
        body=EXAMPLE_DATA_SCAN,
        data_scan_id=DATA_SCAN_ID,
    )

run_data_scan = DataplexRunDataProfileScanOperator(
    task_id="run_data_scan",
    project_id=PROJECT_ID,
    region=REGION,
    dag=dag,
    data_scan_id=DATA_SCAN_ID
)

get_job_result = DataplexGetDataProfileScanResultOperator(
            task_id="get_data_scan_job_result",
            project_id=PROJECT_ID,
            region=REGION,
            dag=dag,
            data_scan_id=DATA_SCAN_ID)

create_data_scan >> run_data_scan >> get_job_result