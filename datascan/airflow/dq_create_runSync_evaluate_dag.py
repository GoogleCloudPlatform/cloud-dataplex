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


"""Airflow dag to create, run and evaluate data quality scan"""
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from google.cloud import dataplex_v1
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateOrUpdateDataQualityScanOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexGetDataQualityScanResultOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexRunDataQualityScanOperator
from datetime import timedelta, datetime

"""replace project id and region with user project and the region respectively"""
PROJECT_ID = "test-project"
REGION = "us-central1"
DATA_SCAN_ID = "airflow-data-quality-scan"

"""replace EXPORT_TABLE for your setup"""
# Table where datascan job results should be exported to.
EXPORT_TABLE = "//bigquery.googleapis.com/projects/test-project/datasets/test_dataset/tables/results_table"

EXAMPLE_DATA_SCAN = dataplex_v1.DataScan()

EXAMPLE_DATA_SCAN.data.resource = (
    f"//bigquery.googleapis.com/projects/bigquery-public-data/datasets/austin_bikeshare/tables/bikeshare_stations"
)
EXAMPLE_DATA_SCAN.data_quality_spec = {
    "sampling_percent": 100,
    "row_filter": "station_id > 1000",
    "post_scan_actions": {"bigquery_export": {"results_table": EXPORT_TABLE } },
    "rules": [
        {
            "non_null_expectation": {},
            "column": "address",
            "dimension": "VALIDITY",
            "threshold": 0.99
        },
        {
            "range_expectation": {
                "min_value": "1",
                "max_value": "10",
                "strict_max_enabled": False,
                "strict_min_enabled": True
            },
            "column": "council_district",
            "ignore_null": True,
            "dimension": "VALIDITY",
            "threshold": 0.9
        },
        {
            "regex_expectation": {
                "regex": ".*solar.*"
            },
            "column": "power_type",
            "dimension": "VALIDITY",
            "ignore_null": False
        },
        {
            "set_expectation": {
                "values": [
                    "sidewalk",
                    "parkland"
                ]
            },
            "column": "property_type",
            "ignore_null": False,
            "dimension": "VALIDITY"
        },
    ],
}

default_args = {
    'start_date': pendulum.today('UTC').add(days=0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'create_runSync_evaluate',
    default_args=default_args,
    description='Test custom dataplex data quality scan create->run_sync->evaluate flow',
    schedule='10 10 * * *',
    start_date=datetime.now(),
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=20)
    )

create_data_scan = DataplexCreateOrUpdateDataQualityScanOperator(
        task_id="create_data_scan",
        project_id=PROJECT_ID,
        region=REGION,
        body=EXAMPLE_DATA_SCAN,
        data_scan_id=DATA_SCAN_ID,
    )

run_data_scan = DataplexRunDataQualityScanOperator(
    task_id="run_data_scan",
    project_id=PROJECT_ID,
    region=REGION,
    dag=dag,
    data_scan_id=DATA_SCAN_ID
)

get_job_result = DataplexGetDataQualityScanResultOperator(
            task_id="get_data_scan_job_result",
            project_id=PROJECT_ID,
            region=REGION,
            dag=dag,
            data_scan_id=DATA_SCAN_ID)

def process_data_from_data_scan_job(**kwargs):
    ti = kwargs['ti']
    job_data = ti.xcom_pull(task_ids='get_data_scan_job_result')
    if "dataQualityResult" not in job_data:
        return "failed_job"
    print(f"data quality job result: {job_data.get('dataQualityResult')}")
    if "passed" in job_data["dataQualityResult"]:
        return "passed_job"
    return "failed_job"


def pass_job(**kwargs):
    print("data quality job passed")

def fail_job(**kwargs):
    print("data quality job failed")

passed_job = PythonOperator(
    task_id = "passed_job",
    python_callable=pass_job,
    dag=dag
    )

failed_job = PythonOperator(
    task_id = "failed_job",
    python_callable=fail_job,
    dag=dag
    )

process_data = BranchPythonOperator(
      task_id='process_data_from_data_scan_job',
      python_callable=process_data_from_data_scan_job,
      dag=dag
      )

create_data_scan >> run_data_scan >> get_job_result >> process_data >> passed_job
create_data_scan >> run_data_scan >> get_job_result >> process_data >> failed_job
