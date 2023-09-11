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

"""Airflow dag to run and evaluate data quality scan with custom configurations"""
import pendulum
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import get_current_context
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexCreateOrUpdateDataQualityScanOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexGetDataQualityScanResultOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexRunDataQualityScanOperator
from datetime import timedelta, datetime
from google.cloud import dataplex_v1

"""Following are the default parameeters for the project id, region and data scan id respectively, and used as default values 
   if no config valuess are provided.
"""
DEFAULT_PROJECT_ID = "test-project"
DEFAULT_REGION = "us-central1"
DEFAULT_DATA_SCAN_ID = "airflow-data-quality-scan"

default_args = {
    'start_date': pendulum.today('UTC').add(days=0),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'runSync_evaluate_export_config',
    default_args=default_args,
    description='Test custom dataplex data quality scan run->evaluate flow with custom configs',
    schedule_interval='0 0 * * *',
    start_date=datetime.now(),
    max_active_runs=2,
    catchup=False,
    render_template_as_native_obj=True,
    dagrun_timeout=timedelta(minutes=20)
    )

def run_scan(**kwargs):
    params = kwargs['params']
    
    #trigger run scan 
    run_data_scan_task = DataplexRunDataQualityScanOperator(
        task_id="run_data_scan_v1",
        project_id=params.get("project_id", DEFAULT_PROJECT_ID),
        region=params.get("region", DEFAULT_REGION),
        dag=dag,
        data_scan_id=params.get("data_scan_id", DEFAULT_DATA_SCAN_ID)
    )
    run_data_scan_task.execute(dict())

def get_scan_result(**kwargs):
    params = kwargs['params']
    
    #trigger job result scan 
    get_job_result_task = DataplexGetDataQualityScanResultOperator(
        task_id="get_data_scan_job_result_v1",
        project_id=params.get("project_id", DEFAULT_PROJECT_ID),
        region=params.get("region", DEFAULT_REGION),
        dag=dag,
        data_scan_id=params.get("data_scan_id", DEFAULT_DATA_SCAN_ID)
    )
    return get_job_result_task.execute(dict())

run_data_scan = PythonOperator(
    task_id = "run_data_scan",
    python_callable=run_scan,
    dag=dag,
    provide_context=True
)

get_job_result = PythonOperator(
    task_id="get_data_scan_job_result",
    python_callable=get_scan_result,
    dag=dag,
    provide_context=True
)

def process_data_from_data_scan_job(**kwargs):
    ti = kwargs['ti']
    job_data = ti.xcom_pull(task_ids='get_data_scan_job_result')
    if "dataQualityResult" not in job_data:
        return "failed_job"
    print(f"data quality job result: {job_data.get('dataQualityResult')}")
    if "passed" in job_data["dataQualityResult"]:
        return "passed_job"
    return "failed_job"

def pass_job(ti=None):
    print("data quality job passed")

def fail_job(ti=None):
    print("data quality job failed")

passed_job = PythonOperator(
    task_id = "passed_job",
    python_callable=pass_job,
    dag=dag,
    provide_context=True
    )

failed_job = PythonOperator(
    task_id = "failed_job",
    python_callable=fail_job,
    dag=dag,
    provide_context=True
    )

process_data = BranchPythonOperator(
      task_id='process_data_from_data_scan_job',
      python_callable=process_data_from_data_scan_job,
      dag=dag,
      provide_context=True
      )

run_data_scan >> get_job_result >> process_data >> passed_job
run_data_scan >> get_job_result >> process_data >> failed_job