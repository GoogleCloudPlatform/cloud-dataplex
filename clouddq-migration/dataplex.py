# Copyright 2024 Google LLC
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

from google.cloud import dataplex_v1
from google.cloud import storage
from permission import check_bucket_permission
import yaml
import zipfile
from io import BytesIO

def get_yaml_data(source_project, source_region, lake_id, task_id):
    '''
        Method to get the data quality yaml spec
    '''
    # Create a client
    client = dataplex_v1.DataplexServiceClient()

    # Initialize request argument(s)
    request = dataplex_v1.GetTaskRequest(
        name=f"projects/{source_project}/locations/{source_region}/lakes/{lake_id}/tasks/{task_id}",
    )

    # Make the request
    response = client.get_task(request=request)

    file_uri = response.spark.file_uris[-1]
    bucket_name = file_uri.split('/')[-2]
    file_name = file_uri.split('/')[-1]

    if not check_bucket_permission(bucket_name):
        raise PermissionError(f"Permission is denied on the bucket '{bucket_name}'.")

    if file_uri.endswith('.zip'):
        yaml_data = unzip_and_read_yaml(bucket_name, file_name)
    else:
        yaml_data = read_yaml_file(bucket_name, file_name)

    trigger_spec = response.trigger_spec
    return yaml_data, trigger_spec

def unzip_and_read_yaml(bucket_name, zip_file_name) -> dict:
    '''
        Method to unzip and read the dq yaml spec files
    '''

    # Initialize a client for Google Cloud Storage
    storage_client = storage.Client()

    # Get the bucket and the blob (zip file)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(zip_file_name)

    # Download the zip file's contents into memory
    zip_data = BytesIO(blob.download_as_bytes())

    # Variable to store all YAML data
    yaml_data = {}

    # Open the zip file from memory
    with zipfile.ZipFile(zip_data, 'r') as zip_ref:
        # Iterate over each file in the zip archive
        for file_name in zip_ref.namelist():
            # Only process .yaml or .yml files
            if file_name.endswith('.yaml') or file_name.endswith('.yml'):
                # Read the content of the YAML file
                with zip_ref.open(file_name) as file:
                    file_data = yaml.safe_load(file)
                    for key, value in file_data.items():
                        if key in yaml_data.keys():
                            value.update(yaml_data[key])
                            yaml_data[key] = value
                        else:
                            yaml_data.update(file_data)
    return yaml_data

def read_yaml_file(bucket_name, file_name) -> dict:
    '''
        Method to read the yaml file from the storage bucket
    '''
    # Initialize a client
    client = storage.Client()

    # Get the bucket
    bucket = client.get_bucket(bucket_name)

    # Get the blob (file)
    blob = bucket.blob(file_name)

    # Download the contents of the file
    content = blob.download_as_string()
    yaml_data = yaml.safe_load(content)

    return yaml_data

def convert_config_to_payload(config):
    '''
        Method to convert a config into payload
    '''
    # Initialize request argument(s)
    data_scan = dataplex_v1.DataScan()

    data_scan.data.resource = config.get('resource')
    data_scan.data.entity = config.get('entity')
    if 'description' in config:
        data_scan.description = config['description']
    if 'displayName' in config:
        data_scan.display_name = config['displayName']
    if 'labels' in config:
        data_scan.labels = config['labels']
    if 'samplingPercent' in config['dataQualitySpec']:
        data_scan.data_quality_spec.sampling_percent = config['dataQualitySpec']['samplingPercent']
    else:
        data_scan.data_quality_spec.sampling_percent = 10
    data_scan.data_quality_spec.rules = config['dataQualitySpec']['rules']
    if 'rowFilter' in config['dataQualitySpec']:
        data_scan.data_quality_spec.row_filter = config['dataQualitySpec']['rowFilter']
    if 'postScanActions' in config['dataQualitySpec']:
        data_scan.data_quality_spec.post_scan_actions.bigquery_export.results_table = config['dataQualitySpec']['postScanActions']['bigqueryExport']['resultsTable']
    if 'executionSpec' in config and 'trigger' in config['executionSpec'] and 'schedule' in config['executionSpec']['trigger']:
        data_scan.execution_spec.trigger.schedule.cron = config['executionSpec']['trigger']['schedule']['cron']
    else:
       data_scan.execution_spec.trigger.on_demand = {}
    if 'executionSpec' in config and 'incrementalField' in config['executionSpec']:
        data_scan.execution_spec.field = config['executionSpec']['incrementalField']
    return data_scan

def create_datascan(gcp_project_id, location_id, datascan_id, datascan):
    '''
        Method to create a data scan
    '''
    try:
        # Create a client
        client = dataplex_v1.DataScanServiceClient()
        request = dataplex_v1.CreateDataScanRequest(
            parent=f"projects/{gcp_project_id}/locations/{location_id}",
            data_scan=datascan,
            data_scan_id=datascan_id,
        )
        print(f'Creating Datascan: {datascan_id}')
        # Make the request
        operation = client.create_data_scan(request=request)
        response = operation.result()
        return response
    except Exception as error:
        print(f'Error: Failed to create {datascan_id}. ')
        print(error)
        return None

def list_lakes(gcp_project_id, region_id) -> list:
    '''
        Method to list lakes
    '''
    try:
        # Create a client
        client = dataplex_v1.DataplexServiceClient()

        # Initialize request argument(s)
        request = dataplex_v1.ListLakesRequest(
            parent=f"projects/{gcp_project_id}/locations/{region_id}",
        )

        # Make the request
        page_result = client.list_lakes(request=request)

        # Handle the response
        lakes = []
        for response in page_result:
            lakes.append(response.name.split('/')[-1])

        return lakes
    except Exception as error:
        print(error)
        return None

def list_tasks(gcp_project_id, region_id, lake_id)-> dict:
    '''
        Method to get the CloudDQ tasks
    '''
    try:
        # Create a client
        client = dataplex_v1.DataplexServiceClient()

        # Initialize request argument(s)
        request = dataplex_v1.ListTasksRequest(
            parent=f"projects/{gcp_project_id}/locations/{region_id}/lakes/{lake_id}",
        )

        # Make the request
        page_result = client.list_tasks(request=request)

        # Handle the response
        tasks = {}
        for response in page_result:
            task_id = response.name.split('/')[-1]
            if response.spark.file_uris:
                file_uri = response.spark.file_uris[-1]
                trigger_spec = response.trigger_spec
                tasks[task_id] = {
                    'file_uri': file_uri,
                    'trigger_spec': trigger_spec
                }
        return tasks
    except Exception as error:
        print(error)
        return None