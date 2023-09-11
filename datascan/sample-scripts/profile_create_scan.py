#!/usr/bin/env python

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

import argparse
from google.cloud import dataplex_v1


# Arguments:
parser = argparse.ArgumentParser()
# data_scan_id
parser.add_argument('--data_scan_id', required=True, help='ID for the new Profile Scan. Must be unique. Only letters, numbers, and dashes allowed. Cannot be changed after creation. See: https://cloud.google.com/compute/docs/naming-resources#resource-name-format')
# project_location / "parent"
parser.add_argument('--profile_project', required=True, help='The project where the Dataplex Profile Scan will reside. Allowed format: projects/{PROJECT-ID}/locations/{REGION-ID}. Example: projects/abc-xyz/locations/us-central1. Region refers to a GCP region.')
# dataset_table resource
parser.add_argument('--data_source_path', required=True, help='Full project path of the source table. Allowed format: //bigquery.googleapis.com/projects/{PROJECT-ID}/datasets/{DATASET-ID}/tables/{TABLE}')
# display_name (optional)
parser.add_argument('--display_name', required=False, help='Display name for the Profile Scan. This field is optional.')

args = parser.parse_args()


def create_data_scan():
    '''
    Create a new Profile DataScan resource.

    Returns: The created DataScan resource in your Dataplex project.
    '''
    # Create a Dataplex client object
    print("Authenticating Dataplex Client...")
    dataplex_client = dataplex_v1.DataScanServiceClient()

    # Define a DataQualitySpec()
    print("Creating DataProfileSpec...")
    profile_spec = dataplex_v1.DataProfileSpec()
    
    # Define a DataScan()
        # resource (str)
        # data_profile_spec = DataProfileSpec()
    print("Creating DataScan...")
    data_scan = dataplex_v1.DataScan()
    # `DATA` is one-of `resource` or `entity`
    data_scan.data.resource = args.data_source_path # format: "//bigquery.googleapis.com/projects/{PROJECT-ID}/datasets/{DATASET}/tables/{TABLE}"
    # data_scan.data.entity = args.data_source_path # format: "projects/{PROJECT-ID}/locations/{REGION}/lakes/{LAKE}/zones/{ZONE}/entities/{ASSET}"
    # data_scan.display_name = args.display_name # Optional field
    data_scan.labels = None # Optional field
    data_scan.data_profile_spec = profile_spec
    data_scan.execution_spec = {
        # Value of the trigger is either "on_demand" or "schedule". If not specified, the default is `on_demand`.
        "trigger": {
        # Value is "on_demand" or "schedule"
            "on_demand": {}
            # "schedule": {
            #     "cron": "0 0 3 * *"
            # }            
        }
    }

    # Define a CreateDataScanRequest()
        # parent (str)
        # data_scan = DataScan()
        # data_scan_id (str)
    print("Creating a CreateDataScanRequest...")
    create_scan_request = dataplex_v1.CreateDataScanRequest()
    create_scan_request.parent = args.profile_project # format: "projects/{PROJECT-ID/locations/{REGION}"
    create_scan_request.data_scan = data_scan
    create_scan_request.data_scan_id = args.data_scan_id

    # Make the scan request
    print("Creating Profile Scan operation...")
    create_scan_operation = dataplex_client.create_data_scan(request=create_scan_request)

    # Handle the response
    create_scan_response = create_scan_operation.result()
    print("Successfully created Scan. Here's the output response:")
    print(create_scan_response)
    print("SUCCESS!")


create_data_scan()