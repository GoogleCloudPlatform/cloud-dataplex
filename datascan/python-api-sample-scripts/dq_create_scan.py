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

# !pip install google-cloud
# !pip install google-cloud-dataplex
# !pip install google-cloud-storage

import argparse
from google.cloud import dataplex_v1


# Arguments:
parser = argparse.ArgumentParser()
# data_scan_id
parser.add_argument('--data_scan_id', required=True, help='ID for the new DQ Scan. Must be unique. Only letters, numbers, and dashes allowed. Cannot be changed after creation. See: https://cloud.google.com/compute/docs/naming-resources#resource-name-format')
# project_location / "parent"
parser.add_argument('--dq_project', required=True, help='The project where the Dataplex Data Quality Scan will reside. Allowed format: projects/{PROJECT-ID}/locations/{REGION-ID}. Example: projects/abc-xyz/locations/us-central1. Region refers to a GCP region.')
# dataset_table resource
parser.add_argument('--data_source_path', required=True, help='Full project path of the source table. Allowed format: //bigquery.googleapis.com/projects/{PROJECT-ID}/datasets/{DATASET-ID}/tables/{TABLE}')
# bq_export table
parser.add_argument('--export_results_path', required=True, help='Full project path of the table the dq results are exported to. Allowed format: //bigquery.googleapis.com/projects/{PROJECT-ID}/datasets/{DATASET-ID}/tables/{TABLE}')
# display_name (optional)
parser.add_argument('--display_name', required=False, help='Display name for the DQ Scan. This field is optional.')

args = parser.parse_args()


def create_data_scan():
    '''
    Given a list of data quality rules, create a new DataScan resource and 
    populate it with the given rules.

    Args:
    data_source_path - Full project path of the source table. 
        Allowed format: //bigquery.googleapis.com/projects/{PROJECT-ID}/datasets/{DATASET-ID}/tables/{TABLE}
    dq_project - The project where the Dataplex Data Quality Scan will reside. 
        Allowed format: projects/{PROJECT-ID}/locations/{REGION-ID}. 
        Example: projects/abc-xyz/locations/us-central1. Region refers to a GCP region.
    data_scan_id - ID for the new DQ Scan. Must be unique. Only letters, numbers, and dashes allowed. Cannot be changed after creation. 
        See: https://cloud.google.com/compute/docs/naming-resources#resource-name-format
    export_results_path - Full project path of the table the dq results are exported to.
        Allowed format: //bigquery.googleapis.com/projects/{PROJECT-ID}/datasets/{DATASET-ID}/tables/{TABLE}

    Returns: The created DataScan resource in your Dataplex project.
    '''
    # Create a Dataplex client object
    print("Authenticating Dataplex Client...")
    dataplex_client = dataplex_v1.DataScanServiceClient()

    # Define DQ Rules:
    ### SAMPLE RULES ###
    rules = [
        { # Rule 1
        "column": "category",
        "dimension": "VALIDITY",
        "ignore_null": False,
        "set_expectation": {
            "values": [
                "Office Supplies",
                "Technology",
                "Furniture"
            ]
        },
        "threshold": 0.98 # Value between 0 and 1
        },
        { # Rule 2
        "column": "order_id",
        "dimension": "COMPLETENESS",
        "non_null_expectation": {},
        "threshold": 0.98
        },
        { # Rule 3
        "column": "sales",
        "dimension": "ACCURACY",
        "row_condition_expectation": {
            "sql_expression": "sales > 0"
            },
        "threshold": 0.90
        },
        { # Rule 4
        "column": "country",
        "dimension": "UNIQUENESS",
        "uniqueness_expectation": {},
        "ignore_null": True,
        "threshold": 0.86
        },
    ]

    # Define a DataQualitySpec()
    print("Creating DataQualitySpec...")
    dq_spec = dataplex_v1.DataQualitySpec()
    dq_spec.rules = rules
    dq_spec.post_scan_actions = {
        # This writes the results of the DQ Scan to a table in BQ. Creates the table if it does not exist.
        "bigquery_export": {
            "results_table": args.export_results_path
        }
    }
    
    # Define a DataScan()
        # resource (str)
        # data_quality_spec = DataQualitySpec()
    print("Creating DataScan...")
    data_scan = dataplex_v1.DataScan()
    # DATA is one-of `resource` or `entity`
    data_scan.data.resource = args.data_source_path # format: "//bigquery.googleapis.com/projects/{PROJECT-ID}/datasets/{DATASET}/tables/{TABLE}"
    # data_scan.data.entity = args.data_source_path # format: "projects/{PROJECT-ID}/locations/{REGION}/lakes/{LAKE}/zones/{ZONE}/entities/{ASSET}"
    # data_scan.display_name = args.display_name # Optional field
    data_scan.labels = None # Optional field
    data_scan.data_quality_spec = dq_spec
    data_scan.execution_spec = {
        # Value of the trigger is either "on_demand" or "schedule". If not specified, the default is `on_demand`.
        # If `field` is set, indicates an "incremental" scan.
        "trigger": {
        # Value is "on_demand" or "schedule"
            "on_demand": {}
            # "schedule": {
            #     "cron": "0 0 3 * *"
            # }            
        }
        # ,
        # "field": "incremental_date" # Optional field
    }

    # Define a CreateDataScanRequest()
        # parent (str)
        # data_scan = DataScan()
        # data_scan_id (str)
    print("Creating a CreateDataScanRequest...")
    create_scan_request = dataplex_v1.CreateDataScanRequest()
    create_scan_request.parent = args.dq_project # format: "projects/{PROJECT-ID/locations/{REGION}"
    create_scan_request.data_scan = data_scan
    create_scan_request.data_scan_id = args.data_scan_id

    # Make the scan request
    print("Creating AutoDQ Scan operation...")
    create_scan_operation = dataplex_client.create_data_scan(request=create_scan_request)

    # Handle the response
    create_scan_response = create_scan_operation.result()
    print("Successfully created Scan. Here's the output response:")
    print(create_scan_response)
    print("SUCCESS! DQ Scan created successfully.")


create_data_scan()