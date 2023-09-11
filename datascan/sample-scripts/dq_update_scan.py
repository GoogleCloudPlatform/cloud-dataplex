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
# scan_name
parser.add_argument('--data_scan_name', required=True, help='Name of the DataScan to run. Allowed format = "projects/{PROJECT-ID}/locations/{REGION}/dataScans/{DATA-SCAN-ID}"')

args = parser.parse_args()


def update_data_scan():
    # Create a Dataplex client object
    print("Authenticating Dataplex Client...")
    dataplex_client = dataplex_v1.DataScanServiceClient()

    rules = [
        { # MODIFY BELOW
        "column": "COLUMN_NAME",
        "dimension": "COMPLETENESS",
        "non_null_expectation": {},
        "threshold": 0.98
        },
    ]

    data_scan_name = args.data_scan_name # format: "projects/{PROJECT-ID}/locations/{REGION}/dataScans/{DATA-SCAN-ID}"
    # Define a DataScan()
    data_scan = dataplex_v1.DataScan()
    data_scan.data_quality_spec.rules = rules
    data_scan.name = data_scan_name

    # Define an UpdateDataScanRequest()
    update_request = dataplex_v1.UpdateDataScanRequest()
    update_request.data_scan = data_scan
    update_request.update_mask = "dataQualitySpec"
    # one of: "data", "dataProfileSpec", "dataQualitySpec", "description", "displayName", "executionSpec", "labels"
    # See details at: https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans/patch

    print("Updating scan...")
    update_scan = dataplex_client.update_data_scan(request=update_request)
    update_scan_result = update_scan.result()
    updated_dq_spec = update_scan_result.data_quality_spec
    print("SUCCESS! Here is the updated DQ Spec --> ")
    print(updated_dq_spec)


update_data_scan()