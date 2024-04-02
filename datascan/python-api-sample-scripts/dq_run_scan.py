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


parser = argparse.ArgumentParser()
parser.add_argument('--datascan_name', '-dsn', help='Full project path of the Datascan. Allowed format = "projects/{PROJECT-ID}/locations/{REGION}/dataScans/{DATASCAN-NAME}"')
args = parser.parse_args()


def run_data_scan():
    # Create a Dataplex client object
    print("Authenticating Dataplex Client...")
    dataplex_client = dataplex_v1.DataScanServiceClient()

    # Define a RunDataScanRequest()
    datascan_name = args.datascan_name # format: "projects/{PROJECT-ID}/locations/{REGION}/dataScans/{DATASCAN-NAME}"
    run_request = dataplex_v1.RunDataScanRequest(
        name = datascan_name
    )
    print("Starting run job for scan...")
    run_scan_job = dataplex_client.run_data_scan(request=run_request)

    # Job Execution Started:
    print("Job ID --> " + str(run_scan_job.job.uid))
    print("Status --> " + str(run_scan_job.job.state))
    print("Data Quality Spec --> " + str(run_scan_job.job.data_quality_spec))
    print("Job started successfully!!")


run_data_scan()