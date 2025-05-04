# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Sends files to Cloud Storage."""
from typing import Dict
from google.cloud import storage
import logging

def upload(config: Dict[str, str], fileDirectory: str, filename: str, folder: str):
    """Uploads a file to a Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.get_bucket((config["output_bucket"]))

    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_filename(f"{fileDirectory}/{filename}")

def checkDestination(bucketpath: str):
    """Check Cloud Storage output folder exists"""
    client = storage.Client()

    if bucketpath.startswith("gs://"):
        raise Exception(f"Please provide output Cloud Storage bucket {bucketpath} without gs:// prefix")
    
    bucket = client.bucket(bucketpath)

    if not bucket.exists():
        raise Exception(f"Cloud Storage bucket {bucketpath} does not exist")
    
    return True
