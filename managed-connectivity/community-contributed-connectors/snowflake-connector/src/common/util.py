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

# Utility convenience functions
import sys
from datetime import datetime
import re
import os
from google.cloud import storage

# Retrieves content as string from file at given path
# supports local file system, cloud storage paths
def loadReferencedFile(path: str) -> str:
    if path.startswith("gs://"):
        client = storage.Client()
        bucket_name, file_path = path[5:].split('/', 1)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        return blob.download_as_text()
    else:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

def upload(config: dict[str, str], fileDirectory: str, filename: str, folder: str):
    """Uploads a file to a Cloud Storage bucket."""
    client = storage.Client()
    bucket = client.get_bucket((config["output_bucket"]))

    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_filename(f"{fileDirectory}/{filename}")

# Convert string to camel case - dataplex v1 property names
def to_camel_case(text) -> str:
    return re.sub(r"[-_]([a-zA-Z])", lambda x: x[1].upper(), text)

# folder name with timestamp
def generateFolderName(SOURCE_TYPE: str) -> str:
    currentDate = datetime.now()
    return f"{SOURCE_TYPE}/{currentDate.year}{currentDate.month}{currentDate.day}-{currentDate.hour}{currentDate.minute}{currentDate.second}"

# True if running in container
def isRunningInContainer() -> bool:
    return os.environ.get("RUNNING_IN_CONTAINER", "").lower() in ("yes", "y", "on", "true", "1")


# Returns True is file exists at given path.
# filePath can also be comma-seperated list of multiple jar paths
def fileExists(filepath : str) -> bool:

    if "," in filepath:
        # Split into individual file paths and check each
        jar_list = filepath.split(",")
        for jar_file_path in jar_list:
            if not os.path.isfile(jar_file_path):
                raise Exception(f"Jar file not found: {jar_file_path}")
    else:
        # check single file path
        if not os.path.isfile(filepath):
            raise Exception(f"Jar file not found: {filepath}")

    return True
