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

# Loads file at a given path and returns the content as a string
def loadReferencedFile(file_path) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        print(f"Error while reading file {file_path}: {e}")
        sys.exit(1)
    return None

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
