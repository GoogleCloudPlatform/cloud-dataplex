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
import argparse
import sys
from datetime import datetime

# Returns string content from file at given path
def loadReferencedFile(file_path) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return content
    except Exception as e:
        print(f"Error while reading file {file_path}: {e}")
        print("Exiting")
        sys.exit(1)
    return None

# folder name with timestamp
def generateFolderName(SOURCE_TYPE : str):
    currentDate = datetime.now()
    return f"{SOURCE_TYPE}/{currentDate.year}{currentDate.month}{currentDate.day}-{currentDate.hour}{currentDate.minute}{currentDate.second}"     