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
#from cryptography.hazmat.backends import default_backend
#from cryptography.hazmat.primitives import serialization

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

''' 
# Loads keyfile
def loadKeyFile(key_file_path):
    with open("{key_file_path}", "rb") as key_file:
        p_key = serialization.load_pem_private_key(
            key_file.read(),
            #password = os.environ['PRIVATE_KEY_PASSPHRASE'].encode(),
            backend = default_backend()
        )

    privateKeyBytes = p_key.private_bytes(
        encoding = serialization.Encoding.PEM,
        format = serialization.PrivateFormat.PKCS8,
        encryption_algorithm = serialization.NoEncryption()
    )

    privateKeyBytes = privateKeyBytes.decode("UTF-8")
    privateKeyBytes = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n","",privateKeyBytes).replace("\n","")

    return privateKeyBytes
'''

# Convert string to camel case - dataplex v1 property names
def to_camel_case(text) -> str:
    return re.sub(r"[-_]([a-zA-Z])", lambda x: x[1].upper(), text)

# folder name with timestamp
def generateFolderName(SOURCE_TYPE : str) -> str:
    currentDate = datetime.now()
    return f"{SOURCE_TYPE}/{currentDate.year}{currentDate.month}{currentDate.day}-{currentDate.hour}{currentDate.minute}{currentDate.second}" 
    