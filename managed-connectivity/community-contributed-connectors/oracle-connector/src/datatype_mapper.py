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

# Maps datatypes from Oracle to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    if data_type.startswith("NUMBER") or data_type in ["INTEGER","SHORTINTEGER","LONGINTEGER","BINARY_FLOAT","BINARY_DOUBLE","FLOAT", "LONG"]:
        return "NUMBER"
    if data_type.startswith("VARCHAR") or data_type in ["NVARCHAR2","CHAR","NCHAR","CLOB","NCLOB"]:
        return "STRING"
    if data_type in ["LONG","BLOB","RAW","LONG RAW"]:
        return "BYTES"
    if data_type.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if data_type == "BOOLEAN":
        return "BOOLEAN"
    if data_type == "DATE":
        return "DATETIME"
    return "OTHER"
