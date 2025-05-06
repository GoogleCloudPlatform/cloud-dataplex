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

# Maps data types from Snowflake to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on Snowflake native type."""
    if data_type.startswith("NUMBER") or data_type in ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL", "DECIMAL", "NUMERIC"]:
        return "NUMBER"
    if data_type.startswith("VARCHAR") or data_type in ["TEXT", "STRING", "CHAR", "CHARACTER", "BINARY", "VARBINARY"]:
        return "STRING"
    if data_type.startswith("TIMESTAMP") or data_type in ["TIME", "DATETIME", "DATE"]:
        return "TIMESTAMP"
    if data_type == "BOOLEAN":
        return "BOOLEAN"
    return "OTHER"
