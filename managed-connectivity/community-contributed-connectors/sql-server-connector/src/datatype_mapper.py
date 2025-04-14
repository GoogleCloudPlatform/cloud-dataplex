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

# Maps data types from SQL Server to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on SQL Server native type."""
    if data_type in ["bigint", "int", "smallint", "tinyint", "decimal", "numeric", "smallmoney", "money", "float", "real"]:
        return "NUMBER"
    if data_type in ["varchar", "nvarchar", "char", "nchar", "text", "ntext", "xml"]:
        return "STRING"
    if data_type in ["binary", "varbinary", "image", "geography", "geometry"]:
        return "BYTES"
    if data_type in ["date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"]:
        return "DATETIME"
    if data_type in ["time"]:
        return "TIME"
    return "OTHER"