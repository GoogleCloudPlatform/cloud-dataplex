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

# Maps data types from PostgreSQL to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on Postgres native type."""
    if data_type in ["numeric","integer","serial","double precision","decimal","smallint","smallserial","bigserial","bigint","real"]:
        return "NUMBER"
    if data_type.startswith("character") or data_type.startswith("bpchar") or data_type in ["text","varchar"]:
        return "STRING"
    if data_type.startswith("bytea"):
        return "BYTES"
    if data_type == "boolean":
        return "BOOLEAN"
    if data_type.startswith("timestamp"):
        return "TIMESTAMP"
    if data_type.startswith("date"):
        return "DATETIME"
    return "OTHER"
