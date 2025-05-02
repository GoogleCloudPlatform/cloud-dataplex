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

"""Constants that are used in the different files."""
import enum
from typing import List

SOURCE_TYPE = "oracle"

# Default JDBC jar file. Can override with --jar
JDBC_JAR = "ojdbc11.jar"

# allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.oracle_connector"
CONNECTOR_CLASS = "OracleConnector"

# Value to test for if column is nullable. SQL Server specific. Matches _get_dataset  
IS_NULLABLE_TRUE = "Y"

class EntryType(enum.Enum):
    """Logical Hierarechy of EntryTypes in Oracle"""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/oracle-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/oracle-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/oracle-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/oracle-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/oracle-view"

# Top-level entries from above hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY : List[EntryType] = [EntryType.INSTANCE, EntryType.DATABASE]

# EntryType in hierarchy under which database objects (tables, views) are organised and processed ( = schema-level)
COLLECTION_ENTRY : EntryType = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS : List[EntryType] = [EntryType.TABLE, EntryType.VIEW]

def generateFileName(config: dict[str:str]) -> str:
    filename = ''
    if config.get('sid') and config.get('sid') is not None:
        filename = f"{SOURCE_TYPE}-{config['host']}-{config['sid']}.jsonl"
    elif config.get('service') and config.get('service') is not None:
        filename = f"{SOURCE_TYPE}-{config['host']}-{config['service']}.jsonl"
    return filename
