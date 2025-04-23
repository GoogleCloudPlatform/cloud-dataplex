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

# SQL Server specific constants and functions

import enum
from typing import List

SOURCE_TYPE = "sqlserver"

# Default JDBC jar file. Can override with --jar
JDBC_JAR = "mssql-jdbc-12.10.0.jre11.jar"

# Allow common bootstrap code to dynamically load connector for specific datasource
CONNECTOR_MODULE = "src.sqlserver_connector"
CONNECTOR_CLASS = "SQLServerConnector"

# Value to test for if column is nullable. SQL Server specific. Matches _get_dataset  
IS_NULLABLE_TRUE = "Y"

class EntryType(enum.Enum):
    """Hierarchy of SQL Server entries"""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-view"

# Top-level entries from above hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY : List[EntryType] = [EntryType.INSTANCE, EntryType.DATABASE]

# EntryType in the hierarchy under which database objects like tables, views are organised and processed
COLLECTION_ENTRY : EntryType = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS : List[EntryType] = [EntryType.TABLE, EntryType.VIEW]

# metadata file name
def generateFileName(config: dict[str:str]) -> str:
    filename = ''
    if config['instancename'] and len(config['instancename']) > 0:
        filename = f"{SOURCE_TYPE}-{config['host']}-{config['instancename']}-{config['database']}.jsonl"
    else:
        filename = f"{SOURCE_TYPE}-{config['host']}-defaultinstance-{config['database']}.jsonl"
    return filename