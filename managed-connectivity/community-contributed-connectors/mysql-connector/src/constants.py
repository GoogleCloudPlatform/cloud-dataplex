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

## MySQL specific constants and functions
import enum
from typing import List

SOURCE_TYPE = "mysql"

# Default JDBC jar file. Can override with --jar
JDBC_JAR = "mysql-connector-j-9.2.0.jar"

# allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.mysql_connector"
CONNECTOR_CLASS = "MysqlConnector"

# Value to test for if column is nullable. SQL Server specific. Matches _get_dataset  
IS_NULLABLE_TRUE = "YES"

class EntryType(enum.Enum):
    """Hierarchy of MySQL entries: Instance, database, table/view"""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/mysql-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/mysql-database"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/mysql-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/mysql-view"

# Top-level entries from EntryType hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY : List[EntryType] = [EntryType.INSTANCE]

# EntryType in hierarchy under which database objects like tables, views are organised and processed
COLLECTION_ENTRY : EntryType = EntryType.DATABASE

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS : List[EntryType] =  [EntryType.TABLE, EntryType.VIEW]

def generateFileName(config: dict[str:str]) -> str:
    return f"{SOURCE_TYPE}-{config['host']}-{config['database']}.jsonl"
