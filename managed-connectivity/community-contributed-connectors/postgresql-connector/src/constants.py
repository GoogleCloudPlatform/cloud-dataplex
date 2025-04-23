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

import enum

SOURCE_TYPE = "postgresql"

JDBC_JAR = "postgresql-42.7.5.jar"

# allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.postgres_connector"
CONNECTOR_CLASS = "PostgresConnector"

# Value to test for if column is nullable. Postgresql specific. Matches _get_dataset  
IS_NULLABLE_TRUE = "YES"

class EntryType(enum.Enum):
    """Hierarchy of Postgresql entries"""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/postgresql-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/postgresql-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/postgresql-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/postgresql-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/postgresql-view"

# Top-level entries from above hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY = [EntryType.INSTANCE, EntryType.DATABASE]

# EntryType in hierarchy under which database objects like tables, views are organised and processed ( = schema-level)
COLLECTION_ENTRY = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS = [EntryType.TABLE, EntryType.VIEW]

def generateFileName(config: dict[str:str]) -> str:
    return f"{SOURCE_TYPE}-{config['host']}-{config['database']}.jsonl"
