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

## Snowflake specific constants and functions

import enum
from typing import List

SOURCE_TYPE = "snowflake"

# Default JDBC and Snowflake Spark JAR file.
# Note: These versions + Spark Scala version must be aligned/compatible.
JDBC_JAR = "snowflake-jdbc-3.19.0.jar"
SNOWFLAKE_SPARK_JAR = "spark-snowflake_2.12-3.1.1.jar"

# Allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.snowflake_connector"
CONNECTOR_CLASS = "SnowflakeConnector"

# Value to test for if column is nullable. Snowflake specific. 
# Matches value in is_nullable column from _get_columns
IS_NULLABLE_TRUE = "Y"

class EntryType(enum.Enum):
    """Hierarchy of Snowflake entries"""
    ACCOUNT: str = "projects/{project}/locations/{location}/entryTypes/snowflake-account"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/snowflake-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/snowflake-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/snowflake-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/snowflake-view"

# Top-level types in EntryType hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY : List[EntryType] = [EntryType.ACCOUNT, EntryType.DATABASE]

# EntryType in the hierarchy under which database objects like tables, views are organised and processed
COLLECTION_ENTRY : EntryType = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS : List[EntryType] = [EntryType.TABLE, EntryType.VIEW]

# metadata file name 
def generateFileName(config: dict[str:str]) -> str:
    return f"{SOURCE_TYPE}-{config['account']}-{config['database']}.jsonl"
