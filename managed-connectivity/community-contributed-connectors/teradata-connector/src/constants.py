# Copyright 2026 Teradata
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

"""Constants for the Teradata connector."""

import enum
from typing import List

SOURCE_TYPE = "teradata"

# Default JDBC jar file. Can override with --jar
JDBC_JAR = "terajdbc4.jar"

# Allow common bootstrap to load connector for this datasource
CONNECTOR_MODULE = "src.teradata_connector"
CONNECTOR_CLASS = "TeradataConnector"

# Value to test for if column is nullable. Teradata DBC.ColumnsV uses Y/N
IS_NULLABLE_TRUE = "Y"


class EntryType(enum.Enum):
    """Logical hierarchy of EntryTypes in Teradata."""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/teradata-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/teradata-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/teradata-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/teradata-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/teradata-view"


# Top-level entries written before schema processing
TOP_ENTRY_HIERARCHY: List[EntryType] = [
    EntryType.INSTANCE,
    EntryType.DATABASE,
]

# EntryType under which tables/views are organized
COLLECTION_ENTRY: EntryType = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS: List[EntryType] = [
    EntryType.TABLE,
    EntryType.VIEW,
]


def generateFileName(config: dict) -> str:
    return f"{SOURCE_TYPE}-{config['host']}.jsonl"
