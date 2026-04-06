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

"""Tests for Teradata constants."""

from src.constants import (
    SOURCE_TYPE,
    JDBC_JAR,
    CONNECTOR_MODULE,
    CONNECTOR_CLASS,
    IS_NULLABLE_TRUE,
    EntryType,
    TOP_ENTRY_HIERARCHY,
    COLLECTION_ENTRY,
    DB_OBJECT_TYPES_TO_PROCESS,
    generateFileName,
)


def test_source_type():
    assert SOURCE_TYPE == "teradata"


def test_jdbc_jar():
    assert JDBC_JAR == "terajdbc4.jar"


def test_connector_module():
    assert CONNECTOR_MODULE == "src.teradata_connector"
    assert CONNECTOR_CLASS == "TeradataConnector"


def test_nullable_value():
    assert IS_NULLABLE_TRUE == "Y"


def test_entry_type_hierarchy():
    """Teradata uses 5-level hierarchy matching Oracle."""
    assert hasattr(EntryType, "INSTANCE")
    assert hasattr(EntryType, "DATABASE")
    assert hasattr(EntryType, "DB_SCHEMA")
    assert hasattr(EntryType, "TABLE")
    assert hasattr(EntryType, "VIEW")


def test_entry_type_values_contain_teradata():
    for et in EntryType:
        assert "teradata-" in et.value


def test_top_entry_hierarchy():
    assert TOP_ENTRY_HIERARCHY == [
        EntryType.INSTANCE, EntryType.DATABASE
    ]


def test_collection_entry():
    assert COLLECTION_ENTRY == EntryType.DB_SCHEMA


def test_db_object_types():
    assert DB_OBJECT_TYPES_TO_PROCESS == [
        EntryType.TABLE, EntryType.VIEW
    ]


def test_generate_filename():
    config = {"host": "server.example.com"}
    assert generateFileName(config) == "teradata-server.example.com.jsonl"
