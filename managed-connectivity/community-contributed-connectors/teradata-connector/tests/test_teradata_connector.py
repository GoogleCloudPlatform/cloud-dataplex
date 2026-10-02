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

"""Tests for TeradataConnector with mocked PySpark and teradatasql."""

import sys
from unittest import mock
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

# Pre-mock heavy dependencies so the module can be imported without
# PySpark or teradatasql installed on the CI runner.
sys.modules.setdefault("pyspark", MagicMock())
sys.modules.setdefault("pyspark.sql", MagicMock())
sys.modules.setdefault("pyspark.sql.types", MagicMock())
sys.modules.setdefault("teradatasql", MagicMock())

from src.constants import EntryType
import src.teradata_connector as _tc_module  # force module registration


BASE_CONFIG = {
    "host": "td-server.example.com",
    "port": 1025,
    "user": "testuser",
    "password": "testpass",
}


def _build_connector(config=None, spark=None, td_conn=None):
    """Build a TeradataConnector with all external deps mocked."""
    cfg = {**BASE_CONFIG, **(config or {})}
    with patch("src.teradata_connector.getJarPath", return_value="/fake.jar"), \
         patch("src.teradata_connector.fileExists", return_value=True), \
         patch("src.teradata_connector.SparkSession") as mock_spark_cls, \
         patch("src.teradata_connector.teradatasql") as mock_td:

        mock_session = spark or MagicMock()
        mock_spark_cls.builder.appName.return_value \
            .config.return_value \
            .config.return_value \
            .getOrCreate.return_value = mock_session

        mock_td.connect.return_value = td_conn or MagicMock()

        from src.teradata_connector import TeradataConnector
        connector = TeradataConnector(cfg)
    return connector


class TestTeradataConnectorInit:
    """Tests for __init__ and connection setup."""

    def test_init_sets_jdbc_url(self):
        connector = _build_connector()
        assert "td-server.example.com" in connector._url
        assert "DBS_PORT=1025" in connector._url
        assert "CHARSET=UTF8" in connector._url

    def test_init_custom_charset(self):
        connector = _build_connector({"charset": "UTF16"})
        assert "CHARSET=UTF16" in connector._url

    def test_init_connect_options(self):
        connector = _build_connector()
        assert connector._connectOptions["driver"] == "com.teradata.jdbc.TeraDriver"
        assert connector._connectOptions["user"] == "testuser"
        assert connector._connectOptions["password"] == "testpass"

    def test_init_with_logmech(self):
        connector = _build_connector({"logmech": "LDAP"})
        assert "LOGMECH=LDAP" in connector._url

    def test_init_without_logmech(self):
        connector = _build_connector()
        assert "LOGMECH" not in connector._url

    def test_init_logdata_appended_to_url(self):
        connector = _build_connector({
            "logmech": "LDAP",
            "logdata": "authdata",
        })
        assert "LOGDATA=authdata" in connector._url

    def test_init_logdata_not_in_url_when_absent(self):
        connector = _build_connector()
        assert "LOGDATA" not in connector._url

    def test_init_teradatasql_receives_logmech_logdata(self):
        mock_td_conn = MagicMock()
        with patch("src.teradata_connector.getJarPath", return_value="/fake.jar"), \
             patch("src.teradata_connector.fileExists", return_value=True), \
             patch("src.teradata_connector.SparkSession") as mock_spark_cls, \
             patch("src.teradata_connector.teradatasql") as mock_td:

            mock_spark_cls.builder.appName.return_value \
                .config.return_value \
                .config.return_value \
                .getOrCreate.return_value = MagicMock()
            mock_td.connect.return_value = mock_td_conn

            cfg = {
                **BASE_CONFIG,
                "logmech": "JWT",
                "logdata": "eyJhbGciOi...",
            }
            from src.teradata_connector import TeradataConnector
            TeradataConnector(cfg)

            call_kwargs = mock_td.connect.call_args[1]
            assert call_kwargs["logmech"] == "JWT"
            assert call_kwargs["logdata"] == "eyJhbGciOi..."

    def test_init_user_password_omitted_when_empty(self):
        mock_td_conn = MagicMock()
        with patch("src.teradata_connector.getJarPath", return_value="/fake.jar"), \
             patch("src.teradata_connector.fileExists", return_value=True), \
             patch("src.teradata_connector.SparkSession") as mock_spark_cls, \
             patch("src.teradata_connector.teradatasql") as mock_td:

            mock_spark_cls.builder.appName.return_value \
                .config.return_value \
                .config.return_value \
                .getOrCreate.return_value = MagicMock()
            mock_td.connect.return_value = mock_td_conn

            cfg = {
                **BASE_CONFIG,
                "user": "",
                "password": "",
                "logmech": "LDAP",
                "logdata": "authdata",
            }
            from src.teradata_connector import TeradataConnector
            connector = TeradataConnector(cfg)

            assert "user" not in connector._connectOptions
            assert "password" not in connector._connectOptions

            call_kwargs = mock_td.connect.call_args[1]
            assert "user" not in call_kwargs
            assert "password" not in call_kwargs


class TestGetDbSchemas:
    """Tests for get_db_schemas()."""

    def test_get_db_schemas_executes_query(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value \
            .options.return_value \
            .option.return_value \
            .load.return_value = mock_df

        connector = _build_connector(spark=mock_spark)
        result = connector.get_db_schemas()

        mock_spark.read.format.assert_called_with("jdbc")
        assert result == mock_df

    def test_get_db_schemas_excludes_system_dbs(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value \
            .options.return_value \
            .option.return_value \
            .load.return_value = mock_df

        connector = _build_connector(spark=mock_spark)
        connector.get_db_schemas()

        query_call = mock_spark.read.format.return_value \
            .options.return_value \
            .option.call_args
        query = query_call[0][1]
        assert "DBC" in query
        assert "NOT IN" in query

    def test_get_db_schemas_with_database_filter(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value \
            .options.return_value \
            .option.return_value \
            .load.return_value = mock_df

        connector = _build_connector(
            config={"database": "MyDB"}, spark=mock_spark
        )
        connector.get_db_schemas()

        query_call = mock_spark.read.format.return_value \
            .options.return_value \
            .option.call_args
        query = query_call[0][1]
        assert "MyDB" in query


class TestGetDataset:
    """Tests for get_dataset() dispatch."""

    def test_get_dataset_table(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value \
            .options.return_value \
            .option.return_value \
            .load.return_value = mock_df
        mock_df.orderBy.return_value = mock_df

        connector = _build_connector(spark=mock_spark)
        result = connector.get_dataset("test_schema", EntryType.TABLE)
        mock_df.orderBy.assert_called_with("TABLE_NAME")

    def test_get_dataset_view_empty(self):
        mock_spark = MagicMock()
        mock_td_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_td_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_td_conn.cursor.return_value.__exit__ = MagicMock(
            return_value=False
        )

        # Return empty view list
        mock_cursor.execute.return_value = None
        mock_cursor.fetchall.return_value = []

        connector = _build_connector(
            spark=mock_spark, td_conn=mock_td_conn
        )
        # _execute_td returns empty → createDataFrame with empty list
        connector._execute_td = MagicMock(return_value=[])
        result = connector.get_dataset("test_schema", EntryType.VIEW)
        mock_spark.createDataFrame.assert_called_once()


class TestGetTables:
    """Tests for _get_tables()."""

    def test_get_tables_query_contains_schema(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value \
            .options.return_value \
            .option.return_value \
            .load.return_value = mock_df
        mock_df.orderBy.return_value = mock_df

        connector = _build_connector(spark=mock_spark)
        connector._get_tables("my_schema")

        query_call = mock_spark.read.format.return_value \
            .options.return_value \
            .option.call_args
        query = query_call[0][1]
        assert "my_schema" in query
        assert "TableKind IN ('T', 'O')" in query

    def test_get_tables_escapes_quotes(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.read.format.return_value \
            .options.return_value \
            .option.return_value \
            .load.return_value = mock_df
        mock_df.orderBy.return_value = mock_df

        connector = _build_connector(spark=mock_spark)
        connector._get_tables("schema'name")

        query_call = mock_spark.read.format.return_value \
            .options.return_value \
            .option.call_args
        query = query_call[0][1]
        assert "schema''name" in query


class TestGetViews:
    """Tests for _get_views() with HELP COLUMN."""

    def test_get_views_with_columns(self):
        mock_spark = MagicMock()
        mock_td_conn = MagicMock()

        # Step 1: view list query
        view_list = [("view1", "comment", None, None)]

        # Step 2: HELP COLUMN cursor
        mock_cursor = MagicMock()
        mock_cursor.description = [
            ("Column Name", None), ("Type", None),
            ("Nullable", None), ("Max Length", None),
            ("Decimal Total Digits", None),
            ("Decimal Fractional Digits", None),
            ("Comment", None),
        ]
        mock_cursor.fetchall.return_value = [
            ("col1", "CV", "Y", 100, None, None, "col comment"),
        ]

        mock_td_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_td_conn.cursor.return_value.__exit__ = MagicMock(
            return_value=False
        )

        connector = _build_connector(
            spark=mock_spark, td_conn=mock_td_conn
        )
        connector._execute_td = MagicMock(return_value=view_list)

        connector._get_views("test_schema")
        mock_spark.createDataFrame.assert_called_once()
        rows = mock_spark.createDataFrame.call_args[0][0]
        assert len(rows) == 1
        assert rows[0][0] == "view1"
        assert rows[0][1] == "col1"

    def test_get_views_help_column_fallback(self):
        """When HELP COLUMN fails, falls back to DBC.ColumnsV."""
        mock_spark = MagicMock()

        # Build connector with a clean td_conn first
        connector = _build_connector(spark=mock_spark)

        # Now replace _td_conn with one whose cursor raises on execute
        mock_td_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("HELP COLUMN failed")
        mock_td_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_td_conn.cursor.return_value.__exit__ = MagicMock(
            return_value=False
        )
        connector._td_conn = mock_td_conn

        view_list = [("bad_view", "comment", None, None)]

        # First call returns view list, second returns fallback columns
        connector._execute_td = MagicMock(
            side_effect=[
                view_list,
                [("fallback_col", "Y")],
            ]
        )

        connector._get_views("test_schema")
        mock_spark.createDataFrame.assert_called_once()
        rows = mock_spark.createDataFrame.call_args[0][0]
        assert len(rows) == 1
        assert rows[0][0] == "bad_view"
        assert rows[0][1] == "fallback_col"


class TestClose:
    """Tests for close() and context manager."""

    def test_close_connections(self):
        mock_td_conn = MagicMock()
        mock_spark = MagicMock()
        connector = _build_connector(
            spark=mock_spark, td_conn=mock_td_conn
        )

        connector.close()
        mock_td_conn.close.assert_called_once()
        mock_spark.stop.assert_called_once()
        assert connector._td_conn is None
        assert connector._spark is None

    def test_close_idempotent(self):
        connector = _build_connector()
        connector._td_conn = None
        connector._spark = None
        connector.close()  # should not raise

    def test_close_handles_exception(self):
        mock_td_conn = MagicMock()
        mock_td_conn.close.side_effect = Exception("close failed")
        mock_spark = MagicMock()

        connector = _build_connector(
            spark=mock_spark, td_conn=mock_td_conn
        )
        connector.close()  # should not raise
        assert connector._td_conn is None

    def test_context_manager(self):
        connector = _build_connector()
        with connector as c:
            assert c is connector
        assert connector._td_conn is None
