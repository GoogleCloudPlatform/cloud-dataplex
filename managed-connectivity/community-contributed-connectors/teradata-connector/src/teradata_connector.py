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


"""Reads Teradata metadata using PySpark JDBC and teradatasql."""

from typing import Dict, List, Tuple, Any

import teradatasql
from pyspark.sql import SparkSession, DataFrame

from src.common.ExternalSourceConnector import IExternalSourceConnector
from src.constants import EntryType, JDBC_JAR
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.common.argument_validator import validateQueryBand

# Teradata system databases to exclude from metadata extraction
SYSTEM_DATABASES = (
    "DBC", "SysAdmin", "SystemFe", "TDQCD", "TDStats",
    "tdwm", "SYSLIB", "SYSBAR", "SYSJDBC", "SYSSPATIAL",
    "SysUDTLib", "dbcmngr", "LockLogShredder", "SQLJ",
    "Crashdumps", "Default", "EXTUSER", "TDPUSER",
    "TDMaps", "TD_SYSXML", "TD_SERVER_DB",
    "All", "Sys_Calendar", "SYSUDTLIB", "SYSUIF",
    "External_AP", "SYSGATEWAY", "TD_SYSFNLIB",
    "TD_SYSGPL",
)


class TeradataConnector(IExternalSourceConnector):
    """Reads metadata from Teradata and returns Spark DataFrames."""

    def __init__(self, config: Dict[str, str]):
        jar_path = getJarPath(config, [JDBC_JAR])
        fileExists(jar_path)

        # Validate config before starting heavyweight resources
        query_band = validateQueryBand(config.get("query_band"))

        self._spark = (
            SparkSession.builder
            .appName("TeradataIngestor")
            .config("spark.jars", jar_path)
            .config("spark.log.level", "ERROR")
            .getOrCreate()
        )
        self._config = config
        charset = config.get("charset", "UTF8")
        self._url = (
            f"jdbc:teradata://{config['host']}"
            f"/DBS_PORT={config['port']},CHARSET={charset}"
        )

        # Append LOGMECH / LOGDATA to the JDBC URL so the
        # Teradata JDBC driver receives them as connection
        # parameters (Spark's BasicConnectionProvider does not
        # forward custom options as Teradata properties).
        if config.get("logmech"):
            self._url += f",LOGMECH={config['logmech']}"
        if config.get("logdata"):
            self._url += f",LOGDATA={config['logdata']}"

        self._connectOptions = {
            "driver": "com.teradata.jdbc.TeraDriver",
            "url": self._url,
        }
        if config.get("user"):
            self._connectOptions["user"] = config["user"]
        if config.get("password"):
            self._connectOptions["password"] = config["password"]

        self._query_band = query_band

        # Safe: validateQueryBand whitelist guarantees no single quotes in _query_band
        self._connectOptions["sessionInitStatement"] = (
            f"SET QUERY_BAND = '{self._query_band}' FOR SESSION"
        )

        # Native Python connection for HELP COLUMN (views)
        td_connect_params = {
            "host": config["host"],
            "dbs_port": str(config["port"]),
        }
        if config.get("user"):
            td_connect_params["user"] = config["user"]
        if config.get("password"):
            td_connect_params["password"] = config["password"]
        if config.get("logmech"):
            td_connect_params["logmech"] = config["logmech"]
        if config.get("logdata"):
            td_connect_params["logdata"] = config["logdata"]
        self._td_conn = teradatasql.connect(**td_connect_params)

        # Safe: validateQueryBand whitelist guarantees no single quotes in _query_band
        with self._td_conn.cursor() as cur:
            cur.execute(
                f"SET QUERY_BAND = '{self._query_band}' FOR SESSION"
            )

    def _execute(self, query: str) -> DataFrame:
        """Execute a query via JDBC and return a DataFrame."""
        return (
            self._spark.read.format("jdbc")
            .options(**self._connectOptions)
            .option("query", query)
            .load()
        )

    def get_db_schemas(self) -> DataFrame:
        """Get database/schema names, excluding system databases."""
        exclusion_list = ",".join(f"'{db}'" for db in SYSTEM_DATABASES)

        # Optional: scope to a single database
        db_filter = ""
        if self._config.get("database"):
            db_filter = (
                f"AND DatabaseName = '{self._config['database']}'"
            )

        query = f"""
            SELECT TRIM(DatabaseName) AS SCHEMA_NAME
            FROM DBC.DatabasesV
            WHERE DatabaseName NOT IN ({exclusion_list})
            {db_filter}
        """
        return self._execute(query)

    def get_dataset(
        self, schema_name: str, entry_type: EntryType
    ) -> DataFrame:
        """Get table or view metadata with columns.

        Args:
            schema_name: the Teradata database/schema to query
            entry_type: EntryType.TABLE or EntryType.VIEW
        """
        if entry_type == EntryType.TABLE:
            return self._get_tables(schema_name)
        return self._get_views(schema_name)

    def _get_tables(self, schema_name: str) -> DataFrame:
        """Get table metadata from DBC.ColumnsV."""
        safe_schema = schema_name.replace("'", "''")
        query = f"""
            SELECT
                TRIM(c.TableName)          AS TABLE_NAME,
                TRIM(c.ColumnName)         AS COLUMN_NAME,
                TRIM(c.ColumnType)         AS DATA_TYPE,
                CASE WHEN c.Nullable = 'Y'
                     THEN 'Y' ELSE 'N'
                END                        AS IS_NULLABLE,
                t.CommentString            AS TABLE_COMMENT,
                c.CommentString            AS COLUMN_COMMENT,
                c.DefaultValue             AS DATA_DEFAULT,
                t.CreateTimeStamp          AS TABLE_CREATE_TIME,
                t.LastAlterTimeStamp       AS TABLE_LAST_ALTER_TIME
            FROM DBC.ColumnsV c
            INNER JOIN DBC.TablesV t
                ON  t.TableName    = c.TableName
                AND t.DatabaseName = c.DatabaseName
            WHERE c.DatabaseName = '{safe_schema}'
              AND t.TableKind IN ('T', 'O')
        """
        return self._execute(query).orderBy("TABLE_NAME")

    def _execute_td(self, query: str) -> List[Tuple[Any, ...]]:
        """Execute a query via teradatasql and return rows."""
        with self._td_conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()

    def _get_views(self, schema_name: str) -> DataFrame:
        """Get view metadata using teradatasql + HELP COLUMN.

        DBC.ColumnsV returns NULL types for view columns and
        DBC.ColumnsQV requires the QVCI feature which is unstable
        and often not enabled. HELP COLUMN reliably resolves
        view column types via the teradatasql Python driver.
        """
        # Step 1: Get list of views with table-level metadata
        safe_schema = schema_name.replace("'", "''")
        view_list_query = f"""
            SELECT TRIM(TableName),
                   CommentString,
                   CreateTimeStamp,
                   LastAlterTimeStamp
            FROM DBC.TablesV
            WHERE DatabaseName = '{safe_schema}'
              AND TableKind = 'V'
            ORDER BY TableName
        """
        views = self._execute_td(view_list_query)

        if not views:
            return self._spark.createDataFrame(
                [], self._view_column_schema()
            )

        # Step 2: For each view, get column metadata via HELP COLUMN.
        # Quote identifiers to handle special characters and reserved words.
        all_rows = []
        for view_name, table_comment, create_time, alter_time in views:
            quoted_schema = schema_name.replace('"', '""')
            quoted_view = view_name.replace('"', '""')
            help_query = f'HELP COLUMN "{quoted_schema}"."{quoted_view}".*'
            try:
                with self._td_conn.cursor() as cur:
                    cur.execute(help_query)
                    col_descriptions = cur.description
                    columns = cur.fetchall()

                    # Build column name index from cursor description
                    col_idx = {
                        desc[0]: i
                        for i, desc in enumerate(col_descriptions)
                    }

                    for col in columns:
                        col_name = col[col_idx["Column Name"]]
                        col_type = col[col_idx["Type"]]
                        nullable = col[col_idx["Nullable"]]
                        max_len = col[col_idx["Max Length"]]
                        dec_total = col[col_idx["Decimal Total Digits"]]
                        dec_frac = col[col_idx["Decimal Fractional Digits"]]
                        comment = col[col_idx.get("Comment", -1)] \
                            if "Comment" in col_idx else None

                        all_rows.append((
                            view_name,
                            col_name.strip() if col_name else "",
                            col_type.strip() if col_type else None,
                            max_len,
                            dec_total,
                            dec_frac,
                            "Y" if nullable == "Y" else "N",
                            table_comment,
                            comment,
                            None,  # DATA_DEFAULT not in HELP COLUMN
                            create_time,
                            alter_time,
                        ))
            except Exception as e:
                # Fall back to DBC.ColumnsV so the view still appears
                # in the catalog (with NULL types) rather than being
                # silently dropped.
                msg = str(e).split("\n")[0].strip()
                print(f"Warning: HELP COLUMN failed for "
                      f"{schema_name}.{view_name}, falling back to "
                      f"DBC.ColumnsV: {msg}")
                fallback_rows = self._get_view_fallback(
                    schema_name, view_name, table_comment,
                    create_time, alter_time,
                )
                all_rows.extend(fallback_rows)

        return self._spark.createDataFrame(
            all_rows, self._view_column_schema()
        )

    def _get_view_fallback(
        self, schema_name, view_name, table_comment,
        create_time, alter_time,
    ) -> List[Tuple[Any, ...]]:
        """Fall back to DBC.ColumnsV for a view when HELP COLUMN fails.

        Returns rows with NULL types — the view still appears in the
        catalog rather than being silently dropped.
        """
        rows = []
        try:
            safe_schema = schema_name.replace("'", "''")
            safe_view = view_name.replace("'", "''")
            fallback_query = f"""
                SELECT TRIM(ColumnName), Nullable
                FROM DBC.ColumnsV
                WHERE DatabaseName = '{safe_schema}'
                  AND TableName = '{safe_view}'
            """
            for col_name, nullable in self._execute_td(fallback_query):
                rows.append((
                    view_name,
                    col_name.strip() if col_name else "",
                    None,   # DATA_TYPE unknown
                    None,   # COLUMN_LENGTH
                    None,   # DECIMAL_TOTAL_DIGITS
                    None,   # DECIMAL_FRACTIONAL_DIGITS
                    "Y" if nullable == "Y" else "N",
                    table_comment,
                    None,   # COLUMN_COMMENT
                    None,   # DATA_DEFAULT
                    create_time,
                    alter_time,
                ))
        except Exception as e2:
            msg2 = str(e2).split("\n")[0].strip()
            print(f"Warning: DBC.ColumnsV fallback also failed for "
                  f"{schema_name}.{view_name}: {msg2}")
        return rows

    def close(self) -> None:
        """Close the teradatasql connection and stop the SparkSession."""
        if getattr(self, "_td_conn", None) is not None:
            try:
                self._td_conn.close()
            except Exception:
                pass
            finally:
                self._td_conn = None
        if getattr(self, "_spark", None) is not None:
            try:
                self._spark.stop()
            except Exception:
                pass
            finally:
                self._spark = None

    def __enter__(self) -> "TeradataConnector":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    @staticmethod
    def _view_column_schema():
        """Schema matching _get_tables output for DataFrame creation."""
        from pyspark.sql.types import (
            StructType, StructField, StringType,
            IntegerType, TimestampType,
        )
        return StructType([
            StructField("TABLE_NAME", StringType()),
            StructField("COLUMN_NAME", StringType()),
            StructField("DATA_TYPE", StringType()),
            StructField("COLUMN_LENGTH", IntegerType()),
            StructField("DECIMAL_TOTAL_DIGITS", IntegerType()),
            StructField("DECIMAL_FRACTIONAL_DIGITS", IntegerType()),
            StructField("IS_NULLABLE", StringType()),
            StructField("TABLE_COMMENT", StringType()),
            StructField("COLUMN_COMMENT", StringType()),
            StructField("DATA_DEFAULT", StringType()),
            StructField("TABLE_CREATE_TIME", TimestampType()),
            StructField("TABLE_LAST_ALTER_TIME", TimestampType()),
        ])
