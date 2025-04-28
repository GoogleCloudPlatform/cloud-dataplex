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

"""Reads Oracle using PySpark."""
from typing import Dict
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from src.common.ExternalSourceConnector import IExternalSourceConnector
from src.constants import EntryType
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.common.entry_builder import COLUMN_IS_NULLABLE

class OracleConnector(IExternalSourceConnector):
    """Reads data from Oracle and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):

        # Get jar file. allow override for local jar file (different version / name)
        jar_path = getJarPath(config)

        if not fileExists(jar_path):
            raise Exception(f"Jar file not found: {jar_path}")

        self._spark = SparkSession.builder.appName("OracleIngestor") \
            .config("spark.jars", jar_path) \
            .getOrCreate()

        self._config = config
        # Use correct JDBC connection string depending on Service vs SID
        if config['sid']:
            self._url = f"jdbc:oracle:thin:@{config['host']}:{config['port']}:{config['sid']}"
        else:
            self._url = f"jdbc:oracle:thin:@{config['host']}:{config['port']}/{config['service']}"
        
        self._connectOptions = {
            "driver": "oracle.jdbc.OracleDriver",
            "url": self._url,
            "user": config['user'],
            "password": config['password']
            }

    def _execute(self, query: str) -> DataFrame:
        """A generic method to execute any query."""
        return self._spark.read.format("jdbc") \
            .options(**self._connectOptions) \
            .option("query", query) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        """Select db schemas to process. Exclude system schemas"""
        query = """
        SELECT username as SCHEMA_NAME 
        FROM dba_users 
        WHERE username not in 
        ('SYS','SYSTEM','XS$NULL','XDB','PDBADMIN',
        'OJVMSYS','LBACSYS','OUTLN',
        'DBSNMP','APPQOSSYS','DBSFWUSER',
        'GGSYS','ANONYMOUS','CTXSYS',
        'DVSYS','DVF','AUDSYS','GSMADMIN_INTERNAL',
        'OLAPSYS','MDSYS','WMSYS','GSMCATUSER',
        'MDDATA','SYSBACKUP','REMOTE_SCHEDULER_AGENT',
        'GSMUSER','SYSRAC','GSMROOTUSER','DIP','ORDPLUGINS','SYSKM','SI_INFORMTN_SCHEMA',
        'DGPDB_INT','ORDDATA','ORACLE_OCM',
        'SYS$UMF','SYSD','ORDSYS','SYSDG','PDADMIN')
        """
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        return (f"SELECT col.TABLE_NAME, col.COLUMN_NAME, "
                f"col.DATA_TYPE, col.NULLABLE as {COLUMN_IS_NULLABLE} "
                f"FROM all_tab_columns col "
                f"INNER JOIN DBA_OBJECTS tab "
                f"ON tab.OBJECT_NAME = col.TABLE_NAME "
                f"WHERE tab.OWNER = '{schema_name}' "
                f"AND tab.OBJECT_TYPE = '{object_type}'")

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        short_type = entry_type.name  # table or view, or the title of enum value
        query = self._get_columns(schema_name, short_type)
        return self._execute(query)
