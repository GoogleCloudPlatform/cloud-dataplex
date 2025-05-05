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

"""Reads MySQL using PySpark."""
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.common.ExternalSourceConnector import IExternalSourceConnector
from src.constants import EntryType
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.constants import JDBC_JAR

class MysqlConnector(IExternalSourceConnector):
    """Reads data from MySQL and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Get jar file, allowing override for local jar file (different version / name)
        jar_path = getJarPath(config,[JDBC_JAR])
        # Check jdbc jar file exist. Throws exception if not found
        jarsExist = fileExists(jar_path)

        self._spark = SparkSession.builder.appName("MySQLIngestor") \
            .config("spark.jars", jar_path) \
            .config("spark.log.level", "ERROR") \
            .getOrCreate()

        self._config = config
        self._url = f"jdbc:mysql://{config['host']}:{config['port']}/{config['database']}?zeroDateTimeBehavior=CONVERT_TO_NULL&allowPublicKeyRetrieval=true"

        self._connectOptions = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "uRL": self._url,
            "user": config['user'],
            "password": config['password'],
            "ssl": config['use_ssl'],
            "sslmode": config['ssl_mode'],
            }

    def _execute(self, query: str) -> DataFrame:
        """A generic method to execute any query."""
        return self._spark.read.format("jdbc") \
            .options(**self._connectOptions) \
            .option("query", query) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        query = f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME = '{self._config['database']}'"
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Gets a list of columns in tables or views in a batch."""
        # Every line here is a column that belongs to the table or to the view.
        # This SQL gets data from ALL the tables in a given schema.
        return(f"select tab.table_name,col.column_name,col.data_type,col.is_nullable "
                f"from information_schema.tables as tab "
                f"inner join information_schema.columns as col "
                f"on col.table_schema = tab.table_schema "
                f"and col.table_name = tab.table_name "
                f"where tab.table_type = '{object_type}' "
                f"and tab.table_schema = '{self._config['database']}' "
                f"order by tab.table_name,col.column_name") 

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        # Dataset means that these entities can contain end user data.
        short_type =  'BASE TABLE' if entry_type.name == 'TABLE' else 'VIEW' # table or view, or the title of enum value
        query = self._get_columns(schema_name, short_type)
        return self._execute(query)
