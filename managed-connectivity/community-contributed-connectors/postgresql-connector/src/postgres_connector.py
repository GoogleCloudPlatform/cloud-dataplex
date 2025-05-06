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

"""Reads Postgres using PySpark."""
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.constants import EntryType
from src.common.ExternalSourceConnector import IExternalSourceConnector
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.constants import JDBC_JAR

class PostgresConnector(IExternalSourceConnector):
    """Reads data from Postgres and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Get jar file, allowing override for local jar file (different version / name)
        jar_path = getJarPath(config,[JDBC_JAR])
        # Check jdbc jar file exist. Throws exception if not found
        jarsExist = fileExists(jar_path)

        self._spark = SparkSession.builder.appName("PostgresIngestor") \
            .config("spark.jars", jar_path) \
            .config("spark.log.level", "ERROR") \
            .getOrCreate()

        self._config = config
        self._url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"

        self._connectOptions = {
            "driver": "org.postgresql.Driver",
            "url": self._url,
            "user": config['user'],
            "password": config['password'],
            "sslMode" : config['ssl_mode'],
            'ssl'     : config['use_ssl']
            } 

    def _execute(self, query: str) -> DataFrame:
        """A generic method to execute any query."""
        return self._spark.read.format("jdbc") \
            .options(**self._connectOptions) \
            .option("query", query) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        query = """
        SELECT DISTINCT schema_name 
        FROM information_schema.schemata
        WHERE schema_name NOT LIKE 'pg_%' 
        AND schema_name <> 'information_schema'
        """
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Gets a list of columns in tables or views in a batch."""
        # Every line here is a column that belongs to the table or to the view.
        # This SQL gets data from ALL the tables in a given schema.
        return (f"SELECT c.table_name, c.column_name,  "
                f"c.data_type, c.is_nullable "
                f"FROM information_schema.columns c, "
                f"information_schema.tables t "
                f"WHERE c.table_schema = '{schema_name}' "
                f"AND t.table_name = c.table_name AND t.table_schema = c.table_schema "
                f"AND c.table_catalog = '{self._config['database']}' "
                f"AND t.table_type = '{object_type}'")

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        # Dataset means that these entities can contain end user data.
        if entry_type == EntryType.TABLE:
            object_type = 'BASE TABLE' 
        if entry_type == EntryType.VIEW:
            object_type = 'VIEW'
        query = self._get_columns(schema_name, object_type)
        return self._execute(query)
