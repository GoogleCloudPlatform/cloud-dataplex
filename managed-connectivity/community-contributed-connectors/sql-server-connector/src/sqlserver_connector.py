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

"""Reads SQL Server using PySpark."""
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.common.ExternalSourceConnector import IExternalSourceConnector
from src.constants import EntryType
from src.common.connection_jar import getJarPath

class SQLServerConnector(IExternalSourceConnector):
    """Reads data from SQL Server and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):

        # Get jar file, allowing override for local jar file (different version / name)
        jar_path = getJarPath(config)

        self._spark = SparkSession.builder.appName("SQLServerIngestor") \
            .config("spark.jars", jar_path) \
            .getOrCreate()

        self._config = config

        if config['instancename'] and len(config['instancename']) > 0:
            self._url = f"jdbc:sqlserver://{config['host']}\{config['instancename']}:{config['port']}"
        else:
            self._url = f"jdbc:sqlserver://{config['host']}:{config['port']}"

        self._connectOptions = {
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "url": self._url,
            "user": config['user'],
            "database": config['database'],
            "password": config['password'],
            "loginTimeout": config['login_timeout'],
            "encrypt" : config['encrypt'],
            "authentication" : config['authentication'],
            "trustServerCertificate": config['trust_server_certificate']
            }
        
        if config.get('hostname_in_certificate') is not None:
            self._connectOptions.hostNameInCertificate = config['hostname_in_certificate']

    def _execute(self, query: str) -> DataFrame:
        """A generic method to execute any query."""
        return self._spark.read.format("jdbc") \
            .options(**self._connectOptions) \
            .option("query", query) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        """Gets a list of schemas in the database"""
        query = """
        SELECT s.name AS SCHEMA_NAME
        FROM sys.schemas s
        WHERE s.name NOT in ('db_accessadmin','db_backupoperator','db_datareader','db_datawriter','db_ddladmin','db_denydatareader','db_denydatawriter','db_owner','db_securityadmin','guest','sys','INFORMATION_SCHEMA')
        """
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Gets a list of columns in tables or views."""
        # Every line here is a column that belongs to the table or to the view.
        # This SQL gets data from ALL the tables in a given schema.
        return (f"SELECT t.name AS TABLE_NAME, "
                f"c.name AS COLUMN_NAME, "
                f"ty.name AS DATA_TYPE, "
                f"c.is_nullable AS IS_NULLABLE "
                f"FROM sys.columns c "
                f"JOIN sys.tables t ON t.object_id = c.object_id "
                f"JOIN sys.types ty ON ty.user_type_id = c.system_type_id "
                f"JOIN sys.schemas s ON s.schema_id = t.schema_id "
                f"WHERE s.name = '{schema_name}' "
                f"AND t.type = '{object_type}'")

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        # Dataset means that these entities can contain end user data.
        short_type = {"TABLE":"U", "VIEW":"V"}
        query = self._get_columns(schema_name, short_type[entry_type.name])
        return self._execute(query)
