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

from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.constants import EntryType
from src.common.connection_jar import getJarPath
from src.common.util import fileExists
from src.constants import JDBC_JAR
from src.constants import SNOWFLAKE_SPARK_JAR
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import re
import os
import io

class SnowflakeConnector:
    """Reads data from Snowflake and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Get jar file, allowing override for local jar file (different version / name)
        jar_path = getJarPath(config,[SNOWFLAKE_SPARK_JAR,JDBC_JAR])
        # Check jar files exist. Throws exception if not found
        jarsExist = fileExists(jar_path)

        self._spark = SparkSession.builder.appName("SnowflakeIngestor") \
            .config("spark.jars",jar_path) \
            .config("spark.log.level", "ERROR") \
            .getOrCreate()

        self._url = f"{config['account']}.snowflakecomputing.com"
        
        self._sfOptions = {
            "sfURL": self._url,
            "sfUser": config['user'],
            "sfDatabase": config['database'],
            }
        
        # Build connection parameters
        if not config.get('authentication') is None:
            match config['authentication']:
                case 'oauth':
                    self._sfOptions['sfAuthenticator'] = config['authentication']
                    self._sfOptions['sfToken'] = config['token']
                case 'password':
                    self._sfOptions['sfPassword'] = config['password']
                case 'key-pair':
                    # Option to provide passphrase via environment variable
                    passphrase = os.environ.get('PRIVATE_KEY_PASSPHRASE')
                    if passphrase is not None:
                        passphrase = config['passphrase_file']
                        if passphrase is not None:
                            passphrase = config['passphrase_secret']

                    if passphrase is not None:
                        passphrase = passphrase.encode()
 
                    p_key = serialization.load_pem_private_key(
                    data=bytes(config['key_secret'], 'utf-8'),
                    password = passphrase,
                    backend = default_backend()
                    )

                    pkb = p_key.private_bytes(
                    encoding = serialization.Encoding.PEM,
                    format = serialization.PrivateFormat.PKCS8,
                    encryption_algorithm = serialization.NoEncryption()
                    )

                    pkb = pkb.decode("UTF-8")
                    pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n","",pkb).replace("\n","")

                    self._sfOptions['pem_private_key'] = pkb
        else:
                self._sfOptions['sfPassword'] = config['password']
        
        if config.get('warehouse') is not None:
            self._sfOptions['sfWarehouse'] = config['warehouse']

        if config.get('schema') is not None:
            self._sfOptions['sfSchema'] = config['schema']
        
        if config.get('role') is not None:
            self._sfOptions['sfRole'] = config['role']

    def _execute(self, query: str) -> DataFrame:
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

        return self._spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**self._sfOptions) \
            .option("query", query) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('INFORMATION_SCHEMA')"
        return self._execute(query)
    
    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Returns list of columns a tables or view"""
        sql = f"""
        SELECT c.table_name, left(t.comment,1024) as TABLE_COMMENT,c.column_name, 
        c.data_type, c.is_nullable, c.comment as COLUMN_COMMENT, c.COLUMN_DEFAULT as DATA_DEFAULT 
        FROM information_schema.columns c 
        JOIN information_schema.tables t ON 
        c.table_catalog = t.table_catalog 
        AND c.table_schema = t.table_schema 
        AND c.table_name = t.table_name 
        WHERE c.table_schema = '{schema_name}' 
        AND t.table_type = '{object_type}'
        """
        return sql

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        short_type = entry_type.name  # table or view, or the title of enum value
        if ( short_type == "TABLE" ):
            object_type = "BASE TABLE"
        else:
            object_type = "VIEW"
        query = self._get_columns(schema_name, object_type)
        return self._execute(query)