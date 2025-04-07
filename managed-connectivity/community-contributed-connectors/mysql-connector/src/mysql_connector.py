"""Reads Mysql using PySpark."""
from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.common.ExternalSourceConnector import IExternalSourceConnector
from src.constants import EntryType
from src.connection_jar import SPARK_JAR_PATH

class MysqlConnector(IExternalSourceConnector):
    """Reads data from Mysql and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Allow override for local jar file (different version / name)
        jar_path = SPARK_JAR_PATH
        if config['jar']:
            jar_path = config['jar']

        self._spark = SparkSession.builder.appName("MySQLIngestor") \
            .config("spark.jars", jar_path) \
            .getOrCreate()

        self._config = config
        self._url = f"jdbc:mysql://{config['host']}:{config['port']}/{config['database']}?zeroDateTimeBehavior=CONVERT_TO_NULL&useSSL=false&allowPublicKeyRetrieval=true"

        self._connectOptions = {
            "driver": "com.mysql.cj.jdbc.Driver",
            "uRL": self._url,
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
        """In Mysql, schemas are the databases."""
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
