"""Reads Oracle using PySpark."""
from typing import Dict
from pyspark.sql import SparkSession, DataFrame

from src.constants import EntryType


SPARK_JAR_PATH = "/opt/spark/jars/ojdbc11.jar"


class OracleConnector:
    """Reads data from Oracle and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint
        self._spark = SparkSession.builder.appName("OracleIngestor") \
            .config("spark.jars", SPARK_JAR_PATH) \
            .getOrCreate()

        self._config = config
        self._url = f"jdbc:oracle:thin:@{config['host_port']}:{config['database']}"

    def _execute(self, query: str) -> DataFrame:
        """A generic method to execute any query."""
        return self._spark.read.format("jdbc") \
            .option("driver", "oracle.jdbc.OracleDriver") \
            .option("url", self._url) \
            .option("query", query) \
            .option("user", self._config["user"]) \
            .option("password", self._config["password"]) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        """In Oracle, schemas are usernames."""
        query = "SELECT username FROM dba_users"
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Gets a list of columns in tables or views in a batch."""
        # Every line here is a column that belongs to the table or to the view.
        # This SQL gets data from ALL the tables in a given schema.
        return (f"SELECT col.TABLE_NAME, col.COLUMN_NAME, "
                f"col.DATA_TYPE, col.NULLABLE "
                f"FROM all_tab_columns col "
                f"INNER JOIN DBA_OBJECTS tab "
                f"ON tab.OBJECT_NAME = col.TABLE_NAME "
                f"WHERE tab.OWNER = '{schema_name}' "
                f"AND tab.OBJECT_TYPE = '{object_type}'")

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        # Dataset means that these entities can contain end user data.
        short_type = entry_type.name  # table or view, or the title of enum value
        query = self._get_columns(schema_name, short_type)
        return self._execute(query)
