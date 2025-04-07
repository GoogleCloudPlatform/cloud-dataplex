from typing import Dict
from pyspark.sql import SparkSession, DataFrame
from src.constants import EntryType
from src.connection_jar import SPARK_JAR_PATH

class SnowflakeConnector:
    """Reads data from Snowflake and returns Spark Dataframes."""

    def __init__(self, config: Dict[str, str]):
        # PySpark entrypoint

        # Allow override for local jar file (different version / name)
        jar_path = SPARK_JAR_PATH
        if config['jar']:
            jar_path = config['jar']

        self._spark = SparkSession.builder.appName("SnowflakeIngestor") \
            .config("spark.jars",jar_path) \
            .getOrCreate()

        self._url = f"{config['account']}.snowflakecomputing.com"
        
        self._sfOptions = {
            "sfURL": self._url,
            "sfUser": config['user'],
            "sfDatabase": config['database'],
            }
        
        # Build connection parameters
        if config.get('authenticaton') is not None:
            match config['authenticaton']:
                case 'oauth':
                    self._sfOptions['sfAuthenticator'] = config['authenticaton']
                    self._sfOptions['sfToken'] = config['token']
                case 'password':
                    self._sfOptions['sfPassword'] = config['password']
        else:
                self._sfOptions['sfPassword'] = config['password']
        
        if config.get('warehouse') is not None:
            self._sfOptions['sfWarehouse'] = config['warehouse']

        if config.get('schema') is not None:
            self._sfOptions['sfSchema'] = config['schema']
        
        if config.get('role') is not None:
            self._sfOptions['sfRole'] = config['role']

    def _execute(self, query: str) -> DataFrame:
        sfOptions = self._sfOptions
        SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

        return self._spark.read.format(SNOWFLAKE_SOURCE_NAME) \
            .options(**self._sfOptions) \
            .option("query", query) \
            .load()

    def get_db_schemas(self) -> DataFrame:
        query = f"""
        SELECT schema_name FROM information_schema.schemata 
        WHERE schema_name != 'INFORMATION_SCHEMA'
        """
        return self._execute(query)

    def _get_columns(self, schema_name: str, object_type: str) -> str:
        """Returns list of columns a tables or view"""
        return (f"SELECT c.table_name, c.column_name,  "
                f"c.data_type, c.is_nullable "
                f"FROM information_schema.columns c "
                f"JOIN information_schema.tables t ON  "
                f"c.table_catalog = t.table_catalog "
                f"AND c.table_schema = t.table_schema "
                f"AND c.table_name = t.table_name "
                f"WHERE c.table_schema = '{schema_name}' "
                f"AND t.table_type = '{object_type}'")

    def get_dataset(self, schema_name: str, entry_type: EntryType):
        """Gets data for a table or a view."""
        short_type = entry_type.name  # table or view, or the title of enum value
        if ( short_type == "TABLE" ):
            object_type = "BASE TABLE"
        else:
            object_type = "VIEW"
        query = self._get_columns(schema_name, object_type)
        return self._execute(query)