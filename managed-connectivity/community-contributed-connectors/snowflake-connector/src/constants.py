## Snowflake specific constants and functions
import enum

SOURCE_TYPE = "snowflake"

# allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.snowflake_connector"
CONNECTOR_CLASS = "SnowflakeConnector"

class EntryType(enum.Enum):
    """Hierarchy of Snowflake entries"""
    ACCOUNT: str = "projects/{project}/locations/{location}/entryTypes/snowflake-account"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/snowflake-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/snowflake-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/snowflake-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/snowflake-view"

# Top-level types in EntryType hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY = [EntryType.ACCOUNT, EntryType.DATABASE]

# EntryType in hierarchy under which database objects like tables, views are organised and processed ( = schema-level)
COLLECTION_ENTRY = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS = [EntryType.TABLE, EntryType.VIEW]

# metadata file name 
def generateFileName(config: dict[str:str]):
    return f"{SOURCE_TYPE}-{config['account']}-{config['database']}.jsonl"