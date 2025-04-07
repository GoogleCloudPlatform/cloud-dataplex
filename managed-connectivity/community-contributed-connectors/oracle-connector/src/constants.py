"""Constants that are used in the different files."""
import enum

SOURCE_TYPE = "oracle"

# allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.oracle_connector"
CONNECTOR_CLASS = "OracleConnector"

class EntryType(enum.Enum):
    """Types of Oracle entries."""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/oracle-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/oracle-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/oracle-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/oracle-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/oracle-view"

# Top-level entries from above hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY = [EntryType.INSTANCE, EntryType.DATABASE]

# EntryType in hierarchy under which database objects (tables, views) are organised and processed ( = schema-level)
COLLECTION_ENTRY = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS = [EntryType.TABLE, EntryType.VIEW]

def generateFileName(config: dict[str:str]) -> str:
    filename = ''
    if config.get('sid'):
        filename = f"{SOURCE_TYPE}-{config['host']}-{config['sid']}.jsonl"
    elif config.get('service') and config.get('service') is not None:
        filename = f"{SOURCE_TYPE}-{config['host']}-{config['service']}.jsonl"
    return filename
