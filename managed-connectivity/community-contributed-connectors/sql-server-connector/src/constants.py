# Snowflake specific constants and functions
import enum

SOURCE_TYPE = "sqlserver"

# allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.sqlserver_connector"
CONNECTOR_CLASS = "SQLServerConnector"

class EntryType(enum.Enum):
    """Types of SQL Server entries."""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/sqlserver-view"

# Top-level entries from above hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY = [EntryType.INSTANCE, EntryType.DATABASE]

# EntryType in hierarchy under which database objects like tables, views are organised and processed
COLLECTION_ENTRY = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS = [EntryType.TABLE, EntryType.VIEW]

# metadata file name
def generateFileName(config: dict[str:str]):
    filename = ''
    if config['instancename'] and len(config['instancename']) > 0:
        filename = f"{SOURCE_TYPE}-{config['host']}-{config['instancename']}-{config['database']}.jsonl"
    else:
        filename = f"{SOURCE_TYPE}-{config['host']}-defaultinstance-{config['database']}.jsonl"
    return filename