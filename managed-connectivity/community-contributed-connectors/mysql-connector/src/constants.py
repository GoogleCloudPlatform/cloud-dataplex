## MySQL specific constants and functions
import enum

SOURCE_TYPE = "mysql"

# Expose to allow common code to load connector for MySQL
CONNECTOR_MODULE = "src.mysql_connector"
CONNECTOR_CLASS = "MysqlConnector"

class EntryType(enum.Enum):
    """Types of Mysql entries. Instance, database, table/view"""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/mysql-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/mysql-database"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/mysql-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/mysql-view"

# Top-level entries from EntryType hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY = [EntryType.INSTANCE, EntryType.DATABASE]

# EntryType in hierarchy under which database objects like tables, views are organised and processed
COLLECTION_ENTRY = EntryType.DATABASE

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS = [EntryType.TABLE, EntryType.VIEW]

def generateFileName(config: dict[str:str]):
    return f"{SOURCE_TYPE}-{config['host']}-{config['database']}.jsonl"