"""Constants that are used in the different files."""
import enum

SOURCE_TYPE = "postgresql"

# allow common bootstrap to load connector for specific datasource
CONNECTOR_MODULE = "src.postgres_connector"
CONNECTOR_CLASS = "PostgresConnector"

class EntryType(enum.Enum):
    """Types of Postgres entries."""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/postgresql-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/postgresql-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/postgresql-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/postgresql-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/postgresql-view"

# Top-level entries from above hierarchy which will be written to file before schema processing starts
TOP_ENTRY_HIERARCHY = [EntryType.INSTANCE, EntryType.DATABASE]

# EntryType in hierarchy under which database objects like tables, views are organised and processed ( = schema-level)
COLLECTION_ENTRY = EntryType.DB_SCHEMA

# DB objects to extract metadata for
DB_OBJECT_TYPES_TO_PROCESS = [EntryType.TABLE, EntryType.VIEW]

def generateFileName(config: dict[str:str]) -> str:
    return f"{SOURCE_TYPE}-{config['host']}-{config['database']}.jsonl"