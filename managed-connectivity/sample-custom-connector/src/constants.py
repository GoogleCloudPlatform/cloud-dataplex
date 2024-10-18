"""Constants that are used in the different files."""
import enum

SOURCE_TYPE = "oracle"

# Symbols for replacement
FORBIDDEN = "#"
ALLOWED = "!"


class EntryType(enum.Enum):
    """Types of Oracle entries."""
    INSTANCE: str = "projects/{project}/locations/{location}/entryTypes/oracle-instance"
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/oracle-database"
    DB_SCHEMA: str = "projects/{project}/locations/{location}/entryTypes/oracle-schema"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/oracle-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/oracle-view"
