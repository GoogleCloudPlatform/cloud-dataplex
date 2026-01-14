"""Constants that are used in the different files."""
import enum

SOURCE_TYPE = "aws_glue"

# Short keys for the aspects map
SCHEMA_ASPECT_KEY = "dataplex-types.global.schema"

# Keys for custom marker aspects (templates)
DATABASE_ASPECT_KEY_TEMPLATE = "{project}.{location}.aws-glue-database"
TABLE_ASPECT_KEY_TEMPLATE = "{project}.{location}.aws-glue-table"
VIEW_ASPECT_KEY_TEMPLATE = "{project}.{location}.aws-glue-view"
LINEAGE_ASPECT_KEY_TEMPLATE = "{project}.{location}.aws-lineage-aspect"

# Full paths for the aspect_type field
SCHEMA_ASPECT_PATH = "projects/dataplex-types/locations/global/aspectTypes/schema"
LINEAGE_ASPECT_PATH = "projects/{project}/locations/{location}/aspectTypes/aws-lineage-aspect"
DATABASE_ASPECT_PATH = "projects/{project}/locations/{location}/aspectTypes/aws-glue-database"
TABLE_ASPECT_PATH = "projects/{project}/locations/{location}/aspectTypes/aws-glue-table"
VIEW_ASPECT_PATH = "projects/{project}/locations/{location}/aspectTypes/aws-glue-view"

class EntryType(enum.Enum):
    """Types of AWS Glue entries."""
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-database"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-view"