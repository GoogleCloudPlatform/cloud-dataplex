"""Constants that are used in the different files."""
import enum

SOURCE_TYPE = "aws_glue"

# Short keys for the aspects map
SCHEMA_ASPECT_KEY = "dataplex-types.global.schema"
LINEAGE_ASPECT_KEY = "gcve-demo-408018.us-central1.aws-lineage-aspect"

# New keys for custom marker aspects
DATABASE_ASPECT_KEY = "gcve-demo-408018.us-central1.aws-glue-database"
TABLE_ASPECT_KEY = "gcve-demo-408018.us-central1.aws-glue-table"
VIEW_ASPECT_KEY = "gcve-demo-408018.us-central1.aws-glue-view"

# Full paths for the aspect_type field
SCHEMA_ASPECT_PATH = "projects/dataplex-types/locations/global/aspectTypes/schema"
LINEAGE_ASPECT_PATH = "projects/{project}/locations/{location}/aspectTypes/aws-lineage-aspect"

class EntryType(enum.Enum):
    """Types of AWS Glue entries."""
    DATABASE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-database"
    TABLE: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-table"
    VIEW: str = "projects/{project}/locations/{location}/entryTypes/aws-glue-view"