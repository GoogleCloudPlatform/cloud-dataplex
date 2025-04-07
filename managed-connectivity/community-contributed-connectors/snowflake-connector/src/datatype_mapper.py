# Maps data types from Snowflake to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on Snowflake native type."""
    if data_type.startswith("NUMBER") or data_type in ["INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL", "DECIMAL", "NUMERIC"]:
        return "NUMBER"
    if data_type.startswith("VARCHAR") or data_type in ["TEXT", "STRING", "CHAR", "CHARACTER", "BINARY", "VARBINARY"]:
        return "STRING"
    if data_type.startswith("TIMESTAMP") or data_type in ["TIME", "DATETIME", "DATE"]:
        return "TIMESTAMP"
    if data_type == "BOOLEAN":
        return "BOOLEAN"
    return "OTHER"
