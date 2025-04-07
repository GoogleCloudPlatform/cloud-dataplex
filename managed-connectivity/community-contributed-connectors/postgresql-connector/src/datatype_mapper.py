# Maps data types from PostgreSQL to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on Postgres native type."""
    if data_type in ["numeric","integer","serial","double precision","decimal","smallint","smallserial","bigserial","bigint","real"]:
        return "NUMBER"
    if data_type.startswith("character") or data_type.startswith("bpchar") or data_type in ["text","varchar"]:
        return "STRING"
    if data_type.startswith("bytea"):
        return "BYTES"
    if data_type == "boolean":
        return "BOOLEAN"
    if data_type.startswith("timestamp"):
        return "TIMESTAMP"
    if data_type.startswith("date"):
        return "DATETIME"
    return "OTHER"
