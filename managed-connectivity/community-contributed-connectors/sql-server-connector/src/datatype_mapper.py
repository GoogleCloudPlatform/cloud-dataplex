# Maps data types from SQL Server to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on SQL Server native type."""
    if data_type in ["bigint", "int", "smallint", "tinyint", "decimal", "numeric", "smallmoney", "money", "float", "real"]:
        return "NUMBER"
    if data_type in ["varchar", "nvarchar", "char", "nchar", "text", "ntext", "xml"]:
        return "STRING"
    if data_type in ["binary", "varbinary", "image", "geography", "geometry"]:
        return "BYTES"
    if data_type in ["date", "datetime", "datetime2", "smalldatetime", "datetimeoffset"]:
        return "DATETIME"
    if data_type in ["time"]:
        return "TIME"
    return "OTHER"