# Maps datatypes from Oracle to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    """Choose the metadata type based on Mysql native type."""
    if data_type.startswith("int") or data_type.startswith("tinyint") or data_type.startswith("smallint") or data_type.startswith("mediumint") or data_type.startswith("bigint") or data_type.startswith("decimal") or data_type.startswith("numeric") or data_type.startswith("float") or  data_type.startswith("double") :
        return "NUMBER"
    if data_type.startswith("varchar") or data_type.startswith("char") or data_type.startswith("text") or data_type.startswith("tinytext") or data_type.startswith("mediumtext") or data_type.startswith("longtext"):
        return "STRING"
    if data_type.startswith("binary") or data_type.startswith("varbinary") or data_type.startswith("blob") or data_type.startswith("tinyblob") or data_type.startswith("mediumblob") or data_type.startswith("longblob"):
        return "BYTES"
    if data_type.startswith("timestamp") or data_type.startswith("datetime"):
        return "TIMESTAMP"
    if data_type.startswith("date"):
        return "DATETIME"
    return "OTHER"