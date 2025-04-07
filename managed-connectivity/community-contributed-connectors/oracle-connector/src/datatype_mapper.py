# Maps datatypes from Oracle to Dataplex Catalog

def get_catalog_metadata_type(data_type: str) -> str:
    if data_type.startswith("NUMBER") or data_type in ["INTEGER","SHORTINTEGER","LONGINTEGER","BINARY_FLOAT","BINARY_DOUBLE","FLOAT", "LONG"]:
        return "NUMBER"
    if data_type.startswith("VARCHAR") or data_type in ["NVARCHAR2","CHAR","NCHAR","CLOB","NCLOB"]:
        return "STRING"
    if data_type in ["LONG","BLOB","RAW","LONG RAW"]:
        return "BYTES"
    if data_type.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    if data_type == "BOOLEAN":
        return "BOOLEAN"
    if data_type == "DATE":
        return "DATETIME"
    return "OTHER"
