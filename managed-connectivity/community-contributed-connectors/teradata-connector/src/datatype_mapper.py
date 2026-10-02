# Copyright 2026 Teradata
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Maps Teradata data types to Dataplex Catalog metadata types.

Handles both short codes from DBC.ColumnsV.ColumnType (e.g. I, CV, DA)
and full type names from the ColumnType() SQL function (e.g. INTEGER,
VARCHAR, DATE) for maximum compatibility across Teradata versions.
"""


def get_catalog_metadata_type(data_type: str) -> str:
    """Map Teradata type to Dataplex metadata type."""
    if data_type is None:
        return "OTHER"
    dt = data_type.strip().upper()

    # --- Short codes (DBC.ColumnsV.ColumnType raw values) ---
    # Date/Time short codes (check before numeric since DA = DATE)
    if dt in ("DA", "AT", "TZ"):
        return "DATETIME"
    if dt in ("TS", "SZ"):
        return "TIMESTAMP"

    # Numeric short codes
    if dt in ("I", "I1", "I2", "I8", "D", "F", "N"):
        return "NUMBER"

    # String short codes
    if dt in ("CV", "CF", "CO", "LV", "JN", "XM", "GF", "GV", "GL"):
        return "STRING"

    # Bytes short codes
    if dt in ("BV", "BF", "BO"):
        return "BYTES"

    # Interval short codes
    if dt in ("YR", "YM", "MO", "DY", "DH", "DM", "DS",
              "HR", "HM", "HS", "MI", "MS", "SC"):
        return "OTHER"

    # Period short codes
    if dt in ("PD", "PT", "PS", "PM", "PZ"):
        return "OTHER"

    # UDT, Dataset, Array short codes
    if dt in ("UT", "DT", "A1", "AN"):
        return "OTHER"

    # --- Full type names (from ColumnType() function) ---
    # ColumnType() may return names with size qualifiers, e.g.
    # DECIMAL(18,2), VARCHAR(100), BYTE(10), FLOAT(53), CLOB(1M)

    # Numeric types (check BYTEINT before BYTE to avoid conflict)
    if dt in ("INTEGER", "SMALLINT", "BIGINT", "REAL") or \
       dt.startswith("BYTEINT") or \
       dt.startswith("FLOAT") or dt.startswith("DOUBLE") or \
       dt.startswith("DECIMAL") or dt.startswith("NUMERIC") or \
       dt.startswith("NUMBER"):
        return "NUMBER"

    # String types (check LONG VARCHAR before VARCHAR, VARGRAPHIC before GRAPHIC)
    if dt.startswith("LONG VARCHAR") or dt.startswith("LONG VARGRAPHIC") or \
       dt.startswith("VARCHAR") or dt.startswith("VARGRAPHIC") or \
       dt.startswith("CHAR") or dt.startswith("GRAPHIC") or \
       dt.startswith("CLOB") or \
       dt.startswith("JSON") or dt.startswith("XML"):
        return "STRING"

    # Binary types (check LONG VARBYTE and VARBYTE before BYTE)
    if dt.startswith("LONG VARBYTE") or dt.startswith("VARBYTE") or \
       dt.startswith("BLOB") or dt.startswith("BYTE"):
        return "BYTES"

    # Timestamp types (check before TIME to avoid TIMESTAMP matching TIME)
    if dt.startswith("TIMESTAMP"):
        return "TIMESTAMP"

    # Date/Time types
    if dt == "DATE":
        return "DATETIME"
    if dt.startswith("TIME"):
        return "DATETIME"

    # Boolean (Teradata 16.20+)
    if dt == "BOOLEAN":
        return "BOOLEAN"

    # Geospatial, Interval, Period, UDT, Dataset, Array, etc.
    return "OTHER"


# Mapping from DBC.ColumnsV short codes to human-readable type names.
_SHORT_CODE_TO_NAME = {
    # Numeric
    "I": "INTEGER",
    "I1": "BYTEINT",
    "I2": "SMALLINT",
    "I8": "BIGINT",
    "D": "DECIMAL",
    "F": "FLOAT",
    "N": "NUMBER",
    # String
    "CV": "VARCHAR",
    "CF": "CHAR",
    "CO": "CLOB",
    "LV": "LONG VARCHAR",
    "JN": "JSON",
    "XM": "XML",
    "GF": "GRAPHIC",
    "GV": "VARGRAPHIC",
    "GL": "LONG VARGRAPHIC",
    # Bytes
    "BV": "VARBYTE",
    "BF": "BYTE",
    "BO": "BLOB",
    # Date/Time
    "DA": "DATE",
    "AT": "TIME",
    "TS": "TIMESTAMP",
    "SZ": "TIMESTAMP WITH TIME ZONE",
    "TZ": "TIME WITH TIME ZONE",
    # Interval
    "YR": "INTERVAL YEAR",
    "YM": "INTERVAL YEAR TO MONTH",
    "MO": "INTERVAL MONTH",
    "DY": "INTERVAL DAY",
    "DH": "INTERVAL DAY TO HOUR",
    "DM": "INTERVAL DAY TO MINUTE",
    "DS": "INTERVAL DAY TO SECOND",
    "HR": "INTERVAL HOUR",
    "HM": "INTERVAL HOUR TO MINUTE",
    "HS": "INTERVAL HOUR TO SECOND",
    "MI": "INTERVAL MINUTE",
    "MS": "INTERVAL MINUTE TO SECOND",
    "SC": "INTERVAL SECOND",
    # Period
    "PD": "PERIOD(DATE)",
    "PT": "PERIOD(TIME)",
    "PS": "PERIOD(TIMESTAMP)",
    "PM": "PERIOD(TIME WITH TIME ZONE)",
    "PZ": "PERIOD(TIMESTAMP WITH TIME ZONE)",
    # Other
    "UT": "UDT",
    "DT": "DATASET",
    "A1": "ARRAY",
    "AN": "MULTI-DIMENSIONAL ARRAY",
}


def get_readable_type_name(data_type: str) -> str:
    """Convert a Teradata short code to a human-readable type name.

    If the value is already a full name (e.g. from ColumnType()),
    it is returned as-is.
    """
    if data_type is None:
        return "UNKNOWN"
    dt = data_type.strip().upper()
    return _SHORT_CODE_TO_NAME.get(dt, dt)
