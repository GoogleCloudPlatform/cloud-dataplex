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

"""Tests for Teradata datatype mapper."""

import pytest
from src.datatype_mapper import get_catalog_metadata_type
from src.datatype_mapper import get_readable_type_name


class TestNumericShortCodes:
    @pytest.mark.parametrize("td_type", [
        "I", "I1", "I2", "I8", "D", "F", "N",
    ])
    def test_numeric_short_codes(self, td_type):
        assert get_catalog_metadata_type(td_type) == "NUMBER"


class TestNumericTypes:
    @pytest.mark.parametrize("td_type", [
        "INTEGER", "SMALLINT", "BYTEINT", "BIGINT",
        "FLOAT", "REAL", "DOUBLE", "DOUBLE PRECISION",
        "DECIMAL", "NUMERIC", "NUMBER",
    ])
    def test_numeric_types(self, td_type):
        assert get_catalog_metadata_type(td_type) == "NUMBER"

    @pytest.mark.parametrize("td_type", [
        "DECIMAL(18,2)", "DECIMAL(10,0)", "NUMERIC(10)",
        "NUMBER(10)", "FLOAT(53)", "DOUBLE PRECISION",
    ])
    def test_numeric_types_with_size(self, td_type):
        assert get_catalog_metadata_type(td_type) == "NUMBER"

    def test_numeric_with_whitespace(self):
        assert get_catalog_metadata_type("  INTEGER  ") == "NUMBER"

    def test_numeric_case_insensitive(self):
        assert get_catalog_metadata_type("integer") == "NUMBER"
        assert get_catalog_metadata_type("Float") == "NUMBER"

    def test_byteint_not_confused_with_byte(self):
        assert get_catalog_metadata_type("BYTEINT") == "NUMBER"


class TestStringShortCodes:
    @pytest.mark.parametrize("td_type", [
        "CV", "CF", "CO", "LV", "JN", "XM", "GF", "GV", "GL",
    ])
    def test_string_short_codes(self, td_type):
        assert get_catalog_metadata_type(td_type) == "STRING"


class TestStringTypes:
    @pytest.mark.parametrize("td_type", [
        "VARCHAR", "VARCHAR(100)", "CHAR", "CHAR(50)",
        "CLOB", "LONG VARCHAR", "LONG VARGRAPHIC",
        "JSON", "XML",
        "GRAPHIC", "GRAPHIC(100)", "VARGRAPHIC", "VARGRAPHIC(200)",
    ])
    def test_string_types(self, td_type):
        assert get_catalog_metadata_type(td_type) == "STRING"

    @pytest.mark.parametrize("td_type", [
        "CLOB(1000000)", "LONG VARCHAR(32000)",
        "JSON(32000)", "XML(2000)",
    ])
    def test_string_types_with_size(self, td_type):
        assert get_catalog_metadata_type(td_type) == "STRING"


class TestBytesShortCodes:
    @pytest.mark.parametrize("td_type", ["BV", "BF", "BO"])
    def test_bytes_short_codes(self, td_type):
        assert get_catalog_metadata_type(td_type) == "BYTES"


class TestBytesTypes:
    @pytest.mark.parametrize("td_type", [
        "BYTE", "VARBYTE", "BLOB", "LONG VARBYTE",
    ])
    def test_bytes_types(self, td_type):
        assert get_catalog_metadata_type(td_type) == "BYTES"

    @pytest.mark.parametrize("td_type", [
        "BYTE(10)", "VARBYTE(100)", "BLOB(1000000)",
        "LONG VARBYTE(32000)",
    ])
    def test_bytes_types_with_size(self, td_type):
        assert get_catalog_metadata_type(td_type) == "BYTES"


class TestTimestampShortCodes:
    @pytest.mark.parametrize("td_type", ["TS", "SZ"])
    def test_timestamp_short_codes(self, td_type):
        assert get_catalog_metadata_type(td_type) == "TIMESTAMP"


class TestTimestampTypes:
    @pytest.mark.parametrize("td_type", [
        "TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP(6)", "TIMESTAMP(0) WITH TIME ZONE",
    ])
    def test_timestamp_types(self, td_type):
        assert get_catalog_metadata_type(td_type) == "TIMESTAMP"


class TestDateTimeShortCodes:
    @pytest.mark.parametrize("td_type", ["DA", "AT", "TZ"])
    def test_datetime_short_codes(self, td_type):
        assert get_catalog_metadata_type(td_type) == "DATETIME"


class TestDateTimeTypes:
    def test_date(self):
        assert get_catalog_metadata_type("DATE") == "DATETIME"

    @pytest.mark.parametrize("td_type", [
        "TIME", "TIME WITH TIME ZONE", "TIME(6)",
    ])
    def test_time_types(self, td_type):
        assert get_catalog_metadata_type(td_type) == "DATETIME"


class TestBooleanType:
    def test_boolean(self):
        assert get_catalog_metadata_type("BOOLEAN") == "BOOLEAN"


class TestOtherShortCodes:
    @pytest.mark.parametrize("td_type", [
        # Interval short codes
        "YR", "YM", "MO", "DY", "DH", "DM", "DS",
        "HR", "HM", "HS", "MI", "MS", "SC",
        # Period short codes
        "PD", "PT", "PS", "PM", "PZ",
        # UDT, Dataset, Array short codes
        "UT", "DT", "A1", "AN",
    ])
    def test_other_short_codes(self, td_type):
        assert get_catalog_metadata_type(td_type) == "OTHER"


class TestOtherTypes:
    @pytest.mark.parametrize("td_type", [
        "INTERVAL YEAR", "PERIOD(DATE)", "UDT",
        "ST_GEOMETRY", "ARRAY", "DATASET",
        "MBR", "MBB", "UNKNOWN_TYPE",
    ])
    def test_other_types(self, td_type):
        assert get_catalog_metadata_type(td_type) == "OTHER"


class TestNullAndEmpty:
    def test_none(self):
        assert get_catalog_metadata_type(None) == "OTHER"

    def test_empty_string(self):
        assert get_catalog_metadata_type("") == "OTHER"


# --- Tests for get_readable_type_name ---

class TestReadableTypeName:
    @pytest.mark.parametrize("short_code,expected", [
        ("I", "INTEGER"),
        ("I1", "BYTEINT"),
        ("I2", "SMALLINT"),
        ("I8", "BIGINT"),
        ("D", "DECIMAL"),
        ("F", "FLOAT"),
        ("N", "NUMBER"),
        ("CV", "VARCHAR"),
        ("CF", "CHAR"),
        ("CO", "CLOB"),
        ("DA", "DATE"),
        ("TS", "TIMESTAMP"),
        ("BV", "VARBYTE"),
        ("JN", "JSON"),
        ("XM", "XML"),
        ("GF", "GRAPHIC"),
        ("GV", "VARGRAPHIC"),
        ("YR", "INTERVAL YEAR"),
        ("PD", "PERIOD(DATE)"),
        ("UT", "UDT"),
        ("DT", "DATASET"),
        ("A1", "ARRAY"),
    ])
    def test_short_code_to_readable(self, short_code, expected):
        assert get_readable_type_name(short_code) == expected

    def test_full_name_passthrough(self):
        assert get_readable_type_name("INTEGER") == "INTEGER"
        assert get_readable_type_name("VARCHAR") == "VARCHAR"

    def test_unknown_passthrough(self):
        assert get_readable_type_name("SOMECUSTOMTYPE") == "SOMECUSTOMTYPE"

    def test_none_returns_unknown(self):
        assert get_readable_type_name(None) == "UNKNOWN"

    def test_whitespace_handling(self):
        assert get_readable_type_name("  I  ") == "INTEGER"

    def test_case_insensitive(self):
        assert get_readable_type_name("cv") == "VARCHAR"
        assert get_readable_type_name("Da") == "DATE"
