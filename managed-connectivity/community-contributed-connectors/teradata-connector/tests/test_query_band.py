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

"""Tests for query band validation and normalization."""

from unittest import mock

import pytest

from src.common.argument_validator import (
    validateQueryBand,
    DEFAULT_QUERY_BAND,
    DEFAULT_QUERY_BAND_ORG,
    DEFAULT_QUERY_BAND_APPNAME,
)
from src.cmd_reader import read_args

# Required CLI args for read_args() to succeed
BASE_ARGS = [
    "main.py",
    "--target_project_id", "test-project",
    "--target_location_id", "us-central1",
    "--target_entry_group_id", "teradata",
    "--host", "10.25.56.44",
    "--user", "testuser",
    "--password", "testpass",
    "--local_output_only",
]


class TestValidateQueryBandDefaults:
    """None, empty, or whitespace input returns the default query band."""

    def test_none_returns_default(self):
        assert validateQueryBand(None) == DEFAULT_QUERY_BAND

    def test_empty_string_returns_default(self):
        assert validateQueryBand("") == DEFAULT_QUERY_BAND

    def test_whitespace_only_returns_default(self):
        assert validateQueryBand("   ") == DEFAULT_QUERY_BAND

    def test_default_contains_org_and_appname(self):
        assert DEFAULT_QUERY_BAND == (
            f"org={DEFAULT_QUERY_BAND_ORG};"
            f"appname={DEFAULT_QUERY_BAND_APPNAME};"
        )


class TestValidateQueryBandValidInput:
    """Valid query band strings are normalized correctly."""

    def test_custom_key_gets_defaults_prepended(self):
        result = validateQueryBand("team=analytics;")
        assert result.startswith(f"org={DEFAULT_QUERY_BAND_ORG};")
        assert f"appname={DEFAULT_QUERY_BAND_APPNAME};" in result
        assert "team=analytics;" in result

    def test_custom_org_preserved(self):
        result = validateQueryBand("org=myorg;")
        assert result.startswith("org=myorg;")

    def test_custom_appname_gets_default_appended(self):
        result = validateQueryBand("appname=myapp;")
        assert f"appname=myapp_{DEFAULT_QUERY_BAND_APPNAME};" in result

    def test_exact_default_appname_not_duplicated(self):
        result = validateQueryBand(f"appname={DEFAULT_QUERY_BAND_APPNAME};")
        assert result.count(DEFAULT_QUERY_BAND_APPNAME) == 1

    def test_trailing_semicolon_added(self):
        result = validateQueryBand("team=analytics")
        assert result.endswith(";")
        assert "team=analytics;" in result

    def test_extra_keys_preserved_after_org_appname(self):
        result = validateQueryBand("org=myorg;appname=myapp;env=prod;")
        parts = result.split(";")
        # org first, appname second, env after
        assert parts[0].startswith("org=")
        assert parts[1].startswith("appname=")
        assert "env=prod" in parts[2]

    def test_ordering_org_appname_first(self):
        """Even if user provides appname before org, output is org → appname → rest."""
        result = validateQueryBand("appname=myapp;org=myorg;team=z;")
        parts = result.split(";")
        assert parts[0] == "org=myorg"
        assert parts[1].startswith("appname=myapp")
        assert parts[2] == "team=z"

    def test_whitespace_in_value_preserved(self):
        result = validateQueryBand("team=data analytics;")
        assert "team=data analytics;" in result

    def test_dots_in_value_allowed(self):
        result = validateQueryBand("version=1.2.3;")
        assert "version=1.2.3;" in result

    def test_uppercase_org_recognized(self):
        """ORG=myorg should be normalized to org=myorg, not create a duplicate."""
        result = validateQueryBand("ORG=myorg;")
        assert result.count("org=") == 1
        assert "org=myorg;" in result

    def test_mixed_case_appname_recognized(self):
        """APPNAME=myapp should be normalized to appname key."""
        result = validateQueryBand("APPNAME=myapp;")
        assert result.count("appname=") == 1
        assert f"appname=myapp_{DEFAULT_QUERY_BAND_APPNAME};" in result

    def test_uppercase_keys_no_duplicates(self):
        """ORG and APPNAME should not produce duplicate org/appname entries."""
        result = validateQueryBand("ORG=myorg;APPNAME=myapp;team=z;")
        assert result.count("org=") == 1
        assert result.count("appname=") == 1
        assert "team=z;" in result


class TestValidateQueryBandRejection:
    """Invalid query band strings are rejected with SystemExit."""

    def test_single_quote_rejected(self):
        with pytest.raises(SystemExit):
            validateQueryBand("team=ana'lytics;")

    def test_double_quote_rejected(self):
        with pytest.raises(SystemExit):
            validateQueryBand('team="analytics";')

    def test_parentheses_rejected(self):
        with pytest.raises(SystemExit):
            validateQueryBand("team=(analytics);")

    def test_reserved_name_proxyuser(self):
        with pytest.raises(SystemExit, match="reserved name"):
            validateQueryBand("proxyuser=admin;")

    def test_reserved_name_proxyrole_case_insensitive(self):
        with pytest.raises(SystemExit, match="reserved name"):
            validateQueryBand("PROXYROLE=admin;")

    def test_malformed_segment_no_equals(self):
        with pytest.raises(SystemExit, match="malformed segment"):
            validateQueryBand("noequals;")

    def test_empty_key_rejected(self):
        """Segment like '=value;' has an empty key and must be rejected."""
        with pytest.raises(SystemExit, match="empty key"):
            validateQueryBand("=value;")

    def test_exceeds_max_length(self):
        long_value = "a" * 2049
        with pytest.raises(SystemExit, match="maximum length"):
            validateQueryBand(f"key={long_value};")


class TestQueryBandCLIIntegration:
    """Tests for --query_band via cmd_reader.read_args()."""

    def test_default_when_not_specified(self):
        with mock.patch("sys.argv", BASE_ARGS):
            config = read_args()
            assert config["query_band"] == DEFAULT_QUERY_BAND

    def test_custom_query_band(self):
        args = BASE_ARGS + ["--query_band", "team=analytics;"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert "team=analytics;" in config["query_band"]
            assert config["query_band"].startswith(f"org={DEFAULT_QUERY_BAND_ORG};")

    def test_invalid_query_band_rejected(self):
        args = BASE_ARGS + ["--query_band", "team='evil;"]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()
