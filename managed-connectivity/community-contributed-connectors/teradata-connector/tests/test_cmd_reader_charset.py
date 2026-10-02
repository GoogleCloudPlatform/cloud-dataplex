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

"""Tests for the --charset CLI argument and JDBC URL construction."""

from unittest import mock

import pytest

from src.cmd_reader import read_args


# Required CLI args for cmd_reader.read_args() to succeed
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


class TestCharsetArgument:
    """Tests for --charset CLI argument parsing."""

    def test_default_charset_is_utf8(self):
        """When --charset is not specified, default is UTF8."""
        with mock.patch("sys.argv", BASE_ARGS):
            config = read_args()
            assert config["charset"] == "UTF8"

    def test_custom_charset_utf16(self):
        """--charset UTF16 is parsed correctly."""
        args = BASE_ARGS + ["--charset", "UTF16"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["charset"] == "UTF16"

    def test_custom_charset_ascii(self):
        """--charset ASCII is parsed correctly."""
        args = BASE_ARGS + ["--charset", "ASCII"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["charset"] == "ASCII"

    def test_custom_charset_kanjisjis(self):
        """Japanese Shift-JIS charset is parsed correctly."""
        args = BASE_ARGS + ["--charset", "KANJISJIS_0S"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["charset"] == "KANJISJIS_0S"

    def test_charset_included_in_config(self):
        """charset key exists in the returned config dict."""
        with mock.patch("sys.argv", BASE_ARGS):
            config = read_args()
            assert "charset" in config

    def test_charset_lowercase_preserved(self):
        """Lowercase charset value is passed through as-is."""
        args = BASE_ARGS + ["--charset", "utf8"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["charset"] == "utf8"

    def test_charset_stripped_of_whitespace(self):
        """Leading/trailing whitespace is stripped."""
        args = BASE_ARGS + ["--charset", "  UTF16  "]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["charset"] == "UTF16"

    def test_charset_rejects_comma_injection(self):
        """Comma in charset would inject JDBC URL params — must be rejected."""
        args = BASE_ARGS + ["--charset", "UTF8,LOGMECH=LDAP"]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()

    def test_charset_rejects_slash(self):
        """Slash in charset would break JDBC URL parsing — must be rejected."""
        args = BASE_ARGS + ["--charset", "UTF8/EXTRA"]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()

    def test_charset_rejects_special_chars(self):
        """Special characters in charset must be rejected."""
        args = BASE_ARGS + ["--charset", "UTF-8"]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()


class TestJdbcUrlConstruction:
    """Tests for JDBC URL charset parameter.

    The JDBC URL is constructed in TeradataConnector.__init__ which
    requires PySpark and a live connection. We test the URL construction
    logic directly here.
    """

    def _build_jdbc_url(self, config):
        """Mirror of the JDBC URL construction in teradata_connector.py.

        Keep in sync with TeradataConnector.__init__ (teradata_connector.py:56-59).
        """
        charset = config.get("charset", "UTF8")
        return (
            f"jdbc:teradata://{config['host']}"
            f"/DBS_PORT={config['port']},CHARSET={charset}"
        )

    def test_default_charset_in_url(self):
        config = {"host": "10.25.56.44", "port": 1025}
        url = self._build_jdbc_url(config)
        assert url == "jdbc:teradata://10.25.56.44/DBS_PORT=1025,CHARSET=UTF8"

    def test_utf16_charset_in_url(self):
        config = {"host": "10.25.56.44", "port": 1025, "charset": "UTF16"}
        url = self._build_jdbc_url(config)
        assert url == "jdbc:teradata://10.25.56.44/DBS_PORT=1025,CHARSET=UTF16"

    def test_ascii_charset_in_url(self):
        config = {"host": "10.25.56.44", "port": 1025, "charset": "ASCII"}
        url = self._build_jdbc_url(config)
        assert url == "jdbc:teradata://10.25.56.44/DBS_PORT=1025,CHARSET=ASCII"

    def test_custom_charset_in_url(self):
        config = {"host": "td-server", "port": 1025, "charset": "KANJISJIS_0S"}
        url = self._build_jdbc_url(config)
        assert "CHARSET=KANJISJIS_0S" in url

    def test_missing_charset_defaults_to_utf8(self):
        """Config without charset key should default to UTF8."""
        config = {"host": "10.25.56.44", "port": 1025}
        url = self._build_jdbc_url(config)
        assert "CHARSET=UTF8" in url

    def test_custom_port_with_charset(self):
        config = {"host": "10.25.56.44", "port": 2025, "charset": "UTF16"}
        url = self._build_jdbc_url(config)
        assert url == "jdbc:teradata://10.25.56.44/DBS_PORT=2025,CHARSET=UTF16"

    def test_hostname_with_charset(self):
        config = {"host": "td-server.example.com", "port": 1025, "charset": "UTF8"}
        url = self._build_jdbc_url(config)
        assert url == "jdbc:teradata://td-server.example.com/DBS_PORT=1025,CHARSET=UTF8"

    def test_lowercase_charset_in_url(self):
        """Lowercase charset flows through to JDBC URL as-is."""
        config = {"host": "10.25.56.44", "port": 1025, "charset": "utf8"}
        url = self._build_jdbc_url(config)
        assert url == "jdbc:teradata://10.25.56.44/DBS_PORT=1025,CHARSET=utf8"

    def test_mixed_case_charset_in_url(self):
        config = {"host": "10.25.56.44", "port": 1025, "charset": "Utf16"}
        url = self._build_jdbc_url(config)
        assert url == "jdbc:teradata://10.25.56.44/DBS_PORT=1025,CHARSET=Utf16"
