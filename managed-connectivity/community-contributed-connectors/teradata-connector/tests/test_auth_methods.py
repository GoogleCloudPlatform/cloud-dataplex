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

"""Tests for --logmech and --logdata CLI arguments."""

from unittest import mock

import pytest

from src.cmd_reader import read_args


# Base args with user + password (TD2 default scenario)
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

# Base args without user/password (for LDAP/JWT tests)
BASE_ARGS_NO_CREDS = [
    "main.py",
    "--target_project_id", "test-project",
    "--target_location_id", "us-central1",
    "--target_entry_group_id", "teradata",
    "--host", "10.25.56.44",
    "--local_output_only",
]


class TestDefaultBehavior:
    """Default (no --logmech) must behave like TD2."""

    def test_default_requires_user_and_password(self):
        """No --logmech still requires --user and a password method."""
        with mock.patch("sys.argv", BASE_ARGS):
            config = read_args()
            assert config["logmech"] is None
            assert config["user"] == "testuser"
            assert config["password"] == "testpass"

    def test_default_missing_user_raises(self):
        args = [
            "main.py",
            "--target_project_id", "test-project",
            "--target_location_id", "us-central1",
            "--target_entry_group_id", "teradata",
            "--host", "10.25.56.44",
            "--password", "testpass",
            "--local_output_only",
        ]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()

    def test_default_missing_password_raises(self):
        args = [
            "main.py",
            "--target_project_id", "test-project",
            "--target_location_id", "us-central1",
            "--target_entry_group_id", "teradata",
            "--host", "10.25.56.44",
            "--user", "testuser",
            "--local_output_only",
        ]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()


class TestLogmechValidation:
    """Logmech value validation and normalization."""

    @pytest.mark.parametrize("logmech", [
        "TD2", "LDAP", "JWT",
    ])
    def test_valid_logmech_accepted(self, logmech):
        args = BASE_ARGS + ["--logmech", logmech]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == logmech

    @pytest.mark.parametrize("logmech", [
        "BROWSER", "INVALID", "browser", "OAuth", "", "TDNEGO", "KRB5",
    ])
    def test_invalid_logmech_rejected(self, logmech):
        args = BASE_ARGS + ["--logmech", logmech]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()

    @pytest.mark.parametrize("input_val,expected", [
        ("ldap", "LDAP"),
        ("td2", "TD2"),
        ("jwt", "JWT"),
        ("Ldap", "LDAP"),
    ])
    def test_logmech_normalized_to_uppercase(self, input_val, expected):
        args = BASE_ARGS + ["--logmech", input_val]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == expected


class TestLDAPAuth:
    """LDAP — user/password optional."""

    def test_ldap_without_credentials(self):
        args = BASE_ARGS_NO_CREDS + ["--logmech", "LDAP"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == "LDAP"
            assert config["user"] == ""
            assert config["password"] == ""

    def test_ldap_with_user_and_password(self):
        args = BASE_ARGS + ["--logmech", "LDAP"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == "LDAP"
            assert config["user"] == "testuser"
            assert config["password"] == "testpass"

    def test_ldap_with_logdata(self):
        args = BASE_ARGS_NO_CREDS + [
            "--logmech", "LDAP",
            "--logdata", "authcid=user realm=CORP",
        ]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == "LDAP"
            assert config["logdata"] == "authcid=user realm=CORP"

    def test_ldap_with_creds_and_logdata(self):
        args = BASE_ARGS + [
            "--logmech", "LDAP",
            "--logdata", "authcid=user realm=CORP",
        ]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == "LDAP"
            assert config["user"] == "testuser"
            assert config["password"] == "testpass"
            assert config["logdata"] == "authcid=user realm=CORP"


class TestJWTAuth:
    """JWT — user/password optional, logdata optional."""

    def test_jwt_without_credentials(self):
        args = BASE_ARGS_NO_CREDS + ["--logmech", "JWT"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == "JWT"
            assert config["user"] == ""
            assert config["password"] == ""

    def test_jwt_with_logdata(self):
        args = BASE_ARGS_NO_CREDS + [
            "--logmech", "JWT",
            "--logdata", "token=eyJhbGciOiJSUzI1NiJ9...",
        ]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == "JWT"
            assert config["logdata"] == "token=eyJhbGciOiJSUzI1NiJ9..."


class TestTD2Auth:
    """TD2 — explicit logmech, user + password required."""

    def test_td2_with_credentials(self):
        args = BASE_ARGS + ["--logmech", "TD2"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logmech"] == "TD2"
            assert config["user"] == "testuser"

    def test_td2_missing_user_raises(self):
        args = BASE_ARGS_NO_CREDS + [
            "--logmech", "TD2",
            "--password", "testpass",
        ]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()

    def test_td2_missing_password_raises(self):
        args = BASE_ARGS_NO_CREDS + [
            "--logmech", "TD2",
            "--user", "testuser",
        ]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()


class TestLogdata:
    """--logdata argument handling."""

    def test_logdata_without_logmech_accepted(self):
        args = BASE_ARGS + ["--logdata", "somedata"]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logdata"] == "somedata"
            assert config["logmech"] is None

    def test_logdata_default_is_none(self):
        with mock.patch("sys.argv", BASE_ARGS):
            config = read_args()
            assert config["logdata"] is None

    @mock.patch("src.cmd_reader.get_password", return_value="authcid=user")
    @mock.patch("src.cmd_reader.validateSecretID")
    def test_logdata_secret_resolves(self, mock_validate, mock_get):
        args = BASE_ARGS + [
            "--logdata_secret",
            "projects/my-proj/secrets/ldap-logdata",
        ]
        with mock.patch("sys.argv", args):
            config = read_args()
            assert config["logdata"] == "authcid=user"
            mock_validate.assert_called_once_with(
                "projects/my-proj/secrets/ldap-logdata"
            )
            mock_get.assert_called_once_with(
                "projects/my-proj/secrets/ldap-logdata"
            )

    def test_logdata_and_logdata_secret_mutual_exclusion(self):
        args = BASE_ARGS + [
            "--logdata", "somedata",
            "--logdata_secret",
            "projects/my-proj/secrets/ldap-logdata",
        ]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit):
                read_args()
