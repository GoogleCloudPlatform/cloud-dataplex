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

"""Tests for password resolution priority in cmd_reader."""

import os
from unittest import mock

import pytest

from src.cmd_reader import read_args


# Required CLI args (no password method included)
BASE_ARGS = [
    "main.py",
    "--target_project_id", "test-project",
    "--target_location_id", "us-central1",
    "--target_entry_group_id", "teradata",
    "--host", "10.25.56.44",
    "--user", "testuser",
    "--local_output_only",
]


class TestPasswordFile:
    """Tests for --password_file resolution."""

    def test_password_file_reads_content(self, tmp_path):
        pw_file = tmp_path / "pw.txt"
        pw_file.write_text("file_secret")
        args = BASE_ARGS + ["--password_file", str(pw_file)]
        with mock.patch("sys.argv", args):
            config = read_args()
        assert config["password"] == "file_secret"

    def test_password_file_takes_precedence_over_password(
        self, tmp_path, capsys
    ):
        pw_file = tmp_path / "pw.txt"
        pw_file.write_text("file_secret")
        args = BASE_ARGS + [
            "--password_file", str(pw_file),
            "--password", "cli_secret",
        ]
        with mock.patch("sys.argv", args):
            config = read_args()
        assert config["password"] == "file_secret"
        stderr = capsys.readouterr().err
        assert "WARNING" not in stderr

    def test_password_file_strips_whitespace(self, tmp_path):
        pw_file = tmp_path / "pw.txt"
        pw_file.write_text("  file_secret\n")
        args = BASE_ARGS + ["--password_file", str(pw_file)]
        with mock.patch("sys.argv", args):
            config = read_args()
        assert config["password"] == "file_secret"

    def test_password_file_empty(self, tmp_path):
        pw_file = tmp_path / "pw.txt"
        pw_file.write_text("   \n")
        args = BASE_ARGS + ["--password_file", str(pw_file)]
        with mock.patch("sys.argv", args):
            with pytest.raises(
                SystemExit, match="password file is empty"
            ):
                read_args()

    def test_password_file_not_found(self):
        args = BASE_ARGS + [
            "--password_file", "/nonexistent/path/pw.txt",
        ]
        with mock.patch("sys.argv", args):
            with pytest.raises(SystemExit, match="password file not found"):
                read_args()

    def test_password_file_unreadable(self, tmp_path):
        pw_file = tmp_path / "pw.txt"
        args = BASE_ARGS + ["--password_file", str(pw_file)]
        with mock.patch("sys.argv", args), \
             mock.patch(
                 "builtins.open",
                 side_effect=PermissionError("Permission denied"),
             ):
            with pytest.raises(
                SystemExit, match="unable to read password file"
            ):
                read_args()

    def test_password_file_invalid_utf8(self, tmp_path):
        pw_file = tmp_path / "pw.bin"
        pw_file.write_bytes(b"\xff\xfe invalid utf-8")
        args = BASE_ARGS + ["--password_file", str(pw_file)]
        with mock.patch("sys.argv", args):
            with pytest.raises(
                SystemExit, match="invalid UTF-8"
            ):
                read_args()


class TestEnvironmentVariable:
    """Tests for TERADATA_PASSWORD env var resolution."""

    def test_env_var_used_when_no_other_method(self):
        with mock.patch("sys.argv", BASE_ARGS), \
             mock.patch.dict(
                 os.environ, {"TERADATA_PASSWORD": "env_secret"}
             ):
            config = read_args()
        assert config["password"] == "env_secret"

    @pytest.mark.parametrize("env_password", ["", "   "])
    def test_env_var_empty_or_whitespace_fails(self, env_password):
        env = os.environ.copy()
        env["TERADATA_PASSWORD"] = env_password
        with mock.patch("sys.argv", BASE_ARGS), \
             mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(
                SystemExit, match="TERADATA_PASSWORD is empty"
            ):
                read_args()

    def test_env_var_ignored_when_password_file_set(self, tmp_path):
        pw_file = tmp_path / "pw.txt"
        pw_file.write_text("file_secret")
        args = BASE_ARGS + ["--password_file", str(pw_file)]
        with mock.patch("sys.argv", args), \
             mock.patch.dict(
                 os.environ, {"TERADATA_PASSWORD": "env_secret"}
             ):
            config = read_args()
        assert config["password"] == "file_secret"


class TestCliPassword:
    """Tests for --password CLI argument."""

    @pytest.mark.parametrize("empty_pw", ["", "   "])
    def test_cli_password_empty_or_whitespace_fails(self, empty_pw):
        args = BASE_ARGS + ["--password", empty_pw]
        env = os.environ.copy()
        env.pop("TERADATA_PASSWORD", None)
        with mock.patch("sys.argv", args), \
             mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(
                SystemExit, match="--password value is empty"
            ):
                read_args()

    def test_cli_password_prints_warning(self, capsys):
        args = BASE_ARGS + ["--password", "cli_secret"]
        env = os.environ.copy()
        env.pop("TERADATA_PASSWORD", None)
        with mock.patch("sys.argv", args), \
             mock.patch.dict(os.environ, env, clear=True):
            config = read_args()
        assert config["password"] == "cli_secret"
        stderr = capsys.readouterr().err
        assert "WARNING" in stderr
        assert "--password_secret" in stderr

    def test_cli_password_lower_priority_than_env(self, capsys):
        args = BASE_ARGS + ["--password", "cli_secret"]
        with mock.patch("sys.argv", args), \
             mock.patch.dict(
                 os.environ, {"TERADATA_PASSWORD": "env_secret"}
             ):
            config = read_args()
        # Env var (priority #3) beats --password (priority #4)
        assert config["password"] == "env_secret"
        # No warning because env var was used, not --password
        stderr = capsys.readouterr().err
        assert "WARNING" not in stderr


class TestPasswordSecretPrecedence:
    """--password_secret takes highest precedence."""

    def test_secret_overrides_all_others(self, tmp_path):
        pw_file = tmp_path / "pw.txt"
        pw_file.write_text("file_secret")
        args = BASE_ARGS + [
            "--password_secret",
            "projects/proj/secrets/mysecret",
            "--password_file", str(pw_file),
            "--password", "cli_secret",
        ]
        with mock.patch("sys.argv", args), \
             mock.patch.dict(
                 os.environ, {"TERADATA_PASSWORD": "env_secret"}
             ), \
             mock.patch(
                 "src.common.argument_validator.get_password",
                 return_value="gcp_secret",
             ), \
             mock.patch(
                 "src.common.argument_validator.checkDestination",
                 return_value=True,
             ):
            config = read_args()
        assert config["password"] == "gcp_secret"


class TestNoPasswordProvided:
    """When no password method is given, clear error is raised."""

    def test_no_password_lists_all_options(self):
        env = os.environ.copy()
        env.pop("TERADATA_PASSWORD", None)
        with mock.patch("sys.argv", BASE_ARGS), \
             mock.patch.dict(os.environ, env, clear=True):
            with pytest.raises(SystemExit) as exc_info:
                read_args()

        message = str(exc_info.value)
        assert "no password provided" in message
        assert "--password_secret" in message
        assert "--password_file" in message
        assert "TERADATA_PASSWORD" in message
