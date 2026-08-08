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

"""Tests for argument_validator with mocked GCP dependencies."""

import argparse
from unittest import mock
from unittest.mock import MagicMock

import pytest

from src.common.argument_validator import (
    validateArguments,
    validateSecretID,
    checkOptionProvided,
    true_or_false,
)


def _make_args(**kwargs):
    """Create a namespace with default valid args."""
    defaults = {
        "local_output_only": True,
        "output_bucket": None,
        "output_folder": None,
        "target_location_id": "us-central1",
        "password_secret": None,
    }
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


class TestValidateArguments:
    """Tests for validateArguments()."""

    @mock.patch(
        "src.common.argument_validator.get_password",
        return_value="secret123",
    )
    def test_password_secret_resolved(self, mock_pw):
        args = _make_args(
            password_secret="projects/proj/secrets/mysecret"
        )
        result = validateArguments(args)
        assert result.password == "secret123"

    def test_output_bucket_required_when_not_local(self):
        args = _make_args(
            local_output_only=False,
            output_bucket=None,
            output_folder=None,
        )
        with pytest.raises(Exception, match="output_bucket"):
            validateArguments(args)

    @mock.patch("src.common.argument_validator.checkDestination")
    def test_invalid_bucket_rejected(self, mock_check):
        mock_check.return_value = False
        args = _make_args(
            local_output_only=False,
            output_bucket="bad-bucket",
            output_folder="folder",
        )
        with pytest.raises(Exception, match="not valid"):
            validateArguments(args)

    def test_invalid_region_rejected(self):
        args = _make_args(target_location_id="invalid-region")
        with pytest.raises(Exception, match="target_location_id"):
            validateArguments(args)

    @mock.patch("src.common.argument_validator.checkDestination")
    def test_valid_region_accepted(self, mock_check):
        mock_check.return_value = True
        args = _make_args(
            local_output_only=False,
            output_bucket="good-bucket",
            output_folder="folder",
            target_location_id="us-east1",
        )
        result = validateArguments(args)
        assert result.target_location_id == "us-east1"

    @mock.patch("src.common.argument_validator.checkDestination")
    def test_global_region_accepted(self, mock_check):
        mock_check.return_value = True
        args = _make_args(
            local_output_only=False,
            output_bucket="bucket",
            output_folder="folder",
            target_location_id="global",
        )
        result = validateArguments(args)
        assert result.target_location_id == "global"


class TestValidateSecretID:
    """Tests for validateSecretID()."""

    def test_valid_secret_id(self):
        assert validateSecretID(
            "projects/my-project/secrets/my-secret"
        ) is True

    def test_invalid_secret_id_missing_projects(self):
        with pytest.raises(Exception, match="not a valid Secret ID"):
            validateSecretID("secrets/my-secret")

    def test_invalid_secret_id_extra_slash(self):
        with pytest.raises(Exception, match="not a valid Secret ID"):
            validateSecretID(
                "projects/proj/secrets/mysecret/versions/1"
            )


class TestCheckOptionProvided:
    """Tests for checkOptionProvided()."""

    def test_option_present(self):
        args = argparse.Namespace(foo="bar", baz=None)
        assert checkOptionProvided(args, ["foo"]) is True

    def test_option_missing(self):
        args = argparse.Namespace(foo=None)
        assert checkOptionProvided(args, ["foo"]) is False

    def test_option_not_in_namespace(self):
        args = argparse.Namespace(foo="bar")
        assert checkOptionProvided(args, ["missing"]) is False


class TestTrueOrFalse:
    """Tests for true_or_false()."""

    @pytest.mark.parametrize("val", ["true", "True", "TRUE", "T", "t"])
    def test_true_values(self, val):
        assert true_or_false(val) is True

    @pytest.mark.parametrize("val", ["false", "False", "FALSE", "F", "f"])
    def test_false_values(self, val):
        assert true_or_false(val) is False
