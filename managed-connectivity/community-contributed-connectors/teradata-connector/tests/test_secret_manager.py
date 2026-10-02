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

"""Tests for Secret Manager password retrieval."""

from unittest import mock
from unittest.mock import MagicMock

from src.common.secret_manager import get_password


class TestGetPassword:
    """Tests for get_password()."""

    @mock.patch("src.common.secret_manager.secretmanager")
    def test_appends_versions_latest(self, mock_sm):
        mock_client = MagicMock()
        mock_sm.SecretManagerServiceClient.return_value = mock_client
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "my_secret"
        mock_client.access_secret_version.return_value = mock_response

        result = get_password("projects/proj/secrets/mysecret")

        mock_client.access_secret_version.assert_called_once_with(
            request={
                "name": "projects/proj/secrets/mysecret/versions/latest"
            }
        )
        assert result == "my_secret"

    @mock.patch("src.common.secret_manager.secretmanager")
    def test_preserves_explicit_version(self, mock_sm):
        mock_client = MagicMock()
        mock_sm.SecretManagerServiceClient.return_value = mock_client
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "versioned"
        mock_client.access_secret_version.return_value = mock_response

        path = "projects/proj/secrets/mysecret/versions/3"
        result = get_password(path)

        mock_client.access_secret_version.assert_called_once_with(
            request={"name": path}
        )
        assert result == "versioned"

    @mock.patch("src.common.secret_manager.secretmanager")
    def test_decodes_utf8(self, mock_sm):
        mock_client = MagicMock()
        mock_sm.SecretManagerServiceClient.return_value = mock_client
        mock_response = MagicMock()
        mock_response.payload.data.decode.return_value = "p@ss"
        mock_client.access_secret_version.return_value = mock_response

        result = get_password("projects/p/secrets/s")
        mock_response.payload.data.decode.assert_called_with("UTF-8")
        assert result == "p@ss"
