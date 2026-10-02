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

"""Tests for GCS uploader with mocked storage client."""

from unittest import mock
from unittest.mock import MagicMock

import pytest

from src.common.gcs_uploader import upload, checkDestination


class TestUpload:
    """Tests for upload()."""

    @mock.patch("src.common.gcs_uploader.storage")
    def test_upload_calls_gcs(self, mock_storage):
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.get_bucket.return_value = mock_bucket
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        config = {"output_bucket": "my-bucket"}
        upload(config, "/tmp/output", "metadata.jsonl", "run1")

        mock_client.get_bucket.assert_called_once_with("my-bucket")
        mock_bucket.blob.assert_called_once_with("run1/metadata.jsonl")
        mock_blob.upload_from_filename.assert_called_once_with(
            "/tmp/output/metadata.jsonl"
        )


class TestCheckDestination:
    """Tests for checkDestination()."""

    @mock.patch("src.common.gcs_uploader.storage")
    def test_valid_bucket(self, mock_storage):
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_bucket.exists.return_value = True
        mock_client.bucket.return_value = mock_bucket

        assert checkDestination("my-bucket") is True

    @mock.patch("src.common.gcs_uploader.storage")
    def test_bucket_not_exists(self, mock_storage):
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_bucket = MagicMock()
        mock_bucket.exists.return_value = False
        mock_client.bucket.return_value = mock_bucket

        with pytest.raises(Exception, match="does not exist"):
            checkDestination("bad-bucket")

    @mock.patch("src.common.gcs_uploader.storage")
    def test_gs_prefix_rejected(self, mock_storage):
        with pytest.raises(Exception, match="without gs://"):
            checkDestination("gs://my-bucket")
