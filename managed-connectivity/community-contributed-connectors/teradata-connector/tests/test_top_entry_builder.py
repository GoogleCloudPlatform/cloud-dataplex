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

"""Tests for top_entry_builder output format."""

import json
import pytest
from src.constants import EntryType
from src.common.top_entry_builder import create

CONFIG = {
    "target_project_id": "my-project",
    "target_location_id": "us-central1",
    "target_entry_group_id": "teradata",
    "host": "td-server.example.com",
}


class TestImportItemFormat:
    """Verify output matches Dataplex import API requirements."""

    @pytest.fixture(params=[EntryType.INSTANCE, EntryType.DATABASE])
    def import_item(self, request):
        json_str = create(CONFIG, request.param)
        return json.loads(json_str)

    def test_top_level_keys(self, import_item):
        assert "entry" in import_item
        assert "aspectKeys" in import_item
        assert "updateMask" in import_item

    def test_update_mask_is_array(self, import_item):
        """Docs require update_mask to be ArrayType(StringType())."""
        assert isinstance(import_item["updateMask"], list)
        assert import_item["updateMask"] == ["aspects"]

    def test_aspect_keys_is_array(self, import_item):
        assert isinstance(import_item["aspectKeys"], list)
        assert len(import_item["aspectKeys"]) > 0

    def test_entry_has_required_fields(self, import_item):
        entry = import_item["entry"]
        assert "name" in entry
        assert "entryType" in entry
        assert "fullyQualifiedName" in entry
        assert "aspects" in entry
        # parentEntry may be empty string for instance
        assert "parentEntry" in entry

    def test_entry_has_entry_source(self, import_item):
        """Docs require entrySource on all entries."""
        entry = import_item["entry"]
        assert "entrySource" in entry
        es = entry["entrySource"]
        assert "displayName" in es
        assert "system" in es
        assert es["system"] == "teradata"

    def test_aspects_have_correct_structure(self, import_item):
        aspects = import_item["entry"]["aspects"]
        for key, aspect in aspects.items():
            assert "aspectType" in aspect
            assert "data" in aspect
            assert aspect["aspectType"] == key

    def test_camel_case_keys(self, import_item):
        """Ensure all keys are camelCase, not snake_case."""
        entry = import_item["entry"]
        assert "entry_type" not in entry
        assert "entryType" in entry
        assert "fully_qualified_name" not in entry
        assert "fullyQualifiedName" in entry
        assert "parent_entry" not in entry
        assert "parentEntry" in entry
        assert "entry_source" not in entry
        assert "entrySource" in entry


class TestInstanceEntry:
    def test_instance_fqn(self):
        item = json.loads(create(CONFIG, EntryType.INSTANCE))
        assert item["entry"]["fullyQualifiedName"] == (
            "custom:`td-server-example-com`"
        )

    def test_instance_parent_empty(self):
        item = json.loads(create(CONFIG, EntryType.INSTANCE))
        assert item["entry"]["parentEntry"] == ""

    def test_instance_entry_type(self):
        item = json.loads(create(CONFIG, EntryType.INSTANCE))
        assert item["entry"]["entryType"].endswith(
            "teradata-instance"
        )


class TestDatabaseEntry:
    def test_database_parent_is_instance(self):
        item = json.loads(create(CONFIG, EntryType.DATABASE))
        assert "/entries/td-server.example.com" in (
            item["entry"]["parentEntry"]
        )
        assert "/databases/" not in item["entry"]["parentEntry"]

    def test_database_name_contains_databases(self):
        item = json.loads(create(CONFIG, EntryType.DATABASE))
        assert "/databases/" in item["entry"]["name"]
