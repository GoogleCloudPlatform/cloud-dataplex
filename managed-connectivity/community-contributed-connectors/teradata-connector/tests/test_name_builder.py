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

"""Tests for Teradata name builder."""

import pytest
from src.constants import EntryType
from src.name_builder import (
    create_fqn,
    create_name,
    create_parent_name,
    create_entry_aspect_name,
    _sanitize_entry_id,
)

CONFIG = {
    "target_project_id": "my-project",
    "target_location_id": "us-central1",
    "target_entry_group_id": "teradata",
    "host": "td-server.example.com",
}

PREFIX = (
    "projects/my-project/locations/us-central1"
    "/entryGroups/teradata/entries"
)


class TestCreateFqn:
    def test_instance(self):
        assert create_fqn(CONFIG, EntryType.INSTANCE) == (
            "custom:`td-server-example-com`"
        )

    def test_database(self):
        assert create_fqn(CONFIG, EntryType.DATABASE) == (
            "custom:`td-server-example-com`.td-server-example-com"
        )

    def test_db_schema(self):
        assert create_fqn(
            CONFIG, EntryType.DB_SCHEMA, schema_name="retail"
        ) == (
            "custom:`td-server-example-com`"
            ".td-server-example-com.retail"
        )

    def test_table(self):
        assert create_fqn(
            CONFIG, EntryType.TABLE,
            schema_name="retail", table_name="orders"
        ) == (
            "custom:`td-server-example-com`"
            ".td-server-example-com.retail.orders"
        )

    def test_view(self):
        assert create_fqn(
            CONFIG, EntryType.VIEW,
            schema_name="retail", table_name="v_orders"
        ) == (
            "custom:`td-server-example-com`"
            ".td-server-example-com.retail.v_orders"
        )


class TestCreateName:
    def test_instance(self):
        assert create_name(CONFIG, EntryType.INSTANCE) == (
            f"{PREFIX}/td-server.example.com"
        )

    def test_database(self):
        assert create_name(CONFIG, EntryType.DATABASE) == (
            f"{PREFIX}/td-server.example.com"
            "/databases/td-server.example.com"
        )

    def test_db_schema(self):
        assert create_name(
            CONFIG, EntryType.DB_SCHEMA, schema_name="retail"
        ) == (
            f"{PREFIX}/td-server.example.com"
            "/databases/td-server.example.com"
            "/database_schemas/retail"
        )

    def test_table(self):
        assert create_name(
            CONFIG, EntryType.TABLE,
            schema_name="retail", table_name="orders"
        ) == (
            f"{PREFIX}/td-server.example.com"
            "/databases/td-server.example.com"
            "/database_schemas/retail/tables/orders"
        )

    def test_view(self):
        assert create_name(
            CONFIG, EntryType.VIEW,
            schema_name="retail", table_name="v_orders"
        ) == (
            f"{PREFIX}/td-server.example.com"
            "/databases/td-server.example.com"
            "/database_schemas/retail/views/v_orders"
        )

    def test_host_colon_replaced(self):
        config = {**CONFIG, "host": "server:1025"}
        name = create_name(config, EntryType.INSTANCE)
        assert "server@1025" in name
        assert ":" not in name.split("/entries/")[1]

    def test_host_colon_sanitized_in_database(self):
        """DATABASE segment must not contain ':' from host:port."""
        config = {**CONFIG, "host": "server:1025"}
        db_name = create_name(config, EntryType.DATABASE)
        db_segment = db_name.split("/databases/")[1]
        assert ":" not in db_segment
        assert "server_1025" in db_segment

    def test_chinese_table_name_sanitized(self):
        """Non-ASCII table names must be converted to _u<codepoint>_ format."""
        name = create_name(
            CONFIG, EntryType.TABLE,
            schema_name="retail", table_name="测试中文"
        )
        assert "测试中文" not in name
        assert "_u6D4B_" in name  # 测 = U+6D4B

    def test_chinese_schema_name_sanitized(self):
        """Non-ASCII schema names must be converted to _u<codepoint>_ format."""
        name = create_name(
            CONFIG, EntryType.DB_SCHEMA,
            schema_name="数据库"
        )
        assert "数据库" not in name
        assert "_u" in name

    def test_space_in_table_name_replaced(self):
        """Spaces replaced with underscores."""
        name = create_name(
            CONFIG, EntryType.TABLE,
            schema_name="retail", table_name="my table"
        )
        assert "my_table" in name

    def test_normal_table_name_unchanged(self):
        """Normal ASCII names should pass through unchanged."""
        name = create_name(
            CONFIG, EntryType.TABLE,
            schema_name="retail", table_name="orders_2024"
        )
        assert name.endswith("/tables/orders_2024")

    def test_special_chars_in_table_name(self):
        """Curly braces and other special chars replaced with underscores."""
        name = create_name(
            CONFIG, EntryType.TABLE,
            schema_name="retail", table_name="test{table}"
        )
        assert "{" not in name
        assert "}" not in name


class TestSanitizeEntryId:
    """Tests for the _sanitize_entry_id helper."""

    def test_ascii_unchanged(self):
        assert _sanitize_entry_id("orders") == "orders"

    def test_dots_preserved(self):
        assert _sanitize_entry_id("my.table") == "my.table"

    def test_hyphens_preserved(self):
        assert _sanitize_entry_id("my-table") == "my-table"

    def test_underscores_preserved(self):
        assert _sanitize_entry_id("my_table") == "my_table"

    def test_chinese_to_unicode_format(self):
        """Chinese chars become _u<codepoint>_ code points."""
        result = _sanitize_entry_id("测试")
        assert result == "_u6D4B__u8BD5_"
        assert "测" not in result

    def test_space_replaced(self):
        assert _sanitize_entry_id("my table") == "my_table"

    def test_curly_braces_replaced(self):
        result = _sanitize_entry_id("test{1}")
        assert "{" not in result
        assert "}" not in result

    def test_hash_replaced(self):
        result = _sanitize_entry_id("test#1")
        assert "#" not in result

    def test_allowed_special_chars_preserved(self):
        """Chars in Dataplex allowed set should pass through."""
        assert _sanitize_entry_id("a-b.c_d") == "a-b.c_d"
        assert _sanitize_entry_id("a~b!c") == "a~b!c"
        assert _sanitize_entry_id("a+b=c") == "a+b=c"
        assert _sanitize_entry_id("%") == "%"
        assert _sanitize_entry_id("a%b-c") == "a%b-c"

    def test_emoji_supplementary_plane(self):
        """Supplementary plane chars (>U+FFFF) use 5+ hex digits."""
        result = _sanitize_entry_id("test_🎉")
        assert result == "test__u1F389_"
        assert "🎉" not in result


class TestCreateParentName:
    def test_instance_has_no_parent(self):
        assert create_parent_name(CONFIG, EntryType.INSTANCE) == ""

    def test_database_parent_is_instance(self):
        parent = create_parent_name(CONFIG, EntryType.DATABASE)
        assert parent == create_name(CONFIG, EntryType.INSTANCE)

    def test_schema_parent_is_database(self):
        parent = create_parent_name(CONFIG, EntryType.DB_SCHEMA)
        assert parent == create_name(CONFIG, EntryType.DATABASE)

    def test_table_parent_is_schema(self):
        parent = create_parent_name(
            CONFIG, EntryType.TABLE, parent_name="retail"
        )
        assert parent == create_name(
            CONFIG, EntryType.DB_SCHEMA, schema_name="retail"
        )

    def test_view_parent_is_schema(self):
        parent = create_parent_name(
            CONFIG, EntryType.VIEW, parent_name="retail"
        )
        assert parent == create_name(
            CONFIG, EntryType.DB_SCHEMA, schema_name="retail"
        )


class TestCreateEntryAspectName:
    def test_instance_aspect(self):
        assert create_entry_aspect_name(CONFIG, EntryType.INSTANCE) == (
            "my-project.us-central1.teradata-instance"
        )

    def test_table_aspect(self):
        assert create_entry_aspect_name(CONFIG, EntryType.TABLE) == (
            "my-project.us-central1.teradata-table"
        )

    def test_schema_aspect(self):
        assert create_entry_aspect_name(CONFIG, EntryType.DB_SCHEMA) == (
            "my-project.us-central1.teradata-schema"
        )
