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

"""Builds Dataplex hierarchy identifiers for Teradata."""

from typing import Dict

from src.constants import EntryType, DB_OBJECT_TYPES_TO_PROCESS

# Dataplex FQN system prefix. Only a fixed set of prefixes are recognized
# (oracle, mysql, postgresql, sqlserver, custom). Use 'custom' for Teradata.
FQN_PREFIX = "custom"

# Characters allowed in a single Dataplex entry ID segment.
# '/' is excluded — it is the hierarchy separator in resource paths
# and must not appear within an individual segment.
_ALLOWED_ENTRY_ID_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "-._~%!$&'()*+,;=@"
)


def encode_non_ascii(text: str) -> str:
    """Replace non-ASCII characters with _u<codepoint>_ format.

    Each character with ord > 127 becomes _u<HEX>_ using its Unicode
    code point (e.g. 测 -> _u6D4B_, 🎉 -> _u1F389_). ASCII characters
    are returned unchanged. Returns None if input is None.
    """
    if text is None:
        return text
    if all(ord(ch) <= 127 for ch in text):
        return text
    return "".join(
        f"_u{ord(ch):04X}_" if ord(ch) > 127 else ch
        for ch in text
    )


def _sanitize_entry_id(segment: str) -> str:
    """Replace characters not allowed in Dataplex entry IDs.

    Non-ASCII characters (Chinese, etc.) are converted to
    _u<codepoint>_ format (e.g. _u6D4B_, _u1F389_ for emoji).
    Other invalid characters (including '/') are replaced with
    underscores.
    """
    encoded = encode_non_ascii(segment) or ""
    return "".join(
        ch if ch in _ALLOWED_ENTRY_ID_CHARS else "_"
        for ch in encoded
    )


def _sanitize_fqn_segment(segment: str) -> str:
    """Replace dots with hyphens so Dataplex FQN parser doesn't split on them."""
    return segment.replace(".", "-")


def create_fqn(
    config: Dict[str, str],
    entry_type: EntryType,
    schema_name: str = "",
    table_name: str = "",
) -> str:
    """Creates a fully qualified name."""
    host = _sanitize_fqn_segment(config["host"])

    if entry_type == EntryType.INSTANCE:
        return f"{FQN_PREFIX}:`{host}`"

    if entry_type == EntryType.DATABASE:
        instance = create_fqn(config, EntryType.INSTANCE)
        return f"{instance}.{host}"

    if entry_type == EntryType.DB_SCHEMA:
        database = create_fqn(config, EntryType.DATABASE)
        return f"{database}.{schema_name}"

    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        database = create_fqn(config, EntryType.DATABASE)
        return f"{database}.{schema_name}.{table_name}"

    return ""


def create_name(
    config: Dict[str, str],
    entry_type: EntryType,
    schema_name: str = "",
    table_name: str = "",
) -> str:
    """Creates a Dataplex v2 hierarchy name (resource path)."""
    if entry_type == EntryType.INSTANCE:
        name_prefix = (
            f"projects/{config['target_project_id']}/"
            f"locations/{config['target_location_id']}/"
            f"entryGroups/{config['target_entry_group_id']}/"
            f"entries/"
        )
        return name_prefix + _sanitize_entry_id(config["host"].replace(":", "@"))

    if entry_type == EntryType.DATABASE:
        instance = create_name(config, EntryType.INSTANCE)
        return f"{instance}/databases/{_sanitize_entry_id(config['host'])}"

    if entry_type == EntryType.DB_SCHEMA:
        database = create_name(config, EntryType.DATABASE)
        return f"{database}/database_schemas/{_sanitize_entry_id(schema_name)}"

    if entry_type == EntryType.TABLE:
        db_schema = create_name(
            config, EntryType.DB_SCHEMA, schema_name
        )
        return f"{db_schema}/tables/{_sanitize_entry_id(table_name)}"

    if entry_type == EntryType.VIEW:
        db_schema = create_name(
            config, EntryType.DB_SCHEMA, schema_name
        )
        return f"{db_schema}/views/{_sanitize_entry_id(table_name)}"

    return ""


def create_parent_name(
    config: Dict[str, str],
    entry_type: EntryType,
    parent_name: str = "",
) -> str:
    """Generates a Dataplex v2 name of the parent."""
    if entry_type == EntryType.DATABASE:
        return create_name(config, EntryType.INSTANCE)

    if entry_type == EntryType.DB_SCHEMA:
        return create_name(config, EntryType.DATABASE)

    if entry_type in DB_OBJECT_TYPES_TO_PROCESS:
        return create_name(config, EntryType.DB_SCHEMA, parent_name)

    return ""


def create_entry_aspect_name(
    config: Dict[str, str], entry_type: EntryType
) -> str:
    """Generates an entry aspect name."""
    last_segment = entry_type.value.split("/")[-1]
    return (
        f"{config['target_project_id']}"
        f".{config['target_location_id']}"
        f".{last_segment}"
    )
