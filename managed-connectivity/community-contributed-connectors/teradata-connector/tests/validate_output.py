# Copyright 2025 Google LLC
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

"""Validate a JSONL output file against Dataplex import API requirements."""

import json
import sys
from pathlib import Path

VALID_METADATA_TYPES = {
    "NUMBER", "STRING", "BYTES", "TIMESTAMP", "DATETIME", "BOOLEAN", "OTHER"
}
VALID_MODES = {"NULLABLE", "REQUIRED"}


def validate_import_item(item, line_num):
    """Validate a single import item. Returns list of errors."""
    errors = []
    prefix = f"Line {line_num}"

    # 1. Top-level keys
    for key in ("entry", "aspectKeys", "updateMask"):
        if key not in item:
            errors.append(f"{prefix}: missing top-level key '{key}'")

    if "entry" not in item:
        return errors

    entry = item["entry"]
    if not isinstance(entry, dict):
        errors.append(f"{prefix}: 'entry' must be a JSON object")
        return errors

    # 2. updateMask must be ["aspects"]
    if item.get("updateMask") != ["aspects"]:
        errors.append(
            f"{prefix}: updateMask should be ['aspects'], "
            f"got {item.get('updateMask')}"
        )

    # 3. aspectKeys must be a non-empty list
    aspect_keys = item.get("aspectKeys", [])
    if not isinstance(aspect_keys, list) or len(aspect_keys) == 0:
        errors.append(f"{prefix}: aspectKeys must be a non-empty list")

    # 4. Required entry fields
    for field in (
        "name", "entryType", "fullyQualifiedName", "aspects", "entrySource"
    ):
        if field not in entry:
            errors.append(f"{prefix}: entry missing '{field}'")

    # 5. No snake_case keys (must be camelCase)
    snake_case_keys = {
        "entry_type", "fully_qualified_name", "parent_entry",
        "entry_source", "aspect_keys", "update_mask",
        "display_name", "aspect_type", "data_type", "metadata_type",
        "default_value",
    }
    for key in entry:
        if key in snake_case_keys:
            errors.append(f"{prefix}: snake_case key '{key}' found in entry")

    # 6. entrySource validation
    entry_source = entry.get("entrySource", {})
    if "displayName" not in entry_source:
        errors.append(f"{prefix}: entrySource missing 'displayName'")
    if entry_source.get("system") != "teradata":
        errors.append(
            f"{prefix}: entrySource.system should be 'teradata', "
            f"got '{entry_source.get('system')}'"
        )

    # 7. Aspects structure
    aspects = entry.get("aspects", {})
    if not isinstance(aspects, dict):
        errors.append(f"{prefix}: aspects must be an object (dictionary)")
        aspects = {}

    for key, aspect in aspects.items():
        if not isinstance(aspect, dict):
            errors.append(
                f"{prefix}: aspect '{key}' must be an object (dictionary)"
            )
            continue
        if "aspectType" not in aspect:
            errors.append(f"{prefix}: aspect '{key}' missing 'aspectType'")
        elif aspect["aspectType"] != key:
            errors.append(
                f"{prefix}: aspect key '{key}' != "
                f"aspectType '{aspect['aspectType']}'"
            )
        if "data" not in aspect:
            errors.append(f"{prefix}: aspect '{key}' missing 'data'")

    # 8. aspectKeys must match aspects keys
    aspect_key_set = set(aspect_keys)
    actual_keys = set(aspects.keys())
    if aspect_key_set != actual_keys:
        errors.append(
            f"{prefix}: aspectKeys {aspect_key_set} != "
            f"aspects keys {actual_keys}"
        )

    # 9. Name pattern
    name = entry.get("name", "")
    if not name.startswith("projects/"):
        errors.append(f"{prefix}: name doesn't start with 'projects/'")
    if "/entryGroups/" not in name:
        errors.append(f"{prefix}: name missing '/entryGroups/'")
    if "/entries/" not in name:
        errors.append(f"{prefix}: name missing '/entries/'")

    # 10. Schema aspect validation (tables and views)
    schema_key = "dataplex-types.global.schema"
    if schema_key in aspects:
        schema_data = aspects[schema_key].get("data", {})
        fields = schema_data.get("fields", [])
        if not fields:
            errors.append(f"{prefix}: schema has no fields")
        for i, field in enumerate(fields):
            fp = f"{prefix}, field[{i}]"
            if "name" not in field:
                errors.append(f"{fp}: missing 'name'")
            if "mode" not in field:
                errors.append(f"{fp}: missing 'mode'")
            elif field["mode"] not in VALID_MODES:
                errors.append(
                    f"{fp}: invalid mode '{field['mode']}'"
                )
            if "dataType" not in field:
                errors.append(f"{fp}: missing 'dataType'")
            if "metadataType" not in field:
                errors.append(f"{fp}: missing 'metadataType'")
            elif field["metadataType"] not in VALID_METADATA_TYPES:
                errors.append(
                    f"{fp}: invalid metadataType "
                    f"'{field['metadataType']}'"
                )

    return errors


def validate_hierarchy(items):
    """Check parent-child relationships are consistent.

    Args:
        items: list of (line_num, parsed_item) tuples.
    """
    errors = []
    # Collect names only from items that have a valid entry with a name.
    entry_names = set()
    for _line_num, item in items:
        entry = item.get("entry")
        if isinstance(entry, dict) and entry.get("name"):
            entry_names.add(entry["name"])

    for line_num, item in items:
        entry = item.get("entry")
        if not isinstance(entry, dict):
            continue
        parent = entry.get("parentEntry", "")
        if parent and parent not in entry_names:
            errors.append(
                f"Line {line_num}: parentEntry '{parent}' "
                f"not found in any entry name"
            )
    return errors


def main():
    if len(sys.argv) < 2:
        print("Usage: python validate_output.py <path-to-jsonl>")
        sys.exit(1)

    filepath = Path(sys.argv[1])
    if not filepath.exists():
        print(f"File not found: {filepath}")
        sys.exit(1)

    items = []
    all_errors = []
    stats = {
        "total": 0, "instance": 0, "database": 0,
        "schema": 0, "table": 0, "view": 0,
        "fields_total": 0,
    }

    with open(filepath, encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue

            try:
                item = json.loads(line)
            except json.JSONDecodeError as e:
                all_errors.append(f"Line {line_num}: invalid JSON: {e}")
                continue

            items.append((line_num, item))
            stats["total"] += 1

            # Classify entry type
            entry_type = item.get("entry", {}).get("entryType", "")
            for t in ("instance", "database", "schema", "table", "view"):
                if entry_type.endswith(f"teradata-{t}"):
                    stats[t] += 1
                    break

            # Count fields
            aspects = item.get("entry", {}).get("aspects", {})
            schema_aspect = aspects.get("dataplex-types.global.schema", {})
            fields = schema_aspect.get("data", {}).get("fields", [])
            stats["fields_total"] += len(fields)

            errors = validate_import_item(item, line_num)
            all_errors.extend(errors)

    # Validate hierarchy
    all_errors.extend(validate_hierarchy(items))

    # Report
    print(f"=== Dataplex Import Validation Report ===")
    print(f"File: {filepath}")
    print(f"Total entries: {stats['total']}")
    print(f"  Instances:  {stats['instance']}")
    print(f"  Databases:  {stats['database']}")
    print(f"  Schemas:    {stats['schema']}")
    print(f"  Tables:     {stats['table']}")
    print(f"  Views:      {stats['view']}")
    print(f"  Total fields (columns): {stats['fields_total']}")
    print()

    if all_errors:
        print(f"FAILED: {len(all_errors)} error(s) found:")
        for err in all_errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("PASSED: All entries are Dataplex-compatible.")
        sys.exit(0)


if __name__ == "__main__":
    main()
