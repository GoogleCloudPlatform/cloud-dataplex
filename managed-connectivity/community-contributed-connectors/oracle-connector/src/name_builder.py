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

"""Builds Dataplex hierarchy identifiers."""
from typing import Dict
from src.constants import EntryType
from src.constants import SOURCE_TYPE
from src.constants import DB_OBJECT_TYPES_TO_PROCESS

# Oracle cluster users start with C## prefix, but Dataplex doesn't accept #.
# In that case in names chang to C!!, escape with backticks in FQNs
FORBIDDEN_SYMBOL = "#"
ALLOWED_SYMBOL = "!"

# Allow for using SID or Service name to connect
def get_database(config: Dict[str, str]):
 if config['sid'] and len(config['sid']) > 0:
     return config['sid']
 else:
     return config['service']

def create_fqn(config: Dict[str, str], entry_type: EntryType,
               schema_name: str = "", table_name: str = ""):
    """Creates a fully qualified name or Dataplex v1 hierarchy name."""
    if FORBIDDEN_SYMBOL in schema_name:
        schema_name = f"`{schema_name}`"

    if entry_type == EntryType.INSTANCE:
        # Requires backticks to escape column
        return f"{SOURCE_TYPE}:`{config['host']}`"
    if entry_type == EntryType.DATABASE:
        instance = create_fqn(config, EntryType.INSTANCE)
        return f"{instance}.{config['service']}"
    if entry_type == EntryType.DB_SCHEMA:
        database = create_fqn(config, EntryType.DATABASE)
        return f"{database}.{schema_name}"
    if entry_type in [EntryType.TABLE, EntryType.VIEW]:
        database = create_fqn(config, EntryType.DATABASE)
        return f"{database}.{schema_name}.{table_name}"
    return ""


def create_name(config: Dict[str, str], entry_type: EntryType,
                schema_name: str = "", table_name: str = ""):
    """Creates a Dataplex v2 hierarchy name."""
    if FORBIDDEN_SYMBOL in schema_name:
        schema_name = schema_name.replace(FORBIDDEN_SYMBOL, ALLOWED_SYMBOL)
    if entry_type == EntryType.INSTANCE:
        name_prefix = (
            f"projects/{config['target_project_id']}/"
            f"locations/{config['target_location_id']}/"
            f"entryGroups/{config['target_entry_group_id']}/"
            f"entries/"
        )
        return name_prefix + config["host"].replace(":", "@")
    if entry_type == EntryType.DATABASE:
        instance = create_name(config, EntryType.INSTANCE)
        return f"{instance}/databases/{get_database(config)}"
    if entry_type == EntryType.DB_SCHEMA:
        database = create_name(config, EntryType.DATABASE)
        return f"{database}/database_schemas/{schema_name}"
    if entry_type == EntryType.TABLE:
        db_schema = create_name(config, EntryType.DB_SCHEMA, schema_name)
        return f"{db_schema}/tables/{table_name}"
    if entry_type == EntryType.VIEW:
        db_schema = create_name(config, EntryType.DB_SCHEMA, schema_name)
        return f"{db_schema}/views/{table_name}"
    return ""


def create_parent_name(config: Dict[str, str], entry_type: EntryType,
                       parent_name: str = ""):
    """Generates a Dataplex v2 name of the parent."""
    if entry_type == EntryType.DATABASE:
        return create_name(config, EntryType.INSTANCE)
    if entry_type == EntryType.DB_SCHEMA:
        return create_name(config, EntryType.DATABASE)
    if entry_type in DB_OBJECT_TYPES_TO_PROCESS:
        return create_name(config, EntryType.DB_SCHEMA, parent_name)
    return ""


def create_entry_aspect_name(config: Dict[str, str], entry_type: EntryType):
    """Generates an entry aspect name."""
    last_segment = entry_type.value.split("/")[-1]
    return f"{config['target_project_id']}.{config['target_location_id']}.{last_segment}"
