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

"""Non-Spark approach for building the entries."""
import dataclasses
import json
from typing import List
from typing import Dict
import re
import proto
from google.cloud import dataplex_v1
from src.constants import EntryType
from src import name_builder as nb

@dataclasses.dataclass(slots=True)
class ImportItem:
    """A template class for Import API."""

    entry: dataplex_v1.Entry = dataclasses.field(default_factory=dataplex_v1.Entry)
    aspect_keys: List[str] = dataclasses.field(default_factory=list)
    update_mask: List[str] = dataclasses.field(default_factory=list)

def _dict_factory(data: object):
    """Factory function required for converting Entry dataclass to dict."""

    def convert(obj: object):
        if isinstance(obj, proto.Message):
            return proto.Message.to_dict(obj)
        return obj

    return dict((k, convert(v)) for k, v in data)

def _to_camel_case(keyname_str: str) -> str:
    """Converts string to camelCase."""

    if not isinstance(keyname_str, str) or '_' not in keyname_str:
        return keyname_str # Return non-strings or already-camel-like strings as is
    
    # Filter so only converting known keys to prevent unexpected errors on aspect keys etc
    if keyname_str not in ["entry_type","aspect_key","aspect_type","source_type","parent_entry","fully_qualified_name","update_mask","aspect_keys"]:
        return keyname_str

    return re.sub(r"[-_]([a-zA-Z])", lambda x: x[1].upper(), keyname_str)


def _dict_factory_camcelCase(data: object):
    """
    return object with camelCase property names
    """

    def convert_recursive(obj: object):
        """Recursively converts objects."""
        if isinstance(obj, proto.Message):
            # Convert proto message to dict first
            native_dict = proto.Message.to_dict(obj)
            # Then recursively process the resulting dict
            return convert_recursive(native_dict)
        elif isinstance(obj, dict):
            # If it's a dict, convert keys to camelCase and recurse on values
            return {
                _to_camel_case(k): convert_recursive(v)
                for k, v in obj.items()
            }
        elif isinstance(obj, (list, tuple)):
            # If it's a list/tuple, recurse on elements
            # Ensure the same type (list/tuple) is returned
            return type(obj)(convert_recursive(item) for item in obj)
        else:
            # Base case: return other types (int, str, bool, etc.) as is
            return obj
    return {
        _to_camel_case(k): convert_recursive(v)
        for k, v in data
    }


def _create_entry(config: Dict[str, str], entry_type: EntryType):
    """Creates a Dataplex Entry."""
    entry = dataplex_v1.Entry()
    entry.name = nb.create_name(config, entry_type)
    
    entry.entry_type = entry_type.value.format(
        project=config["target_project_id"], location=config["target_location_id"])
    
    entry.fully_qualified_name = nb.create_fqn(config, entry_type)
    entry.parent_entry = nb.create_parent_name(config, entry_type)

    aspect_key = nb.create_entry_aspect_name(config, entry_type)

    # Add mandatory aspect
    entry_aspect = dataplex_v1.Aspect()
    entry_aspect.aspect_type = aspect_key
    entry_aspect.data = {}
    entry.aspects[aspect_key] = entry_aspect

    return entry


def _entry_to_import_item(entry: dataplex_v1.Entry):
    """Packs entry to import item, accepted by the API,"""
    import_item = ImportItem()
    import_item.entry = entry
    import_item.aspect_keys = list(entry.aspects.keys())
    import_item.update_mask = "aspects"

    return import_item

def create(config, entry_type: EntryType):
    """Creates a dataplex entry, packs it to Import Item and converts to json."""
    import_item = _entry_to_import_item(_create_entry(config, entry_type))
    camelCase_item = dataclasses.asdict(import_item, dict_factory=_dict_factory_camcelCase)
    return json.dumps(camelCase_item)