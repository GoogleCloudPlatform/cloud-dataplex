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
from typing import List, Dict

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


def _create_entry(config: Dict[str, str], entry_type: EntryType):
    """Creates an entry based on a Dataplex library."""
    entry = dataplex_v1.Entry()
    entry.name = nb.create_name(config, entry_type)
    entry.entry_type = entry_type.value.format(
        project=config["target_project_id"], location=config["target_location_id"]
    )
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
    """Creates an entry, packs it to Import Item and converts to json."""
    import_item = _entry_to_import_item(_create_entry(config, entry_type))
    return json.dumps(dataclasses.asdict(import_item, dict_factory=_dict_factory))
