import re
from src.constants import *
import src.name_builder as nb

def choose_metadata_type(data_type: str):
    """Choose the metadata type based on AWS Glue native type."""
    data_type = data_type.lower()
    if data_type in ['integer', 'int', 'smallint', 'tinyint', 'bigint', 'long', 'float', 'double', 'decimal']:
        return "NUMBER"
    if 'char' in data_type or 'string' in data_type:
        return "STRING"
    if data_type in ['binary', 'array', 'struct', 'map']:
        return "BYTES"
    if data_type == 'timestamp':
        return "TIMESTAMP"
    if data_type == 'date':
        return "DATE"
    return "OTHER"

def build_database_entry(config, db_name):
    """Builds a database entry, mimicking the successful Oracle format."""
    entry_type = EntryType.DATABASE
    full_entry_type = entry_type.value.format(
        project=config["project_id"],
        location=config["location_id"])

    aspects = {
        DATABASE_ASPECT_KEY: {
            "aspect_type": DATABASE_ASPECT_KEY,
            "data": {}
        }
    }

    entry = {
        "name": nb.create_name(config, entry_type, db_name),
        "fully_qualified_name": nb.create_fqn(config, entry_type, db_name),
        "entry_type": full_entry_type,
        "aspects": aspects
    }
    return {
        "entry": entry,
        "aspect_keys": [DATABASE_ASPECT_KEY],
        "update_mask": "aspects"
    }

def build_dataset_entry(config, db_name, table_info, job_lineage):
    """Builds a table or view entry, mimicking the successful Oracle format."""
    table_name = table_info['Name']
    table_type = table_info.get('TableType')

    entry_type = EntryType.VIEW if table_type == 'VIRTUAL_VIEW' else EntryType.TABLE

    # --- Build Schema Aspect ---
    columns = []
    if 'StorageDescriptor' in table_info and 'Columns' in table_info['StorageDescriptor']:
        for col in table_info['StorageDescriptor']['Columns']:
            columns.append({
                "name": col.get("Name"),
                "dataType": col.get("Type"),
                "mode": "NULLABLE",
                "metadataType": choose_metadata_type(col.get("Type", ""))
            })

    aspects = {
        SCHEMA_ASPECT_KEY: {
            "aspect_type": SCHEMA_ASPECT_PATH,
            "data": { "fields": columns }
        }
    }
    aspect_keys = [SCHEMA_ASPECT_KEY]

    # --- Add Custom Marker Aspect ---
    if entry_type == EntryType.TABLE:
        aspects[TABLE_ASPECT_KEY] = {"aspect_type": TABLE_ASPECT_KEY, "data": {}}
        aspect_keys.append(TABLE_ASPECT_KEY)
    elif entry_type == EntryType.VIEW:
        aspects[VIEW_ASPECT_KEY] = {"aspect_type": VIEW_ASPECT_KEY, "data": {}}
        aspect_keys.append(VIEW_ASPECT_KEY)

    # --- Build Lineage Aspect ---
    source_assets = []
    if entry_type == EntryType.VIEW and 'ViewOriginalText' in table_info:
        sql = table_info['ViewOriginalText']
        source_tables = re.findall(r'(?:FROM|JOIN)\s+`?(\w+)`?', sql, re.IGNORECASE)
        source_assets.extend(set(source_tables))

    if table_name in job_lineage:
        source_assets.extend(job_lineage[table_name])

    if source_assets:
        full_lineage_aspect_path = LINEAGE_ASPECT_PATH.format(
            project=config["project_id"],
            location=config["location_id"]
        )
        lineage_aspect = {
            LINEAGE_ASPECT_KEY: {
                "aspect_type": full_lineage_aspect_path,
                "data": {
                    "links": [{
                        "source": { "fully_qualified_name": nb.create_fqn(config, EntryType.TABLE, db_name, src) },
                        "target": { "fully_qualified_name": nb.create_fqn(config, entry_type, db_name, table_name) }
                    } for src in set(source_assets)]
                }
            }
        }
        aspects.update(lineage_aspect)
        aspect_keys.append(LINEAGE_ASPECT_KEY)

    # --- Build General Entry Info ---
    full_entry_type = entry_type.value.format(
        project=config["project_id"],
        location=config["location_id"])
    parent_name = nb.create_parent_name(config, entry_type, db_name)

    entry = {
        "name": nb.create_name(config, entry_type, db_name, table_name),
        "fully_qualified_name": nb.create_fqn(config, entry_type, db_name, table_name),
        "parent_entry": parent_name,
        "entry_type": full_entry_type,
        "entry_source": { "display_name": table_name, "system": SOURCE_TYPE },
        "aspects": aspects
    }

    return {
        "entry": entry,
        "aspect_keys": list(set(aspect_keys)),
        "update_mask": "aspects"
    }