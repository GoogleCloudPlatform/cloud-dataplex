"""The entrypoint of a pipeline."""
from typing import Dict
import os
import importlib
from src import cmd_reader
from src.constants import EntryType
from src.constants import SOURCE_TYPE
from src.constants import DB_OBJECT_TYPES_TO_PROCESS
from src.constants import TOP_ENTRY_HIERARCHY
from src.constants import generateFileName
from src.constants import CONNECTOR_MODULE
from src.constants import CONNECTOR_CLASS
from src.common import entry_builder
from src.common import gcs_uploader
from src.common import top_entry_builder
from src.common.ExternalSourceConnector import IExternalSourceConnector
from src.common.util import generateFolderName

def write_jsonl(output_file, json_strings):
    """Writes a list of string to the file in JSONL format."""
    for string in json_strings:
        output_file.write(string + "\n")

def process_dataset(
    connector: IExternalSourceConnector,
    config: Dict[str, str],
    schema_name: str,
    entry_type: EntryType,
):
    """Builds dataset and converts it to jsonl."""
    df_raw = connector.get_dataset(schema_name, entry_type)
    df = entry_builder.build_dataset(config, df_raw, schema_name, entry_type)
    return df.toJSON().collect()

def run():
    """Runs a pipeline."""
    config = cmd_reader.read_args()

    print(f"\nExtracting metadata from {SOURCE_TYPE}")

    if config['local_output_only']:
        print("File will be generated in local 'output' directory only")
    
    # Build output file name from connection details
    FILENAME = generateFileName(config)
    FOLDERNAME = ''
    if not config['local_output_only']:
        FOLDERNAME = generateFolderName(SOURCE_TYPE)

    # Instantiate connector class 
    ConnectorClass = getattr(importlib.import_module(CONNECTOR_MODULE), CONNECTOR_CLASS)
    connector = ConnectorClass(config)
    
    entries_count = 0

    # Build the output file name from connection details
    FILENAME = generateFileName(config) 

    output_path = './output'
    if not os.path.exists(output_path):
        os.mkdir(output_path)

    with open(f"{output_path}/{FILENAME}", "w", encoding="utf-8") as file:
        # First write the top level entry types to file which can be generated without processing the schemas
        for entry in TOP_ENTRY_HIERARCHY:
            file.writelines(top_entry_builder.create(config, entry))
            file.writelines("\n")

        # Collect list of schemas for extract
        df_raw_schemas = connector.get_db_schemas()
        schemas = [schema.SCHEMA_NAME for schema in df_raw_schemas.select("SCHEMA_NAME").collect()]
        schemas_json = entry_builder.build_schemas(config, df_raw_schemas).toJSON().collect()

        write_jsonl(file, schemas_json)

        # Collect metadata for target db objects in each schema
        for schema in schemas:
            for object_type in DB_OBJECT_TYPES_TO_PROCESS:
                print(f"Processing {object_type.name}S for {schema}:  ",end='')
                objects_json = process_dataset(connector, config, schema, object_type)
                print(f"{len(objects_json)}")
                entries_count += len(objects_json)
                write_jsonl(file, objects_json)

    print(f"{entries_count} rows written to file {FILENAME}") 

    # If 'min_expected_entries set, file must meet minimum number of expected entries
    if entries_count < config['min_expected_entries']:
        print(f"Row count is less then min_expected_entries value of {config['min_expected_entries']}. Will not upload to GCS bucket.")
    elif not config['local_output_only']:
        print(f"Uploading to GCS bucket: {config['output_bucket']}/{FOLDERNAME}")
        gcs_uploader.upload(config,output_path,FILENAME,FOLDERNAME)

