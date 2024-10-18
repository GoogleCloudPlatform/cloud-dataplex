"""The entrypoint of a pipeline."""
from typing import Dict

from src.constants import EntryType
from src import cmd_reader
from src import secret_manager
from src import entry_builder
from src import gcs_uploader
from src import top_entry_builder
from src.oracle_connector import OracleConnector


FILENAME = "output.jsonl"


def write_jsonl(output_file, json_strings):
    """Writes a list of string to the file in JSONL format."""

    # For simplicity, dataset is written into the one file. But it is not
    # mandatory, and the order doesn't matter for Import API.
    # The PySpark itself could dump entries into many smaller JSONL files.
    # Due to performance, it's recommended to dump to many smaller files.
    for string in json_strings:
        output_file.write(string + "\n")


def process_dataset(
    connector: OracleConnector,
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
    config["password"] = secret_manager.get_password(config["password_secret"])
    connector = OracleConnector(config)

    with open(FILENAME, "w", encoding="utf-8") as file:
        # Write top entries that don't require connection to the database
        file.writelines(top_entry_builder.create(config, EntryType.INSTANCE))
        file.writelines("\n")
        file.writelines(top_entry_builder.create(config, EntryType.DATABASE))

        # Get schemas, write them and collect to the list
        df_raw_schemas = connector.get_db_schemas()
        schemas = [schema.USERNAME for schema in df_raw_schemas.select("USERNAME").collect()]
        schemas_json = entry_builder.build_schemas(config, df_raw_schemas).toJSON().collect()

        write_jsonl(file, schemas_json)

        # Ingest tables and views for every schema in a list
        for schema in schemas:
            print(f"Processing tables for {schema}")
            tables_json = process_dataset(connector, config, schema, EntryType.TABLE)
            write_jsonl(file, tables_json)
            print(f"Processing views for {schema}")
            views_json = process_dataset(connector, config, schema, EntryType.VIEW)
            write_jsonl(file, views_json)

    gcs_uploader.upload(config, FILENAME)
