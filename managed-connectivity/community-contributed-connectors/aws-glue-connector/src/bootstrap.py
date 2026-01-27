import json
from src.aws_glue_connector import AWSGlueConnector
from src.entry_builder import build_database_entry, build_dataset_entry
from src.gcs_uploader import GCSUploader
from src.secret_manager import SecretManager

def run():
    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)

    # Fetch AWS credentials from Secret Manager
    aws_access_key_id, aws_secret_access_key = SecretManager.get_aws_credentials(
        project_id=config["project_id"],
        secret_id=config["gcp_secret_id"]
    )

    # Initialize AWS Glue Connector
    glue_connector = AWSGlueConnector(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=config['aws_region']
    )

    # Fetch metadata and lineage
    metadata = glue_connector.get_databases()
    lineage_info = glue_connector.get_lineage_info()

    # Prepare entries for Dataplex
    dataplex_entries = []
    for db_name, tables in metadata.items():
        dataplex_entries.append(build_database_entry(config, db_name))
        for table in tables:
            dataplex_entries.append(build_dataset_entry(config, db_name, table, lineage_info))

    # Initialize GCSUploader
    gcs_uploader = GCSUploader(
        project_id=config['project_id'],
        bucket_name=config['gcs_bucket']
    )

    # Upload to GCS using the correct method
    gcs_uploader.upload_entries(
        entries=dataplex_entries,
        aws_region=config['aws_region'],
        output_folder=config['output_folder']
    )
    print(f"Successfully uploaded entries to GCS bucket: {config['gcs_bucket']}/{config['output_folder']}")

if __name__ == '__main__':
    run()