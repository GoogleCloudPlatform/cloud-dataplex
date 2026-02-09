import json
from pyspark.sql import SparkSession
from src.aws_glue_connector import AWSGlueConnector
from src.entry_builder import build_database_entry, build_dataset_entry
from src.gcs_uploader import GCSUploader
from src.secret_manager import SecretManager

def main():
    """
    Main function to run the AWS Glue to Dataplex metadata connector as a PySpark job.
    """
    # Initialize Spark Session
    spark = SparkSession.builder.appName("AWSGlueToDataplexConnector").getOrCreate()
    
    # Load configuration from a local file
    # In a real cluster environment, this might be passed differently
    with open('config.json', 'r') as f:
        config = json.load(f)

    print("Configuration loaded.")

    # Fetch AWS credentials from Secret Manager
    print("Fetching AWS credentials from GCP Secret Manager...")
    aws_access_key_id, aws_secret_access_key = SecretManager.get_aws_credentials(
        project_id=config["project_id"],
        secret_id=config["gcp_secret_id"]
    )
    print("Credentials fetched successfully.")

    # Initialize AWS Glue Connector
    glue_connector = AWSGlueConnector(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_region=config['aws_region']
    )

    # Fetch metadata and lineage
    print("Fetching metadata from AWS Glue...")
    metadata = glue_connector.get_databases()
    print(f"Found {len(metadata)} databases.")
    
    print("Fetching lineage info from AWS Glue jobs...")
    lineage_info = glue_connector.get_lineage_info()
    print(f"Found {len(lineage_info)} lineage relationships.")

    # Prepare entries for Dataplex
    dataplex_entries = []
    for db_name, tables in metadata.items():
        dataplex_entries.append(build_database_entry(config, db_name))
        for table in tables:
            dataplex_entries.append(build_dataset_entry(config, db_name, table, lineage_info))
    
    print(f"Prepared {len(dataplex_entries)} entries for Dataplex.")

    # Initialize GCSUploader
    gcs_uploader = GCSUploader(
        project_id=config['project_id'],
        bucket_name=config['gcs_bucket']
    )

    # Upload to GCS
    print(f"Uploading entries to GCS bucket: {config['gcs_bucket']}/{config['output_folder']}...")
    gcs_uploader.upload_entries(
        entries=dataplex_entries,
        aws_region=config['aws_region'],
        output_folder=config['output_folder']
    )
    print("Upload complete.")
    
    # Stop the Spark Session
    spark.stop()

if __name__ == '__main__':
    main()
