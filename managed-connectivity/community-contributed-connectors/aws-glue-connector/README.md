# AWS Glue to Google Cloud Dataplex Connector

This connector extracts metadata from AWS Glue and transforms it into a format that can be imported into Google Cloud Dataplex. It captures database, table, and lineage information from AWS Glue and prepares it for ingestion into Dataplex, allowing you to catalog your AWS data assets within Google Cloud.

This connector is designed to be run from a Python virtual environment.

***

## Prerequisites

Before using this connector, you need to have the following set up:

1.  **AWS Credentials**: You will need an AWS access key ID and a secret access key with permissions to access AWS Glue.
2.  **Google Cloud Project**: A Google Cloud project is required to run the script and store the output.
3.  **GCP Secret Manager**: The AWS credentials must be stored in a secret in Google Cloud Secret Manager. The secret should be a single string with the access key ID and secret access key, separated by a comma (e.g., `"your_access_key_id,your_secret_access_key"`).
4.  **Python 3** and **pip** installed.

***

## Configuration

The connector is configured using the `config.json` file. Here is a description of the parameters:

| Parameter | Description |
| :--- | :--- |
| **`aws_region`** | The AWS region where your Glue Data Catalog is located (e.g., "eu-north-1"). |
| **`project_id`** | Your Google Cloud Project ID. |
| **`location_id`** | The Google Cloud region where you want to run the script (e.g., "us-central1"). |
| **`entry_group_id`** | The Dataplex entry group ID where the metadata will be imported. |
| **`gcs_bucket`** | The Google Cloud Storage bucket where the output metadata file will be stored. |
| **`aws_account_id`** | Your AWS account ID. |
| **`output_folder`** | The folder within the GCS bucket where the output file will be stored. |
| **`gcp_secret_id`** | The ID of the secret in GCP Secret Manager that contains your AWS credentials. |

***

## Running the Connector

You can run the connector from your local machine using a Python virtual environment.

### Setup and Execution

1.  **Create a virtual environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```
2.  **Install the required dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Run the connector:**
    Execute the `main.py` script and provide the path to your configuration file.
    ```bash
    python3 main.py --config=config.json
    ```

***

## Output

The connector generates a JSONL file in the specified GCS bucket and folder. This file contains the extracted metadata in a format that can be imported into Dataplex.

***

## Importing Metadata into Dataplex

Once the metadata file has been generated, you can import it into Dataplex using a metadata import job. You can use the provided `request.json` file as a template for the import request.

The `request.json` file specifies the source GCS URI for the metadata, the entry group to import into, and other settings for the import job.

To initiate the import, use the following `curl` command. Make sure to replace the placeholders `{project-id}`, `{location}`, and `{job-id}` with your actual project ID, location, and a unique job ID.

```bash
curl -X POST \
     -H "Authorization: Bearer $(gcloud auth print-access-token)" \
     -H "Content-Type: application/json; charset=utf-8" \
     -d @request.json \
     "[https://dataplex.googleapis.com/v1/projects/](https://dataplex.googleapis.com/v1/projects/){project-id}/locations/{location}/metadataJobs?metadataJobId={job-id}"