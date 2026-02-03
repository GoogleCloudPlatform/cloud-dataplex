# AWS Glue to Google Cloud Dataplex Connector

This connector extracts metadata from AWS Glue and transforms it into a format that can be imported into Google Cloud Dataplex. It captures database, table, and lineage information from AWS Glue and prepares it for ingestion into Dataplex, allowing you to catalog your AWS data assets within Google Cloud.

This connector is designed to be run from a Python virtual environment.

***

## Prerequisites

Before using this connector, you need to have the following set up:

1.  **AWS Credentials**: You will need an AWS access key ID and a secret access key with permissions to access AWS Glue.
2.  **Google Cloud Project**: A Google Cloud project is required to run the script and store the output.
3.  **GCP Secret Manager**: The AWS credentials must be stored in a secret in Google Cloud Secret Manager. The secret payload must be a **JSON object** with the following format:
    ```json
    {
      "access_key_id": "YOUR_AWS_ACCESS_KEY_ID",
      "secret_access_key": "YOUR_AWS_SECRET_ACCESS_KEY"
    }
    ```
4.  **Python 3** and **pip** installed.

***

## Configuration

The connector is configured using the `config.json` file. Ensure this file is present in the same directory as `main.py`. Here is a description of the parameters:

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
    Execute the `main.py` script. It will read settings from `config.json` in the current directory.
    ```bash
    python3 main.py
    ```

***

## Output

The connector generates a JSONL file in the specified GCS bucket and folder. This file contains the extracted metadata in a format that can be imported into Dataplex.

***

## Importing Metadata into Dataplex

Once the metadata file has been generated, you can import it into Dataplex using a metadata import job.

1.  **Prepare the Request File:**
    Open the `request.json` file and replace the following placeholders with your actual values:
    *   `<YOUR_GCS_BUCKET>`: The bucket where the output file was saved.
    *   `<YOUR_OUTPUT_FOLDER>`: The folder where the output file was saved.
    *   `<YOUR_PROJECT_ID>`: Your Google Cloud Project ID.
    *   `<YOUR_LOCATION>`: Your Google Cloud Location (e.g., `us-central1`).
    *   `<YOUR_ENTRY_GROUP_ID>`: The Dataplex Entry Group ID.

2.  **Run the Import Command:**
    Use the following `curl` command to initiate the import. Make sure to replace `{project-id}`, `{location}`, and `{job-id}` in the URL with your actual project ID, location, and a unique job ID.

    ```bash
    curl -X POST \
         -H "Authorization: Bearer $(gcloud auth print-access-token)" \
         -H "Content-Type: application/json; charset=utf-8" \
         -d @request.json \
         "https://dataplex.googleapis.com/v1/projects/{project-id}/locations/{location}/metadataJobs?metadataJobId={job-id}"
    ```

## Setup Resources

### Required Catalog Objects

Note before importing metadata, the Entry Group and all Entry Types and Aspect Types found in the metadata import file must exist in the target project and location. This connector requires the following Entry Group, Entry Types and Aspect Types:

| Catalog Object | IDs required by connector |
| :--- | :--- |
| **Entry Group** | Defined in `config.json` as `entry_group_id` |
| **Entry Types** | `aws-glue-database`&nbsp;&nbsp;`aws-glue-table`&nbsp;&nbsp;`aws-glue-view` |
| **Aspect Types** | `aws-glue-database`&nbsp;&nbsp;`aws-glue-table`&nbsp;&nbsp;`aws-glue-view`&nbsp;&nbsp;`aws-lineage-aspect` |

See [manage entries and create custom sources](https://cloud.google.com/dataplex/docs/ingest-custom-sources) for instructions on creating Entry Groups, Entry Types, and Aspect Types.

### Automated Setup
To run this connector, you must first create the required Dataplex resources. Run the provided script to create all resources automatically:
 
 ```bash
 # Set your project and location
 export PROJECT_ID=your-project-id
 export LOCATION=us-central1
 export ENTRY_GROUP_ID=aws-glue-entries
 
 # Run the setup script
 chmod +x scripts/setup_dataplex_resources.sh
 ./scripts/setup_dataplex_resources.sh
 ```
 
 ### Manual Setup & Schema Definitions
 
 If you prefer to create them manually, ensure you define the following:
 
 #### Entry Types
 *   `aws-glue-database`
 *   `aws-glue-table`
 *   `aws-glue-view`
 
 #### Aspect Types
 
 **1. `aws-lineage-aspect`**
 Used to store lineage relationships.
 
 *   **JSON Schema**:
     ```json
     {
       "type": "record",
       "recordFields": [
         {
           "name": "links",
           "type": "array",
           "index": 1,
           "arrayItems": {
             "type": "record",
             "recordFields": [
               {
                 "name": "source",
                 "type": "record",
                 "index": 1,
                 "recordFields": [
                   { "name": "fully_qualified_name", "type": "string", "index": 1 }
                 ]
               },
               {
                 "name": "target",
                 "type": "record",
                 "index": 2,
                 "recordFields": [
                   { "name": "fully_qualified_name", "type": "string", "index": 1 }
                 ]
               }
             ]
           }
         }
       ]
     }
     ```
 
 **2. Marker Aspects**
 *   `aws-glue-database`
 *   `aws-glue-table`
 *   `aws-glue-view`
 
 These aspects are used primarily for tagging. You can use a minimal schema:
 ```json
 {
   "type": "record",
   "recordFields": [
     {
       "name": "description",
       "type": "string",
       "index": 1,
       "constraints": { "required": false }
     }
   ]
 }
 ```
 
 See [manage entries and create custom sources](https://cloud.google.com/dataplex/docs/ingest-custom-sources) for more details.

## Metadata Extracted

The connector maps AWS Glue objects to Dataplex entries as follows:

| AWS Glue Object | Dataplex Entry Type | Schema Mapping |
| :--- | :--- | :--- |
| **Database** | `aws-glue-database` | N/A |
| **Table** | `aws-glue-table` | `int/bigint` -> `NUMBER`, `string` -> `STRING`, `array/struct` -> `BYTES` |
| **View** | `aws-glue-view` | Parsed SQL used to generate Lineage from source tables |
| **Partition Keys** | N/A | Included as columns in the `schema` aspect |

### Lineage
The connector also parses AWS Glue Job scripts (Python/Scala) to extract lineage:
-   **Source**: `DataSource` nodes in Glue Job graph.
-   **Target**: `DataSink` nodes in Glue Job graph.
-   **Result**: Lineage is visualized in Dataplex from Source Table -> Target Table.

***



## AWS Credentials

This connector requires an IAM User with `GlueConsoleFullAccess` (or read-only equivalent) and `S3ReadOnly` (to download job scripts for lineage).

1.  Create an IAM User in AWS Console.
2.  Attach policies: `AWSGlueConsoleFullAccess`, `AmazonS3ReadOnlyAccess`.
3.  Generate an **Access Key ID** and **Secret Access Key**.
4.  Store these in GCP Secret Manager as a JSON object:
    ```json
    {"access_key_id": "...", "secret_access_key": "..."}
    ```

***

## Docker Setup

You can containerize this connector to run on Cloud Run, Dataproc, or Kubernetes.

1.  **Build the Image**:
    ```bash
    docker build -t aws-glue-connector:latest .
    ```

2.  **Run Locally** (passing config):
    Ensure `config.json` is in the current directory or mounted.
    ```bash
    docker run -v $(pwd)/config.json:/app/config.json -v $(pwd)/src:/app/src aws-glue-connector:latest
    ```

3.  **Push to GCR/Artifact Registry**:
    ```bash
    gcloud auth configure-docker
    docker tag aws-glue-connector:latest gcr.io/YOUR_PROJECT/aws-glue-connector:latest
    docker push gcr.io/YOUR_PROJECT/aws-glue-connector:latest
    ```
