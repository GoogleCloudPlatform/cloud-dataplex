# SQL Server Connector

This connector extracts metadata from SQL Server for Google Cloud Dataplex Catalog.

### Target objects and schemas:

The connector extracts metadata for the following database objects:
* Tables
* Views

## Parameters
The SQL Server connector takes the following parameters:
|Parameter|Value|Description|Required/Optional|
|---------|--|------------|-------------|
|target_project_id||Google Cloud Project ID or number. Used in all generated dataplex entries|REQUIRED|
|target_location_id||GCP region ID. Used in the all generated dataplex entries|REQUIRED|
|target_entry_group_id||Dataplex Catalog Entry Group ID for all generated dataplex entries|REQUIRED|
|host||SQL Server server to connect to|REQUIRED|
|port||SQL Server host port (usually 1443)|REQUIRED|
|instancename||SQL Server instance to connect to. If not provided default instance will be used|OPTIONAL
|database||SQL Server database name|REQUIRED|
|user||Username to connect with|REQUIRED|
|password_secret||Google Cloud Secret Manager ID of the user password. Format: projects/PROJ/secrets/SECRET|REQUIRED|
|login_timeout|0-60|Allowed wait time to establish a connection to SQL Server (seconds)|OPTIONAL
|jar||Path to JDBC jar file. Default is mssql-jdbc-12.10.0.jre11.jar|OPTIONAL 
|local_output_only||Generate metadata file in local directory only, do not send to Cloud Storage bucket|OPTIONAL|
|output_bucket||GCS bucket where generated metadata file will be stored|REQUIRED|
|output_folder||Folder in GCS bucket where the metadata file will be stored|OPTIONAL|

## Preparing your SQL Server environment:

Best practise is to connect to the database using a dedicated user with the minimum privileges required to extract metadata. 

1. Create a user in SQL Server with at minmum the following privileges:
    * CONNECT to database
    * SELECT on sys.columns
    * SELECT on sys.tables
    * SELECT on sys.types

2. Add the password for the user to the Google Cloud Secret Manager in your project and note the Secret ID (format is: projects/[project-number]/secrets/[secret-name])

## Running the connector from the command line

The metadata connector can be run directly from a linux command line for development or testing.

### Preparing the environment:
1. Download the mssql-jdbc-12.10.0.jre11.jar file from the [Microsoft Github Repository](https://github.com/microsoft/mssql-jdbc/releases/tag/v12.10.0) and save it to the sql-server-connector directory.
    * If a different version jar file is used, add the --jar parameter and specify the path and name of the jar file 
2. Ensure a Java Runtime Environment (JRE) is installed in your environment
3. Create a Python virtual environment to isolate the connector dependencies
    Run the following in your home directory
    ```
    pip install virtualenv
    python -m venv myvenv
    source venv/bin/activate
    ```
    See [here](https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/) for more details about virtual environments.

4. Install PySpark
    ```bash
    pip install pyspark
    ```
5. Install all remaining dependencies from the requirements.txt file 
    ```bash
    pip install -r requirements.txt
    ```
6. Ensure you have a clear network path from the machine where you will run the script to the target database server

### Required IAM Roles

Before you run the script ensure you session is authenticated as a user which has the above roles at minimum. 

- roles/secretmanager.secretAccessor
- roles/storage.objectUser

For example, you can use [Google Cloud SDK](https://cloud.google.com/sdk) gcloud command to authenticate as an appropriate user:
```bash
gcloud auth application-default login
```

### Run the connector
To execute the metadata extraction run the following command (substituting appropriate values for your environment):

```shell 
python3 main.py \
--target_project_id my-gcp-project-id \
--target_location_id us-central1 \
--target_entry_group_id sqlserver \
--host the-sqlserver-server \
--port 1433 \
--database dbtoextractfrom \
--user dataplexagent \
--password-secret projects/73813454526/secrets/dataplexagent_sqlserver \
--output_bucket dataplex_connectivity_imports \
--output_folder sqlserver
```

### Metadata Output:
The connector will extract metadata about database objects and generate a metadata import file in JSONL format as described in the [Dataplex documentation](https://cloud.google.com/dataplex/docs/import-metadata#metadata-import-file). 
This file will be created in the 'output' directory and also uploaded to the GCS bucket and folder specified in --output_bucket and --output_folder
A sample output from the SQL Server connector can be found [here](sample/sqlserver_output_sample.jsonl)

## Build a container and extract metadata using Dataproc Serverless

Building a Docker container for the connector allows it to be run from a variety of container services including [Dataproc serverless](https://cloud.google.com/dataproc-serverless/docs)

### Building the container (one-time task)

Ensure you have Docker installed in your environment before you begin. The user you run the script with must have the artifactregistry.repositories.uploadArtifacts IAM privilege on the artifact registry in your project.

1. Edit [build_and_push_docker.sh](build_and_push_docker.sh) and set the PROJECT_ID AND REGION_ID
2. Make the script executable and the run:
    ```bash
    chmod a+x build_and_push_docker.sh
    ./build_and_push_docker.sh
    ``` 
    The process will create a Docker container called **dataplex-sqlserver-pyspark** and store it in Artifact Registry. 
    This can take take up to 10 minutes.

### Submitting a metadata extraction job to Dataproc serverless:
Once the container is built you can run metadata extracts with as Dataproc Serverless batch.

### Preparing the environment

1. Upload the JDBC jar file to a Google Cloud Storage bucket. Use the path to it in the **--jars** parameter below.
2. Create a second Cloud Storage bucket which will be used for Dataproc Serverless as a working directory. Use the url to this in the **--deps-bucket** parameter below.
3. The service account which runs the job ( **--service-account** ) must have the IAM roles described [here](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source#required-roles).
You can use this [script](../common_scripts/grant_SA_dataproc_roles.sh) to grant the required roles to your service account.

```shell
gcloud dataproc batches submit pyspark \
    --project=my-gcp-project-id \
    --region=us-central1 \
    --batch=0001 \
    --deps-bucket=dataplex-metadata-collection-usc1 \  
    --container-image=us-central1-docker.pkg.dev/my-gcp-project-id/docker-repo/sqlserver-pyspark@sha256:dab02ca02f60a9e12767996191b06d859b947d89490d636a34fc734d4a0b6d08 \
    --service-account=440165342669-compute@developer.gserviceaccount.com \
    --jars=gs://path/to/mssql-jdbc-9.4.1.jre8.jar  \
    --network=Your-Network-Name \
    main.py \
--  --target_project_id my-gcp-project-id \
      --target_location_id us-central1	\
      --target_entry_group_id XXX \
      --host the-sqlserver-server \
      --port 1433 \
      --user dataplexagent \
      --password-secret projects/73813454526/dataplexagent_sqlserver \
      --database dbtoextractfrom \
      --output_bucket gs://dataplex_connectivity_imports \
      --output_folder sqlserver
```

### Schedule end-to-end metadata extraction and import using Google Cloud Workflows

To run an end-to-end metadata extraction and import process, run the container via Google Cloud Workflows. 

Follow the Dataplex documentation here: [Import metadata from a custom source using Workflows](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source) and use [this yaml file](https://github.com/GoogleCloudPlatform/cloud-dataplex/blob/main/managed-connectivity/cloud-workflows/byo-connector/templates/byo-connector.yaml) as a template.


## Manually running a metadata import into Dataplex

To import a metadata import file into Dataplex, see the [Dataplex documetation](https://cloud.google.com/dataplex/docs/import-metadata#import-metadata) for full instructions about calling the API.
The [samples](/samples) directory contains an examples metadata import file and request file for callng the API