# SQL Server Connector

This connector extracts metadata from SQL Server to Google Cloud Dataplex Catalog.

### Target objects and schemas:

The connector extracts metadata for the following database objects:
* Tables
* Views

## Parameters
The SQL Server connector takes the following parameters:
|Parameter|Description|Required/Optional|
|---------|------------|-------------|
|target_project_id|GCP Project ID or number which will be used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_location_id|GCP region ID which will be used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_entry_group_id|The Dataplex Entry Group ID to be used in the metadata|REQUIRED|
|host|SQL Server server to connect to|REQUIRED|
|port|SQL Server host port (usually 1443)|REQUIRED|
|instancename|The SQL Server instance to connect to. If not provided the default instance will be used|OPTIONAL
|database|The SQL Server database name|REQUIRED|
|logintimeout|0-60 Allowed timeout in seconds to establish connection to SQL Server|OPTIONAL
|encrypt|True/False Encrypt connection to database|OPTIONAL
|trustservercertificate|True/False SQL Server TLS certificate or not|OPTIONAL
|hostnameincertificate|domain of host certificate|OPTIONAL
|user|Username to connect with|REQUIRED|
|password-secret|GCP Secret Manager ID holding the password for the user. Format: projects/PROJ/secrets/SECRET|REQUIRED|
|output_bucket|GCS bucket where the output file will be stored|REQUIRED|
|output_folder|Folder in the GCS bucket where the export output file will be stored|OPTIONAL|

## Prepare your SQL Server environment:

Best practise is to connect to the database using a dedicated user with the minimum privileges required to extract metadata. 

1. Create a user in SQL Server with at minmum the following privileges:
    * CONNECT to database
    * SELECT on sys.columns
    * SELECT on sys.tables
    * SELECT on sys.types

2. Add the password for the user to the Google Cloud Secret Manager in your project and note the Secret ID (format is: projects/[project-number]/secrets/[secret-name])

## Running the connector from the command line

The metadata connector can be run directly from the command line for development or testing.

### Prepare the environment:
1. Download the mssql-jdbc-12.10.0.jre11.jar file [from Microsoft](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server?view=sql-server-2022) to the sql-server-connector directory.
    * If a different version jar file is used, add the --jar parameter to specify the name of the jar file 
2. Ensure a Java Runtime Environment (JRE) is installed in your environment
3. Create a Python virtual environment to isolate the connector dependencies.
    Run the following in your home directory
    ```
    pip install virtualenv
    python -m venv myvenv
    source venv/bin/activate
    ```
    See [here](https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/) for more details about virtual environments.

4. Install PySpark
    ```bash
    pip3 install pyspark
    ```
5. Install all remaining dependencies from the requirements.txt file 
    ```bash
    pip3 install -r requirements.txt
    ```
6. Ensure you have a clear network path from the machine where you will run the script to the target database server

### Required IAM Roles
- roles/secretmanager.secretAccessor
- roles/storage.objectUser

System 

Before you run the script ensure you session is authenticated as a user which has the above roles at minimum. For example, using 
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

### Output:
The connector generates a metadata import file in JSONL format as described [in the Dataplex documentation](https://cloud.google.com/dataplex/docs/import-metadata#metadata-import-file). 
The file will be created in the 'output' directory and also uploaded to the GCS bucket and folder specified in --output_bucket and --output_folder
A sample output from the SQL Server connector can be found [here](sample/sqlserver_output_sample.jsonl)

## Build a container and extract metadata using Dataproc Serverless

Building a Docker container for the connector allows it to be run from a variety of container services including [Dataproc serverless](https://cloud.google.com/dataproc-serverless/docs)

### Building the container (one-time task)

Ensure you have Docker installed in your environment before you begin and that the user you run the script with has artifactregistry.repositories.uploadArtifacts privilege on the artfiact registry in your project.

1. Edit [build_and_push_docker.sh](build_and_push_docker.sh) and set the PROJECT_ID AND REGION_ID
2. Make the script executable and run
    ```bash
    chmod a+x build_and_push_docker.sh
    ./build_and_push_docker.sh
    ``` 
    This will build a Docker container called **dataplex-sqlserver-pyspark** and store it in Artifact Registry. 
    This process can take take up to 10 minutes.

### Submitting a metadata extraction job to Dataproc serverless:
Once the container is built you can run the metadata extract with the following command (substituting appropriate values for your environment). 

First please please ensure you have:
1. Uploaded the JDBC jar file to a Google Cloud Storage bucket. Use the path to it in the **--jars** parameter below.
2. Also create a Cloud Storage bucket which will be used for Dataproc Serverless as a working directory, use it in the **--deps-bucket** parameter below.
3. The service account you run the job with using **--service-account** below must have the IAM roles described [here](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source#required-roles).
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
      --database AdventureWorksDW2019 \
      --output_bucket gs://dataplex_connectivity_imports \
      --output_folder sqlserver
```

### 3. Schedule end-to-end metadata extraction and import using Google Cloud Workflows

To run an end-to-end metadata extraction and import process, run the container via Google Cloud Workflows. 

Follow the Dataplex documentation here: [Import metadata from a custom source using Workflows](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source) and use [this yaml file](https://github.com/GoogleCloudPlatform/cloud-dataplex/blob/main/managed-connectivity/cloud-workflows/byo-connector/templates/byo-connector.yaml) as a template.


## Manually running a metadata import into Dataplex

To import a metadata import file into Dataplex, see the [Dataplex documetation](https://cloud.google.com/dataplex/docs/import-metadata#import-metadata) for full instructions about calling the API.
The [samples](/samples) directory contains an examples metadata import file and request file for callng the API