# Oracle Connector

This connector extracts metadata from Snowflake to Google Cloud Dataplex Catalog.

### Target objects and schemas:

The connector extracts metadata for the following database objects:
* Tables
* Views

### Parameters
The Snowflake connector takes the following parameters:
|Parameter|Description|Required/Optional|
|---------|------------|-------------|
|target_project_id|GCP Project ID, or 'global'. Used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_location_id|GCP Region ID, or 'global'. Used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_entry_group_id|Dataplex Entry Group ID to use in generated metadata|REQUIRED|
|account|Snowflake account to connect to|REQUIRED|
|user|Snowflake username to connect with|REQUIRED|
|authentication|Authentication method: password or oauth (default is 'password')|OPTIONAL
|password_secret|GCP Secret Manager ID holding the password for the Snowflake user. Format: projects/[PROJ]/secrets/[SECRET]|REQUIRED if using password auth|
|token|Token for oauth authentication.|REQUIRED if using oauth authentication|
|database|Snowflake database to connect to|REQUIRED|
|warehouse|Snowflake warehouse to connect to|OPTIONAL|
|output_bucket|GCS bucket where the output file will be stored|REQUIRED|
|output_folder|Folder in the GCS bucket where the export output file will be stored|OPTIONAL|

### Prepare your Snowflake environment:

Best practise is to connect to the database using a dedicated user with the minimum privileges required to extract metadata. 
 The user for connecting should be granted a Security Role with the following privileges for the database and schemas, tables, views, materialized views for which metadata needs to be extracted:
```sql
grant usage on warehouse <warehouse_name> to role <role_name>;
grant usage on database <database_name> to role <role_name>;
grant usage on all schemas in database <database_name> to role <role_name>;
grant references on all tables in schema <schema_name> to role <role_name>;
grant references on all views in schema <schema_name> to role <role_name>;
grant references on all materialized views in schema <schema_name> to role <role_name>;
```

2. Add the password for the snowflake user to the Google Cloud Secret Manager in your project and note the Secret ID (format is: projects/[project-number]/secrets/[secret-name])

## Running the connector
There are three ways to run the connector:
1) [Run the script directly from the command line](###running-from-the-command-line) (extract metadata to GCS only)
2) [Run as a container via a Dataproc Serverless job](###build-a-container-and-extract-metadata-with-a-dataproc-serverless-job) (extract metadata to GCS only)
3) [Schedule and run as a container via Workflows](###schedule-end-to-end-metadata-extraction-and-import-using-google-cloud-workflows) (End-to-end. Extracts metadata into GCS and imports into Dataplex)

### Running from the command line

The metadata connector can be run ad-hoc from the command line for development or testing by directly executing the [main.py](main.py) script.

#### Prepare the environment:

1. Download the following jars [from Maven](https://repo1.maven.org/maven2/net/snowflake/)
    * [snowflake-jdbc-3.19.0.jar](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.19.0/)
    * [spark-snowflake_2.12-3.1.1.jar](https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/3.1.1/)
2. Edit the SPARK_JAR_PATH variable in [snowflake_connector.py](src/connection_jar.py) to match the location of the jar files
3. Ensure a Java Runtime Environment (JRE) is installed in your environment
4. If you don't have one set up already, create a Python virtual environment to isolate the connector.
    See [here](https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/) for more details but the TL;DR instructions are to run the following in your home directory:
```
pip install virtualenv
python -m venv myvenv
source myvenv/bin/activate
```
5. Install PySpark in your local environment
```
pip3 install pyspark==3.5.3
```
6. Install all remaining dependencies for the connector 
```
pip3 install -r requirements.txt
```
7. Ensure you have a clear network path from the machine where you will run the script to the target database server

Before you run the script ensure you session is authenticated as a user which has these roles at minimum 
- roles/secretmanager.secretAccessor
- roles/storage.objectUser

You can authenticate as a user which has these roles using gcloud, or give them to a Service Account.

```
gcloud auth application-default login
```

### Run the connector
To execute metadata extraction run the following command, substituting appropriate values for your environment:

```shell 
python3 main.py \
--target_project_id my-gcp-project \
--target_location_id us-central1 \
--target_entry_group_id snowflake \
--account RXXXXXA-GX00020 \
--user dataplex_snowflake_user \
--password-secret projects/499965349999/secrets/snowflake \
--database my_snowflake_database \
--warehouse COMPUTE_WH \
--output_bucket my-gcs-bucket
--output_folder snowflake
```

#### Output:
The connector generates a metadata extract in JSONL format as described [in the documentation](https://cloud.google.com/dataplex/docs/import-metadata#metadata-import-file) and stores it in the GCS output bucket and folder defined above. 
A copy of the generated file will also be saved in the "output" folder in the local directory.

A sample output from the Snowflake connector can be found [here](sample/)

To import a metadata file run the Dataplex Import API as explained below in "Manually running a metadata import into Dataplex"

### Build a container and extract metadata with a Dataproc Serverless job:

To build a Docker container for the connector (one-time task) and run the extraction process as a Dataproc Serverless job:

#### Build the container

Ensure the user you run the script with has /artifactregistry.repositories.uploadArtifacts on the artficate registry in your project 

1. Ensure you are authenticated to your Google Cloud account by running ```gcloud auth login```
2. Run the following the make the script executable
```
chmod a+x build_and_push_docker.sh
``` 

3. Edit [build_and_push_docker.sh](/build_and_push_docker.sh) and set PROJECT_ID and REGION_ID to the appropriate values for your project
4. Build the Docker container and store it in Artifact Registry 
```
./build_and_push_docker.sh
```
This process can take several minutes.

#### Submitting a metadata extraction job to Dataproc serverless:
Once the container is built you can run the metadata extract with the following command (substituting appropriate values for your environment). 

#### Required IAM Roles|Parameter|Description|Required/Optional|
The service account you submit for the job using **--service-account** below needs to have the following IAM roles:

- roles/dataplex.catalogEditor
- roles/dataplex.entryGroupOwner
- roles/dataplex.metadataJobOwner
- roles/dataproc.admin
- roles/dataproc.editor
- roles/dataproc.worker
- roles/iam.serviceAccountUser
- roles/logging.logWriter
- roles/secretmanager.secretAccessor
- roles/workflows.invoker

You can use this [script](../common_scripts/grant_SA_dataproc_roles.sh) to grant the roles to your service account

```shell
gcloud dataproc batches submit pyspark \
    --project=my-gcp-project-id \
    --region=us-central1 \
    --batch=0001 \
    --deps-bucket=dataplex-metadata-collection-bucket \  
    --container-image=us-central1-docker.pkg.dev/my-gcp-project-id/docker-repo/snowflake-pyspark@sha256:dab02ca02f60a9e12769999191b06d859b947d89490d636a34fc734d4a0b6d08 \
    --service-account=440199992669-compute@developer.gserviceaccount.com \
    --network=Your-Network-Name \
    main.py \
    --target_project_id my-gcp-project \
    --target_location_id us-central1 \
    --target_entry_group_id snowflake \
    --account RXXXXXA-GX00020 \
    --user snowflakeuser \
    --password-secret projects/499965349999/secrets/snowflake \
    --database SNOWFLAKE_SAMPLE_DATA \
    --output_bucket my-gcs-bucket
```

See the documentatrion for [gcloud dataproc batches submit pyspark](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark) for more information

### Schedule end-to-end metadata extraction and import using Google Cloud Workflows

To run an end-to-end metadata extraction and import process, run the container via Google Cloud Workflows. 

Follow the Dataplex documentation here: [Import metadata from a custom source using Workflows ](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source)


## Importing a metadata file into Dataplex

To import metadata import files into Dataplex Catalog use the Import API as detailed in the [Dataplex documetation](https://cloud.google.com/dataplex/docs/import-metadata#import-metadata).