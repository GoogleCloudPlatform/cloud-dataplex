# Oracle Connector

This connector extracts metadata from Oracle databases to Google Cloud Dataplex Catalog.

### Target objects and schemas:

The connector extracts metadata for the following database objects:
* Tables
* Views

### Parameters
The Oracle connector takes the following parameters:
|Parameter|Description|Required/Optional|
|---------|------------|-------------|
|target_project_id|GCP Project ID/Project Number, or 'global'. Used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_location_id|GCP Region ID, or 'global'. Used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_entry_group_id|Dataplex Entry Group ID to use in the generated data|REQUIRED|
|host|Oracle server to connect to|REQUIRED|
|port|Oracle server port (usually 1521)|REQUIRED|
|service|Oracle service to connect to. **One of either service or sid must be specified**|
|sid|Oracle SID (Service Identifier). **One of either service or sid must be specified**|
|user|Oracle Username to connect with|REQUIRED|
|password-secret|GCP Secret Manager ID holding the password for the Oracle user. Format: projects/[PROJ]/secrets/[SECRET]|REQUIRED|
|output_bucket|GCS bucket where the output file will be stored (do not include gs:// prefix)|REQUIRED|
|output_folder|Folder in the GCS bucket where the export output file will be stored|OPTIONAL|

### Prepare your database environment:

Best practise is to connect to the database using a dedicated user with the minimum privileges required to extract metadata. 
1. Create a user in the Oracle instance(s) which has the following privileges and roles: 
    * CONNECT and CREATE SESSION
    * SELECT on DBA_OBJECTS
    * SELECT on all schemas for which metadata needs to be extracted
2. Add the password for the user to the Google Cloud Secret Manager in your project and note the Secret ID.

## Running the connector
There are three ways to run the connector:
1) [From the command line](###running-from-the-command-line) (extract metadata to GCS only)
2) [As a container via a Dataproc Serverless job](###build-a-container-and-extract-metadata-with-a-dataproc-serverless-job) (extract metadata to GCS only)
3) [Scheduled and run via Workflows](###schedule-end-to-end-metadata-extraction-and-import-using-google-cloud-workflows) (End-to-end: Extracts and stores metadata file in GCS bucket and then calls API to import into Dataplex)

### 1. Running from the command line

The metadata connector can be from the command line by directly executing the main.py script.

#### Prepare the environment:
1. Download **ojdbc11.jar** [from Oracle](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)
2. Edit JAR_FILE and SPARK_JAR_PATH in [connection_jar.py](src/connection_jar.py) to match the name and location of the jar file
3. Ensure a Java Runtime Environment (JRE) is installed in your environment
4. If you don't have one set up already, create a Python virtual environment to isolate the connector.
    See [here](https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/) for more details but the TL;DR instructions are to run the following in your home directory:
```
pip install virtualenv
python -m venv myvenv
source venv/bin/activate
```
5. Install PySpark for your local environment
```bash
pip3 install pyspark
```
6. Install all remaining dependencies for the connector 
```bash
pip3 install -r requirements.txt
```
7. Ensure you have a clear network path from the machine where you will run the script to the target database server
8. Ensure the user you are running the script with has the following Google Cloud IAM roles:
-   roles/secretmanager.secretAccessor
-   roles/storage.objectUser

You can authenticate your user with GCP 
```bash
gcloud auth application-default login
```

Execute metadata extraction with the following command, substituting appropriate values for your environment:

```shell 
python3 main.py \
--target_project_id my-gcp-project-id \
--target_location_id us-central1 \
--target_entry_group_id oracle \
--host the-oracle-server \
--port 1521 \
--user dataplexagent \
--password-secret projects/73819994526/secrets/dataplexagent_oracle \
--service XEPDB1 \
--output_bucket dataplex_connectivity_imports \
--output_folder oracle
```

#### Output:
The connector generates a metadata extract in JSONL format as described [in the documentation](https://cloud.google.com/dataplex/docs/import-metadata#metadata-import-file) and stores it in the GCS output bucket and folder defined above. A copy of the generated file will also be saved in the "output" folder in the local directory.

A sample output from the Oracle connector can be found [here](sample/)

To import a metadata file run the Dataplex Import API as explained below in "Manually running a metadata import into Dataplex"

### 2. Build a container and extract metadata with a Dataproc Serverless job:

You can build a Docker container for the connector and run the extraction process as a Dataproc Serverless job.

#### Building the container (one-time task)

Before you begin ensure you have Docker installed in your environment and that the user you run the script with has artifactregistry.repositories.uploadArtifacts privilege on the artfiact registry in your project.

1. Edit [build_and_push_docker.sh](build_and_push_docker.sh) and set the PROJECT AND REGION_ID
2. Make the script executable and run
    ```bash
    chmod a+x build_and_push_docker.sh
    ./build_and_push_docker.sh
    ``` 
    A Docker container called **dataplex-oracle-pyspark** will be built and stored it in Artifact Registry. 
    This process can take take up to 10 minutes.

#### Submitting a metadata extraction job to Dataproc serverless:

To prepare to run a Dataproc Serverless job:

1. Upload the **odjcb11.jar** file to a Google Cloud Storage bucket in your project and use this path for the **--jars** parameter in the command below
2. Create a GCS bucket which will be used for Dataproc Serverless as a working directory and use it to the **--deps-bucket** parameter below

Note: The service account you submit for the job using **--service-account** below needs to have the following IAM roles described [here](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source#required-roles). You can use [this script](../common_scripts/grant_SA_dataproc_roles.sh) to grant the roles to your service account.

```shell
gcloud dataproc batches submit pyspark \
    --project=my-gcp-project-id \
    --region=us-central1 \
    --batch=0001 \
    --deps-bucket=dataplex-metadata-collection-bucket \  
    --container-image=us-central1-docker.pkg.dev/my-gcp-project-id/docker-repo/oracle-pyspark@sha256:dab02ca02f60a9e12769999191b06d859b947d89490d636a34fc734d4a0b6d08 \
    --service-account=440199992669-compute@developer.gserviceaccount.com \
    --jars=gs://gcs/path/to/ojdbc11.jar  \
    --network=Your-Network-Name \
    main.py \
--  --target_project_id my-gcp-project-id \
      --target_location_id us-central1	\
      --target_entry_group_id oracle \
      --host the-oracle-server \
      --port 1521 \
      --user dataplexagent \
      --password-secret projects/73819994526/secrets/dataplexagent_oracle \
      --service XEPDB1 \
      --output_bucket gcs_output_bucket_path \
      --output_folder oracle
```

See the documentation for [gcloud dataproc batches submit pyspark](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark) for more information.

### 3. Schedule end-to-end metadata extraction and import using Google Cloud Workflows

To run an end-to-end metadata extraction and import process, run the container via Google Cloud Workflows. 

Follow the Dataplex documentation here: [Import metadata from a custom source using Workflows](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source) and use [this yaml file](https://github.com/GoogleCloudPlatform/cloud-dataplex/blob/main/managed-connectivity/cloud-workflows/byo-connector/templates/byo-connector.yaml) as a template.


## Manually running a metadata import into Dataplex

To import a metadata import file into Dataplex, see the [Dataplex documetation](https://cloud.google.com/dataplex/docs/import-metadata#import-metadata) for full instructions about calling the API.
The [samples](/samples) directory contains an examples metadata import file for Oracle and request file for callng the API