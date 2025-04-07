# MySQL Connector

This connector extracts metadata from MySQL to Google Cloud Dataplex Catalog.

### Target objects and schemas:

The connector will extract metadata for the following database objects:
* Tables
* Views

### Parameters
The MySQL connector takes the following parameters:
|Parameter|Description|Requiredy/Optional|
|---------|------------|-------------|
|target_project_id|A GCP Project ID/Project Number, or 'global'. Used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_location_id|GCP Region ID, or 'global'. Used in the generated Dataplex Entry, Aspects and AspectTypes|REQUIRED|
|target_entry_group_id|Dataplex Entry Group ID to use in the generated data|REQUIRED|
|host|MySQL server to connect to|REQUIRED|
|port|MySQL server port (usually 3306)|REQUIRED|
|database|MySQL database to connect to|REQUIRED|
|user|MySQL Username to connect with|REQUIRED|
|password-secret|GCP Secret Manager ID holding the password for the MySQL user. Format: projects/PROJECT-ID/secrets/SECRET|REQUIRED|
|output_bucket|GCS bucket where the output file will be stored (do not include gs:// prefix)|REQUIRED|
|output_folder|Folder in the GCS bucket where the export output file will be stored|OPTIONAL|

### Prepare your MySQL environment:

Best practise is to connect to the database with a dedicated user with the minimum privileges required to extract metadata. 
1. Create a user in the MySQL instance(s) with the following privileges: 
    * SELECT on information_schema.tables
    * SELECT on information_schema.columns
    * SELECT on information_schema.views
2. Add the password for the user to the Google Cloud Secret Manager in your project and note the Secret ID (format is: projects/[project-number]/secrets/[secret-name])

## Running the connector
There are three ways to run the connector:
1) [Run the script directly from the command line](###running-from-the-command-line) (extract metadata to GCS only)
2) [Run as a container via a Dataproc Serverless job](###build-a-container-and-extract-metadata-with-a-dataproc-serverless-job) (extract metadata to GCS only)
3) [Schedule and run as a container via Workflows](###schedule-end-to-end-metadata-extraction-and-import-using-google-cloud-workflows) (End-to-end. Extracts metadata into GCS + imports into Dataplex)

### 1. Running from the command line

The metadata connector can be run directly from the command line for development or testing by executing the main.py script.

#### Prepare the environment:
1. Download **mysql-connector-j-9.2.0.jar** [from MySQL](https://dev.mysql.com/downloads/connector/j/?os=26)
2. Edit SPARK_JAR_PATH in [mysql_connector.py](src/mysql_connector.py) to match the location of the jar file
3. Ensure a Java Runtime Environment (JRE) is installed in your environment
4. Create a Python virtual environment to isolate the connector environment.
    See [here](https://www.freecodecamp.org/news/how-to-setup-virtual-environments-in-python/) for more details but the TLDR; instructions are to run the following in your home directory:
```
pip install virtualenv
python -m venv myvenv
source myvenv/bin/activate
```
5. Install PySpark in your local environment
```
pip3 install pyspark
```
6. Install all other dependencies for the connector 
```
pip3 install -r requirements.txt
```
7. Ensure you have a clear network path from the machine where you will run the script to the target database server.
8. Ensure the user you are running the script with a user which has the following Google Cloud IAM roles:
-   roles/secretmanager.secretAccessor (to get the user password from Secret Manager)
-   roles/storage.objectUser (on the GCS bucket where the connector will store the output)

You can authenticate your user with 
```bash
gcloud auth application-default login
```

Execute metadata extraction with the following command, substituting appropriate values for your environment:

```shell 
python3 main.py \
--target_project_id my-gcp-project-id \
--target_location_id us-central1 \
--target_entry_group_id mysql \
--host the-mysql-server \
--port 3306 \
--user dataplexagent \
--password-secret projects/73869994526/secrets/dataplexagent_mysql \
--database employees \
--output_bucket dataplex_connectivity_imports \
--output_folder mysql
```

#### Output:
The connector generates a metadata extract in JSONL format as described [in the documentation](https://cloud.google.com/dataplex/docs/import-metadata#metadata-import-file) and stores it in the GCS output bucket and folder defined above. A copy of the generated file will also be saved in the "output" folder in the local directory.

A sample output from the MySQL connector can be found [here](sample/)

To import a metadata file run the Dataplex Import API as explained below in "Manually running a metadata import into Dataplex"

### 2. Build a container and extract metadata with a Dataproc Serverless job:

You can build a Docker container for the connector and run the extraction process as a Dataproc Serverless job.

#### Building the container (one-time task)

Before you begin ensure you have Docker installed in your environment and that the user you run the script with has artifactregistry.repositories.uploadArtifacts privilege on the artfiact registry in your project.

Use this command:
```bash
gcloud artifacts repositories add-iam-policy-binding docker-repo \
--location=us-central1 --member=serviceAccount:440165342669-compute@developer.gserviceaccount.com --role=roles/artifactregistry.writer
```

1. Edit [build_and_push_docker.sh](build_and_push_docker.sh) and set the PROJECT AND REGION_ID
2. Make the script executable and run
    ```bash
    chmod a+x build_and_push_docker.sh
    ./build_and_push_docker.sh
    ``` 
    A Docker container called **dataplex-mysql-pyspark*** will be built and stored it in Artifact Registry. 
    This process can take take up to 10 minutes.

#### Submitting a metadata extraction job to Dataproc serverless:

To prepare to run a Dataproc Serverless job:

1. Upload **mysql-connector-j-9.2.0.jar** to a Google Cloud Storage bucket in your project and use this path for the **--jars** parameter in the command below
2. Create a GCS bucket which will be used for Dataproc Serverless as a working directory and use it to the **--deps-bucket** parameter below

Note: The service account you submit for the job using **--service-account** below needs to have the following IAM roles described [here](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source#required-roles). You can use [this script](../common_scripts/grant_SA_dataproc_roles.sh) to grant the roles to your service account.

```shell
gcloud dataproc batches submit pyspark \
    --project=my-gcp-project-id \
    --region=us-central1 \
    --batch=0001 \
    --deps-bucket=dataplex-metadata-collection-bucket  \
    --container-image=us-central1-docker.pkg.dev/my-gcp-project-id/docker-repo/mysql-pyspark@sha256:dab02ca02f60a9e12769999191b06d859b947d89490d636a34fc734d4a0b6d08 \
    --service-account=440165342669-compute@developer.gserviceaccount.com \
    --jars=gs://gcs/path/to/mysql-connector-j-9.2.0.jar  \
    --network=Your-Network-Name \
    main.py \
--  --target_project_id my-gcp-project-id \
      --target_location_id us-central1	\
      --target_entry_group_id mysql \
      --host the-mysql-server \
      --port 3306 \
      --user dataplexagent \
      --password-secret projects/73819994526/secrets/dataplexagent_mysql \
      --database employees \
      --output_bucket gcs_output_bucket_path \
      --output_folder mysql
```

See the documentatrion for [gcloud dataproc batches submit pyspark](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark) for more information

### 3. Schedule end-to-end metadata extraction and import using Google Cloud Workflows

To run an end-to-end metadata extraction and import process, run the container via Google Cloud Workflows. 

Follow the Dataplex documentation here [Import metadata from a custom source using Workflows](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source) and use [this yaml file](https://github.com/GoogleCloudPlatform/cloud-dataplex/blob/main/managed-connectivity/cloud-workflows/byo-connector/templates/byo-connector.yaml) as a template.


## Manually running a metadata import into Dataplex

To import a metadata import file into Dataplex, see the [Dataplex documetation](https://cloud.google.com/dataplex/docs/import-metadata#import-metadata) for full instructions about calling the API.
The [samples](/samples) directory contains an examples metadata import file and request file for callng the API