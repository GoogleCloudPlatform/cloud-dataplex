# Teradata Connector

This custom connector extracts metadata from Teradata databases for import into [Dataplex Universal Catalog](https://cloud.google.com/dataplex/docs/introduction).

Custom connectors are part of the [Managed Connectivity framework](https://cloud.google.com/dataplex/docs/managed-connectivity-overview) and are responsible for the export of metadata from external systems into correctly formatted import files. See [Develop Custom Connectors](https://cloud.google.com/dataplex/docs/develop-custom-connector) for more information.

This is not an officially supported Google product and is provided on an as-is basis, without warranty. This project is not eligible for the [Google Open Source Software Vulnerability Rewards Program](https://bughunters.google.com/open-source-security).

## Overview

### Extracted metadata

|Object|Metadata Extracted|
|------|------------------|
|Tables|Table name, column names, column data types, column NULL/NOT NULL, column default value, table and column comments|
|Views|View name, column names, column data types, column NULL/NOT NULL, column default value, view and column comments|

### Dataplex entry hierarchy

```
teradata-instance (host)
  └── teradata-database (host)
        └── teradata-schema (database from DBC.DatabasesV)
              ├── teradata-table (with dataplex-types.global.schema aspect for columns)
              └── teradata-view  (with dataplex-types.global.schema aspect for columns)
```

System databases (DBC, SysAdmin, SystemFe, TDStats, etc.) are automatically excluded.

## Parameters

|Parameter|Description|Default|Required/Optional|
|---------|-----------|-------|-----------------|
|target_project_id|Google Cloud Project ID for the generated metadata||REQUIRED|
|target_location_id|Google Cloud Region ID, or `global`||REQUIRED|
|target_entry_group_id|Entry Group ID for the imported entries||REQUIRED|
|host|Teradata server hostname||REQUIRED|
|port|Teradata DBS port number|1025|OPTIONAL|
|user|Teradata username||REQUIRED for TD2; OPTIONAL for LDAP/JWT|
|database|Scope extraction to a specific database. If omitted, all non-system databases are extracted||OPTIONAL|
|password_secret|Secret Manager ID for password. Format: `projects/PROJECT_ID/secrets/SECRET_NAME`||See [Password methods](#password-methods)|
|password_file|Path to a file containing the password||See [Password methods](#password-methods)|
|password|Password provided directly (least secure)||See [Password methods](#password-methods)|
|logmech|Teradata logon mechanism: TD2, LDAP, or JWT|TD2|OPTIONAL|
|logdata|Additional logon data for the selected logmech (e.g., LDAP credentials, JWT tokens)||OPTIONAL|
|logdata_secret|Secret Manager ID for logdata. Format: `projects/PROJECT_ID/secrets/SECRET_NAME`. Mutually exclusive with `--logdata`||OPTIONAL|
|query_band|Teradata session query band for tracking. See [Query band](#query-band)|See below|OPTIONAL|
|charset|JDBC session character set|UTF8|OPTIONAL|
|local_output_only|Write metadata file locally only, do not upload to Cloud Storage|False|OPTIONAL|
|output_bucket|GCS bucket for metadata output (no `gs://` prefix). Required unless `--local_output_only`||REQUIRED|
|output_folder|Folder within the GCS bucket. Required unless `--local_output_only`||REQUIRED|
|jar|Path to JDBC jar file|terajdbc4.jar|OPTIONAL|
|min_expected_entries|Minimum entries expected; fewer means no upload to GCS|-1|OPTIONAL|

The `TERADATA_PASSWORD` environment variable can also be used to provide the password. See [Password methods](#password-methods) for the full priority order.

Note: **target_project_id**, **target_location_id** and **target_entry_group_id** are string values in the generated metadata file and define the import scope. They do not need to match the project where the connector runs. See [components of a metadata job](https://cloud.google.com/dataplex/docs/import-metadata#components) for details.

### Password methods

The connector resolves the password using the first available method in this priority order:

|Priority|Method|Description|
|--------|------|-----------|
|1|`--password_secret`|Google Secret Manager (recommended for production)|
|2|`--password_file`|Path to a local file containing the password|
|3|`TERADATA_PASSWORD`|Environment variable|
|4|`--password`|CLI argument (a security warning is printed to stderr)|

If multiple methods are provided, the highest-priority method is used. For **TD2** (default), at least one password method is required. For **LDAP** and **JWT**, password is optional and defaults to empty if not provided. Empty or whitespace-only values are rejected for all methods.

### Authentication methods

The connector supports multiple Teradata logon mechanisms via the `--logmech` parameter:

|Method|`--user`|`--password`|`--logdata`|Notes|
|------|:------:|:------:|:---------:|-----|
|**TD2** (default)|Required|Required|Optional|Traditional username/password|
|**LDAP**|Optional|Optional|Optional|Enterprise directory authentication|
|**JWT**|Optional|Optional|Optional (for token)|Token-based authentication|

When using LDAP or JWT, the `--logdata` parameter (or `--logdata_secret` for secure retrieval from Secret Manager) can pass additional authentication data such as LDAP credentials or JWT tokens.

### Query band

The `--query_band` parameter sets a Teradata session query band for tracking and telemetry. If not provided, the following default is applied:

```
org=teradata-internal-telem;appname=teradata-dataplex-connector;
```

Custom query bands must use `key=value;` format. The `org` and `appname` keys are always enforced -- if omitted, defaults are added; if a custom `appname` is provided, the default is appended (e.g., `appname=myapp_teradata-dataplex-connector;`).

Restrictions:
* Only alphanumeric characters, hyphens, underscores, dots, equals, semicolons, commas, and spaces are allowed
* Reserved names (`proxyuser`, `proxyrole`) are rejected
* Maximum length: 2048 characters

## Getting started

### Prerequisites

* **Python 3.x**
    ```bash
    sudo apt update
    sudo apt install python3 python3-dev python3-venv python3-pip
    ```
* **Python virtual environment**
    ```bash
    python3 -m venv env
    source env/bin/activate
    ```
    Run `source env/bin/activate` each time before using the connector.

* **Java Runtime Environment (JRE)**
    ```bash
    sudo apt install default-jre
    ```
* **PySpark**
    ```bash
    pip3 install pyspark
    ```

#### Windows prerequisites

PySpark on Windows requires Hadoop's `winutils.exe`. Download or build `winutils.exe` for your Hadoop version, place it at `C:\hadoop\bin\winutils.exe`, and set the environment variables:

```shell
set HADOOP_HOME=C:\hadoop
set PYSPARK_PYTHON=C:\Path\To\python.exe
set PYSPARK_DRIVER_PYTHON=C:\Path\To\python.exe
```

### Install

1. Clone the repository:
    ```bash
    git clone https://github.com/GoogleCloudPlatform/cloud-dataplex.git
    cd cloud-dataplex/managed-connectivity/community-contributed-connectors/teradata-connector
    ```

2. Install Python dependencies:
    ```bash
    pip3 install -r requirements.txt
    ```

3. Download the Teradata JDBC driver **terajdbc4.jar** from [Teradata Downloads](https://downloads.teradata.com/) and place it in the connector directory.

    Note: The Teradata JDBC driver is not available on Maven Central and must be downloaded manually. Use `--jar` to specify a different version or path.

### Create a database user

Best practice is to create a dedicated database user with the minimum privileges required:
* SELECT on DBC.DatabasesV
* SELECT on DBC.TablesV
* SELECT on DBC.ColumnsV

Note: When using LDAP or JWT authentication (`--logmech LDAP` or `--logmech JWT`), a dedicated database user may not be required. Authentication is handled by the external identity provider, though the authenticated identity still requires the privileges listed above.

### Store the password in Secret Manager

Create a secret:

```bash
echo -n "YOUR_PASSWORD" | gcloud secrets create teradata-password \
  --project=PROJECT_ID --data-file=-
```

To update an existing secret with a new version:

```bash
echo -n "YOUR_NEW_PASSWORD" | gcloud secrets versions add teradata-password \
  --project=PROJECT_ID --data-file=-
```

### GCP authentication and authorization

Before running the connector, ensure your session is authenticated as a Google Cloud identity with the required IAM roles:

* `roles/storage.objectUser` -- required when using `--output_bucket`
* `roles/secretmanager.secretAccessor` -- required when using `--password_secret` or `--logdata_secret`

```bash
gcloud auth application-default login
```

Note: If you are not running in a Google Cloud managed environment, first install the [Google Cloud CLI](https://cloud.google.com/sdk/docs/install-sdk).

## Run the connector

Run from the connector root directory, substituting placeholder values for your environment.

### Basic usage (TD2 with Secret Manager)

```shell
python3 main.py \
  --target_project_id PROJECT_ID \
  --target_location_id us-central1 \
  --target_entry_group_id teradata \
  --host TERADATA_HOST \
  --user USERNAME \
  --password_secret projects/PROJECT_ID/secrets/teradata-password \
  --output_bucket OUTPUT_BUCKET \
  --output_folder teradata_metadata
```

To scope extraction to a single database, add `--database DATABASE_NAME`.
For local output only (no GCS upload), replace the output options with `--local_output_only`.

### LDAP authentication

```shell
python3 main.py \
  --target_project_id PROJECT_ID \
  --target_location_id us-central1 \
  --target_entry_group_id teradata \
  --host TERADATA_HOST \
  --logmech LDAP \
  --logdata_secret projects/PROJECT_ID/secrets/ldap-credentials \
  --local_output_only
```

If you prefer to pass logdata directly (not recommended for production), use `--logdata` instead of `--logdata_secret`.

### JWT authentication

```shell
python3 main.py \
  --target_project_id PROJECT_ID \
  --target_location_id us-central1 \
  --target_entry_group_id teradata \
  --host TERADATA_HOST \
  --logmech JWT \
  --logdata_secret projects/PROJECT_ID/secrets/jwt-token \
  --local_output_only
```

### Alternative password methods

```shell
# Using a password file
python3 main.py \
  --target_project_id PROJECT_ID \
  --target_location_id us-central1 \
  --target_entry_group_id teradata \
  --host TERADATA_HOST \
  --user USERNAME \
  --password_file /path/to/password.txt \
  --local_output_only
```

```shell
# Using TERADATA_PASSWORD environment variable
export TERADATA_PASSWORD="YOUR_PASSWORD"
python3 main.py \
  --target_project_id PROJECT_ID \
  --target_location_id us-central1 \
  --target_entry_group_id teradata \
  --host TERADATA_HOST \
  --user USERNAME \
  --local_output_only
```

### Custom query band

Add `--query_band` to any command to set a session query band:

```shell
python3 main.py \
  --target_project_id PROJECT_ID \
  --target_location_id us-central1 \
  --target_entry_group_id teradata \
  --host TERADATA_HOST \
  --user USERNAME \
  --password_secret projects/PROJECT_ID/secrets/teradata-password \
  --query_band "org=myorg;appname=myapp;env=prod;" \
  --output_bucket OUTPUT_BUCKET \
  --output_folder teradata_metadata
```

## Import metadata into Dataplex Universal Catalog

### Connector output

The connector generates a JSONL metadata import file as described [in the documentation](https://cloud.google.com/dataplex/docs/import-metadata#metadata-import-file). The file is always written to the local `output/` directory. If `--output_bucket` and `--output_folder` are provided (and `--local_output_only` is not set), the file is also uploaded to Cloud Storage automatically.

A sample output file is available in the [sample/](sample/) directory.

### Validate the output

Use the validation script to verify the generated JSONL file is compatible with the Dataplex import API before uploading:

```bash
python3 tests/validate_output.py output/teradata-TERADATA_HOST.jsonl
```

The script checks each entry for:
* Required top-level keys (`entry`, `aspectKeys`, `updateMask`)
* Correct camelCase field naming (no snake_case)
* Valid `entrySource` with `system: "teradata"`
* Aspect structure and key consistency
* Valid schema fields (mode, dataType, metadataType)
* Parent-child hierarchy integrity

It prints a summary of entry counts by type (instances, databases, schemas, tables, views) and total column count. Exits with code 0 on success or 1 if errors are found.

### Upload to Cloud Storage

If you used `--local_output_only`, upload the file manually before importing:

```bash
gsutil cp output/teradata-TERADATA_HOST.jsonl gs://OUTPUT_BUCKET/import/
```

### Create Dataplex catalog resources

Before importing, the Entry Group, Entry Types, and Aspect Types must exist in the target project. Resources must be created in this dependency order:

```
template.json --> Aspect Types --> Entry Types
                                        \
                             Entry Group --> Metadata Import
```

Aspect Types must exist before Entry Types (because Entry Types reference them). The Entry Group must exist before import (because entries are stored in it).

This connector requires:

|Catalog Object|IDs|
|---------|---|
|Entry Group|Defined by `--target_entry_group_id`|
|Entry Types|teradata-instance, teradata-database, teradata-schema, teradata-table, teradata-view|
|Aspect Types|teradata-instance, teradata-database, teradata-schema, teradata-table, teradata-view|

#### Using the setup script (Linux/macOS)

```bash
PROJECT_ID=my-project-id LOCATION=us-central1 bash scripts/setup_dataplex_resources.sh
```

#### Manual setup

**1. Create a metadata template file** (`template.json`):

```json
{"name":"marker","type":"record","recordFields":[{"name":"description","type":"string","index":1,"constraints":{"required":false}}]}
```

**2. Create the Entry Group:**

```bash
gcloud dataplex entry-groups create teradata \
  --project=PROJECT_ID \
  --location=us-central1 \
  --description="Entry group for Teradata metadata"
```

**3. Create Aspect Types** (one per entry level):

```bash
gcloud dataplex aspect-types create teradata-instance --project=PROJECT_ID --location=us-central1 --display-name="Teradata Instance" --metadata-template-file-name=template.json
gcloud dataplex aspect-types create teradata-database --project=PROJECT_ID --location=us-central1 --display-name="Teradata Database" --metadata-template-file-name=template.json
gcloud dataplex aspect-types create teradata-schema   --project=PROJECT_ID --location=us-central1 --display-name="Teradata Schema"   --metadata-template-file-name=template.json
gcloud dataplex aspect-types create teradata-table    --project=PROJECT_ID --location=us-central1 --display-name="Teradata Table"    --metadata-template-file-name=template.json
gcloud dataplex aspect-types create teradata-view     --project=PROJECT_ID --location=us-central1 --display-name="Teradata View"     --metadata-template-file-name=template.json
```

**4. Create Entry Types** (instance, database, schema -- single required aspect each):

```bash
gcloud dataplex entry-types create teradata-instance --project=PROJECT_ID --location=us-central1 --display-name="Teradata Instance" --required-aspects=type=projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-instance
gcloud dataplex entry-types create teradata-database --project=PROJECT_ID --location=us-central1 --display-name="Teradata Database" --required-aspects=type=projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-database
gcloud dataplex entry-types create teradata-schema   --project=PROJECT_ID --location=us-central1 --display-name="Teradata Schema"   --required-aspects=type=projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-schema
```

**5. Create Entry Types** (table, view -- require both custom aspect and global schema):

```bash
gcloud dataplex entry-types create teradata-table --project=PROJECT_ID --location=us-central1 --display-name="Teradata Table" --required-aspects=type=projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-table --required-aspects=type=projects/dataplex-types/locations/global/aspectTypes/schema
gcloud dataplex entry-types create teradata-view  --project=PROJECT_ID --location=us-central1 --display-name="Teradata View"  --required-aspects=type=projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-view  --required-aspects=type=projects/dataplex-types/locations/global/aspectTypes/schema
```

### Run the metadata import

After uploading the JSONL file to GCS and creating the catalog resources:

```bash
gcloud dataplex metadata-jobs create \
  --project=PROJECT_ID \
  --location=us-central1 \
  --type=IMPORT \
  --import-source-storage-uri=gs://OUTPUT_BUCKET/import/ \
  --import-entry-sync-mode=FULL \
  --import-aspect-sync-mode=INCREMENTAL \
  --import-entry-groups=projects/PROJECT_ID/locations/us-central1/entryGroups/teradata \
  --import-entry-types=projects/PROJECT_ID/locations/us-central1/entryTypes/teradata-instance,projects/PROJECT_ID/locations/us-central1/entryTypes/teradata-database,projects/PROJECT_ID/locations/us-central1/entryTypes/teradata-schema,projects/PROJECT_ID/locations/us-central1/entryTypes/teradata-table,projects/PROJECT_ID/locations/us-central1/entryTypes/teradata-view \
  --import-aspect-types=projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-instance,projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-database,projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-schema,projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-table,projects/PROJECT_ID/locations/us-central1/aspectTypes/teradata-view,projects/dataplex-types/locations/global/aspectTypes/schema
```

A sample metadata import request is available at [sample/metadata_import_request.json](sample/metadata_import_request.json).

### Monitor import jobs

List all metadata jobs:

```bash
gcloud dataplex metadata-jobs list --project=PROJECT_ID --location=us-central1
```

Check the status of a specific job:

```bash
gcloud dataplex metadata-jobs describe JOB_ID --project=PROJECT_ID --location=us-central1
```

View warning and error logs:

```bash
gcloud logging read \
  "resource.type=dataplex.googleapis.com/MetadataJob \
   AND resource.labels.metadata_job_id=JOB_ID \
   AND severity>=WARNING" \
  --project=PROJECT_ID \
  --format="value(jsonPayload.message)"
```

See [manage entries and create custom sources](https://cloud.google.com/dataplex/docs/ingest-custom-sources) for more information.

## Dataproc Serverless

Follow these instructions to build a Docker container and run the connector with [Dataproc Serverless](https://cloud.google.com/dataproc-serverless/docs).

### Build the container (one-time)

1. Ensure [Docker](https://docs.docker.com/engine/install/) is installed.

2. Create an Artifact Registry repository (if one does not already exist):
    ```bash
    gcloud artifacts repositories create docker-repo \
      --repository-format=docker \
      --location=us-central1 \
      --project=PROJECT_ID
    ```

3. Configure Docker to authenticate with Artifact Registry:
    ```bash
    gcloud auth configure-docker us-central1-docker.pkg.dev
    ```

4. Edit [build_and_push_docker.sh](build_and_push_docker.sh) and set `PROJECT_ID` and `REGION`.

5. Build and push:
    ```bash
    chmod +x build_and_push_docker.sh
    ./build_and_push_docker.sh
    ```
    This builds a container called **catalog-teradata-pyspark** and pushes it to Artifact Registry (~5 minutes).

### Set up IAM roles

Grant the required IAM roles to the service account that will run the Dataproc job. If `--service-account` is not provided, the default Compute Engine service account is used.

```bash
SA="my-sa@my-project-id.iam.gserviceaccount.com"
PROJECT_ID="my-project-id"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA}" --role="roles/secretmanager.secretAccessor"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA}" --role="roles/storage.objectUser"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA}" --role="roles/dataproc.worker"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA}" --role="roles/dataplex.entryOwner"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA}" --role="roles/dataplex.catalogEditor"
```

You can also use this [script](../common_scripts/grant_SA_dataproc_roles.sh) to grant the required roles.

### Submit a job

1. Create or choose a Cloud Storage bucket for Dataproc (used as `--deps-bucket`).

2. Identify the subnet for Dataproc Serverless:
    ```bash
    gcloud compute networks subnets list \
      --project=PROJECT_ID \
      --regions=us-central1 \
      --format="table(name,network.basename())"
    ```

3. Submit the job:
    ```shell
    gcloud dataproc batches submit pyspark \
        --project=PROJECT_ID \
        --region=us-central1 \
        --batch=teradata-metadata-0001 \
        --deps-bucket=DEPS_BUCKET \
        --container-image=us-central1-docker.pkg.dev/PROJECT_ID/docker-repo/catalog-teradata-pyspark:latest \
        --service-account=SERVICE_ACCOUNT_EMAIL \
        --jars=terajdbc4.jar \
        --subnet=SUBNET_NAME \
        main.py \
    --  --target_project_id PROJECT_ID \
          --target_location_id us-central1 \
          --target_entry_group_id teradata \
          --host TERADATA_HOST \
          --port 1025 \
          --user USERNAME \
          --password_secret projects/PROJECT_ID/secrets/teradata-password \
          --output_bucket OUTPUT_BUCKET \
          --output_folder import
    ```

    Notes:
    * Use `--network=default` instead of `--subnet` if your project uses the default network.
    * To use a different JDBC jar version, store it in GCS: `--jars=gs://BUCKET/path/to/terajdbc4.jar`

4. Monitor the job:
    ```bash
    gcloud dataproc batches describe BATCH_ID \
      --project=PROJECT_ID --region=us-central1
    ```

See the [documentation](https://cloud.google.com/sdk/gcloud/reference/dataproc/batches/submit/pyspark) for more information about Dataproc Serverless.

## Cloud Workflows (automated pipeline)

An end-to-end metadata extraction and import pipeline with monitoring can be created using [Workflows](https://cloud.google.com/workflows) and scheduled to run on a regular basis.

A Teradata-specific workflow template is included at [teradata-connector-workflow.yaml](teradata-connector-workflow.yaml).

### Deploy and execute

```bash
# Deploy the workflow
gcloud workflows deploy teradata-metadata-import \
  --project=PROJECT_ID \
  --location=us-central1 \
  --source=teradata-connector-workflow.yaml

# Execute the workflow
gcloud workflows execute teradata-metadata-import \
  --project=PROJECT_ID \
  --location=us-central1 \
  --data='{
    "PROJECT_ID": "my-project-id",
    "CLOUD_REGION": "us-central1",
    "TERADATA_HOST": "teradata.example.com",
    "TERADATA_PORT": "1025",
    "TERADATA_USER": "dataplexagent",
    "PASSWORD_SECRET": "projects/my-project-id/secrets/teradata-password",
    "OUTPUT_BUCKET": "my-project-dataplex-teradata",
    "SERVICE_ACCOUNT": "my-sa@my-project-id.iam.gserviceaccount.com",
    "CONTAINER_IMAGE": "us-central1-docker.pkg.dev/my-project-id/docker-repo/catalog-teradata-pyspark:latest",
    "DEPS_BUCKET": "my-project-dataplex-teradata"
  }'
```

You can also use the generic [byo-connector.yaml](https://github.com/GoogleCloudPlatform/cloud-dataplex/blob/main/managed-connectivity/cloud-workflows/byo-connector/templates/byo-connector.yaml) template. Follow the documentation: [Import metadata from a custom source using Workflows](https://cloud.google.com/dataplex/docs/import-using-workflows-custom-source).

## Known limitations

* **Non-ASCII column names** -- Non-ASCII characters in column names (e.g., Chinese, Japanese, accented characters) are encoded to `_u<codepoint>_` format for Dataplex compatibility.
* **Special characters in column names** -- Column names containing ASCII special characters (e.g., `!@#$%^&*{}|,?:;~`) are passed through as-is. The Dataplex import API may reject entries with these characters in schema field paths, resulting in `INVALID_UPDATE_ENTRY_REQUEST` errors. The connector does not alter these names to preserve the original metadata. Tables with affected columns will be partially imported (the entry is created but the schema aspect is rejected).
