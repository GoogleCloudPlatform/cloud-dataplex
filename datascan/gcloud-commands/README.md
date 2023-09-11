
Copyright 2023 Google. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreement with Google.

## In this folder,
* gcloud commands to:
  * create and update data quality and profile scans.
  * run and delete a data scan.
  * list data scans under a project.
  * View details of a data scan.
  * list job runs of a data scan.
  * View details of a data scan job. 
  * get and set IAM policy for a data scan. 

## Setup

<b>Note</b>: Make sure that the Cloud SDK version is <b>440.0.0 or higher</b>

### Check Cloud SDK version:
To check SDK version, run:

```
gcloud --version
```

### Update Cloud SDK version:
To update SDK version, run following set of commands:

```
sudo apt update
sudo apt-get upgrade google-cloud-sdk
```

## Create a data quality scan
To create a data quality scan <b>data-quality-datascan</b> in project <b>test-project</b> located in <b>us-central1</b> on bigquery resource table <b>test-table</b> in dataset <b>test-dataset</b> with data quality spec file <b>data-quality-spec.yaml</b>, run:

```
gcloud dataplex datascans create data-quality data-quality-datascan --project=test-project --location=us-central1 --data-source-resource="//bigquery.googleapis.com/projects/test-project/datasets/test-dataset/tables/test-table" --data-quality-spec-file="data-quality-spec.yaml"
```

<b>Note</b>: The argument <b>--data-quality-spec-file</b> accepts file in both <i>json</i> and <i>yaml</i> format, and supports both local and cloud storage bucket file starting with prefix <b>gs://</b>, for e.g. <i>gs://{bucket_name}/{path_to_spec_file}</i> (see example yaml file in `spec-files` folder). 

For optional arguments, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/create/data-quality).

## Create a data profile scan
To create a data profile scan <b>data-profile-datascan</b> in project <b>test-project</b> located in <b>us-central1</b> on bigquery resource table <b>test-table</b> in dataset <b>test-dataset</b>, run:

```
gcloud dataplex datascans create data-profile data-profile-datascan --project=test-project --location=us-central1 --data-source-resource="//bigquery.googleapis.com/projects/test-project/datasets/test-dataset/tables/test-table"
```

<b>Note</b>: The data profile specs are optional and can be added using both <b>command line arguments</b>, and <b>--data-profile-spec-file</b>, which accepts file in both <i>json</i> and <i>yaml</i> format, and supports both local and cloud storage bucket file starting with prefix <b>gs://</b>, for e.g. <i>gs://{bucket_name}/{path_to_spec_file}</i> (see example yaml file in `spec-files` folder). 

For optional arguments and providing data profile specs via command line, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/create/data-profile).

## Run a data scan
To run a data scan <b>example-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans run example-datascan --project=test-project --location=us-central1
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/run).

## List all data scans
To list all data scans in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans list --project=test-project --location=us-central1
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/list).

## Describe a data scan
To describe a data scan <b>example-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans describe example-datascan --project=test-project --location=us-central1
```

To view the latest successful scan job, use full view:

```
gcloud dataplex datascans describe example-datascan --project=test-project --location=us-central1 --view=FULL
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/describe).

## Update a data quality scan
To update a data quality scan <b>data-quality-datascan</b> in project <b>test-project</b> located in <b>us-central1</b> with data quality spec file <b>data-quality-spec.yaml</b>, run:

```
gcloud dataplex datascans update data-quality data-quality-datascan --project=test-project --location=us-central1 --data-quality-spec-file="data-quality-spec.yaml"
```

<b>Note</b>: The specs defined in data-quality-spec-file are overridden. 

For supported update parameters, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/update/data-quality).

## Update a data profile scan
To update description of a data profile scan <b>data-profile-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans update data-profile data-profile-datascan --project=test-project --location=us-central1 --description="Updated description"
```

<b>Note</b>: The data profile specs can be updated using both <b>command line arguments</b>, and <b>data-profile-spec-file</b>.

For supported update parameters, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/update/data-profile).


## List all job runs of a data scan
To list all job runs of a data scan <b>example-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans jobs list --project=test-project --location=us-central1 --datascan=example-datascan
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/jobs/list).

## Describe a data scan
To describe a data scan job <b>example-job</b> running a data scan <b>example-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans jobs describe example-job --project=test-project --location=us-central1 --datascan=example-datascan
```

To describe the details of data scan job, use full view:

```
gcloud dataplex datascans jobs describe example-job --project=test-project --location=us-central1 --datascan=example-datascan --view=FULL
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/jobs/describe).

## Delete a data scan
To delete a data scan <b>example-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans delete example-datascan --project=test-project --location=us-central1
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/delete).

## Get IAM policy for a data scan
Displays the IAM policy associated with a Dataplex datascan resource. If formatted as JSON, the output can be edited and used as a policy file for set-iam-policy. The output includes an <b>etag</b> field identifying the version emitted and allowing detection of concurrent policy updates.

To print the IAM policy for a data scan <b>example-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans get-iam-policy example-datascan --project=test-project --location=us-central1
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/get-iam-policy).

## Set IAM policy for a data scan
sets the IAM policy to a Dataplex datascan as defined in a JSON or YAML file

To read an IAM policy defined in a JSON file <b>policy.json</b> and set it for a data scan <b>example-datascan</b> in project <b>test-project</b> located in <b>us-central1</b>, run:

```
gcloud dataplex datascans set-iam-policy --project=test-project --location=us-central1 example-datascan policy.json
```

For more information, see the [gcloud CLI reference](https://cloud.google.com/sdk/gcloud/reference/dataplex/datascans/set-iam-policy).