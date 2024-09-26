# Google Cloud Dataplex - Managed Connectivity

## Samples in this directory

* `sample_custom_connector`: This directory provides a sample custom connector for importing metadata from an Oracle database to Google Cloud Dataplex. It demonstrates how to build a connector that extracts schema information from Oracle and transforms it into the format required by Dataplex.

  * src: Contains the Python code for the connector.
      * It handles user-provided arguments (GCP project, location, Oracle connection details, etc.).
      * Uses Google Cloud Secret Manager to securely retrieve the Oracle database password.
      * Connects to the Oracle database and extracts metadata about the instance, databases, and tables.
      * Converts the extracted metadata into Dataplex ImportItem format (compatible with the [Metadata import file](https://cloud.google.com/dataplex/docs/import-metadata#metadata-import-file)).
      * Writes the formatted metadata to a user-specified Google Cloud Storage (GCS) bucket.
  * build_and_push_docker.sh: A shell script that automates the process of Building the Docker image from the Dockerfile and    pushing the built image to a Google Cloud Container Registry.

* `cloud-workflows`: This directory provides resources for orchestrating and running Dataplex metadata import workflows using Google Cloud Workflows.

      * `byo-connector`: Contains sample code and configurations for running the "Bring Your Own Connector" (BYOC) flow in Dataplex. Includes sample Cloud Workflow arguments, template files, and Terraform configurations.


## Additional Resources
