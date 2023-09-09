# Google Cloud Dataplex - DataScans

## Samples in this directory
 * `sample-scripts` folder provides examples in python to:
    * create, run, and update a Data Quality Scan
    * get job results of a Data Quality Scan
    * create a Profile Scan
 * `airflow-dags` folder provides airflow dags examples in python to:
    * create, run, and evaluate a Data Quality Scan
    * async run and evaluate a DataScan
    * fail a Data quality scan if rules fail
    * run and evaluate a DataScan with custom config parameters
 * `gcloud-commands` folder provides sample gcloud commands to:
    * create, run, view, list and update datasacan
    * list all job runs of datacan and view details of a data scan job
    * sample yaml spec files for data profile and data quality

## Additional DataScan Resources

### Terraform 
* [DataScan Resource](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataplex_datascan)
* [DataScan Module](https://github.com/GoogleCloudPlatform/cloud-foundation-fabric/tree/master/modules/dataplex-datascan)