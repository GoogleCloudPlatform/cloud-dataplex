
Copyright 2023 Google. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreement with Google.

## In this folder
* airflow dags examples in python to:
    * create, run and evaluate a Data Quality Scan
    * async run and evaluate a DataScan
    * fail a Data quality scan if rules fail
    * run and evaluate a DataScan with custom config parameters

## Setup
To download the necessary client libraries, download the following packages from pip:

`!pip install google-cloud`  
`!pip install google-cloud-dataplex`  
`!pip install apache-airflow-providers-google`   
`!pip install apache-airflow-providers-sendgrid`

<b>Note</b>: 

`google-cloud-dataplex` version should be <b>1.6.2 or higher</b>  
`apache-airflow-providers-google` version should be <b>10.7.0 or higher</b>  

Dataplex operators: https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataplex.html
