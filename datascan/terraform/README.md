# Data Quality and Profile as Code Using Terraform

### Project Overview
This project provides a system to define, execute, and monitor data quality (DQ) rules for datasets managed within Google Dataplex. The core components allow for:
* **Flexible Rule Definition**: DQ rules are expressed in YAML template (`templates.yaml`) and binding (`bindings.yaml`) files. Templates define the reusable structure of a rule (e.g., nullness check, uniqueness check), while bindings map templates to specific columns for rule application.
* **Automated Rule Parsing**: The Python script (`rules_parsing.py`) reads the YAML files, combines their structure and generates a unified, ready-to-use rules file (`parsed_rules_combined.yaml`).
* **Dataplex Integration**: The Terraform modules (`dataplex_data_quality.tf`, `dataplex_profile.tf`, `rules_file_parsing.tf`, `main.tf`) provision Google Dataplex Profile and Data Quality DataScans, parse the combined Data Quality rules and inject them into the Data Quality scan configuration for automated data quality assessments during execution.

### Setup
#### Defining Rules:
* `templates.yaml`: This YAML file contains a list of rule templates, each defining a specific data quality rule. These common rules can be used across multiple tables and columns. Each template includes the rule name, dimension, description, threshold, and specific expectation fields for the rule.
* `bindings.yaml`: This YAML file defines the binding between rule templates and the columns they should be applied to. It contains a list of binding objects, each specifying a template_ref and a list of columns
* **Structure**:
    * Recommended 1 `template` per project
    * 1 `binding` for *each* DQ Scan (e.g., 10 tables = 10 DQ Scans = 10 bindings)

#### Parsing Rules:
* `rules_parsing.py`: This Python script parses the `templates.yaml` and `bindings.yaml` files and generates a combined `parsed_rules_combined.yaml` file with a list of rules for the Data Quality spec.

#### Deployment:
* `dataplex_data_quality.tf`: This Terraform module defines a Dataplex Data Quality scan resource and uses the `parsed_rules_combined.yaml` file to configure the scan's rules.
* `rules_file_parsing.tf`: This Terraform module is responsible for parsing the `parsed_rules_combined.yaml` file and extracting individual rules. It converts the YAML structure into a format suitable for use in the data_quality_spec block of the Dataplex data scan resource.
* `dataplex_profile.tf`: This Terraform module defines a Dataplex Data Profile scan for each of the datasets/tables defined in the `bq_datasets_tables` variable.


### Project Structure
* `rules_parsing.py`: Python script responsible for parsing rule templates and bindings YAML files, and generating the consolidated parsed_rules_combined.yaml file.
* `bindings.yaml`: YAML file specifying bindings between rule templates and target data columns.
* `templates.yaml`: YAML file containing reusable rule templates for data quality checks.
* `dataplex_data_quality.tf`: Terraform module defining the configuration of a Dataplex Data Quality Scan job, including the integration with the parsed data quality rules.
* `rules_file_parsing.tf`: Terraform module responsible for parsing the parsed_rules_combined.yaml file and dynamically creating Dataplex's Data Quality Spec within the DataScan configuration.
* `dataplex_profile.tf`: Terraform module defining the configuration of a Dataplex Data Profile Scan job, including the automatic profile generation of multiple BigQuery tables/datasets.
* `main.tf`: Main Terraform file for infrastructure provisioning.
* `variables.tf`: File containing project-wide variables for customization.

### Usage Instructions
1) Prerequisites:
    * A Google Cloud Project with Dataplex API enabled.
    * Python 3.x installed
    * Terraform installed

2) Customize Variables: 
  * In the variables.tf file: 
    * Set project_id to your GCP project ID.
    * Adjust the region if needed.
    * Modify labels if desired.
    * Add BigQuery datasets and tables to `bq_datasets_tables` variable for automatic Profile Scans.

3) Modify Rule Definitions:
    * Edit the `templates.yaml` file to change or add data quality rule templates.
    * Update the `bindings.yaml` file to create new bindings between templates and specific data columns and tables.


4) Generate Combined Rules:
    * Run the Python script to process your rule definition files: `rules_parsing.py`
    * Verify the output `parsed_rules_combined.yaml` file is generated.

5) Deploy Infrastructure:
    * Initialize Terraform: `terraform init`
    * Plan the deployment: `terraform plan`
    * Apply the changes: `terraform apply`


### Monitoring
Once the infrastructure is deployed, the configured Dataplex DataScan will initiate based on its schedule (default: on-demand). Data quality results can be monitored and reviewed within the Dataplex console. Additionally, logs are sent to Cloud Logging for further analysis.

### Important Notes
The provided rule templates are examples. You'll likely extend and customize these to suit your specific data quality requirements.
This project assumes a basic understanding of Terraform and Google Dataplex concepts.
