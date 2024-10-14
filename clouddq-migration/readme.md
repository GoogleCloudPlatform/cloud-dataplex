Copyright 2023 Google. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreement with Google.

# Application Setup Instructions

### Install Python 3.9
Ensure that the python3 command points to the Python 3.9 interpreter. If Python 3.9 is not already installed on your system, follow the appropriate steps to install it.
### Install Necessary Python Packages
To download the necessary client libraries, install the following packages from pip:

`pip install click==8.1.7`

`pip install google-cloud-dataplex==2.2.1`

`pip install pyyaml==6.0.1`

`pip install google-cloud-storage==2.18.2`

### Input Arguments Description

The table below lists the input arguments:

<table>
  <thead>
    <tr>
      <th>Field</th>
      <th>Description</th>
    </tr>
  </thead>
<tbody>
    <tr>
      <td><code>gcp_project_id</code></td>
      <td>(string) GCP Project ID where AutoDQ tasks will be created. </td>
    </tr>
    <tr>
      <td><code>source_project</code></td>
      <td>(string)GCP Project ID where CloudDQ tasks exists. If config_path or task_ids is provided, this option will be ignored. </td>
    </tr>
    <tr>
      <td><code>source_region</code></td>
      <td>(string)GCP region Id where CloudDQ tasks exists.  If config_path or task_ids is provided, this option will be ignored. </td>
    </tr>
    <tr>
      <td><code>task_ids</code></td>
      <td>(list) A list of existing CloudDQ Task Ids.  
      <br> Expected format: project_id.location_id.lake_id.task_id,project_id.location_id.lake_id.task_id,...         
    </td>
    </tr>
    <tr>
      <td><code>config_path</code></td>
      <td>(path) Users can choose to provide the configuration via a YAML file by specifying the file path.</td>
    </tr>
</tbody>
</table>

### Bulk Creation of Data Quality Scans

To migrate the existing cloudDQ tasks into autoDQ tasks, use the command below:

```
python3 main.py --gcp_project_id <your_gcp_project_id> --location_id <your_gcp_region_id> --task_ids <list_of_task_ids>
```

To migrate the existing cloudDQ tasks into autoDQ tasks using a specification file, execute the following command(see sample yaml file in clouddq-migration folder):

```
python3 main.py --config_path <path_to_your_data_quality_spec_file>
```

To migrate all the existing cloudDQ tasks from the project into autoDQ tasks, use the command below:

```
python3 main.py --gcp_project_id <your_gcp_project_id> --region_id <your_gcp_region_id> --source_project <gcp_source_project> --source_region <gcp_source_region>
```


#### <b>Note</b>

The list of CloudDQ task IDs should follow this format:
project_id.location_id.lake_id.task_id,project_id.location_id.lake_id.task_id,...