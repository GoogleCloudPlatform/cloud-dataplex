Copyright 2023 Google. This software is provided as-is, without warranty or representation for any use or purpose. Your use of it is subject to your agreement with Google.

# Application Setup Instructions

### Install Python 3.9
Ensure that the python3 command points to the Python 3.9 interpreter. If Python 3.9 is not already installed on your system, follow the appropriate steps to install it. 
### Install Necessary Python Packages
To download the necessary client libraries, install the following packages from pip:

`pip install click==8.1.7`

`pip install google-cloud-dataplex==2.2.1`

`pip install pyyaml==6.0.1`

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
      <td>(string) GCP Project ID where the data quality scans will be created. If config_path is provided, this option will be ignored.</td>
    </tr>
    <tr>
      <td><code>location_id</code></td>
      <td>(string) GCP region where the data quality scans will be created. If config_path is provided, this option will be ignored.</td>
    </tr>
    <tr>
      <td><code>data_profile_ids</code></td>
      <td>(list) A list of existing Data Profile Ids. 
      <br> Format: project_id.location_id.datascan_id,project_id.location_id.datascan_id,...         
        <br>If config_path is provided, this option will be ignored.</td>
    </tr>
    <tr>
      <td><code>config_path</code></td>
      <td>(path) Users can choose to provide the configuration via a YAML file by specifying the file path.</td>
    </tr>
</tbody>
</table>

### Bulk Creation of Data Quality Scans

To create multiple data quality scans using a data quality specification file, execute the following command(see sample yaml file in data-quality folder):

```
python3 main.py --config_path <path_to_your_data_quality_spec_file>
```

For creating multiple data quality scans for existing Data Profile Scans, use the command below:

```
python3 main.py --gcp_project_id <your_gcp_project_id> --location_id <your_gcp_region_id> --data_profile_ids <list_of_data_profile_ids>
```

#### <b>Note</b> 

The list of Data Profile IDs should follow this format:
project_id.location_id.datascan_id,project_id.location_id.datascan_id,...