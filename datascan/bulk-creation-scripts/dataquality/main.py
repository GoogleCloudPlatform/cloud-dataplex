# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import click
from pathlib import Path
from lib import validateConfigFile
from lib import validateCLI
from lib import generateDataScanId
from datascan import convertConfigToPayload
from datascan import getDatascan
from datascan import createDatascan
from datascan import generateDataQualityRules
from datascan import parseResponse

class ListParamType(click.ParamType):
    name = 'list'

    def convert(self, value, param, ctx):
        try:
            bq_tables = value.split(',')
            return bq_tables
        except Exception as e:
            self.fail('Could not parse list. Expected format: project_id.location_id.datascan_id,project_id.location_id.datascan_id,...')

@click.command()
@click.option(
    "--gcp_project_id",
    help="GCP Project ID where the data quality scans will be created. "
    "If --config_path is provided, this option will be ignored. ",
    default=None,
    type=str,
)
@click.option(
    "--location_id",
    help="GCP region where the data quality scans will be created. "
    "If --config_path is provided, this option will be ignored. ",
    default=None,
    type=str,
)
@click.option(
    "--data_profile_ids", 
    help="A list of existing Data Profile Ids. "
    "Format: project_id.location_id.datascan_id,project_id.location_id.datascan_id,... "
    "If --config_path is provided, this option will be ignored. ",
    type=ListParamType(),
)
@click.option(
    "--config_path",
    help="Users can choose to provide the configuration via a YAML file by specifying the file path. ",
    type=click.Path(exists=True),
    default=None,
)
def main(
    gcp_project_id: str,
    location_id: str,
    data_profile_ids: list,
    config_path: Path,
) -> None:

    if config_path:
        # validate config file
        print(f'Checking the configuration file located at {config_path}')
        configs = validateConfigFile(config_path)

        for config in configs:
            gcp_project_id = config['projectId']
            location_id = config['locationId']
            full_table_name = config['bqTable']
            project_id = full_table_name.split('.')[0]
            dataset_id = full_table_name.split('.')[1]
            table_id = full_table_name.split('.')[2]

            # generate payload
            resource = {
                'resource': f'//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}'
            }
            payload = convertConfigToPayload(config, resource)

            # generate datascan id
            datascan_id = generateDataScanId()
            print(f'Generated DataQuality scan Id for bq table {project_id}.{dataset_id}.{table_id}: {datascan_id}')

            # create datascan
            response = createDatascan(
                gcp_project_id,
                location_id,
                datascan_id,
                payload
            )

            if response is not None:
                    print(f"{datascan_id} has been created successfully.")
                    print(f"LRO ID for {datascan_id}: ",response.name.split('/')[-1])

    else:
        #validate CLI arguments
        print('Checking the CLI arguments')
        validateCLI(gcp_project_id, location_id, data_profile_ids)

        for data_profile_id in data_profile_ids:
            project_id = data_profile_id.split('.')[0]
            region_id = data_profile_id.split('.')[1]
            data_profile_id = data_profile_id.split('.')[2]

            # get datascan
            datascan = getDatascan(project_id, region_id, data_profile_id)

            if not datascan:
                print(f'Skipped Dataprofile Id - {gcp_project_id}.{location_id}.{data_profile_id}')
            else:
                data_quality_rules_list = generateDataQualityRules(project_id, region_id, data_profile_id)
                rules = parseResponse(data_quality_rules_list)
                resource = datascan.data
                config = {'dataQualitySpec': {
                        'rules': rules,
                    }}

                cron = ' '.join(datascan.execution_spec.trigger.schedule.cron.split()[1:])
                # generate payload
                if cron:
                    config.setdefault('executionSpec', {}).setdefault('trigger', {}).setdefault('schedule', {})['cron'] = cron
                payload = convertConfigToPayload(config, resource)

                # generate datascan id
                datascan_id = generateDataScanId()
                print(f'Generated DataQuality scan Id for data_profile_id {data_profile_id}: {datascan_id}')

                # create datascan
                response = createDatascan(
                    gcp_project_id,
                    location_id,
                    datascan_id,
                    payload
                )
                if response is not None:
                    print(f"{datascan_id} has been created successfully.")
                    print(f"LRO ID for {datascan_id}: ",response.name.split('/')[-1])

if __name__ == "__main__":
    main()