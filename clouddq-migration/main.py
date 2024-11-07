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
from lib import validate_task
from lib import generate_config
from lib import generate_id
from lib import validateConfigFile
from lib import merge_configs
from dataplex import get_yaml_data
from dataplex import convert_config_to_payload
from dataplex import create_datascan
from dataplex import list_lakes
from dataplex import list_tasks

class ListParamType(click.ParamType):
    name = 'list'

    def convert(self, value, param, ctx):
        try:
            task_ids = value.split(',')
            return task_ids
        except Exception as e:
            self.fail('Could not parse list. Expected format: project_id.location_id.lake_id.task_id,project_id.location_id.lake_id.task_id,...')

@click.command()
@click.option(
    "--gcp_project_id",
    help="GCP Project ID where AutoDQ tasks will be created. ",
    default=None,
    type=str,
)
@click.option(
    "--region_id",
    help="GCP region Id where the AutoDQ tasks will be created. ",
    default="us-central1",
    type=str,
)
@click.option(
    "--source_project",
    help="GCP Project ID where CloudDQ tasks exists. ",
    default=None,
    type=str,
)
@click.option(
    "--source_region",
    help="GCP region Id where CloudDQ tasks exists. ",
    default="None",
    type=str,
)
@click.option(
    "--task_ids",
    help="A list of existing CloudDQ Task Ids. "
    "Expected format: project_id.location_id.lake_id.task_id,project_id.location_id.lake_id.task_id,...",
    default=None,
    type=ListParamType(),
)
@click.option(
    "--config_path",
    help="Users can choose to update the configuration via a YAML file by specifying the file path. ",
    type=click.Path(exists=True),
    default=None,
)
def main(
    gcp_project_id: str,
    region_id: str,
    source_project: str,
    source_region:str,
    task_ids: list,
    config_path: Path,
) -> None:
    if not gcp_project_id or not region_id:
        raise ValueError(
            "CLI input must define the required configs using the parameter: "
            "'--gcp_project_id', '--region_id')."
        )

    if task_ids:
        for task in task_ids:
            print('Validating the task(s)')
            validate_task(task)

            task_fields = task.split('.')
            source_project = task_fields[0]
            source_region = task_fields[1]
            lake_id = task_fields[2]
            task_id = task_fields[3]

            # get data quality yaml spec
            yaml_data, trigger_spec = get_yaml_data(source_project,source_region, lake_id, task_id)

            # generate config
            config = generate_config(yaml_data)

            if trigger_spec.type_ == 2:
                cron = trigger_spec.schedule
                config.update(
                    {
                        'executionSpec': {
                            'trigger': {
                                'schedule': {
                                    'cron': cron
                                }
                            }
                        }
                    }
                )

            # generate payload
            payload = convert_config_to_payload(config)

            # generate AuotoDQ Id
            datascan_id = generate_id()
            print(f'Generated task Id for clouddq task {task_id}: {datascan_id}')

            #  create datascan
            response = create_datascan(
                gcp_project_id,
                region_id,
                datascan_id,
                payload
            )
            if response is not None:
                print(f"{datascan_id} has been created successfully.")

    elif config_path:
        # validate config file
        print(f'Checking the configuration file located at {config_path}')
        config_file = validateConfigFile(config_path)

        for new_config in config_file:
            taskId = new_config['taskId']

            task_fields = taskId.split('.')
            source_project = task_fields[0]
            source_region = task_fields[1]
            lake_id = task_fields[2]
            task_id = task_fields[3]

            # get data quality yaml spec
            yaml_data, trigger_spec = get_yaml_data(source_project,source_region, lake_id, task_id)

            # generate config
            config = generate_config(yaml_data)

            # final config
            final_config = merge_configs(config, new_config)

            if trigger_spec.type_ == 2:
                cron = trigger_spec.schedule
                final_config.update(
                    {
                        'executionSpec': {
                            'trigger': {
                                'schedule': {
                                    'cron': cron
                                }
                            }
                        }
                    }
                )

            # generate payload
            payload = convert_config_to_payload(final_config)

            # generate AuotoDQ Id
            datascan_id = generate_id()
            print(f'Generated task Id for clouddq task {task_id}: {datascan_id}')

            # create datascan
            response = create_datascan(
                gcp_project_id,
                region_id,
                datascan_id,
                payload
            )
            if response is not None:
                print(f"{datascan_id} has been created successfully.")

    else:
        if not source_project or not source_region:
            raise ValueError(
                "CLI input must define the required configs using the parameter: "
                "'--source_project', '--source_region')."
            )

        # get lakes
        lakes = list_lakes(source_project, source_region)
        if lakes:
            for lake in lakes:
                # get tasks
                tasks = list_tasks(source_project, source_region, lake)
                if tasks:
                    for task_id, task_details in tasks.items():
                        # get data quality yaml spec
                        yaml_data, trigger_spec = get_yaml_data(source_project,source_region, lake, task_id)

                        # generate config
                        config = generate_config(yaml_data)

                        if trigger_spec.type_ == 2:
                            cron = trigger_spec.schedule
                            config.update(
                                    {
                                        'executionSpec': {
                                        'trigger': {
                                        'schedule': {
                                            'cron': cron
                                                }}}})
                        payload = convert_config_to_payload(config)

                        # generate AuotoDQ Id
                        datascan_id = generate_id()
                        print(f'Generated task Id for clouddq task {task_id}: {datascan_id}')

                        # create datascan
                        response = create_datascan(
                            gcp_project_id,
                            region_id,
                            datascan_id,
                            payload
                        )
                        if response is not None:
                            print(f"{datascan_id} has been created successfully.")
                else:
                    print(f'No CloudDQ tasks exists in lake: {lake}')
        else:
            print(f'No CloudDQ tasks exists in project: {source_project}')

if __name__ == "__main__":
    main()