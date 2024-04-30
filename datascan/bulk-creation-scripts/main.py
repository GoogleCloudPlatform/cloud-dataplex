import click
from pathlib import Path

from datascan import convertConfigToPayload
from datascan import getDatascan
from datascan import createDatascan
from lib import validateConfigFile
from lib import validateCLI
from lib import generateDataScanId

class ListParamType(click.ParamType):
    name = 'list'

    def convert(self, value, param, ctx):
        try:
            bq_tables = value.split(',')
            return bq_tables
        except Exception as e:
            self.fail('Could not parse list. Expected format: project_name.dataset_name.table_name,project_name.dataset_name.table_name,...')

@click.command()
@click.option(
    "--gcp_project_id",
    help="GCP Project ID where the data profile scans will be created. "
    "If --config_path is provided, this option will be ignored. ",
    default=None,
    type=str,
)
@click.option(
    "--location_id",
    help="GCP region where the data profile scans will be created. "
    "If --config_path is provided, this option will be ignored. ",
    default=None,
    type=str,
)
@click.option(
    "--bq_tables",
    help="A list of BQ tables. Each BQ table name should be unique; otherwise, an error will occur while creating the datascan with the same BQ table name."
    "Format: project_name.dataset_name.table_name,project_name.dataset_name.table_name,... "
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
    bq_tables: list,
    config_path: Path,
) -> None:

    if config_path:
        # validate the config file
        print(f'Checking the configuration file located at {config_path}')
        configs = validateConfigFile(config_path)

        for config in configs:
            gcp_project_id = config['projectId']
            location_id = config['locationId']
            full_table_name = config['bqTable']
            project_id = full_table_name.split('.')[0]
            dataset_id = full_table_name.split('.')[1]
            table_id = full_table_name.split('.')[2]

            # generate datascan id
            datascan_id = generateDataScanId(table_id)

            # check if datascan id already exists
            datascan = getDatascan(gcp_project_id, location_id, datascan_id)
            if datascan:
                print(f'{datascan_id} resource already exists. ')
            else:
                # generate payload
                payload = convertConfigToPayload(config, project_id, dataset_id, table_id)

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
        validateCLI(gcp_project_id, location_id, bq_tables)

        for full_table_name in bq_tables:
            project_id = full_table_name.split('.')[0]
            dataset_id = full_table_name.split('.')[1]
            table_id = full_table_name.split('.')[2]

            # generate datascan id
            datascan_id = generateDataScanId(table_id)

            # check if datascan id already exists
            datascan = getDatascan(gcp_project_id, location_id, datascan_id)
            if datascan:
                print(f'{datascan_id} resource already exists. ')
            else:
                # generate payload
                config = []
                payload = convertConfigToPayload(config, project_id, dataset_id, table_id)

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