import yaml
from yaml.loader import SafeLoader
import re


class LineNumberLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        """
            Method to add line number into config
        """
        mapping = super().construct_mapping(node, deep)
        mapping['__line__'] = node.start_mark.line
        return mapping


def removeLineKeys(config):
    """
        Method to recursively remove '__line__' keys from config.

        return: config
    """
    if not isinstance(config, dict):
        return config
    return {
        key: removeLineKeys(value)
        for key, value in config.items()
        if key != '__line__'
    }


def validateConfigFields(config) -> None:
    if isinstance(config, dict):
        for key, value in config.items():
            if value is None:
                raise ValueError(f"Field '{key}' is None for the block at line {config.get('__line__')}")
            validateConfigFields(value)
    elif isinstance(config, list):
        for item in config:
            validateConfigFields(item)


def validateConfigFile(config_path) -> list:
    """
        Method to valide the Config File

        return: configs
    """
    # load the config file
    with open(config_path, 'r') as f:
        config_file = list(yaml.load_all(f, Loader=LineNumberLoader))

    # validate the config file
    for config in config_file:
        if not {'projectId', 'locationId', 'bqTable'} <= config.keys():
            raise ValueError(
                "Config file must define all the required configs: "
                "'projectId', 'locationId', 'bqTable') at line ", config.get('__line__')
            )

        # validate format for bqTable
        full_table_name = config['bqTable']
        if not re.match(r'^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$', full_table_name):
            raise ValueError(
                f"bqTable - {full_table_name} does not match the expected format 'project_id.dataset_id.table_id'"
                "at line ", config.get('__line__')
            )

        # validate nested fields
        validateConfigFields(config)

    configs = [removeLineKeys(config) for config in config_file]
    return configs


def validateCLI(gcp_project_id, location_id, bq_tables) -> None:
    """
        Method to valide the CLI Input

        return: None
    """
    # check for all the CLI arguments
    if not gcp_project_id or not location_id or not bq_tables:
        raise ValueError(
            "CLI input must define configs using the parameters: "
            "('--gcp_project_id', '--location_id', '--bq_tables'). "
        )

    for full_table_name in bq_tables:
        if not re.match(r'^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$', full_table_name):
            raise ValueError(
                f"bqTable - {full_table_name} does not match the expected format 'project_id.dataset_id.table_id'"
            )
    return None


def generateDataScanId(table_id) -> str:
    """
        Method to generate the Datascan Id

        return: Datascan ID
    """

    # generate datascan id
    datascan_id = f'dp-{table_id}'.replace('_', '-')

    return datascan_id