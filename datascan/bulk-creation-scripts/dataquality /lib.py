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

import yaml
from yaml.loader import SafeLoader
import re
import random
import string
from datascan import getDatascan

class LineNumberLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        """
            Method to add line number into config
        """
        mapping = super().construct_mapping(node, deep)
        mapping['__line__'] = node.start_mark.line
        return mapping

def removeLineKeys(config) -> list:
    """
        Method to recursively remove '__line__' keys from config.

        return: config
    """
    if isinstance(config, list):
        return [removeLineKeys(item) for item in config]
    elif isinstance(config, dict):
        return {
            key: removeLineKeys(value)
            for key, value in config.items()
            if key != '__line__'
        }
    else:
        return config

def validateConfigFields(config) -> None:
    """
        Method to validate the Config Fields

        return: None
    """
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
        if not {'projectId', 'locationId', 'bqTable', 'dataQualitySpec'} <= config.keys():
            raise ValueError(
                "Config file must define all the required config fields: "
                "'projectId', 'locationId', 'bqTable', 'dataQualitySpec') at line ",config.get('__line__')
            )
        
        if not 'rules' in config['dataQualitySpec']:
            raise ValueError(
                "Config file must define at least 1 rule for the block at line ",config.get('__line__')
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

def validateCLI(gcp_project_id, location_id, data_profile_ids) -> None:
    """
        Method to valide the CLI Input

        return: None
    """
    #check for all the CLI arguments
    if not gcp_project_id or not location_id or not data_profile_ids:
        raise ValueError(
            "CLI input must define configs using the parameters: "
            "('--gcp_project_id', '--location_id', '--data_profile_ids'). "
        )

    for data_profile in data_profile_ids:
        if not re.match(r'^[a-zA-Z0-9_-]+\.[a-z][a-z0-9-]+[a-z0-9]\.[a-z][a-z0-9-]+[a-z0-9]$', data_profile):
            raise ValueError(
                f"data_profile_id - {data_profile} does not match the expected format 'project_id.location_id.datascan_id'"
            )

    return None

def generateDataScanId() -> str:
    """
        Method to generate the random datascan Id

        return: Datascan ID
    """

    # generate datascan id
    letters_and_digits = string.ascii_lowercase + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for i in range(33))
    datascan_id = f'dq-{random_string}'
    return datascan_id