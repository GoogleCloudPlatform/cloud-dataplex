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

import re
import random
import string
import yaml
from yaml.loader import SafeLoader

class LineNumberLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        """
            Method to add line number into config
        """
        mapping = super().construct_mapping(node, deep)
        mapping['__line__'] = node.start_mark.line
        return mapping

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
        if not {'taskId'} <= config.keys():
            raise ValueError(
                "Config file must define the required config field: "
                "'taskId' at line ",config.get('__line__')
            )

        # validate format for taskId
        task = config['taskId']
        validate_task(task)

        # validate nested fields
        validateConfigFields(config)
    configs = [removeLineKeys(config) for config in config_file]
    return configs

def merge_configs(config, new_config) -> dict:
    """"
        Method to merge configs

        return: final_config
    """
    # Copy original config to avoid modifying it directly
    final_config = config.copy()

    # Update the final config with the new config
    for key, value in new_config.items():
        if isinstance(value, dict) and key in final_config:
            # Recursively update dictionary if the key exists and both are dictionaries
            final_config[key] = merge_configs(final_config.get(key, {}), value)
        else:
            # Otherwise, update or add the key
            final_config[key] = value

    return final_config

def validate_task(task)-> None:
    """
        Method to validate the format for tasks ids
    """
    if not re.match(r'^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$', task):
        raise ValueError(
            f"Error: {task} does not match the expected format 'project_id.location_id.lake_id.task_id'")

def generate_config(yaml_data) -> dict:
    '''
        Method to generate the config of the task
    '''
    config = {
        'dataQualitySpec': {
            'rules': []
        }
    }
    rule_bindings = yaml_data.get('rule_bindings')
    entities = yaml_data.get('entities')
    rules = yaml_data.get('rules')
    row_filters = yaml_data.get('row_filters')
    if not rule_bindings:
        print('No rule binding Id present to migrate. Skipped')
    else:
        for rule_binding in rule_bindings:
            column_id = rule_bindings[rule_binding]['column_id']
            if 'entity_id' in rule_bindings[rule_binding]:
                entity_id = rule_bindings[rule_binding]['entity_id']
                project_id = entities[entity_id]['project_name']
                dataset_id = entities[entity_id]['dataset_name']
                table_id = entities[entity_id]['table_name']
                if column_id in entities[entity_id]['columns'].keys():
                    column = entities[entity_id]['columns'][column_id]['name']
                else:
                    column_id = column_id.upper()
                    column = entities[entity_id]['columns'][column_id]['name']
                config['resource'] =  f'//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}'
            else:
                entity_uri = rule_bindings[rule_binding]['entity_uri']
                if entity_uri.startswith('dataplex'):
                    print('Does not support enitity source type. ')
                    continue
                else:
                    entity = entity_uri.split('/')
                    project_id = entity[3]
                    dataset_id = entity[5]
                    table_id = entity[7]
                    column = column_id
                    config['resource'] =  f'//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}'
            rule_ids = rule_bindings[rule_binding]['rule_ids']
            for rule_id in rule_ids:
                if isinstance(rule_id, str):
                    if rules[rule_id]['rule_type'] == 'NOT_NULL':
                        config['dataQualitySpec']['rules'].append({
                            'dimension': 'COMPLETENESS',
                            'column': column,
                            'non_null_expectation': {},
                        })
                    elif rules[rule_id]['rule_type'] == 'REGEX':
                        config['dataQualitySpec']['rules'].append({
                            'dimension': 'ACCURACY',
                            'column': column,
                            'regex_expectation': {
                                "regex": rules[rule_id]['params']['pattern']
                            },
                        })
                    else:
                        custom_sql_expr = f'NOT($column = "") '
                        custom_sql_expr = custom_sql_expr.replace(f"$column", column)
                        config['dataQualitySpec']['rules'].append({
                            'dimension': 'COMPLETENESS',
                            'column': column,
                            'row_condition_expectation': {
                                "sql_expression": custom_sql_expr
                            },
                        })
                else:
                    for key, value in rule_id.items():
                        rule = key
                        params = value

                    if rules[rule]['rule_type'] == 'CUSTOM_SQL_EXPR':
                        custom_sql_expr = rules[rule]['params']['custom_sql_expr']
                        for key, value in params.items():
                            custom_sql_expr = custom_sql_expr.replace(f"${key}", str(value))
                        custom_sql_expr = custom_sql_expr.replace(f"$column", column)
                        config['dataQualitySpec']['rules'].append({
                            'dimension': 'ACCURACY',
                            'column': column,
                            'row_condition_expectation': {
                                "sql_expression": custom_sql_expr
                            },
                        })
                    else:

                        custom_sql_statement = rules[rule]['params']['custom_sql_statement']

                        for key, value in params.items():
                            custom_sql_statement = custom_sql_statement.replace(f"${key}", str(value))
                        custom_sql_statement = custom_sql_statement.replace(f"$column", column)
                        custom_sql_statement = custom_sql_statement.replace(f"data", f'{project_id}.{dataset_id}.{table_id}')

                        config['dataQualitySpec']['rules'].append({
                            'dimension': 'ACCURACY',
                            'column': column,
                            'sql_assertion': {
                                "sql_statement": custom_sql_statement
                            },
                        })

    return config

def generate_id() -> str:
    '''
        Method to generate the autoDQ task id
    '''
    # generate datascan id
    letters_and_digits = string.ascii_lowercase + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for i in range(28))
    datascan_id = f'auto-dq-{random_string}'
    return datascan_id