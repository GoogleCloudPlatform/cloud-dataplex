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

from google.cloud import dataplex_v1

def createDatascan(gcp_project_id, location_id, datascan_id, datascan):
    """
        Method to create a data quality scan
    """
    try:
        # Create a client
        client = dataplex_v1.DataScanServiceClient()     

        # Initialize request argument(s)
        request = dataplex_v1.CreateDataScanRequest(
            parent=f"projects/{gcp_project_id}/locations/{location_id}",
            data_scan=datascan,
            data_scan_id=datascan_id,
        )
        print(f'Creating Datascan: {datascan_id}')
        # Make the request
        operation = client.create_data_scan(request=request)
        response = operation.result()
        return response
    except Exception as error:
        print(f'Error: Failed to create {datascan_id}. ')
        print(error)
        return None

def getDatascan(gcp_project_id, location_id, datascan_id):
    """
        Method to get the datascan
    """
    try:
        # Create a client
        client = dataplex_v1.DataScanServiceClient()

        # Initialize request argument(s)
        request = dataplex_v1.GetDataScanRequest(
            name=f"projects/{gcp_project_id}/locations/{location_id}/dataScans/{datascan_id}",
        )

        # Make the request
        response = client.get_data_scan(request=request)
        return response
    except Exception as error:
        print(f'Failed to get DataProfile Id - {gcp_project_id}.{location_id}.{datascan_id}')
        print(f'Error: {error}')
        return None

def convertConfigToPayload(config, resource):
    """
        Method to convert a config into payload
    """
 
    # Initialize request argument(s)
    data_scan = dataplex_v1.DataScan()
    data_scan.data = resource

    if 'description' in config:
        data_scan.description = config['description']
    if 'displayName' in config:
        data_scan.display_name = config['displayName']
    if 'labels' in config:
        data_scan.labels = config['labels']
    if 'samplingPercent' in config['dataQualitySpec']:
        data_scan.data_quality_spec.sampling_percent = config['dataQualitySpec']['samplingPercent']
    else:
        data_scan.data_quality_spec.sampling_percent = 10
    data_scan.data_quality_spec.rules = config['dataQualitySpec']['rules']
    if 'rowFilter' in config['dataQualitySpec']:
        data_scan.data_quality_spec.row_filter = config['dataQualitySpec']['rowFilter']
    if 'postScanActions' in config['dataQualitySpec']:
        data_scan.data_quality_spec.post_scan_actions.bigquery_export.results_table = config['dataQualitySpec']['postScanActions']['bigqueryExport']['resultsTable']
    if 'executionSpec' in config and 'trigger' in config['executionSpec'] and 'schedule' in config['executionSpec']['trigger']:
        data_scan.execution_spec.trigger.schedule.cron = config['executionSpec']['trigger']['schedule']['cron']
    else:
       data_scan.execution_spec.trigger.on_demand = {}
    if 'executionSpec' in config and 'incrementalField' in config['executionSpec']:
        data_scan.execution_spec.field = config['executionSpec']['incrementalField']
    return data_scan

def generateDataQualityRules(project_id, location_id, dataprofile_id) -> list:
    """
        Method to get the recommended data quality rules from the existing data profiling scan
    """
    # Create a client
    client = dataplex_v1.DataScanServiceClient()

    # Initialize request argument(s)
    request = dataplex_v1.GenerateDataQualityRulesRequest(
        name=f"projects/{project_id}/locations/{location_id}/dataScans/{dataprofile_id}",
    )

    # Make the request
    response = client.generate_data_quality_rules(request=request)

    if hasattr(response, 'rule'):
        data_quality_rules_list = response.rule
    else:
        data_quality_rules_list = []

    return data_quality_rules_list

def parseResponse(rules) -> list:
    """
        Method to parse the generated data quality rules
    """
    new_list = []
    for rule in rules:
        if rule.set_expectation:
            new_item = {
            'dimension': rule.dimension,
            'column': rule.column,
            'threshold': rule.threshold,
            'ignore_null': rule.ignore_null,
            'set_expectation': rule.set_expectation
            }
        elif rule.row_condition_expectation:
            new_item = {
                'dimension': rule.dimension,
                'column': rule.column,
                'threshold': rule.threshold,
                'row_condition_expectation': rule.row_condition_expectation
            }
        elif rule.table_condition_expectation:
            new_item = {
                'dimension': rule.dimension,
                'column': rule.column,
                'threshold': rule.threshold,
                'table_condition_expectation': rule.table_condition_expectation
            }
        elif rule.sql_assertion:
            new_item = {
                'dimension': rule.dimension,
                'sql_assertion': rule.sql_assertion
            }
        elif rule.regex_expectation:
            new_item = {
                'dimension': rule.dimension,
                'column': rule.column,
                'threshold': rule.threshold,
                'regex_expectation': rule.regex_expectation
            }
        elif rule.statistic_range_expectation:
            new_item = {
                'dimension': rule.dimension,
                'column': rule.column,
                'statistic_range_expectation': rule.statistic_range_expectation 
            }
        elif rule.range_expectation:
            new_item = {
                'dimension': rule.dimension,
                'column': rule.column,
                'threshold': rule.threshold,
                'ignore_null': rule.ignore_null,
                'range_expectation': rule.range_expectation
            }
        elif rule.dimension == 'UNIQUENESS':
            new_item = {
                'dimension': rule.dimension,
                'column': rule.column,
                'threshold': rule.threshold,
                'uniqueness_expectation': {}
            }
        else:
            new_item = {
                'dimension': rule.dimension,
                'column': rule.column,
                'threshold': rule.threshold,
                'non_null_expectation': {}
            }
        new_list.append(new_item)
    return new_list