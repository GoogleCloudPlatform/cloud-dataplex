from google.cloud import dataplex_v1


def createDatascan(gcp_project_id, location_id, datascan_id, datascan):
    """
        Method to create a datascan
    """
    try:
        # Create a client
        client = dataplex_v1.DataScanServiceClient()

        # Initialize request argument
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
        Method to get the list of datascans
    """
    try:
        # Create a client
        client = dataplex_v1.DataScanServiceClient()

        # Initialize request argument
        request = dataplex_v1.GetDataScanRequest(
            name=f"projects/{gcp_project_id}/locations/{location_id}/dataScans/{datascan_id}",
        )

        # Make the request
        operation = client.get_data_scan(request=request)
        return operation
    except Exception as error:
        return None


def convertConfigToPayload(config, project_id, dataset_id, table_id):
    # Initialize request argument(s)
    data_scan = dataplex_v1.DataScan()

    data_scan.data.resource = f'//bigquery.googleapis.com/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}'
    if 'description' in config:
        data_scan.description = config['description']
    if 'displayName' in config:
        data_scan.display_name = config['displayName']
    if 'labels' in config:
        data_scan.labels = config['labels']
    if 'dataProfileSpec' in config:
        if 'samplingPercent' in config['dataProfileSpec']:
            data_scan.data_profile_spec.sampling_percent = config['dataProfileSpec']['samplingPercent']
        if 'rowFilter' in config['dataProfileSpec']:
            data_scan.data_profile_spec.row_filter = config['dataProfileSpec']['rowFilter']
        if 'excludeFields' in config['dataProfileSpec'] and 'fieldNames' in config['dataProfileSpec']['excludeFields']:
            data_scan.data_profile_spec.exclude_fields.field_names = config['dataProfileSpec']['excludeFields'][
                'fieldNames']
        if 'includeFields' in config['dataProfileSpec'] and 'fieldNames' in config['dataProfileSpec']['includeFields']:
            data_scan.data_profile_spec.include_fields.field_names = config['dataProfileSpec']['includeFields'][
                'fieldNames']
        if 'postScanActions' in config['dataProfileSpec'] and 'bigqueryExport' in config['dataProfileSpec'][
            'postScanActions'] and 'resultsTable' in config['dataProfileSpec']['postScanActions']['bigqueryExport']:
            data_scan.data_profile_spec.post_scan_actions.bigquery_export.results_table = \
            config['dataProfileSpec']['postScanActions']['bigqueryExport']['resultsTable']
    else:
        data_scan.data_profile_spec.sampling_percent = 10
    if 'executionSpec' in config and 'trigger' in config['executionSpec'] and 'schedule' in config['executionSpec'][
        'trigger'] and 'cron' in config['executionSpec']['trigger']['schedule']:
        data_scan.execution_spec.trigger.schedule.cron = config['executionSpec']['trigger']['schedule']['cron']
    else:
        data_scan.execution_spec.trigger.on_demand = {}
    if 'executionSpec' in config and 'incrementalField' in config['executionSpec']:
        data_scan.execution_spec.field = config['executionSpec']['incrementalField']
    return data_scan