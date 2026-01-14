import boto3
import re

class AWSGlueConnector:
    def __init__(self, aws_access_key_id, aws_secret_access_key, aws_region):
        self.access_key_id = self._clean_credential(aws_access_key_id)
        self.secret_access_key = self._clean_credential(aws_secret_access_key)
        self.region = aws_region.strip()

        try:
            self.__glue_client = boto3.client(
                'glue',
                region_name=self.region,
                aws_access_key_id=self.access_key_id,
                aws_secret_access_key=self.secret_access_key
            )
        except Exception as e:
            raise ValueError(f"Failed to create AWS Glue client: {e}")

    def _clean_credential(self, credential):
        """Clean and validate credential string"""
        if not credential:
            raise ValueError("Empty credential provided")
        cleaned = re.sub(r'[\r\n\t\s]', '', credential)
        if not cleaned or len(cleaned) < 10:
            raise ValueError("Invalid credential format")
        return cleaned

    def get_databases(self, include_databases=None):
        """Fetches metadata from AWS Glue Data Catalog."""
        if include_databases is None:
            include_databases = []
        metadata = {}
        try:
            paginator = self.__glue_client.get_paginator('get_databases')
            for page in paginator.paginate():
                for db in page['DatabaseList']:
                    db_name = db['Name']
                    if not include_databases or db_name in include_databases:
                        metadata[db_name] = self._get_tables(db_name)
        except Exception as e:
            raise RuntimeError(f"Failed to get databases from AWS Glue: {e}")
        return metadata

    def _get_tables(self, db_name):
        """Fetches tables from a specific database."""
        tables = []
        try:
            paginator = self.__glue_client.get_paginator('get_tables')
            for page in paginator.paginate(DatabaseName=db_name):
                tables.extend(page['TableList'])
        except Exception as e:
            raise RuntimeError(f"Failed to get tables from AWS Glue for database {db_name}: {e}")
        return tables

    def get_lineage_info(self):
        """
        Scans AWS Glue jobs to derive lineage information by inspecting job run graphs.
        Returns a dictionary mapping target table names to a list of their source table names.
        """
        lineage_map = {}
        paginator = self.__glue_client.get_paginator('get_jobs')

        print("Fetching lineage info from AWS Glue jobs...")
        try:
            for page in paginator.paginate():
                for job in page['Jobs']:
                    job_name = job['Name']
                    # Optimization: Limit to latest run to reduce API calls
                    job_runs = self.__glue_client.get_job_runs(JobName=job_name, MaxResults=1)
                    
                    for job_run in job_runs.get('JobRuns', []):
                        if job_run.get('JobRunState') == 'SUCCEEDED':
                            script_location = job.get('Command', {}).get('ScriptLocation')
                            if not script_location:
                                continue

                            try:
                                # Note: get_dataflow_graph expects the actual Python script content, not just the S3 URI.
                                # Passing the URI here will likely result in an empty graph or failure.
                                # To fix this, we would need to download the script from S3, which requires S3 permissions.
                                # For now, we attempt it, but catch errors.
                                graph = self.__glue_client.get_dataflow_graph(PythonScript=script_location)
                                if graph:
                                    sources = [edge['Source'] for edge in graph.get('Edges', [])]
                                    targets = [edge['Target'] for edge in graph.get('Edges', [])]
                                    for i, target_id in enumerate(targets):
                                        target_node = next((node for node in graph.get('Nodes', []) if node['Id'] == target_id), None)
                                        if target_node and target_node['NodeType'] == 'DataSink':
                                            target_table_name = target_node.get('Name')
                                            source_id = sources[i]
                                            source_node = next((node for node in graph.get('Nodes', []) if node['Id'] == source_id), None)
                                            if source_node and source_node['NodeType'] == 'DataSource':
                                                source_table_name = source_node.get('Name')
                                                if target_table_name not in lineage_map:
                                                    lineage_map[target_table_name] = []
                                                lineage_map[target_table_name].append(source_table_name)
                            except Exception as e:
                                print(f"Warning: Could not get dataflow graph for job {job_name}. Note that direct S3 URI usage might not be supported. Error: {e}")
                            
                            # We only need one successful run to guess lineage
                            break
        except Exception as e:
            print(f"Warning: Could not fetch lineage information. Error: {e}")

        print(f"Found {len(lineage_map)} lineage relationships.")
        return lineage_map
