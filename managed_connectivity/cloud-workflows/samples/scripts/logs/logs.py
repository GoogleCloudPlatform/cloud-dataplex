from urllib.parse import urlencode
from datetime import datetime, timedelta

def build_dataproc_logs_url(project_id, region, metadata_job_id):
    base_url = "https://console.cloud.google.com/logs/query"

    # Calculate timestamp for one hour ago
    one_hour_ago = datetime.utcnow() - timedelta(hours=1)
    timestamp_format = "%Y-%m-%dT%H:%M:%S.%fZ"  # Cloud Logging timestamp format
    one_hour_ago_str = one_hour_ago.strftime(timestamp_format)

    query_params = {
        'project': project_id,
        'query': f'resource.type="dataplex.googleapis.com/MetadataJob" AND resource.labels.location="{region}" AND resource.labels.metadata_job_id="{metadata_job_id}"',
    }

    encoded_params = urlencode(query_params)
    final_url = f"{base_url}?{encoded_params}"
    return final_url

# Example usage
project_id = "your-project" # qc-cloudsql-connector-devproj
region = "your-region" # us-central1
metadata_job_id = "your-metadata-job-id" #

logs_url = build_dataproc_logs_url(project_id, region, metadata_job_id)
print(logs_url)