project_id="your-project-id" # qc-cloudsql-connector-devproj
region="your-region" # us-central1
metadata_job_id="your-metadata-job-id" # metadatajob-4d1faafb-2c8e-4552-8ba1-f998660b70b8
batch_job_id="your-batch-job-id" # metadataworkflow-967f4370-f648-452a-a492-0d963158a81a

gcloud logging read "resource.type=dataplex.googleapis.com/MetadataJob AND
resource.labels.location=${region} AND 
resource.labels.metadata_job_id=${metadata_job_id}" --freshness=7d


curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "resourceNames": ["projects/${project_id}"],
    "filter": "resource.type=\"dataplex.googleapis.com/MetadataJob\" AND resource.labels.location=\"${region}\"",
    "orderBy": "timestamp desc",
    "pageSize": 10
  }' \
  "https://logging.googleapis.com/v2/entries:list"


curl -X POST \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d '{
    "resourceNames": ["projects/${project_id}"],
    "filter": "resource.type=\"cloud_dataproc_batch\" AND resource.labels.project_id=\"${project_id}\" AND resource.labels.location=\"${region}\" AND resource.labels.batch_id=\"${batch_job_id}\"",
    "orderBy": "timestamp desc",
    "pageSize": 10
  }' \
  "https://logging.googleapis.com/v2/entries:list"
