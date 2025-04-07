To use this metadata import file:

1. Search and replace all instances of "gcp-project-id" with your project ID
2. (optional) Search and replace all instances of "us-central1" with your region, or "global"

In metadata_import_request.json replace "the-gcp-project" with your project ID
Ensure all Entry Types and Aspect Types exist in your project

Import via Import REST API with:

curl -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
-H "Content-Type: application/json; charset=utf-8" \
-d @metadata_import_request.json \
"https://dataplex.googleapis.com/v1/projects/the-gcp-project/locations/us-central1/metadataJobs?metadataJobId=a001"