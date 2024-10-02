workflow_name="connector-workflow"
workflow_args='{"key1": "value1", "key2": "value2"}'

gcloud workflows run "${workflow_name}" --project=${project_id} --location=${location} --data '${workflow_args}'