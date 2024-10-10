workflow_name="WORKFLOW_NAME"
workflow_args='WORKFLOW_ARGUMENTS'

gcloud workflows run "${workflow_name}" --project=${project_id} --location=${location} --data '${workflow_args}'
