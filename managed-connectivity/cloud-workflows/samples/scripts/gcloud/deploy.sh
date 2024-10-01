# Define Bash Variables (replace with your actual values)
project_id="<your-project>"
region="<your-region>"
service_account="<your-service-account>"
workflow_source="</path/to/your/connector-workflow.yaml>"
workflow_name="connector-workflow"
workflow_args='{"key1": "value1", "key2": "value2"}'

# Create Cloud Workflows Resource
gcloud workflows deploy ${workflow_name} \
  --project=${project_id} \
  --location=${region} \
  --source=${workflow_source} \
  --service-account=${service_account}

# Create Cloud Scheduler Job
gcloud scheduler jobs create http ${workflow_name}-scheduler \
  --project=${project_id} \
  --location=${region} \
  --schedule="0 0 * * *" \
  --time-zone="UTC" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/${project_id}/locations/${region}/workflows/${workflow_name}/executions" \
  --http-method="POST" \
  --oauth-service-account-email=${service_account} \
  --headers="Content-Type=application/json" \
  --message-body='{"argument": ${workflow_args}}'