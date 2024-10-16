# Define Bash variables (replace with your actual values)
project_id="PROJECT_ID"
region="LOCATION_ID"
service_account="SERVICE_ACCOUNT_ID"
workflow_source="WORKFLOW_DEFINITION_FILE.yaml"
workflow_name="WORKFLOW_NAME"
workflow_args='WORKFLOW_ARGUMENTS'

# Create Workflows resource
gcloud workflows deploy ${workflow_name} \
  --project=${project_id} \
  --location=${region} \
  --source=${workflow_source} \
  --service-account=${service_account}

# Create Cloud Scheduler job
gcloud scheduler jobs create http ${workflow_name}-scheduler \
  --project=${project_id} \
  --location=${region} \
  --schedule="CRON_SCHEDULE_EXPRESSION" \
  --time-zone="UTC" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/${project_id}/locations/${region}/workflows/${workflow_name}/executions" \
  --http-method="POST" \
  --oauth-service-account-email=${service_account} \
  --headers="Content-Type=application/json" \
  --message-body='{"argument": ${workflow_args}}'
