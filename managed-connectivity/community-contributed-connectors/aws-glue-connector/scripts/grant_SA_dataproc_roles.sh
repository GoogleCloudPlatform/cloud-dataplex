#!/bin/bash

# Set variables
PROJECT_ID="your-project-id"  # Replace with your Google Cloud project ID
SERVICE_ACCOUNT_EMAIL="your-service-account@your-project-id.iam.gserviceaccount.com" # Replace with your service account email

# Roles to be granted for running Dataplex metadata extract as Dataproc Serveless job 
ROLES=(
  "roles/dataplex.catalogEditor"
  "roles/dataplex.entryGroupOwner"
  "roles/dataplex.metadataJobOwner"
  "roles/dataproc.admin"
  "roles/dataproc.editor"
  "roles/dataproc.worker"
  "roles/iam.serviceAccountUser"
  "roles/logging.logWriter"
  "roles/secretmanager.secretAccessor"
  "roles/workflows.invoker"
)

# Loop through the roles and grant each one
for ROLE in "${ROLES[@]}"; do
  echo "Granting role: $ROLE to service account: $SERVICE_ACCOUNT_EMAIL"

  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:$SERVICE_ACCOUNT_EMAIL" \
    --role="$ROLE"
  
  if [[ $? -eq 0 ]]; then
    echo "Successfully granted $ROLE"
  else
    echo "Error granting $ROLE. Check the gcloud command above for details."
    exit 1 # Exit script with error if any role grant fails.
  fi
done

echo "Finished granting roles."
