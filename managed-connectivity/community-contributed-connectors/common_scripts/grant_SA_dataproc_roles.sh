#!/bin/bash
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Grants the required Google Cloud IAM roles to a service account in order to run Dataproc serverless pyspark jobs for custom metadata connectors

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

echo "Finished granting roles to ${SERVICE_ACCOUNT_EMAIL}"
