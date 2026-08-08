#!/bin/bash
# Copyright 2026 Teradata
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

# Setup script for Teradata Dataplex connector.
# Creates the required Entry Group, Aspect Types, and Entry Types
# in Dataplex Universal Catalog before metadata can be imported.
#
# Usage:
#   PROJECT_ID=my-project LOCATION=us-central1 ./setup_dataplex_resources.sh
#   PROJECT_ID=my-project LOCATION=us-central1 ENTRY_GROUP_ID=my-group ./setup_dataplex_resources.sh

set -e

# Configuration
PROJECT_ID="${PROJECT_ID:-YOUR_PROJECT_ID}"
LOCATION="${LOCATION:-us-central1}"
ENTRY_GROUP_ID="${ENTRY_GROUP_ID:-teradata}"

echo "Using Project: $PROJECT_ID"
echo "Using Location: $LOCATION"
echo "Target Entry Group: $ENTRY_GROUP_ID"

# 1. Create Entry Group
echo "----------------------------------------------------------------"
echo "Creating Entry Group: $ENTRY_GROUP_ID..."
gcloud dataplex entry-groups create "$ENTRY_GROUP_ID" \
    --project="$PROJECT_ID" \
    --location="$LOCATION" \
    --description="Entry group for Teradata metadata" || echo "Entry Group might already exist."

# 2. Create Aspect Types
echo "----------------------------------------------------------------"
echo "Creating Aspect Types..."

# Create metadata template file (JSON format)
cat > template.json <<EOF
{"name":"marker","type":"record","recordFields":[{"name":"description","type":"string","index":1,"constraints":{"required":false}}]}
EOF

ASPECT_TYPES=("teradata-instance" "teradata-database" "teradata-schema" "teradata-table" "teradata-view")
DISPLAY_NAMES=("Teradata Instance" "Teradata Database" "Teradata Schema" "Teradata Table" "Teradata View")

for i in "${!ASPECT_TYPES[@]}"; do
    ASPECT="${ASPECT_TYPES[$i]}"
    DISPLAY="${DISPLAY_NAMES[$i]}"
    echo "Creating Aspect Type: $ASPECT..."
    gcloud dataplex aspect-types create "$ASPECT" \
        --project="$PROJECT_ID" \
        --location="$LOCATION" \
        --display-name="$DISPLAY" \
        --metadata-template-file-name=template.json || echo "Aspect Type $ASPECT might already exist."
done

rm template.json

# 3. Create Entry Types
echo "----------------------------------------------------------------"
echo "Creating Entry Types..."

# Entry Types for instance, database, schema (single required aspect each)
ENTRY_TYPES=("teradata-instance" "teradata-database" "teradata-schema")
ENTRY_DISPLAY=("Teradata Instance" "Teradata Database" "Teradata Schema")

for i in "${!ENTRY_TYPES[@]}"; do
    TYPE="${ENTRY_TYPES[$i]}"
    DISPLAY="${ENTRY_DISPLAY[$i]}"
    echo "Creating Entry Type: $TYPE..."
    gcloud dataplex entry-types create "$TYPE" \
        --project="$PROJECT_ID" \
        --location="$LOCATION" \
        --display-name="$DISPLAY" \
        --required-aspects="type=projects/$PROJECT_ID/locations/$LOCATION/aspectTypes/$TYPE" || echo "Entry Type $TYPE might already exist."
done

# Entry Types for table, view (require both custom aspect and global schema)
SCHEMA_TYPES=("teradata-table" "teradata-view")
SCHEMA_DISPLAY=("Teradata Table" "Teradata View")

for i in "${!SCHEMA_TYPES[@]}"; do
    TYPE="${SCHEMA_TYPES[$i]}"
    DISPLAY="${SCHEMA_DISPLAY[$i]}"
    echo "Creating Entry Type: $TYPE..."
    gcloud dataplex entry-types create "$TYPE" \
        --project="$PROJECT_ID" \
        --location="$LOCATION" \
        --display-name="$DISPLAY" \
        --required-aspects="type=projects/$PROJECT_ID/locations/$LOCATION/aspectTypes/$TYPE" \
        --required-aspects="type=projects/dataplex-types/locations/global/aspectTypes/schema" || echo "Entry Type $TYPE might already exist."
done

echo "----------------------------------------------------------------"
echo "Setup complete. Please verify resources in the Google Cloud Console."
