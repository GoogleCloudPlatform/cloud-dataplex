#!/bin/bash
set -e

# Configuration
# Replace these values with your actual project and location
PROJECT_ID="${PROJECT_ID:-YOUR_PROJECT_ID}"
LOCATION="${LOCATION:-us-central1}"
ENTRY_GROUP_ID="${ENTRY_GROUP_ID:-aws-glue-entries}"

echo "Using Project: $PROJECT_ID"
echo "Using Location: $LOCATION"
echo "Target Entry Group: $ENTRY_GROUP_ID"

# 1. Create Entry Group
echo "----------------------------------------------------------------"
echo "Creating Entry Group: $ENTRY_GROUP_ID..."
gcloud dataplex entry-groups create "$ENTRY_GROUP_ID" \
    --project="$PROJECT_ID" \
    --location="$LOCATION" \
    --description="Entry group for AWS Glue metadata" || echo "Entry Group might already exist."

# 2. Create Entry Types
ENTRY_TYPES=("aws-glue-database" "aws-glue-table" "aws-glue-view")

for TYPE in "${ENTRY_TYPES[@]}"; do
    echo "----------------------------------------------------------------"
    echo "Creating Entry Type: $TYPE..."
    gcloud dataplex entry-types create "$TYPE" \
        --project="$PROJECT_ID" \
        --location="$LOCATION" \
        --description="Entry type for $TYPE" || echo "Entry Type $TYPE might already exist."
done

# 3. Create Aspect Types
echo "----------------------------------------------------------------"
echo "Creating Aspect Types..."

# 3a. Marker Aspect Types (Database, Table, View)
# We define a minimal schema for these markers since they are primarily used for identification.
# Marker schema definition moved into loop below

MARKER_ASPECTS=("aws-glue-database" "aws-glue-table" "aws-glue-view")

for ASPECT in "${MARKER_ASPECTS[@]}"; do
    echo "Creating Aspect Type: $ASPECT..."
    cat > "${ASPECT}.yaml" <<EOF
name: "$ASPECT"
type: record
recordFields:
  - name: description
    type: string
    index: 1
    annotations:
      description: "Optional description for this marker."
    constraints:
        required: false
EOF
    gcloud dataplex aspect-types create "$ASPECT" \
        --project="$PROJECT_ID" \
        --location="$LOCATION" \
        --metadata-template-file-name="${ASPECT}.yaml" || echo "Aspect Type $ASPECT might already exist."
    rm "${ASPECT}.yaml"
done

# 3b. Lineage Aspect Type
# Defines the schema for lineage links (source -> target)
cat > lineage_aspect.yaml <<EOF
name: "aws-lineage-aspect"
type: record
recordFields:
  - name: links
    type: array
    index: 1
    annotations:
      description: "List of lineage links."
    arrayItems:
      name: "link"
      type: record
      recordFields:
        - name: source
          type: record
          index: 1
          annotations:
            description: "Source entity in the lineage relationship."
          recordFields:
            - name: fully_qualified_name
              type: string
              index: 1
              annotations:
                description: "FQN of the source entity."
        - name: target
          type: record
          index: 2
          annotations:
            description: "Target entity in the lineage relationship."
          recordFields:
            - name: fully_qualified_name
              type: string
              index: 1
              annotations:
                description: "FQN of the target entity."
EOF

echo "Creating Aspect Type: aws-lineage-aspect..."
gcloud dataplex aspect-types create "aws-lineage-aspect" \
    --project="$PROJECT_ID" \
    --location="$LOCATION" \
    --metadata-template-file-name=lineage_aspect.yaml || echo "Aspect Type aws-lineage-aspect might already exist."

# Clean up temporary files
rm lineage_aspect.yaml

echo "----------------------------------------------------------------"
echo "Setup complete. Please verify resources in the Google Cloud Console."
