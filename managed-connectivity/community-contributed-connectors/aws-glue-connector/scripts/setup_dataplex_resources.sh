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

# 2. Create Aspect Types
echo "----------------------------------------------------------------"
echo "Creating Aspect Types..."

# 2a. Marker Aspect Types (Database, Table, View)
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

# 2b. Lineage Aspect Type
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

# 3. Create Entry Types
# Note: We enforce that each Entry Type requires its corresponding Aspect Type.
echo "----------------------------------------------------------------"

# aws-glue-database
echo "Creating Entry Type: aws-glue-database..."
gcloud dataplex entry-types create "aws-glue-database" \
    --project="$PROJECT_ID" \
    --location="$LOCATION" \
    --required-aspects="type=projects/$PROJECT_ID/locations/$LOCATION/aspectTypes/aws-glue-database" \
    --description="Entry type for aws-glue-database" || echo "Entry Type aws-glue-database might already exist."

# aws-glue-table
echo "Creating Entry Type: aws-glue-table..."
gcloud dataplex entry-types create "aws-glue-table" \
    --project="$PROJECT_ID" \
    --location="$LOCATION" \
    --required-aspects="type=projects/$PROJECT_ID/locations/$LOCATION/aspectTypes/aws-glue-table" \
    --description="Entry type for aws-glue-table" || echo "Entry Type aws-glue-table might already exist."

# aws-glue-view
echo "Creating Entry Type: aws-glue-view..."
gcloud dataplex entry-types create "aws-glue-view" \
    --project="$PROJECT_ID" \
    --location="$LOCATION" \
    --required-aspects="type=projects/$PROJECT_ID/locations/$LOCATION/aspectTypes/aws-glue-view" \
    --description="Entry type for aws-glue-view" || echo "Entry Type aws-glue-view might already exist."

echo "----------------------------------------------------------------"
echo "Setup complete. Please verify resources in the Google Cloud Console."
