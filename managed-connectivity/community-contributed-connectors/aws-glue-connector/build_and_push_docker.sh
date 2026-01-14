#!/bin/bash

# Terminate script on error
set -e

# --- Read script arguments ---
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -p|--project_id)
    PROJECT_ID="$2"
    shift # past argument
    shift # past value
    ;;
    -r|--repo)
    REPO="$2"
    shift # past argument
    shift # past value
    ;;
    -i|--image_name)
    IMAGE_NAME="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

# --- Validate arguments ---
if [ -z "$PROJECT_ID" ]; then
    echo "Project ID not provided. Please provide project ID with the -p flag."
    exit 1
fi

if [ -z "$REPO" ]; then
    # Default to gcr.io/[PROJECT_ID] if no repo is provided
    REPO="gcr.io/${PROJECT_ID}"
    echo "Repository not provided, defaulting to: ${REPO}"
fi

if [ -z "$IMAGE_NAME" ]; then
    IMAGE_NAME="aws-glue-to-dataplex-pyspark"
    echo "Image name not provided, defaulting to: ${IMAGE_NAME}"
fi

IMAGE_TAG="latest"
IMAGE_URI="${REPO}/${IMAGE_NAME}:${IMAGE_TAG}"

# --- Build the Docker Image ---
echo "Building Docker image: ${IMAGE_URI}..."
# Use the Dockerfile for PySpark
docker build -t "${IMAGE_URI}" -f Dockerfile.pyspark .

if [ $? -ne 0 ]; then
    echo "Docker build failed."
    exit 1
fi
echo "Docker build successful."

# --- Run the Docker Container ---
echo "Running the PySpark job in a Docker container..."
echo "Using local gcloud credentials for authentication."

# We mount the local gcloud config directory into the container.
# This allows the container to use your Application Default Credentials.
# Make sure you have run 'gcloud auth application-default login' on your machine.
docker run --rm \
    -v ~/.config/gcloud:/root/.config/gcloud \
    "${IMAGE_URI}"

if [ $? -ne 0 ]; then
    echo "Docker run failed."
    exit 1
fi

echo "PySpark job completed successfully."

# --- Optional: Push to Google Container Registry ---
read -p "Do you want to push the image to ${REPO}? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "Pushing image to ${REPO}..."
    gcloud auth configure-docker
    docker push "${IMAGE_URI}"
    echo "Image pushed successfully."
fi

