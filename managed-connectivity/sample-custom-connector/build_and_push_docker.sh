#!/bin/bash

IMAGE=oracle-pyspark:0.0.1
PROJECT=<PROJECT_ID>


REPO_IMAGE=us-central1-docker.pkg.dev/${PROJECT}/docker-repo/oracle-pyspark

docker build -t "${IMAGE}" .

# Tag and push to GCP container registry
gcloud config set project ${PROJECT}
gcloud auth configure-docker us-central1-docker.pkg.dev
docker tag "${IMAGE}" "${REPO_IMAGE}"
docker push "${REPO_IMAGE}"
