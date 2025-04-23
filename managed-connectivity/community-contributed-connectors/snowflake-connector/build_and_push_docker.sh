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


# Edit PROJECT_ID and REGION to match your environment
PROJECT_ID=PROJECTID
REGION=us-central1

IMAGE_NAME="universal-catalog-snowflake-pyspark"
IMAGE_VERSION="0.0.1"
IMAGE=${IMAGE_NAME}:${IMAGE_VERSION}

REPO_IMAGE=${REGION}-docker.pkg.dev/${PROJECT_ID}/docker-repo/${IMAGE_NAME}

docker build -t "${IMAGE}" .

# Tag and push to GCP container registry
gcloud config set project ${PROJECT_ID}
gcloud auth configure-docker ${REGION}-docker.pkg.dev
docker tag "${IMAGE}" "${REPO_IMAGE}"
docker push "${REPO_IMAGE}"
