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

from src.common.gcs_uploader import checkDestination
from src.common.secret_manager import get_password
import argparse
import sys
import re
import logging

GCP_REGIONS = ['asia-east1', 'asia-east2', 'asia-northeast1', 'asia-northeast2', 'asia-northeast3', 'asia-south1', 'asia-south2', 'asia-southeast1', 'asia-southeast2', 'australia-southeast1', 'australia-southeast2', 'europe-central2', 'europe-north1', 'europe-southwest1', 'europe-west1', 'europe-west2', 'europe-west3',
               'europe-west4', 'europe-west6', 'europe-west8', 'europe-west9', 'europe-west12', 'me-central1', 'me-west1', 'northamerica-northeast1', 'northamerica-northeast2', 'southamerica-east1', 'southamerica-east2', 'us-central1', 'us-east1', 'us-east4', 'us-east5', 'us-south1', 'us-west1', 'us-west2', 'us-west3', 'us-west4']

# Standard validation checks and value replacements. Additional checks can be applied in cmd_reader for specific data sources
def validateArguments(parsed_args):

    if parsed_args.local_output_only == False and (parsed_args.output_bucket is None or parsed_args.output_folder is None):
        raise Exception("both --output_bucket and --output_folder must be supplied if not using --local_output_only")

    if not parsed_args.local_output_only and not checkDestination(parsed_args.output_bucket):
        raise Exception(f"--output_bucket {parsed_args.output_bucket} is not valid")

    if parsed_args.target_location_id not in (GCP_REGIONS + ['global']):
        raise Exception(f"--target_location_id must be valid google cloud region or 'global' : {parsed_args.target_location_id}")

    if parsed_args.password_secret is not None:

        validateSecretID(parsed_args.password_secret)

        parsed_args.password = get_password(parsed_args.password_secret)

    return parsed_args


def validateSecretID(secretpath: str) -> bool:
    pattern = r"^projects/[^/]+/secrets/[^/]+$"

    if not re.match(pattern, secretpath):
        raise Exception(f"{secretpath} is not a valid Secret ID. Format is projects/PROJECTID/secrets/SECRETNAME.\nExiting.")
    return True

# Validates that a value for least one of given list arguments has been supplied
def checkOptionProvided(args: argparse.Namespace, checkParams: list):
    provided = False
    for arg in checkParams:
        if args.__contains__(arg) and getattr(args, arg) is not None:
            return True
    return False

# true/false argument type
def true_or_false(arg):
    ua = str(arg).upper()
    if 'TRUE'.startswith(ua):
        return True
    elif 'FALSE'.startswith(ua):
        return False
    else:
        logging.fatal(
            f"Received parameter value '{arg}' but expected true or false")
