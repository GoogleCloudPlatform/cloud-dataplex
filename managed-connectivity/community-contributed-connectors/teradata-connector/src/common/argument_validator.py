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
from typing import Optional
import argparse
import re
import logging

GCP_REGIONS = ['asia-east1', 'asia-east2', 'asia-northeast1', 'asia-northeast2', 'asia-northeast3', 'asia-south1', 'asia-south2', 'asia-southeast1', 'asia-southeast2', 'australia-southeast1', 'australia-southeast2', 'europe-central2', 'europe-north1', 'europe-southwest1', 'europe-west1', 'europe-west2', 'europe-west3',
               'europe-west4', 'europe-west6', 'europe-west8', 'europe-west9', 'europe-west12', 'me-central1', 'me-west1', 'northamerica-northeast1', 'northamerica-northeast2', 'southamerica-east1', 'southamerica-east2', 'us-central1', 'us-east1', 'us-east4', 'us-east5', 'us-south1', 'us-west1', 'us-west2', 'us-west3', 'us-west4']

DEFAULT_QUERY_BAND_ORG = "teradata-internal-telem"
DEFAULT_QUERY_BAND_APPNAME = "teradata-dataplex-connector"
DEFAULT_QUERY_BAND = f"org={DEFAULT_QUERY_BAND_ORG};appname={DEFAULT_QUERY_BAND_APPNAME};"
MAX_QUERY_BAND_LENGTH = 2048
QUERY_BAND_RESERVED_NAMES = {"proxyuser", "proxyrole"}

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


def validateQueryBand(query_band: Optional[str]) -> str:
    """Validate and normalize a Teradata query band string.

    Returns a normalized query band with org and appname enforced.
    Returns DEFAULT_QUERY_BAND if input is None or empty.
    """
    if query_band is None or query_band.strip() == "":
        return DEFAULT_QUERY_BAND

    # Whitelist allowed characters to prevent SQL injection via SET QUERY_BAND
    allowed_pattern = r"^[A-Za-z0-9\-_\.=;, ]+$"
    if not re.match(allowed_pattern, query_band):
        raise SystemExit(
            f"Error: invalid --query_band value '{query_band}'. "
            "Must contain only letters, numbers, hyphens, underscores, "
            "dots, equals, semicolons, commas, and spaces."
        )

    # Normalize: trim whitespace and ensure trailing semicolon
    query_band = query_band.strip()
    if not query_band.endswith(";"):
        query_band += ";"

    # Validate format and parse key-value pairs preserving order
    pairs = {}
    order = []
    for part in query_band.split(";"):
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            raise SystemExit(
                f"Error: --query_band has malformed segment '{part}'. "
                "Expected format: name=value;"
            )
        key, value = part.split("=", 1)
        key = key.strip().lower()
        value = value.strip()
        if not key:
            raise SystemExit(
                f"Error: --query_band has segment with empty key: '{part}'."
            )
        if key not in pairs:
            order.append(key)
        pairs[key] = value

    # Reject Teradata reserved names
    for key in pairs:
        if key in QUERY_BAND_RESERVED_NAMES:
            raise SystemExit(
                f"Error: --query_band contains reserved name '{key}'. "
                "PROXYUSER and PROXYROLE require Trusted Session privileges and are not allowed."
            )

    # Enforce org
    if "org" not in pairs or not pairs["org"]:
        pairs["org"] = DEFAULT_QUERY_BAND_ORG

    # Enforce appname
    if "appname" not in pairs or not pairs["appname"]:
        pairs["appname"] = DEFAULT_QUERY_BAND_APPNAME
    elif not pairs["appname"].endswith(DEFAULT_QUERY_BAND_APPNAME):
        pairs["appname"] = f"{pairs['appname']}_{DEFAULT_QUERY_BAND_APPNAME}"

    # Build final query band with required ordering: org → appname → rest
    remaining = [k for k in order if k not in ("org", "appname")]
    final_order = ["org", "appname"] + remaining

    result = "".join(f"{k}={pairs[k]};" for k in final_order)

    # Enforce maximum length
    if len(result) > MAX_QUERY_BAND_LENGTH:
        raise SystemExit(
            f"Error: --query_band exceeds maximum length of {MAX_QUERY_BAND_LENGTH} characters "
            f"(current: {len(result)}). Reduce the number or length of key-value pairs."
        )

    return result


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
