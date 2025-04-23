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

# Standard validation checks and value replacements. Additional checks can be applied in cmd_reader for specific data sources
def validateArguments(parsed_args):

    if parsed_args.local_output_only == False and (parsed_args.output_bucket is None or parsed_args.output_folder is None):
        print("ERROR: both --output_bucket and --output_folder must be supplied if not using --local_output_only")
        sys.exit(1)

    if not parsed_args.local_output_only and not checkDestination(parsed_args.output_bucket):
            print("Exiting")
            sys.exit(1)     

    if parsed_args.password_secret is not None:
        
        validateSecretID(parsed_args.password_secret)

        try:
            parsed_args.password = get_password(parsed_args.password_secret)
        except Exception as ex:
            print(ex)
            print("Exiting")
            sys.exit(1)
    return parsed_args

def validateSecretID(secretpath : str) -> bool:
    pattern = r"^projects/[^/]+/secrets/[^/]+$"

    if not re.match(pattern, secretpath):
        print(f"ERROR: {secretpath} is not a valid Secret ID. Format is projects/PROJECTID/secrets/SECRETNAME.\nExiting.")
        sys.exit(1)
    return True

# Validates that a value for least one of given list arguments has been supplied
def checkOptionProvided(args : argparse.Namespace, checkParams : list):
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
       print(f"ERROR: Received parameter value '{arg}' but expected true or false")
       print("Exiting")
       sys.exit(1)
