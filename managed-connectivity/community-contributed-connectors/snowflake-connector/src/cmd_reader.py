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

import argparse
import sys
from src.common.util import loadReferencedFile
from src.common.argument_validator import validateArguments
from src.common.secret_manager import get_secret
from src.common.argument_validator import validateSecretID

def read_args():
    parser = argparse.ArgumentParser()

    # Project arguments for basic generation of metadata entries
    parser.add_argument("--target_project_id", type=str, required=True,
                        help="Google Cloud Project ID metadata entries will be import into")
    parser.add_argument("--target_location_id", type=str, required=True,
                        help="Google Cloud region metadata will be imported into")
    parser.add_argument("--target_entry_group_id", type=str, required=True,
                        help="Dataplex Entry Group ID to import metadata into")
    
    parser.add_argument("--jar", type=str, required=False, help="path to jar file")
    
    # Snowflake specific arguments
    parser.add_argument("--account", type=str, required=True,help="Snowflake account to connect to")
    parser.add_argument("--user", type=str, required=True, help="Snowflake User")
    parser.add_argument("--database", type=str, required=True, help="Snowflake database")
    parser.add_argument("--warehouse", type=str,required=False,help="Snowflake warehouse")
    parser.add_argument("--schema", type=str,required=False,help="Snowflake schema")
    parser.add_argument("--role", type=str,required=False,help="Snowflake Role")

    # Authentication arguments
    parser.add_argument("--authentication",type=str,required=False,choices=['oauth','password','key-pair'],help="Authentication method")
    credentials_group = parser.add_mutually_exclusive_group()
    credentials_group.add_argument("--password_secret", type=str,help="Google Cloud Secret Manager ID for password")
    credentials_group.add_argument("--password_file", type=str,help="Path to file containing password")

    privatekey_option_group = parser.add_mutually_exclusive_group()
    privatekey_option_group.add_argument("--key_secret", type=str,help="Google Cloud Secret Manager ID of the private key")
    privatekey_option_group.add_argument("--key_file", type=str, help="Path to private key file for Key pair authentication")

    parser.add_argument("--passphrase_file",type=str,required=False,help="Path to file containing passphrase for private key file. Can also use environment variable PRIVATE_KEY_PASSPHRASE")
    parser.add_argument("--passphrase_secret",type=str,required=False,help="Google Cloud Secret Manager ID forprivate key passphrase.")
    
    credentials_group.add_argument("--token_file", type=str, help="Path to file containing OAUTH authentication token")

    # Output destination arguments. Generate local only, or local + to Cloud Storage bucket
    output_option_group = parser.add_mutually_exclusive_group()
    output_option_group.add_argument("--local_output_only",action="store_true",help="Output metadata file in local directory only" )
    output_option_group.add_argument("--output_bucket", type=str,help="Cloud Storage bucket for metadata import file. Do not include gs:// prefix")  
    parser.add_argument("--output_folder", type=str, required=False,help="Folder within bucket where generated metadata import file will be written. Specify folder name only")

    parser.add_argument("--min_expected_entries", type=int, required=False,default=-1,help="Minimum number of entries expected in metadata file, if less entries then file gets deleted. Safety mechanism for when using Full Entry Sync metadata jobs")
    
    parsed_args = parser.parse_known_args()[0]

    # Apply common argument validation checks first
    parsed_args = validateArguments(parsed_args)

    # Snowflake specific authentication validation checks
    if parsed_args.authentication == 'oauth' and parsed_args.token is None:
        print("--token must also be supplied if using --authentication oauth")
        sys.exit(1)
    
    if parsed_args.authentication == 'key-pair' and parsed_args.key_secret is None and parsed_args.key_file is None:
        print("either --key_secret or --key_file must be provided if using key-pair authentication")
        sys.exit(1) 
    
    if (parsed_args.authentication is None or parsed_args.authentication == 'password') and (parsed_args.password_secret is None and parsed_args.password_file is None):
        print("--password_secret or --password_file must supplied if using password authentication")
        sys.exit(1)
    
    if parsed_args.key_secret is not None:

        validateSecretID(parsed_args.key_secret)
        try:
            parsed_args.key_secret = get_secret(parsed_args.key_secret)
        except Exception as e:
            print(f"Error retrieving from Secret Manager: {parsed_args.key_secret}")
            raise Exception(e)
    
    if parsed_args.key_file is not None:

        try:
            parsed_args.key_secret = loadReferencedFile(parsed_args.key_file)
        except Exception as e:
            print(f"Error retrieving from file at {parsed_args.key_file}")
            raise Exception(e)
        
    if parsed_args.passphrase_file is not None:

        try:
            parsed_args.key_secret = loadReferencedFile(parsed_args.passphrase_file)
        except Exception as e:
            print(f"Error retrieving from file at {parsed_args.passphrase_file}")
            raise Exception(e)
    
    if parsed_args.passphrase_secret is not None:

        try:
            parsed_args.passphrase_secrett = get_secret(parsed_args.passphrase_secret)
        except Exception as e:
            print(f"Error retrieving from file at {parsed_args.passphrase_secret}")
        raise Exception(e)
    
    return vars(parsed_args)