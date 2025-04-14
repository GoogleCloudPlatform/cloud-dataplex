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
from src.common.argument_validator import validateArguments
from src.common.argument_validator import true_or_false
from src.common.argument_validator import checkOptionProvided

def read_args():
    """Reads arguments from the command line."""
    parser = argparse.ArgumentParser()

    # Project arguments for basic generation of metadata entries
    parser.add_argument("--target_project_id", type=str, required=True,
                        help="GCP Project ID metadata entries will be import into")
    parser.add_argument("--target_location_id", type=str, required=True,
                        help="GCP region metadata will be imported into")
    parser.add_argument("--target_entry_group_id", type=str, required=True,
                        help="Dataplex Entry Group ID to import metadata into")

    # SQL Server specific arguments
    parser.add_argument("--host", type=str, required=True,
        help="The SQL Server host server")
    parser.add_argument("--port", type=int, required=False,default=1433,
        help="SQL Server port number ")
    parser.add_argument("--user", type=str, required=True, help="SQL Server User")

    parser.add_argument("--jar", type=str, required=False, help="path to JDBC jar file")

    password_option_group = parser.add_mutually_exclusive_group()
    password_option_group.add_argument("--password_secret",type=str,help="Google Cloud Secret Manager ID of the password")
    password_option_group.add_argument("--password",type=str,help="Password. Recommended only for development or testing, use --password_secret instead")
    
    parser.add_argument("--instancename",type=str,required=False,
        help="SQL Server instance to connect to")
    parser.add_argument("--database",type=str,required=True,
        help="SQL Server database")
    parser.add_argument("--login_timeout",type=int,required=False,default=0,
        help="Allowed timeout in seconds to establish connection")
    parser.add_argument("--encrypt", type=true_or_false,required=False,default=True,
        help="Encrypt connection to database")
    parser.add_argument("--trust_server_certificate", type=true_or_false,required=False,default=True,
        help="Trust server TLS certificate for connection")
    parser.add_argument("--hostname_in_certificate",type=str,required=False,
        help="Domain of the host certificate")
    
    parser.add_argument("--ssl", type=true_or_false,required=False,default=True,help="connect with SSL")
    parser.add_argument("--ssl_mode",type=str,required=False,default='prefer',choices=['prefer','require','allow','verify-ca','verify-full'],help="SSL mode requirement")

    # Output destination arguments. Generate metadata file in local directory only, or local + to GCS bucket
    output_option_group = parser.add_mutually_exclusive_group()
    output_option_group.add_argument("--local_output_only",action="store_true",help="Output metadata file in local directory only" )
    output_option_group.add_argument("--output_bucket", type=str,
                        help="Output Cloud Storage bucket for generated metadata import file. Do not include gs:// prefix ")  
    parser.add_argument("--output_folder", type=str, required=False,
                        help="Folder within bucket where generated metadata import file will be written. Name only required")
    
    parser.add_argument("--min_expected_entries", type=int, required=False,default=-1,
                        help="Minimum number of entries expected in metadata file. If less then file not uploaded to GCS. Prevents unintended deletion of entries when using Full Entry Sync metadata jobs")
    
    parsed_args = parser.parse_known_args()[0]

     # Apply common argument validation checks first
    parsed_args = validateArguments(parsed_args)

    if not checkOptionProvided(parsed_args, ["password_secret", "password"]):
        print("Error: Either --password_secret or --password must be provided. Exiting")
        sys.exit(1)

    return vars(parsed_args)
