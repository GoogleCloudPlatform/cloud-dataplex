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
from src.common.argument_validator import true_or_false

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

    # Mysql specific arguments
    parser.add_argument("--host", type=str, required=True,
                        help="Mysql host server")
    parser.add_argument("--port", type=str, required=True,
                        help="The port number (usually 3306)")
    parser.add_argument("--database", type=str, required=True,
                        help="MySQL database to connect to")
    parser.add_argument("--user", type=str, required=True, help="Mysql User")
    parser.add_argument("--password_secret", type=str, required=True,
                        help="Google Secrets Manager ID for MySQL password")
    
    parser.add_argument("--jar", type=str, required=False, help="path to jar file")
    
    parser.add_argument("--use_ssl", type=true_or_false,required=False,default=True,help="connect with SSL")
    parser.add_argument("--ssl_mode", type=str, required=False,choices=['prefer','require','allow','verify-ca','verify-full'],default='prefer',help="SSL mode requirement")
    
    # Output destination arguments. Generate local only, or local + to GCS bucket
    output_option_group = parser.add_mutually_exclusive_group()
    output_option_group.add_argument("--local_output_only",action="store_true",help="Output metadata file in local directory only" )
    output_option_group.add_argument("--output_bucket", type=str,
                        help="Destination Google Cloud Storage bucket for generated metadata import file. Do not include gs:// prefix ")  
    parser.add_argument("--output_folder", type=str, required=False,
                        help="Folder within bucket where generated metadata import file will be written. Name only required")
    
    parser.add_argument("--min_expected_entries", type=int, required=False,default=-1,
                        help="Minimum number of entries expected in metadata file, if less entries then file gets deleted. Saftey mechanism for when using Full Entry Sync metadata jobs")
    
    parsed_args = parser.parse_known_args()[0]

    # Apply common argument validation checks first
    parsed_args = validateArguments(parsed_args)

    return vars(parsed_args)
