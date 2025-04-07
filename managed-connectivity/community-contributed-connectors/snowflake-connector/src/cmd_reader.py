import argparse
import sys
from src.common.util import loadReferencedFile
from src.common.gcs_uploader import checkDestination
from src.common.secret_manager import get_password

def read_args():
    parser = argparse.ArgumentParser()

    # Project arguments for basic generation of metadata entries
    parser.add_argument("--target_project_id", type=str, required=True,
                        help="GCP Project ID metadata entries will be import into")
    parser.add_argument("--target_location_id", type=str, required=True,
                        help="GCP region metadata will be imported into")
    parser.add_argument("--target_entry_group_id", type=str, required=True,
                        help="Dataplex Entry Group ID to import metadata into")
    
    parser.add_argument("--jar", type=str, required=False, help="path to jar file")
    
    # Snowflake specific arguments
    parser.add_argument("--account", type=str, required=True,help="Snowflake account to connect to")
    parser.add_argument("--user", type=str, required=True, help="Snowflake User")
    parser.add_argument("--database", type=str, required=True, help="Snowflake database")
    parser.add_argument("--warehouse", type=str,required=False,help="Snowflake warehouse")
    parser.add_argument("--schema", type=str,required=False,help="Snowflake schema")

    # Authentication arguments
    parser.add_argument("--authentication",type=str,required=False,choices=['oauth','password'],help="Authentication method")
    credentials_group = parser.add_mutually_exclusive_group()
    credentials_group.add_argument("--password_secret", type=str,help="Google Cloud Secret Manager ID for password")
    credentials_group.add_argument("--token", type=str, help="Authentication token for oauth")

    # Output destination arguments. Generate local only, or local + to GCS bucket
    output_option_group = parser.add_mutually_exclusive_group()
    output_option_group.add_argument("--local_output_only",action="store_true",help="Output metadata file in local directory only" )
    output_option_group.add_argument("--output_bucket", type=str,help="Cloud Storage bucket for metadata import file. Do not include gs:// prefix")  
    parser.add_argument("--output_folder", type=str, required=False,help="Folder within bucket where generated metadata import file will be written")

    parser.add_argument("--min_expected_entries", type=int, required=False,default=-1,help="Minimum number of entries expected in metadata file, if less entries then file gets deleted. Saftey mechanism for when using Full Entry Sync metadata jobs")
    
    parsed_args = parser.parse_known_args()[0]

    # Argument Validation
    if not parsed_args.local_output_only and parsed_args.output_bucket is None:
        print("--output_bucket must be supplied if not in --local_output_only mode")
        sys.exit(1)

    if parsed_args.authentication == 'oauth' and parsed_args.token is None:
        print("--token must also be supplied if using oauth authentication")
        sys.exit(1)
    
    if (parsed_args.authentication is None or parsed_args.authentication == 'password') and parsed_args.password_secret is None:
        print("--password_secret must also be supplied if using password authentication")
        sys.exit(1)

    if not parsed_args.local_output_only and not checkDestination(parsed_args.output_bucket):
            print("Exiting")
            sys.exit(1)     
        
    if parsed_args.password_secret is not None:
        try:
            parsed_args.password = get_password(parsed_args.password_secret)
        except Exception as ex:
            print(ex)
            print("Exiting")
            sys.exit(1)
    
    return vars(parsed_args)