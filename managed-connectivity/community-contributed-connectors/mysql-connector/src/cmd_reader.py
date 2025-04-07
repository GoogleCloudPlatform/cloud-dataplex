import argparse
import sys
from src.common.util import loadReferencedFile
from src.common.gcs_uploader import checkDestination
from src.common.secret_manager import get_password

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
                        help="Resource name in the Google Cloud Secret Manager for the Mysql password")
    
    parser.add_argument("--jar", type=str, required=False, help="path to jar file")
    
    parser.add_argument("--ssl_mode", type=str, required=False,choices=['prefer','require','allow','verify-ca','verify-full'],default='prefer',help="SSL mode requirement")
    parser.add_argument("--ssl_cert", type=str, required=False,help="SSL cert file path")
    parser.add_argument("--ssl_key", type=str, required=False,help="SSL key file path")
    parser.add_argument("--ssl_rootcert", type=str, required=False,help="SSL root cert file path")
    
    # Output destination arguments. Generate local only, or local + to GCS bucket
    output_option_group = parser.add_mutually_exclusive_group()
    output_option_group.add_argument("--local_output_only",action="store_true",help="Output metadata file in local directory only" )
    output_option_group.add_argument("--output_bucket", type=str,
                        help="Output Cloud Storage bucket for generated metadata import file. Do not include gs:// prefix ")  
    parser.add_argument("--output_folder", type=str, required=False,
                        help="Folder within bucket where generated metadata import file will be written. Name only required")
    
    parser.add_argument("--min_expected_entries", type=int, required=False,default=-1,
                        help="Minimum number of entries expected in metadata file, if less entries then file gets deleted. Saftey mechanism for when using Full Entry Sync metadata jobs")
    
    
    parsed_args = parser.parse_known_args()[0]

    if not parsed_args.local_output_only and parsed_args.output_bucket is None:
        print("--output_bucket must be supplied if not in --local_output_only mode")
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

    if parsed_args.ssl_cert is not None:
        try:
            parsed_args.password = loadReferencedFile(parsed_args.ssl_cert)
        except Exception as ex:
            print(ex)
            print("Exiting")
            sys.exit(1)

    return vars(parsed_args)