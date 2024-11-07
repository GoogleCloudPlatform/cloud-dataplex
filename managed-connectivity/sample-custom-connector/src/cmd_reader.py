"""Command line reader."""
import argparse


def read_args():
    """Reads arguments from the command line."""
    parser = argparse.ArgumentParser()

    # Dataplex arguments
    parser.add_argument("--target_project_id", type=str, required=True,
        help="The name of the target Google Cloud project to import the metadata into.")
    parser.add_argument("--target_location_id", type=str, required=True,
        help="The target Google Cloud location where the metadata will be imported into.")
    parser.add_argument("--target_entry_group_id", type=str, required=True,
        help="The ID of the entry group to import metadata into. "
             "The metadata will be imported into entry group with the following"
             "full resource name: projects/${target_project_id}/"
             "locations/${target_location_id}/entryGroups/${target_entry_group_id}.")

    # Oracle arguments
    parser.add_argument("--host_port", type=str, required=True,
        help="Oracle host and port number separated by the colon (:).")
    parser.add_argument("--user", type=str, required=True, help="Oracle User.")
    parser.add_argument("--password-secret", type=str, required=True,
        help="Secret resource name in the Secret Manager for the Oracle password.")
    parser.add_argument("--database", type=str, required=True,
        help="Source Oracle database.")

    # Google Cloud Storage arguments
    # It is assumed that the bucket is in the same region as the entry group
    parser.add_argument("--output_bucket", type=str, required=True,
        help="The Cloud Storage bucket to write the generated metadata import file.")
    parser.add_argument("--output_folder", type=str, required=True,
        help="A folder in the Cloud Storage bucket, to write the generated metadata import files.")

    return vars(parser.parse_known_args()[0])
