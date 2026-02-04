import json
import os
from google.cloud import storage

class GCSUploader:
    def __init__(self, project_id: str, bucket_name: str):
        self.client = storage.Client(project=project_id)
        self.bucket = self.client.bucket(bucket_name)

    def upload_entries(self, entries: list, aws_region: str, output_folder: str = None):
        """
        Converts a list of dictionary entries to a JSONL file and uploads it to GCS,
        optionally within a specified folder.
        """
        if not entries:
            print("No entries to upload.")
            return

        # Define the output file name
        file_name = f"aws-glue-output-{aws_region}.jsonl"
        
        # Convert list of entries to a JSONL formatted string
        content = "\n".join(json.dumps(entry) for entry in entries)

        # If an output folder is provided, create the full destination path
        if output_folder:
            blob_name = os.path.join(output_folder, file_name)
        else:
            blob_name = file_name
            
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(content)
        
        # The final print statement is now in bootstrap.py for better context