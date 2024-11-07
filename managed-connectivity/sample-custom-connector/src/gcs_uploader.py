"""Sends files to GCP storage."""
from typing import Dict
from google.cloud import storage


def upload(config: Dict[str, str], filename: str):
    """Uploads a file to GCP bucket."""
    client = storage.Client()
    bucket = client.get_bucket(config["output_bucket"])
    folder = config["output_folder"]

    blob = bucket.blob(f"{folder}/{filename}")
    blob.upload_from_filename(filename)
