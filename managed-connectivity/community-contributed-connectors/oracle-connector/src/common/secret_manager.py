"""A module to get a password from the Secret Manager."""
from google.cloud import secretmanager


def get_password(secret_path: str) -> str:
    """Gets password from a GCP service."""
    client = secretmanager.SecretManagerServiceClient()
    if "versions" not in secret_path:
        # If not specified, we need the latest version of a password
        secret_path = f"{secret_path}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")
