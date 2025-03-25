"""Cloud storage sftp server get related helper functions."""

import paramiko
from google.cloud import storage
from pydantic import BaseModel
import os
import stat
from datetime import datetime, timedelta
import io
import logging
from typing import List, Dict, Any
import requests
import pandas as pd
import re
import google.auth
import google.auth.transport.requests as google_requests
from .config import sftp_vars, env_vars

logging.basicConfig(format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_sftp_bucket_name(env_var: str) -> str:
    return env_var.lower() + "_sftp_ingestion"


# For functionality that interfaces with GCS, wrap it in a class for easier mock unit testing.
class StorageControl(BaseModel):
    def copy_from_sftp_to_gcs(
        self,
        sftp_host: str,
        sftp_port: int,
        sftp_user: str,
        sftp_password: str,
        sftp_file: str,
        bucket_name: str,
        blob_name: str,
    ) -> None:
        """Copies a file from an SFTP server to a GCS bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        if blob.exists():
            logger.debug(
                f">>>> File {blob_name} already exists in the bucket {bucket_name}. Skipping upload."
            )
            return  # Exit the function if the file already exists.

        with paramiko.Transport((sftp_host, sftp_port)) as transport:
            transport.connect(username=sftp_user, password=sftp_password)
            client = paramiko.SFTPClient.from_transport(transport)
            if client is None:
                raise RuntimeError("Failed to create SFTP client.")
            # Open the file in binary read mode.
            with client.open(sftp_file, "rb") as f:
                blob.upload_from_file(f)

    def list_sftp_files(
        self,
        sftp_host: str,
        sftp_port: int,
        sftp_user: str,
        sftp_password: str,
        remote_path: str = ".",
    ) -> List[Dict[str, Any]]:
        """
        Connects to an SFTP server and recursively lists all files under the given remote_path.
        For each file, it returns the file path, size (in MB), and last modified date (ISO format).

        Args:
            sftp_host (str): SFTP server hostname.
            sftp_port (int): SFTP server port.
            sftp_user (str): SFTP username.
            sftp_password (str): SFTP password.
            remote_path (str): Remote directory to start listing from (default is ".").

        Returns:
            List[Dict[str, Any]]: A list of dictionaries for each file with keys 'path', 'size', and 'modified'.
        """
        file_list: List[Dict[str, Any]] = []
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Connect using the SSH key
            ssh.connect(sftp_host, username=sftp_user, password=sftp_password)

            sftp = ssh.open_sftp()
            if sftp is None:
                logger.debug("Failed to create SFTP client.")
                raise RuntimeError("Failed to create SFTP client.")

            def recursive_list(path: str) -> None:
                for attr in sftp.listdir_attr(path):
                    entry_path = os.path.join(path, attr.filename)
                    # Ensure attr.st_mode is an int.
                    if attr.st_mode is not None and stat.S_ISDIR(attr.st_mode):
                        recursive_list(entry_path)
                    else:
                        # Cast modification time and file size.
                        st_mtime = (
                            float(attr.st_mtime) if attr.st_mtime is not None else 0.0
                        )
                        st_size = int(attr.st_size) if attr.st_size is not None else 0
                        modified_iso = datetime.fromtimestamp(st_mtime).isoformat()
                        size_mb = round(st_size / (1024 * 1024), 2)
                        file_info = {
                            "path": entry_path,
                            "size": size_mb,  # in MB
                            "modified": modified_iso,
                        }
                        file_list.append(file_info)

            recursive_list(remote_path)
            sftp.close()
            ssh.close()
        except Exception as e:
            ssh.close()
            raise e  # Re-raise the exception after closing the connection

        return file_list

    def create_bucket_if_not_exists(
        self,
        bucket_name: str,
    ) -> None:
        """Copies a file from an SFTP server to a GCS bucket."""
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        if bucket.exists():
            logging.info(f"Bucket '{bucket_name}' already exists.")
            return

        # Update with URL?
        # fmt: off
        bucket.cors = [
            {
                "origin": ["*"],
                "responseHeader": ["*"],
                "method": ["GET", "OPTIONS", "PUT", "POST"],
                "maxAgeSeconds": 3600
            }
        ]
        # fmt: on
        bucket.storage_class = "STANDARD"
        storage_client.create_bucket(bucket, location="us")


def get_token(backend_api_key: str, webapp_url: str) -> Any:
    """
    Fetches institution IDs for a list of PDP IDs using an API and returns a dictionary of valid IDs and a list of problematic IDs.

    Args:
        pdp_ids (list): List of PDP IDs to process.
        backend_api_key (str): API key required for authorization.

    Returns:
        tuple: A tuple containing a dictionary mapping PDP IDs to Institution IDs and a list of problematic PDP IDs.
    """
    if not backend_api_key:
        logger.error("Missing BACKEND_API_KEY in environment variables.")
        raise ValueError("Missing BACKEND_API_KEY in environment variables.")

    token_response = requests.post(
        f"{webapp_url}/api/v1/token-from-api-key",
        headers={"accept": "application/json", "X-API-KEY": backend_api_key},
    )
    if token_response.status_code != 200:
        logger.error(f"Failed to get token: {token_response.text}")
        return f"Failed to get token: {token_response.text}"

    access_token = token_response.json().get("access_token")

    return access_token


def fetch_institution_ids(
    pdp_ids: List[str], backend_api_key: str, webapp_url: str
) -> Any:
    """
    Fetches institution IDs for a list of PDP IDs using an API and returns a dictionary of valid IDs and a list of problematic IDs.

    Args:
        pdp_ids (list): List of PDP IDs to process.
        backend_api_key (str): API key required for authorization.

    Returns:
        tuple: A tuple containing a dictionary mapping PDP IDs to Institution IDs and a list of problematic PDP IDs.
    """

    # Dictionary to store successful PDP ID to Institution ID mappings
    inst_id_dict: Dict[str, str] = {}
    # List to track problematic IDs
    problematic_ids: List[str] = []
    logger.info(f">>>> Fetching institution ids for {pdp_ids}")
    # Obtain the access token
    access_token = get_token(backend_api_key=backend_api_key, webapp_url=webapp_url)
    if not access_token:
        logger.error("<<<< ???? Access token not found in the response.")
        problematic_ids.append("Access token not found in the response.")
        return {}, problematic_ids

    # Process each PDP ID in the list
    for pdp_id in pdp_ids:
        inst_response = requests.get(
            f"{webapp_url}/api/v1/institutions/pdp-id/{pdp_id}",
            headers={
                "accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Bearer {access_token}",
            },
        )

        if inst_response.status_code == 200:
            inst_data = inst_response.json()
            inst_id = inst_data.get("inst_id")
            if inst_id:
                inst_id_dict[pdp_id] = inst_id
            else:
                logger.error(f"<<<< ???? No institution ID found for PDP ID: {pdp_id}")
                problematic_ids.append(pdp_id)
        else:
            logger.error(
                f"<<<< ???? Failed to fetch institution ID for PDP ID {pdp_id}: {inst_response.text}"
            )
            problematic_ids.append(pdp_id)

    return inst_id_dict, problematic_ids


def fetch_upload_url(
    file_name: str, institution_id: int, webapp_url: str, backend_api_key: str
) -> str:
    """
    Fetches an upload URL from an API for a given file and institution.

    Args:
    file_name (str): The name of the file for which the upload URL is needed.
    institution_id (int): The ID of the institution associated with the file.
    access_token (str): The Bearer token for Authorization header.

    Returns:
    str: The upload URL or an error message.
    """
    # Construct the URL with institution_id and file_name as parameters

    # Set the headers including the Authorization header
    access_token = get_token(backend_api_key=backend_api_key, webapp_url=webapp_url)
    if not access_token:
        logger.error("<<<< ???? Access token not found in the response.")
        return "Access token not found in the response."

    # Make the GET request to the API
    response = requests.get(
        f"{webapp_url}/api/v1/institutions/{institution_id}/upload-url/{file_name}",
        headers={
            "accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
    )

    # Check if the request was successful
    if response.status_code == 200:
        logger.info(response.text)
        return response.text  # or response.json() if the response is JSON
    else:
        logger.error(
            f"<<<< ???? Error fetching URL: {response.status_code} {response.text}"
        )
        return f"Error fetching URL: {response.status_code} {response.text}"


def transfer_file(download_url: str, upload_signed_url: str) -> str:
    """
    Downloads a file from a URL and uploads it directly to a signed URL.

    Args:
    download_url (str): The URL from which the file will be downloaded.
    upload_signed_url (str): The signed URL to which the file should be uploaded.

    Returns:
    str: The response from the server after attempting to upload.
    """
    # Download the file content
    download_response = requests.get(download_url)
    if download_response.status_code != 200:
        logger.error(
            f"<<<< ???? Failed to download file: {download_response.status_code} {download_response.text}"
        )
        return f"Failed to download file: {download_response.status_code} {download_response.text}"

    # Get the file content from the download
    file_content = download_response.content

    # POST the file content to the signed upload URL
    upload_headers = {
        "Content-Type": "text/csv"  # Match the content type used when generating the signed URL
    }
    upload_response = requests.put(
        upload_signed_url, data=file_content, headers=upload_headers
    )

    # Check the response
    if upload_response.status_code == 200:
        logger.info(">>>> File transferred successfully.")
        return "File transferred successfully."
    else:
        logger.error(
            f"<<<< ???? Failed to transfer file: {upload_response.status_code} {upload_response.text}"
        )
        return f"Failed to transfer file: {upload_response.status_code} {upload_response.text}"


def generate_signed_url(
    bucket_name: str, object_name: str, expiration: int = 600000
) -> str:
    """
    Generates a signed URL for a Cloud Storage object using V4 signing.
    This function is production-ready with error handling and logging.

    Returns:
        tuple: A tuple containing the signed URL and an HTTP status code.

    Raises:
        Exception: Re-raises exceptions encountered during the process.
    """
    try:
        # Obtain default credentials and project ID.
        credentials, project_id = google.auth.default()
        logger.info(">>>> Obtained default credentials.")
    except Exception as e:
        logger.error("<<<< ???? Failed to get default credentials: %s", e)
        raise Exception("Credential error") from e

    try:
        # Refresh credentials to ensure an access token is available.
        request_obj = google_requests.Request()
        credentials.refresh(request_obj)
        logger.info(">>>> Credentials refreshed successfully.")
    except Exception as e:
        logger.error("<<<< ???? Failed to refresh credentials: %s", e)
        raise Exception("Credential refresh error") from e

    try:
        # Create a storage client using the refreshed credentials.
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.get_blob(object_name)
        if blob is None:
            raise ValueError(
                f"Blob '{object_name}' not found in bucket '{bucket_name}'."
            )
        logger.info(">>>> Accessed bucket and blob successfully.")
    except Exception as e:
        logger.error("<<<< ???? Failed to access bucket or blob: %s", e)
        raise Exception("Bucket/blob access error") from e

    # Set expiration for the signed URL.
    expires = datetime.utcnow() + timedelta(seconds=expiration)  # 24 hours

    # Determine the service account email.
    service_account_email = ""  # Fallback value.
    if hasattr(credentials, "service_account_email"):
        service_account_email = credentials.service_account_email
    logger.debug(">>>> ???? Using service account email: %s", service_account_email)

    try:
        # Generate the signed URL.
        signed_url = blob.generate_signed_url(
            expiration=expires,
            service_account_email=service_account_email,
            access_token=credentials.token,
            method="GET",
        )
        logger.debug(">>>> Signed URL generated successfully.")
    except Exception as e:
        logger.error("<<<< ???? Failed to generate signed URL: %s", e)
        raise Exception("Signed URL generation error") from e

    return str(signed_url)


def split_csv_and_generate_signed_urls(
    bucket_name: str, source_blob_name: str
) -> Dict[str, Dict[str, str]]:
    """
    Fetches a CSV from Google Cloud Storage, splits it by a specified column, uploads the results,
    and returns a dictionary where each key is an institution ID and the value is another dictionary
    containing both a signed URL and file name for the uploaded file.

    Parameters:
        storage_client (storage.Client): The Google Cloud Storage client instance.
        bucket_name (str): The name of the GCS bucket containing the source CSV.
        source_blob_name (str): The blob name of the source CSV file.
        destination_folder (str): The destination folder in the bucket to store split files.
        institution_column (str): The column to split the CSV file on.
        url_expiration_minutes (int): The duration in minutes for which the signed URLs are valid.

    Returns:
        Dict[str, Dict[str, str]]: A dictionary with institution IDs as keys and dictionaries with 'signed_url' and 'file_name' as values.
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(source_blob_name)

    try:
        logger.debug(
            f">>>> Attempting to download the source blob to memory: {source_blob_name}"
        )
        csv_string = source_blob.download_as_text()
        csv_data = io.StringIO(csv_string)
        df = pd.read_csv(csv_data)
        logger.debug(
            f">>>> {source_blob_name} data successfully loaded into DataFrame."
        )
    except Exception as e:
        logger.error(f"<<<< ???? Failed to process blob {source_blob_name}: {e}")
        return {}

    pattern = re.compile(r"(?=.*institution)(?=.*id)", re.IGNORECASE)
    institution_column = None

    # Identify the correct column based on the pattern
    for column in df.columns:
        if pattern.search(column):
            institution_column = column
            logger.debug(f">>>> Matching column found: {column} in {source_blob_name}")
            break

    if not institution_column:
        error_message = (
            "<<<< ???? No column found matching the pattern for 'institution' and 'id'."
        )
        logger.debug(error_message)
        return {"error": {"message": "Failed to download or parse CSV"}}

    current_time = datetime.now().strftime("%Y%m%d_%H")
    all_data = {}

    # Processing the DataFrame
    unique_inst_ids = df[institution_column].unique()
    for inst_id in unique_inst_ids:
        group = df[df[institution_column] == inst_id]
        output = io.StringIO()
        group.to_csv(output, index=False)
        output.seek(0)

        file_name = f"{source_blob_name.split('.')[0]}_{inst_id}.csv"
        timestamped_folder = f"{inst_id}"
        new_blob_name = f"{timestamped_folder}/{current_time}/{file_name}"
        new_blob = storage_client.bucket(bucket_name).blob(new_blob_name)

        # Check if blob exists and upload if not
        if not new_blob.exists():
            try:
                logger.info(
                    f">>>> Uploading new CSV for institution ID {inst_id} to {new_blob_name}"
                )
                new_blob.upload_from_string(output.getvalue(), content_type="text/csv")
            except Exception as e:
                logger.error(
                    f"<<<< ???? Failed to upload CSV for institution ID {inst_id}: {e}"
                )
                continue

        # Generate a signed URL for the new or existing blob
        try:
            signed_url = generate_signed_url(
                bucket_name=bucket_name,
                object_name=new_blob_name,
            )
            all_data[str(inst_id)] = {
                "signed_url": signed_url,
                "file_name": file_name,
            }
            logger.info(f">>>> Signed URL generated for institution ID {inst_id}")
        except Exception as e:
            logger.error(
                f"<<<< ???? Failed to generate signed URL for institution ID {inst_id}: {e}"
            )

    return all_data


def sftp_file_to_gcs_helper(
    storage_control: StorageControl, sftp_source_filename: dict
) -> str:
    """
    For each source file in sftp_source_filenames, copies the file from the SFTP
    server to GCS. The destination filename is automatically generated by prefixing
    the base name of the source file with "processed_".

    Args:
        storage_control (StorageControl): An instance with a method `copy_from_sftp_to_gcs`.
        sftp_source_filenames (list): A list of file paths on the SFTP server.
    """
    # all_blobs = []  # List to keep track of all processed files
    logger.info(
        f"<<<<<<<<<>>>>>>>>> Starting sftp to gcs copy for {sftp_source_filename} file(s). <<<<<<<<<<<>>>>>>>>>>>>>"
    )
    # for sftp_source_filename in sftp_source_filenames:
    # Extract the base filename and prepare the destination filename
    source_filename = sftp_source_filename["path"]
    # Extract the base filename.
    base_filename = os.path.basename(source_filename)
    dest_filename = f"{base_filename}"

    # Check if the file has already been processed
    # if dest_filename in all_blobs:
    #    logger.info(f"Skipping already processed file: {dest_filename}")
    #    continue

    logger.debug(f">>>> Processing source file: {source_filename}")
    logger.debug(f">>>> Destination filename will be: {dest_filename}")

    try:
        storage_control.copy_from_sftp_to_gcs(
            sftp_vars["SFTP_HOST"],
            22,
            sftp_vars["SFTP_USER"],
            sftp_vars["SFTP_PASSWORD"],
            source_filename,
            get_sftp_bucket_name(env_vars["ENV"]),
            dest_filename,
        )
        # all_blobs.append(dest_filename)
        logger.info(
            f">>>> Successfully processed '{source_filename}' as '{dest_filename}'."
        )
    except Exception as e:
        logger.error(
            f"<<<< ????? Error processing '{source_filename}': {e}", exc_info=True
        )

    return dest_filename


def validate_sftp_file(
    file_name: str, institution_id: int, webapp_url: str, backend_api_key: str
) -> str:
    """
    Sends a POST request to validate an SFTP file.

    Args:
        institution_id (str): The ID of the institution for which the file validation is intended.
        file_name (str): The name of the file to be validated.
        access_token (str): The bearer token used for authorization.

    Returns:
        str: The server's response to the validation request.
    """
    access_token = get_token(backend_api_key=backend_api_key, webapp_url=webapp_url)
    if not access_token:
        logger.error("<<<< ???? Access token not found in the response.")
        return "Access token not found in the response."

    url = f"{webapp_url}/api/v1/institutions/{institution_id}/input/validate-sftp/{file_name}"
    headers = {"accept": "application/json", "Authorization": f"Bearer {access_token}"}

    logger.debug(f">>>> Sending validation request to {url}")

    response = requests.post(url, headers=headers)

    if response.status_code == 200:
        logger.info(">>>> File validation successfully initiated.")
        return "File validation successfully initiated."
    else:
        error_message = f"<<<< ???? Failed to initiate file validation: {response.status_code} {response.text}"
        logger.error(error_message)
        return error_message
