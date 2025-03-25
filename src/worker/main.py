"""Main file for the SST Worker."""

import numpy as np
import logging
from typing import Any, Annotated, Dict

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.responses import FileResponse

from pydantic import BaseModel
from fastapi.security import OAuth2PasswordRequestForm
from .utilities import (
    get_sftp_bucket_name,
    StorageControl,
    fetch_institution_ids,
    split_csv_and_generate_signed_urls,
    fetch_upload_url,
    transfer_file,
    sftp_file_to_gcs_helper,
    validate_sftp_file,
)
from .config import sftp_vars, env_vars, startup_env_vars
from .authn import Token, get_current_username, check_creds, create_access_token
from datetime import timedelta

# Set the logging
logging.basicConfig(format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

app = FastAPI(
    servers=[
        # TODO: placeholders
        {"url": "https://stag.example.com", "description": "Staging environment"},
        {"url": "https://prod.example.com", "description": "Production environment"},
    ],
    root_path="/worker/api/v1",
)

# this uses api key to auth to backend api, but credentials to auth to this service


class PdpPullRequest(BaseModel):
    """Params for the PDP pull request."""

    placeholder: str | None = None


class PdpPullResponse(BaseModel):
    """Fields for the PDP pull response."""

    sftp_files: list[dict]
    pdp_inst_generated: list[Any]
    pdp_inst_not_found: list[Any]
    upload_status: dict

    class Config:
        json_encoders = {np.int64: lambda v: int(v)}


@app.on_event("startup")
def on_startup():
    print("Starting up app...")
    startup_env_vars()


# On shutdown, we have to cleanup the GCP database connections
@app.on_event("shutdown")
def shutdown_event():
    print("Performing shutdown tasks...")


# The following root paths don't have pre-authn.
@app.get("/")
def read_root() -> Any:
    """Returns the index.html file."""
    return FileResponse("src/worker/index.html")


@app.post("/token")
async def login_for_access_token(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    valid = check_creds(form_data.username, form_data.password)
    if not valid:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(
        minutes=int(env_vars["ACCESS_TOKEN_EXPIRE_MINUTES"])
    )
    access_token = create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return Token(access_token=access_token, token_type="bearer")


async def process_file(
    storage_control: StorageControl, blob: str, env_vars: Dict[str, Any]
) -> Dict[str, Any]:
    """Process a single file: generate URLs, transfer, and validate."""
    logger.debug(f">>>> Splitting {blob} to extract institution data")
    signed_urls = split_csv_and_generate_signed_urls(
        bucket_name=get_sftp_bucket_name(env_vars["BUCKET_ENV"]),
        source_blob_name=blob,
    )
    logger.info(f">>>> Signed URLs, File names generated for {blob}")

    temp_valid_inst_ids, temp_invalid_ids = fetch_institution_ids(
        pdp_ids=list(signed_urls.keys()),
        backend_api_key=env_vars["BACKEND_API_KEY"],
        webapp_url=env_vars["WEBAPP_URL"],
    )

    uploads = {}
    if temp_valid_inst_ids:
        logger.info(f">>>> Attempting to transfer {temp_valid_inst_ids}")
        for ids in temp_valid_inst_ids:
            logger.debug(f"----------Generating upload url and moving {ids}---------")
            inst_id = temp_valid_inst_ids[ids]
            upload_url = fetch_upload_url(
                file_name=blob,
                institution_id=inst_id,
                webapp_url=env_vars["WEBAPP_URL"],
                backend_api_key=env_vars["BACKEND_API_KEY"],
            )
            logger.info(f">>>> Upload URL successfully retrieved {upload_url}")
            transfer_status = transfer_file(
                download_url=signed_urls[ids]["signed_url"].strip('"'),
                upload_signed_url=upload_url.strip('"'),
            )

            validation_status = validate_sftp_file(
                file_name=blob,
                institution_id=inst_id,
                webapp_url=env_vars["WEBAPP_URL"],
                backend_api_key=env_vars["BACKEND_API_KEY"],
            )

            uploads[str(ids)] = {
                "file_name": signed_urls[ids]["file_name"].strip().strip('"'),
                "transfer_status": (
                    transfer_status.strip()
                    if isinstance(transfer_status, str)
                    else transfer_status
                ),
                "validation_status": validation_status,
            }
    return {
        "valid_inst_ids": temp_valid_inst_ids,
        "invalid_ids": temp_invalid_ids,
        "uploads": uploads,
    }


@app.post("/execute-pdp-pull", response_model=PdpPullResponse)
async def execute_pdp_pull(
    req: PdpPullRequest,
    current_username: Annotated[str, Depends(get_current_username)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Performs the PDP pull of the file."""

    storage_control.create_bucket_if_not_exists(
        get_sftp_bucket_name(env_vars["BUCKET_ENV"])
    )
    files = storage_control.list_sftp_files(
        sftp_vars["SFTP_HOST"],
        22,
        sftp_vars["SFTP_USER"],
        sftp_vars["SFTP_PASSWORD"],
        remote_path="./receive",
    )

    results = []
    for file in files:
        gcs_blob = sftp_file_to_gcs_helper(storage_control, file)
        result = await process_file(storage_control, gcs_blob, env_vars)
        results.append(result)

    # Aggregate results to return
    return {
        "sftp_files": files,
        "pdp_inst_generated": list(result["valid_inst_ids"]),
        "pdp_inst_not_found": list(result["invalid_ids"]),
        "upload_status": dict(result["uploads"]),
    }
