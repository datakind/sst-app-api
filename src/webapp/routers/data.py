"""API functions related to data."""

import uuid
from datetime import datetime, date
from databricks.sdk import WorkspaceClient
from typing import Annotated, Any, Dict, List, cast, IO, Optional
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status, Response, Query
from fastapi.responses import FileResponse
from sqlalchemy import and_, or_
from sqlalchemy.orm import Session
from sqlalchemy.future import select
import os
import logging
from sqlalchemy.exc import IntegrityError
from ..config import databricks_vars, env_vars, gcs_vars
import tempfile
import pathlib
import re
from ..validation import HardValidationError

from ..utilities import (
    has_access_to_inst_or_err,
    has_full_data_access_or_err,
    BaseUser,
    model_owner_and_higher_or_err,
    uuid_to_str,
    str_to_uuid,
    get_current_active_user,
    DataSource,
    get_external_bucket_name,
    decode_url_piece,
    databricksify_inst_name,
)

from ..database import (
    get_session,
    local_session,
    BatchTable,
    FileTable,
    InstTable,
    SchemaRegistryTable,
    DocType,
)

from ..databricks import DatabricksControl
from ..gcsdbutils import update_db_from_bucket

from ..gcsutil import StorageControl

# Set the logging
logging.basicConfig(format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = APIRouter(
    prefix="/institutions",
    tags=["data"],
)

LOGGER = logging.getLogger(__name__)


class BatchCreationRequest(BaseModel):
    """The Batch creation request."""

    # Must be unique within an institution to avoid confusion
    name: str
    # Disabled data means it is no longer in use or not available for use.
    batch_disabled: bool = False
    # You can specify files to include as ids or names.
    file_ids: set[str] | None = None
    file_names: set[str] | None = None
    completed: bool | None = None
    # Set this to set this batch for deletion.
    deleted: bool = False


class BatchInfo(BaseModel):
    """The Batch Data object that's returned."""

    # In order to allow PATCH commands, each field must be marked as nullable.
    batch_id: str | None = None
    inst_id: str | None = None
    file_names_to_ids: Dict[str, str] = {}
    # Must be unique within an institution to avoid confusion
    name: str | None = None
    # User id of uploader or person who triggered this data ingestion.
    created_by: str | None = None
    # Deleted data means this batch has a pending deletion request and can no longer be used.
    deleted: bool | None = None
    # Completed batches means this batch is ready for use. Completed batches will
    # trigger notifications to Datakind.
    # Can be modified after completion, but this information will not re-trigger
    # notifications to Datakind.
    completed: bool | None = None
    # Date in form YYMMDD. Deletion of a batch will apply to all files in a batch,
    # unless the file is present in other batches.
    deletion_request_time: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    # The following is the user who last updated this batch.
    updated_by: str | None = None


class DeletedFile(BaseModel):
    file: str = Field(..., description="Basename of the deleted file")
    path: str = Field(..., description="Bucket object path, e.g. 'validated/<name>'")
    deleted_at: datetime = Field(
        ..., description="UTC timestamp when deletion occurred"
    )


class DeleteBatchResponse(BaseModel):
    inst_id: str
    batch_id: str
    deleted: List[DeletedFile] = Field(
        default_factory=list, description="Files deleted in storage"
    )
    not_found: List[str] = Field(
        default_factory=list, description="Files not found in storage"
    )
    errors: List[str] = Field(
        default_factory=list, description="Errors encountered during delete"
    )
    db_deleted_rows: int = Field(..., description="Number of FileTable rows deleted")
    batch_deleted: bool = Field(
        ..., description="Whether the BatchTable row was deleted"
    )
    message: Optional[str] = Field(None, description="Optional info message")


class DataInfo(BaseModel):
    """The Data object that's returned. Generally maps to a file, but technically maps to a GCS blob."""

    # Must be unique within an institution to avoid confusion.
    name: str
    data_id: str
    # The batch(es) that this data is present in.
    batch_ids: set[str] = set()
    inst_id: str
    # Size to the nearest MB.
    # size_mb: int
    # User id of uploader or person who triggered this data ingestion. For SST generated files, this field would be null.
    uploader: str | None = None
    # Can be PDP_SFTP, MANUAL_UPLOAD etc.
    source: DataSource | None = None
    # Deleted data means this file has a pending deletion request or is deleted and can no longer be used.
    deleted: bool = False
    # Date in form YYMMDD
    deletion_request_time: date | None = None
    # How long to retain the data.
    # By default (None) -- it is deleted after a successful run. For training dataset it
    # is deleted after the trained model is approved. For inference input, it is deleted
    # after the inference run occurs. For inference output, it is retained indefinitely
    # unless an ad hoc deletion request is received. The type of data is determined by
    # the storage location.
    retention_days: int | None = None
    # Whether the file was generated by SST. (e.g. was it input or output)
    sst_generated: bool
    # Whether the file was validated (in the case of input) or approved (in the case of output).
    valid: bool = False
    uploaded_date: datetime


class ValidationResult(BaseModel):
    """The returned validation result."""

    # Must be unique within an institution to avoid confusion.
    name: str
    inst_id: str
    file_types: List[str]
    source: str


class DataOverview(BaseModel):
    """All data for a given institution (batches and files)."""

    batches: list[BatchInfo]
    files: list[DataInfo]


# Data related operations. Input files mean files sourced from the institution. Output files are generated by SST.


def get_all_files(
    inst_id: str,
    sst_generated_value: bool | None,
    sess: Session,
    storage_control: Any,
) -> list[DataInfo]:
    """Retrieve all files."""
    # Update from bucket
    if sst_generated_value:
        update_db_from_bucket(inst_id, sess, storage_control)
        sess.commit()
    # construct query
    query = None
    if sst_generated_value is None:
        query = select(FileTable).where(
            FileTable.inst_id == str_to_uuid(inst_id),
        )
    else:
        query = select(FileTable).where(
            and_(
                FileTable.inst_id == str_to_uuid(inst_id),
                FileTable.sst_generated == sst_generated_value,
            )
        )

    result_files = []
    for e in sess.execute(query).all():
        elem = e[0]
        result_files.append(
            {
                "name": elem.name,
                "data_id": uuid_to_str(elem.id),
                "batch_ids": uuids_to_strs(elem.batches),
                "inst_id": uuid_to_str(elem.inst_id),
                # "size_mb": elem.size_mb,
                "uploader": uuid_to_str(elem.uploader),
                "source": elem.source,
                "deleted": False if elem.deleted is None else elem.deleted,
                "deletion_request_time": elem.deleted_at,
                "retention_days": elem.retention_days,
                "sst_generated": elem.sst_generated,
                "valid": elem.valid,
                "uploaded_date": elem.created_at,
            }
        )
    return result_files  # type: ignore


def get_all_batches(
    inst_id: str, output_batches_only: bool, sess: Session
) -> list[BatchInfo]:
    """Some batches are associated with output. This function lets you decide if you want only those batches."""
    query_result_batches = sess.execute(
        select(BatchTable).where(BatchTable.inst_id == str_to_uuid(inst_id))
    ).all()
    result_batches = []
    for e in query_result_batches:
        # Note that batches may show file ids of invalid or unapproved files.
        # And will show input and output files.
        elem = e[0]
        if output_batches_only:
            output_files = [x for x in elem.files if x.sst_generated]
            if not output_files:
                continue
        result_batches.append(
            {
                "batch_id": uuid_to_str(elem.id),
                "inst_id": uuid_to_str(elem.inst_id),
                "name": elem.name,
                "file_names_to_ids": {x.name: uuid_to_str(x.id) for x in elem.files},
                "created_by": uuid_to_str(elem.created_by),
                "deleted": False if elem.deleted is None else elem.deleted,
                "completed": False if elem.completed is None else elem.completed,
                "deletion_request_time": elem.deleted_at,
                "created_at": elem.created_at,
                "updated_by": uuid_to_str(elem.updated_by),
                "updated_at": elem.updated_at,
            }
        )
    return result_batches  # type: ignore


def uuids_to_strs(files: Any) -> set[str]:
    """Convert a set of uuids to strings.
    The input is of type sqlalchemy.orm.collections.InstrumentedSet.
    """
    return [uuid_to_str(x.id) for x in files]  # type: ignore


def strs_to_uuids(files: Any) -> set[uuid.UUID]:
    """Convert a set of strs to uuids."""
    return [str_to_uuid(x) for x in files]  # type: ignore


@router.get("/{inst_id}/input", response_model=DataOverview)
def read_inst_all_input_files(
    inst_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Returns top-level overview of input data (date uploaded, size, file names etc.).

    Only visible to data owners of that institution or higher.
    """
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "input data")
    # Datakinders can see unapproved files as well.
    local_session.set(sql_session)
    return {
        "batches": get_all_batches(inst_id, False, local_session.get()),
        # Set sst_generated_value=false to get input only
        "files": get_all_files(inst_id, False, local_session.get(), None),
    }


@router.get("/{inst_id}/output", response_model=DataOverview)
def read_inst_all_output_files(
    inst_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Returns top-level overview of output data (date uploaded, size, file names etc.) and batch info.

    Only visible to data owners of that institution or higher.
    """
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "output data")
    local_session.set(sql_session)
    return {
        # Set output_batches_only=true to get output related batches only.
        "batches": get_all_batches(inst_id, True, local_session.get()),
        # Set sst_generated_value=true to get output only.
        "files": get_all_files(
            inst_id,
            True,
            local_session.get(),
            storage_control,
        ),
    }


# TODO: rename this function to better reflect its behavior.
@router.post("/{inst_id}/update-data")
def update_data(
    inst_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Updates the database depending on what's new in the bucket. For instance, if
    a pipeline run completed and there are new outputs in the bucket, we want to
    update the database so that the API can be aware of these changes."""
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    update_db_from_bucket(inst_id, local_session.get(), storage_control)
    local_session.get().commit()


@router.get("/{inst_id}/output-file-contents/{file_name:path}", response_model=bytes)
def retrieve_file_as_bytes(
    inst_id: str,
    file_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Returns top-level overview of output data (date uploaded, size, file names etc.) and batch info.

    Only visible to data owners of that institution or higher.
    """
    file_name = decode_url_piece(file_name)
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "output file")
    local_session.set(sql_session)
    # TODO: consider removing this call here and forcing users to call <inst-id>/update-data
    update_db_from_bucket(inst_id, local_session.get(), storage_control)
    local_session.get().commit()
    # We don't include the valid check, because we want to return unapproved AND approved data.
    query_result = (
        local_session.get()
        .execute(
            select(FileTable).where(
                and_(
                    FileTable.sst_generated,
                    FileTable.name == file_name,
                    FileTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    if len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No such output file exists.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multiple matches found. Unexpected.",
        )
    if query_result[0][0].sst_generated:
        if query_result[0][0].valid:
            file_name = "approved/" + file_name
        else:
            file_name = "unapproved/" + file_name
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Only SST generated files can be retrieved.",
        )
    res = storage_control.get_file_contents(
        get_external_bucket_name(inst_id), file_name
    )
    return Response(res)


@router.get("/{inst_id}/batch/{batch_id}", response_model=DataOverview)
def read_batch_info(
    inst_id: str,
    batch_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Returns batch info and files in that batch.

    Only visible to users of that institution or Datakinder access types.
    """
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "batch data")
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(
            select(BatchTable).where(
                and_(
                    BatchTable.id == str_to_uuid(batch_id),
                    BatchTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No such batch exists.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch duplicates found.",
        )
    res = query_result[0][0]
    batch_info = {
        "batch_id": uuid_to_str(res.id),
        "inst_id": uuid_to_str(res.inst_id),
        "name": res.name,
        "file_names_to_ids": {x.name: uuid_to_str(x.id) for x in res.files},
        "created_by": uuid_to_str(res.created_by),
        "deleted": False if res.deleted is None else res.deleted,
        "completed": False if res.completed is None else res.completed,
        "deletion_request_time": res.deleted_at,
        "created_at": res.created_at,
        "updated_at": res.updated_at,
        "updated_by": uuid_to_str(res.updated_by),
    }
    data_infos = []
    for elem in res.files:
        data_infos.append(
            {
                "name": elem.name,
                "data_id": uuid_to_str(elem.id),
                "batch_ids": uuids_to_strs(elem.batches),
                "inst_id": uuid_to_str(elem.inst_id),
                # "size_mb": elem.size_mb,
                "uploader": uuid_to_str(elem.uploader),
                "source": elem.source,
                "deleted": False if elem.deleted is None else elem.deleted,
                "deletion_request_time": elem.deleted_at,
                "retention_days": elem.retention_days,
                "sst_generated": elem.sst_generated,
                "valid": elem.valid,
                "uploaded_date": elem.created_at,
            }
        )
    return {"batches": [batch_info], "files": data_infos}


@router.post("/{inst_id}/batch", response_model=BatchInfo)
def create_batch(
    inst_id: str,
    req: BatchCreationRequest,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Create a new batch."""
    has_access_to_inst_or_err(inst_id, current_user)
    model_owner_and_higher_or_err(current_user, "batch")
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(
            select(BatchTable).where(
                and_(
                    BatchTable.name == req.name,
                    BatchTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    if len(query_result) == 0:
        batch = BatchTable(
            name=req.name,
            inst_id=str_to_uuid(inst_id),
            created_by=str_to_uuid(current_user.user_id),  # type: ignore
        )
        f_names = [] if not req.file_names else req.file_names
        f_ids = [] if not req.file_ids else strs_to_uuids(req.file_ids)
        print(f"File names: {f_names}, File Ids: {f_ids}")
        # Check that the files requested for this batch exists.
        # Only valid non-sst generated files can be added to a batch at creation time.
        query_result_files = (
            local_session.get()
            .execute(
                select(FileTable).where(
                    and_(
                        or_(
                            FileTable.id.in_(f_ids),
                            FileTable.name.in_(f_names),
                        ),
                        FileTable.inst_id == str_to_uuid(inst_id),
                        FileTable.valid == True,
                        FileTable.sst_generated == False,
                    )
                )
            )
            .all()
        )
        if not query_result_files or len(query_result_files) == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="file in request not found.",
            )
        for elem in query_result_files:
            batch.files.add(elem[0])
        local_session.get().add(batch)
        local_session.get().commit()
        query_result = (
            local_session.get()
            .execute(
                select(BatchTable).where(
                    and_(
                        BatchTable.name == req.name,
                        BatchTable.inst_id == str_to_uuid(inst_id),
                    )
                )
            )
            .all()
        )
        if not query_result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database write of the batch creation failed.",
            )
        if len(query_result) > 1:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database write of the batch created duplicate entries.",
            )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch with this name already exists.",
        )
    return {
        "batch_id": uuid_to_str(query_result[0][0].id),
        "inst_id": uuid_to_str(query_result[0][0].inst_id),
        "name": query_result[0][0].name,
        "file_names_to_ids": {
            x.name: uuid_to_str(x.id) for x in query_result[0][0].files
        },
        "created_by": uuid_to_str(query_result[0][0].created_by),
        "deleted": False,
        "completed": False,
        "deletion_request_time": None,
        "created_at": query_result[0][0].created_at,
        "updated_by": uuid_to_str(query_result[0][0].updated_by),
        "updated_at": query_result[0][0].updated_at,
    }


@router.patch("/{inst_id}/batch/{batch_id}", response_model=BatchInfo)
def update_batch(
    inst_id: str,
    batch_id: str,
    request: BatchCreationRequest,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Modifies an existing batch. Only some fields are allowed to be modified."""
    has_access_to_inst_or_err(inst_id, current_user)
    model_owner_and_higher_or_err(current_user, "modify batch")

    update_data_req = request.model_dump(exclude_unset=True)
    local_session.set(sql_session)
    # Check that the batch exists.
    query_result = (
        local_session.get()
        .execute(
            select(BatchTable).where(
                and_(
                    BatchTable.id == str_to_uuid(batch_id),
                    BatchTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Batch not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multiple batches with same unique id found.",
        )
    existing_batch = query_result[0][0]
    if existing_batch.deleted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Batch is set for deletion, no modifications allowed.",
        )
    if "deleted" in update_data_req and update_data_req["deleted"]:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Batch deletion not yet implemented.",
        )
    if "file_ids" in update_data_req or "file_names" in update_data_req:
        existing_batch.files.clear()

    if "file_ids" in update_data_req:
        for f in strs_to_uuids(update_data_req["file_ids"]):
            # Check that the files requested for this batch exists
            query_result_file = (
                local_session.get()
                .execute(
                    select(FileTable).where(
                        and_(
                            FileTable.id == f,
                            FileTable.inst_id == str_to_uuid(inst_id),
                        )
                    )
                )
                .all()
            )
            if not query_result_file or len(query_result_file) == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="file in request not found.",
                )
            if len(query_result_file) > 1:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Multiple files in request with same unique id found.",
                )
            existing_batch.files.add(query_result_file[0][0])
    if "file_names" in update_data_req:
        for f in update_data_req["file_names"]:
            # Check that the files requested for this batch exists
            query_result_file = (
                local_session.get()
                .execute(
                    select(FileTable).where(
                        and_(
                            FileTable.name == f,
                            FileTable.inst_id == str_to_uuid(inst_id),
                        )
                    )
                )
                .all()
            )
            if not query_result_file or len(query_result_file) == 0:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="file in request not found.",
                )
            if len(query_result_file) > 1:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Multiple files in request with same unique id found.",
                )
            existing_batch.files.add(query_result_file[0][0])

    if "name" in update_data_req:
        existing_batch.name = update_data_req["name"]
    if "completed" in update_data_req:
        existing_batch.completed = update_data_req["completed"]
    existing_batch.updated_by = str_to_uuid(current_user.user_id)  # type: ignore
    local_session.get().commit()
    res = (
        local_session.get()
        .execute(
            select(BatchTable).where(
                and_(
                    BatchTable.id == str_to_uuid(batch_id),
                    BatchTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    return {
        "batch_id": uuid_to_str(res[0][0].id),
        "inst_id": uuid_to_str(res[0][0].inst_id),
        "name": res[0][0].name,
        "file_names_to_ids": {x.name: uuid_to_str(x.id) for x in res[0][0].files},
        "created_by": uuid_to_str(res[0][0].created_by),
        "deleted": res[0][0].deleted,
        "completed": res[0][0].completed,
        "deletion_request_time": res[0][0].deleted_at,
        "created_at": res[0][0].created_at,
        "updated_by": uuid_to_str(query_result[0][0].updated_by),
        "updated_at": query_result[0][0].updated_at,
    }


@router.delete("/{inst_id}/batch/{batch_id}", response_model=DeleteBatchResponse)
def delete_batch(
    inst_id: str,
    batch_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    has_access_to_inst_or_err(inst_id, current_user)
    model_owner_and_higher_or_err(current_user, "modify batch")

    local_session.set(sql_session)
    sess = local_session.get()

    batch = sess.execute(
        select(BatchTable).where(
            BatchTable.id == str_to_uuid(batch_id),
            BatchTable.inst_id == str_to_uuid(inst_id),
        )
    ).scalar_one_or_none()
    if batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Batch not found."
        )

    # 2) Gather filenames to delete

    batch_files: list[str] = list(
        sess.execute(
            select(FileTable.name)
            .join(FileTable.batches)  # many-to-many via association_table
            .where(
                BatchTable.id == str_to_uuid(batch_id),
                FileTable.inst_id == str_to_uuid(inst_id),
            )
        )
        .scalars()
        .all()
    )

    if not batch_files:
        sess.delete(batch)
        sess.flush()
        return {
            "inst_id": inst_id,
            "batch_id": batch_id,
            "deleted": [],
            "not_found": [],
            "errors": [],
            "db_deleted_rows": 0,
            "batch_deleted": True,
            "message": "No files associated with this batch id.",
        }

    gcs_result = storage_control.delete_batch_files(
        bucket_name=get_external_bucket_name(inst_id), batch_files=batch_files
    )

    if gcs_result.get("errors"):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unable to delete files {gcs_result['errors']}.",
        )

    # 4) Delete DB rows only for blobs that were actually deleted
    deleted_names = {d["file"] for d in gcs_result.get("deleted", [])}
    not_found_names = set(gcs_result.get("not_found", []))
    target_names = {n for n in (deleted_names | not_found_names) if n}

    db_deleted_rows = 0
    if target_names:
        try:
            rows = (
                sess.execute(
                    select(FileTable)
                    .join(FileTable.batches)
                    .where(
                        BatchTable.id == str_to_uuid(batch_id),
                        FileTable.inst_id == str_to_uuid(inst_id),
                        FileTable.name.in_(target_names),
                    )
                )
                .scalars()
                .all()
            )
            for r in rows:
                sess.delete(r)
            db_deleted_rows = len(rows)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Deleted in storage, but DB file-row cleanup failed: {e}",
            )
    try:
        sess.delete(batch)
        sess.commit()
    except Exception as e:
        sess.rollback()
        raise HTTPException(
            status_code=500, detail=f"DB batch delete failed after file cleanup: {e}"
        )

    return {
        "inst_id": inst_id,
        "batch_id": batch_id,
        "deleted": gcs_result.get("deleted", []),  # [{file, path, deleted_at}, ...]
        "not_found": sorted(not_found_names),
        "errors": gcs_result.get("errors", []),
        "db_deleted_rows": db_deleted_rows,
        "batch_deleted": True,
    }


@router.get("/{inst_id}/file-id/{file_id}", response_model=DataInfo)
def read_file_id_info(
    inst_id: str,
    file_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Returns details on a given file.

    Only visible to users of that institution or Datakinder access types.
    """
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "file data")
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(
            select(FileTable).where(
                and_(
                    FileTable.id == str_to_uuid(file_id),
                    FileTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    # This should only result in a match of a single file.
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="File duplicates found.",
        )
    res = query_result[0][0]
    return {
        "name": res.name,
        "data_id": uuid_to_str(res.id),
        "batch_ids": uuids_to_strs(res.batches),
        "inst_id": uuid_to_str(res.inst_id),
        # "size_mb": res.size_mb,
        "uploader": uuid_to_str(res.uploader),
        "source": res.source,
        "deleted": False if res.deleted is None else res.deleted,
        "deletion_request_time": res.deleted_at,
        "retention_days": res.retention_days,
        "sst_generated": res.sst_generated,
        "valid": res.valid,
        "uploaded_date": res.created_at,
    }


@router.get("/{inst_id}/file/{file_name:path}", response_model=DataInfo)
def read_file_info(
    inst_id: str,
    file_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Returns a given file's data.

    Only visible to users of that institution or Datakinder access types.
    """
    file_name = decode_url_piece(file_name)
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "file data")
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(
            select(FileTable).where(
                and_(
                    FileTable.name == file_name,
                    FileTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    # This should only result in a match of a single file.
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="File duplicates found.",
        )
    res = query_result[0][0]
    return {
        "name": res.name,
        "data_id": uuid_to_str(res.id),
        "batch_ids": uuids_to_strs(res.batches),
        "inst_id": uuid_to_str(res.inst_id),
        # "size_mb": res.size_mb,
        "uploader": uuid_to_str(res.uploader),
        "source": res.source,
        "deleted": False if res.deleted is None else res.deleted,
        "deletion_request_time": res.deleted_at,
        "retention_days": res.retention_days,
        "sst_generated": res.sst_generated,
        "valid": res.valid,
        "uploaded_date": res.created_at,
    }


@router.get("/{inst_id}/download-url/{file_name:path}", response_model=str)
def download_url_inst_file(
    inst_id: str,
    file_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Enables download of output files (approved and unapproved).

    Only visible to users of that institution or Datakinder access types.
    """
    file_name = decode_url_piece(file_name)
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "file data")
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(
            select(FileTable).where(
                and_(
                    FileTable.name == file_name,
                    FileTable.inst_id == str_to_uuid(inst_id),
                    FileTable.sst_generated,
                )
            )
        )
        .all()
    )
    # This should only result in a match of a single file.
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File does not exist or is not available for download.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="File duplicates found.",
        )
    if query_result[0][0].deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="File has been deleted.",
        )
    if query_result[0][0].sst_generated:
        if query_result[0][0].valid:
            file_name = "approved/" + file_name
        else:
            file_name = "unapproved/" + file_name
    else:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Only SST generated files can be downloaded.",
        )
    return storage_control.generate_download_signed_url(
        get_external_bucket_name(inst_id), file_name
    )


_AR_WORD = re.compile(r"(?<![A-Za-z0-9])ar(?![A-Za-z0-9])", re.IGNORECASE)


def infer_models_from_filename(file_path: str) -> List[str]:
    name = os.path.basename(file_path).lower()

    inferred = set()
    if "course" in name:
        inferred.add("COURSE")
    if "student" in name:
        inferred.add("STUDENT")
    if "semester" in name:
        inferred.add("SEMESTER")
    if "cohort" in name:
        inferred.add("STUDENT")
    if "course" not in name and (_AR_WORD.search(name) or "deidentified" in name):
        inferred.add("STUDENT")

    if not inferred:
        raise ValueError(
            f"Could not infer model(s) from file name: {name}. "
            "Filenames should be descriptive (e.g., include 'course', 'cohort', 'student', or 'semester')."
        )

    return sorted(inferred)


def validation_helper(
    source_str: str,
    inst_id: str,
    file_name: str,
    current_user: BaseUser,
    storage_control: StorageControl,
    sql_session: Session,
) -> Any:
    """Helper function for file validation (self-contained & optimized)."""
    import time
    import re
    import os

    # --- access check & quick input validation
    has_access_to_inst_or_err(inst_id, current_user)
    if "/" in file_name:
        raise HTTPException(status_code=422, detail="File name can't contain '/'.")

    # --- bind session once
    local_session.set(sql_session)
    sess = local_session.get()

    # --- one-time initialization on the function object (kept in-process)
    if not hasattr(validation_helper, "_ar_re"):
        validation_helper._ar_re = re.compile(
            r"(?<![A-Za-z0-9])ar(?![A-Za-z0-9])", re.IGNORECASE
        )
    if not hasattr(validation_helper, "_base_cache"):
        # {"exp": <monotonic expiry>, "val": (<schema_id>, <json_doc>)}
        validation_helper._base_cache = {"exp": 0.0, "val": None}
    if not hasattr(validation_helper, "_ext_cache"):
        # { str(inst_uuid): (exp, extension_json_doc) }
        validation_helper._ext_cache = {}
    if not hasattr(validation_helper, "_pdp_cache"):
        # PDP-wide extension (active), cached: (exp, doc)
        validation_helper._pdp_cache = (0.0, None)

    AR_RE = validation_helper._ar_re
    BASE_TTL = 300  # seconds
    EXT_TTL = 120  # seconds

    # --- filename → allowed_schemas (fast, single pass)
    name = os.path.basename(file_name).lower()
    has_course = "course" in name
    has_semester = "semester" in name
    has_student = (
        ("student" in name)
        or ("cohort" in name)
        or (
            (not has_course)
            and (AR_RE.search(name) is not None or "deidentified" in name)
        )
    )

    inferred_from_name: set[str] = set()
    if has_course:
        inferred_from_name.add("COURSE")
    if has_student:
        inferred_from_name.add("STUDENT")
    if has_semester:
        inferred_from_name.add("SEMESTER")

    if not inferred_from_name:
        raise ValueError(
            f"Could not infer model(s) from file name: {name}. "
            "Filenames should be descriptive (e.g., include 'course', 'cohort', 'student', or 'semester')."
        )

    allowed_schemas = sorted(inferred_from_name)

    # --- fetch active base schema (cached)
    now = time.monotonic()
    base_cache = validation_helper._base_cache
    if now < base_cache["exp"] and base_cache["val"] is not None:
        base_schema_id, base_schema = base_cache["val"]
    else:
        row = sess.execute(
            select(SchemaRegistryTable.schema_id, SchemaRegistryTable.json_doc)
            .where(
                SchemaRegistryTable.doc_type == DocType.base,
                SchemaRegistryTable.is_active.is_(True),
            )
            .limit(1)
        ).first()
        if row is None:
            raise RuntimeError("No active base schema found")
        base_schema_id, base_schema = row
        base_cache["exp"] = now + BASE_TTL
        base_cache["val"] = (base_schema_id, base_schema)

    # --- fetch institution record
    inst = sess.execute(
        select(InstTable).where(InstTable.id == str_to_uuid(inst_id))
    ).scalar_one_or_none()
    if inst is None:
        raise ValueError(f"Institution {inst_id} not found")

    bucket = get_external_bucket_name(inst_id)

    # --- choose / prepare extension schema (try to avoid heavy path)
    updated_inst_schema: Optional[dict] = None

    def _ext_models_set(doc: Optional[dict]) -> set[str]:
        """Extract model keys from an extension document (root or institutions.* layout)."""
        if not doc or not isinstance(doc, dict):
            return set()
        # root-level
        if isinstance(doc.get("data_models"), dict):
            return {str(k).lower() for k in doc["data_models"].keys()}
        # nested by institution
        inst_key_candidates = {str(getattr(inst, "id", "")), inst_id}
        insts = doc.get("institutions", {})
        if isinstance(insts, dict):
            for key in inst_key_candidates:
                block = insts.get(key)
                if isinstance(block, dict) and isinstance(
                    block.get("data_models"), dict
                ):
                    return {str(k).lower() for k in block["data_models"].keys()}
        return set()

    if getattr(inst, "pdp_id", None):
        # PDP institutions: use active PDP extension (cached)
        pdp_exp, pdp_doc = validation_helper._pdp_cache
        if now < pdp_exp and pdp_doc is not None:
            inst_schema = pdp_doc
        else:
            inst_schema = sess.execute(
                select(SchemaRegistryTable.json_doc)
                .where(
                    SchemaRegistryTable.is_pdp.is_(True),
                    SchemaRegistryTable.is_active.is_(True),
                )
                .limit(1)
            ).scalar_one_or_none()
            validation_helper._pdp_cache = (now + EXT_TTL, inst_schema)
        updated_inst_schema = inst_schema
    else:
        # custom institutions: try cached extension first
        ext_cache = validation_helper._ext_cache
        key = str(getattr(inst, "id", ""))
        cached = ext_cache.get(key)
        if cached and now < cached[0]:
            inst_schema = cached[1]
        else:
            inst_schema = sess.execute(
                select(SchemaRegistryTable.json_doc)
                .where(
                    SchemaRegistryTable.inst_id == getattr(inst, "id", None),
                    SchemaRegistryTable.is_active.is_(True),
                    SchemaRegistryTable.doc_type == DocType.extension,
                )
                .limit(1)
            ).scalar_one_or_none()
            ext_cache[key] = (now + EXT_TTL, inst_schema)

        # If extension already includes all inferred models, skip Databricks work.
        inferred_lower = {m.lower() for m in allowed_schemas}
        ext_models = _ext_models_set(inst_schema)
        if inferred_lower.issubset(ext_models):
            updated_inst_schema = inst_schema
        else:
            # heavy path only when needed
            dbc = DatabricksControl()
            schema_extension = dbc.create_custom_schema_extension(
                bucket_name=bucket,
                inst_query=inst,
                file_name=file_name,
                base_schema=base_schema,
                extension_schema=inst_schema,
            )
            if schema_extension is not None:
                updated_inst_schema = schema_extension
                try:
                    new_schema_extension_record = SchemaRegistryTable(
                        doc_type=DocType.extension,
                        inst_id=str_to_uuid(inst_id),
                        is_pdp=False,  # type: ignore
                        version_label="1.0.0",
                        extends_schema_id=base_schema_id,
                        json_doc=schema_extension,
                        is_active=True,
                    )
                    sess.add(new_schema_extension_record)
                    sess.flush()
                    logging.info("Schema record inserted for '%s'", inst_id)
                    # refresh cache
                    validation_helper._ext_cache[key] = (
                        time.monotonic() + EXT_TTL,
                        schema_extension,
                    )
                except IntegrityError as e:
                    sess.rollback()
                    logging.warning("IntegrityError: %s", e)
                except Exception as e:
                    sess.rollback()
                    logging.error("Unexpected DB error: %s", e)
                    raise HTTPException(
                        status_code=500,
                        detail=f"Unexpected database error while inserting file record: {e}",
                    )
            else:
                logging.info(
                    "No-op: extension already contains this model for inst %s", inst_id
                )
                updated_inst_schema = inst_schema

    # --- run file validation (I/O + Pandera work happens inside storage layer)
    try:
        inferred_schemas = storage_control.validate_file(
            bucket,
            file_name,
            allowed_schemas,
            base_schema,
            updated_inst_schema,
        )
        logging.debug("Inferred Schemas success %s", list(inferred_schemas))
    except HardValidationError as e:
        logging.debug("Inferred Schemas FAILED (hard) %s", e)
        parts = ["VALIDATION_FAILED"]
        if e.missing_required:
            parts.append(f"missing_required={e.missing_required}")
        if e.extra_columns:
            parts.append(f"extra_columns={e.extra_columns}")
        if e.schema_errors is not None:
            parts.append(f"schema_errors={e.schema_errors}")
        if e.failure_cases is not None:
            try:
                sample = (
                    e.failure_cases[:5]
                    if isinstance(e.failure_cases, list)
                    else str(e.failure_cases)[:500]
                )
            except Exception:
                sample = "see server logs"
            parts.append(f"failure_cases_sample={sample}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="; ".join(parts)
        )
    except Exception as e:
        logging.debug("Inferred Schemas FAILED (other) %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"VALIDATION_ERROR: {type(e).__name__}: {e}",
        )

    # --- upsert file record (cheap path)
    existing_file = (
        sess.query(FileTable)
        .filter_by(name=file_name, inst_id=str_to_uuid(inst_id))
        .first()
    )
    if set(inferred_schemas) != set(allowed_schemas):
        logging.info(
            "Filename inference %s differs from validator result %s for %s; "
            "returning filename-based types to preserve API contract.",
            allowed_schemas,
            inferred_schemas,
            file_name,
        )
    if existing_file:
        logging.info("File '%s' already exists for institution %s.", file_name, inst_id)
        db_status = f"File '{file_name}' already exists for institution {inst_id}."
    else:
        try:
            new_file_record = FileTable(
                name=file_name,
                inst_id=str_to_uuid(inst_id),
                uploader=str_to_uuid(current_user.user_id),  # type: ignore
                source=source_str,
                sst_generated=False,
                schemas=list(allowed_schemas),
                valid=True,
            )
            sess.add(new_file_record)
            sess.flush()
            logging.info("File record inserted for '%s'", file_name)
            db_status = f"File record inserted for '{file_name}'"
        except IntegrityError as e:
            sess.rollback()
            logging.warning("IntegrityError: %s", e)
            db_status = "Already exists"
        except Exception as e:
            sess.rollback()
            logging.error("Unexpected DB error: %s", e)
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected database error while inserting file record: {e}",
            )

    return {
        "name": file_name,
        "inst_id": inst_id,
        "file_types": list(allowed_schemas),
        "source": source_str,
        "status": db_status,
    }


@router.post(
    "/{inst_id}/input/validate-sftp/{file_name:path}", response_model=ValidationResult
)
def validate_file_sftp(
    inst_id: str,
    file_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Validate a given file pulled from SFTP. The file_name should be url encoded."""
    file_name = decode_url_piece(file_name)
    if not current_user.is_datakinder:  # type: ignore
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="SFTP validation needs to be done by a datakinder.",
        )
    return validation_helper(
        "PDP_SFTP", inst_id, file_name, current_user, storage_control, sql_session
    )


@router.post(
    "/{inst_id}/input/validate-upload/{file_name:path}", response_model=ValidationResult
)
def validate_file_manual_upload(
    inst_id: str,
    file_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Validate a given file. The file_name should be url encoded."""

    file_name = decode_url_piece(file_name)

    return validation_helper(
        "MANUAL_UPLOAD", inst_id, file_name, current_user, storage_control, sql_session
    )


@router.get("/{inst_id}/upload-url/{file_name:path}", response_model=str)
def get_upload_url(
    inst_id: str,
    file_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Returns a signed URL for uploading data to a specific institution."""
    file_name = decode_url_piece(file_name)
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    try:
        signed_url = storage_control.generate_upload_signed_url(
            get_external_bucket_name(inst_id), file_name
        )
        return signed_url
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


## FE Inference Tables


# Get SHAP Values for Inference
@router.get("/{inst_id}/inference/top-features/{job_run_id}")
def get_inference_top_features(
    inst_id: str,
    job_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> List[dict[str, Any]]:
    """Returns top n features table for a specific institution."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"inference_{job_run_id}_features_with_most_impact",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


# Get Box plot values
@router.get("/{inst_id}/inference/features-boxplot-stat/{job_run_id}")
def get_inference_feature_boxstats(
    inst_id: str,
    job_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    feature_name: Optional[str] = Query(
        None, description="If provided, filter by this feature name"
    ),
) -> List[dict[str, Any]]:
    """Returns box-plot stats for an institution/run. If `feature_name` is supplied,
    only rows for that feature are returned."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"inference_{job_run_id}_box_plot_table",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )
        if not feature_name:
            return rows

        # Helper: extract feature_name from various shapes (top-level or JSON column)
        def row_feature_name(row: dict[str, Any]) -> Optional[str]:
            # common case: it's a top-level column
            if "feature_name" in row and row["feature_name"] is not None:
                return str(row["feature_name"])
            # fallback: search any dict-valued column for a 'feature_name' key
            for v in row.values():
                if (
                    isinstance(v, dict)
                    and "feature_name" in v
                    and v["feature_name"] is not None
                ):
                    return str(v["feature_name"])
            return None

        filtered = [r for r in rows if row_feature_name(r) == feature_name]

        if not filtered:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Feature '{feature_name}' not found for run_id '{job_run_id}'.",
            )

        return filtered

    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


# Get SHAP Values for Inference
@router.get("/{inst_id}/inference/support-overview/{job_run_id}")
def get_inference_support_overview(
    inst_id: str,
    job_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> List[dict[str, Any]]:
    """Returns support score distribution table for a  specific institution."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"inference_{job_run_id}_support_overview",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/inference/feature_importance/{job_run_id}")
def get_inference_feature_importance(
    inst_id: str,
    job_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> List[dict[str, Any]]:
    """Returns feature importance table for a specific institution."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"inference_{job_run_id}_shap_feature_importance",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


## FE Training Tables


@router.get("/{inst_id}/training/feature_importance/{experiment_run_id}")
def get_training_feature_importance(
    inst_id: str,
    experiment_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> List[dict[str, Any]]:
    """Returns training feature importance table for a specific institution."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"training_{experiment_run_id}_shap_feature_importance",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/confusion_matrix/{experiment_run_id}")
def get_training_confusion_matrix(
    inst_id: str,
    experiment_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> List[dict[str, Any]]:
    """Returns training confusion matrix table for a specific instituion."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"training_{experiment_run_id}_confusion_matrix",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/roc_curve/{experiment_run_id}")
def get_training_roc_curve(
    inst_id: str,
    experiment_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> List[dict[str, Any]]:
    """Returns training roc curve table for a specific institution."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"training_{experiment_run_id}_roc_curve",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/support-overview/{experiment_run_id}")
def get_training_support_overview(
    inst_id: str,
    experiment_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> List[dict[str, Any]]:
    """Returns training support overview table for a specific institution."""
    # raise error at this level instead bc otherwise it's getting wrapped as a 200
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        dbc = DatabricksControl()
        rows = dbc.fetch_table_data(
            catalog_name=env_vars["CATALOG_NAME"],  # type: ignore
            inst_name=f"{query_result[0][0].name}",
            table_name=f"training_{experiment_run_id}_support_overview",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/model-cards/{model_name}")
def get_model_cards(
    inst_id: str,
    model_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> FileResponse:
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )
    if not query_result or len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    try:
        w = WorkspaceClient(
            host=databricks_vars["DATABRICKS_HOST_URL"],
            google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )

        LOGGER.info("Successfully created Databricks WorkspaceClient.")
    except Exception as e:
        LOGGER.exception(
            "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
            databricks_vars["DATABRICKS_HOST_URL"],
            gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )
        raise ValueError(
            f"get_model_cards(): Workspace client initialization failed: {e}"
        )

    try:
        volume_path = f"/Volumes/staging_sst_01/{databricksify_inst_name(query_result[0][0].name)}_gold/gold_volume/model_cards/model-card-{model_name}.pdf"
        LOGGER.info(f"Attempting to download from {volume_path}")
        response = w.files.download(volume_path)
        stream = cast(IO[bytes], response.contents)
        pdf_bytes = stream.read()

        LOGGER.info("Download successful, received %d bytes", len(pdf_bytes))
    except Exception as e:
        LOGGER.exception(f"Failed to fetch model card: {e}")
        raise HTTPException(500, detail=f"Failed to fetch model card: {e}")

    # Stream back as FileResponse
    tmp = tempfile.NamedTemporaryFile(suffix=".pdf", delete=False)
    tmp.write(pdf_bytes)
    tmp.flush()

    return FileResponse(
        tmp.name,
        filename=pathlib.Path(tmp.name).name,
        media_type="application/pdf",
    )
