"""API functions related to data."""

import json
import uuid
from datetime import datetime, date
from typing import Annotated, Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field
from fastapi import APIRouter, Depends, HTTPException, status, Response, Query
from sqlalchemy import and_, false, or_
from sqlalchemy.orm import Session
from sqlalchemy.future import select
import os
import logging
from sqlalchemy.exc import IntegrityError
import re
import requests
from ..validation import HardValidationError
from ..validation_error_formatter import format_validation_error
import pandas as pd
from cachetools import TTLCache

from ..utilities import (
    has_access_to_inst_or_err,
    has_at_most_one_school_type,
    has_full_data_access_or_err,
    BaseUser,
    model_owner_and_higher_or_err,
    uuid_to_str,
    str_to_uuid,
    get_current_active_user,
    DataSource,
    get_external_bucket_name,
    decode_url_piece,
    expand_batch_file_name_lookups,
    file_name_variants_for_lookup,
    SchemaType,
)

from ..database import (
    get_session,
    local_session,
    BatchTable,
    FileTable,
    InstTable,
    JobTable,
    ModelTable,
)

from ..databricks import (
    VALIDATED_BRONZE_SYNC_JOB_NAME,
    DatabricksBronzeSyncRequest,
    DatabricksControl,
)
from .models import require_named_inference_batch, require_named_inference_model
from ..gcsdbutils import update_db_from_bucket

from ..gcsutil import StorageControl
from ..config import ENV_TO_VOLUME_SCHEMA, env_vars
from edvise.data_audit.eda import EdaSummary
from edvise.shared.utils import cohort_pair_columns
from edvise.student_selection.filter_inference import exclude_training_cohort_students
from edvise.student_selection.select_students_attributes import (
    select_students_by_attributes,
)

# Set the logging
logging.basicConfig(format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def _gcs_bronze_sync_skip_reason(
    edvise_id: Optional[str],
    legacy_id: Optional[str],
    genai_id: Optional[str],
) -> Optional[str]:
    """
    If sync should not run, return a stable reason code; otherwise None.

    Used for logging (skip_reason when sync is not run).
    """
    if os.environ.get("ENABLE_GCS_BRONZE_SYNC_ON_VALIDATION", "true").lower() not in (
        "1",
        "true",
        "yes",
    ):
        return "env_disabled"
    if edvise_id is None and legacy_id is None and genai_id is None:
        return "not_edvise_legacy_or_genai"
    return None


def _log_validation_trace_json(event: str, **fields: Any) -> None:
    """Emit one JSON object per line for log aggregators (Cloud Logging, Datadog, etc.)."""
    payload: Dict[str, Any] = {"event": event, **fields}
    logger.info("%s", json.dumps(payload, default=str, separators=(",", ":")))


def _bronze_sync_trace_base(
    correlation_id: str, inst_id: str, bucket: str, file_name: str
) -> Dict[str, Any]:
    """Shared fields for GCS→bronze trace log lines."""
    return {
        "correlation_id": correlation_id,
        "inst_id": inst_id,
        "bucket": bucket,
        "file_name": file_name,
    }


def _log_bronze_sync_skipped(trace_base: Dict[str, Any], skip_reason: str) -> None:
    _log_validation_trace_json(
        "gcs_bronze_sync_background_done",
        **trace_base,
        outcome="skipped",
        skip_reason=skip_reason,
    )


def _log_bronze_sync_success(
    trace_base: Dict[str, Any],
    validated_blob_path: str,
    job_run_id: int,
) -> None:
    _log_validation_trace_json(
        "gcs_bronze_sync_background_done",
        **trace_base,
        outcome="success",
        validated_blob_path=validated_blob_path,
        databricks_job_run_id=job_run_id,
        databricks_job_name=VALIDATED_BRONZE_SYNC_JOB_NAME,
    )


def _log_bronze_sync_trigger_failed(
    trace_base: Dict[str, Any], validated_blob_path: str, correlation_id: str
) -> None:
    _log_validation_trace_json(
        "gcs_bronze_sync_background_done",
        **trace_base,
        outcome="trigger_failed",
        validated_blob_path=validated_blob_path,
        databricks_job_name=VALIDATED_BRONZE_SYNC_JOB_NAME,
    )
    logger.exception(
        "Failed to trigger GCS→bronze Databricks job after validation (non-fatal). "
        "correlation_id=%s",
        correlation_id,
    )


def _attempt_gcs_bronze_sync_trigger(
    inst_name: str,
    bucket: str,
    validated_blob_paths: list[str],
    batch_id: str,
    databricks_control: DatabricksControl,
    trace_base: Dict[str, Any],
    correlation_id: str,
) -> None:
    """Call Databricks to start the bronze sync job and log success."""
    sync_resp = databricks_control.run_validated_gcs_to_bronze_sync(
        DatabricksBronzeSyncRequest(
            inst_name=inst_name,
            gcp_bucket_name=bucket,
            validated_blob_paths=validated_blob_paths,
            batch_id=batch_id,
        )
    )
    path_for_log = (
        validated_blob_paths[0]
        if len(validated_blob_paths) == 1
        else f"{len(validated_blob_paths)}_objects"
    )
    _log_bronze_sync_success(trace_base, path_for_log, sync_resp.job_run_id)


def _load_batch_for_bronze_sync(
    sess: Session, inst_id: str, batch_id: str
) -> BatchTable:
    """Return the batch row or raise HTTPException when batch_id is invalid."""
    try:
        batch = sess.execute(
            select(BatchTable).where(
                and_(
                    BatchTable.id == str_to_uuid(batch_id),
                    BatchTable.inst_id == str_to_uuid(inst_id),
                )
            )
        ).scalar_one_or_none()
    except (ValueError, TypeError):
        batch = None
    if batch is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Batch not found.",
        )
    if batch.deleted:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Batch is set for deletion, no modifications allowed.",
        )
    return batch


def _trigger_gcs_bronze_sync_if_applicable(
    inst_name: str,
    edvise_id: Optional[str],
    legacy_id: Optional[str],
    genai_id: Optional[str],
    inst_id: str,
    file_name: str,
    bucket: str,
    databricks_control: DatabricksControl,
    correlation_id: str,
    batch_id: Optional[str],
) -> None:
    """Trigger Databricks job to copy validated/ into bronze without waiting for the copy."""
    trace_base = _bronze_sync_trace_base(correlation_id, inst_id, bucket, file_name)
    if batch_id is not None:
        trace_base["batch_id"] = batch_id
    _log_validation_trace_json("gcs_bronze_sync_background_start", **trace_base)

    skip_reason = _gcs_bronze_sync_skip_reason(edvise_id, legacy_id, genai_id)
    if skip_reason is not None:
        _log_bronze_sync_skipped(trace_base, skip_reason)
        return
    if batch_id is None:
        _log_bronze_sync_skipped(trace_base, "no_batch_id")
        return

    validated_blob_path = f"validated/{file_name}"
    try:
        _attempt_gcs_bronze_sync_trigger(
            inst_name,
            bucket,
            [validated_blob_path],
            batch_id,
            databricks_control,
            trace_base,
            correlation_id,
        )
    except Exception:
        _log_bronze_sync_trigger_failed(trace_base, validated_blob_path, correlation_id)


def _trigger_batch_bronze_sync_if_applicable(
    inst: InstTable,
    inst_id: str,
    batch_id: str,
    file_names: list[str],
    databricks_control: DatabricksControl,
) -> None:
    """After batch creation, copy validated batch files into bronze under batch_id."""
    skip_reason = _gcs_bronze_sync_skip_reason(
        inst.edvise_id, inst.legacy_id, inst.genai_id
    )
    if skip_reason is not None or not file_names:
        return

    correlation_id = str(uuid.uuid4())
    bucket = get_external_bucket_name(inst_id)
    trace_base: Dict[str, Any] = {
        "correlation_id": correlation_id,
        "inst_id": inst_id,
        "bucket": bucket,
        "batch_id": batch_id,
        "trigger_source": "batch_create",
    }
    _log_validation_trace_json("gcs_bronze_sync_background_start", **trace_base)

    validated_blob_paths = [f"validated/{name}" for name in file_names]
    try:
        _attempt_gcs_bronze_sync_trigger(
            inst.name,
            bucket,
            validated_blob_paths,
            batch_id,
            databricks_control,
            trace_base,
            correlation_id,
        )
    except Exception:
        _log_bronze_sync_trigger_failed(
            trace_base, validated_blob_paths[0], correlation_id
        )


# Cache for EDA data - TTL of 10 minutes (600 seconds)
# Cache key format: f"{inst_id}:{batch_id}"
EDA_CACHE_TTL = int(os.getenv("EDA_CACHE_TTL", "600"))  # Default 10 minutes
EDA_CACHE: Any = TTLCache(maxsize=64, ttl=EDA_CACHE_TTL)

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


class BronzeImportRequest(BaseModel):
    """Request to import a dataset from the institution's bronze volume into GCS."""

    name: str


class BronzeImportResponse(BaseModel):
    """Response for bronze import request."""

    file_name: str
    message: str


def _upload_file_bytes_to_signed_url(file_bytes: bytes, upload_signed_url: str) -> None:
    """Upload file bytes to a signed GCS URL using the same request shape as the worker path."""
    upload_response = requests.put(
        upload_signed_url,
        data=file_bytes,
        headers={"Content-Type": "text/csv"},
        timeout=600,
    )
    if upload_response.status_code != 200:
        raise requests.RequestException(
            f"{upload_response.status_code} {upload_response.text}"
        )


class DataOverview(BaseModel):
    """All data for a given institution (batches and files)."""

    batches: list[BatchInfo]
    files: list[DataInfo]


class EligibleInferenceTerm(BaseModel):
    """An academic term with students eligible for inference."""

    term_label: str
    valid_student_count: int


class EligibleInferenceTermsResponse(BaseModel):
    """Eligible academic terms for a model and input batch."""

    status: Literal["valid", "invalid"]
    batch_name: str | None = None
    terms: list[EligibleInferenceTerm] = Field(default_factory=list)
    reason: str | None = None


_ACADEMIC_TERM_ORDER = {"FALL": 1, "WINTER": 2, "SPRING": 3, "SUMMER": 4}
_STUDENT_ID_COLUMNS = ("student_id", "study_id", "learner_id")
_EDA_BATCH_READ_SUFFIX = (
    "Files must be uploaded and validated before they can be used for EDA."
)
_ELIGIBLE_TERMS_BATCH_READ_SUFFIX = (
    "Files must be uploaded and validated before they can be used "
    "for inference eligibility."
)


def _invalid_eligible_terms(
    reason: str, batch_name: str | None = None
) -> EligibleInferenceTermsResponse:
    """Build a consistent invalid eligible-terms response."""
    logger.warning("Eligible inference terms unavailable: %s", reason)
    return EligibleInferenceTermsResponse(
        status="invalid",
        batch_name=batch_name,
        reason=reason,
    )


def _academic_year_sort_value(value: object) -> int:
    """Return the ending year from an academic-year label."""
    years = re.findall(r"\d{2,4}", str(value))
    if not years:
        return -1
    year = int(years[-1])
    return year if year >= 100 else 2000 + year


def _normalize_identifier_series(values: pd.Series) -> pd.Series:
    """Canonicalize identifiers so int, float, and string forms join."""
    stripped = values.astype("string").str.strip()
    numeric = pd.to_numeric(stripped, errors="coerce")
    is_whole_number = numeric.notna() & (numeric == numeric.round())
    result = stripped.fillna("")
    if bool(is_whole_number.any()):
        whole_as_int = numeric.loc[is_whole_number].astype("int64").astype(str)
        result = result.mask(is_whole_number, whole_as_int)
    return result.replace({"<NA>": "", "nan": "", "None": "", "NaN": ""})


def _shared_student_id_column(
    students: pd.DataFrame,
    courses: pd.DataFrame,
    preferred: str | None = None,
) -> str | None:
    """Return the config student id column, else a supported identifier in both files."""
    if preferred and preferred in students.columns and preferred in courses.columns:
        return preferred
    for column in _STUDENT_ID_COLUMNS:
        if column in students.columns and column in courses.columns:
            return column
    return None


def _eligible_terms_reason_from_batch_read(detail: object) -> str:
    """Map batch-read errors to eligible-terms wording."""
    return str(detail).replace(
        _EDA_BATCH_READ_SUFFIX, _ELIGIBLE_TERMS_BATCH_READ_SUFFIX
    )


def _project_schema_type_for_institution(institution: Any) -> str | None:
    """Map an institution's school type to an edvise project config schema."""
    pdp_id = getattr(institution, "pdp_id", None)
    edvise_id = getattr(institution, "edvise_id", None)
    legacy_id = getattr(institution, "legacy_id", None)
    genai_id = getattr(institution, "genai_id", None)
    if not has_at_most_one_school_type(pdp_id, edvise_id, legacy_id, genai_id):
        return None
    if pdp_id:
        return "pdp"
    if edvise_id or genai_id:
        return "edvise"
    if legacy_id:
        return "legacy"
    return None


def _exclude_training_cohorts(
    students: pd.DataFrame,
    training_cohorts: list[str],
) -> tuple[pd.DataFrame, str | None]:
    """Exclude students whose entry cohort was used for model training."""
    if not training_cohorts:
        return students, None
    cohort_pair = cohort_pair_columns(students)
    if cohort_pair is None:
        return (
            students.iloc[0:0],
            "Student file is missing entry cohort columns needed to exclude "
            "training cohorts.",
        )
    year_column, term_column = cohort_pair
    normalized_students = students.copy()
    for column in (year_column, term_column):
        normalized_students[column] = normalized_students[column].map(
            lambda value: value.strip() if isinstance(value, str) else value
        )
    try:
        filtered_students = exclude_training_cohort_students(
            normalized_students,
            training_cohorts=training_cohorts,
            cohort_term_column=term_column,
            cohort_column=year_column,
        )
        return students.loc[filtered_students.index].copy(), None
    except ValueError as exc:
        return students.iloc[0:0], str(exc)


def _apply_student_criteria(
    students: pd.DataFrame,
    config: dict[str, Any],
    student_id_column: str,
) -> pd.DataFrame:
    """Apply upload-available training selection criteria via edvise."""
    criteria = config.get("student_criteria")
    if not isinstance(criteria, dict) or not criteria:
        return students
    applicable = {
        column: value for column, value in criteria.items() if column in students
    }
    skipped = sorted(str(column) for column in criteria if column not in students)
    if skipped:
        logger.warning(
            "Skipping student_criteria columns missing from the student file: %s",
            ", ".join(skipped),
        )
    if not applicable:
        return students
    work = students.copy()
    prepared: dict[str, Any] = {}
    for column, value in applicable.items():
        if isinstance(value, list) and all(isinstance(item, str) for item in value):
            work[column] = (
                work[column].astype("string").str.strip().str.casefold().fillna("")
            )
            prepared[column] = [item.strip().casefold() for item in value]
        elif isinstance(value, str):
            work[column] = (
                work[column].astype("string").str.strip().str.casefold().fillna("")
            )
            prepared[column] = value.strip().casefold()
        else:
            prepared[column] = value
    selected = select_students_by_attributes(
        work, student_id_cols=student_id_column, **prepared
    )
    selected_ids = set(
        _normalize_identifier_series(pd.Series(selected.index))
        .replace("", pd.NA)
        .dropna()
    )
    student_ids = _normalize_identifier_series(students[student_id_column])
    return students[student_ids.isin(selected_ids)].copy()


def resolve_eligible_inference_terms(
    students: pd.DataFrame,
    courses: pd.DataFrame,
    config: dict[str, Any],
    batch_name: str | None = None,
) -> EligibleInferenceTermsResponse:
    """Return newest-first academic terms containing eligible students."""
    required_course_columns = {"academic_term", "academic_year"}
    missing_course_columns = sorted(required_course_columns - set(courses.columns))
    if missing_course_columns:
        return _invalid_eligible_terms(
            "Course file is missing academic term columns: "
            + ", ".join(missing_course_columns)
            + ".",
            batch_name,
        )

    student_id_column = _shared_student_id_column(
        students, courses, config.get("student_id_col")
    )
    if student_id_column is None:
        return _invalid_eligible_terms(
            "Student and course files must share student_id, study_id, or learner_id.",
            batch_name,
        )

    selected_students = _apply_student_criteria(students, config, student_id_column)
    selected_students, cohort_error = _exclude_training_cohorts(
        selected_students,
        config.get("training_cohorts", []),
    )
    if cohort_error is not None:
        return _invalid_eligible_terms(cohort_error, batch_name)

    eligible_student_ids = set(
        _normalize_identifier_series(selected_students[student_id_column])
        .replace("", pd.NA)
        .dropna()
        .unique()
    )
    normalized_course_ids = _normalize_identifier_series(courses[student_id_column])
    eligible_courses = courses[normalized_course_ids.isin(eligible_student_ids)].copy()
    eligible_courses = eligible_courses[
        eligible_courses["academic_term"].notna()
        & eligible_courses["academic_year"].notna()
    ].copy()
    eligible_courses["_academic_term_label"] = (
        eligible_courses["academic_term"].astype(str).str.strip().str.lower()
    )
    eligible_courses["_academic_year_label"] = (
        eligible_courses["academic_year"].astype(str).str.strip().str.lower()
    )
    eligible_courses = eligible_courses[
        (eligible_courses["_academic_term_label"] != "")
        & (eligible_courses["_academic_year_label"] != "")
    ]

    terms: list[EligibleInferenceTerm] = []
    for (academic_year, academic_term), rows in eligible_courses.groupby(
        ["_academic_year_label", "_academic_term_label"]
    ):
        valid_student_count = int(
            _normalize_identifier_series(rows[student_id_column])
            .replace("", pd.NA)
            .nunique(dropna=True)
        )
        if valid_student_count:
            terms.append(
                EligibleInferenceTerm(
                    term_label=f"{academic_term} {academic_year}",
                    valid_student_count=valid_student_count,
                )
            )
    terms.sort(
        key=lambda term: (
            _academic_year_sort_value(term.term_label),
            _ACADEMIC_TERM_ORDER.get(term.term_label.split(" ", 1)[0].upper(), -1),
        ),
        reverse=True,
    )
    if not terms:
        return _invalid_eligible_terms(
            "No academic term has eligible students with course data.",
            batch_name,
        )
    return EligibleInferenceTermsResponse(
        status="valid",
        batch_name=batch_name,
        terms=terms,
    )


@router.get(
    "/{inst_id}/eligible-inference-terms",
    response_model=EligibleInferenceTermsResponse,
)
def get_eligible_inference_terms(
    inst_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
    model_name: str = Query(...),
    batch_name: str = Query(...),
) -> EligibleInferenceTermsResponse:
    """Return academic terms eligible for inference for a selected batch."""
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    session = local_session.get()
    institution = session.scalar(
        select(InstTable).where(InstTable.id == str_to_uuid(inst_id))
    )
    if institution is None:
        return _invalid_eligible_terms("Institution not found.")

    model = require_named_inference_model(session, inst_id, model_name)
    batch = require_named_inference_batch(session, inst_id, batch_name)
    try:
        model_version = databricks_control.fetch_model_version(
            catalog_name=str(env_vars["CATALOG_NAME"]),
            inst_name=institution.name,
            model_name=model.name,
        )
    except Exception:
        logger.exception("Could not resolve latest model version for %s", model.name)
        return _invalid_eligible_terms(
            "Latest model version could not be resolved.", batch.name
        )
    model_run_id = getattr(model_version, "run_id", None)
    if not model_run_id:
        return _invalid_eligible_terms(
            "Latest model version has no training run id.", batch.name
        )

    current_env = str(env_vars["ENV"]).strip().upper()
    if current_env not in ENV_TO_VOLUME_SCHEMA:
        return _invalid_eligible_terms(
            f"Training config volumes are not configured for ENV={current_env}.",
            batch.name,
        )
    schema_type = _project_schema_type_for_institution(institution)
    if schema_type is None:
        return _invalid_eligible_terms(
            "Institution schema type could not be resolved.", batch.name
        )
    config = databricks_control.read_volume_training_config(
        institution.name, str(model_run_id), schema_type
    )
    if config is None:
        return _invalid_eligible_terms(
            "Training config could not be loaded.", batch.name
        )

    try:
        dataframes = read_batch_files_as_dataframes(
            inst_id, batch.files, storage_control
        )
    except HTTPException as exc:
        return _invalid_eligible_terms(
            _eligible_terms_reason_from_batch_read(exc.detail),
            batch.name,
        )
    students = dataframes.get(SchemaType.STUDENT.value)
    courses = dataframes.get(SchemaType.COURSE.value)
    if students is None or courses is None:
        return _invalid_eligible_terms(
            "Batch is missing readable student or course data.",
            batch.name,
        )
    return resolve_eligible_inference_terms(students, courses, config, batch.name)


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


## EDA (Exploratory Data Analysis) Endpoints
class SummaryStats(BaseModel):
    """Summary statistics for the EDA dashboard."""

    total_students: int
    transfer_students: int
    avg_year1_gpa_all_students: float


class SummaryMetric(BaseModel):
    """A named metric with a single value (e.g. for EDA summary stats)."""

    name: str
    value: Union[int, float]


class GpaSeriesData(BaseModel):
    """GPA data series for a chart."""

    name: str
    data: List[Optional[float]]


class GpaChartData(BaseModel):
    """GPA chart data with cohort years and series."""

    cohort_years: List[str]
    series: List[GpaSeriesData]
    min_gpa: Optional[float] = None


class TermCountPct(BaseModel):
    count: int
    percentage: float
    name: str


class YearTermSummary(BaseModel):
    year: str
    total: int
    terms: List[TermCountPct]


class StudentsByCohortTerm(BaseModel):
    years: List[str]
    by_year: List[YearTermSummary]


class TermChartData(BaseModel):
    """Chart-ready term data: cohort_years, terms."""

    cohort_years: List[str]
    terms: List[Dict[str, Any]]


class EdaDataResponse(BaseModel):
    """EDA API response: summary metrics, GPA/time-series data, and category+series blobs."""

    total_students: Optional[SummaryMetric] = None
    transfer_students: Optional[SummaryMetric] = None
    avg_year1_gpa_all_students: Optional[SummaryMetric] = None
    gpa_by_enrollment_type: Optional[GpaChartData] = None
    gpa_by_enrollment_intensity: Optional[GpaChartData] = None
    students_by_cohort_term: Optional[StudentsByCohortTerm] = None
    course_enrollments: Optional[StudentsByCohortTerm] = None
    degree_types: Optional[Dict[str, Any]] = (
        None  # { "total": int, "degrees": [{ "count", "percentage", "name" }, ...] }
    )
    enrollment_type_by_intensity: Optional[Dict[str, Any]] = None
    pell_recipient_status: Optional[Dict[str, Any]] = None
    pell_recipient_by_first_gen: Optional[Dict[str, Any]] = None
    student_age_by_gender: Optional[Dict[str, Any]] = None
    race_by_pell_status: Optional[Dict[str, Any]] = None

    @classmethod
    def from_eda_summary(cls, eda: Any) -> "EdaDataResponse":
        return cls(
            total_students=eda.total_students,
            transfer_students=eda.transfer_students,
            avg_year1_gpa_all_students=eda.avg_year1_gpa_all_students,
            gpa_by_enrollment_type=eda.gpa_by_enrollment_type,
            gpa_by_enrollment_intensity=eda.gpa_by_enrollment_intensity,
            students_by_cohort_term=eda.students_by_cohort_term,
            course_enrollments=eda.course_enrollments,
            degree_types=eda.degree_types,
            enrollment_type_by_intensity=eda.enrollment_type_by_intensity,
            pell_recipient_status=eda.pell_recipient_status,
            pell_recipient_by_first_gen=eda.pell_recipient_by_first_gen,
            student_age_by_gender=eda.student_age_by_gender,
            race_by_pell_status=eda.race_by_pell_status,
        )


def read_batch_files_as_dataframes(
    inst_id: str,
    batch_files: Any,  # Set[FileTable]
    storage_control: StorageControl,
) -> Dict[str, pd.DataFrame]:
    """Read CSV files from a batch and return as DataFrames.

    Args:
        inst_id: Institution ID
        batch_files: Set of FileTable objects from the batch
        storage_control: StorageControl instance for GCS access

    Returns:
        Dictionary mapping schema_type -> pandas.DataFrame

    Raises:
        HTTPException: If no valid files found
    """
    bucket_name = get_external_bucket_name(inst_id)

    # Temporary storage: file_record -> DataFrame
    loaded_files: Dict[Any, pd.DataFrame] = {}
    missing_files: List[str] = []

    for file_record in batch_files:
        file_name = file_record.name

        # Skip SST-generated output files (only process input files)
        if file_record.sst_generated:
            logger.debug(f"Skipping SST-generated file: {file_name}")
            continue

        df = None

        # Read from GCS
        try:
            blob_path = f"validated/{file_name}"
            df = storage_control.read_csv_as_dataframe(bucket_name, blob_path)
            logger.info(f"Loaded {file_name} from GCS ({len(df)} rows)")
        except ValueError as e:
            logger.warning(f"File not found in GCS: {e}")
            missing_files.append(file_name)
        except Exception as e:
            logger.error(f"Failed to read from GCS: {e}")
            missing_files.append(file_name)

        if df is not None:
            loaded_files[file_record] = df

    if not loaded_files:
        error_msg = f"No valid input files found in batch (checked GCS: {bucket_name}/validated/)"
        if missing_files:
            error_msg += f". Expected files not found: {', '.join(missing_files[:5])}"
            if len(missing_files) > 5:
                error_msg += f" (and {len(missing_files) - 5} more)"
        error_msg += (
            ". Files must be uploaded and validated before they can be used for EDA."
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=error_msg,
        )

    # Group by schema type and combine DataFrames
    schema_dataframes: Dict[str, List[pd.DataFrame]] = {}
    for file_record, df in loaded_files.items():
        for schema in file_record.schemas:
            if schema not in schema_dataframes:
                schema_dataframes[schema] = []
            schema_dataframes[schema].append(df)

    result = {}
    for schema, dfs in schema_dataframes.items():
        if len(dfs) == 1:
            result[schema] = dfs[0]
        else:
            result[schema] = pd.concat(dfs, ignore_index=True)
            logger.info(
                f"Combined {len(dfs)} files for schema {schema} ({len(result[schema])} total rows)"
            )

    return result


@router.get("/{inst_id}/batch/{batch_id}/eda", response_model=EdaDataResponse)
def get_eda_data(
    inst_id: str,
    batch_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
    clear_cache: Annotated[Optional[str], Query(alias="clear-cache")] = None,
) -> Any:
    """Returns EDA (Exploratory Data Analysis) data for a specific batch.

    This endpoint provides all the data needed to populate the EDA dashboard,
    including summary statistics, GPA charts, enrollment data, and demographic breakdowns.
    Analyzes all files in the batch together to provide comprehensive insights.
    Pass query ``clear-cache=1`` to drop any cached EDA result for this batch before serving.
    """
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "EDA data")
    local_session.set(sql_session)

    batch_result = (
        local_session.get()
        .execute(
            select(BatchTable).where(
                and_(
                    BatchTable.id == str_to_uuid(batch_id),
                    BatchTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .scalar_one_or_none()
    )
    if batch_result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Batch not found.",
        )

    cache_key = f"{inst_id}:{batch_id}"
    if clear_cache == "1":
        EDA_CACHE.pop(cache_key, None)

    cached_result = EDA_CACHE.get(cache_key)
    if cached_result is not None:
        logger.debug(f"EDA cache hit for {cache_key}")
        return cached_result
    logger.debug(f"EDA cache miss for {cache_key}, computing...")

    file_dataframes = read_batch_files_as_dataframes(
        inst_id, batch_result.files, storage_control
    )
    df_cohort = file_dataframes.get("STUDENT")
    if df_cohort is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No STUDENT schema files found in batch for EDA.",
        )

    eda = EdaSummary(
        df_cohort=df_cohort,
        df_course=file_dataframes.get("COURSE"),
    )
    result = EdaDataResponse.from_eda_summary(eda)
    EDA_CACHE[cache_key] = result

    return result


@router.post("/{inst_id}/batch", response_model=BatchInfo)
def create_batch(
    inst_id: str,
    req: BatchCreationRequest,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
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
    created_new_batch = False
    if len(query_result) == 0:
        batch = BatchTable(
            name=req.name,
            inst_id=str_to_uuid(inst_id),
            created_by=str_to_uuid(current_user.user_id),  # type: ignore
        )
        f_names = [] if not req.file_names else list(req.file_names)
        f_ids = [] if not req.file_ids else strs_to_uuids(req.file_ids)
        file_match_parts: List[Any] = []
        if f_ids:
            file_match_parts.append(FileTable.id.in_(f_ids))
        if f_names:
            file_match_parts.append(
                FileTable.name.in_(expand_batch_file_name_lookups(f_names))
            )
        file_clause = or_(*file_match_parts) if file_match_parts else false()
        # Check that the files requested for this batch exists.
        # Only valid non-sst generated files can be added to a batch at creation time.
        query_result_files = (
            local_session.get()
            .execute(
                select(FileTable).where(
                    and_(
                        file_clause,
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
        created_new_batch = True
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Batch with this name already exists.",
        )
    batch_row = query_result[0][0]
    batch_id_str = uuid_to_str(batch_row.id)
    inst = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .scalar_one_or_none()
    )
    if inst is not None and created_new_batch:
        _trigger_batch_bronze_sync_if_applicable(
            inst,
            inst_id,
            batch_id_str,
            [f.name for f in batch_row.files],
            databricks_control,
        )
    return {
        "batch_id": batch_id_str,
        "inst_id": uuid_to_str(batch_row.inst_id),
        "name": batch_row.name,
        "file_names_to_ids": {x.name: uuid_to_str(x.id) for x in batch_row.files},
        "created_by": uuid_to_str(batch_row.created_by),
        "deleted": False,
        "completed": False,
        "deletion_request_time": None,
        "created_at": batch_row.created_at,
        "updated_by": uuid_to_str(batch_row.updated_by),
        "updated_at": batch_row.updated_at,
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
            name_variants = list(file_name_variants_for_lookup(f))
            query_result_file = (
                local_session.get()
                .execute(
                    select(FileTable).where(
                        and_(
                            FileTable.name.in_(name_variants)
                            if name_variants
                            else false(),
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


class _ValidationState:
    _ar_re = re.compile(r"(?<![A-Za-z0-9])ar(?![A-Za-z0-9])", re.IGNORECASE)


STATE = _ValidationState()


def _infer_allowed_schemas_from_filename(file_name: str, inst: Any) -> List[str]:
    """Infer allowed schema names from file name; legacy/genai may use any name (UNKNOWN).

    Args:
        file_name: Name of the file (used for keyword inference).
        inst: Institution row (legacy_id or genai_id enables arbitrary-name UNKNOWN fallback).

    Returns:
        Sorted list of allowed schema names (e.g. ["COURSE"], ["STUDENT"], ["UNKNOWN"]).

    Raises:
        HTTPException: 422 if name is non-descriptive and institution is not legacy/genai.
    """
    name = os.path.basename(file_name).lower()
    has_course = "course" in name
    has_semester = "semester" in name
    has_student = (
        ("student" in name)
        or ("cohort" in name)
        or (
            (not has_course)
            and (STATE._ar_re.search(name) is not None or "deidentified" in name)
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
        if getattr(inst, "legacy_id", None) or getattr(inst, "genai_id", None):
            return ["UNKNOWN"]
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Could not infer model(s) from file name: {name}. "
                "Filenames should be descriptive (e.g., include 'course', 'cohort', "
                "'student', or 'semester')."
            ),
        )
    return sorted(inferred_from_name)


def _resolve_schema_namespace(inst: Any) -> str:
    """Resolve validation namespace by institution type (edvise/pdp/legacy/genai)."""
    pdp_id = getattr(inst, "pdp_id", None)
    edvise_id = getattr(inst, "edvise_id", None)
    legacy_id = getattr(inst, "legacy_id", None)
    genai_id = getattr(inst, "genai_id", None)
    if not has_at_most_one_school_type(pdp_id, edvise_id, legacy_id, genai_id):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution configuration error: cannot have more than one of "
            "pdp_id, edvise_id, legacy_id, or genai_id set",
        )
    if edvise_id:
        return "edvise"
    if pdp_id:
        return "pdp"
    if legacy_id or genai_id:
        return "legacy"
    raise HTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        detail=(
            "Institution configuration error: institution has no pdp_id, edvise_id, "
            "legacy_id, or genai_id; cannot resolve validation schema."
        ),
    )


def _run_validation_and_upsert_file_record(
    bucket: str,
    file_name: str,
    allowed_schemas: List[str],
    schema_namespace: str,
    inst_id: str,
    source_str: str,
    current_user: BaseUser,
    storage_control: StorageControl,
    sess: Session,
    institution_name: Optional[str] = None,
) -> Dict[str, Any]:
    """Run storage validate_file, then upsert file record and return response dict."""
    try:
        inferred_schemas = storage_control.validate_file(
            bucket,
            file_name,
            allowed_schemas,
            institution_id=schema_namespace,
            institution_identifier=(
                institution_name if schema_namespace == "edvise" else None
            ),
        )
    except HardValidationError as e:
        logging.debug("Inferred Schemas FAILED (hard) %s", e)
        try:
            formatted_msg = format_validation_error(e)
        except Exception as format_err:
            logging.warning("Error formatting validation message: %s", format_err)
            parts = ["VALIDATION_FAILED"]
            if e.missing_required:
                parts.append(f"missing_required={e.missing_required}")
            if e.extra_columns:
                parts.append(f"extra_columns={e.extra_columns}")
            if e.schema_errors is not None:
                parts.append(f"schema_errors={e.schema_errors}")
            formatted_msg = "; ".join(parts)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=formatted_msg
        )
    except Exception as e:
        logging.debug("Inferred Schemas FAILED (other) %s", e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"VALIDATION_ERROR: {type(e).__name__}: {e}",
        )
    logging.debug("Inferred Schemas success %s", list(inferred_schemas))
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


def validation_helper(
    source_str: str,
    inst_id: str,
    file_name: str,
    current_user: BaseUser,
    storage_control: StorageControl,
    sql_session: Session,
    databricks_control: DatabricksControl,
    batch_id: Optional[str] = None,
) -> Any:
    """Run file validation for an institution and upsert the file record.

    Validates file name and institution, infers allowed schemas from filename
    (or UNKNOWN for legacy when inference fails), resolves validation namespace,
    runs storage validation, then upserts the file record.

    Args:
        source_str: Source label for the upload (e.g. MANUAL_UPLOAD).
        inst_id: Institution UUID (hex string).
        file_name: Name of the file (no path separators).
        current_user: Authenticated user; must have access to inst_id.
        storage_control: StorageControl instance for GCS and validate_file.
        sql_session: DB session for institution and file record.
        databricks_control: Starts the GCS→Databricks bronze sync after validation when
            batch_id is provided (non-PDP schools only).

    Returns:
        Dict with name, inst_id, file_types, source, status.

    Raises:
        HTTPException: 401 if no access, 404 if institution not found or invalid id,
            422 if file name invalid or non-descriptive (non-legacy), 400 on validation failure.
    """
    has_access_to_inst_or_err(inst_id, current_user)
    if not file_name or not file_name.strip():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="File name is required and must be non-empty.",
        )
    if "/" in file_name:
        raise HTTPException(status_code=422, detail="File name can't contain '/'.")

    local_session.set(sql_session)
    sess = local_session.get()

    try:
        inst = sess.execute(
            select(InstTable).where(InstTable.id == str_to_uuid(inst_id))
        ).scalar_one_or_none()
    except (ValueError, TypeError):
        logging.warning("Invalid institution id for validation: %s", inst_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found or invalid identifier.",
        )
    if inst is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Institution {inst_id} not found",
        )

    allowed_schemas = _infer_allowed_schemas_from_filename(file_name, inst)
    bucket = get_external_bucket_name(inst_id)
    correlation_id = str(uuid.uuid4())
    _log_validation_trace_json(
        "validation_request",
        correlation_id=correlation_id,
        inst_id=inst_id,
        bucket=bucket,
        file_name=file_name,
        validation_source=source_str,
    )
    schema_namespace = _resolve_schema_namespace(inst)
    result = _run_validation_and_upsert_file_record(
        bucket,
        file_name,
        allowed_schemas,
        schema_namespace,
        inst_id,
        source_str,
        current_user,
        storage_control,
        sess,
        institution_name=inst.name,
    )
    if batch_id is not None:
        _load_batch_for_bronze_sync(sess, inst_id, batch_id)
    # GCS validated/ write is complete; start the Databricks run now. The API waits
    # only for run_now to return a run id, not for cluster startup or file copying.
    _trigger_gcs_bronze_sync_if_applicable(
        inst.name,
        inst.edvise_id,
        inst.legacy_id,
        inst.genai_id,
        inst_id,
        file_name,
        bucket,
        databricks_control,
        correlation_id,
        batch_id,
    )
    return result


@router.post(
    "/{inst_id}/input/validate-sftp/{file_name:path}", response_model=ValidationResult
)
def validate_file_sftp(
    inst_id: str,
    file_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
    sql_session: Annotated[Session, Depends(get_session)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
    batch_id: Annotated[Optional[str], Query()] = None,
) -> Any:
    """Validate a given file pulled from SFTP. The file_name should be url encoded."""
    file_name = decode_url_piece(file_name)
    if not current_user.is_datakinder:  # type: ignore
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="SFTP validation needs to be done by a datakinder.",
        )
    return validation_helper(
        "PDP_SFTP",
        inst_id,
        file_name,
        current_user,
        storage_control,
        sql_session,
        databricks_control,
        batch_id=batch_id,
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
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
    batch_id: Annotated[Optional[str], Query()] = None,
) -> Any:
    """Validate a given file. The file_name should be url encoded."""

    file_name = decode_url_piece(file_name)

    return validation_helper(
        "MANUAL_UPLOAD",
        inst_id,
        file_name,
        current_user,
        storage_control,
        sql_session,
        databricks_control,
        batch_id=batch_id,
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


@router.get("/{inst_id}/input/bronze-datasets", response_model=list[str])
def list_bronze_datasets(
    inst_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
) -> Any:
    """List `.csv` files directly under the institution's Databricks bronze volume root."""
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)

    inst = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .scalar_one_or_none()
    )
    if inst is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )

    try:
        return databricks_control.list_bronze_volume_csvs(inst.name)
    except ValueError as ve:
        msg = str(ve)
        if "not configured" in msg.lower():
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )


@router.post(
    "/{inst_id}/input/upload-from-volume-to-gcs-bucket",
    response_model=BronzeImportResponse,
)
def upload_from_volume_to_gcs_bucket(
    inst_id: str,
    req: BronzeImportRequest,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
) -> Any:
    """Import a selected dataset from the institution's bronze volume into GCS unvalidated/."""
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)

    inst = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .scalar_one_or_none()
    )
    if inst is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )

    requested_name = (req.name or "").strip()
    if not requested_name:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Dataset name is required.",
        )
    if "/" in requested_name:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Dataset name can't contain '/'.",
        )

    # Ensure this is actually present in the bronze root (and matches naming rules).
    try:
        available = databricks_control.list_bronze_volume_csvs(inst.name)
    except ValueError as ve:
        msg = str(ve)
        if "not configured" in msg.lower():
            raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail=msg)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=msg
        )

    available_map = {x.lower(): x for x in available}
    file_name = available_map.get(requested_name.lower())
    if not file_name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Bronze dataset not found.",
        )

    stream = None
    try:
        stream = databricks_control.download_bronze_volume_file(inst.name, file_name)
        file_bytes = stream.read()
        upload_url = storage_control.generate_upload_signed_url(
            get_external_bucket_name(inst_id), file_name
        )
        _upload_file_bytes_to_signed_url(file_bytes, upload_url)
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
    except requests.RequestException as rexc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload dataset to GCS: {rexc}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error importing dataset: {e}",
        )
    finally:
        if stream is not None and hasattr(stream, "close"):
            try:
                stream.close()
            except Exception:
                pass

    return {"file_name": file_name, "message": "Upload successful."}


@router.post("/{inst_id}/add-custom-school-job/{job_run_id}")
def add_custom_school_job(
    inst_id: str,
    job_run_id: str,
    model_name: str,
    sql_session: Annotated[Session, Depends(get_session)],
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
) -> Any:
    """Fill in a JobTable ."""
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "this model")
    local_session.set(sql_session)

    model_name = decode_url_piece(model_name)
    inst_result = (
        local_session.get()
        .execute(
            select(InstTable).where(
                and_(
                    InstTable.id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )

    query_result = (
        local_session.get()
        .execute(
            select(ModelTable).where(
                and_(
                    ModelTable.name == model_name,
                    ModelTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )

    if not inst_result or not query_result:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution or model does not exist.",
        )

    try:
        triggered_timestamp = datetime.now()

        latest_model_version = databricks_control.fetch_model_version(
            catalog_name=str(env_vars["CATALOG_NAME"]),
            inst_name=inst_result[0][0].name,
            model_name=model_name,
        )
        model_version = (
            str(latest_model_version.version)
            if latest_model_version.version is not None
            else None
        )

        job = JobTable(
            id=job_run_id,
            triggered_at=triggered_timestamp,
            created_by=str_to_uuid(current_user.user_id),
            batch_name=f"{model_name}_{triggered_timestamp}",  # update later when we figure out how to add batches to custom jobs
            output_filename=f"{job_run_id}/inference_output.csv",
            model_id=query_result[0][0].id,
            output_valid=True,
            completed=True,
            model_version=model_version,
            model_run_id=latest_model_version.run_id,
        )
        local_session.get().add(job)

        return {
            "inst_id": inst_id,
            "m_name": model_name,
            "run_id": job_run_id,
            "output_filename": f"{job_run_id}/inference_output.csv",
            "model_version": model_version,
            "model_run_id": latest_model_version.run_id,
            "created_by": current_user.user_id,
            "triggered_at": triggered_timestamp,
        }
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))
