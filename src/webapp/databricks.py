"""Databricks SDk related helper functions."""

import os
import logging
import tomllib
from pydantic import BaseModel, field_validator
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service import catalog
from databricks.sdk.service.sql import (
    Format,
    ExecuteStatementRequestOnWaitTimeout,
    Disposition,
    StatementState,
)
from google.cloud import storage
from google.api_core import exceptions as gcs_errors
from edvise.configs.schema_type import project_config_class

from .config import ENV_TO_VOLUME_SCHEMA, databricks_vars, env_vars, gcs_vars
from .utilities import databricksify_inst_name, SchemaType
from typing import List, Any, Dict, Optional
import requests
import hashlib
import json
import gzip
from cachetools import TTLCache
import threading
import re

# Setting up logger
LOGGER = logging.getLogger(__name__)


# List of data medallion levels
MEDALLION_LEVELS = ["silver", "gold", "bronze"]

# The name of the deployed pipeline in Databricks. Must match the job's `name` in that workspace.
# Override with LEGACY_INFERENCE_JOB_NAME, ES_INFERENCE_JOB_NAME (and PDP_INFERENCE_JOB_NAME)
# when dev/staging deploy uses a different bundle target or a stub job that matches the same parameters.
PDP_INFERENCE_JOB_NAME = "edvise_versioned_inference_launcher"
LEGACY_INFERENCE_JOB_NAME = "edvise_github_sourced_legacy_inference_pipeline"
ES_INFERENCE_JOB_NAME = "github_sourced_genai_es_inference_pipeline"
# Dev bundle prefix for the Cloud Run service principal job target.
CLOUDRUN_BUNDLE_JOB_PREFIX = "[dev dev_cloudrun_sa]"
# GCS validated/ → institution bronze_volume/gcs_uploads (edvise bundle job name).
VALIDATED_BRONZE_SYNC_JOB_NAME = "edvise_validated_gcs_to_bronze_sync"
# Optional: numeric Databricks job id. If unset, the job is resolved by name (must be unique).
DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV = "DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID"
# Environment-specific Databricks job ids for deployed API environments.
VALIDATED_BRONZE_SYNC_JOB_IDS_BY_ENV = {
    "DEV": 1005654397694881,
    "STAGING": 611181637854021,
}

# Must match edvise bundle job parameters (github_validated_bronze_sync.yml).
BRONZE_SYNC_GCS_SOURCE_PREFIX = "validated/"
BRONZE_SYNC_BRONZE_SUBDIR = "gcs_uploads"
BRONZE_SYNC_MAX_OBJECTS = "1000"
BRONZE_SYNC_REQUIRE_AT_LEAST_ONE_FILE = "true"
BRONZE_SYNC_STRICT_MODE = "auto"

VALID_BRONZE_FILE_RE = re.compile(
    r"^[a-z0-9]+pdp_[a-z0-9]+_(course_level_)?ar_.*\.csv$",
    re.IGNORECASE,
)

# Only these basenames may be read from bronze_volume/training_inputs/ (path traversal safe).
ALLOWED_BRONZE_TRAINING_INPUTS_FILES = frozenset({"dataio.py", "config.toml"})


def _create_databricks_workspace_client(operation: str) -> WorkspaceClient:
    """
    Create a Databricks WorkspaceClient using configured host and GCP service account.

    Args:
        operation: Label for error messages (e.g. ``run_validated_gcs_to_bronze_sync``).

    Returns:
        Initialized workspace client.

    Raises:
        ValueError: If client creation fails.
    """
    try:
        return WorkspaceClient(
            host=databricks_vars["DATABRICKS_HOST_URL"],
            google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )
    except (OSError, DatabricksError) as exc:
        LOGGER.exception(
            "Failed to create Databricks WorkspaceClient for %s: host=%s service_account=%s",
            operation,
            databricks_vars["DATABRICKS_HOST_URL"],
            gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )
        raise ValueError(f"{operation}(): Workspace client failed: {exc}") from exc


def _run_databricks_job_now(
    workspace: WorkspaceClient,
    job_id: int,
    job_parameters: dict[str, str],
    operation: str,
) -> int:
    """
    Start a Databricks job run and return the run id.

    Raises:
        ValueError: If the Jobs API does not return a run id.
    """
    try:
        run_job: Any = workspace.jobs.run_now(job_id, job_parameters=job_parameters)
    except DatabricksError as exc:
        LOGGER.exception(
            "Databricks job run failed for %s (job_id=%s).", operation, job_id
        )
        raise ValueError(f"{operation}(): Job could not be run: {exc}") from exc

    if not run_job.response or run_job.response.run_id is None:
        raise ValueError(f"{operation}(): No run_id returned.")

    return int(run_job.response.run_id)


def _resolve_validated_bronze_sync_job_id(w: WorkspaceClient) -> int:
    """
    Return the job id for the GCS→bronze sync job.

    Prefer ``DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID`` when set (stable across renames).
    Otherwise resolve by exact name, deployed environment, then a unique bundle-prefixed name.
    """
    raw = (os.environ.get(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV) or "").strip()
    if raw:
        if not raw.isdigit():
            raise ValueError(
                f"{DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV} must be a positive integer "
                f"(Databricks job id) if set; got {raw!r}."
            )
        job_id = int(raw)
        if job_id <= 0:
            raise ValueError(
                f"{DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV} must be positive; got {job_id}."
            )
        LOGGER.info(
            "Bronze sync job id from %s=%s",
            DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV,
            job_id,
        )
        return job_id

    jobs = list(w.jobs.list(name=VALIDATED_BRONZE_SYNC_JOB_NAME))
    if len(jobs) == 0:
        env_job_id = _resolve_validated_bronze_sync_job_id_by_environment()
        if env_job_id is not None:
            return env_job_id
    if len(jobs) == 0:
        jobs = _find_validated_bronze_sync_jobs_by_suffix(w)
    if len(jobs) == 0:
        raise ValueError(
            f"Job named {VALIDATED_BRONZE_SYNC_JOB_NAME!r} or a unique bundle-prefixed "
            f"variant was not found. "
            f"Deploy the bundle job or set {DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV} "
            "to the numeric job id from the Databricks UI / API."
        )
    if len(jobs) > 1:
        ids = [j.job_id for j in jobs if j.job_id is not None]
        raise ValueError(
            f"Multiple ({len(jobs)}) jobs matched {VALIDATED_BRONZE_SYNC_JOB_NAME!r}; "
            f"set {DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV} to the correct id. Found job_ids={ids}."
        )
    job = jobs[0]
    if job.job_id is None:
        raise ValueError(
            f"Job matching {VALIDATED_BRONZE_SYNC_JOB_NAME!r} has no job_id in list response."
        )
    job_id = job.job_id
    LOGGER.info(
        "Resolved bronze sync job %r: job_id=%s",
        _databricks_job_name(job) or VALIDATED_BRONZE_SYNC_JOB_NAME,
        job_id,
    )
    return job_id


def _resolve_validated_bronze_sync_job_id_by_environment() -> Optional[int]:
    """Return the deployed Databricks job id for the current API environment."""
    env = (os.environ.get("ENV") or "").strip().upper()
    job_id = VALIDATED_BRONZE_SYNC_JOB_IDS_BY_ENV.get(env)
    if job_id is not None:
        LOGGER.info("Bronze sync job id from ENV=%s mapping: job_id=%s", env, job_id)
    return job_id


def _databricks_job_name(job: Any) -> Optional[str]:
    """Return the display name from a Databricks job list item, if present."""
    settings = getattr(job, "settings", None)
    name = getattr(settings, "name", None)
    if isinstance(name, str):
        return name
    name = getattr(job, "name", None)
    if isinstance(name, str):
        return name
    return None


def _find_validated_bronze_sync_jobs_by_suffix(w: WorkspaceClient) -> list[Any]:
    """
    Find a Databricks Asset Bundle dev-mode job with a prefixed display name.

    Development-mode bundle jobs can be named like
    ``[dev service_principal] edvise_validated_gcs_to_bronze_sync``. Only a
    single suffix match is accepted by the caller.
    """
    suffix = f" {VALIDATED_BRONZE_SYNC_JOB_NAME}"
    return [
        job
        for job in w.jobs.list()
        if (name := _databricks_job_name(job)) is not None and name.endswith(suffix)
    ]


def _pdp_inference_job_name() -> str:
    name = os.environ.get("PDP_INFERENCE_JOB_NAME", "").strip()
    return name or PDP_INFERENCE_JOB_NAME


def _legacy_inference_job_name() -> str:
    name = os.environ.get("LEGACY_INFERENCE_JOB_NAME", "").strip()
    return name or LEGACY_INFERENCE_JOB_NAME


def _es_inference_job_name() -> str:
    name = os.environ.get("ES_INFERENCE_JOB_NAME", "").strip()
    return name or ES_INFERENCE_JOB_NAME


def _disambiguate_pipeline_job_matches(
    matches: list[tuple[str, Any]],
    pipeline_type: str,
    caller_label: str,
) -> tuple[str, Any]:
    """Prefer the Cloud Run bundle job when several dev-prefixed jobs match."""
    cloudrun_matches = [
        (name, job) for name, job in matches if CLOUDRUN_BUNDLE_JOB_PREFIX in name
    ]
    if len(cloudrun_matches) == 1:
        return cloudrun_matches[0]
    if len(cloudrun_matches) > 1:
        picked_name, picked = sorted(cloudrun_matches, key=lambda item: item[0])[0]
        LOGGER.warning(
            "%s: Multiple Cloud Run bundle jobs match substring %r: %s; using %r.",
            caller_label,
            pipeline_type,
            [name for name, _ in cloudrun_matches],
            picked_name,
        )
        return picked_name, picked

    picked_name, picked = sorted(matches, key=lambda item: item[0])[0]
    LOGGER.warning(
        "%s: Multiple jobs match substring %r: %s; no Cloud Run bundle job found; using first match %r.",
        caller_label,
        pipeline_type,
        [name for name, _ in matches],
        picked_name,
    )
    return picked_name, picked


def _resolve_pipeline_job(w: Any, pipeline_type: str, caller_label: str) -> Any:
    """Find a job by exact name, else by unique substring match on display name.

    Development bundles often prefix job names (e.g. ``[dev vishakh] edvise_...``) while
    the API passes the canonical base name. Optional env ``PDP_INFERENCE_JOB_NAME`` /
    ``LEGACY_INFERENCE_JOB_NAME`` still wins when set to the full exact name.
    """
    job = next(w.jobs.list(name=pipeline_type), None)
    if job is not None and getattr(job, "job_id", None) is not None:
        LOGGER.info(
            "%s: resolved job by exact name %r (job_id=%s)",
            caller_label,
            pipeline_type,
            job.job_id,
        )
        return job

    matches: list[tuple[str, Any]] = []
    for j in w.jobs.list():
        settings = getattr(j, "settings", None)
        name = getattr(settings, "name", None) if settings is not None else None
        if not name:
            continue
        if pipeline_type in name:
            matches.append((name, j))

    if len(matches) == 1:
        picked_name, picked = matches[0]
        if getattr(picked, "job_id", None) is None:
            raise ValueError(
                f"{caller_label}: Job name {picked_name!r} matched substring {pipeline_type!r} but has no job_id."
            )
        LOGGER.info(
            "%s: resolved job by substring %r -> display name %r (job_id=%s)",
            caller_label,
            pipeline_type,
            picked_name,
            picked.job_id,
        )
        return picked

    if len(matches) > 1:
        picked_name, picked = _disambiguate_pipeline_job_matches(
            matches, pipeline_type, caller_label
        )
        if getattr(picked, "job_id", None) is None:
            raise ValueError(
                f"{caller_label}: Job name {picked_name!r} matched substring {pipeline_type!r} but has no job_id."
            )
        LOGGER.info(
            "%s: resolved job by substring %r -> display name %r (job_id=%s)",
            caller_label,
            pipeline_type,
            picked_name,
            picked.job_id,
        )
        return picked

    raise ValueError(
        f"{caller_label}: Job {pipeline_type!r} was not found (exact name or unique substring of settings.name) "
        f"for '{gcs_vars['GCP_SERVICE_ACCOUNT_EMAIL']}' and '{databricks_vars['DATABRICKS_HOST_URL']}'."
    )


class DatabricksPDPInferenceRunRequest(BaseModel):
    """Databricks parameters for a PDP inference run."""

    inst_name: str
    # Note that the following should be the filepath.
    filepath_to_type: dict[str, list[SchemaType]]
    model_name: str
    # The email where notifications will get sent.
    email: str
    gcp_external_bucket_name: str
    term_filter: list[str] | None = None


class DatabricksSharedInferenceRunRequest(BaseModel):
    """Databricks parameters for a legacy schools inference run."""

    inst_name: str
    model_name: str
    config_file_name: str = ""
    features_table_name: str = ""
    # The email where notifications will get sent.
    email: str = ""
    gcp_external_bucket_name: str
    # Batch UUID hex; bronze copies live under gcs_uploads/<batch_id>/.
    batch_id: str = ""
    # Full GCS object paths, e.g. ["validated/file.csv"].
    validated_blob_paths: list[str] = []
    # ES: True when institution has genai_id; False for edvise_id schools.
    is_genai_institution: bool = True
    term_filter: list[str] | None = None

    @field_validator("config_file_name", "features_table_name", "email", mode="before")
    @classmethod
    def _none_to_empty_str(cls, v: object) -> object:
        """Allow callers to omit or pass null; Databricks job treats empty like YAML defaults."""
        return "" if v is None else v


class DatabricksInferenceRunResponse(BaseModel):
    """Databricks parameters for an inference run."""

    job_run_id: int


class DatabricksBronzeSyncRequest(BaseModel):
    """Parameters to copy validated GCS objects into the institution bronze volume."""

    inst_name: str
    gcp_bucket_name: str
    # Full object paths in the bucket, e.g. ["validated/file.csv"].
    validated_blob_paths: list[str]
    # When set, bronze copies land under gcs_uploads/<batch_id>/.
    batch_id: str | None = None


class DatabricksBronzeSyncResponse(BaseModel):
    """Result of triggering the bronze sync Databricks job."""

    job_run_id: int


def _build_pdp_inference_job_parameters(
    req: DatabricksPDPInferenceRunRequest,
    databricks_institution_name: str,
) -> dict[str, str]:
    """Build PDP inference parameters, including an optional academic-term filter."""
    parameters = {
        "cohort_file_name": get_filepath_of_filetype(
            req.filepath_to_type, SchemaType.STUDENT
        ),
        "course_file_name": get_filepath_of_filetype(
            req.filepath_to_type, SchemaType.COURSE
        ),
        "databricks_institution_name": databricks_institution_name,
        "DB_workspace": databricks_vars["DATABRICKS_WORKSPACE"],
        "gcp_bucket_name": req.gcp_external_bucket_name,
        "model_name": req.model_name,
        "datakind_notification_email": req.email,
        "DK_CC_EMAIL": req.email,
    }
    if req.term_filter is not None:
        parameters["term_filter"] = json.dumps(req.term_filter)
    return parameters


def _build_shared_inference_job_parameters(
    req: DatabricksSharedInferenceRunRequest,
    databricks_institution_name: str,
) -> dict[str, str]:
    """Build common job_parameters for legacy and ES inference runs."""
    parameters = {
        "databricks_institution_name": databricks_institution_name,
        "DB_workspace": databricks_vars["DATABRICKS_WORKSPACE"],
        "model_name": uc_model_name(req.model_name),
        "config_file_name": req.config_file_name,
        "gcp_bucket_name": req.gcp_external_bucket_name,
        "datakind_notification_email": req.email,
        "DK_CC_EMAIL": req.email,
        "batch_id": req.batch_id,
        "validated_blob_paths_json": json.dumps(
            req.validated_blob_paths, separators=(",", ":")
        ),
    }
    if req.term_filter is not None:
        parameters["term_filter"] = json.dumps(req.term_filter)
    return parameters


def _build_validated_bronze_sync_job_parameters(
    req: DatabricksBronzeSyncRequest,
    databricks_institution_name: str,
) -> dict[str, str]:
    """Build job_parameters dict for the GCS→bronze sync Databricks job."""
    include_json = json.dumps(req.validated_blob_paths, separators=(",", ":"))
    return {
        "gcp_bucket_name": req.gcp_bucket_name,
        "databricks_institution_name": databricks_institution_name,
        "DB_workspace": databricks_vars["DATABRICKS_WORKSPACE"],
        "batch_id": (req.batch_id or "").strip(),
        "gcs_source_prefix": BRONZE_SYNC_GCS_SOURCE_PREFIX,
        "bronze_subdir": BRONZE_SYNC_BRONZE_SUBDIR,
        "max_objects": BRONZE_SYNC_MAX_OBJECTS,
        "require_at_least_one_file": BRONZE_SYNC_REQUIRE_AT_LEAST_ONE_FILE,
        "strict_mode": BRONZE_SYNC_STRICT_MODE,
        "include_blob_paths_json": include_json,
    }


def get_filepath_of_filetype(
    file_dict: dict[str, list[SchemaType]], file_type: SchemaType
) -> str:
    """Helper functions to get a file of a given file_type.
    For both, we will return the first file that matches the schema."""
    for k, v in file_dict.items():
        if file_type in v:
            return k
    return ""


def check_types(dict_values: list[list[SchemaType]], file_type: SchemaType) -> bool:
    """Check the file type is in the dict dictionary."""
    for elem in dict_values:
        if file_type in elem:
            return True
    return False


def _sha256_json(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
    ).hexdigest()


def _parse_training_config(raw: bytes, schema_type: str) -> dict[str, Any] | None:
    """Validate a training TOML with the schema-specific Pydantic config."""
    try:
        data = tomllib.loads(raw.decode("utf-8"))
        cfg = project_config_class(schema_type).model_validate(data)
    except (UnicodeDecodeError, tomllib.TOMLDecodeError, ValueError):
        LOGGER.debug(
            "Training config failed validation for schema_type=%s",
            schema_type,
            exc_info=True,
        )
        return None

    if cfg.preprocessing is None or cfg.modeling is None:
        LOGGER.debug(
            "Training config lacks preprocessing or modeling for schema_type=%s",
            schema_type,
        )
        return None

    student_criteria = dict(cfg.preprocessing.selection.student_criteria)
    cohorts = cfg.modeling.training.cohort or []
    training_cohorts = [str(label).strip() for label in cohorts if str(label).strip()]
    return {
        "student_id_col": cfg.student_id_col,
        "student_criteria": student_criteria,
        "training_cohorts": training_cohorts,
    }


def _find_training_config_in_training_dir(
    workspace: WorkspaceClient,
    training_directory: str,
    schema_type: str,
) -> dict[str, Any] | None:
    """Load the training config TOML from the model run's training directory."""
    try:
        entries = list(workspace.files.list_directory_contents(training_directory))
    except Exception:
        LOGGER.debug(
            "Could not list Databricks training config path %s",
            training_directory,
            exc_info=True,
        )
        return None
    toml_entries = [
        (entry.name, entry.path)
        for entry in entries
        if entry.path is not None
        and not entry.is_directory
        and entry.name is not None
        and entry.name.lower().endswith(".toml")
    ]
    config_entries = [
        entry for entry in toml_entries if entry[0].lower() == "config.toml"
    ]
    if len(config_entries) == 1:
        _entry_name, entry_path = config_entries[0]
    elif not config_entries and len(toml_entries) == 1:
        _entry_name, entry_path = toml_entries[0]
    else:
        LOGGER.warning(
            "Expected one training config TOML in %s; found %d",
            training_directory,
            len(toml_entries),
        )
        return None

    try:
        response = workspace.files.download(entry_path)
        if response.contents is None:
            return None
        raw = response.contents.read()
        raw_bytes = raw if isinstance(raw, bytes) else raw.encode("utf-8")
    except Exception:
        LOGGER.debug("Could not read Databricks config %s", entry_path, exc_info=True)
        return None
    return _parse_training_config(raw_bytes, schema_type)


L1_RESP_CACHE_TTL = int("600")  # seconds
L1_VER_CACHE_TTL = int("3600")  # seconds
L1_RESP_CACHE: Any = TTLCache(maxsize=128, ttl=L1_RESP_CACHE_TTL)
L1_VER_CACHE: Any = TTLCache(maxsize=256, ttl=L1_VER_CACHE_TTL)
_L1_LOCK = threading.RLock()


# Wrapping the usages in a class makes it easier to unit test via mocks.
class DatabricksControl(BaseModel):
    """Object to manage interfacing with GCS."""

    def setup_new_inst(self, inst_name: str) -> None:
        """Sets up Databricks resources for a new institution."""
        LOGGER.info("Setting up new institution.")
        try:
            w = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            LOGGER.exception(
                "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
                databricks_vars["DATABRICKS_HOST_URL"],
                gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
            raise ValueError(f"setup_new_inst(): Workspace client creation failed: {e}")

        db_inst_name = databricksify_inst_name(inst_name)
        cat_name = databricks_vars["CATALOG_NAME"]
        for medallion in MEDALLION_LEVELS:
            try:
                w.schemas.create(
                    name=f"{db_inst_name}_{medallion}", catalog_name=cat_name
                )
            except Exception as e:
                LOGGER.exception(
                    f"Failed to provision schemas in databricks for {db_inst_name}_{medallion}: {e}"
                )
                raise ValueError(
                    f"setup_new_inst(): Failed to provision schemas in databricks for {db_inst_name}_{medallion}: {e}"
                )
            LOGGER.info(
                f"Creating medallion level schemas for {db_inst_name} & {medallion}."
            )
        # Create a managed volume in the bronze schema for internal pipeline data.
        # update to include a managed volume for toml files
        try:
            created_volume_bronze = w.volumes.create(
                catalog_name=cat_name,
                schema_name=f"{db_inst_name}_bronze",
                name="bronze_volume",
                volume_type=catalog.VolumeType.MANAGED,
            )
            LOGGER.info(
                f"Created volume 'bronze_volume' in schema '{db_inst_name}_bronze'."
            )

            created_volume_silver = w.volumes.create(
                catalog_name=cat_name,
                schema_name=f"{db_inst_name}_silver",
                name="silver_volume",
                volume_type=catalog.VolumeType.MANAGED,
            )
            LOGGER.info(
                f"Created volume 'silver_volume' in schema '{db_inst_name}_silver'."
            )

            created_volume_gold = w.volumes.create(
                catalog_name=cat_name,
                schema_name=f"{db_inst_name}_gold",
                name="gold_volume",
                volume_type=catalog.VolumeType.MANAGED,
            )
            LOGGER.info(
                f"Created volume 'gold_volume' in schema '{db_inst_name}_gold'."
            )

        except Exception as e:
            LOGGER.exception("Failed to create one or more volumes.")
            raise ValueError(f"setup_new_inst(): Volume creation failed: {e}")

        if (
            created_volume_bronze is None
            or created_volume_silver is None
            or created_volume_gold is None
        ):
            raise ValueError("setup_new_inst() volume creation failed.")
        # Create directory on the volume
        os.makedirs(
            f"/Volumes/{cat_name}/{db_inst_name}_gold/gold_volume/configuration_files/",
            exist_ok=True,
        )
        # Create directory on the volume
        os.makedirs(
            f"/Volumes/{cat_name}/{db_inst_name}_bronze/bronze_volume/raw_files/",
            exist_ok=True,
        )

    def list_bronze_volume_csvs(self, inst_name: str) -> list[str]:
        """List `.csv` files directly under the institution's bronze volume root."""
        if not databricks_vars.get("DATABRICKS_HOST_URL") or not databricks_vars.get(
            "CATALOG_NAME"
        ):
            raise ValueError("Databricks integration not configured.")
        if not gcs_vars.get("GCP_SERVICE_ACCOUNT_EMAIL"):
            raise ValueError("GCP service account email not configured.")

        try:
            w = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            LOGGER.exception(
                "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
                databricks_vars.get("DATABRICKS_HOST_URL"),
                gcs_vars.get("GCP_SERVICE_ACCOUNT_EMAIL"),
            )
            raise ValueError(f"Workspace client creation failed: {e}")

        db_inst_name = databricksify_inst_name(inst_name)
        volume_root = (
            f"/Volumes/{databricks_vars['CATALOG_NAME']}/"
            f"{db_inst_name}_bronze/bronze_volume"
        )

        try:
            entries = list(w.dbfs.list(f"dbfs:{volume_root}") or [])
        except Exception as e:
            LOGGER.exception("Failed to list bronze volume directory: %s", volume_root)
            raise ValueError(f"Failed to list bronze volume directory: {e}")

        csvs: list[str] = []
        for entry in entries:
            entry_path = getattr(entry, "path", None)
            is_dir = getattr(entry, "is_dir", False)
            if not entry_path or is_dir:
                continue
            basename = os.path.basename(str(entry_path))
            if not VALID_BRONZE_FILE_RE.match(basename):
                continue
            csvs.append(basename)
        csvs.sort()
        return csvs

    def download_bronze_volume_file(self, inst_name: str, file_name: str) -> Any:
        """Download a file from the institution's bronze volume root and return a byte stream."""
        if "/" in file_name:
            raise ValueError("file_name must not contain '/'.")
        if not VALID_BRONZE_FILE_RE.match(file_name):
            raise ValueError("Invalid bronze dataset filename.")
        if not databricks_vars.get("DATABRICKS_HOST_URL") or not databricks_vars.get(
            "CATALOG_NAME"
        ):
            raise ValueError("Databricks integration not configured.")
        if not gcs_vars.get("GCP_SERVICE_ACCOUNT_EMAIL"):
            raise ValueError("GCP service account email not configured.")

        try:
            w = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            LOGGER.exception(
                "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
                databricks_vars.get("DATABRICKS_HOST_URL"),
                gcs_vars.get("GCP_SERVICE_ACCOUNT_EMAIL"),
            )
            raise ValueError(f"Workspace client creation failed: {e}")

        db_inst_name = databricksify_inst_name(inst_name)
        volume_path = (
            f"/Volumes/{databricks_vars['CATALOG_NAME']}/"
            f"{db_inst_name}_bronze/bronze_volume/{file_name}"
        )

        try:
            response = w.files.download(volume_path)
        except Exception as e:
            LOGGER.exception("Failed to download from %s", volume_path)
            raise ValueError(f"Failed to download bronze dataset: {e}")

        stream = getattr(response, "contents", None)
        if stream is None:
            raise ValueError("Databricks download returned no contents.")
        return stream

    def download_bronze_training_inputs_file(
        self, inst_name: str, relative_path: str = "dataio.py"
    ) -> Any:
        """
        Download a file from ``bronze_volume/training_inputs/`` for an institution.

        Only allowlisted basenames (currently ``dataio.py`` and ``config.toml``) are
        permitted; nested paths, ``..``, and absolute paths are rejected.

        Args:
            inst_name: Institution display name (passed through ``databricksify_inst_name``).
            relative_path: Basename under ``training_inputs/`` (default ``dataio.py``).

        Returns:
            Byte stream of file contents (same shape as ``download_bronze_volume_file``).

        Raises:
            ValueError: If path is unsafe, Databricks is not configured, or download fails.
        """
        if not relative_path or not isinstance(relative_path, str):
            raise ValueError("relative_path is required.")
        if relative_path != os.path.basename(relative_path):
            raise ValueError(
                "relative_path must be a basename only (no directories or separators)."
            )
        if relative_path in (".", "..") or ".." in relative_path:
            raise ValueError("relative_path must not contain path traversal.")
        if relative_path not in ALLOWED_BRONZE_TRAINING_INPUTS_FILES:
            raise ValueError(
                f"relative_path must be one of {sorted(ALLOWED_BRONZE_TRAINING_INPUTS_FILES)}."
            )
        if not databricks_vars.get("DATABRICKS_HOST_URL") or not databricks_vars.get(
            "CATALOG_NAME"
        ):
            raise ValueError("Databricks integration not configured.")
        if not gcs_vars.get("GCP_SERVICE_ACCOUNT_EMAIL"):
            raise ValueError("GCP service account email not configured.")

        try:
            w = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            LOGGER.exception(
                "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
                databricks_vars.get("DATABRICKS_HOST_URL"),
                gcs_vars.get("GCP_SERVICE_ACCOUNT_EMAIL"),
            )
            raise ValueError(f"Workspace client creation failed: {e}") from e

        db_inst_name = databricksify_inst_name(inst_name)
        volume_path = (
            f"/Volumes/{databricks_vars['CATALOG_NAME']}/"
            f"{db_inst_name}_bronze/bronze_volume/training_inputs/{relative_path}"
        )

        try:
            response = w.files.download(volume_path)
        except Exception as e:
            LOGGER.exception("Failed to download from %s", volume_path)
            raise ValueError(
                f"Failed to download bronze training_inputs file: {e}"
            ) from e

        stream = getattr(response, "contents", None)
        if stream is None:
            raise ValueError("Databricks download returned no contents.")
        return stream

    # Note that for each unique PIPELINE, we'll need a new function, this is by nature of how unique pipelines
    # may have unique parameters and would have a unique name (i.e. the name field specified in w.jobs.list()). But any run of a given pipeline (even across institutions) can use the same function.
    # E.g. there is one PDP inference pipeline, so one PDP inference function here.

    def run_pdp_inference(
        self, req: DatabricksPDPInferenceRunRequest
    ) -> DatabricksInferenceRunResponse:
        """Triggers PDP inference Databricks run."""
        LOGGER.info(f"Running PDP inference for institution: {req.inst_name}")
        if (
            not req.filepath_to_type
            or not check_types(list(req.filepath_to_type.values()), SchemaType.COURSE)
            or not check_types(list(req.filepath_to_type.values()), SchemaType.STUDENT)
        ):
            LOGGER.error("Missing required file types: COURSE and STUDENT")
            raise ValueError(
                "run_pdp_inference() requires COURSE and STUDENT type files to run."
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
                f"run_pdp_inference(): Workspace client initialization failed: {e}"
            )

        db_inst_name = databricksify_inst_name(req.inst_name)
        pipeline_type = _pdp_inference_job_name()

        try:
            job = _resolve_pipeline_job(w, pipeline_type, "run_pdp_inference")
            job_id = job.job_id
            LOGGER.info(f"Resolved job ID for '{pipeline_type}': {job_id}")
        except Exception as e:
            LOGGER.exception(f"Job lookup failed for '{pipeline_type}'.")
            raise ValueError(f"run_pdp_inference(): Failed to find job: {e}")

        try:
            run_job: Any = w.jobs.run_now(
                job_id,
                job_parameters=_build_pdp_inference_job_parameters(req, db_inst_name),
            )
            LOGGER.info(
                f"Successfully triggered job run. Run ID: {run_job.response.run_id}"
            )
        except Exception as e:
            LOGGER.exception("Failed to run the PDP inference job.")
            raise ValueError(f"run_pdp_inference(): Job could not be run: {e}")

        if not run_job.response or run_job.response.run_id is None:
            raise ValueError("run_pdp_inference(): Job did not return a valid run_id.")

        run_id = run_job.response.run_id
        LOGGER.info(f"Successfully triggered job run. Run ID: {run_id}")

        return DatabricksInferenceRunResponse(job_run_id=run_id)

    def run_legacy_inference(
        self, req: DatabricksSharedInferenceRunRequest
    ) -> DatabricksInferenceRunResponse:
        """Triggers legacy schools inference Databricks run."""
        LOGGER.info(f"Running legacy inference for institution: {req.inst_name}")
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
                f"run_legacy_inference(): Workspace client initialization failed: {e}"
            )

        db_inst_name = databricksify_inst_name(req.inst_name)
        pipeline_type = _legacy_inference_job_name()

        try:
            job = _resolve_pipeline_job(w, pipeline_type, "run_legacy_inference")
            job_id = job.job_id
            LOGGER.info(f"Resolved job ID for '{pipeline_type}': {job_id}")
        except Exception as e:
            LOGGER.exception(f"Job lookup failed for '{pipeline_type}'.")
            raise ValueError(f"run_legacy_inference(): Failed to find job: {e}")

        try:
            job_parameters = _build_shared_inference_job_parameters(req, db_inst_name)
            job_parameters["features_table_name"] = req.features_table_name
            run_job: Any = w.jobs.run_now(
                job_id,
                job_parameters=job_parameters,
            )
            LOGGER.info(
                f"Successfully triggered job run. Run ID: {run_job.response.run_id}"
            )
        except Exception as e:
            LOGGER.exception("Failed to run the legacy inference job.")
            raise ValueError(f"run_legacy_inference(): Job could not be run: {e}")

        if not run_job.response or run_job.response.run_id is None:
            raise ValueError(
                "run_legacy_inference(): Job did not return a valid run_id."
            )

        run_id = run_job.response.run_id
        LOGGER.info(f"Successfully triggered job run. Run ID: {run_id}")

        return DatabricksInferenceRunResponse(job_run_id=run_id)

    def run_es_inference(
        self, req: DatabricksSharedInferenceRunRequest
    ) -> DatabricksInferenceRunResponse:
        """Triggers Edvise Schema (ES) inference Databricks run."""
        LOGGER.info(f"Running ES inference for institution: {req.inst_name}")
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
                f"run_es_inference(): Workspace client initialization failed: {e}"
            ) from e

        db_inst_name = databricksify_inst_name(req.inst_name)
        pipeline_type = _es_inference_job_name()

        try:
            job = _resolve_pipeline_job(w, pipeline_type, "run_es_inference")
            job_id = job.job_id
            LOGGER.info(f"Resolved job ID for '{pipeline_type}': {job_id}")
        except Exception as e:
            LOGGER.exception(f"Job lookup failed for '{pipeline_type}'.")
            raise ValueError(f"run_es_inference(): Failed to find job: {e}") from e

        try:
            job_parameters = _build_shared_inference_job_parameters(req, db_inst_name)
            job_parameters["schema_type"] = "edvise"
            job_parameters["is_genai_institution"] = (
                "true" if req.is_genai_institution else "false"
            )
            run_job: Any = w.jobs.run_now(
                job_id,
                job_parameters=job_parameters,
            )
            LOGGER.info(
                f"Successfully triggered job run. Run ID: {run_job.response.run_id}"
            )
        except Exception as e:
            LOGGER.exception("Failed to run the ES inference job.")
            raise ValueError(f"run_es_inference(): Job could not be run: {e}") from e

        if not run_job.response or run_job.response.run_id is None:
            raise ValueError("run_es_inference(): Job did not return a valid run_id.")

        run_id = run_job.response.run_id
        LOGGER.info(f"Successfully triggered job run. Run ID: {run_id}")

        return DatabricksInferenceRunResponse(job_run_id=run_id)

    def run_validated_gcs_to_bronze_sync(
        self, req: DatabricksBronzeSyncRequest
    ) -> DatabricksBronzeSyncResponse:
        """
        Trigger the job that copies validated/ objects from GCS into bronze_volume/gcs_uploads.

        Args:
            req: Institution name, bucket, and full GCS object paths under validated/.

        Returns:
            Response containing the Databricks job run id (run started, not completed).

        Raises:
            ValueError: If paths are empty, configuration is invalid, or the job cannot start.
        """
        operation = "run_validated_gcs_to_bronze_sync"
        if not req.validated_blob_paths:
            raise ValueError(f"{operation}: validated_blob_paths must be non-empty.")

        LOGGER.info(
            "Triggering GCS→bronze sync for institution: %s (%s objects)",
            req.inst_name,
            len(req.validated_blob_paths),
        )

        workspace = _create_databricks_workspace_client(operation)
        try:
            job_id = _resolve_validated_bronze_sync_job_id(workspace)
        except ValueError as exc:
            LOGGER.exception("Job resolution failed for GCS→bronze sync.")
            raise ValueError(f"{operation}(): Failed to resolve job: {exc}") from exc

        db_inst_name = databricksify_inst_name(req.inst_name)
        job_parameters = _build_validated_bronze_sync_job_parameters(req, db_inst_name)
        run_id = _run_databricks_job_now(workspace, job_id, job_parameters, operation)
        LOGGER.info("GCS→bronze sync job started. Run ID: %s", run_id)
        return DatabricksBronzeSyncResponse(job_run_id=run_id)

    def delete_inst(self, inst_name: str) -> None:
        """Cleanup tasks required on the Databricks side to delete an institution."""
        db_inst_name = databricksify_inst_name(inst_name)
        cat_name = databricks_vars["CATALOG_NAME"]

        LOGGER.info(f"Starting deletion of Databricks resources for: {db_inst_name}")

        try:
            w = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                # This should still be cloud run, since it's cloud run triggering the databricks
                # this account needs to exist on Databricks as well and needs to have permissions.
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            LOGGER.exception(
                "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
                databricks_vars["DATABRICKS_HOST_URL"],
                gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
            raise ValueError(
                f"delete_inst(): Workspace client initialization failed: {e}"
            )

        # Delete managed volumes
        for medallion in MEDALLION_LEVELS:
            volume_name = f"{cat_name}.{db_inst_name}_{medallion}.{medallion}_volume"
            try:
                w.volumes.delete(name=volume_name)
                LOGGER.info(f"Deleted volume: {volume_name}")
            except Exception as e:
                LOGGER.exception(
                    f"Volume not found or could not be deleted: {volume_name} — {e}"
                )

        # TODO implement model deletion

        # Delete tables and schemas for each medallion level.
        for medallion in MEDALLION_LEVELS:
            try:
                all_tables = [
                    table.name
                    for table in w.tables.list(
                        catalog_name=cat_name,
                        schema_name=f"{db_inst_name}_{medallion}",
                    )
                ]
                for table in all_tables:
                    w.tables.delete(
                        full_name=f"{cat_name}.{db_inst_name}_{medallion}.{table}"
                    )
                w.schemas.delete(full_name=f"{cat_name}.{db_inst_name}_{medallion}")
            except Exception as e:
                LOGGER.exception(
                    f"Tables or schemas could not be deleted for {medallion}  — {e}"
                )

    def fetch_table_data(
        self,
        catalog_name: str,
        inst_name: str,
        table_name: str,
        warehouse_id: str,
    ) -> Any:
        """
        Execute SELECT * via Databricks SQL Statement Execution API using EXTERNAL_LINKS.
        Blocks server-side for up to 30s; if not SUCCEEDED, raises. Downloads presigned
        URLs in-memory and returns rows as List[Dict[str, Any]].
        """
        w = WorkspaceClient(
            host=databricks_vars["DATABRICKS_HOST_URL"],
            google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )

        bucket_name = databricks_vars["GCP_CACHE_BUCKET"]
        schema = databricksify_inst_name(inst_name)
        table_fqn = f"`{catalog_name}`.`{schema}_silver`.`{table_name}`"
        sql = f"SELECT * FROM {table_fqn}"

        ver_cache_key = f"ver:{table_fqn}"
        with _L1_LOCK:
            table_version = L1_VER_CACHE.get(ver_cache_key)

        if table_version is None:
            ver_sql = f"DESCRIBE HISTORY {table_fqn} LIMIT 1"
            ver_resp = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=ver_sql,
                disposition=Disposition.INLINE,
                format=Format.JSON_ARRAY,
                wait_timeout="30s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
            )

            if not ver_resp.status or ver_resp.status.state != StatementState.SUCCEEDED:
                raise TimeoutError("DESCRIBE HISTORY did not finish within 30s")
            cols = [c.name for c in ver_resp.manifest.schema.columns]  # type: ignore
            idx = {n: i for i, n in enumerate(cols)}
            rows = ver_resp.result.data_array or []  # type: ignore
            if not rows or "version" not in idx:
                raise ValueError("DESCRIBE HISTORY returned no version")
            table_version = str(rows[0][idx["version"]])

            with _L1_LOCK:
                L1_VER_CACHE[ver_cache_key] = table_version

        sql_h = _sha256_json({"sql": sql})
        l1_key = f"v1:{warehouse_id}:{catalog_name}.{schema}.{table_name}:{sql_h}:{table_version}"

        with _L1_LOCK:
            cached_records = L1_RESP_CACHE.get(l1_key)
        if cached_records is not None:
            return cached_records

        try:
            object_name = f"{warehouse_id}/{catalog_name}.{schema}.{table_name}/{sql_h}/{table_version}.json.gz"
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(object_name)
            try:
                blob.reload()  # HEAD for metadata (ETag, etc.)
                body = blob.download_as_bytes(raw_download=False)
                data = json.loads(body)
                if isinstance(data, list):
                    with _L1_LOCK:
                        L1_RESP_CACHE[l1_key] = data
                    return data  # cache hit
            except gcs_errors.NotFound:
                pass
        except Exception:
            pass

        resp = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            disposition=Disposition.EXTERNAL_LINKS,
            format=Format.JSON_ARRAY,
            wait_timeout="30s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        )

        stmt_id = resp.statement_id
        if stmt_id is None:
            raise ValueError("Databricks returned a null statement_id")

        # No client-side polling; require SUCCEEDED within 30s.
        if (resp.status is None) or (resp.status.state != StatementState.SUCCEEDED):
            state = resp.status.state if resp.status else "UNKNOWN"
            msg = (
                resp.status.error.message
                if (resp.status and resp.status.error)
                else "Query not finished within wait_timeout"
            )
            raise TimeoutError(
                f"Statement {stmt_id} not finished (state={state}): {msg}"
            )

        # Columns (ensure List[str] for type-checkers)
        if not (
            resp.manifest and resp.manifest.schema and resp.manifest.schema.columns
        ):
            raise ValueError("Schema/columns missing (EXTERNAL_LINKS).")
        cols: List[str] = []  # type: ignore
        for c in resp.manifest.schema.columns:
            if c.name is None:
                raise ValueError("Encountered a column without a name.")
            cols.append(c.name)

        records: Any = []

        def _link_attr(link_obj: Any, name: str) -> Any:
            if isinstance(link_obj, dict):
                return link_obj.get(name)
            return getattr(link_obj, name, None)

        def _next_chunk_index(chunk_obj: Any) -> int | None:
            """Return next chunk index for EXTERNAL_LINKS (on links) or INLINE."""
            links = getattr(chunk_obj, "external_links", None) or []
            next_idx: int | None = None
            for link_obj in links:
                idx = _link_attr(link_obj, "next_chunk_index")
                if idx is not None:
                    next_idx = int(idx)
            if next_idx is not None:
                return next_idx
            idx = getattr(chunk_obj, "next_chunk_index", None)
            return int(idx) if idx is not None else None

        def _consume_chunk(chunk_obj: Any) -> int | None:
            """Download EXTERNAL_LINKS payloads; return next_chunk_index if any."""
            links = getattr(chunk_obj, "external_links", None) or []
            for link_obj in links:
                url = _link_attr(link_obj, "external_link")
                if not url:
                    continue
                # IMPORTANT: do not send Databricks auth header to presigned URLs.
                r = requests.get(url, timeout=120)
                r.raise_for_status()
                rows = r.json()
                if not isinstance(rows, list):
                    raise ValueError(
                        "Unexpected external link payload (expected JSON array)."
                    )
                for row in rows:
                    if not isinstance(row, list):
                        raise ValueError("Unexpected row shape (expected list).")
                    records.append(dict(zip(cols, row)))
            return _next_chunk_index(chunk_obj)

        total_chunks = getattr(resp.manifest, "total_chunk_count", None)
        expected_rows = getattr(resp.manifest, "total_row_count", None)

        # Prefer manifest chunk count: EXTERNAL_LINKS next_chunk_index is on each
        # link, and relying only on result.next_chunk_index truncates large tables.
        if total_chunks is not None and int(total_chunks) > 0:
            if resp.result:
                _consume_chunk(resp.result)
            for chunk_index in range(1, int(total_chunks)):
                chunk = w.statement_execution.get_statement_result_chunk_n(
                    statement_id=stmt_id,
                    chunk_index=chunk_index,
                )
                _consume_chunk(chunk)
        elif resp.result:
            next_idx = _consume_chunk(resp.result)
            while next_idx is not None:
                chunk = w.statement_execution.get_statement_result_chunk_n(
                    statement_id=stmt_id,
                    chunk_index=next_idx,
                )
                next_idx = _consume_chunk(chunk)

        if expected_rows is not None and len(records) != int(expected_rows):
            raise ValueError(
                f"Incomplete Databricks EXTERNAL_LINKS fetch for {table_fqn}: "
                f"got {len(records)} rows, expected {expected_rows} "
                f"(chunks={total_chunks})"
            )

        with _L1_LOCK:
            if records:
                L1_RESP_CACHE[l1_key] = records

        if bucket_name and object_name and records:
            try:
                raw = json.dumps(
                    records, ensure_ascii=False, separators=(",", ":")
                ).encode("utf-8")
                gz = gzip.compress(raw, compresslevel=6)
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(object_name)
                blob.content_encoding = "gzip"
                try:
                    blob.upload_from_string(
                        gz,
                        content_type="application/json",
                        if_generation_match=0,  # write-once; 412 if someone beat us
                    )
                except gcs_errors.PreconditionFailed:
                    # Another writer won; fine—object exists now.
                    pass
            except Exception:
                # Cache write failures must not impact the request
                pass
        return records

    def fetch_model_version(
        self, catalog_name: str, inst_name: str, model_name: str
    ) -> Any:
        schema = databricksify_inst_name(inst_name)
        model_name_path = f"{catalog_name}.{schema}_gold.{uc_model_name(model_name)}"

        try:
            w = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            LOGGER.exception(
                "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
                databricks_vars["DATABRICKS_HOST_URL"],
                gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
            raise ValueError(f"setup_new_inst(): Workspace client creation failed: {e}")

        model_versions: Any = list(
            w.model_versions.list(
                full_name=model_name_path,
            )
        )

        if not model_versions:
            raise ValueError(f"No versions found for model: {model_name_path}")

        latest_version = max(model_versions, key=lambda v: int(v.version))

        return latest_version

    def read_volume_training_config(
        self, inst_name: str, model_run_id: str, schema_type: str
    ) -> dict[str, Any] | None:
        """Read selection criteria and training cohorts from a model run."""
        if not inst_name.strip() or not model_run_id.strip() or not schema_type.strip():
            return None
        env_schema = ENV_TO_VOLUME_SCHEMA.get(str(env_vars["ENV"]).strip().upper())
        if env_schema is None:
            LOGGER.warning(
                "Training config volumes are not configured for ENV=%r",
                env_vars["ENV"],
            )
            return None
        try:
            inst_slug = databricksify_inst_name(inst_name)
            workspace = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception:
            LOGGER.exception(
                "Could not initialize training config lookup for %s", inst_name
            )
            return None

        training_directory = (
            f"/Volumes/{env_schema}/{inst_slug}_silver/silver_volume/"
            f"{model_run_id}/training/"
        )
        return _find_training_config_in_training_dir(
            workspace, training_directory, schema_type
        )

    def delete_model(self, catalog_name: str, inst_name: str, model_name: str) -> None:
        schema = databricksify_inst_name(inst_name)
        model_name_path = f"{catalog_name}.{schema}_gold.{uc_model_name(model_name)}"

        try:
            w = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            LOGGER.exception(
                "Failed to create Databricks WorkspaceClient with host: %s and service account: %s",
                databricks_vars["DATABRICKS_HOST_URL"],
                gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
            raise ValueError(f"setup_new_inst(): Workspace client creation failed: {e}")

        try:
            w.registered_models.delete(full_name=model_name_path)
            LOGGER.info("Deleted registration model: %s", model_name_path)
        except Exception:
            LOGGER.exception("Failed to delete registered model: %s", model_name_path)
            raise

    def get_key_for_file(
        self, mapping: Dict[str, Any], file_name: str
    ) -> Optional[str]:
        """
        Case-insensitive match of file_name against mapping values.
        Values may be:
        - str literal (e.g., "student.csv") → allow optional base suffixes before the ext.
        - str regex (e.g., r"^course_.*\\.csv$") → re.IGNORECASE fullmatch.
        - compiled regex (re.Pattern) → fullmatch, adding IGNORECASE if missing.
        - list of any of the above.
        """
        # normalize filename (handles windows paths + stray whitespace)
        name = os.path.basename(file_name.replace("\\", "/")).strip()

        REGEX_META = re.compile(r"[()\[\]\{\}\|\?\+\*\\]")

        def looks_like_regex(s: str) -> bool:
            s = s.strip()
            return (
                s.startswith("^") or s.endswith("$") or REGEX_META.search(s) is not None
            )

        def matches_one(pat: Any) -> bool:
            # compiled regex
            if isinstance(pat, re.Pattern):
                # ensure case-insensitive
                flags = pat.flags | re.IGNORECASE
                return re.fullmatch(re.compile(pat.pattern, flags), name) is not None

            # string literal / regex
            if isinstance(pat, str):
                p = pat.strip()

                # exact literal (case-insensitive)
                if name.casefold() == p.casefold():
                    return True

                if looks_like_regex(p):
                    try:
                        return re.fullmatch(p, name, flags=re.IGNORECASE) is not None
                    except re.error:
                        return False

                # literal with suffix tolerance
                p_base, p_ext = os.path.splitext(p)
                if p_ext:
                    # ^base(?:[._-].+)?ext$
                    rx = re.compile(
                        rf"^{re.escape(p_base)}(?:[._-].+)?{re.escape(p_ext)}$",
                        re.IGNORECASE,
                    )
                else:
                    # ^literal(?:[._-].+)?(?:\..+)?$
                    rx = re.compile(
                        rf"^{re.escape(p)}(?:[._-].+)?(?:\..+)?$",
                        re.IGNORECASE,
                    )
                return rx.fullmatch(name) is not None

            # unsupported type
            return False

        for key, val in mapping.items():
            items = val if isinstance(val, list) else [val]
            for pat in items:
                if matches_one(pat):
                    return key

        return None
