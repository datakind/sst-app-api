"""API functions related to models."""

from datetime import datetime
from typing import Annotated, Any, cast
import jsonpickle
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, field_serializer
from sqlalchemy import and_, update, or_
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from ..databricks import (
    DatabricksControl,
    DatabricksPDPInferenceRunRequest,
    DatabricksSharedInferenceRunRequest,
)
from ..utilities import (
    has_access_to_inst_or_err,
    has_full_data_access_or_err,
    BaseUser,
    model_owner_and_higher_or_err,
    uuid_to_str,
    str_to_uuid,
    get_current_active_user,
    get_external_bucket_name,
    SchemaType,
    decode_url_piece,
    display_model_name,
    LEGACY_TO_NEW_SCHEMA,
    batch_input_validated_blob_paths,
)
from ..database import (
    get_session,
    local_session,
    BatchTable,
    FileTable,
    InstTable,
    ModelTable,
    JobTable,
)
import traceback
import logging
from ..gcsdbutils import get_filename_without_approve_dir, update_db_from_bucket
from ..config import env_vars

from ..gcsutil import StorageControl

router = APIRouter(
    prefix="/institutions",
    tags=["models"],
)


class SchemaConfigObj(BaseModel):
    """The Schema configuration for a model. What's considered valid for that model."""

    schema_type: SchemaType
    # If both of the following is set to False, you have to have 1 and only 1 of these file types. If both are set to True, you can have any number of these file types.
    # If optional is set to True, you can have 0 of these.
    optional: bool = False
    # If multiple_allowed is set to True, you can have more than 1 of these.
    multiple_allowed: bool = False


# Input file schema types used when deriving batch rules from inst.schemas.
_BATCH_INPUT_SCHEMA_ORDER: tuple[SchemaType, ...] = (
    SchemaType.COURSE,
    SchemaType.STUDENT,
    SchemaType.SEMESTER,
)
_NON_INPUT_SCHEMA_TYPES: frozenset[SchemaType] = frozenset(
    {SchemaType.UNKNOWN, SchemaType.SST_OUTPUT, SchemaType.PNG}
)


def default_schema_configs_from_inst_schemas(
    inst_schemas: list[str] | None,
) -> list[list[SchemaConfigObj]]:
    """Build a required one-of-each batch config from institution allowed schemas."""
    if not inst_schemas:
        return []

    allowed = {str(s) for s in inst_schemas}
    ordered_types: list[SchemaType] = []
    for schema_type in _BATCH_INPUT_SCHEMA_ORDER:
        if schema_type.value in allowed:
            ordered_types.append(schema_type)

    for raw in sorted(allowed):
        if raw in {t.value for t in _NON_INPUT_SCHEMA_TYPES}:
            continue
        try:
            schema_type = SchemaType(raw)
        except ValueError:
            continue
        if schema_type in _NON_INPUT_SCHEMA_TYPES or schema_type in ordered_types:
            continue
        ordered_types.append(schema_type)

    if not ordered_types:
        # Legacy and GenAI institutions allow arbitrary uploads (UNKNOWN only).
        if SchemaType.UNKNOWN.value in allowed:
            return [
                [
                    SchemaConfigObj(
                        schema_type=SchemaType.UNKNOWN,
                        optional=False,
                        multiple_allowed=True,
                    )
                ]
            ]
        return []

    return [
        [
            SchemaConfigObj(
                schema_type=schema_type,
                optional=False,
                multiple_allowed=False,
            )
            for schema_type in ordered_types
        ]
    ]


def resolve_model_schema_configs(
    raw_config: Any,
    inst_schemas: list[str] | None,
) -> list[list[SchemaConfigObj]]:
    """Return batch schema rules from the model row or derive from institution schemas."""
    if raw_config is None:
        derived = default_schema_configs_from_inst_schemas(inst_schemas)
        if not derived:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=(
                    "Model has no schema_configs and the institution schemas could not "
                    "be used to derive a default batch configuration. Configure input "
                    "schema types on the institution (e.g. STUDENT, COURSE)."
                ),
            )
        return derived

    if not isinstance(raw_config, str):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Model schema_configs must be a jsonpickle-encoded string.",
        )

    normalized = raw_config
    for legacy, new in LEGACY_TO_NEW_SCHEMA.items():
        normalized = normalized.replace(f'"{legacy}"', f'"{new}"')
    return cast(list[list[SchemaConfigObj]], jsonpickle.decode(normalized))


def _is_any_format_schema_config(config: list[SchemaConfigObj]) -> bool:
    """True when config means any upload schema is acceptable (legacy/genai)."""
    return (
        len(config) == 1
        and config[0].schema_type == SchemaType.UNKNOWN
        and config[0].multiple_allowed
        and not config[0].optional
    )


def check_file_types_valid_schema_configs(
    file_types: list[list[SchemaType]],
    valid_schema_configs: list[list[SchemaConfigObj]],
) -> bool:
    """Check that a list of files are valid for a given schema configuration."""
    for config in valid_schema_configs:
        if _is_any_format_schema_config(config):
            if file_types:
                return True
            continue
        found = True
        map_file_to_schema_config_obj: dict = {}
        for idx, s in enumerate(file_types):
            for c in config:
                if c.schema_type in s:
                    if not map_file_to_schema_config_obj.get(c.schema_type):
                        map_file_to_schema_config_obj[c.schema_type] = [idx]
                    else:
                        map_file_to_schema_config_obj[c.schema_type] = (
                            map_file_to_schema_config_obj[c.schema_type] + [idx]
                        )
        for c in config:
            val = map_file_to_schema_config_obj.get(c.schema_type)
            if (not val and not c.optional) or (
                val and len(val) > 1 and not c.multiple_allowed
            ):
                found = False
        for idx, s in enumerate(file_types):
            # Check for if files that didn't match the allowed schemas were present.
            found_v = False
            for v in map_file_to_schema_config_obj.values():
                if idx in v:
                    found_v = True
            if not found_v:
                found = False
        if found:
            return True
    return False


class ModelCreationRequest(BaseModel):
    """Model creation request object."""

    name: str


class ModelInfo(BaseModel):
    """The model object that's returned."""

    # The model id is unique for every instance of the model (e.g. model name + version id pair)
    m_id: str
    name: str
    inst_id: str
    # User id of created_by.
    created_by: str | None = None
    valid: bool = True
    deleted: bool | None = None
    archived: bool = False

    @field_serializer("name")
    def _display_name(self, name: str) -> str:
        # UC always has 4d5; 4.5 is frontend display only.
        return display_model_name(name)


def _model_version_as_str(version: Any) -> str | None:
    """Databricks model versions are ints; RunInfo and job rows store them as str."""
    if version is None:
        return None
    return str(version)


class RunInfo(BaseModel):
    """The RunInfo object that's returned."""

    run_id: int
    inst_id: str
    m_name: str
    # user id of the person who executed this run.
    created_by: str | None = None
    # Time the run info was triggered if it was triggered in the webapp
    triggered_at: datetime | None = None
    # Batch used for the run
    batch_name: str | None = None
    # output file name
    output_filename: str | None = None
    output_valid: bool = False
    completed: bool | None = None
    err_msg: str | None = None
    model_run_id: str | None = None
    model_version: str | None = None

    @field_serializer("m_name")
    def _display_m_name(self, m_name: str) -> str:
        # UC always has 4d5; 4.5 is frontend display only.
        return display_model_name(m_name)


class InferenceRunRequest(BaseModel):
    """Parameters for an inference run."""

    batch_name: str
    # Note: is_pdp is kept for backward compatibility but is ignored.
    # PDP status is derived from the institution's pdp_id field.
    is_pdp: bool = False
    # Legacy schools inference parameters (optional passthrough; ignored for PDP)
    config_file_name: str | None = None
    features_table_name: str | None = None


# Model related operations. Or model specific data.


@router.get("/{inst_id}/models", response_model=list[ModelInfo])
def read_inst_models(
    inst_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Returns top-level view of all models attributed to a given institution. Versions and model history are not retained in the model table. That will need to be looked up in Databricks.

    Only visible to data owners of that institution or higher.
    """
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "models")
    local_session.set(sql_session)
    query_result = (
        local_session.get()
        .execute(
            select(ModelTable).where(
                and_(
                    ModelTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    res = []
    for elem in query_result:
        res.append(
            {
                "m_id": uuid_to_str(elem[0].id),
                "inst_id": uuid_to_str(elem[0].inst_id),
                "name": elem[0].name,
                "created_by": uuid_to_str(elem[0].created_by),
                "deleted": elem[0].deleted,
                "valid": elem[0].valid,
                "archived": bool(elem[0].archived),
            }
        )
    return res


@router.post("/{inst_id}/models/", response_model=ModelInfo)
def create_model(
    inst_id: str,
    req: ModelCreationRequest,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Create a new model (kicks off training a new model).

    Only visible to model owners of that institution or higher. This function may take a
    list of training data batch ids.
    """
    # TODO add validity check for the schema config obj
    has_access_to_inst_or_err(inst_id, current_user)
    model_owner_and_higher_or_err(current_user, "model training")
    local_session.set(sql_session)
    req_name = decode_url_piece(req.name.strip())
    query_result = (
        local_session.get()
        .execute(
            select(ModelTable).where(
                and_(
                    ModelTable.name == req_name,
                    ModelTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    if len(query_result) == 0:
        model = ModelTable(
            name=req_name,
            inst_id=str_to_uuid(inst_id),
            created_by=str_to_uuid(current_user.user_id),
            valid=True,
        )
        local_session.get().add(model)
        local_session.get().commit()
        query_result = (
            local_session.get()
            .execute(
                select(ModelTable).where(
                    and_(
                        ModelTable.name == req_name,
                        ModelTable.inst_id == str_to_uuid(inst_id),
                    )
                )
            )
            .all()
        )
        if not query_result:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database write of the model creation failed.",
            )
        if len(query_result) > 1:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database write of the model created duplicate entries.",
            )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Model with this name already exists.",
        )
    return {
        "m_id": uuid_to_str(query_result[0][0].id),
        "inst_id": uuid_to_str(query_result[0][0].inst_id),
        "name": query_result[0][0].name,
        "created_by": uuid_to_str(query_result[0][0].created_by),
        "deleted": query_result[0][0].deleted,
        "valid": query_result[0][0].valid,
        "archived": bool(query_result[0][0].archived),
    }


@router.get("/{inst_id}/models/{model_name}", response_model=ModelInfo)
def read_inst_model(
    inst_id: str,
    model_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Returns a specific model's details e.g. model card.

    Only visible to data owners of that institution or higher.
    """
    model_name = decode_url_piece(model_name)
    has_access_to_inst_or_err(inst_id, current_user)
    has_full_data_access_or_err(current_user, "this model")
    local_session.set(sql_session)
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
    if len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multiple models found.",
        )
    return {
        "m_id": uuid_to_str(query_result[0][0].id),
        "inst_id": uuid_to_str(query_result[0][0].inst_id),
        "name": query_result[0][0].name,
        "created_by": uuid_to_str(query_result[0][0].created_by),
        "deleted": query_result[0][0].deleted,
        "valid": query_result[0][0].valid,
        "archived": bool(query_result[0][0].archived),
    }


@router.delete("/{inst_id}/models/{model_name}")
def delete_model(
    inst_id: str,
    model_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    transformed_model_name = str(decode_url_piece(model_name)).strip()
    has_access_to_inst_or_err(inst_id, current_user)

    local_session.set(sql_session)
    sess = local_session.get()

    model_list = sess.execute(
        select(ModelTable).where(
            ModelTable.name == transformed_model_name,
            ModelTable.inst_id == str_to_uuid(inst_id),
        )
    ).scalar_one_or_none()
    if model_list is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Model not found."
        )

    # 2) Optionally Delete models from databricks itself
    # TODO: Add databricks deletion functionality

    try:
        sess.delete(model_list)
        sess.commit()
    except Exception as e:
        sess.rollback()
        raise HTTPException(
            status_code=500, detail=f"DB batch delete failed after file cleanup: {e}"
        )

    return {
        "inst_id": inst_id,
        "model_name": transformed_model_name,
        "status": "Model deleted",
    }


@router.patch("/{inst_id}/models/{model_name}/archive")
def archive_model(
    inst_id: str,
    model_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
    """Archive a model by setting ``archived`` from 0 to 1."""
    transformed_model_name = str(decode_url_piece(model_name)).strip()
    has_access_to_inst_or_err(inst_id, current_user)

    local_session.set(sql_session)
    sess = local_session.get()

    model = sess.execute(
        select(ModelTable).where(
            ModelTable.name == transformed_model_name,
            ModelTable.inst_id == str_to_uuid(inst_id),
        )
    ).scalar_one_or_none()
    if model is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Model not found."
        )
    if model.archived:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Model is already archived.",
        )

    model.archived = 1
    sess.commit()

    return {
        "inst_id": inst_id,
        "model_name": transformed_model_name,
        "archived": 1,
        "status": "Model archived",
    }


@router.get("/{inst_id}/models/{model_name}/runs", response_model=list[RunInfo])
def read_inst_model_outputs(
    inst_id: str,
    model_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Returns top-level info around all executions of a given model.

    Only visible to users of that institution or Datakinder access types.
    Returns a list of runs in order of most recent to least recent based on triggering time.
    """
    model_name = decode_url_piece(model_name)
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
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
    if len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multiple models of the same name found, this should not have happened.",
        )
    update_db_from_bucket(inst_id, local_session.get(), storage_control)
    local_session.get().commit()
    if not query_result[0][0].jobs:
        return []
    result = list(query_result[0][0].jobs)
    result.sort(key=lambda x: x.triggered_at, reverse=True)
    ret_val = []
    for elem in result:
        # This will show incomplete runs as well.
        # TODO make a query to databricks to retrieve status.
        ret_val.append(
            {
                # JobTable doesn't have inst_id, so we retrieve that from the model query.
                "inst_id": uuid_to_str(query_result[0][0].inst_id),
                "m_name": query_result[0][0].name,
                "run_id": elem.id,
                "model_run_id": elem.model_run_id,
                "model_version": elem.model_version,
                "created_by": uuid_to_str(elem.created_by),
                "triggered_at": elem.triggered_at,
                "batch_name": elem.batch_name,
                "output_filename": elem.output_filename,
                "output_valid": False if not elem.output_valid else elem.output_valid,
                "completed": False if not elem.completed else elem.completed,
            }
        )
    return ret_val


@router.get(
    "/{inst_id}/models/{model_name}/run/{run_id}",
    response_model=RunInfo,
)
def read_inst_model_output(
    inst_id: str,
    model_name: str,
    run_id: int,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Returns a given executions of a given model.

    Only visible to users of that institution or Datakinder access types.
    If a viewer has record allowlist restrictions applied, only those records are returned.
    """
    model_name = decode_url_piece(model_name)
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
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
    if len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multiple models of the same name found, this should not have happened.",
        )
    update_db_from_bucket(inst_id, local_session.get(), storage_control)
    local_session.get().commit()
    for elem in query_result[0][0].jobs or []:
        if elem.id == run_id:
            # TODO: if the output_filename is empty make a query to Databricks
            return {
                "inst_id": uuid_to_str(query_result[0][0].inst_id),
                "m_name": query_result[0][0].name,
                "run_id": elem.id,
                "created_by": uuid_to_str(elem.created_by),
                "triggered_at": elem.triggered_at,
                "batch_name": elem.batch_name,
                "output_filename": elem.output_filename,
                "output_valid": False if not elem.output_valid else elem.output_valid,
                "completed": False if not elem.completed else elem.completed,
                "model_run_id": elem.model_run_id,
                "model_version": elem.model_version,
            }
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Run not found.",
    )


@router.delete("/{inst_id}/models/{model_name}/run/{job_run_id}")
def delete_model_run(
    inst_id: str,
    model_name: str,
    job_run_id: int,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    storage_control: Annotated[StorageControl, Depends(StorageControl)],
) -> Any:
    """Deletes a given inference job run for a model.

    ``job_run_id`` is the Databricks Jobs run id (also ``job.id``), not the
    MLflow ``model_run_id``.

    Only visible to users of that institution or Datakinder access types.
    """
    model_name = decode_url_piece(model_name)
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    sess = local_session.get()
    query_result = sess.execute(
        select(ModelTable).where(
            and_(
                ModelTable.name == model_name,
                ModelTable.inst_id == str_to_uuid(inst_id),
            )
        )
    ).all()
    if len(query_result) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Model not found.",
        )
    if len(query_result) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Multiple models of the same name found, this should not have happened.",
        )
    job = next(
        (elem for elem in (query_result[0][0].jobs or []) if elem.id == job_run_id),
        None,
    )
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Run not found.",
        )

    bucket = get_external_bucket_name(inst_id)
    output_names: set[str] = set()
    for dir_prefix in ("approved/", "unapproved/"):
        for blob_path in storage_control.list_blobs_in_folder(
            bucket, f"{dir_prefix}{job_run_id}/"
        ):
            try:
                storage_control.delete_file(bucket_name=bucket, file_name=blob_path)
                output_names.add(get_filename_without_approve_dir(blob_path))
            except ValueError:
                pass

    if output_names:
        file_rows = (
            sess.execute(
                select(FileTable).where(
                    and_(
                        FileTable.inst_id == str_to_uuid(inst_id),
                        FileTable.name.in_(output_names),
                    )
                )
            )
            .scalars()
            .all()
        )
        for file_row in file_rows:
            sess.delete(file_row)

    try:
        sess.delete(job)
        sess.commit()
    except Exception as e:
        sess.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"DB run delete failed: {e}",
        ) from e

    return {
        "inst_id": inst_id,
        "model_name": model_name,
        "run_id": job_run_id,
        "status": "Run deleted",
    }


def convert_files_to_dict(files):
    """Convert files to a dictionary."""
    res = {}
    for f in files:
        # TODO: construct the filepath instead -- where does the filepath need to start? bucket level?
        res[f.name] = f.schemas
    return res


@router.post(
    "/{inst_id}/models/{model_name}/run-inference",
    response_model=RunInfo,
)
def trigger_inference_run(
    inst_id: str,
    model_name: str,
    req: InferenceRunRequest,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
) -> Any:
    """Returns top-level info around all executions of a given model.

    Only visible to users of that institution or Datakinder access types.
    """
    model_name = decode_url_piece(model_name)
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
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
    if len(inst_result) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unexpected number of institutions found: Expected 1, got "
            + str(len(inst_result)),
        )
    inst = inst_result[0][0]
    # Determine institution type: PDP, Edvise Schema (ES), Legacy, or GenAI.
    # Follows the same pattern as validation_helper in data.py.
    pdp_id = getattr(inst, "pdp_id", None)
    edvise_id = getattr(inst, "edvise_id", None)
    legacy_id = getattr(inst, "legacy_id", None)
    genai_id = getattr(inst, "genai_id", None)
    if sum(bool(x) for x in (pdp_id, edvise_id, legacy_id, genai_id)) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                "Institution configuration error: cannot have more than one of "
                "pdp_id, edvise_id, legacy_id, or genai_id set"
            ),
        )
    is_pdp = bool(pdp_id)
    is_legacy = bool(legacy_id)
    is_edvise = bool(edvise_id) or bool(genai_id)

    # Legacy, Edvise Schema (ES), and GenAI inference
    if is_legacy or is_edvise:
        # or: legacy_or_edvise_model_result ?
        shared_model_result = (
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
        if len(shared_model_result) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unexpected number of models found: Expected 1, got "
                + str(len(shared_model_result)),
            )

        batch_result = (
            local_session.get()
            .execute(
                select(BatchTable).where(
                    and_(
                        BatchTable.name == req.batch_name,
                        BatchTable.inst_id == str_to_uuid(inst_id),
                    )
                )
            )
            .all()
        )
        if len(batch_result) != 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Unexpected number of batches found: Expected 1, got "
                + str(len(batch_result)),
            )
        batch = batch_result[0][0]
        inst_file_schemas = [list({s for f in batch.files for s in f.schemas})]
        schema_configs = resolve_model_schema_configs(
            shared_model_result[0][0].schema_configs,
            inst.schemas,
        )
        if not check_file_types_valid_schema_configs(
            inst_file_schemas,
            schema_configs,
        ):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"The files in this batch don't conform to the schema configs allowed by this model. For debugging reference - file_schema={inst_file_schemas} and model_schema={schema_configs}",
            )

        db_req = DatabricksSharedInferenceRunRequest(
            inst_name=inst_result[0][0].name,
            model_name=model_name,
            config_file_name=req.config_file_name or "",
            features_table_name=req.features_table_name or "",
            gcp_external_bucket_name=get_external_bucket_name(inst_id),
            email=current_user.email or "",
            batch_id=uuid_to_str(batch.id),
            validated_blob_paths=batch_input_validated_blob_paths(batch.files),
            is_genai_institution=bool(genai_id),
        )
        try:
            if is_legacy:
                res = databricks_control.run_legacy_inference(db_req)
            else:
                res = databricks_control.run_es_inference(db_req)
        except Exception as e:
            tb = traceback.format_exc()
            logging.error(f"Databricks run failure:\n{tb}")
            op = "run_legacy_inference" if is_legacy else "run_es_inference"
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Databricks {op} error. Error = {str(e)}",
            ) from e
        triggered_timestamp = datetime.now()
        latest_model_version = databricks_control.fetch_model_version(
            catalog_name=str(env_vars["CATALOG_NAME"]),
            inst_name=inst_result[0][0].name,
            model_name=model_name,
        )
        model_version = _model_version_as_str(latest_model_version.version)
        model_run_id = latest_model_version.run_id
        job = JobTable(
            id=res.job_run_id,
            triggered_at=triggered_timestamp,
            created_by=str_to_uuid(current_user.user_id),
            batch_name=req.batch_name,
            model_id=shared_model_result[0][0].id,
            output_valid=False,
            model_version=model_version,
            model_run_id=model_run_id,
        )
        local_session.get().add(job)
        return {
            "inst_id": inst_id,
            "m_name": model_name,
            "run_id": res.job_run_id,
            "created_by": current_user.user_id,
            "triggered_at": triggered_timestamp,
            "batch_name": req.batch_name,
            "output_valid": False,
            "model_version": model_version,
            "model_run_id": model_run_id,
        }

    # PDP inference (existing logic)
    if not is_pdp:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Currently, only PDP, Legacy, and Edvise Schema (ES) schools inference are supported.",
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
    if len(query_result) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unexpected number of models found: Expected 1, got "
            + str(len(query_result)),
        )

    # Get all the files in the batch and check that it matches the model specifications.
    batch_result = (
        local_session.get()
        .execute(
            select(BatchTable).where(
                and_(
                    BatchTable.name == req.batch_name,
                    BatchTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    if len(batch_result) != 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Unexpected number of batches found: Expected 1, got "
            + str(len(inst_result)),
        )
    # inst_file_schemas = [x.schemas for x in batch_result[0][0].files]
    inst_file_schemas = [list({s for f in batch_result[0][0].files for s in f.schemas})]
    schema_configs = resolve_model_schema_configs(
        query_result[0][0].schema_configs,
        inst.schemas,
    )

    if not check_file_types_valid_schema_configs(
        inst_file_schemas,
        schema_configs,
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The files in this batch don't conform to the schema configs allowed by this model. For debugging reference - file_schema={inst_file_schemas} and model_schema={schema_configs}",
        )
    # Note to Datakind: In the long-term, this is where you would have a case block or something that would call different types of pipelines.
    pdp_db_req = DatabricksPDPInferenceRunRequest(
        inst_name=inst_result[0][0].name,
        filepath_to_type=convert_files_to_dict(batch_result[0][0].files),
        model_name=model_name,
        gcp_external_bucket_name=get_external_bucket_name(inst_id),
        # The institution email to which pipeline success/failure notifications will get sent.
        email=cast(str, current_user.email),
    )
    try:
        res = databricks_control.run_pdp_inference(pdp_db_req)
    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"Databricks run failure:\n{tb}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Databricks run_pdp_inference error. Error = {str(e)}",
        ) from e
    triggered_timestamp = datetime.now()
    latest_model_version = databricks_control.fetch_model_version(
        catalog_name=str(env_vars["CATALOG_NAME"]),
        inst_name=inst_result[0][0].name,
        model_name=model_name,
    )
    model_version = _model_version_as_str(latest_model_version.version)
    model_run_id = latest_model_version.run_id
    job = JobTable(
        id=res.job_run_id,
        triggered_at=triggered_timestamp,
        created_by=str_to_uuid(current_user.user_id),
        batch_name=req.batch_name,
        model_id=query_result[0][0].id,
        output_valid=False,
        model_version=model_version,
        model_run_id=model_run_id,
    )
    local_session.get().add(job)
    return {
        "inst_id": inst_id,
        "m_name": model_name,
        "run_id": res.job_run_id,
        "created_by": current_user.user_id,
        "triggered_at": triggered_timestamp,
        "batch_name": req.batch_name,
        "output_valid": False,
        "model_version": model_version,
        "model_run_id": model_run_id,
    }


@router.get("/{inst_id}/models/{model_name}/get-model-versions")
def get_model_versions(
    inst_id: str,
    model_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
) -> Any:
    transformed_model_name = str(decode_url_piece(model_name)).strip()
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

    print(f"Initial model name = {model_name}")
    print(f"Converted model name {transformed_model_name}")

    latest_model_version = databricks_control.fetch_model_version(
        catalog_name=str(env_vars["CATALOG_NAME"]),
        inst_name=f"{query_result[0][0].name}",
        model_name=transformed_model_name,
    )

    return latest_model_version


@router.post("/{inst_id}/models/{model_name}/backfill-model-runs")
def backfill_model_runs(
    inst_id: str,
    model_name: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
    databricks_control: Annotated[DatabricksControl, Depends(DatabricksControl)],
) -> Any:
    """Backfills missing model run metadata and returns the latest model version info.

    Temporary endpoint to populate model_run_id and model_version on existing jobs for this model.
    Use only when backfilling historical job runs, not for regular operation.
    """
    model_name = str(decode_url_piece(model_name)).strip()
    has_access_to_inst_or_err(inst_id, current_user)

    # Load institution
    local_session.set(sql_session)
    inst_row = (
        local_session.get()
        .execute(select(InstTable).where(InstTable.id == str_to_uuid(inst_id)))
        .all()
    )

    model_id = (
        local_session.get()
        .execute(
            select(ModelTable).where(
                and_(
                    ModelTable.inst_id == str_to_uuid(inst_id),
                    ModelTable.name == model_name,
                )
            )
        )
        .all()
    )

    if not inst_row or len(inst_row) == 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Institution not found.",
        )
    if len(inst_row) > 1:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Institution duplicates found.",
        )

    latest_mv = databricks_control.fetch_model_version(
        catalog_name=str(env_vars["CATALOG_NAME"]),
        inst_name=f"{inst_row[0][0].name}",
        model_name=model_name,
    )

    mv_version = str(latest_mv.version)
    mv_run_id = str(latest_mv.run_id)

    # UPDATE existing jobs for this model (only those missing values)
    stmt = (
        update(JobTable)
        .where(JobTable.model_id == model_id[0][0].id)
        .where(
            or_(
                JobTable.model_run_id.is_(None),
                JobTable.model_run_id == "",
                JobTable.model_version.is_(None),
                JobTable.model_version == "",
            )
        )
        .values(model_run_id=mv_run_id, model_version=mv_version)
    )
    result = local_session.get().execute(stmt)
    updated_count = result.rowcount or 0  # type: ignore
    local_session.get().commit()

    return {
        "inst_id": str(inst_id),
        "model_id": str(model_id[0][0].id),
        "model_name": model_name,
        "latest_model_version": {"version": mv_version, "run_id": mv_run_id},
        "updated_count": updated_count,
    }
