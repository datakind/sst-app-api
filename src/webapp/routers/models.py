"""API functions related to models."""

from datetime import datetime
from typing import Annotated, Any, cast
import jsonpickle
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import and_
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from ..databricks import DatabricksControl, DatabricksInferenceRunRequest
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
    LEGACY_TO_NEW_SCHEMA,
)
from ..database import (
    get_session,
    local_session,
    BatchTable,
    InstTable,
    ModelTable,
    JobTable,
)
import traceback
import logging
from ..gcsdbutils import update_db_from_bucket

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


def check_file_types_valid_schema_configs(
    file_types: list[list[SchemaType]],
    valid_schema_configs: list[list[SchemaConfigObj]],
) -> bool:
    """Check that a list of files are valid for a given schema configuration."""
    for config in valid_schema_configs:
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
    # valid = False, means the model is not ready for use.
    valid: bool = False
    schema_configs: list[list[SchemaConfigObj]]
    framework: str


class ModelInfo(BaseModel):
    """The model object that's returned."""

    # The model id is unique for every instance of the model (e.g. model name + version id pair)
    m_id: str
    name: str
    inst_id: str
    # User id of created_by.
    created_by: str | None = None
    valid: bool = False
    deleted: bool | None = None


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


class InferenceRunRequest(BaseModel):
    """Parameters for an inference run."""

    batch_name: str
    # This MUST be set for uses of the PDP inference pipeline.
    is_pdp: bool = False


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
    query_result = (
        local_session.get()
        .execute(
            select(ModelTable).where(
                and_(
                    ModelTable.name == req.name,
                    ModelTable.inst_id == str_to_uuid(inst_id),
                )
            )
        )
        .all()
    )
    if len(query_result) == 0:
        model = ModelTable(
            name=req.name,
            inst_id=str_to_uuid(inst_id),
            created_by=str_to_uuid(current_user.user_id),
            valid=req.valid,
            schema_configs=jsonpickle.encode(req.schema_configs),
            framework=(
                f
                if (f := (req.framework or "").strip().lower()) in {"sklearn", "h2o"}
                else "sklearn"
            ),
        )
        local_session.get().add(model)
        local_session.get().commit()
        query_result = (
            local_session.get()
            .execute(
                select(ModelTable).where(
                    and_(
                        ModelTable.name == req.name,
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
        "framework": query_result[0][0].framework,
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
        "framework": query_result[0][0].framework,
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
            }
    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail="Run not found.",
    )


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
    if not req.is_pdp:
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Currently, only PDP inference is supported.",
        )
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
    raw_config = query_result[0][0].schema_configs

    # Inline legacy → new mapping directly on the string
    for legacy, new in LEGACY_TO_NEW_SCHEMA.items():
        # This replaces every occurrence of "PDP_COURSE", "PDP_COHORT", etc.
        raw_config = raw_config.replace(f'"{legacy}"', f'"{new}"')

    schema_configs = jsonpickle.decode(raw_config)
    # POTENTIAL_REVERSE: schema_configs = jsonpickle.decode(query_result[0][0].schema_configs)

    if not check_file_types_valid_schema_configs(
        inst_file_schemas,
        schema_configs,
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"The files in this batch don't conform to the schema configs allowed by this model. For debugging reference - file_schema={inst_file_schemas} and model_schema={schema_configs}",
        )
    # Note to Datakind: In the long-term, this is where you would have a case block or something that would call different types of pipelines.
    db_req = DatabricksInferenceRunRequest(
        inst_name=inst_result[0][0].name,
        filepath_to_type=convert_files_to_dict(batch_result[0][0].files),
        model_name=model_name,
        gcp_external_bucket_name=get_external_bucket_name(inst_id),
        # The institution email to which pipeline success/failure notifications will get sent.
        email=cast(str, current_user.email),
        model_type=query_result[0][0].framework,
    )
    try:
        res = databricks_control.run_pdp_inference(db_req)
    except Exception as e:
        tb = traceback.format_exc()
        logging.error(f"Databricks run failure:\n{tb}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Databricks run_pdp_inference error. Error = {str(e)}",
        ) from e
    triggered_timestamp = datetime.now()
    job = JobTable(
        id=res.job_run_id,
        triggered_at=triggered_timestamp,
        created_by=str_to_uuid(current_user.user_id),
        batch_name=req.batch_name,
        model_id=query_result[0][0].id,
        output_valid=False,
        framework=query_result[0][0].framework,
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
        "framework": query_result[0][0].framework,
    }
