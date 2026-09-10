"""API functions related to data."""

from databricks.sdk import WorkspaceClient
from typing import Annotated, Any, cast, IO, Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy.future import select
import logging
from ..config import ENV_TO_VOLUME_SCHEMA, databricks_vars, env_vars, gcs_vars
import tempfile
import pathlib

from ..utilities import (
    has_access_to_inst_or_err,
    BaseUser,
    str_to_uuid,
    get_current_active_user,
    databricksify_inst_name,
    uc_model_name,
)

from ..database import (
    get_session,
    local_session,
    InstTable,
    JobTable,
)

from ..databricks import DatabricksControl

# Set the logging
logging.basicConfig(format="%(asctime)s [%(levelname)s]: %(message)s")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

router = APIRouter(
    prefix="/institutions",
    tags=["front_end_tables"],
)

LOGGER = logging.getLogger(__name__)


## FE Inference Tables


# Get SHAP Values for Inference
@router.get("/{inst_id}/inference/top-features/{job_run_id}")
def get_inference_top_features(
    inst_id: str,
    job_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
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
) -> Any:
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
) -> Any:
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
) -> Any:
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


@router.get("/{inst_id}/training/feature_importance/{model_run_id}")
def get_training_feature_importance(
    inst_id: str,
    model_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
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
            table_name=f"training_{model_run_id}_shap_feature_importance",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/confusion_matrix/{model_run_id}")
def get_training_confusion_matrix(
    inst_id: str,
    model_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
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
            table_name=f"training_{model_run_id}_confusion_matrix",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/roc_curve/{model_run_id}")
def get_training_roc_curve(
    inst_id: str,
    model_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
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
            table_name=f"training_{model_run_id}_roc_curve",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/support-overview/{model_run_id}")
def get_training_support_overview(
    inst_id: str,
    model_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> Any:
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
            table_name=f"training_{model_run_id}_support_overview",
            warehouse_id=env_vars["SQL_WAREHOUSE_ID"],  # type: ignore
        )

        return rows
    except ValueError as ve:
        # Return a 400 error with the specific message from ValueError
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))


@router.get("/{inst_id}/training/model-cards/{model_run_id}")
def get_model_cards(
    inst_id: str,
    model_run_id: str,
    current_user: Annotated[BaseUser, Depends(get_current_active_user)],
    sql_session: Annotated[Session, Depends(get_session)],
) -> FileResponse:
    has_access_to_inst_or_err(inst_id, current_user)
    local_session.set(sql_session)
    session = local_session.get()
    query_result = session.execute(
        select(InstTable).where(InstTable.id == str_to_uuid(inst_id))
    ).all()

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

    job_result = session.scalars(
        select(JobTable)
        .where(JobTable.model_run_id == model_run_id)
        .order_by(
            JobTable.triggered_at.desc()
        )  # keep if multiple jobs can share a model_run_id
    ).first()

    if job_result is None or not job_result.model_run_id:
        raise HTTPException(
            status_code=404, detail="No model run found for this model."
        )

    run_id = job_result.model_run_id
    model_name = uc_model_name(job_result.model.name)

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
        env = str(env_vars["ENV"]).strip().upper()
        if env not in ENV_TO_VOLUME_SCHEMA:
            raise ValueError(
                f"Unsupported ENV {env_vars.get('ENV')!r}; expected DEV or STAGING"
            )
        env_schema = ENV_TO_VOLUME_SCHEMA[env]

        volume_path = f"/Volumes/{env_schema}/{databricksify_inst_name(query_result[0][0].name)}_gold/gold_volume/model_cards/{run_id}/model-card-{model_name}.pdf"
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
