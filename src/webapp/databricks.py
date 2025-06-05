"""Databricks SDk related helper functions."""

import os
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog
from databricks.sdk.service.sql import (
    Format,
    ExecuteStatementRequestOnWaitTimeout,
    Disposition,
    StatementState,
)
from .config import databricks_vars, gcs_vars
from .utilities import databricksify_inst_name, SchemaType
from typing import List, Any, Dict
from databricks.sdk.errors import DatabricksError


# List of data medallion levels
MEDALLION_LEVELS = ["silver", "gold", "bronze"]

# The name of the deployed pipeline in Databricks. Must match directly.
PDP_INFERENCE_JOB_NAME = "github_sourced_pdp_inference_pipeline"


class DatabricksInferenceRunRequest(BaseModel):
    """Databricks parameters for an inference run."""

    inst_name: str
    # Note that the following should be the filepath.
    filepath_to_type: dict[str, list[SchemaType]]
    model_name: str
    model_type: str = "sklearn"
    # The email where notifications will get sent.
    email: str
    gcp_external_bucket_name: str


class DatabricksInferenceRunResponse(BaseModel):
    """Databricks parameters for an inference run."""

    job_run_id: int


def get_filepath_of_filetype(
    file_dict: dict[str, list[SchemaType]], file_type: SchemaType
):
    """Helper functions to get a file of a given file_type.
    For both, we will return the first file that matches the schema."""
    for k, v in file_dict.items():
        if file_type in v:
            return k
    return ""


def check_types(dict_values, file_type: SchemaType):
    """Check the file type is in the dict dictionary."""
    for elem in dict_values:
        if file_type in elem:
            return True
    return False


# Wrapping the usages in a class makes it easier to unit test via mocks.
class DatabricksControl(BaseModel):
    """Object to manage interfacing with GCS."""

    def setup_new_inst(self, inst_name: str) -> None:
        """Sets up Databricks resources for a new institution."""
        w = WorkspaceClient(
            host=databricks_vars["DATABRICKS_HOST_URL"],
            # This should still be cloud run, since it's cloud run triggering the databricks
            # this account needs to exist on Databricks as well and needs to hvae the creation and job management permissions
            google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )
        if w is None:
            raise ValueError("setup_new_inst() workspace retrieval failed.")

        db_inst_name = databricksify_inst_name(inst_name)
        cat_name = databricks_vars["CATALOG_NAME"]
        for medallion in MEDALLION_LEVELS:
            w.schemas.create(name=f"{db_inst_name}_{medallion}", catalog_name=cat_name)
        # Create a managed volume in the bronze schema for internal pipeline data.
        # update to include a managed volume for toml files
        created_volume_bronze = w.volumes.create(
            catalog_name=cat_name,
            schema_name=f"{db_inst_name}_bronze",
            name="bronze_volume",
            volume_type=catalog.VolumeType.MANAGED,
        )
        created_volume_silver = w.volumes.create(
            catalog_name=cat_name,
            schema_name=f"{db_inst_name}_silver",
            name="silver_volume",
            volume_type=catalog.VolumeType.MANAGED,
        )
        created_volume_gold = w.volumes.create(
            catalog_name=cat_name,
            schema_name=f"{db_inst_name}_gold",
            name="gold_volume",
            volume_type=catalog.VolumeType.MANAGED,
        )
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

    # Note that for each unique PIPELINE, we'll need a new function, this is by nature of how unique pipelines
    # may have unique parameters and would have a unique name (i.e. the name field specified in w.jobs.list()). But any run of a given pipeline (even across institutions) can use the same function.
    # E.g. there is one PDP inference pipeline, so one PDP inference function here.

    def run_pdp_inference(
        self, req: DatabricksInferenceRunRequest
    ) -> DatabricksInferenceRunResponse:
        """Triggers PDP inference Databricks run."""
        if (
            not req.filepath_to_type
            or not check_types(req.filepath_to_type.values(), SchemaType.PDP_COURSE)
            or not check_types(req.filepath_to_type.values(), SchemaType.PDP_COHORT)
        ):
            raise ValueError(
                "run_pdp_inference() requires PDP_COURSE and PDP_COHORT type files to run."
            )
        w = WorkspaceClient(
            host=databricks_vars["DATABRICKS_HOST_URL"],
            google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )
        if not w:
            raise ValueError("run_pdp_inference(): Databricks workspace not found.")
        db_inst_name = databricksify_inst_name(req.inst_name)
        job_id = next(w.jobs.list(name=PDP_INFERENCE_JOB_NAME)).job_id
        if not job_id:
            raise ValueError("run_pdp_inference(): Job was not created.")
        run_job = w.jobs.run_now(
            job_id,
            job_parameters={
                "cohort_file_name": get_filepath_of_filetype(
                    req.filepath_to_type, SchemaType.PDP_COHORT
                ),
                "course_file_name": get_filepath_of_filetype(
                    req.filepath_to_type, SchemaType.PDP_COURSE
                ),
                "databricks_institution_name": db_inst_name,
                "DB_workspace": databricks_vars[
                    "DATABRICKS_WORKSPACE"
                ],  # is this value the same PER environ? dev/staging/prod
                "gcp_bucket_name": req.gcp_external_bucket_name,
                "model_name": req.model_name,
                "model_type": req.model_type,
                "notification_email": req.email,
            },
        )
        if not run_job:
            raise ValueError("run_pdp_inference(): Job could not be run.")
        return DatabricksInferenceRunResponse(job_run_id=run_job.response.run_id)

    def delete_inst(self, inst_name: str) -> None:
        """Cleanup tasks required on the Databricks side to delete an institution."""
        db_inst_name = databricksify_inst_name(inst_name)
        cat_name = databricks_vars["CATALOG_NAME"]
        w = WorkspaceClient(
            host=databricks_vars["DATABRICKS_HOST_URL"],
            # This should still be cloud run, since it's cloud run triggering the databricks
            # this account needs to exist on Databricks as well and needs to have permissions.
            google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
        )
        if not w:
            raise ValueError("delete_inst(): Databricks workspace not found.")
        # Delete the managed volume.
        w.volumes.delete(name=f"{cat_name}.{db_inst_name}_bronze.bronze_volume")
        w.volumes.delete(name=f"{cat_name}.{db_inst_name}_silver.silver_volume")
        w.volumes.delete(name=f"{cat_name}.{db_inst_name}_gold.gold_volume")

        # TODO implement model deletion

        # Delete tables and schemas for each medallion level.
        for medallion in MEDALLION_LEVELS:
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

    def fetch_table_data(
        self,
        catalog_name: str,
        inst_name: str,
        table_name: str,
        warehouse_id: str,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """
        Executes a SELECT * query on the specified table within the given catalog and schema,
        using the provided SQL warehouse. Returns the result as a list of dictionaries.
        """
        try:
            # Initialize the WorkspaceClient with default authentication
            client = WorkspaceClient(
                host=databricks_vars["DATABRICKS_HOST_URL"],
                google_service_account=gcs_vars["GCP_SERVICE_ACCOUNT_EMAIL"],
            )
        except Exception as e:
            raise ValueError(f"Failed to initialize WorkspaceClient: {e}")

        # Construct the fully qualified table name
        schema_name = databricksify_inst_name(inst_name)
        fully_qualified_table = (
            f"`{catalog_name}`.`{schema_name}_silver`.`{table_name}`"
        )
        sql_query = f"SELECT * FROM {fully_qualified_table} LIMIT {limit}"

        try:
            # Execute the SQL statement
            response = client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=sql_query,
                disposition=Disposition.INLINE,  # Use Enum member
                format=Format.JSON_ARRAY,  # Use Enum member
                wait_timeout="30s",  # Wait up to 30 seconds for execution
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,  # Use Enum member
            )
        except DatabricksError as e:
            raise ValueError(f"Databricks API call failed: {e}")

        # Check if the query execution was successful
        if response.status.state != StatementState.SUCCEEDED:
            error_message = (
                response.status.error.message
                if response.status.error
                else "No additional error info."
            )
            raise ValueError(
                f"Query did not succeed (state={response.status.state}): {error_message}"
            )

        # Validate the presence of the result and schema
        if not response.manifest or not response.manifest.schema:
            raise ValueError("Query succeeded but schema manifest is missing.")
        if not response.result or not response.result.data_array:
            raise ValueError("Query succeeded but result data is missing.")

        # Extract column names and data rows
        column_names = [column.name for column in response.manifest.schema]
        data_rows = response.result.data_array

        # Combine column names with corresponding row values
        return [dict(zip(column_names, row)) for row in data_rows]
