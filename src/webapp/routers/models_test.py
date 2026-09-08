"""Test file for the models.py file and constituent API functions."""

import uuid
from unittest import mock
from typing import Any
import pytest
import jsonpickle
from fastapi.testclient import TestClient
import sqlalchemy
from sqlalchemy.pool import StaticPool
from ..test_helper import (
    USR,
    DATAKINDER,
    USER_VALID_INST_UUID,
    USER_UUID,
    UUID_INVALID,
    DATETIME_TESTING,
    SAMPLE_UUID,
)
from ..main import app
from ..database import (
    FileTable,
    BatchTable,
    InstTable,
    Base,
    get_session,
    ModelTable,
    JobTable,
)
from ..utilities import uuid_to_str, get_current_active_user, SchemaType
from .models import (
    router,
    ModelInfo,
    RunInfo,
    check_file_types_valid_schema_configs,
    SchemaConfigObj,
    default_schema_configs_from_inst_schemas,
    resolve_model_schema_configs,
)
from ..utilities import batch_input_validated_blob_paths
from ..gcsutil import StorageControl
from ..databricks import DatabricksControl, DatabricksInferenceRunResponse

MOCK_STORAGE = mock.Mock()
MOCK_DATABRICKS = mock.Mock()

UUID_2 = uuid.UUID("9bcbc782-2e71-4441-afa2-7a311024a5ec")
EDVISE_INST_UUID = uuid.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")
FILE_UUID_1 = uuid.UUID("f0bb3a20-6d92-4254-afed-6a72f43c562a")
FILE_UUID_2 = uuid.UUID("cb02d06c-2a59-486a-9bdd-d394a4fcb833")
FILE_UUID_3 = uuid.UUID("fbe67a2e-50e0-40c7-b7b8-07043cb813a5")
BATCH_UUID = uuid.UUID("5b2420f3-1035-46ab-90eb-74d5df97de43")
created_by_UUID = uuid.UUID("0ad8b77c-49fb-459a-84b1-8d2c05722c4a")
RUN_ID = 123


# TODO plumb through schema configs
def same_model_orderless(a_elem: ModelInfo, b_elem: ModelInfo) -> bool:
    """Check ModelInfo equality without order."""
    if (
        a_elem.inst_id != b_elem.inst_id
        or a_elem.name != b_elem.name
        or a_elem.m_id != b_elem.m_id
        or a_elem.valid != b_elem.valid
        or a_elem.deleted != b_elem.deleted
        or a_elem.archived != b_elem.archived
    ):
        return False
    return True


def same_run_info_orderless(a_elem: RunInfo, b_elem: RunInfo) -> bool:
    """Check RunInfo equality without order."""
    if (
        a_elem.inst_id != b_elem.inst_id
        or a_elem.m_name != b_elem.m_name
        or a_elem.run_id != b_elem.run_id
        or a_elem.created_by != b_elem.created_by
        or a_elem.triggered_at != b_elem.triggered_at
        or a_elem.output_filename != b_elem.output_filename
        or a_elem.output_valid != b_elem.output_valid
        or a_elem.err_msg != b_elem.err_msg
        or a_elem.batch_name != b_elem.batch_name
        or a_elem.completed != b_elem.completed
        or a_elem.model_run_id != b_elem.model_run_id
        or a_elem.model_version != b_elem.model_version
    ):
        return False
    return True


@pytest.fixture(name="session")
def session_fixture():
    """Unit test database setup."""
    engine = sqlalchemy.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    batch_0 = BatchTable(
        id=UUID_INVALID,
        inst_id=USER_VALID_INST_UUID,
        name="batch_none",
        created_by=created_by_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    batch_1 = BatchTable(
        id=BATCH_UUID,
        inst_id=USER_VALID_INST_UUID,
        name="batch_foo",
        created_by=created_by_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    file_1 = FileTable(
        id=FILE_UUID_1,
        inst_id=USER_VALID_INST_UUID,
        name="file_input_one",
        source="MANUAL_UPLOAD",
        batches={batch_1},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.COURSE],
    )
    file_3 = FileTable(
        id=FILE_UUID_3,
        inst_id=USER_VALID_INST_UUID,
        name="file_output_one",
        batches={batch_1},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=True,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    model_1 = ModelTable(
        id=SAMPLE_UUID,
        inst_id=USER_VALID_INST_UUID,
        name="sample_model_for_school_1",
        schema_configs=jsonpickle.encode(
            [
                [
                    SchemaConfigObj(
                        schema_type=SchemaType.COURSE,
                        optional=False,
                        multiple_allowed=False,
                    ),
                    SchemaConfigObj(
                        schema_type=SchemaType.STUDENT,
                        optional=False,
                        multiple_allowed=False,
                    ),
                ]
            ]
        ),
        valid=True,
    )
    run_1 = JobTable(
        id=RUN_ID,
        model=model_1,
        triggered_at=DATETIME_TESTING,
        batch_name="batch_foo",
        completed=True,
        output_filename="file_output_one",
        created_by=created_by_UUID,
        model_run_id="T2UFD",
    )
    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=USER_VALID_INST_UUID,
                        name="school_1",
                        pdp_id="12345",
                        edvise_id=None,
                        schemas=[SchemaType.STUDENT, SchemaType.COURSE],
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    batch_0,
                    batch_1,
                    file_1,
                    FileTable(
                        id=FILE_UUID_2,
                        inst_id=USER_VALID_INST_UUID,
                        name="file_input_two",
                        source="PDP_SFTP",
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                        sst_generated=False,
                        valid=False,
                        schemas=[SchemaType.COURSE],
                    ),
                    file_3,
                    model_1,
                    run_1,
                ]
            )
            session.commit()
            yield session
    finally:
        Base.metadata.drop_all(engine)


@pytest.fixture(name="client")
def client_fixture(session: sqlalchemy.orm.Session) -> Any:
    """Unit test mocks setup."""

    def get_session_override():
        return session

    def get_current_active_user_override():
        return USR

    def storage_control_override():
        return MOCK_STORAGE

    def databricks_control_override():
        return MOCK_DATABRICKS

    app.include_router(router)
    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_current_active_user] = get_current_active_user_override
    app.dependency_overrides[StorageControl] = storage_control_override
    app.dependency_overrides[DatabricksControl] = databricks_control_override

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_read_inst_models(client: TestClient) -> None:
    """Test GET /institutions/345/models."""
    response = client.get(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/models"
    )
    assert response.status_code == 200
    assert same_model_orderless(
        ModelInfo(**response.json()[0]),
        ModelInfo(
            m_id="e4862c62829440d8ab4c9c298f02f619",
            name="sample_model_for_school_1",
            inst_id="1d7c75c33eda42949c6675ea8af97b55",
            deleted=None,
            valid=True,
            archived=False,
        ),
    )


def test_read_inst_model(client: TestClient) -> None:
    """Test GET /institutions/345/models/10. For various user access types."""
    # Unauthorized cases.
    response_unauth = client.get(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/models/sample_model_for_school_1"
    )
    assert str(response_unauth) == "<Response [401 Unauthorized]>"
    assert (
        response_unauth.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )

    # Authorized.
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/sample_model_for_school_1"
    )
    assert response.status_code == 200
    response_model = ModelInfo(**response.json())
    expected_model = ModelInfo(
        deleted=None,
        inst_id="1d7c75c33eda42949c6675ea8af97b55",
        m_id="e4862c62829440d8ab4c9c298f02f619",
        name="sample_model_for_school_1",
        valid=True,
        archived=False,
    )
    assert same_model_orderless(response_model, expected_model)


def test_archive_model(client: TestClient, session: sqlalchemy.orm.Session) -> None:
    """Test PATCH /institutions/{inst_id}/models/{model_name}/archive."""
    base = "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/models/"
    assert client.patch(base + "missing_model/archive").status_code == 404

    response = client.patch(base + "sample_model_for_school_1/archive")
    assert response.status_code == 200
    assert response.json() == {
        "inst_id": uuid_to_str(USER_VALID_INST_UUID),
        "model_name": "sample_model_for_school_1",
        "archived": 1,
        "status": "Model archived",
    }
    model_row = session.get(ModelTable, SAMPLE_UUID)
    assert model_row is not None
    assert model_row.archived == 1
    assert client.patch(base + "sample_model_for_school_1/archive").status_code == 409

    # Confirm the archived state is reflected on the model read endpoints.
    get_response = client.get(base + "sample_model_for_school_1")
    assert get_response.status_code == 200
    assert get_response.json()["archived"] is True

    list_response = client.get(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/models"
    )
    assert list_response.status_code == 200
    assert next(
        m["archived"]
        for m in list_response.json()
        if m["name"] == "sample_model_for_school_1"
    )


def test_read_inst_model_outputs(client: TestClient) -> None:
    """Test GET /institutions/345/models/10/output."""
    MOCK_STORAGE.list_blobs_in_folder.return_value = []
    # Authorized.
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/sample_model_for_school_1/runs"
    )
    assert response.status_code == 200
    response_model = RunInfo(**response.json()[0])
    expected_model = RunInfo(
        batch_name="batch_foo",
        created_by="0ad8b77c49fb459a84b18d2c05722c4a",
        err_msg=None,
        inst_id="1d7c75c33eda42949c6675ea8af97b55",
        m_name="sample_model_for_school_1",
        model_run_id="T2UFD",
        model_version=None,
        output_filename="file_output_one",
        output_valid=False,
        run_id=123,
        triggered_at=response_model.triggered_at,  # copy from response
        completed=response_model.completed,
    )
    assert same_run_info_orderless(response_model, expected_model)


def test_read_inst_model_output(client: TestClient) -> None:
    """Test GET /institutions/345/models/10/output/1."""
    # Authorized.
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/sample_model_for_school_1/run/"
        + str(RUN_ID)
    )
    assert response.status_code == 200
    response_model = RunInfo(**response.json())
    expected_model = RunInfo(
        batch_name="batch_foo",
        completed=True,
        created_by="0ad8b77c49fb459a84b18d2c05722c4a",
        err_msg=None,
        inst_id="1d7c75c33eda42949c6675ea8af97b55",
        m_name="sample_model_for_school_1",
        model_run_id="T2UFD",
        model_version=None,
        output_filename="file_output_one",
        output_valid=False,
        run_id=123,
        triggered_at=response_model.triggered_at,  # copy from response
    )
    assert same_run_info_orderless(response_model, expected_model)


def test_delete_model_run(client: TestClient, session: sqlalchemy.orm.Session) -> None:
    """Test DELETE /institutions/{inst_id}/models/{model_name}/run/{job_run_id}."""
    MOCK_STORAGE.list_blobs_in_folder.return_value = []
    url = (
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/sample_model_for_school_1/run/"
        + str(RUN_ID)
    )

    response = client.delete(url)
    assert response.status_code == 200
    assert response.json() == {
        "inst_id": uuid_to_str(USER_VALID_INST_UUID),
        "model_name": "sample_model_for_school_1",
        "run_id": RUN_ID,
        "status": "Run deleted",
    }
    assert session.get(JobTable, RUN_ID) is None

    MOCK_STORAGE.list_blobs_in_folder.return_value = []
    get_response = client.get(url)
    assert get_response.status_code == 404
    assert get_response.json() == {"detail": "Run not found."}


def test_delete_model_run_not_found(client: TestClient) -> None:
    """Deleting a missing run returns 404."""
    MOCK_STORAGE.list_blobs_in_folder.return_value = []
    response = client.delete(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/sample_model_for_school_1/run/999"
    )
    assert response.status_code == 404
    assert response.json() == {"detail": "Run not found."}


def test_create_model(client: TestClient) -> None:
    """Depending on timeline, fellows may not get to this."""
    schema_config_1 = {
        "schema_type": SchemaType.COURSE,
        "count": 1,
    }
    schema_config_2 = {
        "schema_type": SchemaType.STUDENT,
        "count": 1,
    }
    response = client.post(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/models/",
        json={
            "name": "my_model",
            "schema_configs": [[schema_config_1, schema_config_2]],
        },
    )

    assert response.status_code == 200


def test_trigger_inference_run(client: TestClient) -> None:
    """Depending on timeline, fellows may not get to this."""
    MOCK_DATABRICKS.run_pdp_inference.return_value = DatabricksInferenceRunResponse(
        job_run_id=123
    )
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(
        version=1, run_id="run-inference"
    )
    response = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/sample_model_for_school_1/run-inference",
        json={
            "batch_name": "batch_none",
            "is_pdp": True,
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"].startswith(
        "The files in this batch don't conform to the schema configs allowed by this model."
    )

    response = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/sample_model_for_school_1/run-inference",
        json={
            "batch_name": "batch_foo",
            "is_pdp": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["inst_id"] == uuid_to_str(USER_VALID_INST_UUID)
    assert response.json()["m_name"] == "sample_model_for_school_1"
    assert response.json()["run_id"] == 123
    assert response.json()["created_by"] == uuid_to_str(USER_UUID)
    assert response.json()["triggered_at"] is not None
    assert response.json()["batch_name"] == "batch_foo"
    assert response.json()["model_run_id"] == "run-inference"
    assert response.json()["model_version"] == "1"


def test_check_file_types_valid_schema_configs():
    """Test batch schema validation logic."""
    file_types1 = [
        [SchemaType.COURSE],
        [SchemaType.STUDENT],
        [SchemaType.UNKNOWN],
    ]
    file_types2 = [
        [SchemaType.STUDENT],
        [SchemaType.COURSE],
    ]
    file_types3 = [
        [SchemaType.STUDENT, SchemaType.UNKNOWN],
        [SchemaType.COURSE],
    ]
    file_types4 = [
        [SchemaType.STUDENT, SchemaType.UNKNOWN],
        [SchemaType.UNKNOWN],
    ]
    pdp_configs = [
        SchemaConfigObj(
            schema_type=SchemaType.COURSE,
            optional=False,
            multiple_allowed=False,
        ),
        SchemaConfigObj(
            schema_type=SchemaType.STUDENT,
            optional=False,
            multiple_allowed=False,
        ),
    ]
    sst_configs = [
        SchemaConfigObj(
            schema_type=SchemaType.STUDENT,
            optional=False,
            multiple_allowed=False,
        ),
        SchemaConfigObj(
            schema_type=SchemaType.COURSE,
            optional=False,
            multiple_allowed=False,
        ),
    ]
    custom = [
        SchemaConfigObj(
            schema_type=SchemaType.UNKNOWN,
            optional=False,
            multiple_allowed=True,
        ),
    ]
    schema_configs1 = [
        pdp_configs,
        sst_configs,
        custom,
    ]
    assert not check_file_types_valid_schema_configs(file_types1, [pdp_configs])
    assert not check_file_types_valid_schema_configs(file_types1, [sst_configs])
    assert check_file_types_valid_schema_configs(file_types1, [custom])
    assert check_file_types_valid_schema_configs(file_types1, schema_configs1)
    assert check_file_types_valid_schema_configs(file_types2, [sst_configs])
    assert check_file_types_valid_schema_configs(file_types2, [pdp_configs])
    assert check_file_types_valid_schema_configs(file_types2, [custom])
    assert check_file_types_valid_schema_configs(file_types3, [sst_configs])
    assert check_file_types_valid_schema_configs(file_types3, [pdp_configs])
    assert check_file_types_valid_schema_configs(file_types3, [custom])
    assert not check_file_types_valid_schema_configs(file_types4, [sst_configs])
    assert not check_file_types_valid_schema_configs(file_types4, [pdp_configs])
    assert check_file_types_valid_schema_configs(file_types4, [custom])
    assert not check_file_types_valid_schema_configs([], [custom])


def test_default_schema_configs_from_inst_schemas():
    """Standard PDP institutions derive required COURSE + STUDENT batch rules."""
    derived = default_schema_configs_from_inst_schemas(["STUDENT", "COURSE"])
    assert len(derived) == 1
    assert [c.schema_type for c in derived[0]] == [
        SchemaType.COURSE,
        SchemaType.STUDENT,
    ]
    assert all(not c.optional and not c.multiple_allowed for c in derived[0])

    assert default_schema_configs_from_inst_schemas([]) == []
    assert default_schema_configs_from_inst_schemas(None) == []
    assert default_schema_configs_from_inst_schemas(["SST_OUTPUT", "PNG"]) == []

    unknown_only = default_schema_configs_from_inst_schemas(["UNKNOWN"])
    assert len(unknown_only) == 1
    assert len(unknown_only[0]) == 1
    assert unknown_only[0][0].schema_type == SchemaType.UNKNOWN
    assert not unknown_only[0][0].optional
    assert unknown_only[0][0].multiple_allowed


def test_resolve_model_schema_configs_falls_back_to_institution():
    """Null model schema_configs uses institution allowed input schemas."""
    resolved = resolve_model_schema_configs(None, ["STUDENT", "COURSE"])
    assert [c.schema_type for c in resolved[0]] == [
        SchemaType.COURSE,
        SchemaType.STUDENT,
    ]


def test_resolve_model_schema_configs_unknown_only_institution():
    """Legacy/GenAI institutions with UNKNOWN-only schemas derive flexible batch rules."""
    resolved = resolve_model_schema_configs(None, ["UNKNOWN"])
    assert len(resolved) == 1
    assert resolved[0][0].schema_type == SchemaType.UNKNOWN
    assert resolved[0][0].multiple_allowed


def test_trigger_inference_run_derives_schema_configs_when_null(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """PDP inference succeeds when model.schema_configs is null but inst.schemas is set."""
    null_config_model = ModelTable(
        id=UUID_2,
        inst_id=USER_VALID_INST_UUID,
        name="pdp_model_without_schema_configs",
        schema_configs=None,
        valid=True,
    )
    session.add(null_config_model)
    session.commit()

    MOCK_DATABRICKS.run_pdp_inference.return_value = DatabricksInferenceRunResponse(
        job_run_id=456
    )
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(
        version=1, run_id="run-abc"
    )

    response = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/models/pdp_model_without_schema_configs/run-inference",
        json={"batch_name": "batch_foo", "is_pdp": True},
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == 456
    assert response.json()["m_name"] == "pdp_model_without_schema_configs"


def test_batch_input_validated_blob_paths_skips_sst_generated() -> None:
    """Only non-SST-generated files become validated/ blob paths."""
    batch = BatchTable(
        id=BATCH_UUID,
        inst_id=USER_VALID_INST_UUID,
        name="batch_foo",
        created_by=created_by_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    input_file = FileTable(
        id=FILE_UUID_1,
        inst_id=USER_VALID_INST_UUID,
        name="file_input_one",
        source="MANUAL_UPLOAD",
        batches={batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.COURSE],
    )
    output_file = FileTable(
        id=FILE_UUID_3,
        inst_id=USER_VALID_INST_UUID,
        name="file_output_one",
        batches={batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=True,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    assert batch_input_validated_blob_paths({input_file, output_file}) == [
        "validated/file_input_one"
    ]


def test_trigger_es_inference_run_edvise_institution(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """Edvise Schema institutions trigger run_es_inference instead of run_pdp_inference."""
    app.dependency_overrides[get_current_active_user] = lambda: DATAKINDER
    MOCK_DATABRICKS.reset_mock()
    edvise_inst = InstTable(
        id=EDVISE_INST_UUID,
        name="edvise_school",
        edvise_id="edvise_test_1",
        schemas=[SchemaType.STUDENT, SchemaType.COURSE],
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    edvise_model = ModelTable(
        id=uuid.uuid4(),
        inst_id=EDVISE_INST_UUID,
        name="es_model",
        schema_configs=jsonpickle.encode(
            [
                [
                    SchemaConfigObj(
                        schema_type=SchemaType.COURSE,
                        optional=False,
                        multiple_allowed=False,
                    ),
                    SchemaConfigObj(
                        schema_type=SchemaType.STUDENT,
                        optional=False,
                        multiple_allowed=False,
                    ),
                ]
            ]
        ),
        valid=True,
    )
    edvise_batch = BatchTable(
        id=uuid.uuid4(),
        inst_id=EDVISE_INST_UUID,
        name="es_batch_foo",
        created_by=created_by_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    edvise_course_file = FileTable(
        id=uuid.uuid4(),
        inst_id=EDVISE_INST_UUID,
        name="es_course.csv",
        source="MANUAL_UPLOAD",
        batches={edvise_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.COURSE],
    )
    edvise_student_file = FileTable(
        id=uuid.uuid4(),
        inst_id=EDVISE_INST_UUID,
        name="es_student.csv",
        source="MANUAL_UPLOAD",
        batches={edvise_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    session.add_all(
        [
            edvise_inst,
            edvise_model,
            edvise_batch,
            edvise_course_file,
            edvise_student_file,
        ]
    )
    session.commit()

    MOCK_DATABRICKS.run_es_inference.return_value = DatabricksInferenceRunResponse(
        job_run_id=789
    )
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(
        version=2, run_id="run-es"
    )

    response = client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/models/es_model/run-inference",
        json={"batch_name": "es_batch_foo", "config_file_name": "config.toml"},
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == 789
    assert response.json()["m_name"] == "es_model"
    assert response.json()["batch_name"] == "es_batch_foo"
    MOCK_DATABRICKS.run_es_inference.assert_called_once()
    db_req = MOCK_DATABRICKS.run_es_inference.call_args[0][0]
    assert db_req.batch_id == uuid_to_str(edvise_batch.id)
    assert db_req.validated_blob_paths == [
        "validated/es_course.csv",
        "validated/es_student.csv",
    ]
    assert db_req.is_genai_institution is False
    MOCK_DATABRICKS.run_pdp_inference.assert_not_called()


def test_trigger_es_inference_run_genai_institution(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """GenAI institutions pass is_genai_institution=True to run_es_inference."""
    app.dependency_overrides[get_current_active_user] = lambda: DATAKINDER
    MOCK_DATABRICKS.reset_mock()
    genai_inst = InstTable(
        id=uuid.uuid4(),
        name="genai_school",
        genai_id="genai_test_1",
        schemas=[SchemaType.STUDENT, SchemaType.COURSE],
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    genai_model = ModelTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="genai_es_model",
        schema_configs=None,
        valid=True,
    )
    genai_batch = BatchTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="genai_batch_foo",
        created_by=created_by_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    genai_course_file = FileTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="genai_course.csv",
        source="MANUAL_UPLOAD",
        batches={genai_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.COURSE],
    )
    genai_student_file = FileTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="genai_student.csv",
        source="MANUAL_UPLOAD",
        batches={genai_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    session.add_all(
        [
            genai_inst,
            genai_model,
            genai_batch,
            genai_course_file,
            genai_student_file,
        ]
    )
    session.commit()

    MOCK_DATABRICKS.run_es_inference.return_value = DatabricksInferenceRunResponse(
        job_run_id=790
    )
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(
        version=1, run_id="run-genai"
    )

    response = client.post(
        "/institutions/"
        + uuid_to_str(genai_inst.id)
        + "/models/genai_es_model/run-inference",
        json={"batch_name": "genai_batch_foo", "config_file_name": "config.toml"},
    )

    assert response.status_code == 200
    MOCK_DATABRICKS.run_es_inference.assert_called_once()
    db_req = MOCK_DATABRICKS.run_es_inference.call_args[0][0]
    assert db_req.is_genai_institution is True
    assert db_req.batch_id == uuid_to_str(genai_batch.id)
    MOCK_DATABRICKS.run_pdp_inference.assert_not_called()


def test_trigger_es_inference_run_genai_unknown_only_schemas(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """GenAI institutions with UNKNOWN-only schemas can run inference without model schema_configs."""
    app.dependency_overrides[get_current_active_user] = lambda: DATAKINDER
    MOCK_DATABRICKS.reset_mock()
    genai_inst = InstTable(
        id=uuid.uuid4(),
        name="genai_school_unknown",
        genai_id="genai_unknown_test_1",
        schemas=[SchemaType.UNKNOWN],
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    genai_model = ModelTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="genai_model_no_schema_configs",
        schema_configs=None,
        valid=True,
    )
    genai_batch = BatchTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="genai_batch_unknown",
        created_by=created_by_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    genai_file_one = FileTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="student_file.csv",
        source="MANUAL_UPLOAD",
        batches={genai_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    genai_file_two = FileTable(
        id=uuid.uuid4(),
        inst_id=genai_inst.id,
        name="course_file.csv",
        source="MANUAL_UPLOAD",
        batches={genai_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.COURSE],
    )
    session.add_all(
        [
            genai_inst,
            genai_model,
            genai_batch,
            genai_file_one,
            genai_file_two,
        ]
    )
    session.commit()

    MOCK_DATABRICKS.run_es_inference.return_value = DatabricksInferenceRunResponse(
        job_run_id=791
    )
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(
        version=1, run_id="run-genai-unknown"
    )

    response = client.post(
        "/institutions/"
        + uuid_to_str(genai_inst.id)
        + "/models/genai_model_no_schema_configs/run-inference",
        json={"batch_name": "genai_batch_unknown", "config_file_name": "config.toml"},
    )

    assert response.status_code == 200
    assert response.json()["run_id"] == 791
    MOCK_DATABRICKS.run_es_inference.assert_called_once()
    db_req = MOCK_DATABRICKS.run_es_inference.call_args[0][0]
    assert db_req.is_genai_institution is True
    assert db_req.batch_id == uuid_to_str(genai_batch.id)
    MOCK_DATABRICKS.run_pdp_inference.assert_not_called()


def test_uc_decimal_model_name_is_displayed_as_dot(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """UC stores 4d5; list/detail responses should show 4.5 and accept either spelling."""
    uc_name = "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    display_name = "graduation_in_3y_ft_4.5y_pt_checkpoint_30_credits"
    session.add(
        ModelTable(
            id=uuid.uuid4(),
            inst_id=USER_VALID_INST_UUID,
            name=uc_name,
            valid=True,
        )
    )
    session.commit()

    inst = uuid_to_str(USER_VALID_INST_UUID)
    names = [m["name"] for m in client.get(f"/institutions/{inst}/models").json()]
    assert display_name in names
    assert uc_name not in names

    by_display = client.get(f"/institutions/{inst}/models/{display_name}")
    assert by_display.status_code == 200
    assert by_display.json()["name"] == display_name

    by_uc = client.get(f"/institutions/{inst}/models/{uc_name}")
    assert by_uc.status_code == 200
    assert by_uc.json()["name"] == display_name


def test_create_model_encodes_decimal_dots_for_storage(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """New models with 4.5 are stored as 4d5 but returned as 4.5."""
    display_name = "graduation_in_3y_ft_4.5y_pt_checkpoint_30_credits"
    uc_name = "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    response = client.post(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/models/",
        json={"name": display_name},
    )
    assert response.status_code == 200
    assert response.json()["name"] == display_name
    stored = session.execute(
        sqlalchemy.select(ModelTable).where(ModelTable.name == uc_name)
    ).scalar_one()
    assert stored.name == uc_name
