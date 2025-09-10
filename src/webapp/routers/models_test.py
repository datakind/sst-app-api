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
)
from ..gcsutil import StorageControl
from ..databricks import DatabricksControl, DatabricksInferenceRunResponse

MOCK_STORAGE = mock.Mock()
MOCK_DATABRICKS = mock.Mock()

UUID_2 = uuid.UUID("9bcbc782-2e71-4441-afa2-7a311024a5ec")
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
    ):
        return False
    return True


@pytest.fixture(name="session")
def session_fixture():
    """Unit test database setup."""
    engine = sqlalchemy.create_engine(
        "sqlite://",
        echo=True,
        echo_pool="debug",
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
        framework="sklearn",
    )
    run_1 = JobTable(
        id=RUN_ID,
        model=model_1,
        triggered_at=DATETIME_TESTING,
        batch_name="batch_foo",
        completed=True,
        output_filename="file_output_one",
        created_by=created_by_UUID,
        framework="sklearn",
    )
    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=USER_VALID_INST_UUID,
                        name="school_1",
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
            name= "sample_model_for_school_1",
            inst_id= "1d7c75c33eda42949c6675ea8af97b55",
            deleted= None,
            valid= True,
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
    )
    assert same_model_orderless(response_model, expected_model)


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
        output_filename="file_output_one",
        output_valid=False,
        run_id=123,
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
    assert same_run_info_orderless(
        response.json(),
        RunInfo(
            batch_name="batch_foo",
            completed=True,
            created_by="0ad8b77c49fb459a84b18d2c05722c4a",
            err_msg=None,
            inst_id="1d7c75c33eda42949c6675ea8af97b55",
            m_name="sample_model_for_school_1",
            output_filename="file_output_one",
            output_valid=False,
            run_id=123,
        ),
    )


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
            "framework": "h2o",
        },
    )

    assert response.status_code == 200


def test_trigger_inference_run(client: TestClient) -> None:
    """Depending on timeline, fellows may not get to this."""
    MOCK_DATABRICKS.run_pdp_inference.return_value = DatabricksInferenceRunResponse(
        job_run_id=123
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
    assert not check_file_types_valid_schema_configs(file_types1, [custom])
    assert not check_file_types_valid_schema_configs(file_types1, schema_configs1)
    assert check_file_types_valid_schema_configs(file_types2, [sst_configs])
    assert check_file_types_valid_schema_configs(file_types2, [pdp_configs])
    assert not check_file_types_valid_schema_configs(file_types2, [custom])
    assert check_file_types_valid_schema_configs(file_types3, [sst_configs])
    assert check_file_types_valid_schema_configs(file_types3, [pdp_configs])
    assert not check_file_types_valid_schema_configs(file_types3, [custom])
    assert not check_file_types_valid_schema_configs(file_types4, [sst_configs])
    assert not check_file_types_valid_schema_configs(file_types4, [pdp_configs])
    assert check_file_types_valid_schema_configs(file_types4, [custom])
