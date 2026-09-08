"""Test file for the data.py file and constituent API functions."""

import io
import logging
import uuid
from types import SimpleNamespace
from unittest import mock
from collections import Counter
from fastapi.testclient import TestClient
from typing import Any
import pytest
import pandas as pd
import sqlalchemy
from sqlalchemy.pool import StaticPool
from sqlalchemy.future import select
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
    ModelTable,
    SchemaRegistryTable,
    DocType,
    Base,
    get_session,
)
from ..utilities import (
    uuid_to_str,
    get_current_active_user,
    SchemaType,
    get_external_bucket_name,
)
from . import data as data_router
from .data import (
    router,
    DataOverview,
    DataInfo,
    _infer_allowed_schemas_from_filename,
    resolve_eligible_inference_terms,
)
from fastapi import HTTPException
from ..gcsutil import StorageControl
from ..databricks import DatabricksControl

MOCK_STORAGE = mock.Mock()
MOCK_DATABRICKS = mock.Mock()
MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.return_value = mock.Mock(job_run_id=1)

UUID_2 = uuid.UUID("9bcbc782-2e71-4441-afa2-7a311024a5ec")
FILE_UUID_1 = uuid.UUID("f0bb3a20-6d92-4254-afed-6a72f43c562a")
FILE_UUID_2 = uuid.UUID("cb02d06c-2a59-486a-9bdd-d394a4fcb833")
FILE_UUID_3 = uuid.UUID("fbe67a2e-50e0-40c7-b7b8-07043cb813a5")
BATCH_UUID = uuid.UUID("5b2420f3-1035-46ab-90eb-74d5df97de43")
CREATOR_UUID = uuid.UUID("0ad8b77c-49fb-459a-84b1-8d2c05722c4a")


def test_resolve_eligible_inference_terms_uses_academic_terms_and_excludes_training_cohorts():
    students = pd.DataFrame(
        {
            "student_id": [1, 2, 3],
            "enrollment_type": [" first-time ", "FIRST-TIME", "TRANSFER"],
            "cohort_term": ["FALL", "SPRING", "FALL"],
            "cohort": ["2022-23", "2023-24", "2023-24"],
        }
    )
    courses = pd.DataFrame(
        {
            "student_id": ["1", "2", "2", "3"],
            "academic_term": ["FALL", "FALL", "SPRING", "SPRING"],
            "academic_year": ["2024-25", "2024-25", "2024-25", "2024-25"],
        }
    )
    config = {
        "student_criteria": {"enrollment_type": "FIRST-TIME"},
        "training_cohorts": ["fall 2022-23"],
    }

    result = resolve_eligible_inference_terms(
        students, courses, config, "inference batch"
    )

    assert result.status == "valid"
    assert result.batch_name == "inference batch"
    assert [term.model_dump() for term in result.terms] == [
        {"term_label": "spring 2024-25", "valid_student_count": 1},
        {"term_label": "fall 2024-25", "valid_student_count": 1},
    ]


def test_resolve_eligible_inference_terms_skips_missing_student_criteria_columns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    students = pd.DataFrame(
        {
            "student_id": [1, 2],
            "enrollment_type": ["FIRST-TIME", "TRANSFER"],
        }
    )
    courses = pd.DataFrame(
        {
            "student_id": [1, 2],
            "academic_term": ["FALL", "SPRING"],
            "academic_year": ["2024-25", "2024-25"],
        }
    )

    with caplog.at_level(logging.WARNING):
        result = resolve_eligible_inference_terms(
            students,
            courses,
            {
                "student_criteria": {
                    "enrollment_type": "FIRST-TIME",
                    "cummean_course_grade_numeric_mean": 3.0,
                }
            },
            "batch",
        )

    assert result.status == "valid"
    assert [term.model_dump() for term in result.terms] == [
        {"term_label": "fall 2024-25", "valid_student_count": 1}
    ]
    assert "Skipping student_criteria columns missing from the student file" in (
        caplog.text
    )
    assert "cummean_course_grade_numeric_mean" in caplog.text


def test_resolve_eligible_inference_terms_joins_numeric_identifier_forms():
    students = pd.DataFrame({"student_id": [1]})
    courses = pd.DataFrame(
        {
            "student_id": [1.0, float("nan")],
            "academic_term": ["FALL", "SPRING"],
            "academic_year": ["2024-25", "2024-25"],
        }
    )

    result = resolve_eligible_inference_terms(students, courses, {}, "batch")

    assert result.status == "valid"
    assert [term.model_dump() for term in result.terms] == [
        {"term_label": "fall 2024-25", "valid_student_count": 1}
    ]


def test_resolve_eligible_inference_terms_prefers_config_student_id_col():
    students = pd.DataFrame(
        {
            "student_id": ["mismatch-a", "mismatch-b"],
            "learner_id": ["1", "2"],
        }
    )
    courses = pd.DataFrame(
        {
            "student_id": ["other-a", "other-b"],
            "learner_id": ["1", "2"],
            "academic_term": ["FALL", "SPRING"],
            "academic_year": ["2024-25", "2024-25"],
        }
    )

    default_join = resolve_eligible_inference_terms(students, courses, {}, "batch")
    config_join = resolve_eligible_inference_terms(
        students, courses, {"student_id_col": "learner_id"}, "batch"
    )

    assert default_join.status == "invalid"
    assert config_join.status == "valid"
    assert [term.model_dump() for term in config_join.terms] == [
        {"term_label": "spring 2024-25", "valid_student_count": 1},
        {"term_label": "fall 2024-25", "valid_student_count": 1},
    ]


def test_resolve_eligible_inference_terms_returns_invalid_without_term_columns():
    result = resolve_eligible_inference_terms(
        pd.DataFrame({"student_id": [1]}),
        pd.DataFrame({"student_id": [1]}),
        {},
        "batch",
    )

    assert result.status == "invalid"
    assert result.terms == []
    assert "academic term columns" in (result.reason or "")


def test_resolve_eligible_inference_terms_requires_entry_columns_for_exclusion():
    result = resolve_eligible_inference_terms(
        pd.DataFrame({"student_id": [1]}),
        pd.DataFrame(
            {
                "student_id": [1],
                "academic_term": ["FALL"],
                "academic_year": ["2024-25"],
            }
        ),
        {"training_cohorts": ["fall 2022-23"]},
        "batch",
    )

    assert result.status == "invalid"
    assert "entry cohort columns" in (result.reason or "")


def test_resolve_eligible_inference_terms_invalid_when_exclusion_empties_students():
    result = resolve_eligible_inference_terms(
        pd.DataFrame(
            {
                "student_id": [1],
                "cohort_term": ["FALL"],
                "cohort": ["2022-23"],
            }
        ),
        pd.DataFrame(
            {
                "student_id": [1],
                "academic_term": ["FALL"],
                "academic_year": ["2024-25"],
            }
        ),
        {"training_cohorts": ["fall 2022-23"]},
        "batch",
    )

    assert result.status == "invalid"
    assert "empty DataFrame" in (result.reason or "")


def test_resolve_eligible_inference_terms_strips_cohort_values_before_exclusion():
    result = resolve_eligible_inference_terms(
        pd.DataFrame(
            {
                "student_id": [1, 2],
                "cohort_term": [" FALL ", "SPRING"],
                "cohort": [" 2022-23 ", "2023-24"],
            }
        ),
        pd.DataFrame(
            {
                "student_id": [1, 2],
                "academic_term": ["FALL", "SPRING"],
                "academic_year": ["2024-25", "2024-25"],
            }
        ),
        {"training_cohorts": ["fall 2022-23"]},
        "batch",
    )

    assert result.status == "valid"
    assert [term.model_dump() for term in result.terms] == [
        {"term_label": "spring 2024-25", "valid_student_count": 1}
    ]


@pytest.mark.parametrize(
    ("institution_ids", "expected"),
    [
        ({"pdp_id": "pdp"}, "pdp"),
        ({"edvise_id": "edvise"}, "edvise"),
        ({"genai_id": "genai"}, "edvise"),
        ({"legacy_id": "legacy"}, "legacy"),
        ({}, None),
        ({"pdp_id": "pdp", "legacy_id": "legacy"}, None),
    ],
)
def test_project_schema_type_for_institution(
    institution_ids: dict[str, str],
    expected: str | None,
) -> None:
    values: dict[str, str | None] = {
        "pdp_id": None,
        "edvise_id": None,
        "legacy_id": None,
        "genai_id": None,
    }
    values.update(institution_ids)
    institution = SimpleNamespace(**values)

    assert data_router._project_schema_type_for_institution(institution) == expected


def test_resolve_eligible_inference_terms_supports_entry_columns_and_multi_year_order():
    students = pd.DataFrame(
        {
            "study_id": ["1", "2"],
            "entry_year": ["2022-23", "2023-24"],
            "entry_term": ["FALL", "SPRING"],
        }
    )
    courses = pd.DataFrame(
        {
            "study_id": [1, 2, 2],
            "academic_term": ["FALL", "FALL", "SPRING"],
            "academic_year": ["2023-24", "2024-25", "2024-25"],
        }
    )

    result = resolve_eligible_inference_terms(
        students,
        courses,
        {"training_cohorts": ["fall 2022-23"]},
    )

    assert [term.term_label for term in result.terms] == [
        "spring 2024-25",
        "fall 2024-25",
    ]


def test_resolve_eligible_inference_terms_supports_edvise_learner_id():
    students = pd.DataFrame(
        {
            "learner_id": ["1", "2", "3"],
            "enrollment_type": ["FIRST-TIME", "FIRST-TIME", "TRANSFER"],
            "entry_year": ["2022-23", "2023-24", "2023-24"],
            "entry_term": ["FALL", "SPRING", "FALL"],
        }
    )
    courses = pd.DataFrame(
        {
            "learner_id": ["1", "2", "2", "3"],
            "academic_term": ["FALL", "FALL", "SPRING", "SPRING"],
            "academic_year": ["2024-25", "2024-25", "2024-25", "2024-25"],
        }
    )

    result = resolve_eligible_inference_terms(
        students,
        courses,
        {
            "student_criteria": {"enrollment_type": "FIRST-TIME"},
            "training_cohorts": ["fall 2022-23"],
        },
        "es batch",
    )

    assert result.status == "valid"
    assert [term.model_dump() for term in result.terms] == [
        {"term_label": "spring 2024-25", "valid_student_count": 1},
        {"term_label": "fall 2024-25", "valid_student_count": 1},
    ]


def counter_repr(x):
    """Orderless comparison of two iterables."""
    return {frozenset(Counter(item).items()) for item in x}


def same_file_orderless(a_elem: DataInfo, b_elem: DataInfo):  # type: ignore
    """Compares two DataInfo objects."""
    if (
        a_elem["inst_id"] != b_elem["inst_id"]  # type: ignore
        or counter_repr(a_elem["batch_ids"]) != counter_repr(b_elem["batch_ids"])  # type: ignore
        or a_elem["name"] != b_elem["name"]  # type: ignore
        or a_elem["uploader"] != b_elem["uploader"]  # type: ignore
        or a_elem["deleted"] != b_elem["deleted"]  # type: ignore
        or a_elem["source"] != b_elem["source"]  # type: ignore
        or a_elem["deletion_request_time"] != b_elem["deletion_request_time"]  # type: ignore
        or a_elem["retention_days"] != b_elem["retention_days"]  # type: ignore
        or a_elem["sst_generated"] != b_elem["sst_generated"]  # type: ignore
        or a_elem["valid"] != b_elem["valid"]  # type: ignore
        or a_elem["uploaded_date"] != b_elem["uploaded_date"]  # type: ignore
    ):
        return False
    return True


def same_orderless(a: DataOverview, b: DataOverview) -> bool:
    """Compares two DataOverview objects."""
    for a_elem in a["batches"]:  # type: ignore
        found = False
        for b_elem in b["batches"]:  # type: ignore
            if a_elem["batch_id"] != b_elem["batch_id"]:
                continue
            found = True
            if (
                a_elem["inst_id"] != b_elem["inst_id"]
                or a_elem["file_names_to_ids"] == b_elem["file_names_to_ids"]
                or a_elem["name"] != b_elem["name"]
                or a_elem["created_by"] != b_elem["created_by"]
                or a_elem["deleted"] != b_elem["deleted"]
                or a_elem["completed"] != b_elem["completed"]
                or a_elem["deletion_request_time"] != b_elem["deletion_request_time"]
                or a_elem["created_at"] != b_elem["created_at"]
            ):
                return False
        if not found:
            return False
    for a_elem in a["files"]:  # type: ignore
        found = False
        for b_elem in b["files"]:  # type: ignore
            if a_elem["data_id"] != b_elem["data_id"]:
                continue
            found = True
            if not same_file_orderless(a_elem, b_elem):
                return False
        if not found:
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
    batch_1 = BatchTable(
        id=BATCH_UUID,
        inst_id=USER_VALID_INST_UUID,
        name="batch_foo",
        created_by=CREATOR_UUID,
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
        schemas=[SchemaType.UNKNOWN],
    )
    file_3 = FileTable(
        id=FILE_UUID_3,
        inst_id=USER_VALID_INST_UUID,
        name="file_output_three",
        batches={batch_1},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=True,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    file_4 = FileTable(
        id=SAMPLE_UUID,
        inst_id=USER_VALID_INST_UUID,
        name="file_output_four",
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=True,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=USER_VALID_INST_UUID,
                        name="school_1",
                        legacy_id="legacy_test",
                        pdp_id=None,
                        edvise_id=None,
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    SchemaRegistryTable(
                        doc_type=DocType.base,  # ✅ fix this
                        is_pdp=False,
                        version_label="1.0.0",
                        json_doc={"version": "1.0.0", "base": {"data_models": {}}},
                        is_active=True,
                        created_at=DATETIME_TESTING,
                    ),
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
                    file_4,
                ]
            )
            session.commit()
            yield session
    finally:
        Base.metadata.drop_all(engine)


@pytest.fixture(name="client")
def client_fixture(session: sqlalchemy.orm.Session, monkeypatch: Any) -> Any:
    """Unit test mocks setup."""
    monkeypatch.setenv("SST_SKIP_EXT_GEN", "1")
    MOCK_STORAGE.reset_mock()
    MOCK_DATABRICKS.reset_mock()

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
    MOCK_STORAGE.reset_mock()
    MOCK_DATABRICKS.reset_mock()


def _add_registered_model(
    session: sqlalchemy.orm.Session,
    name: str = "retention_model",
) -> None:
    session.add(
        ModelTable(
            inst_id=USER_VALID_INST_UUID,
            name=name,
            created_by=CREATOR_UUID,
            valid=True,
            deleted=False,
            archived=0,
            created_at=DATETIME_TESTING,
            updated_at=DATETIME_TESTING,
        )
    )
    session.commit()


def _prepare_inference_batch(session: sqlalchemy.orm.Session) -> None:
    batch = session.get(BatchTable, BATCH_UUID)
    student_file = session.get(FileTable, FILE_UUID_1)
    course_file = session.get(FileTable, FILE_UUID_2)
    assert batch is not None and student_file is not None and course_file is not None
    batch.completed = True
    student_file.schemas = [SchemaType.STUDENT]
    course_file.valid = True
    batch.files.add(course_file)
    session.commit()


def test_get_eligible_inference_terms_for_selected_batch(
    client: TestClient,
    session: sqlalchemy.orm.Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(data_router.env_vars, "ENV", "DEV")
    _prepare_inference_batch(session)
    _add_registered_model(session)

    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(run_id="run-123")
    MOCK_DATABRICKS.read_volume_training_config.return_value = {
        "student_criteria": {"enrollment_type": "FIRST-TIME"},
        "training_cohorts": [],
    }

    def read_csv(_bucket_name: str, blob_path: str) -> pd.DataFrame:
        if "file_input_one" in blob_path:
            return pd.DataFrame(
                {"student_id": [1, 2], "enrollment_type": ["FIRST-TIME", "TRANSFER"]}
            )
        return pd.DataFrame(
            {
                "student_id": [1, 2],
                "academic_term": ["FALL", "SPRING"],
                "academic_year": ["2024-25", "2024-25"],
            }
        )

    MOCK_STORAGE.read_csv_as_dataframe.side_effect = read_csv
    response = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "retention_model", "batch_name": "batch_foo"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "status": "valid",
        "batch_name": "batch_foo",
        "terms": [{"term_label": "fall 2024-25", "valid_student_count": 1}],
        "reason": None,
    }
    MOCK_DATABRICKS.read_volume_training_config.assert_called_with(
        "school_1", "run-123", "legacy"
    )


def test_get_eligible_inference_terms_rejects_unauthorized_institution(
    client: TestClient,
) -> None:
    response = client.get(
        f"/institutions/{uuid_to_str(UUID_INVALID)}/eligible-inference-terms",
        params={"model_name": "retention_model", "batch_name": "batch_foo"},
    )

    assert response.status_code == 401


def test_get_eligible_inference_terms_requires_model_and_batch_name(
    client: TestClient,
) -> None:
    missing_params = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms"
    )
    missing_batch = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "retention_model"},
    )

    assert missing_params.status_code == 422
    assert missing_batch.status_code == 422


def test_get_eligible_inference_terms_reports_missing_model_or_batch(
    client: TestClient,
    session: sqlalchemy.orm.Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(data_router.env_vars, "ENV", "DEV")
    _add_registered_model(session)

    missing_model = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "missing_model", "batch_name": "batch_foo"},
    )
    missing_batch = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "retention_model", "batch_name": "missing"},
    )

    assert missing_model.status_code == 400
    assert missing_model.json()["detail"] == (
        "Unexpected number of models found: Expected 1, got 0"
    )
    assert missing_batch.status_code == 400
    assert missing_batch.json()["detail"] == (
        "Unexpected number of batches found: Expected 1, got 0"
    )


def test_get_eligible_inference_terms_reports_missing_config(
    client: TestClient,
    session: sqlalchemy.orm.Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(data_router.env_vars, "ENV", "DEV")
    _prepare_inference_batch(session)
    _add_registered_model(session)
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(run_id="run-123")
    MOCK_DATABRICKS.read_volume_training_config.return_value = None

    response = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "retention_model", "batch_name": "batch_foo"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "invalid"
    assert response.json()["batch_name"] == "batch_foo"
    assert response.json()["reason"] == "Training config could not be loaded."


def test_get_eligible_inference_terms_reports_unsupported_environment(
    client: TestClient,
    session: sqlalchemy.orm.Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(data_router.env_vars, "ENV", "LOCAL")
    _add_registered_model(session)
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(run_id="run-123")

    response = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "retention_model", "batch_name": "batch_foo"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "invalid"
    assert response.json()["batch_name"] == "batch_foo"
    assert response.json()["reason"] == (
        "Training config volumes are not configured for ENV=LOCAL."
    )


def test_get_eligible_inference_terms_reports_no_eligible_students(
    client: TestClient,
    session: sqlalchemy.orm.Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(data_router.env_vars, "ENV", "DEV")
    _prepare_inference_batch(session)
    _add_registered_model(session)
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(run_id="run-123")
    MOCK_DATABRICKS.read_volume_training_config.return_value = {
        "student_criteria": {"enrollment_type": "FIRST-TIME"},
        "training_cohorts": [],
    }

    def read_csv(_bucket_name: str, blob_path: str) -> pd.DataFrame:
        if "file_input_one" in blob_path:
            return pd.DataFrame({"student_id": [1], "enrollment_type": ["TRANSFER"]})
        return pd.DataFrame(
            {
                "student_id": [1],
                "academic_term": ["FALL"],
                "academic_year": ["2024-25"],
            }
        )

    MOCK_STORAGE.read_csv_as_dataframe.side_effect = read_csv
    response = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "retention_model", "batch_name": "batch_foo"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "invalid"
    assert response.json()["batch_name"] == "batch_foo"
    assert response.json()["reason"] == (
        "No academic term has eligible students with course data."
    )


def test_get_eligible_inference_terms_looks_up_percent_encoded_model_name(
    client: TestClient,
    session: sqlalchemy.orm.Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(data_router.env_vars, "ENV", "DEV")
    _prepare_inference_batch(session)
    _add_registered_model(session, name="risk%20model")
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(run_id="run-123")
    MOCK_DATABRICKS.read_volume_training_config.return_value = None

    response = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "risk%20model", "batch_name": "batch_foo"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "invalid"
    assert response.json()["batch_name"] == "batch_foo"
    assert response.json()["reason"] == "Training config could not be loaded."


def test_get_eligible_inference_terms_rewrites_eda_batch_read_reason(
    client: TestClient,
    session: sqlalchemy.orm.Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(data_router.env_vars, "ENV", "DEV")
    _prepare_inference_batch(session)
    _add_registered_model(session)
    MOCK_DATABRICKS.fetch_model_version.return_value = mock.Mock(run_id="run-123")
    MOCK_DATABRICKS.read_volume_training_config.return_value = {
        "student_criteria": {},
        "training_cohorts": [],
    }

    def raise_eda_error(*_args: object, **_kwargs: object) -> None:
        raise HTTPException(
            status_code=404,
            detail=(
                "No valid input files found in batch (checked GCS: bucket/validated/). "
                "Files must be uploaded and validated before they can be used for EDA."
            ),
        )

    monkeypatch.setattr(data_router, "read_batch_files_as_dataframes", raise_eda_error)
    response = client.get(
        f"/institutions/{uuid_to_str(USER_VALID_INST_UUID)}/eligible-inference-terms",
        params={"model_name": "retention_model", "batch_name": "batch_foo"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "invalid"
    assert response.json()["batch_name"] == "batch_foo"
    assert "inference eligibility" in response.json()["reason"]
    assert "EDA" not in response.json()["reason"]


def test_read_inst_all_input_files(client: TestClient) -> Any:
    """Test GET /institutions/<uuid>/input."""
    response = client.get("/institutions/" + uuid_to_str(UUID_INVALID) + "/input")

    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response = client.get(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/input"
    )
    assert response.status_code == 200
    assert same_orderless(  # type: ignore
        response.json(),
        {  # type: ignore
            "batches": [
                {
                    "batch_id": "5b2420f3103546ab90eb74d5df97de43",
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "file_names_to_ids": [
                        {"file_input_one": "f0bb3a206d924254afed6a72f43c562a"},
                        {"file_output_one": "fbe67a2e50e040c7b7b807043cb813a5"},
                    ],
                    "name": "batch_foo",
                    "created_by": "0ad8b77c49fb459a84b18d2c05722c4a",
                    "deleted": False,
                    "completed": False,
                    "deletion_request_time": None,
                    "created_at": "2024-12-24T20:22:20.132022",
                }
            ],
            "files": [
                {
                    "name": "file_input_one",
                    "data_id": "f0bb3a206d924254afed6a72f43c562a",
                    "batch_ids": ["5b2420f3103546ab90eb74d5df97de43"],
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "uploader": "",
                    "source": "MANUAL_UPLOAD",
                    "deleted": False,
                    "deletion_request_time": None,
                    "retention_days": None,
                    "sst_generated": False,
                    "valid": True,
                    "uploaded_date": "2024-12-24T20:22:20.132022",
                },
                {
                    "name": "file_input_two",
                    "data_id": "cb02d06c2a59486a9bddd394a4fcb833",
                    "batch_ids": [],
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "uploader": "",
                    "source": "PDP_SFTP",
                    "deleted": False,
                    "deletion_request_time": None,
                    "retention_days": None,
                    "sst_generated": False,
                    "valid": False,
                    "uploaded_date": "2024-12-24T20:22:20.132022",
                },
            ],
        },
    )


def test_list_bronze_datasets(client: TestClient) -> Any:
    """Test GET /institutions/<uuid>/input/bronze-datasets."""
    MOCK_DATABRICKS.reset_mock()
    MOCK_DATABRICKS.list_bronze_volume_csvs.return_value = ["a.csv", "b.csv"]

    response = client.get(
        "/institutions/" + uuid_to_str(UUID_INVALID) + "/input/bronze-datasets"
    )
    assert response.status_code == 401

    response = client.get(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/input/bronze-datasets"
    )
    assert response.status_code == 200
    assert response.json() == ["a.csv", "b.csv"]
    MOCK_DATABRICKS.list_bronze_volume_csvs.assert_called_with("school_1")


def test_upload_from_volume_to_gcs_bucket(client: TestClient) -> Any:
    """Test POST /institutions/<uuid>/input/upload-from-volume-to-gcs-bucket."""
    MOCK_DATABRICKS.reset_mock()
    MOCK_STORAGE.reset_mock()

    response = client.post(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/input/upload-from-volume-to-gcs-bucket",
        json={"name": "file.csv"},
    )
    assert response.status_code == 401

    MOCK_DATABRICKS.list_bronze_volume_csvs.return_value = ["file.csv"]
    MOCK_DATABRICKS.download_bronze_volume_file.return_value = io.BytesIO(
        b"col1,col2\n1,2\n"
    )
    MOCK_STORAGE.generate_upload_signed_url.return_value = "https://signed.example"

    with mock.patch("src.webapp.routers.data.requests.put") as mock_put:
        mock_put.return_value.status_code = 200
        mock_put.return_value.text = ""
        response = client.post(
            "/institutions/"
            + uuid_to_str(USER_VALID_INST_UUID)
            + "/input/upload-from-volume-to-gcs-bucket",
            json={"name": "file.csv"},
        )
        assert response.status_code == 200
        assert response.json() == {
            "file_name": "file.csv",
            "message": "Upload successful.",
        }

    MOCK_DATABRICKS.list_bronze_volume_csvs.assert_called_with("school_1")
    MOCK_DATABRICKS.download_bronze_volume_file.assert_called_with(
        "school_1", "file.csv"
    )
    MOCK_STORAGE.generate_upload_signed_url.assert_called_with(
        get_external_bucket_name(uuid_to_str(USER_VALID_INST_UUID)), "file.csv"
    )
    mock_put.assert_called_with(
        "https://signed.example",
        data=b"col1,col2\n1,2\n",
        headers={"Content-Type": "text/csv"},
        timeout=600,
    )


def test_read_inst_all_output_files(client: TestClient) -> Any:
    """Test GET /institutions/<uuid>/output."""
    MOCK_STORAGE.list_blobs_in_folder.return_value = []
    response = client.get("/institutions/" + uuid_to_str(UUID_INVALID) + "/output")

    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response = client.get(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/output"
    )
    assert response.status_code == 200
    assert same_orderless(  # type: ignore
        response.json(),
        {  # type: ignore
            "batches": [
                {
                    "batch_id": "5b2420f3103546ab90eb74d5df97de43",
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "file_names_to_ids": [
                        {"file_input_one": "f0bb3a206d924254afed6a72f43c562a"},
                        {"file_output_three": "fbe67a2e50e040c7b7b807043cb813a5"},
                    ],
                    "name": "batch_foo",
                    "created_by": "0ad8b77c49fb459a84b18d2c05722c4a",
                    "deleted": False,
                    "completed": False,
                    "deletion_request_time": None,
                    "created_at": "2024-12-24T20:22:20.132022",
                }
            ],
            "files": [
                {
                    "name": "file_output_three",
                    "data_id": "fbe67a2e50e040c7b7b807043cb813a5",
                    "batch_ids": ["5b2420f3103546ab90eb74d5df97de43"],
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "uploader": "",
                    "source": None,
                    "deleted": False,
                    "deletion_request_time": None,
                    "retention_days": None,
                    "sst_generated": True,
                    "valid": True,
                    "uploaded_date": "2024-12-24T20:22:20.132022",
                },
                {
                    "name": "file_output_four",
                    "data_id": "e4862c62829440d8ab4c9c298f02f619",
                    "batch_ids": [],
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "uploader": "",
                    "source": None,
                    "deleted": False,
                    "deletion_request_time": None,
                    "retention_days": None,
                    "sst_generated": True,
                    "valid": True,
                    "uploaded_date": "2024-12-24T20:22:20.132022",
                },
            ],
        },
    )


def test_read_batch_info(client: TestClient) -> Any:
    """Test GET /institutions/<uuid>/batch/<uuid>."""
    response = client.get(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/batch/"
        + uuid_to_str(BATCH_UUID)
    )

    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/batch/"
        + uuid_to_str(BATCH_UUID)
    )
    assert response.status_code == 200
    assert same_orderless(  # type: ignore
        response.json(),
        {  # type: ignore
            "batches": [
                {
                    "batch_id": "5b2420f3103546ab90eb74d5df97de43",
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "file_names_to_ids": [
                        {"file_input_one": "f0bb3a206d924254afed6a72f43c562a"},
                        {"file_output_three": "fbe67a2e50e040c7b7b807043cb813a5"},
                    ],
                    "name": "batch_foo",
                    "created_by": "0ad8b77c49fb459a84b18d2c05722c4a",
                    "deleted": False,
                    "completed": False,
                    "deletion_request_time": None,
                    "created_at": "2024-12-24T20:22:20.132022",
                }
            ],
            "files": [
                {
                    "name": "file_output_three",
                    "data_id": "fbe67a2e50e040c7b7b807043cb813a5",
                    "batch_ids": ["5b2420f3103546ab90eb74d5df97de43"],
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "uploader": "",
                    "source": None,
                    "deleted": False,
                    "deletion_request_time": None,
                    "retention_days": None,
                    "sst_generated": True,
                    "valid": True,
                    "uploaded_date": "2024-12-24T20:22:20.132022",
                },
                {
                    "name": "file_input_one",
                    "data_id": "f0bb3a206d924254afed6a72f43c562a",
                    "batch_ids": ["5b2420f3103546ab90eb74d5df97de43"],
                    "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
                    "uploader": "",
                    "source": "MANUAL_UPLOAD",
                    "deleted": False,
                    "deletion_request_time": None,
                    "retention_days": None,
                    "sst_generated": False,
                    "valid": True,
                    "uploaded_date": "2024-12-24T20:22:20.132022",
                },
            ],
        },
    )


def test_read_file_id_info(client: TestClient) -> Any:
    """Test GET /institutions/<uuid>/file-id/<uuid>."""
    response = client.get(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/file-id/"
        + uuid_to_str(FILE_UUID_1)
    )

    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/file-id/"
        + uuid_to_str(FILE_UUID_1)
    )
    assert response.status_code == 200
    assert same_file_orderless(  # type: ignore
        response.json(),
        {  # type: ignore
            "name": "file_input_one",
            "data_id": "f0bb3a206d924254afed6a72f43c562a",
            "batch_ids": ["5b2420f3103546ab90eb74d5df97de43"],
            "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
            "uploader": "",
            "source": "MANUAL_UPLOAD",
            "deleted": False,
            "deletion_request_time": None,
            "retention_days": None,
            "sst_generated": False,
            "valid": True,
            "uploaded_date": "2024-12-24T20:22:20.132022",
        },
    )


def test_retrieve_file_as_bytes(client: TestClient) -> Any:
    """Test GET /institutions/<uuid>/output-file-contents/<file_name>."""
    response = client.get(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/output-file-contents/"
        + "val%2Ffile_does_not_exist.csv"
    )

    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/output-file-contents/"
        + "val%2Ffile_does_not_exist.csv"
    )
    assert str(response) == "<Response [404 Not Found]>"
    assert response.text == '{"detail":"No such output file exists."}'


def test_create_batch(client: TestClient) -> None:
    """Test POST /institutions/<uuid>/batch."""
    MOCK_DATABRICKS.reset_mock()
    response = client.post(
        "/institutions/" + uuid_to_str(UUID_INVALID) + "/batch",
        json={"name": "batch_name_foo"},
    )
    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response = client.post(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/batch",
        json={
            "name": "batch_foobar",
            "batch_disabled": "False",
            "file_ids": [uuid_to_str(FILE_UUID_1)],
            "file_names": ["file_input_one", "file_input_two", "file_input_four"],
        },
    )
    assert response.status_code == 200
    assert response.json()["name"] == "batch_foobar"
    assert response.json()["created_by"] == uuid_to_str(USER_UUID)
    assert response.json()["deleted"] is False
    assert response.json()["completed"] is False
    assert response.json()["deletion_request_time"] is None
    assert response.json()["inst_id"] == uuid_to_str(USER_VALID_INST_UUID)
    # file_input_two isn't valid so it shouldn't be addable to a batch.
    assert "file_input_two" not in response.json()["file_names_to_ids"]
    assert "file_input_one" in response.json()["file_names_to_ids"]
    assert (
        uuid_to_str(FILE_UUID_1)
        in response.json()["file_names_to_ids"]["file_input_one"]
    )
    assert len(response.json()["file_names_to_ids"]) == 1
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_called_once()
    sync_req = MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.call_args[0][0]
    assert sync_req.batch_id == response.json()["batch_id"]
    assert sync_req.validated_blob_paths == ["validated/file_input_one"]
    assert sync_req.inst_name == "school_1"


def test_update_batch(client: TestClient) -> None:
    """Test PATCH /institutions/<uuid>/batch."""
    response = client.patch(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/batch/"
        + uuid_to_str(BATCH_UUID),
        json={"name": "batch_name_updated_foo"},
    )
    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response = client.patch(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/batch/"
        + uuid_to_str(BATCH_UUID),
        json={
            "name": "batch_name_updated_foo",
            "completed": True,
            "file_ids": [uuid_to_str(FILE_UUID_2)],
        },
    )
    assert response.status_code == 200
    assert response.json()["name"] == "batch_name_updated_foo"
    assert response.json()["created_by"] == uuid_to_str(CREATOR_UUID)
    assert response.json()["deleted"] is None
    assert response.json()["completed"] is True
    assert response.json()["deletion_request_time"] is None
    assert response.json()["inst_id"] == uuid_to_str(USER_VALID_INST_UUID)
    assert response.json()["file_names_to_ids"] == {
        "file_input_two": uuid_to_str(FILE_UUID_2)
    }


def test_validate_success_batch(client: TestClient) -> None:
    """Test PATCH /institutions/<uuid>/batch."""
    MOCK_STORAGE.validate_file.return_value = ["UNKNOWN"]

    # Use validate for manual upload
    response_upload = client.post(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/input/validate-upload/file_name.csv",
    )
    assert str(response_upload) == "<Response [401 Unauthorized]>"
    assert (
        response_upload.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response_upload = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/input/validate-upload/pdp_course_deidentified.csv",
    )
    assert response_upload.status_code == 200
    assert response_upload.json()["name"] == "pdp_course_deidentified.csv"
    assert response_upload.json()["file_types"] == ["COURSE"]
    assert response_upload.json()["inst_id"] == uuid_to_str(USER_VALID_INST_UUID)
    assert response_upload.json()["source"] == "MANUAL_UPLOAD"

    # Use validate for SFTP
    response_sftp = client.post(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/input/validate-sftp/pdp_ar_deidentified.csv",
    )
    assert str(response_sftp) == "<Response [401 Unauthorized]>"
    assert (
        response_sftp.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )
    # Authorized.
    response_sftp = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/input/validate-sftp/pdp_ar_deidentified.csv",
    )
    assert response_sftp.status_code == 200
    assert response_sftp.json()["name"] == "pdp_ar_deidentified.csv"
    assert response_sftp.json()["file_types"] == ["STUDENT"]
    assert response_sftp.json()["inst_id"] == uuid_to_str(USER_VALID_INST_UUID)
    assert response_sftp.json()["source"] == "PDP_SFTP"


def test_validate_failure_batch(client: TestClient) -> None:
    """Test PATCH /institutions/<uuid>/batch."""
    MOCK_STORAGE.validate_file.return_value = ["COURSE"]
    # Authorized.
    # Use validate upload
    response_upload = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/input/validate-upload/file_name_course.csv",
    )
    assert response_upload.status_code == 200
    assert response_upload.json()["name"] == "file_name_course.csv"
    assert response_upload.json()["file_types"] == ["COURSE"]
    assert response_upload.json()["inst_id"] == uuid_to_str(USER_VALID_INST_UUID)
    assert response_upload.json()["source"] == "MANUAL_UPLOAD"

    # Use valiate sftp
    response_sftp = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/input/validate-upload/file_name_course.csv",
    )
    assert response_sftp.status_code == 200
    assert response_sftp.json()["name"] == "file_name_course.csv"
    assert response_sftp.json()["file_types"] == ["COURSE"]
    assert response_sftp.json()["inst_id"] == uuid_to_str(USER_VALID_INST_UUID)
    assert response_sftp.json()["source"] == "MANUAL_UPLOAD"


def test_get_eda_data_unauthorized(client: TestClient) -> None:
    """Test GET /institutions/<uuid>/batch/<uuid>/eda with unauthorized access."""
    response = client.get(
        "/institutions/"
        + uuid_to_str(UUID_INVALID)
        + "/batch/"
        + uuid_to_str(BATCH_UUID)
        + "/eda"
    )
    assert str(response) == "<Response [401 Unauthorized]>"
    assert (
        response.text
        == '{"detail":"Not authorized to read this institution\'s resources."}'
    )


def test_get_eda_data_batch_not_found(client: TestClient) -> None:
    """Test GET /institutions/<uuid>/batch/<uuid>/eda with non-existent batch."""
    fake_batch_uuid = uuid.UUID("00000000-0000-0000-0000-000000000000")
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/batch/"
        + uuid_to_str(fake_batch_uuid)
        + "/eda"
    )
    assert response.status_code == 404
    assert response.json()["detail"] == "Batch not found."


def test_get_eda_data_no_student_files(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """Test GET /institutions/<uuid>/batch/<uuid>/eda with batch containing no STUDENT files."""
    # Create a batch with only COURSE files
    batch_with_course = BatchTable(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        inst_id=USER_VALID_INST_UUID,
        name="batch_course_only",
        created_by=CREATOR_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    course_file = FileTable(
        id=uuid.UUID("22222222-2222-2222-2222-222222222222"),
        inst_id=USER_VALID_INST_UUID,
        name="course_file.csv",
        source="MANUAL_UPLOAD",
        batches={batch_with_course},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.COURSE],
    )
    session.add_all([batch_with_course, course_file])
    session.commit()

    # Mock storage to return empty (no files found)
    MOCK_STORAGE.read_csv_as_dataframe.side_effect = ValueError("File not found")

    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/batch/"
        + uuid_to_str(batch_with_course.id)
        + "/eda"
    )
    assert response.status_code == 404
    # When files can't be loaded from GCS, we get "No valid input files found"
    # The "No STUDENT schema files found" error only occurs after files are loaded
    assert "No valid input files found" in response.json()["detail"]


def test_get_eda_data_success(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """Test GET /institutions/<uuid>/batch/<uuid>/eda with valid data."""
    import pandas as pd

    # Create a batch with STUDENT and COURSE files
    eda_batch = BatchTable(
        id=uuid.UUID("33333333-3333-3333-3333-333333333333"),
        inst_id=USER_VALID_INST_UUID,
        name="batch_eda_test",
        created_by=CREATOR_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        completed=True,
    )
    student_file = FileTable(
        id=uuid.UUID("44444444-4444-4444-4444-444444444444"),
        inst_id=USER_VALID_INST_UUID,
        name="student_file.csv",
        source="MANUAL_UPLOAD",
        batches={eda_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    course_file = FileTable(
        id=uuid.UUID("55555555-5555-5555-5555-555555555555"),
        inst_id=USER_VALID_INST_UUID,
        name="course_file.csv",
        source="MANUAL_UPLOAD",
        batches={eda_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.COURSE],
    )
    session.add_all([eda_batch, student_file, course_file])
    session.commit()

    df_cohort = pd.DataFrame(
        {
            "student_id": ["S001", "S002", "S003", "S001"],
            "cohort": ["2020", "2020", "2021", "2021"],
            "cohort_term": ["FALL", "FALL", "SPRING", "SPRING"],
            "enrollment_type": [
                "FIRST-TIME",
                "TRANSFER-IN",
                "FIRST-TIME",
                "TRANSFER-IN",
            ],
            "enrollment_intensity_first_term": [
                "Full-Time",
                "Part-Time",
                "Full-Time",
                "Part-Time",
            ],
            "gpa_group_year_1": [3.5, 3.2, 3.8, 2.9],
            "credential_type_sought_year_1": [
                "Bachelor",
                "Associate",
                "Bachelor",
                "Associate",
            ],
            "pell_status_first_year": ["Y", "N", "Y", "N"],
            "first_gen": ["Y", "N", "Y", "N"],
            "gender": ["Female", "Male", "Female", "Male"],
            "race": ["White", "Black or African American", "Asian", "White"],
            "student_age": ["20 - 24", "20 or younger", "Older than 24", "20 - 24"],
        }
    )
    df_course = pd.DataFrame(
        {
            "study_id": ["S001", "S002", "S003"],
            "cohort": ["2020", "2020", "2021"],
            "cohort_term": ["FALL", "FALL", "SPRING"],
        }
    )

    # Mock storage to return our test DataFrames
    def mock_read_csv(bucket_name: str, blob_path: str) -> pd.DataFrame:
        if "student" in blob_path.lower():
            return df_cohort
        elif "course" in blob_path.lower():
            return df_course
        else:
            raise ValueError(f"File not found: {blob_path}")

    MOCK_STORAGE.read_csv_as_dataframe.side_effect = mock_read_csv

    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/batch/"
        + uuid_to_str(eda_batch.id)
        + "/eda"
    )

    assert response.status_code == 200
    data = response.json()

    # Check response structure
    assert "total_students" in data
    assert "transfer_students" in data
    assert "avg_year1_gpa_all_students" in data
    assert "gpa_by_enrollment_type" in data
    assert "gpa_by_enrollment_intensity" in data
    assert "students_by_cohort_term" in data
    assert "course_enrollments" in data
    assert "degree_types" in data
    assert "total" in data["degree_types"]
    assert "degrees" in data["degree_types"]
    assert "enrollment_type_by_intensity" in data
    assert "pell_recipient_status" in data
    assert "pell_recipient_by_first_gen" in data
    assert "student_age_by_gender" in data
    assert "race_by_pell_status" in data

    # Check summary stats (each is { name, value })
    assert data["total_students"]["name"] == "Total Students"
    assert data["total_students"]["value"] == 3  # unique student_id (S001, S002, S003)
    assert data["transfer_students"]["name"] == "Transfer Students"
    assert data["transfer_students"]["value"] == 2  # 2 TRANSFER-IN

    # Check GPAs have cohort years
    assert "cohort_years" in data["gpa_by_enrollment_type"]
    assert len(data["gpa_by_enrollment_type"]["cohort_years"]) == 2  # 2020, 2021
    assert "2020" in data["gpa_by_enrollment_type"]["cohort_years"]
    assert "2021" in data["gpa_by_enrollment_type"]["cohort_years"]

    # Check term data structure (degree_types style: years + by_year with total and terms[{ count, percentage, name }])
    assert "years" in data["students_by_cohort_term"]
    assert "by_year" in data["students_by_cohort_term"]
    assert len(data["students_by_cohort_term"]["years"]) == 2
    by_year = data["students_by_cohort_term"]["by_year"]
    assert len(by_year) == 2
    assert "year" in by_year[0] and "total" in by_year[0] and "terms" in by_year[0]
    assert all(
        "count" in t and "percentage" in t and "name" in t for t in by_year[0]["terms"]
    )

    # Check enrollment type by intensity has categories and series
    assert "categories" in data["enrollment_type_by_intensity"]
    assert "series" in data["enrollment_type_by_intensity"]
    assert len(data["enrollment_type_by_intensity"]["series"]) > 0

    # Check pell recipients
    assert "categories" in data["pell_recipient_by_first_gen"]
    assert "series" in data["pell_recipient_by_first_gen"]

    # Check student age by gender structure
    assert "categories" in data["student_age_by_gender"]
    assert "series" in data["student_age_by_gender"]

    # Check race by pell status structure
    assert "categories" in data["race_by_pell_status"]
    assert "series" in data["race_by_pell_status"]


def test_get_eda_data_clear_cache(
    client: TestClient, session: sqlalchemy.orm.Session
) -> None:
    """clear-cache=1 evicts the TTL entry so batch files are read from storage again."""
    import pandas as pd

    data_router.EDA_CACHE.clear()

    eda_batch = BatchTable(
        id=uuid.UUID("66666666-6666-6666-6666-666666666666"),
        inst_id=USER_VALID_INST_UUID,
        name="batch_eda_clear_cache",
        created_by=CREATOR_UUID,
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        completed=True,
    )
    student_file = FileTable(
        id=uuid.UUID("77777777-7777-7777-7777-777777777777"),
        inst_id=USER_VALID_INST_UUID,
        name="student_clear_cache.csv",
        source="MANUAL_UPLOAD",
        batches={eda_batch},
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
        sst_generated=False,
        valid=True,
        schemas=[SchemaType.STUDENT],
    )
    session.add_all([eda_batch, student_file])
    session.commit()

    df_cohort = pd.DataFrame(
        {
            "student_id": ["S001"],
            "cohort": ["2020"],
            "cohort_term": ["FALL"],
            "enrollment_type": ["FIRST-TIME"],
            "enrollment_intensity_first_term": ["Full-Time"],
            "gpa_group_year_1": [3.5],
            "credential_type_sought_year_1": ["Bachelor"],
            "pell_status_first_year": ["N"],
            "first_gen": ["N"],
            "gender": ["Female"],
            "race": ["White"],
            "student_age": ["20 - 24"],
        }
    )

    def mock_read_csv(bucket_name: str, blob_path: str) -> pd.DataFrame:
        if "student" in blob_path.lower():
            return df_cohort
        raise ValueError(f"File not found: {blob_path}")

    MOCK_STORAGE.read_csv_as_dataframe.side_effect = mock_read_csv

    base_path = (
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/batch/"
        + uuid_to_str(eda_batch.id)
        + "/eda"
    )

    try:
        with mock.patch.object(
            data_router,
            "read_batch_files_as_dataframes",
            wraps=data_router.read_batch_files_as_dataframes,
        ) as read_dfs:
            r1 = client.get(base_path)
            assert r1.status_code == 200
            assert read_dfs.call_count == 1
            r2 = client.get(base_path)
            assert r2.status_code == 200
            assert read_dfs.call_count == 1
            r3 = client.get(base_path + "?clear-cache=1")
            assert r3.status_code == 200
            assert read_dfs.call_count == 2
            r4 = client.get(base_path)
            assert r4.status_code == 200
            assert read_dfs.call_count == 2
    finally:
        MOCK_STORAGE.reset_mock()


# ==================== EDVISE VALIDATION TESTS ====================

EDVISE_INST_UUID = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
EDVISE_INST_2_UUID = uuid.UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
EDVISE_SCHEMA_UUID = uuid.UUID("cccccccc-cccc-cccc-cccc-cccccccccccc")
LEGACY_INST_UUID = uuid.UUID("dddddddd-dddd-dddd-dddd-dddddddddddd")
GENAI_INST_UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
PDP_ONLY_INST_UUID = uuid.UUID("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")


@pytest.fixture(name="pdp_only_session")
def pdp_only_session_fixture():
    """Database setup for PDP-only institution (pdp_id set; no edvise_id or legacy_id)."""
    engine = sqlalchemy.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    pdp_schema_doc = {
        "version": "1.0.0",
        "institutions": {"pdp": {"data_models": {}}},
    }
    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=PDP_ONLY_INST_UUID,
                        name="pdp_only_school",
                        pdp_id="pdp999",
                        edvise_id=None,
                        legacy_id=None,
                        schemas=["STUDENT"],
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    SchemaRegistryTable(
                        doc_type=DocType.base,
                        is_pdp=False,
                        is_edvise=False,
                        version_label="1.0.0",
                        json_doc={"version": "1.0.0", "base": {"data_models": {}}},
                        is_active=True,
                        created_at=DATETIME_TESTING,
                    ),
                    SchemaRegistryTable(
                        doc_type=DocType.extension,
                        is_pdp=True,
                        is_edvise=False,
                        version_label="pdp-1.0.0",
                        json_doc=pdp_schema_doc,
                        is_active=True,
                        created_at=DATETIME_TESTING,
                    ),
                ]
            )
            session.commit()
            yield session
    finally:
        Base.metadata.drop_all(engine)


@pytest.fixture(name="pdp_only_client")
def pdp_only_client_fixture(
    pdp_only_session: sqlalchemy.orm.Session, monkeypatch: Any
) -> Any:
    """Test client for PDP-only institution validation tests."""
    monkeypatch.setenv("SST_SKIP_EXT_GEN", "1")

    def get_session_override():
        return pdp_only_session

    def get_current_active_user_override():
        from ..utilities import AccessType, BaseUser

        return BaseUser(
            uuid_to_str(USER_UUID),
            uuid_to_str(PDP_ONLY_INST_UUID),
            AccessType.MODEL_OWNER,
            "abc@example.com",
        )

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


@pytest.fixture(name="legacy_session")
def legacy_session_fixture():
    """Database setup for Legacy (any-format) tests: one institution with legacy_id."""
    engine = sqlalchemy.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=LEGACY_INST_UUID,
                        name="legacy_school",
                        legacy_id="legacy123",
                        pdp_id=None,
                        edvise_id=None,
                        schemas=["UNKNOWN"],
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    SchemaRegistryTable(
                        doc_type=DocType.base,
                        is_pdp=False,
                        is_edvise=False,
                        version_label="1.0.0",
                        json_doc={"version": "1.0.0", "base": {"data_models": {}}},
                        is_active=True,
                        created_at=DATETIME_TESTING,
                    ),
                ]
            )
            session.commit()
            yield session
    finally:
        Base.metadata.drop_all(engine)


@pytest.fixture(name="legacy_client")
def legacy_client_fixture(
    legacy_session: sqlalchemy.orm.Session, monkeypatch: Any
) -> Any:
    """Test client for Legacy institution tests."""
    monkeypatch.setenv("SST_SKIP_EXT_GEN", "1")

    def get_session_override():
        return legacy_session

    def get_current_active_user_override():
        from ..utilities import AccessType, BaseUser

        return BaseUser(
            uuid_to_str(USER_UUID),
            None,
            AccessType.DATAKINDER,
            "abc@example.com",
        )

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


@pytest.fixture(name="genai_session")
def genai_session_fixture():
    """Database setup for GenAI (any-format) tests: one institution with genai_id."""
    engine = sqlalchemy.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=GENAI_INST_UUID,
                        name="genai_school",
                        genai_id="genai123",
                        pdp_id=None,
                        edvise_id=None,
                        legacy_id=None,
                        schemas=["UNKNOWN"],
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    SchemaRegistryTable(
                        doc_type=DocType.base,
                        is_pdp=False,
                        is_edvise=False,
                        version_label="1.0.0",
                        json_doc={"version": "1.0.0", "base": {"data_models": {}}},
                        is_active=True,
                        created_at=DATETIME_TESTING,
                    ),
                ]
            )
            session.commit()
            yield session
    finally:
        Base.metadata.drop_all(engine)


@pytest.fixture(name="genai_client")
def genai_client_fixture(
    genai_session: sqlalchemy.orm.Session, monkeypatch: Any
) -> Any:
    """Test client for GenAI institution tests."""
    monkeypatch.setenv("SST_SKIP_EXT_GEN", "1")

    def get_session_override():
        return genai_session

    def get_current_active_user_override():
        from ..utilities import AccessType, BaseUser

        return BaseUser(
            uuid_to_str(USER_UUID),
            None,
            AccessType.DATAKINDER,
            "abc@example.com",
        )

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


@pytest.fixture(name="edvise_session")
def edvise_session_fixture():
    """Unit test database setup for Edvise Schema (ES) tests."""
    engine = sqlalchemy.create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)

    # Mock Edvise Schema (ES) extension (schema type, not product)
    edvise_schema_doc = {
        "version": "1.0.0",
        "institutions": {
            "edvise": {
                "data_models": {
                    "student": {
                        "columns": {
                            "student_id": {"type": "string", "required": True},
                            "cohort": {"type": "string", "required": True},
                        }
                    },
                    "course": {
                        "columns": {
                            "student_id": {"type": "string", "required": True},
                            "course_id": {"type": "string", "required": True},
                        }
                    },
                }
            }
        },
    }

    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=EDVISE_INST_UUID,
                        name="edvise_school",
                        edvise_id="edvise123",
                        pdp_id=None,
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    InstTable(
                        id=EDVISE_INST_2_UUID,
                        name="edvise_school_2",
                        edvise_id="edvise456",
                        pdp_id=None,
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    SchemaRegistryTable(
                        doc_type=DocType.base,
                        is_pdp=False,
                        is_edvise=False,
                        version_label="1.0.0",
                        json_doc={"version": "1.0.0", "base": {"data_models": {}}},
                        is_active=True,
                        created_at=DATETIME_TESTING,
                    ),
                    SchemaRegistryTable(
                        doc_type=DocType.extension,
                        is_pdp=False,
                        is_edvise=True,
                        version_label="edvise-1.0.0",
                        json_doc=edvise_schema_doc,
                        is_active=True,
                        created_at=DATETIME_TESTING,
                    ),
                    # Note: Edvise extension uses version_label="edvise-1.0.0" to avoid violating
                    # uq_pdp_version constraint (is_pdp, version_label) in MySQL, which requires
                    # unique (is_pdp, version_label) combinations across all rows. The base schema
                    # uses version_label="1.0.0" with is_pdp=False, so Edvise must use a different
                    # version_label. The JSON doc's "version": "1.0.0" field maintains semantic
                    # versioning, while the database version_label is prefixed for constraint uniqueness.
                ]
            )
            session.commit()
            yield session
    finally:
        Base.metadata.drop_all(engine)


@pytest.fixture(name="edvise_client")
def edvise_client_fixture(
    edvise_session: sqlalchemy.orm.Session, monkeypatch: Any
) -> Any:
    """Unit test mocks setup for Edvise Schema (ES) tests."""
    monkeypatch.setenv("SST_SKIP_EXT_GEN", "1")

    def get_session_override():
        return edvise_session

    def get_current_active_user_override():
        # Create DATAKINDER user with access to all institutions (needed for tests
        # that access multiple Edvise Schema (ES) institutions)
        from ..utilities import AccessType, BaseUser

        return BaseUser(
            uuid_to_str(USER_UUID),
            None,  # DATAKINDER has no specific institution
            AccessType.DATAKINDER,
            "abc@example.com",
        )

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


def test_validate_file_with_edvise_schema(edvise_client: TestClient) -> None:
    """Test file upload validation uses Edvise Schema (ES) when edvise_id is set."""
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]
    MOCK_DATABRICKS.reset_mock()

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/edvise_student_file.csv",
    )

    assert response.status_code == 200
    assert response.json()["name"] == "edvise_student_file.csv"
    assert response.json()["file_types"] == ["STUDENT"]
    assert response.json()["inst_id"] == uuid_to_str(EDVISE_INST_UUID)
    assert response.json()["source"] == "MANUAL_UPLOAD"

    # Verify that validate_file was called with institution_identifier for Edvise Schema (ES)
    assert MOCK_STORAGE.validate_file.called
    call_kwargs = MOCK_STORAGE.validate_file.call_args.kwargs
    assert call_kwargs.get("institution_id") == "edvise"
    assert call_kwargs.get("institution_identifier") == "edvise_school"

    # Non-PDP bronze sync runs only when batch_id is provided at validation time.
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_not_called()


def test_validate_with_batch_id_triggers_bronze_sync(
    client: TestClient,
) -> None:
    """Validate with batch_id copies validated files into bronze under that batch folder."""
    MOCK_STORAGE.validate_file.return_value = ["COURSE"]
    MOCK_DATABRICKS.reset_mock()

    response = client.post(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/input/validate-upload/batch_scoped_file.csv"
        + "?batch_id="
        + uuid_to_str(BATCH_UUID),
    )

    assert response.status_code == 200
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_called_once()
    sync_req = MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.call_args[0][0]
    assert sync_req.batch_id == uuid_to_str(BATCH_UUID)
    assert sync_req.validated_blob_paths == ["validated/batch_scoped_file.csv"]


def test_validate_file_with_legacy_schema(legacy_client: TestClient) -> None:
    """Test file upload validation uses legacy (any-format) path when legacy_id is set."""
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]
    MOCK_DATABRICKS.reset_mock()

    response = legacy_client.post(
        "/institutions/"
        + uuid_to_str(LEGACY_INST_UUID)
        + "/input/validate-upload/legacy_student_data.csv",
    )

    assert response.status_code == 200
    assert response.json()["name"] == "legacy_student_data.csv"
    assert response.json()["file_types"] == ["STUDENT"]
    assert response.json()["inst_id"] == uuid_to_str(LEGACY_INST_UUID)

    assert MOCK_STORAGE.validate_file.called
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_not_called()
    call_kwargs = MOCK_STORAGE.validate_file.call_args.kwargs
    assert call_kwargs.get("institution_id") == "legacy"


def test_validate_file_with_genai_schema(genai_client: TestClient) -> None:
    """GenAI schools use legacy any-format validation and trigger GCS→bronze sync."""
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]
    MOCK_DATABRICKS.reset_mock()

    response = genai_client.post(
        "/institutions/"
        + uuid_to_str(GENAI_INST_UUID)
        + "/input/validate-upload/genai_student_data.csv",
    )

    assert response.status_code == 200
    assert response.json()["name"] == "genai_student_data.csv"
    assert response.json()["file_types"] == ["STUDENT"]
    assert response.json()["inst_id"] == uuid_to_str(GENAI_INST_UUID)

    assert MOCK_STORAGE.validate_file.called
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_not_called()
    call_kwargs = MOCK_STORAGE.validate_file.call_args.kwargs
    assert call_kwargs.get("institution_id") == "legacy"


def test_validate_file_genai_accepts_arbitrary_filename(
    genai_client: TestClient,
) -> None:
    """GenAI schools may use any filename; when inference fails, allowed_schemas is UNKNOWN."""
    MOCK_STORAGE.validate_file.return_value = ["UNKNOWN"]
    MOCK_DATABRICKS.reset_mock()

    response = genai_client.post(
        "/institutions/"
        + uuid_to_str(GENAI_INST_UUID)
        + "/input/validate-upload/export_2024.csv",
    )

    assert response.status_code == 200
    assert response.json()["name"] == "export_2024.csv"
    assert response.json()["file_types"] == ["UNKNOWN"]
    assert response.json()["inst_id"] == uuid_to_str(GENAI_INST_UUID)

    assert MOCK_STORAGE.validate_file.called
    assert MOCK_STORAGE.validate_file.call_args.args[2] == ["UNKNOWN"]
    assert MOCK_STORAGE.validate_file.call_args.kwargs.get("institution_id") == "legacy"
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_not_called()


def test_validate_upload_pdp_only_institution_skips_bronze_sync(
    pdp_only_client: TestClient,
) -> None:
    """PDP-only schools do not trigger GCS→bronze sync (Edvise/Legacy/GenAI only)."""
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]
    MOCK_DATABRICKS.reset_mock()

    response = pdp_only_client.post(
        "/institutions/"
        + uuid_to_str(PDP_ONLY_INST_UUID)
        + "/input/validate-upload/pdp_student_file.csv",
    )

    assert response.status_code == 200
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_not_called()


def test_validate_upload_skips_bronze_sync_when_env_disabled(
    edvise_client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ENABLE_GCS_BRONZE_SYNC_ON_VALIDATION=false disables bronze sync for Edvise schools."""
    monkeypatch.setenv("ENABLE_GCS_BRONZE_SYNC_ON_VALIDATION", "false")
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]
    MOCK_DATABRICKS.reset_mock()

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/edvise_student_file.csv",
    )

    assert response.status_code == 200
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_not_called()


def test_validate_upload_databricks_trigger_failure_is_non_fatal(
    client: TestClient,
) -> None:
    """Databricks trigger errors do not fail validation after the file is validated."""
    MOCK_STORAGE.validate_file.return_value = ["COURSE"]
    MOCK_DATABRICKS.reset_mock()
    MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.side_effect = RuntimeError(
        "network failed"
    )
    try:
        response = client.post(
            "/institutions/"
            + uuid_to_str(USER_VALID_INST_UUID)
            + "/input/validate-upload/edvise_student_file.csv"
            + "?batch_id="
            + uuid_to_str(BATCH_UUID),
        )

        assert response.status_code == 200
        assert response.json()["name"] == "edvise_student_file.csv"
        MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.assert_called_once()
    finally:
        MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.side_effect = None
        MOCK_DATABRICKS.run_validated_gcs_to_bronze_sync.return_value = mock.Mock(
            job_run_id=1
        )


def test_validate_upload_rejects_empty_file_name(legacy_client: TestClient) -> None:
    """Empty or whitespace-only file name returns 422."""
    # Trailing space in path decodes to " "
    response = legacy_client.post(
        "/institutions/" + uuid_to_str(LEGACY_INST_UUID) + "/input/validate-upload/%20",
    )
    assert response.status_code == 422
    assert (
        "required" in response.json()["detail"].lower()
        or "non-empty" in response.json()["detail"].lower()
    )


def test_validate_upload_invalid_inst_id_returns_404(legacy_client: TestClient) -> None:
    """Invalid institution id (non-UUID) returns 404, not 500."""
    response = legacy_client.post(
        "/institutions/not-a-valid-uuid/input/validate-upload/foo.csv",
    )
    assert response.status_code == 404
    assert (
        "institution" in response.json()["detail"].lower()
        or "invalid" in response.json()["detail"].lower()
    )


def test_validate_file_legacy_accepts_arbitrary_filename(
    legacy_client: TestClient,
) -> None:
    """Legacy schools may use any filename; when inference fails, allowed_schemas is UNKNOWN."""
    MOCK_STORAGE.validate_file.return_value = ["UNKNOWN"]

    response = legacy_client.post(
        "/institutions/"
        + uuid_to_str(LEGACY_INST_UUID)
        + "/input/validate-upload/export_2024.csv",
    )

    assert response.status_code == 200
    assert response.json()["name"] == "export_2024.csv"
    assert response.json()["file_types"] == ["UNKNOWN"]
    assert response.json()["inst_id"] == uuid_to_str(LEGACY_INST_UUID)

    assert MOCK_STORAGE.validate_file.called
    # allowed_schemas is passed positionally (args[2])
    assert MOCK_STORAGE.validate_file.call_args.args[2] == ["UNKNOWN"]
    assert MOCK_STORAGE.validate_file.call_args.kwargs.get("institution_id") == "legacy"


def test_validate_file_legacy_pii_rejection_returns_400(
    legacy_client: TestClient,
) -> None:
    """When legacy validation raises HardValidationError (e.g. PII columns), API returns 400."""
    from ..validation import HardValidationError

    MOCK_STORAGE.validate_file.side_effect = HardValidationError(
        schema_errors="Legacy upload: file contains columns that may contain personally identifiable information (PII).",
        failure_cases=["email", "first_name"],
    )

    response = legacy_client.post(
        "/institutions/"
        + uuid_to_str(LEGACY_INST_UUID)
        + "/input/validate-upload/legacy_student_data.csv",
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "PII" in detail or "personally" in detail.lower()

    MOCK_STORAGE.validate_file.side_effect = None
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]


# --- Unit tests for validation helpers ---


def _make_inst(legacy_id: Any = None) -> Any:
    """Minimal institution-like object for _infer_allowed_schemas_from_filename."""

    class Inst:
        legacy_id: Any = None

    inst = Inst()
    inst.legacy_id = legacy_id
    return inst


def test_infer_allowed_schemas_course_only() -> None:
    """Filename with 'course' infers [COURSE]."""
    inst = _make_inst(legacy_id=None)
    assert _infer_allowed_schemas_from_filename("course_export.csv", inst) == ["COURSE"]


def test_infer_allowed_schemas_student_only() -> None:
    """Filename with 'student' infers [STUDENT]."""
    inst = _make_inst(legacy_id=None)
    assert _infer_allowed_schemas_from_filename("student_data.csv", inst) == ["STUDENT"]


def test_infer_allowed_schemas_semester_only() -> None:
    """Filename with 'semester' infers [SEMESTER]."""
    inst = _make_inst(legacy_id=None)
    assert _infer_allowed_schemas_from_filename("semester.csv", inst) == ["SEMESTER"]


def test_infer_allowed_schemas_cohort_infers_student() -> None:
    """Filename with 'cohort' (no course) infers [STUDENT]."""
    inst = _make_inst(legacy_id=None)
    assert _infer_allowed_schemas_from_filename("cohort_2024.csv", inst) == ["STUDENT"]


def test_infer_allowed_schemas_multiple_keywords() -> None:
    """Filename with student and course infers both, sorted."""
    inst = _make_inst(legacy_id=None)
    result = _infer_allowed_schemas_from_filename("student_course_combined.csv", inst)
    assert sorted(result) == ["COURSE", "STUDENT"]


def test_infer_allowed_schemas_legacy_arbitrary_returns_unknown() -> None:
    """Legacy institution with non-descriptive filename gets [UNKNOWN]."""
    inst = _make_inst(legacy_id="legacy_1")
    assert _infer_allowed_schemas_from_filename("report_2024.csv", inst) == ["UNKNOWN"]


def test_infer_allowed_schemas_non_legacy_arbitrary_raises_422() -> None:
    """Non-legacy institution with non-descriptive filename raises 422."""
    inst = _make_inst(legacy_id=None)
    with pytest.raises(HTTPException) as exc_info:
        _infer_allowed_schemas_from_filename("random.csv", inst)
    assert exc_info.value.status_code == 422
    assert "Could not infer model(s)" in exc_info.value.detail
    assert "random" in exc_info.value.detail


def test_validate_edvise_non_descriptive_filename_returns_422(
    edvise_client: TestClient,
) -> None:
    """Edvise institution with non-descriptive filename (no student/course/semester) returns 422."""
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/report_2024.csv",
    )

    assert response.status_code == 422
    assert "Could not infer model(s)" in response.json()["detail"]
    assert "report" in response.json()["detail"]


def test_validate_edvise_unsupported_model_validation_error_returns_400(
    edvise_client: TestClient,
) -> None:
    """Unsupported Edvise model sets surface as validation errors."""
    from ..validation import HardValidationError

    MOCK_STORAGE.validate_file.side_effect = HardValidationError(
        schema_errors=(
            "edvise upload validation only supports STUDENT and COURSE "
            "single-model uploads through the edvise repo. Requested model(s): SEMESTER."
        ),
        failure_cases=[],
    )

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/semester.csv",
    )

    assert response.status_code == 400
    detail = response.json()["detail"]
    assert "edvise repo" in detail
    assert "SEMESTER" in detail

    MOCK_STORAGE.validate_file.side_effect = None
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]


def test_validate_upload_second_call_succeeds_and_idempotent(
    legacy_client: TestClient,
) -> None:
    """Validating the same file twice returns 200 both times; no duplicate insert failure."""
    MOCK_STORAGE.validate_file.return_value = ["UNKNOWN"]

    response1 = legacy_client.post(
        "/institutions/"
        + uuid_to_str(LEGACY_INST_UUID)
        + "/input/validate-upload/duplicate_test.csv",
    )
    assert response1.status_code == 200
    assert response1.json()["name"] == "duplicate_test.csv"
    assert response1.json()["file_types"] == ["UNKNOWN"]

    response2 = legacy_client.post(
        "/institutions/"
        + uuid_to_str(LEGACY_INST_UUID)
        + "/input/validate-upload/duplicate_test.csv",
    )
    assert response2.status_code == 200
    # Second call hits existing-file path; response payload is unchanged (ValidationResult omits status)
    assert response2.json()["name"] == response1.json()["name"]
    assert response2.json()["file_types"] == response1.json()["file_types"]
    assert response2.json()["inst_id"] == response1.json()["inst_id"]


def test_validate_file_with_edvise_does_not_require_active_schema_doc(
    edvise_client: TestClient, edvise_session: sqlalchemy.orm.Session
) -> None:
    """Edvise uploads no longer require active JSON schema registry docs."""
    edvise_schema = edvise_session.execute(
        select(SchemaRegistryTable).where(
            SchemaRegistryTable.is_edvise.is_(True),
            SchemaRegistryTable.is_active.is_(True),
        )
    ).scalar_one_or_none()
    if edvise_schema:
        edvise_schema.is_active = False
        edvise_session.commit()

    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/test_student_file.csv",
    )

    assert response.status_code == 200
    assert MOCK_STORAGE.validate_file.call_args.kwargs.get("institution_id") == "edvise"
    assert (
        MOCK_STORAGE.validate_file.call_args.kwargs.get("institution_identifier")
        == "edvise_school"
    )


def test_validation_helper_pdp_and_edvise_mutual_exclusivity(
    edvise_client: TestClient, edvise_session: sqlalchemy.orm.Session
) -> None:
    """Test that validation_helper rejects institutions with both pdp_id and edvise_id."""
    # Corrupt the institution data to have both pdp_id and edvise_id
    corrupted_inst = edvise_session.execute(
        select(InstTable).where(InstTable.id == EDVISE_INST_UUID)
    ).scalar_one_or_none()
    corrupted_inst.pdp_id = "pdp999"  # type: ignore
    edvise_session.commit()

    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/test_student_file.csv",
    )

    assert response.status_code == 500
    assert (
        "cannot have more than one of pdp_id, edvise_id, legacy_id, or genai_id set"
        in response.json()["detail"]
    )

    # Restore for other tests
    corrupted_inst.pdp_id = None  # type: ignore
    edvise_session.commit()


def test_validation_helper_rejects_institution_without_school_type(
    edvise_client: TestClient, edvise_session: sqlalchemy.orm.Session
) -> None:
    """Upload validation requires a school-type id on the institution."""
    inst = edvise_session.execute(
        select(InstTable).where(InstTable.id == EDVISE_INST_UUID)
    ).scalar_one()
    saved = (inst.edvise_id, inst.pdp_id, inst.legacy_id, inst.genai_id)
    inst.edvise_id = None  # type: ignore
    inst.pdp_id = None  # type: ignore
    inst.legacy_id = None  # type: ignore
    inst.genai_id = None  # type: ignore
    edvise_session.commit()

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/test_student_file.csv",
    )
    assert response.status_code == 500
    assert "no pdp_id, edvise_id, legacy_id, or genai_id" in response.json()["detail"]

    inst.edvise_id, inst.pdp_id, inst.legacy_id, inst.genai_id = saved
    edvise_session.commit()


def test_validate_file_edvise_schema_validation_errors(
    edvise_client: TestClient,
) -> None:
    """Test that validation errors are returned correctly for Edvise Schema (ES)."""
    from ..validation import HardValidationError

    # Mock validation to raise an error
    def mock_validate_file(*args, **kwargs):
        raise HardValidationError(
            missing_required=["student_id"],
            extra_columns=[],
            schema_errors=None,
            failure_cases=None,
        )

    MOCK_STORAGE.validate_file.side_effect = mock_validate_file

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/invalid_student_file.csv",
    )

    assert response.status_code == 400
    # Check for user-friendly error message (Phase 4: Error Message Improvements)
    detail = response.json()["detail"]
    assert "Missing required columns" in detail or "student_id" in detail
    # The message should be user-friendly, not technical "VALIDATION_FAILED"
    assert "VALIDATION_FAILED" not in detail or "missing_required=" not in detail

    # Reset mock
    MOCK_STORAGE.validate_file.side_effect = None
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]


def test_edvise_validation_does_not_pass_custom_schema_doc(
    edvise_client: TestClient, edvise_session: sqlalchemy.orm.Session
) -> None:
    """Edvise uploads pass namespace only, not JSON schema registry docs."""
    # Add a custom extension for this institution with unique version_label
    custom_schema = SchemaRegistryTable(
        doc_type=DocType.extension,
        inst_id=EDVISE_INST_UUID,
        is_pdp=False,
        is_edvise=False,
        version_label="1.0.1",  # Use different version to avoid unique constraint
        json_doc={"version": "1.0.1", "custom": {"data_models": {}}},
        is_active=True,
        created_at=DATETIME_TESTING,
    )
    edvise_session.add(custom_schema)
    edvise_session.commit()

    # Capture active validation metadata and ensure only bucket/name/models are positional.
    captured_arg_count = None
    captured_institution_id = None
    captured_institution_identifier = None

    def capture_schema(*args, **kwargs):
        # fmt: off
        nonlocal captured_arg_count, captured_institution_id, captured_institution_identifier
        # fmt: on
        # validate_file(bucket, file_name, allowed_schemas, institution_id=..., institution_identifier=...)
        captured_arg_count = len(args)
        captured_institution_id = kwargs.get("institution_id")
        captured_institution_identifier = kwargs.get("institution_identifier")
        return ["STUDENT"]

    MOCK_STORAGE.validate_file.side_effect = capture_schema

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/test_student_file.csv",
    )

    assert response.status_code == 200

    assert captured_arg_count == 3
    assert captured_institution_id == "edvise"
    assert captured_institution_identifier == "edvise_school"

    # Reset mock
    MOCK_STORAGE.validate_file.side_effect = None
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]


def test_validate_sftp_with_edvise_schema(edvise_client: TestClient) -> None:
    """Test SFTP file validation uses Edvise Schema (ES) when edvise_id is set."""
    MOCK_STORAGE.validate_file.return_value = ["COURSE"]

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-sftp/edvise_course_file.csv",
    )

    assert response.status_code == 200
    assert response.json()["name"] == "edvise_course_file.csv"
    assert response.json()["file_types"] == ["COURSE"]
    assert response.json()["inst_id"] == uuid_to_str(EDVISE_INST_UUID)
    assert response.json()["source"] == "PDP_SFTP"

    # Verify that validate_file was called
    assert MOCK_STORAGE.validate_file.called


def test_validate_edvise_unauthorized(
    edvise_session: sqlalchemy.orm.Session, monkeypatch: Any
) -> None:
    """Test validation endpoint with unauthorized access."""
    monkeypatch.setenv("SST_SKIP_EXT_GEN", "1")

    # Create a test client with a MODEL_OWNER user who only has access to EDVISE_INST_UUID
    # This user should NOT have access to EDVISE_INST_2_UUID
    def get_session_override():
        return edvise_session

    def get_current_active_user_override():
        # User belongs to EDVISE_INST_UUID, not DATAKINDER, so access is restricted
        from ..utilities import AccessType, BaseUser

        return BaseUser(
            uuid_to_str(USER_UUID),
            uuid_to_str(EDVISE_INST_UUID),  # User belongs to this institution
            AccessType.MODEL_OWNER,  # Not DATAKINDER, so access is restricted
            "abc@example.com",
        )

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
    try:
        # Try to access EDVISE_INST_2_UUID which exists but user doesn't have access to
        response = client.post(
            "/institutions/"
            + uuid_to_str(
                EDVISE_INST_2_UUID
            )  # Institution exists but user is unauthorized
            + "/input/validate-upload/test_student.csv",
        )
        assert response.status_code == 401
        assert "Not authorized" in response.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_validate_edvise_invalid_filename(edvise_client: TestClient) -> None:
    """Test validation rejects file names with '/'."""
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/invalid/file.csv",
    )
    assert response.status_code == 422
    assert "can't contain '/'" in response.json()["detail"]


def test_validate_edvise_course_file(edvise_client: TestClient) -> None:
    """Test COURSE file validation with Edvise Schema (ES)."""
    MOCK_STORAGE.validate_file.return_value = ["COURSE"]

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(EDVISE_INST_UUID)
        + "/input/validate-upload/edvise_course.csv",
    )

    assert response.status_code == 200
    assert response.json()["file_types"] == ["COURSE"]
    assert response.json()["name"] == "edvise_course.csv"
    assert response.json()["inst_id"] == uuid_to_str(EDVISE_INST_UUID)


def test_validate_edvise_inst_not_found(edvise_client: TestClient) -> None:
    """Test validation with non-existent institution."""
    fake_uuid = uuid.UUID("00000000-0000-0000-0000-000000000000")
    MOCK_STORAGE.validate_file.return_value = ["STUDENT"]

    response = edvise_client.post(
        "/institutions/"
        + uuid_to_str(fake_uuid)
        + "/input/validate-upload/test_student.csv",
    )
    # Should fail - either 401 (unauthorized) or 404 (not found)
    assert response.status_code in [401, 404]
