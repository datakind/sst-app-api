from types import SimpleNamespace
from unittest import mock

import pytest
from unittest.mock import MagicMock

from .databricks import (
    BRONZE_SYNC_BRONZE_SUBDIR,
    BRONZE_SYNC_GCS_SOURCE_PREFIX,
    BRONZE_SYNC_MAX_OBJECTS,
    BRONZE_SYNC_REQUIRE_AT_LEAST_ONE_FILE,
    BRONZE_SYNC_STRICT_MODE,
    CLOUDRUN_BUNDLE_JOB_PREFIX,
    DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV,
    DatabricksBronzeSyncRequest,
    DatabricksControl,
    DatabricksPDPInferenceRunRequest,
    DatabricksSharedInferenceRunRequest,
    _build_pdp_inference_job_parameters,
    _build_shared_inference_job_parameters,
    _build_validated_bronze_sync_job_parameters,
    _parse_training_config,
    _resolve_pipeline_job,
    _resolve_validated_bronze_sync_job_id,
)
from .utilities import SchemaType

VALID_PDP_TRAINING_TOML = b"""
institution_id = "test_inst"
institution_name = "Test School"

[datasets]
raw_course = "course.csv"
raw_cohort = "cohort.csv"

[preprocessing.features]
min_passing_grade = 1.0
min_num_credits_full_time = 12.0

[preprocessing.selection]
student_criteria = { enrollment_type = "FIRST-TIME" }

[preprocessing.checkpoint]
name = "first_within_cohort"
type_ = "first_within_cohort"

[preprocessing.target]
name = "retention"
type_ = "retention"
max_academic_year = "2024"

[modeling.training]
primary_metric = "logloss"
cohort = ["fall 2022-23", "spring 2022-23"]

[inference]
term = ["fall 2024-25"]
"""

VALID_LEGACY_TRAINING_TOML = b"""
institution_id = "legacy_inst"
institution_name = "Legacy School"
student_group_cols = []

[datasets.bronze]
[datasets.silver.modeling]
train_table_path = "catalog.schema.modeling"
[datasets.silver.model_features]
predict_table_path = "catalog.schema.features"
[datasets.gold]

[preprocessing.selection]
student_criteria = { enrollment_type = "FIRST-TIME" }
student_criteria_aliases = { enrollment_type = "Enrollment Type" }

[preprocessing.checkpoint]
name = "30_credits"
unit = "credit"
value = 30

[preprocessing.target]
name = "graduation"
category = "graduation"
unit = "pct_completion"
value = 150

[modeling.training]
cohort = ["fall 2022-23"]
"""


@pytest.fixture
def ctrl():
    return DatabricksControl()


@pytest.mark.parametrize(
    ("schema_type", "student_id_col"),
    [("pdp", "student_id"), ("edvise", "learner_id")],
)
def test_parse_training_config_returns_selection_and_training_cohorts(
    schema_type: str,
    student_id_col: str,
) -> None:
    assert _parse_training_config(VALID_PDP_TRAINING_TOML, schema_type) == {
        "student_id_col": student_id_col,
        "student_criteria": {"enrollment_type": "FIRST-TIME"},
        "training_cohorts": ["fall 2022-23", "spring 2022-23"],
    }


def test_parse_training_config_supports_legacy_schema() -> None:
    assert _parse_training_config(VALID_LEGACY_TRAINING_TOML, "legacy") == {
        "student_id_col": "student_id",
        "student_criteria": {"enrollment_type": "FIRST-TIME"},
        "training_cohorts": ["fall 2022-23"],
    }


def test_parse_training_config_rejects_invalid_or_missing_selection():
    assert _parse_training_config(b"not valid {{{", "pdp") is None
    assert _parse_training_config(b"[modeling.training]\ncohort = []", "pdp") is None
    assert (
        _parse_training_config(
            b"""
institution_id = "test_inst"
institution_name = "Test School"
[datasets]
raw_course = "course.csv"
raw_cohort = "cohort.csv"
""",
            "pdp",
        )
        is None
    )


def test_parse_training_config_rejects_cross_schema_config() -> None:
    assert _parse_training_config(VALID_PDP_TRAINING_TOML, "legacy") is None


def test_read_volume_training_config_reads_model_run_toml(
    monkeypatch: pytest.MonkeyPatch,
    ctrl: DatabricksControl,
) -> None:
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(db_mod.env_vars, "ENV", "DEV")
    response = mock.Mock()
    response.contents.read.return_value = VALID_PDP_TRAINING_TOML
    workspace = mock.Mock()
    workspace.files.list_directory_contents.return_value = [
        SimpleNamespace(
            path="/Volumes/dev_sst_02/test_school_silver/silver_volume/run-1/training/config.toml",
            name="config.toml",
            is_directory=False,
        )
    ]
    workspace.files.download.return_value = response

    with mock.patch.object(db_mod, "WorkspaceClient", return_value=workspace):
        config = ctrl.read_volume_training_config("Test School", "run-1", "pdp")

    assert config == {
        "student_id_col": "student_id",
        "student_criteria": {"enrollment_type": "FIRST-TIME"},
        "training_cohorts": ["fall 2022-23", "spring 2022-23"],
    }
    workspace.files.list_directory_contents.assert_called_once_with(
        "/Volumes/dev_sst_02/test_school_silver/silver_volume/run-1/training/"
    )


def test_read_volume_training_config_returns_none_when_training_dir_missing(
    monkeypatch: pytest.MonkeyPatch,
    ctrl: DatabricksControl,
) -> None:
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(db_mod.env_vars, "ENV", "DEV")
    workspace = mock.Mock()
    workspace.files.list_directory_contents.side_effect = FileNotFoundError("missing")

    with mock.patch.object(db_mod, "WorkspaceClient", return_value=workspace):
        assert ctrl.read_volume_training_config("Test School", "run-1", "pdp") is None

    workspace.files.list_directory_contents.assert_called_once_with(
        "/Volumes/dev_sst_02/test_school_silver/silver_volume/run-1/training/"
    )
    workspace.files.download.assert_not_called()


def test_read_volume_training_config_does_not_fallback_from_invalid_config_toml(
    monkeypatch: pytest.MonkeyPatch,
    ctrl: DatabricksControl,
) -> None:
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(db_mod.env_vars, "ENV", "DEV")
    response = mock.Mock()
    response.contents.read.return_value = b"invalid {{{"
    workspace = mock.Mock()
    workspace.files.list_directory_contents.return_value = [
        SimpleNamespace(
            path="/training/config.toml",
            name="config.toml",
            is_directory=False,
        ),
        SimpleNamespace(
            path="/training/other.toml",
            name="other.toml",
            is_directory=False,
        ),
    ]
    workspace.files.download.return_value = response

    with mock.patch.object(db_mod, "WorkspaceClient", return_value=workspace):
        assert ctrl.read_volume_training_config("Test School", "run-1", "pdp") is None

    workspace.files.download.assert_called_once_with("/training/config.toml")


def test_read_volume_training_config_rejects_ambiguous_tomls(
    monkeypatch: pytest.MonkeyPatch,
    ctrl: DatabricksControl,
) -> None:
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(db_mod.env_vars, "ENV", "DEV")
    workspace = mock.Mock()
    workspace.files.list_directory_contents.return_value = [
        SimpleNamespace(path="/training/one.toml", name="one.toml", is_directory=False),
        SimpleNamespace(path="/training/two.toml", name="two.toml", is_directory=False),
    ]

    with mock.patch.object(db_mod, "WorkspaceClient", return_value=workspace):
        assert ctrl.read_volume_training_config("Test School", "run-1", "pdp") is None

    workspace.files.download.assert_not_called()


@pytest.mark.parametrize("environment", ["LOCAL", "PROD"])
def test_read_volume_training_config_returns_none_without_volume_mapping(
    monkeypatch: pytest.MonkeyPatch,
    ctrl: DatabricksControl,
    environment: str,
) -> None:
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(db_mod.env_vars, "ENV", environment)

    assert ctrl.read_volume_training_config("Test School", "run-1", "pdp") is None


def test_exact_literal_case_insensitive(ctrl):
    mapping = {"student": "student.csv"}
    assert ctrl.get_key_for_file(mapping, "Student.csv") == "student"


def test_literal_with_suffix_and_same_ext(ctrl):
    mapping = {"student": "student.csv"}
    assert ctrl.get_key_for_file(mapping, "student_20240101.csv") == "student"
    assert ctrl.get_key_for_file(mapping, "student-final.csv") == "student"
    # should not match a different extension
    assert ctrl.get_key_for_file(mapping, "student_20240101.tsv") is None


def test_literal_without_ext_allows_suffix_and_optional_ext(ctrl):
    mapping = {"student": "student"}
    assert ctrl.get_key_for_file(mapping, "student") == "student"
    assert ctrl.get_key_for_file(mapping, "student_v2") == "student"
    assert ctrl.get_key_for_file(mapping, "student_v2.csv") == "student"


def test_regex_fullmatch_ignorecase(ctrl):
    mapping = {"course": r"^course(?:[._-].+)?\.csv$"}
    assert ctrl.get_key_for_file(mapping, "Course_20240101.CSV") == "course"
    assert ctrl.get_key_for_file(mapping, "COURSE.csv") == "course"
    # ensure fullmatch (not substring)
    assert ctrl.get_key_for_file(mapping, "my_course_20240101.csv") is None


def test_list_values_mixed_literal_and_regex(ctrl):
    mapping = {"student": ["students.csv", r"^stud\d+\.csv$"]}
    assert ctrl.get_key_for_file(mapping, "STUD123.csv") == "student"
    assert ctrl.get_key_for_file(mapping, "students_2024.csv") == "student"


def test_invalid_regex_is_ignored(ctrl):
    mapping = {"bad": ["(unclosed", "ok.csv"]}
    # bad regex should be skipped; literal should match
    assert ctrl.get_key_for_file(mapping, "OK.csv") == "bad"


def test_returns_none_when_no_match(ctrl):
    mapping = {"student": "student.csv"}
    assert ctrl.get_key_for_file(mapping, "unknown.csv") is None


def _job_named(full_name: str, job_id: int = 42) -> MagicMock:
    j = MagicMock()
    j.job_id = job_id
    j.settings = MagicMock()
    j.settings.name = full_name
    return j


def test_resolve_pipeline_job_exact_match_skips_scan():
    canonical = "edvise_github_sourced_pdp_inference_pipeline"
    hit = _job_named(canonical, job_id=7)

    def list_jobs(name=None):
        if name == canonical:
            return iter([hit])
        return iter([])

    w = MagicMock()
    w.jobs.list.side_effect = list_jobs

    assert _resolve_pipeline_job(w, canonical, "test").job_id == 7
    w.jobs.list.assert_called_once()


def test_resolve_pipeline_job_substring_dev_prefix():
    canonical = "edvise_github_sourced_pdp_inference_pipeline"
    hit = _job_named(f"[dev vishakh] {canonical}", job_id=11)

    def list_jobs(name=None):
        if name is not None:
            return iter([])
        return iter([hit])

    w = MagicMock()
    w.jobs.list.side_effect = list_jobs

    assert _resolve_pipeline_job(w, canonical, "test").job_id == 11
    assert w.jobs.list.call_count == 2


def test_resolve_pipeline_job_ambiguous_substring_uses_first_match():
    canonical = "edvise_github_sourced_pdp_inference_pipeline"
    a = _job_named(f"[dev a] {canonical}", job_id=1)
    b = _job_named(f"[dev b] {canonical}", job_id=2)

    def list_jobs(name=None):
        if name is not None:
            return iter([])
        return iter([b, a])

    w = MagicMock()
    w.jobs.list.side_effect = list_jobs

    assert _resolve_pipeline_job(w, canonical, "test").job_id == 1


def test_resolve_pipeline_job_prefers_cloudrun_bundle_job():
    canonical = "edvise_github_sourced_pdp_inference_pipeline"
    jobs = [
        _job_named(f"[dev kayla] {canonical}", job_id=1),
        _job_named(f"{CLOUDRUN_BUNDLE_JOB_PREFIX} {canonical}", job_id=99),
        _job_named(f"[dev vishakh] {canonical}", job_id=3),
    ]

    def list_jobs(name=None):
        if name is not None:
            return iter([])
        return iter(jobs)

    w = MagicMock()
    w.jobs.list.side_effect = list_jobs

    assert _resolve_pipeline_job(w, canonical, "test").job_id == 99


def test_resolve_bronze_sync_job_id_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, "12345")
    w = mock.Mock()
    assert _resolve_validated_bronze_sync_job_id(w) == 12345
    w.jobs.list.assert_not_called()


def test_resolve_bronze_sync_job_id_env_invalid_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, "not-a-number")
    w = mock.Mock()
    with pytest.raises(ValueError, match="positive integer"):
        _resolve_validated_bronze_sync_job_id(w)


def test_resolve_bronze_sync_job_id_by_name_single(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, raising=False)
    monkeypatch.setenv("ENV", "DEV")
    job = mock.Mock(job_id=99)
    w = mock.Mock()
    w.jobs.list.return_value = [job]
    assert _resolve_validated_bronze_sync_job_id(w) == 99


def test_resolve_bronze_sync_job_id_by_name_ambiguous_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, raising=False)
    w = mock.Mock()
    w.jobs.list.return_value = [mock.Mock(job_id=1), mock.Mock(job_id=2)]
    with pytest.raises(ValueError, match="Multiple"):
        _resolve_validated_bronze_sync_job_id(w)


def test_resolve_bronze_sync_job_id_by_dev_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, raising=False)
    monkeypatch.setenv("ENV", "DEV")
    w = mock.Mock()
    w.jobs.list.return_value = []
    assert _resolve_validated_bronze_sync_job_id(w) == 1005654397694881
    w.jobs.list.assert_called_once_with(name="edvise_validated_gcs_to_bronze_sync")


def test_resolve_bronze_sync_job_id_by_staging_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, raising=False)
    monkeypatch.setenv("ENV", "STAGING")
    w = mock.Mock()
    w.jobs.list.return_value = []
    assert _resolve_validated_bronze_sync_job_id(w) == 611181637854021


def test_resolve_bronze_sync_job_id_by_prefixed_bundle_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, raising=False)
    monkeypatch.setenv("ENV", "LOCAL")
    job = SimpleNamespace(
        job_id=123,
        settings=SimpleNamespace(
            name="[dev dev_cloudrun_sa] edvise_validated_gcs_to_bronze_sync"
        ),
    )
    w = mock.Mock()
    w.jobs.list.side_effect = [[], [job]]
    assert _resolve_validated_bronze_sync_job_id(w) == 123
    assert w.jobs.list.call_args_list == [
        mock.call(name="edvise_validated_gcs_to_bronze_sync"),
        mock.call(),
    ]


def test_resolve_bronze_sync_job_id_by_prefixed_bundle_name_ambiguous_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, raising=False)
    monkeypatch.setenv("ENV", "LOCAL")
    w = mock.Mock()
    w.jobs.list.side_effect = [
        [],
        [
            SimpleNamespace(
                job_id=1,
                settings=SimpleNamespace(
                    name="[dev user_a] edvise_validated_gcs_to_bronze_sync"
                ),
            ),
            SimpleNamespace(
                job_id=2,
                settings=SimpleNamespace(
                    name="[dev user_b] edvise_validated_gcs_to_bronze_sync"
                ),
            ),
        ],
    ]
    with pytest.raises(ValueError, match="Multiple"):
        _resolve_validated_bronze_sync_job_id(w)


def test_resolve_bronze_sync_job_id_by_name_missing_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, raising=False)
    monkeypatch.setenv("ENV", "LOCAL")
    w = mock.Mock()
    w.jobs.list.return_value = []
    with pytest.raises(ValueError, match="not found"):
        _resolve_validated_bronze_sync_job_id(w)


def test_build_validated_bronze_sync_job_parameters_shape() -> None:
    req = DatabricksBronzeSyncRequest(
        inst_name="Test School",
        gcp_bucket_name="bucket-a",
        validated_blob_paths=["validated/student.csv"],
        batch_id="abc123batch",
    )
    params = _build_validated_bronze_sync_job_parameters(req, "test_school")
    assert params["gcp_bucket_name"] == "bucket-a"
    assert params["databricks_institution_name"] == "test_school"
    assert params["gcs_source_prefix"] == BRONZE_SYNC_GCS_SOURCE_PREFIX
    assert params["bronze_subdir"] == BRONZE_SYNC_BRONZE_SUBDIR
    assert params["max_objects"] == BRONZE_SYNC_MAX_OBJECTS
    assert params["require_at_least_one_file"] == BRONZE_SYNC_REQUIRE_AT_LEAST_ONE_FILE
    assert params["strict_mode"] == BRONZE_SYNC_STRICT_MODE
    assert params["batch_id"] == "abc123batch"
    assert params["include_blob_paths_json"] == '["validated/student.csv"]'


def test_build_shared_inference_job_parameters_shape() -> None:
    req = DatabricksSharedInferenceRunRequest(
        inst_name="Test School",
        model_name="retention_model",
        config_file_name="config.toml",
        gcp_external_bucket_name="bucket-a",
        email="user@example.com",
        batch_id="5b2420f3103546ab90eb74d5df97de43",
        validated_blob_paths=[
            "validated/course.csv",
            "validated/student.csv",
        ],
    )
    params = _build_shared_inference_job_parameters(req, "test_school")
    assert params["databricks_institution_name"] == "test_school"
    assert params["model_name"] == "retention_model"
    assert params["config_file_name"] == "config.toml"
    decimal_req = req.model_copy(
        update={"model_name": "graduation_in_3y_ft_4.5y_pt_checkpoint_30_credits"}
    )
    decimal_params = _build_shared_inference_job_parameters(decimal_req, "test_school")
    assert (
        decimal_params["model_name"]
        == "graduation_in_3y_ft_4d5y_pt_checkpoint_30_credits"
    )
    assert params["gcp_bucket_name"] == "bucket-a"
    assert params["datakind_notification_email"] == "user@example.com"
    assert params["batch_id"] == "5b2420f3103546ab90eb74d5df97de43"
    assert (
        params["validated_blob_paths_json"]
        == '["validated/course.csv","validated/student.csv"]'
    )
    assert "term_filter" not in params


def test_inference_job_parameters_include_term_filter_for_all_pipelines() -> None:
    term_filter = ["fall 2024-25", "spring 2024-25"]
    pdp_request = DatabricksPDPInferenceRunRequest(
        inst_name="Test School",
        filepath_to_type={
            "student.csv": [SchemaType.STUDENT],
            "course.csv": [SchemaType.COURSE],
        },
        model_name="retention_model",
        email="user@example.com",
        gcp_external_bucket_name="bucket-a",
        term_filter=term_filter,
    )
    shared_request = DatabricksSharedInferenceRunRequest(
        inst_name="Test School",
        model_name="retention_model",
        gcp_external_bucket_name="bucket-a",
        term_filter=term_filter,
    )

    assert (
        _build_pdp_inference_job_parameters(pdp_request, "test_school")["term_filter"]
        == '["fall 2024-25", "spring 2024-25"]'
    )
    assert (
        _build_shared_inference_job_parameters(shared_request, "test_school")[
            "term_filter"
        ]
        == '["fall 2024-25", "spring 2024-25"]'
    )


def test_download_bronze_training_inputs_rejects_path_traversal(
    ctrl: DatabricksControl,
) -> None:
    with pytest.raises(ValueError, match="basename"):
        ctrl.download_bronze_training_inputs_file("School", "../dataio.py")
    with pytest.raises(ValueError, match="basename"):
        ctrl.download_bronze_training_inputs_file("School", "foo/dataio.py")
    with pytest.raises(ValueError, match="one of"):
        ctrl.download_bronze_training_inputs_file("School", "secrets.py")


def test_download_bronze_training_inputs_file_builds_training_inputs_path(
    monkeypatch: pytest.MonkeyPatch, ctrl: DatabricksControl
) -> None:
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(
        db_mod.databricks_vars, "DATABRICKS_HOST_URL", "https://example.databricks.com"
    )
    monkeypatch.setitem(db_mod.databricks_vars, "CATALOG_NAME", "dev_catalog")
    monkeypatch.setitem(db_mod.gcs_vars, "GCP_SERVICE_ACCOUNT_EMAIL", "sa@example.com")

    mock_stream = MagicMock()
    mock_response = MagicMock()
    mock_response.contents = mock_stream
    workspace = MagicMock()
    workspace.files.download.return_value = mock_response

    with mock.patch.object(db_mod, "WorkspaceClient", return_value=workspace):
        result = ctrl.download_bronze_training_inputs_file("Edvise School", "dataio.py")

    assert result is mock_stream
    workspace.files.download.assert_called_once_with(
        "/Volumes/dev_catalog/edvise_school_bronze/bronze_volume/training_inputs/dataio.py"
    )


def test_download_bronze_training_inputs_allows_config_toml(
    monkeypatch: pytest.MonkeyPatch, ctrl: DatabricksControl
) -> None:
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(
        db_mod.databricks_vars, "DATABRICKS_HOST_URL", "https://example.databricks.com"
    )
    monkeypatch.setitem(db_mod.databricks_vars, "CATALOG_NAME", "dev_catalog")
    monkeypatch.setitem(db_mod.gcs_vars, "GCP_SERVICE_ACCOUNT_EMAIL", "sa@example.com")

    mock_stream = MagicMock()
    mock_response = MagicMock()
    mock_response.contents = mock_stream
    workspace = MagicMock()
    workspace.files.download.return_value = mock_response

    with mock.patch.object(db_mod, "WorkspaceClient", return_value=workspace):
        result = ctrl.download_bronze_training_inputs_file(
            "Edvise School", "config.toml"
        )

    assert result is mock_stream
    workspace.files.download.assert_called_once_with(
        "/Volumes/dev_catalog/edvise_school_bronze/bronze_volume/training_inputs/config.toml"
    )


def test_run_validated_gcs_to_bronze_sync_calls_run_now_with_bundle_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(DATABRICKS_VALIDATED_BRONZE_SYNC_JOB_ID_ENV, "42")
    workspace = mock.Mock()
    run_response = mock.Mock()
    run_response.response.run_id = 9001
    workspace.jobs.run_now.return_value = run_response

    with mock.patch("src.webapp.databricks.WorkspaceClient", return_value=workspace):
        ctrl = DatabricksControl()
        req = DatabricksBronzeSyncRequest(
            inst_name="My Inst",
            gcp_bucket_name="my-bucket",
            validated_blob_paths=["validated/foo.csv"],
        )
        resp = ctrl.run_validated_gcs_to_bronze_sync(req)

    assert resp.job_run_id == 9001
    workspace.jobs.run_now.assert_called_once()
    run_args, run_kwargs = workspace.jobs.run_now.call_args
    assert run_args[0] == 42
    params = run_kwargs["job_parameters"]
    assert params["include_blob_paths_json"] == '["validated/foo.csv"]'
    assert params["gcs_source_prefix"] == BRONZE_SYNC_GCS_SOURCE_PREFIX


def test_run_pdp_inference_sends_datakind_notification_email_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """run_pdp_inference must send the archived job's real parameter names.

    Regression test: this previously sent a stale ``notification_email`` key
    that doesn't match any declared job parameter, so the value was silently
    dropped and the launcher failed with a missing DK_CC_EMAIL error.
    """
    import src.webapp.databricks as db_mod

    monkeypatch.setitem(
        db_mod.databricks_vars, "DATABRICKS_HOST_URL", "https://example.databricks.com"
    )
    monkeypatch.setitem(db_mod.databricks_vars, "DATABRICKS_WORKSPACE", "dev_sst_02")
    monkeypatch.setitem(db_mod.gcs_vars, "GCP_SERVICE_ACCOUNT_EMAIL", "sa@example.com")

    workspace = mock.Mock()
    job = SimpleNamespace(job_id=123)
    workspace.jobs.list.return_value = iter([job])
    run_response = mock.Mock()
    run_response.response.run_id = 9002
    workspace.jobs.run_now.return_value = run_response

    with mock.patch.object(db_mod, "WorkspaceClient", return_value=workspace):
        ctrl = DatabricksControl()
        req = DatabricksPDPInferenceRunRequest(
            inst_name="My Inst",
            filepath_to_type={
                "student.csv": [SchemaType.STUDENT],
                "course.csv": [SchemaType.COURSE],
            },
            model_name="retention_model",
            email="user@example.com",
            gcp_external_bucket_name="my-bucket",
        )
        resp = ctrl.run_pdp_inference(req)

    assert resp.job_run_id == 9002
    workspace.jobs.run_now.assert_called_once()
    _run_args, run_kwargs = workspace.jobs.run_now.call_args
    params = run_kwargs["job_parameters"]
    assert params["datakind_notification_email"] == "user@example.com"
    assert params["DK_CC_EMAIL"] == "user@example.com"
    assert "notification_email" not in params
    assert "term_filter" not in params
