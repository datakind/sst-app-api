"""Test file for the main.py file and constituent API functions."""

import pytest
from typing import Any

from fastapi.testclient import TestClient
from .main import app
from .authn import get_current_username
from unittest import mock
from .utilities import StorageControl
from unittest.mock import patch, MagicMock
from .config import env_vars

MOCK_STORAGE = mock.Mock()


@pytest.fixture(name="client")
def client_fixture():
    def get_current_username_override():
        return "testing_username"

    def storage_control_override():
        return MOCK_STORAGE

    app.dependency_overrides[StorageControl] = storage_control_override

    app.dependency_overrides[get_current_username] = get_current_username_override

    client = TestClient(app, root_path="/workers/api/v1")
    yield client
    app.dependency_overrides.clear()


def test_get_root(client: TestClient) -> Any:
    """Test GET /."""
    response = client.get("/")
    assert response.status_code == 200


def test_retrieve_token(client: TestClient) -> Any:
    """Test POST /token."""
    response = client.post(
        "/token",
        data={"username": "tester-user", "password": "tester-pw"},
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert response.status_code == 200


@patch("google.auth.default")
def test_execute_pdp_pull(
    mock_auth_default: Any, client: TestClient, monkeypatch: Any
) -> None:
    """Test POST /execute-pdp-pull with mocked authentication."""
    # Set up dummy credentials with the correct universe_domain.
    monkeypatch.setitem(env_vars, "BACKEND_API_KEY", "dummy_api_key")
    monkeypatch.setitem(env_vars, "BUCKET_ENV", "testbucket")
    monkeypatch.setitem(env_vars, "WEBAPP_URL", "https://example.com")
    dummy_credentials = MagicMock()
    dummy_credentials.token = "fake-token"
    dummy_credentials.universe_domain = "googleapis.com"  # Set the expected domain
    mock_auth_default.return_value = (dummy_credentials, "dummy-project")

    MOCK_STORAGE.copy_from_sftp_to_gcs.side_effect = (
        lambda sftp_host, sftp_port, sftp_user, sftp_password, sftp_file, bucket_name, blob_name: None
    )
    MOCK_STORAGE.create_bucket_if_not_exists.return_value = None
    MOCK_STORAGE.list_sftp_files.return_value = [
        {"path": "file1.csv"},
        {"path": "file2.csv"},
    ]
    # Optionally, if there's a process_file or similar function, you can mock it too.
    # For this test, we're focusing on the overall endpoint behavior.

    response = client.post("/execute-pdp-pull", json={"placeholder": "val"})

    # Verify the response status and content.
    assert response.status_code == 200
    assert response.json() == {
        "sftp_files": [{"path": "file1.csv"}, {"path": "file2.csv"}],
        "pdp_inst_generated": [],
        "pdp_inst_not_found": [],
        "upload_status": {},
    }
