"""Test file for the main.py file and constituent API functions."""

import pytest
import json
from typing import Any, Annotated

from fastapi.testclient import TestClient
from .main import app
import uuid
from .authn import get_current_username, get_api_key
from unittest import mock
from .utilities import StorageControl
from unittest.mock import patch

MOCK_STORAGE = mock.Mock()


@pytest.fixture(name="client")
def client_fixture():
    def get_current_username_override():
        return "testing_username"

    def storage_control_override():
        return MOCK_STORAGE

    def get_api_key_override():
        return ("valid_api_key", "end_user")

    app.dependency_overrides[StorageControl] = storage_control_override

    app.dependency_overrides[get_current_username] = get_current_username_override

    app.dependency_overrides[get_api_key] = get_api_key_override
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


def test_execute_pdp_pull(client: TestClient) -> Any:
    """Test POST /execute-pdp-pull."""
    MOCK_STORAGE.copy_from_sftp_to_gcs.side_effect = (
        lambda filename: f"processed_{filename}"
    )
    MOCK_STORAGE.create_bucket_if_not_exists.return_value = None
    MOCK_STORAGE.list_sftp_files.return_value = [
        {"path": "file1.csv"},
        {"path": "file2.csv"},
    ]

    response = client.post("/execute-pdp-pull", json={"placeholder": "val"})

    # Verify the response status and content.
    assert response.status_code == 200
    assert response.json() == {
        "sftp_files": [{"path": "file1.csv"}, {"path": "file2.csv"}],
        "pdp_inst_generated": [],
        "pdp_inst_not_found": [],
    }
