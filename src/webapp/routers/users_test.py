"""Test file for the users.py file and constituent API functions."""

import uuid
from datetime import datetime
from fastapi.testclient import TestClient
import pytest
import sqlalchemy
from sqlalchemy.pool import StaticPool

from ..test_helper import USR, DATAKINDER
from .users import router
from ..main import app
from ..database import AccountTable, InstTable, Base, get_session

from ..utilities import uuid_to_str, get_current_active_user

DATETIME_TESTING = datetime(2024, 12, 24, 20, 22, 20, 132022)
UUID_1 = uuid.UUID("64dbce41-111b-46fe-8e84-c38757477ef2")
USER_UUID = uuid.UUID("5301a352-c03d-4a39-beec-16c5668c4700")
USER_VALID_INST_UUID = uuid.UUID("1d7c75c3-3eda-4294-9c66-75ea8af97b55")
INVALID_UUID = uuid.UUID("27316b89-5e04-474a-9ea4-97beaf72c9af")


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
    user_1 = AccountTable(
        id=UUID_1,
        inst_id=USER_VALID_INST_UUID,
        name="John Smith",
        email="johnsmith@example.com",
        email_verified_at=None,
        password="xxxx",
        access_type="DATAKINDER",
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    user_2 = AccountTable(
        id=USER_UUID,
        inst_id=USER_VALID_INST_UUID,
        name="Jane Doe",
        email="janedoe@example.com",
        email_verified_at=None,
        password="xxxx",
        access_type="DATAKINDER",
        created_at=DATETIME_TESTING,
        updated_at=DATETIME_TESTING,
    )
    try:
        with sqlalchemy.orm.Session(engine) as session:
            session.add_all(
                [
                    InstTable(
                        id=USER_VALID_INST_UUID,
                        name="school_1",
                        allowed_emails={
                            "hello@example.com": "VIEWER",
                            "testy@test.test": "MODEL_OWNER",
                        },
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    ),
                    user_1,
                    user_2,
                ]
            )
            session.commit()
            yield session
    finally:
        Base.metadata.drop_all(engine)


@pytest.fixture(name="client")
def client_fixture(session: sqlalchemy.orm.Session):
    """Unit test mocks setup for non-Datakinder."""

    def get_session_override():
        return session

    def get_current_active_user_override():
        return USR

    app.include_router(router)
    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_current_active_user] = get_current_active_user_override

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


@pytest.fixture(name="datakinder_client")
def datakinder_client_fixture(session: sqlalchemy.orm.Session):
    """Unit test mocks setup for datakinder."""

    def get_session_override():
        return session

    def get_current_active_user_override():
        return DATAKINDER

    app.include_router(router)
    app.dependency_overrides[get_session] = get_session_override
    app.dependency_overrides[get_current_active_user] = get_current_active_user_override

    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_read_inst_users(client: TestClient):
    """Test GET /institutions/<uuid>/users."""
    response = client.get(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/users"
    )
    assert response.status_code == 200
    assert response.json() == [
        {
            "access_type": "DATAKINDER",
            "email": "johnsmith@example.com",
            "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
            "name": "John Smith",
            "user_id": "64dbce41111b46fe8e84c38757477ef2",
        },
        {
            "access_type": "DATAKINDER",
            "email": "janedoe@example.com",
            "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
            "name": "Jane Doe",
            "user_id": "5301a352c03d4a39beec16c5668c4700",
        },
    ]


def test_read_inst_user(client: TestClient):
    """Test GET /institutions/<uuid>/users/<uuid>. For various user access types."""
    # Authorized.
    response = client.get(
        "/institutions/"
        + uuid_to_str(USER_VALID_INST_UUID)
        + "/users/"
        + uuid_to_str(UUID_1)
    )
    assert response.status_code == 200
    assert response.json() == {
        "user_id": "64dbce41111b46fe8e84c38757477ef2",
        "name": "John Smith",
        "inst_id": "1d7c75c33eda42949c6675ea8af97b55",
        "access_type": "DATAKINDER",
        "email": "johnsmith@example.com",
    }


def test_read_inst_allowed_emails(datakinder_client: TestClient):
    """Test GET /institutions/<uuid>/allowable-emails."""
    # Authorized.
    response = datakinder_client.get(
        "/institutions/" + uuid_to_str(USER_VALID_INST_UUID) + "/allowable-emails",
    )
    assert response.status_code == 200
    assert response.json() == {
        "hello@example.com": "VIEWER",
        "testy@test.test": "MODEL_OWNER",
    }
