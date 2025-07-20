"""Database configuration."""

import uuid
import datetime
from typing import Set, List
from contextvars import ContextVar
import enum
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.mutable import MutableDict, MutableList
from sqlalchemy import (
    Column,
    Uuid,
    DateTime,
    ForeignKey,
    Table,
    String,
    UniqueConstraint,
    Text,
    Enum,
    Boolean,
    JSON,
    Integer,
    BigInteger,
    Index,
)
from sqlalchemy.orm import sessionmaker, Session, relationship, mapped_column, Mapped
from sqlalchemy.sql import func
from sqlalchemy.pool import StaticPool
from .config import engine_vars, ssl_env_vars, setup_database_vars
from .authn import get_password_hash, get_api_key_hash

Base = declarative_base()
LocalSession = None
local_session: ContextVar[Session] = ContextVar("local_session")
db_engine = None

# GCP MYSQL will throw an error if we don't specify the length for any varchar
# fields. So we can't use mapped_column in string cases.
VAR_CHAR_LENGTH = 36
VAR_CHAR_STANDARD_LENGTH = 255
# Constants for the local env
LOCAL_INST_UUID = uuid.UUID("14c81c50-935e-4151-8561-c2fc3bdabc0f")
LOCAL_USER_UUID = uuid.UUID("f21a3e53-c967-404e-91fd-f657cb922c39")
LOCAL_APIKEY_UUID = uuid.UUID("cd65804d-c597-40c2-bc89-0b0810e66346")
LOCAL_USER_EMAIL = "tester@datakind.org"
LOCAL_PASSWORD = "tester_password"
DATETIME_TESTING = datetime.datetime(2024, 12, 26, 19, 37, 59, 753357)


def init_db(env: str):
    """Initialize the database for LOCAL and DEV environemtns for ease of use."""
    # add some sample users to the database for development utility.
    if env not in ("LOCAL", "DEV"):
        # this init db setup only applies for Local and Dev.
        return
    try:
        with sqlalchemy.orm.Session(db_engine) as session:
            # Only create a fake user in the local environment. The backend doesn't allow creating users, so
            # in dev, users should be created in the frontend.
            if env == "LOCAL":
                session.merge(
                    AccountTable(
                        id=LOCAL_USER_UUID,
                        inst_id=None,
                        name="Tester S",
                        email=LOCAL_USER_EMAIL,
                        email_verified_at=None,
                        password=get_password_hash(LOCAL_PASSWORD),
                        access_type="DATAKINDER",
                        created_at=DATETIME_TESTING,
                        updated_at=DATETIME_TESTING,
                    )
                )
            session.merge(
                InstTable(
                    id=LOCAL_INST_UUID,
                    name="Foobar University",
                    created_at=DATETIME_TESTING,
                    updated_at=DATETIME_TESTING,
                )
            )
            session.merge(
                ApiKeyTable(
                    id=LOCAL_APIKEY_UUID,
                    hashed_key_value=get_api_key_hash("key_1"),
                    allows_enduser=True,
                    created_at=DATETIME_TESTING,
                    updated_at=DATETIME_TESTING,
                    created_by=LOCAL_USER_UUID,
                    access_type="DATAKINDER",
                    valid=True,
                )
            )
            session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


class InstTable(Base):
    """The institution overview table that maps ids to names. The parent table to
    all other tables except for AccountHistory and JobTable."""

    __tablename__ = "inst"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Linked children tables.
    accounts: Mapped[Set["AccountTable"]] = relationship(back_populates="inst")
    apikeys: Mapped[Set["ApiKeyTable"]] = relationship(back_populates="inst")
    files: Mapped[Set["FileTable"]] = relationship(back_populates="inst")
    batches: Mapped[Set["BatchTable"]] = relationship(back_populates="inst")
    account_histories: Mapped[List["AccountHistoryTable"]] = relationship(
        back_populates="inst"
    )
    models: Mapped[Set["ModelTable"]] = relationship(back_populates="inst")
    schemas_registry: Mapped[List["SchemaRegistryTable"]] = relationship(
        "SchemaRegistryTable", back_populates="inst", cascade="all, delete-orphan"
    )

    name = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False, unique=True)
    # If retention unset, the Datakind default is used. File-level retentions overrides
    # this value.
    retention_days: Mapped[int] = mapped_column(nullable=True)
    # The emails for which self sign up will be allowed for this institution and will automatically be assigned to this institution.
    # The dict structure is {email: AccessType string}
    allowed_emails = Column(MutableDict.as_mutable(JSON))
    # Schemas that are allowed for validation.
    schemas = Column(MutableList.as_mutable(JSON))
    state = Column(String(VAR_CHAR_LENGTH), nullable=True)
    # Only populated for PDP schools.
    pdp_id = Column(String(VAR_CHAR_LENGTH), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    created_by = Column(Uuid(as_uuid=True), nullable=True)

    # Within the institutions, the set of name + state should be unique
    __table_args__ = (UniqueConstraint("name", "state", name="inst_name_state_uc"),)


class ApiKeyTable(Base):
    """API KEYS should match the format generated by `openssl rand -hex 32`"""

    __tablename__ = "apikey"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    # A hash of the key_value, so the user must store the generated key_value secretly.
    hashed_key_value = Column(
        String(VAR_CHAR_STANDARD_LENGTH), nullable=False, unique=True
    )

    # Set the foreign key to link to the institution table.
    inst_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("inst.id", ondelete="CASCADE"),
        nullable=True,
    )
    inst: Mapped["InstTable"] = relationship(back_populates="apikeys")
    created_by = Column(Uuid(as_uuid=True), nullable=False)
    notes = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=True)
    # Whether this key allows changing the enduser. ONLY SET FOR THE FRONTEND KEY. Can only be set when the API key has DATAKINDER access type as this allows Datakinder level endusers.
    allows_enduser: Mapped[bool] = mapped_column(nullable=True)

    access_type = Column(String(VAR_CHAR_LENGTH), nullable=False)
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    # API key must be valid and not deleted.
    deleted: Mapped[bool] = mapped_column(nullable=True)
    valid: Mapped[bool] = mapped_column(nullable=True)

    # Within the api keys, the set of inst + access type should be unique
    __table_args__ = (
        UniqueConstraint("inst_id", "access_type", name="apikeys_inst_access_uc"),
    )


class AccountTable(Base):
    """
    NOTE: only users created by the frontend are accessible through the fronted. Users created by API calls can only directly call API calls. Frontend will not work.
    The user accounts table"""

    __tablename__ = "users"  # Name to be compliant with Laravel.
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Set account histories to be children
    account_histories: Mapped[List["AccountHistoryTable"]] = relationship(
        back_populates="account"
    )

    # Set the foreign key to link to the institution table.
    inst_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("inst.id", ondelete="CASCADE"),
        nullable=True,
    )
    inst: Mapped["InstTable"] = relationship(back_populates="accounts")

    name = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False)
    email = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False, unique=True)
    google_id = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=True)
    azure_id = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=True)

    email_verified_at = Column(DateTime(timezone=True), nullable=True)
    password = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False)
    two_factor_secret = Column(Text, nullable=True)
    two_factor_recovery_codes = Column(Text, nullable=True)
    two_factor_confirmed_at = Column(DateTime(timezone=True), nullable=True)

    remember_token = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=True)
    # Required for team integration with laravel
    current_team_id = Column(Uuid(as_uuid=True), nullable=True)
    access_type = Column(String(VAR_CHAR_LENGTH), nullable=True)
    # profile_photo_path = Column(String(VAR_CHAR_LENGTH), nullable=True)
    created_at = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())


class AccountHistoryTable(Base):
    """The user history table"""

    __tablename__ = "account_history"
    id = Column(Integer, primary_key=True)  # Auto-increment should be default
    timestamp = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Set the parent foreign key to link to the users table.
    account_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    account: Mapped["AccountTable"] = relationship(back_populates="account_histories")

    # This field is nullable if the action was taken by a Datakinder.
    inst_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("inst.id", ondelete="CASCADE"),
        nullable=True,
    )
    inst: Mapped["InstTable"] = relationship(back_populates="account_histories")

    action = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False)
    resource_id = Column(Uuid(as_uuid=True), nullable=False)


# An intermediary association table allows bi-directional many-to-many between files and batches.
association_table = Table(
    "file_batch_association_table",
    Base.metadata,
    Column(
        "file_val",
        Uuid(as_uuid=True),
        ForeignKey("file.id", ondelete="CASCADE"),
        primary_key=True,
    ),
    Column(
        "batch_val",
        Uuid(as_uuid=True),
        ForeignKey("batch.id", ondelete="CASCADE"),
        primary_key=True,
    ),
)


class FileTable(Base):
    """The file table"""

    __tablename__ = "file"
    name = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False)
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    batches: Mapped[Set["BatchTable"]] = relationship(
        secondary=association_table, back_populates="files"
    )
    # Set the parent foreign key to link to the institution table.
    inst_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("inst.id", ondelete="CASCADE"),
        nullable=False,
    )
    inst: Mapped["InstTable"] = relationship(back_populates="files")
    # The size to the nearest mb.
    # size_mb: Mapped[int] = mapped_column(nullable=False)
    # Who uploaded the file. For SST generated files, this field would be null.
    uploader = Column(Uuid(as_uuid=True), nullable=True)
    # Can be PDP_SFTP, MANUAL_UPLOAD etc. May be empty for generated files.
    source = Column(String(VAR_CHAR_LENGTH), nullable=True)
    # The schema type(s) of this file.
    schemas = Column(MutableList.as_mutable(JSON), nullable=False)
    # If null, the following is non-deleted.
    # The deleted field indicates whether there is a pending deletion request on the data.
    # The data may stil be available to Datakind debug role in a soft-delete state but for all
    # intents and purposes is no longer accessible by the app.
    deleted: Mapped[bool] = mapped_column(nullable=True)
    # When the deletion request was made
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    retention_days: Mapped[int] = mapped_column(nullable=True)
    # Whether the file was generated by SST. (e.g. was it input or output)
    sst_generated: Mapped[bool] = mapped_column(nullable=False)
    # Whether the file was approved (in the case of output) or valid for input.
    valid: Mapped[bool] = mapped_column(nullable=False)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

    # Within a given institution, there should be no duplicated file names.
    __table_args__ = (UniqueConstraint("name", "inst_id", name="file_name_inst_uc"),)


class BatchTable(Base):
    """The batch table"""

    __tablename__ = "batch"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Set the parent foreign key to link to the institution table.
    inst_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("inst.id", ondelete="CASCADE"),
        nullable=False,
    )
    inst: Mapped["InstTable"] = relationship(back_populates="batches")

    files: Mapped[Set["FileTable"]] = relationship(
        secondary=association_table, back_populates="batches"
    )

    name = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False)
    created_by = Column(Uuid(as_uuid=True))
    # If null, the following is non-deleted.
    deleted: Mapped[bool] = mapped_column(nullable=True)
    # If true, the batch is ready for use.
    completed: Mapped[bool] = mapped_column(nullable=True)
    # The time the deletion request was set.
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    # If a batch is deleted, the uuid of the user in the updated_by section is the deleter.
    updated_by = Column(Uuid(as_uuid=True), nullable=True)
    # Within a given institution, there should be no duplicated batch names.
    __table_args__ = (UniqueConstraint("name", "inst_id", name="batch_name_inst_uc"),)


class ModelTable(Base):
    """The model table"""

    __tablename__ = "model"
    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Set the parent foreign key to link to the institution table.
    inst_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("inst.id", ondelete="CASCADE"),
        nullable=False,
    )
    inst: Mapped["InstTable"] = relationship(back_populates="models")

    jobs: Mapped[Set["JobTable"]] = relationship(back_populates="model")

    name = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False)
    # What configuration of schemas are allowed (list of maps e.g. [PDP Course : 1 + PDP Cohort : 1, X_schema :1 + Y_schema: 2])
    schema_configs = Column(JSON, nullable=True)
    created_by = Column(Uuid(as_uuid=True), nullable=True)
    # If null, the following is non-deleted.
    deleted: Mapped[bool] = mapped_column(nullable=True)
    # If true, the model has been approved and is ready for use.
    valid: Mapped[bool] = mapped_column(nullable=True)
    # The time the deletion request was set.
    deleted_at = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    # version is unused. version is not currently supported. The webapp only knows about the name of the model and any usages of a model will only use the live version.
    version = Column(Integer, default=0)

    # Within a given institution, there should be no duplicated model names.
    __table_args__ = (UniqueConstraint("name", "inst_id", name="model_name_inst_uc"),)


class JobTable(Base):
    """The job table"""

    __tablename__ = "job"
    id = Column(BigInteger, primary_key=True)

    # Set the parent foreign key to link to the institution table.
    model_id = Column(
        Uuid(as_uuid=True),
        ForeignKey("model.id", ondelete="CASCADE"),
        nullable=False,
    )
    model: Mapped["ModelTable"] = relationship(back_populates="jobs")

    created_by = Column(Uuid(as_uuid=True), nullable=False)
    # The time the deletion request was set.
    triggered_at = Column(DateTime(timezone=True), nullable=False)
    batch_name = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=False)
    # The following will be empty if not completed or if job errored out. Getting additional details will require a call to the Databricks table.
    output_filename = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=True)
    # Whether the file was approved.
    output_valid: Mapped[bool] = mapped_column(nullable=True, default=False)
    err_msg = Column(String(VAR_CHAR_STANDARD_LENGTH), nullable=True)
    completed: Mapped[bool] = mapped_column(nullable=True)


class DocType(enum.Enum):
    base = "base"
    extension = "extension"


class SchemaRegistryTable(Base):
    """
    Stores versioned schema documents:
      - Base schema (doc_type=base, is_pdp=False, inst_id NULL)
      - PDP shared extension (doc_type=extension, is_pdp=True, inst_id NULL)
      - Custom institution extension (doc_type=extension, is_pdp=False, inst_id=<UUID>)
    Layers can reference a parent (extends_schema_id) that they extend.
    """

    __tablename__ = "schema_registry"

    schema_id: Mapped[int] = mapped_column(
        BigInteger, primary_key=True, autoincrement=True
    )
    doc_type: Mapped[DocType] = mapped_column(Enum(DocType), nullable=False)
    # Nullable: NULL for base and PDP shared extension
    inst_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("inst.id", ondelete="RESTRICT", onupdate="CASCADE"), nullable=True
    )
    is_pdp: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    version_label: Mapped[str] = mapped_column(String(32), nullable=False)
    extends_schema_id: Mapped[int | None] = mapped_column(
        BigInteger,
        ForeignKey(
            "schema_registry.schema_id", ondelete="SET NULL", onupdate="CASCADE"
        ),
        nullable=True,
    )
    json_doc: Mapped[dict] = mapped_column(MutableDict.as_mutable(JSON), nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # ---------------- Relationships ----------------
    inst: Mapped["InstTable | None"] = relationship(
        "InstTable",
        back_populates="schemas_registry",  # we'll add this new relationship on InstTable (see below)
    )

    parent_schema: Mapped["SchemaRegistryTable | None"] = relationship(
        "SchemaRegistry",
        remote_side="SchemaRegistry.schema_id",
        foreign_keys=[extends_schema_id],
        back_populates="child_schemas",
    )

    child_schemas: Mapped[List["SchemaRegistryTable"]] = relationship(
        "SchemaRegistry", back_populates="parent_schema", cascade="all, delete-orphan"
    )

    __table_args__ = (
        UniqueConstraint("doc_type", "version_label", name="uq_base_version"),
        UniqueConstraint("is_pdp", "version_label", name="uq_pdp_version"),
        UniqueConstraint("inst_id", "version_label", name="uq_inst_version"),
        Index("idx_schema_active_base", "doc_type", "is_active"),
        Index("idx_schema_active_pdp", "is_pdp", "is_active"),
        Index("idx_schema_active_inst", "inst_id", "is_active"),
    )

    # Convenience: identify logical namespace
    @property
    def namespace(self) -> str:
        if self.doc_type == DocType.base:
            return "base"
        if self.is_pdp:
            return "pdp"
        if self.inst_id:
            return f"inst:{self.inst_id}"
        return "unknown"


def get_session():
    """Get the session."""
    sess: Session = LocalSession()
    try:
        yield sess
        sess.commit()
    except Exception as e:
        sess.rollback()
        raise e
    finally:
        sess.close()


def init_connection_pool_local() -> sqlalchemy.engine.base.Engine:
    """Creates a local sqlite db for local env testing."""
    return sqlalchemy.create_engine(
        "sqlite://",
        echo=True,
        echo_pool="debug",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


# The following functions are heavily inspired from the GCP open source sample code.


def connect_tcp_socket(
    engine_args: dict[str, str], connect_args: dict[str, str]
) -> sqlalchemy.engine.base.Engine:
    """Initializes a TCP connection pool for a Cloud SQL instance of MySQL."""
    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # mysql+pymysql://<db_user>:<db_pass>@<db_host>:<db_port>/<db_name>
        # https://github.com/GoogleCloudPlatform/python-docs-samples/blob/main/cloud-sql/postgres/sqlalchemy/connect_tcp.py
        sqlalchemy.engine.url.URL.create(
            drivername="mysql+pymysql",
            username=engine_args["DB_USER"],
            password=engine_args["DB_PASS"],
            host=engine_args["INSTANCE_HOST"],
            port=engine_args["DB_PORT"],
            database=engine_args["DB_NAME"],
        ),
        connect_args=connect_args,
        # Pool size is the maximum number of permanent connections to keep.
        pool_size=5,
        # Temporarily exceeds the set pool_size if no connections are available.
        max_overflow=2,
        # The total number of concurrent connections for your application will be
        # a total of pool_size and max_overflow.
        # SQLAlchemy automatically uses delays between failed connection attempts,
        # but provides no arguments for configuration.
        # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
        # new connection from the pool. After the specified amount of time, an
        # exception will be thrown.
        pool_timeout=30,  # 30 seconds
        # 'pool_recycle' is the maximum number of seconds a connection can persist.
        # Connections that live longer than the specified amount of time will be
        # re-established
        pool_recycle=1800,  # 30 minutes
    )
    return pool


def init_connection_pool() -> sqlalchemy.engine.Engine:
    """Helper function to return SQLAlchemy connection pool."""
    setup_database_vars()
    # Set up ssl context for the connection args.
    ssl_args = {
        "ssl_ca": ssl_env_vars["DB_ROOT_CERT"],
        "ssl_cert": ssl_env_vars["DB_CERT"],
        "ssl_key": ssl_env_vars["DB_KEY"],
    }
    return connect_tcp_socket(engine_vars, ssl_args)


def setup_db(env: str):
    """Setup Database. Called by all environments."""
    # initialize connection pool
    global db_engine
    if env == "LOCAL":
        db_engine = init_connection_pool_local()
    else:
        db_engine = init_connection_pool()
    # Integrating FastAPI with SQL DB
    # create SQLAlchemy ORM session
    global LocalSession
    LocalSession = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    # TODO: instead of create_all, check if exists and only create all if not existing
    # https://stackoverflow.com/questions/33053241/sqlalchemy-if-table-does-not-exist
    Base.metadata.create_all(db_engine)
    if env in ("LOCAL", "DEV"):
        # Creates a fake user in the local db
        init_db(env)
