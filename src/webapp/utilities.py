"""Helper functions that may be used across multiple API router subpackages."""

import uuid
import re
from typing import Annotated, Final, Any, Iterable, Optional, Tuple, Union, cast
from urllib.parse import unquote_plus
from strenum import StrEnum  # needed for python pre 3.11
import jwt
from fastapi import HTTPException, status, Depends
from pydantic import BaseModel, ConfigDict
from jwt.exceptions import InvalidTokenError
from sqlalchemy.orm import Session
from sqlalchemy.future import select
from sqlalchemy import and_
from fastapi.security import HTTPAuthorizationCredentials
from edvise.utils.uc_model_name import (
    decode_uc_model_name,
    encode_uc_model_name,
)

from .authn import (
    verify_api_key,
    oauth2_apikey_scheme,
)
from .database import get_session, AccountTable, ApiKeyTable, FileTable
from .config import env_vars


def decode_url_piece(src: str) -> str:
    """Decode a URL path segment the way most clients encode names.

    Uses :func:`urllib.parse.unquote_plus` so ``+`` is treated as a space (common
    when clients apply form-style encoding to paths). A literal ``+`` in a name
    must be sent as ``%2B``.

    Decimal time limits (``4.5Y``) are encoded to ``4d5y`` so DB and Databricks
    lookups use the Unity Catalog name. UC always has ``4d5``, never ``4.5``.
    """
    return uc_model_name(unquote_plus(src))


def display_model_name(name: str) -> str:
    """Frontend-only: show ``4.5Y`` for a UC name that is always stored as ``4d5y``."""
    return cast(str, decode_uc_model_name(name)).replace("4.5y", "4.5Y")


def uc_model_name(name: str) -> str:
    """Encode display decimals for Unity Catalog / Databricks ids (``4.5Y`` → ``4d5y``)."""
    return cast(str, encode_uc_model_name(name.replace("4.5Y", "4.5y")))


def file_name_variants_for_lookup(name: str) -> set[str]:
    """Return spellings to try when matching a stored ``file.name``.

    Accepts common mismatches between spaces and ``+`` (and ``decode_url_piece``-style
    decoding) so batch endpoints stay usable even if the client and DB disagree.
    """
    n = unquote_plus(name.strip())
    if not n:
        return set()
    return {n, n.replace("+", " "), n.replace(" ", "+")}


def expand_batch_file_name_lookups(names: list[str]) -> list[str]:
    """Flatten :func:`file_name_variants_for_lookup` for a SQL ``IN`` clause."""
    expanded: set[str] = set()
    for name in names:
        expanded |= file_name_variants_for_lookup(name)
    return list(expanded)


class AccessType(StrEnum):
    """Access types available."""

    UNKNOWN = ""
    DATAKINDER = "DATAKINDER"
    MODEL_OWNER = "MODEL_OWNER"
    DATA_OWNER = "DATA_OWNER"
    VIEWER = "VIEWER"


class DataSource(StrEnum):
    """Where the Data was created from."""

    UNNOWN = ""
    PDP_SFTP = "PDP_SFTP"
    MANUAL_UPLOAD = "MANUAL_UPLOAD"


class ApiKeyResponse(BaseModel):
    """The API key object."""

    access_type: AccessType | None = None
    key: str
    inst_id: str | None = None
    allows_enduser: bool | None = None


class ApiKeyRequest(BaseModel):
    """The API key creation object."""

    access_type: AccessType
    inst_id: str | None = None
    allows_enduser: bool | None = None
    valid: bool | None = None
    notes: str | None = None


class UsState(StrEnum):
    """A U.S. state an inst is located in."""

    UNNOWN = ""
    AK = "AK"
    AL = "AL"
    AR = "AR"
    AZ = "AZ"
    CA = "CA"
    CO = "CO"
    CT = "CT"
    DE = "DE"
    FL = "FL"
    GA = "GA"
    HI = "HI"
    IA = "IA"
    ID = "ID"
    IL = "IL"
    IN = "IN"
    KS = "KS"
    KY = "KY"
    LA = "LA"
    MA = "MA"
    MD = "MD"
    ME = "ME"
    MI = "MI"
    MN = "MN"
    MO = "MO"
    MS = "MS"
    MT = "MT"
    NC = "NC"
    ND = "ND"
    NE = "NE"
    NH = "NH"
    NJ = "NJ"
    NM = "NM"
    NV = "NV"
    NY = "NY"
    OH = "OH"
    OK = "OK"
    OR = "OR"
    PA = "PA"
    RI = "RI"
    SC = "SC"
    SD = "SD"
    TN = "TN"
    TX = "TX"
    UT = "UT"
    VA = "VA"
    VT = "VT"
    WA = "WA"
    WI = "WI"
    WV = "WV"
    WY = "WY"


class SchemaType(StrEnum):
    """The schema type a given file adheres to."""

    # If an institution uses UNKNOWN as an allowed schema, it means bypass the validation check.
    UNKNOWN = "UNKNOWN"
    STUDENT = "STUDENT"
    SEMESTER = "SEMESTER"
    COURSE = "COURSE"

    # Schema Types of output files
    SST_OUTPUT = "SST_OUTPUT"
    PNG = "PNG"


LEGACY_TO_NEW_SCHEMA = {
    "PDP_COHORT": "STUDENT",
    "SST_PDP_COHORT": "STUDENT",
    "PDP_COURSE": "COURSE",
    "SST_PDP_COURSE": "COURSE",
    "SST_PDP_FINANCE": "STUDENT",
}

PDP_SCHEMA_GROUP: Final = {
    SchemaType.STUDENT,
    SchemaType.COURSE,
}

EDVISE_SCHEMA_GROUP: Final = {
    SchemaType.STUDENT,
    SchemaType.COURSE,
}

LEGACY_SCHEMA_GROUP: Final = {
    SchemaType.UNKNOWN,
}

GENAI_SCHEMA_GROUP: Final = {
    SchemaType.UNKNOWN,
}


def has_at_most_one_school_type(
    pdp_id: str | None,
    edvise_id: str | None,
    legacy_id: str | None,
    genai_id: str | None = None,
) -> bool:
    """
    Return True if at most one of pdp_id, edvise_id, legacy_id, or genai_id is set.

    Used to enforce mutual exclusivity: at most one of PDP, Edvise Schema (ES),
    Legacy, or GenAI may be set (create requires exactly one).

    Args:
        pdp_id: PDP institution identifier, or None.
        edvise_id: Edvise Schema (ES) institution identifier, or None.
        legacy_id: Legacy institution identifier, or None.
        genai_id: GenAI institution identifier, or None.

    Returns:
        True if zero or one of the four IDs is set; False if two or more are set.
    """
    return sum(bool(x) for x in (pdp_id, edvise_id, legacy_id, genai_id)) <= 1


class BaseUser(BaseModel):
    """BaseUser represents an access type. The frontend will include more detailed User info."""

    model_config = ConfigDict(use_enum_values=True)
    # user_id is permanent and each frontend orginated account will map to a unique user_id.
    # Bare API callers will likely not include a user_id.
    # The actual types of the ids will be UUIDs.
    user_id: str | None = None
    email: str | None = None
    # For Datakinders and people who have not been added to an institution, this is None which means "no inst specified".
    institution: str | None = None
    access_type: AccessType | None = None
    disabled: bool | None = None

    # Constructor
    def __init__(
        self, usr: str | None, inst: str | None, access: str | None, email: str | None
    ) -> None:
        super().__init__(user_id=usr, institution=inst, access_type=access, email=email)

    def is_datakinder(self) -> Any:
        """Whether a given user is a Datakinder."""
        return self.access_type and self.access_type == AccessType.DATAKINDER

    def is_model_owner(self) -> Any:
        """Whether a given user is a model owner."""
        return self.access_type and self.access_type == AccessType.MODEL_OWNER

    def is_data_owner(self) -> Any:
        """Whether a given user is a data owner."""
        return self.access_type and self.access_type == AccessType.DATA_OWNER

    def is_viewer(self) -> Any:
        """Whether a given user is a viewer."""
        return self.access_type and self.access_type == AccessType.VIEWER

    def has_access_to_inst(self, inst: str | None) -> Any:
        """Whether a given user has access to a given institution."""
        return self.access_type and (
            self.access_type == AccessType.DATAKINDER or self.institution == inst
        )

    def has_full_data_access(self) -> Any:
        """Datakinders, model_owners, data_owners, all have full data access."""
        return self.access_type and self.access_type in (
            AccessType.DATAKINDER,
            AccessType.MODEL_OWNER,
            AccessType.DATA_OWNER,
        )

    def has_stronger_permissions_than(self, other_access_type: AccessType) -> bool:
        """Check that self has stronger permissions than other."""
        if not self.access_type:
            return False
        if self.access_type == AccessType.DATAKINDER:
            return True
        if self.access_type == AccessType.MODEL_OWNER:
            return other_access_type in (
                AccessType.MODEL_OWNER,
                AccessType.DATA_OWNER,
                AccessType.VIEWER,
            )
        if self.access_type == AccessType.DATA_OWNER:
            return other_access_type in (AccessType.DATA_OWNER, AccessType.VIEWER)
        if self.access_type == AccessType.VIEWER:
            return other_access_type == AccessType.VIEWER
        return False


def get_user(sess: Session, username: str) -> Optional[BaseUser]:
    """Get user from a given username."""
    if username == "api_key_initial":
        return BaseUser(
            usr=str(env_vars["INITIAL_API_KEY_ID"]),
            inst=None,
            access="DATAKINDER",
            email="api_key_initial",
        )
    if username.startswith("api_key_"):
        api_key_uuid = username.removeprefix("api_key_")
        apikey_query_result = sess.execute(
            select(ApiKeyTable).where(
                ApiKeyTable.id == str_to_uuid(api_key_uuid),
            )
        ).all()
        if len(apikey_query_result) == 0 or len(apikey_query_result) > 1:
            return None
        return BaseUser(
            usr=uuid_to_str(apikey_query_result[0][0].id),
            inst=uuid_to_str(apikey_query_result[0][0].inst_id),
            access=apikey_query_result[0][0].access_type,
            email=username,
        )
    query_result = sess.execute(
        select(AccountTable).where(
            AccountTable.email == username,
        )
    ).all()
    if len(query_result) == 0 or len(query_result) > 1:
        return None
    return BaseUser(
        usr=uuid_to_str(query_result[0][0].id),
        inst=uuid_to_str(query_result[0][0].inst_id),
        access=query_result[0][0].access_type,
        email=username,
    )


def authenticate_api_key(
    api_key_enduser_tuple: Tuple[str, Optional[str], Optional[str]], sess: Session
) -> Union[BaseUser, bool]:
    """Authenticate an API key."""
    (key, inst, enduser) = api_key_enduser_tuple
    # Check if it's the initial API key. This doesn't have enduser or inst.
    if key == env_vars["INITIAL_API_KEY"]:
        return BaseUser(
            usr=str(env_vars["INITIAL_API_KEY_ID"]),
            inst=None,
            access="DATAKINDER",
            email="api_key_initial",
        )
    query_result = sess.execute(
        select(ApiKeyTable).where(
            ApiKeyTable.inst_id == inst,
        )
    ).all()
    if len(query_result) == 0:
        return False
    for elem in query_result:
        if (
            verify_api_key(key, elem[0].hashed_key_value)
            and elem[0].valid
            and not elem[0].deleted
        ):
            # If enduser is set, only allow if the API allows the enduser and the enduser exists.
            if enduser:
                if not elem[0].allows_enduser:
                    return False
                user_query = select(AccountTable).where(
                    AccountTable.email == enduser,
                )
                if inst:
                    # If there's an institution set, ensure the user is part of the institution.
                    # Datakinders should not set an institution in the header.
                    user_query = select(AccountTable).where(
                        and_(
                            AccountTable.email == enduser,
                            AccountTable.inst_id == inst,
                        )
                    )
                user_result = sess.execute(user_query).all()
                if len(user_result) == 0 or len(user_result) > 1:
                    return False
                return BaseUser(
                    usr=uuid_to_str(user_result[0][0].id),
                    inst=uuid_to_str(user_result[0][0].inst_id),
                    access=user_result[0][0].access_type,
                    email=enduser,
                )
            return BaseUser(
                usr=uuid_to_str(elem[0].id),
                inst=uuid_to_str(elem[0].inst_id),
                access=elem[0].access_type,
                email="api_key_" + uuid_to_str(elem[0].id),
            )
    return False


async def get_current_user(
    sess: Annotated[Session, Depends(get_session)],
    token_from_key: Annotated[
        HTTPAuthorizationCredentials, Depends(oauth2_apikey_scheme)
    ],
) -> BaseUser:
    """Get the user from a given token."""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    usrname = None
    token_from_key = token_from_key.credentials
    try:
        if not token_from_key:
            raise credentials_exception
        payload = jwt.decode(
            token_from_key,
            str(env_vars["SECRET_KEY"]),
            algorithms=env_vars["ALGORITHM"],
        )
        usrname = payload.get("sub")
        if usrname is None:
            raise credentials_exception
    except InvalidTokenError as e:
        raise credentials_exception from e
    user = get_user(sess, username=usrname)
    if user is None:
        raise credentials_exception
    return user


async def get_current_active_user(
    current_user: Annotated[BaseUser, Depends(get_current_user)],
) -> BaseUser:
    """Get the active user.."""
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user


def has_access_to_inst_or_err(inst: str, user: BaseUser) -> None:
    """Raise error if a given user does not have access to a given institution."""
    if not user.has_access_to_inst(inst):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authorized to read this institution's resources.",
        )


def has_full_data_access_or_err(user: BaseUser, resource_type: str) -> None:
    """Raise error if a given user does not have data access to a given institution."""
    if not user.has_full_data_access():
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authorized to view " + resource_type + " for this institution.",
        )


def model_owner_and_higher_or_err(user: BaseUser, resource_type: str) -> None:
    """Raise error if a given user does not have model ownership or higher."""
    if not user.access_type or user.access_type not in (
        AccessType.MODEL_OWNER,
        AccessType.DATAKINDER,
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="No permissions for " + resource_type + " for this institution.",
        )


def prepend_env_prefix(name: str) -> Any:
    """Prepend the env prefix. At this point the value should not be empty as we checked on app startup."""
    env = str(env_vars["ENV"]).lower()
    # Use dev_ prefix for LOCAL environment
    if env == "local":
        env = "dev"
    return env + "_" + name


def uuid_to_str(uuid_val: uuid.UUID) -> Any:
    """Convert UUID obj to string."""
    if uuid_val is None:
        return ""
    return uuid_val.hex


def str_to_uuid(hex_str: Optional[str]) -> uuid.UUID:
    """Convert str to UUID obj (database needs UUID obj)."""
    return uuid.UUID(hex_str)


def batch_input_validated_blob_paths(
    files: Iterable[FileTable],
) -> list[str]:
    """Return validated/ GCS paths for non-SST-generated batch input files."""
    paths = sorted(f"validated/{f.name}" for f in files if not f.sst_generated)
    if not paths:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Batch has no input files.",
        )
    return paths


def get_external_bucket_name_from_uuid(inst_id: uuid.UUID) -> Any:
    """Get the GCP bucket name which has the env prepended taking in the UUID obj."""
    return prepend_env_prefix(uuid_to_str(inst_id))


def get_external_bucket_name(inst_id: str) -> Any:
    """Get the GCP bucket name which has the env prepended taking in the uuid as str."""
    return prepend_env_prefix(inst_id)


def databricksify_inst_name(inst_name: str) -> str:
    """
    Follow DK standardized rules for naming conventions used in Databricks.
    """
    name = inst_name.lower()
    # This needs to be in order from most verbose and encompassing other replacement keys to least.
    dk_replacements = {
        "community technical college": "ctc",
        "community college": "cc",
        "of science and technology": "st",
        "university": "uni",
        "college": "col",
    }

    for old, new in dk_replacements.items():
        name = name.replace(old, new)
    special_char_replacements = {" & ": " ", "&": " ", "-": " "}

    for old, new in special_char_replacements.items():
        name = name.replace(old, new)

    # Databricks uses underscores, so we'll do that here.
    final_name = name.replace(" ", "_")

    # Check to see that no special characters exist
    pattern = "^[a-z0-9_]*$"
    if not re.match(pattern, final_name):
        raise ValueError("Unexpected character found in Databricks compatible name.")
    return final_name
